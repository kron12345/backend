import { Injectable, Logger } from '@nestjs/common';
import { DatabaseService } from '../database/database.service';
import {
  Activity,
  PersonnelPool,
  PersonnelServicePool,
  Resource,
  StageId,
  TimelineRange,
  VehicleComposition,
  VehiclePool,
  VehicleServicePool,
  VehicleType,
} from './planning.types';

interface StageRow {
  stage_id: string;
  version: string | null;
  timeline_start: string;
  timeline_end: string;
}

interface ResourceRow {
  id: string;
  name: string;
  kind: string;
  daily_service_capacity: number | null;
  attributes: Record<string, unknown> | null;
}

interface ActivityRow {
  id: string;
  resource_id: string;
  participant_resource_ids: string[] | null;
  client_id: string | null;
  title: string;
  start: string;
  end: string | null;
  type: string | null;
  from: string | null;
  to: string | null;
  remark: string | null;
  service_id: string | null;
  service_template_id: string | null;
  service_date: string | Date | null;
  service_category: string | null;
  service_role: string | null;
  location_id: string | null;
  location_label: string | null;
  capacity_group_id: string | null;
  required_qualifications: string[] | null;
  assigned_qualifications: string[] | null;
  work_rule_tags: string[] | null;
  attributes: Record<string, unknown> | null;
  meta: Record<string, unknown> | null;
}

export interface StageData {
  stageId: StageId;
  timelineRange?: TimelineRange;
  version?: string | null;
  resources: Resource[];
  activities: Activity[];
}

export interface MasterDataSets {
  personnelServicePools: PersonnelServicePool[];
  personnelPools: PersonnelPool[];
  vehicleServicePools: VehicleServicePool[];
  vehiclePools: VehiclePool[];
  vehicleTypes: VehicleType[];
  vehicleCompositions: VehicleComposition[];
}

interface PersonnelServicePoolRow {
  id: string;
  name: string;
  description: string | null;
  service_ids: string[];
  shift_coordinator: string | null;
  contact_email: string | null;
  attributes: Record<string, unknown> | null;
}

interface PersonnelPoolRow {
  id: string;
  name: string;
  description: string | null;
  personnel_ids: string[];
  location_code: string | null;
  attributes: Record<string, unknown> | null;
}

interface VehicleServicePoolRow {
  id: string;
  name: string;
  description: string | null;
  service_ids: string[];
  dispatcher: string | null;
  attributes: Record<string, unknown> | null;
}

interface VehiclePoolRow {
  id: string;
  name: string;
  description: string | null;
  vehicle_ids: string[];
  depot_manager: string | null;
  attributes: Record<string, unknown> | null;
}

interface VehicleTypeRow {
  id: string;
  label: string;
  category: string | null;
  capacity: number | null;
  max_speed: number | null;
  maintenance_interval_days: number | null;
  energy_type: string | null;
  manufacturer: string | null;
  train_type_code: string | null;
  length_meters: string | null;
  weight_tons: string | null;
  brake_type: string | null;
  brake_percentage: number | null;
  tilting_capability: string | null;
  power_supply_systems: string[] | null;
  train_protection_systems: string[] | null;
  etcs_level: string | null;
  gauge_profile: string | null;
  max_axle_load: string | null;
  noise_category: string | null;
  remarks: string | null;
  attributes: Record<string, unknown> | null;
}

interface VehicleCompositionRow {
  id: string;
  name: string;
  turnaround_buffer: string | null;
  remark: string | null;
  attributes: Record<string, unknown> | null;
}

interface VehicleCompositionEntryRow {
  composition_id: string;
  type_id: string;
  quantity: number;
}

@Injectable()
export class PlanningRepository {
  private readonly logger = new Logger(PlanningRepository.name);

  constructor(private readonly database: DatabaseService) {}

  get isEnabled(): boolean {
    return this.database.enabled;
  }

  async loadStageData(stageId: StageId): Promise<StageData | null> {
    if (!this.isEnabled) {
      return null;
    }

    const stageResult = await this.database.query<StageRow>(
      `
        SELECT stage_id, version, timeline_start, timeline_end
        FROM planning_stage
        WHERE stage_id = $1
      `,
      [stageId],
    );
    const stageRow = stageResult.rows[0];
    if (!stageRow) {
      return null;
    }

    const resourcesResult = await this.database.query<ResourceRow>(
      `
        SELECT id, name, kind, daily_service_capacity, attributes
        FROM planning_resource
        WHERE stage_id = $1
        ORDER BY name
      `,
      [stageId],
    );

    const activitiesResult = await this.database.query<ActivityRow>(
      `
        SELECT
          id,
          resource_id,
          participant_resource_ids,
          client_id,
          title,
          start,
          "end",
          type,
          "from",
          "to",
          remark,
          service_id,
          service_template_id,
          service_date,
          service_category,
          service_role,
          location_id,
          location_label,
          capacity_group_id,
          required_qualifications,
          assigned_qualifications,
          work_rule_tags,
          attributes,
          meta
        FROM planning_activity
        WHERE stage_id = $1
        ORDER BY start
      `,
      [stageId],
    );

    return {
      stageId,
      timelineRange: {
        start: this.toIso(stageRow.timeline_start),
        end: this.toIso(stageRow.timeline_end),
      },
      version: stageRow.version ? this.toIso(stageRow.version) : null,
      resources: resourcesResult.rows.map((row) => this.mapResource(row)),
      activities: activitiesResult.rows.map((row) => this.mapActivity(row)),
    };
  }

  async updateStageMetadata(
    stageId: StageId,
    timeline: TimelineRange,
    version?: string | null,
  ): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    const resolvedVersion = version ?? new Date().toISOString();
    await this.database.query(
      `
        INSERT INTO planning_stage (stage_id, version, timeline_start, timeline_end)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (stage_id) DO UPDATE
        SET version = EXCLUDED.version,
            timeline_start = EXCLUDED.timeline_start,
            timeline_end = EXCLUDED.timeline_end
      `,
      [stageId, resolvedVersion, timeline.start, timeline.end],
    );
  }

  async applyResourceMutations(
    stageId: StageId,
    upserts: Resource[],
    deleteIds: string[],
  ): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    if (!upserts.length && !deleteIds.length) {
      return;
    }

    await this.database.withClient(async (client) => {
      await client.query('BEGIN');
      try {
        if (upserts.length) {
          await client.query(
            `
              WITH payload AS (
                SELECT *
                FROM jsonb_to_recordset($2::jsonb)
                     AS r(
                       id TEXT,
                       name TEXT,
                       kind TEXT,
                       "dailyServiceCapacity" INTEGER,
                       attributes JSONB
                     )
              )
              INSERT INTO planning_resource (
                id, stage_id, name, kind, daily_service_capacity, attributes
              )
              SELECT
                id,
                $1,
                name,
                kind,
                "dailyServiceCapacity",
                attributes
              FROM payload
              ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                daily_service_capacity = EXCLUDED.daily_service_capacity,
                attributes = EXCLUDED.attributes,
                updated_at = now()
            `,
            [stageId, JSON.stringify(upserts)],
          );
        }

        if (deleteIds.length) {
          await client.query(
            `
              DELETE FROM planning_resource
              WHERE stage_id = $1
                AND id = ANY($2::text[])
            `,
            [stageId, deleteIds],
          );
        }

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        this.logger.error(
          `Fehler beim Speichern der Ressourcen für Stage ${stageId}`,
          (error as Error).stack ?? String(error),
        );
        throw error;
      }
    });
  }

  async applyActivityMutations(
    stageId: StageId,
    upserts: Activity[],
    deleteIds: string[],
  ): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    if (!upserts.length && !deleteIds.length) {
      return;
    }

    await this.database.withClient(async (client) => {
      await client.query('BEGIN');
      try {
        if (upserts.length) {
          await client.query(
            `
              WITH payload AS (
                SELECT *
                FROM jsonb_to_recordset($2::jsonb)
                     AS a(
                       id TEXT,
                       "resourceId" TEXT,
                       "participantResourceIds" TEXT[],
                       "clientId" TEXT,
                       title TEXT,
                       start TIMESTAMPTZ,
                       "end" TIMESTAMPTZ,
                       type TEXT,
                       "from" TEXT,
                       "to" TEXT,
                       remark TEXT,
                       "serviceId" TEXT,
                       "serviceTemplateId" TEXT,
                       "serviceDate" DATE,
                       "serviceCategory" TEXT,
                       "serviceRole" TEXT,
                       "locationId" TEXT,
                       "locationLabel" TEXT,
                       "capacityGroupId" TEXT,
                       "requiredQualifications" TEXT[],
                       "assignedQualifications" TEXT[],
                       "workRuleTags" TEXT[],
                       attributes JSONB,
                       meta JSONB
                     )
              )
              INSERT INTO planning_activity (
                id,
                stage_id,
                resource_id,
                participant_resource_ids,
                client_id,
                title,
                start,
                "end",
                type,
                "from",
                "to",
                remark,
                service_id,
                service_template_id,
                service_date,
                service_category,
                service_role,
                location_id,
                location_label,
                capacity_group_id,
                required_qualifications,
                assigned_qualifications,
                work_rule_tags,
                attributes,
                meta
              )
              SELECT
                id,
                $1,
                "resourceId",
                "participantResourceIds",
                "clientId",
                title,
                start,
                "end",
                type,
                "from",
                "to",
                remark,
                "serviceId",
                "serviceTemplateId",
                "serviceDate",
                "serviceCategory",
                "serviceRole",
                "locationId",
                "locationLabel",
                "capacityGroupId",
                "requiredQualifications",
                "assignedQualifications",
                "workRuleTags",
                attributes,
                meta
              FROM payload
              ON CONFLICT (id) DO UPDATE SET
                resource_id = EXCLUDED.resource_id,
                participant_resource_ids = EXCLUDED.participant_resource_ids,
                client_id = EXCLUDED.client_id,
                title = EXCLUDED.title,
                start = EXCLUDED.start,
                "end" = EXCLUDED."end",
                type = EXCLUDED.type,
                "from" = EXCLUDED."from",
                "to" = EXCLUDED."to",
                remark = EXCLUDED.remark,
                service_id = EXCLUDED.service_id,
                service_template_id = EXCLUDED.service_template_id,
                service_date = EXCLUDED.service_date,
                service_category = EXCLUDED.service_category,
                service_role = EXCLUDED.service_role,
                location_id = EXCLUDED.location_id,
                location_label = EXCLUDED.location_label,
                capacity_group_id = EXCLUDED.capacity_group_id,
                required_qualifications = EXCLUDED.required_qualifications,
                assigned_qualifications = EXCLUDED.assigned_qualifications,
                work_rule_tags = EXCLUDED.work_rule_tags,
                attributes = EXCLUDED.attributes,
                meta = EXCLUDED.meta,
                updated_at = now()
            `,
            [stageId, JSON.stringify(upserts)],
          );
        }

        if (deleteIds.length) {
          await client.query(
            `
              DELETE FROM planning_activity
              WHERE stage_id = $1
                AND id = ANY($2::text[])
            `,
            [stageId, deleteIds],
          );
        }

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        this.logger.error(
          `Fehler beim Speichern der Aktivitäten für Stage ${stageId}`,
          (error as Error).stack ?? String(error),
        );
        throw error;
      }
    });
  }

  async deleteActivities(stageId: StageId, deleteIds: string[]): Promise<void> {
    if (!this.isEnabled || !deleteIds.length) {
      return;
    }
    await this.database.query(
      `
        DELETE FROM planning_activity
        WHERE stage_id = $1
          AND id = ANY($2::text[])
      `,
      [stageId, deleteIds],
    );
  }

  async loadMasterData(): Promise<MasterDataSets> {
    if (!this.isEnabled) {
      return this.createEmptyMasterData();
    }

    const [
      personnelServicePools,
      personnelPools,
      vehicleServicePools,
      vehiclePools,
      vehicleTypes,
      vehicleCompositions,
    ] = await Promise.all([
      this.database
        .query<PersonnelServicePoolRow>(
          `
            SELECT id, name, description, service_ids, shift_coordinator, contact_email, attributes
            FROM personnel_service_pool
            ORDER BY name
          `,
        )
        .then((result) =>
          result.rows.map((row) => this.mapPersonnelServicePool(row)),
        ),
      this.database
        .query<PersonnelPoolRow>(
          `
            SELECT id, name, description, personnel_ids, location_code, attributes
            FROM personnel_pool
            ORDER BY name
          `,
        )
        .then((result) => result.rows.map((row) => this.mapPersonnelPool(row))),
      this.database
        .query<VehicleServicePoolRow>(
          `
            SELECT id, name, description, service_ids, dispatcher, attributes
            FROM vehicle_service_pool
            ORDER BY name
          `,
        )
        .then((result) =>
          result.rows.map((row) => this.mapVehicleServicePool(row)),
        ),
      this.database
        .query<VehiclePoolRow>(
          `
            SELECT id, name, description, vehicle_ids, depot_manager, attributes
            FROM vehicle_pool
            ORDER BY name
          `,
        )
        .then((result) => result.rows.map((row) => this.mapVehiclePool(row))),
      this.database
        .query<VehicleTypeRow>(
          `
            SELECT
              id,
              label,
              category,
              capacity,
              max_speed,
              maintenance_interval_days,
              energy_type,
              manufacturer,
              train_type_code,
              length_meters,
              weight_tons,
              brake_type,
              brake_percentage,
              tilting_capability,
              power_supply_systems,
              train_protection_systems,
              etcs_level,
              gauge_profile,
              max_axle_load,
              noise_category,
              remarks,
              attributes
            FROM vehicle_type
            ORDER BY label
          `,
        )
        .then((result) => result.rows.map((row) => this.mapVehicleType(row))),
      this.loadVehicleCompositions(),
    ]);

    return {
      personnelServicePools,
      personnelPools,
      vehicleServicePools,
      vehiclePools,
      vehicleTypes,
      vehicleCompositions,
    };
  }

  async replacePersonnelServicePools(items: PersonnelServicePool[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.replaceCollection(
      'personnel_service_pool',
      items,
      `
        WITH incoming AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS t(
            id TEXT,
            name TEXT,
            description TEXT,
            "serviceIds" TEXT[],
            "shiftCoordinator" TEXT,
            "contactEmail" TEXT,
            attributes JSONB
          )
        )
        INSERT INTO personnel_service_pool (
          id,
          name,
          description,
          service_ids,
          shift_coordinator,
          contact_email,
          attributes
        )
        SELECT
          id,
          name,
          description,
          "serviceIds",
          "shiftCoordinator",
          "contactEmail",
          attributes
        FROM incoming
      `,
    );
  }

  async replacePersonnelPools(items: PersonnelPool[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.replaceCollection(
      'personnel_pool',
      items,
      `
        WITH incoming AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS t(
            id TEXT,
            name TEXT,
            description TEXT,
            "personnelIds" TEXT[],
            "locationCode" TEXT,
            attributes JSONB
          )
        )
        INSERT INTO personnel_pool (
          id,
          name,
          description,
          personnel_ids,
          location_code,
          attributes
        )
        SELECT
          id,
          name,
          description,
          "personnelIds",
          "locationCode",
          attributes
        FROM incoming
      `,
    );
  }

  async replaceVehicleServicePools(items: VehicleServicePool[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.replaceCollection(
      'vehicle_service_pool',
      items,
      `
        WITH incoming AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS t(
            id TEXT,
            name TEXT,
            description TEXT,
            "serviceIds" TEXT[],
            dispatcher TEXT,
            attributes JSONB
          )
        )
        INSERT INTO vehicle_service_pool (
          id,
          name,
          description,
          service_ids,
          dispatcher,
          attributes
        )
        SELECT
          id,
          name,
          description,
          "serviceIds",
          dispatcher,
          attributes
        FROM incoming
      `,
    );
  }

  async replaceVehiclePools(items: VehiclePool[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.replaceCollection(
      'vehicle_pool',
      items,
      `
        WITH incoming AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS t(
            id TEXT,
            name TEXT,
            description TEXT,
            "vehicleIds" TEXT[],
            "depotManager" TEXT,
            attributes JSONB
          )
        )
        INSERT INTO vehicle_pool (
          id,
          name,
          description,
          vehicle_ids,
          depot_manager,
          attributes
        )
        SELECT
          id,
          name,
          description,
          "vehicleIds",
          "depotManager",
          attributes
        FROM incoming
      `,
    );
  }

  async replaceVehicleTypes(items: VehicleType[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.replaceCollection(
      'vehicle_type',
      items,
      `
        WITH incoming AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS t(
            id TEXT,
            label TEXT,
            category TEXT,
            capacity INTEGER,
            "maxSpeed" INTEGER,
            "maintenanceIntervalDays" INTEGER,
            "energyType" TEXT,
            manufacturer TEXT,
            "trainTypeCode" TEXT,
            "lengthMeters" NUMERIC,
            "weightTons" NUMERIC,
            "brakeType" TEXT,
            "brakePercentage" INTEGER,
            "tiltingCapability" TEXT,
            "powerSupplySystems" TEXT[],
            "trainProtectionSystems" TEXT[],
            "etcsLevel" TEXT,
            "gaugeProfile" TEXT,
            "maxAxleLoad" NUMERIC,
            "noiseCategory" TEXT,
            remarks TEXT,
            attributes JSONB
          )
        )
        INSERT INTO vehicle_type (
          id,
          label,
          category,
          capacity,
          max_speed,
          maintenance_interval_days,
          energy_type,
          manufacturer,
          train_type_code,
          length_meters,
          weight_tons,
          brake_type,
          brake_percentage,
          tilting_capability,
          power_supply_systems,
          train_protection_systems,
          etcs_level,
          gauge_profile,
          max_axle_load,
          noise_category,
          remarks,
          attributes
        )
        SELECT
          id,
          label,
          category,
          capacity,
          "maxSpeed",
          "maintenanceIntervalDays",
          "energyType",
          manufacturer,
          "trainTypeCode",
          "lengthMeters",
          "weightTons",
          "brakeType",
          "brakePercentage",
          "tiltingCapability",
          "powerSupplySystems",
          "trainProtectionSystems",
          "etcsLevel",
          "gaugeProfile",
          "maxAxleLoad",
          "noiseCategory",
          remarks,
          attributes
        FROM incoming
      `,
    );
  }

  async replaceVehicleCompositions(items: VehicleComposition[]): Promise<void> {
    if (!this.isEnabled) {
      return;
    }
    await this.database.withClient(async (client) => {
      await client.query('BEGIN');
      try {
        await client.query('DELETE FROM vehicle_composition_entry');
        await client.query('DELETE FROM vehicle_composition');
        if (items.length) {
          await client.query(
            `
              WITH incoming AS (
                SELECT *
                FROM jsonb_to_recordset($1::jsonb) AS t(
                  id TEXT,
                  name TEXT,
                  "turnaroundBuffer" TEXT,
                  remark TEXT,
                  attributes JSONB
                )
              )
              INSERT INTO vehicle_composition (
                id,
                name,
                turnaround_buffer,
                remark,
                attributes
              )
              SELECT
                id,
                name,
                NULLIF("turnaroundBuffer", '')::interval,
                remark,
                attributes
              FROM incoming
            `,
            [JSON.stringify(items)],
          );

          const entries = this.flattenVehicleCompositionEntries(items);
          if (entries.length) {
            await client.query(
              `
                WITH incoming AS (
                  SELECT *
                  FROM jsonb_to_recordset($1::jsonb) AS t(
                    "compositionId" TEXT,
                    "typeId" TEXT,
                    quantity INTEGER
                  )
                )
                INSERT INTO vehicle_composition_entry (
                  composition_id,
                  type_id,
                  quantity
                )
                SELECT
                  "compositionId",
                  "typeId",
                  quantity
                FROM incoming
              `,
              [JSON.stringify(entries)],
            );
          }
        }
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        this.logger.error(
          'Fehler beim Speichern der Fahrzeugzusammenstellungen',
          (error as Error).stack ?? String(error),
        );
        throw error;
      }
    });
  }

  private mapResource(row: ResourceRow): Resource {
    return {
      id: row.id,
      name: row.name,
      kind: row.kind as Resource['kind'],
      dailyServiceCapacity: row.daily_service_capacity ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private mapActivity(row: ActivityRow): Activity {
    return {
      id: row.id,
      resourceId: row.resource_id,
      participantResourceIds: row.participant_resource_ids ?? undefined,
      clientId: row.client_id ?? undefined,
      title: row.title,
      start: this.toIso(row.start),
      end: row.end ? this.toIso(row.end) : undefined,
      type: row.type ?? undefined,
      from: row.from ?? undefined,
      to: row.to ?? undefined,
      remark: row.remark ?? undefined,
      serviceId: row.service_id ?? undefined,
      serviceTemplateId: row.service_template_id ?? undefined,
      serviceDate: this.toDateString(row.service_date),
      serviceCategory: row.service_category ?? undefined,
      serviceRole: row.service_role ?? undefined,
      locationId: row.location_id ?? undefined,
      locationLabel: row.location_label ?? undefined,
      capacityGroupId: row.capacity_group_id ?? undefined,
      requiredQualifications: row.required_qualifications ?? undefined,
      assignedQualifications: row.assigned_qualifications ?? undefined,
      workRuleTags: row.work_rule_tags ?? undefined,
      attributes: row.attributes ?? undefined,
      meta: row.meta ?? undefined,
    };
  }

  private toIso(value: string | Date): string {
    return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
  }

  private toDateString(value: string | Date | null): string | undefined {
    if (!value) {
      return undefined;
    }
    if (value instanceof Date) {
      return value.toISOString().substring(0, 10);
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
      return value;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime())
      ? undefined
      : parsed.toISOString().substring(0, 10);
  }

  private mapPersonnelServicePool(row: PersonnelServicePoolRow): PersonnelServicePool {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      serviceIds: [...(row.service_ids ?? [])],
      shiftCoordinator: row.shift_coordinator ?? undefined,
      contactEmail: row.contact_email ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private mapPersonnelPool(row: PersonnelPoolRow): PersonnelPool {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      personnelIds: [...(row.personnel_ids ?? [])],
      locationCode: row.location_code ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private mapVehicleServicePool(row: VehicleServicePoolRow): VehicleServicePool {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      serviceIds: [...(row.service_ids ?? [])],
      dispatcher: row.dispatcher ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private mapVehiclePool(row: VehiclePoolRow): VehiclePool {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      vehicleIds: [...(row.vehicle_ids ?? [])],
      depotManager: row.depot_manager ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private mapVehicleType(row: VehicleTypeRow): VehicleType {
    return {
      id: row.id,
      label: row.label,
      category: row.category ?? undefined,
      capacity: row.capacity ?? undefined,
      maxSpeed: row.max_speed ?? undefined,
      maintenanceIntervalDays: row.maintenance_interval_days ?? undefined,
      energyType: row.energy_type ?? undefined,
      manufacturer: row.manufacturer ?? undefined,
      trainTypeCode: row.train_type_code ?? undefined,
      lengthMeters: this.toNumber(row.length_meters),
      weightTons: this.toNumber(row.weight_tons),
      brakeType: row.brake_type ?? undefined,
      brakePercentage: row.brake_percentage ?? undefined,
      tiltingCapability: (row.tilting_capability as VehicleType['tiltingCapability']) ?? undefined,
      powerSupplySystems: row.power_supply_systems ?? undefined,
      trainProtectionSystems: row.train_protection_systems ?? undefined,
      etcsLevel: row.etcs_level ?? undefined,
      gaugeProfile: row.gauge_profile ?? undefined,
      maxAxleLoad: this.toNumber(row.max_axle_load),
      noiseCategory: row.noise_category ?? undefined,
      remarks: row.remarks ?? undefined,
      attributes: row.attributes ?? undefined,
    };
  }

  private async loadVehicleCompositions(): Promise<VehicleComposition[]> {
    if (!this.isEnabled) {
      return [];
    }
    const [compositionResult, entryResult] = await Promise.all([
      this.database.query<VehicleCompositionRow>(
        `
          SELECT id, name, turnaround_buffer, remark, attributes
          FROM vehicle_composition
          ORDER BY name
        `,
      ),
      this.database.query<VehicleCompositionEntryRow>(
        `
          SELECT composition_id, type_id, quantity
          FROM vehicle_composition_entry
          ORDER BY composition_id, type_id
        `,
      ),
    ]);

    return this.mapVehicleCompositions(
      compositionResult.rows,
      entryResult.rows,
    );
  }

  private mapVehicleCompositions(
    rows: VehicleCompositionRow[],
    entryRows: VehicleCompositionEntryRow[],
  ): VehicleComposition[] {
    const entriesByComposition = new Map<string, VehicleCompositionEntryRow[]>();
    entryRows.forEach((entry) => {
      const list = entriesByComposition.get(entry.composition_id) ?? [];
      list.push(entry);
      entriesByComposition.set(entry.composition_id, list);
    });

    return rows.map((row) => ({
      id: row.id,
      name: row.name,
      entries: (entriesByComposition.get(row.id) ?? []).map((entry) => ({
        typeId: entry.type_id,
        quantity: entry.quantity,
      })),
      turnaroundBuffer: row.turnaround_buffer ?? undefined,
      remark: row.remark ?? undefined,
      attributes: row.attributes ?? undefined,
    }));
  }

  private flattenVehicleCompositionEntries(
    items: VehicleComposition[],
  ): { compositionId: string; typeId: string; quantity: number }[] {
    const payload: { compositionId: string; typeId: string; quantity: number }[] = [];
    items.forEach((composition) => {
      (composition.entries ?? []).forEach((entry) => {
        payload.push({
          compositionId: composition.id,
          typeId: entry.typeId,
          quantity: entry.quantity,
        });
      });
    });
    return payload;
  }

  private async replaceCollection(
    tableName: string,
    items: unknown[],
    insertSql: string,
  ): Promise<void> {
    await this.database.withClient(async (client) => {
      await client.query('BEGIN');
      try {
        await client.query(`DELETE FROM ${tableName}`);
        if (items.length) {
          await client.query(insertSql, [JSON.stringify(items)]);
        }
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        this.logger.error(
          `Fehler beim Aktualisieren von ${tableName}`,
          (error as Error).stack ?? String(error),
        );
        throw error;
      }
    });
  }

  private toNumber(value: string | number | null): number | undefined {
    if (value === null || value === undefined) {
      return undefined;
    }
    const parsed = typeof value === 'number' ? value : Number(value);
    return Number.isNaN(parsed) ? undefined : parsed;
  }

  private createEmptyMasterData(): MasterDataSets {
    return {
      personnelServicePools: [],
      personnelPools: [],
      vehicleServicePools: [],
      vehiclePools: [],
      vehicleTypes: [],
      vehicleCompositions: [],
    };
  }
}
