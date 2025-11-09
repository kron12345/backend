import { Injectable, NotFoundException } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import {
  Activity,
  ActivityFilters,
  ActivityMutationRequest,
  ActivityMutationResponse,
  ActivityValidationIssue,
  ActivityValidationRequest,
  ActivityValidationResponse,
  PlanningStageSnapshot,
  Resource,
  ResourceMutationRequest,
  ResourceMutationResponse,
  PersonnelServicePool,
  PersonnelServicePoolListRequest,
  PersonnelServicePoolListResponse,
  PersonnelPool,
  PersonnelPoolListRequest,
  PersonnelPoolListResponse,
  VehicleServicePool,
  VehicleServicePoolListRequest,
  VehicleServicePoolListResponse,
  VehiclePool,
  VehiclePoolListRequest,
  VehiclePoolListResponse,
  VehicleType,
  VehicleTypeListRequest,
  VehicleTypeListResponse,
  VehicleComposition,
  VehicleCompositionListRequest,
  VehicleCompositionListResponse,
  PlanningStageRealtimeEvent,
  StageId,
  TimelineRange,
  STAGE_IDS,
  isStageId,
} from './planning.types';

interface StageState {
  stageId: StageId;
  resources: Resource[];
  activities: Activity[];
  timelineRange: TimelineRange;
  version: string | null;
}

interface SourceContext {
  userId?: string;
  connectionId?: string;
}

@Injectable()
export class PlanningService {
  private readonly stages = new Map<StageId, StageState>();
  private validationIssueCounter = 0;
  private readonly stageEventSubjects = new Map<
    StageId,
    Subject<PlanningStageRealtimeEvent>
  >();
  private readonly heartbeatIntervalMs = 30000;
  private personnelServicePools = this.createSeedPersonnelServicePools();
  private personnelPools = this.createSeedPersonnelPools();
  private vehicleServicePools = this.createSeedVehicleServicePools();
  private vehiclePools = this.createSeedVehiclePools();
  private vehicleTypes = this.createSeedVehicleTypes();
  private vehicleCompositions = this.createSeedVehicleCompositions();

  constructor() {
    STAGE_IDS.forEach((stageId) => {
      this.stages.set(stageId, this.createInitialStage(stageId));
    });
  }

  getStageSnapshot(stageId: string): PlanningStageSnapshot {
    const stage = this.getStage(stageId);
    return {
      stageId: stage.stageId,
      resources: stage.resources.map((resource) => this.cloneResource(resource)),
      activities: stage.activities.map((activity) => this.cloneActivity(activity)),
      timelineRange: { ...stage.timelineRange },
      version: stage.version,
    };
  }

  listActivities(stageId: string, filters: ActivityFilters = {}): Activity[] {
    const stage = this.getStage(stageId);
    const filtered = this.applyActivityFilters(stage.activities, filters);
    return filtered.map((activity) => this.cloneActivity(activity));
  }

  listResources(stageId: string): Resource[] {
    const stage = this.getStage(stageId);
    return stage.resources.map((resource) => this.cloneResource(resource));
  }

  mutateActivities(
    stageId: string,
    request?: ActivityMutationRequest,
  ): ActivityMutationResponse {
    const stage = this.getStage(stageId);
    const previousTimeline = { ...stage.timelineRange };
    const upserts = request?.upserts ?? [];
    const deleteIds = new Set(request?.deleteIds ?? []);
    const appliedUpserts: string[] = [];
    const deletedIds: string[] = [];

    upserts.forEach((incoming) => {
      this.upsertActivity(stage, incoming);
      appliedUpserts.push(incoming.id);
      deleteIds.delete(incoming.id);
    });

    if (deleteIds.size > 0) {
      stage.activities = stage.activities.filter((activity) => {
        if (deleteIds.has(activity.id)) {
          deletedIds.push(activity.id);
          return false;
        }
        return true;
      });
    }

    stage.version = this.nextVersion();
    stage.timelineRange = this.computeTimelineRange(
      stage.activities,
      stage.timelineRange,
    );
    const timelineChanged =
      previousTimeline.start !== stage.timelineRange.start ||
      previousTimeline.end !== stage.timelineRange.end;

    const sourceContext = this.extractSourceContext(request?.clientRequestId);
    if (appliedUpserts.length || deletedIds.length) {
      this.emitStageEvent(stage.stageId, {
        stageId: stage.stageId,
        scope: 'activities',
        version: stage.version,
        sourceClientId: sourceContext.userId,
        sourceConnectionId: sourceContext.connectionId,
        upserts: appliedUpserts.length
          ? this.collectActivitySnapshots(stage, appliedUpserts)
          : undefined,
        deleteIds: deletedIds.length ? [...deletedIds] : undefined,
      });
    }
    if (timelineChanged) {
      this.emitTimelineEvent(stage, sourceContext);
    }

    return {
      appliedUpserts,
      deletedIds,
      version: stage.version,
    };
  }

  listPersonnelServicePools(): PersonnelServicePoolListResponse {
    return {
      items: this.personnelServicePools.map((pool) =>
        this.clonePersonnelServicePool(pool),
      ),
    };
  }

  savePersonnelServicePools(
    request?: PersonnelServicePoolListRequest,
  ): PersonnelServicePoolListResponse {
    const incoming = request?.items ?? [];
    this.personnelServicePools = incoming.map((pool) =>
      this.clonePersonnelServicePool(pool),
    );
    return this.listPersonnelServicePools();
  }

  listPersonnelPools(): PersonnelPoolListResponse {
    return {
      items: this.personnelPools.map((pool) => this.clonePersonnelPool(pool)),
    };
  }

  savePersonnelPools(
    request?: PersonnelPoolListRequest,
  ): PersonnelPoolListResponse {
    const incoming = request?.items ?? [];
    this.personnelPools = incoming.map((pool) => this.clonePersonnelPool(pool));
    return this.listPersonnelPools();
  }

  listVehicleServicePools(): VehicleServicePoolListResponse {
    return {
      items: this.vehicleServicePools.map((pool) =>
        this.cloneVehicleServicePool(pool),
      ),
    };
  }

  saveVehicleServicePools(
    request?: VehicleServicePoolListRequest,
  ): VehicleServicePoolListResponse {
    const incoming = request?.items ?? [];
    this.vehicleServicePools = incoming.map((pool) =>
      this.cloneVehicleServicePool(pool),
    );
    return this.listVehicleServicePools();
  }

  listVehiclePools(): VehiclePoolListResponse {
    return {
      items: this.vehiclePools.map((pool) => this.cloneVehiclePool(pool)),
    };
  }

  saveVehiclePools(request?: VehiclePoolListRequest): VehiclePoolListResponse {
    const incoming = request?.items ?? [];
    this.vehiclePools = incoming.map((pool) => this.cloneVehiclePool(pool));
    return this.listVehiclePools();
  }

  listVehicleTypes(): VehicleTypeListResponse {
    return {
      items: this.vehicleTypes.map((type) => this.cloneVehicleType(type)),
    };
  }

  saveVehicleTypes(request?: VehicleTypeListRequest): VehicleTypeListResponse {
    const incoming = request?.items ?? [];
    this.vehicleTypes = incoming.map((type) => this.cloneVehicleType(type));
    return this.listVehicleTypes();
  }

  listVehicleCompositions(): VehicleCompositionListResponse {
    return {
      items: this.vehicleCompositions.map((composition) =>
        this.cloneVehicleComposition(composition),
      ),
    };
  }

  saveVehicleCompositions(
    request?: VehicleCompositionListRequest,
  ): VehicleCompositionListResponse {
    const incoming = request?.items ?? [];
    this.vehicleCompositions = incoming.map((composition) =>
      this.cloneVehicleComposition(composition),
    );
    return this.listVehicleCompositions();
  }

  validateActivities(
    stageId: string,
    request?: ActivityValidationRequest,
  ): ActivityValidationResponse {
    const stage = this.getStage(stageId);
    const filters: ActivityFilters = {
      from: request?.windowStart,
      to: request?.windowEnd,
      resourceIds: request?.resourceIds,
    };

    let selected = this.applyActivityFilters(stage.activities, filters);
    if (request?.activityIds?.length) {
      const ids = new Set(request.activityIds);
      selected = selected.filter((activity) => ids.has(activity.id));
    }

    const issues = this.detectOverlapIssues(selected);

    return {
      generatedAt: this.nextVersion(),
      issues,
    };
  }

  streamStageEvents(
    stageId: string,
    _userId?: string,
    _connectionId?: string,
  ): Observable<PlanningStageRealtimeEvent> {
    const stage = this.getStage(stageId);
    const subject = this.getStageEventSubject(stage.stageId);
    return new Observable<PlanningStageRealtimeEvent>((subscriber) => {
      const subscription = subject.subscribe({
        next: (event) => subscriber.next(event),
        error: (error) => subscriber.error(error),
        complete: () => subscriber.complete(),
      });
      subscriber.next(this.createTimelineEvent(stage));
      const heartbeat = setInterval(() => {
        const currentStage = this.getStage(stage.stageId);
        subscriber.next(this.createTimelineEvent(currentStage));
      }, this.heartbeatIntervalMs);
      return () => {
        clearInterval(heartbeat);
        subscription.unsubscribe();
      };
    });
  }

  mutateResources(
    stageId: string,
    request?: ResourceMutationRequest,
  ): ResourceMutationResponse {
    const stage = this.getStage(stageId);
    const previousTimeline = { ...stage.timelineRange };
    const upserts = request?.upserts ?? [];
    const deleteIds = new Set(request?.deleteIds ?? []);
    const appliedUpserts: string[] = [];
    const deletedIds: string[] = [];

    upserts.forEach((incoming) => {
      this.upsertResource(stage, incoming);
      appliedUpserts.push(incoming.id);
      deleteIds.delete(incoming.id);
    });

    if (deleteIds.size > 0) {
      stage.resources = stage.resources.filter((resource) => {
        if (deleteIds.has(resource.id)) {
          deletedIds.push(resource.id);
          return false;
        }
        return true;
      });
    }

    const deletedSet = deletedIds.length ? new Set(deletedIds) : undefined;
    let activitiesChanged = false;
    const orphanedActivityIds: string[] = [];
    if (deletedSet) {
      const originalLength = stage.activities.length;
      // Drop Aktivitäten ohne gültige Ressource, damit der Snapshot konsistent bleibt.
      stage.activities = stage.activities.filter(
        (activity) => {
          if (deletedSet.has(activity.resourceId)) {
            orphanedActivityIds.push(activity.id);
            return false;
          }
          return true;
        },
      );
      activitiesChanged = stage.activities.length !== originalLength;
    }

    stage.version = this.nextVersion();
    let timelineChanged = false;
    if (activitiesChanged) {
      stage.timelineRange = this.computeTimelineRange(
        stage.activities,
        stage.timelineRange,
      );
      timelineChanged =
        previousTimeline.start !== stage.timelineRange.start ||
        previousTimeline.end !== stage.timelineRange.end;
    }

    const sourceContext = this.extractSourceContext(request?.clientRequestId);
    if (appliedUpserts.length || deletedIds.length) {
      this.emitStageEvent(stage.stageId, {
        stageId: stage.stageId,
        scope: 'resources',
        version: stage.version,
        sourceClientId: sourceContext.userId,
        sourceConnectionId: sourceContext.connectionId,
        upserts: appliedUpserts.length
          ? this.collectResourceSnapshots(stage, appliedUpserts)
          : undefined,
        deleteIds: deletedIds.length ? [...deletedIds] : undefined,
      });
    }
    if (orphanedActivityIds.length) {
      this.emitStageEvent(stage.stageId, {
        stageId: stage.stageId,
        scope: 'activities',
        version: stage.version,
        sourceClientId: sourceContext.userId,
        sourceConnectionId: sourceContext.connectionId,
        deleteIds: [...orphanedActivityIds],
      });
    }
    if (timelineChanged) {
      this.emitTimelineEvent(stage, sourceContext);
    }

    return {
      appliedUpserts,
      deletedIds,
      version: stage.version,
    };
  }

  private createInitialStage(stageId: StageId): StageState {
    const resources = this.createSeedResources();
    const activities = this.createSeedActivities(resources);
    return {
      stageId,
      resources,
      activities,
      timelineRange: this.computeTimelineRange(
        activities,
        this.defaultTimelineRange(),
      ),
      version: this.nextVersion(),
    };
  }

  private createSeedResources(): Resource[] {
    const base: Resource[] = [
      {
        id: 'res-tech-001',
        name: 'Team Alpha',
        kind: 'personnel-service',
        dailyServiceCapacity: 8,
        attributes: { shift: 'early', skills: ['assembly', 'inspection'] },
      },
      {
        id: 'res-tech-002',
        name: 'Team Beta',
        kind: 'personnel-service',
        dailyServiceCapacity: 8,
        attributes: { shift: 'late', skills: ['assembly'] },
      },
      {
        id: 'veh-001',
        name: 'Transporter 1',
        kind: 'vehicle-service',
        attributes: { capacityTons: 2.5 },
      },
    ];
    return base.map((resource) => this.cloneResource(resource));
  }

  private createSeedActivities(resources: Resource[]): Activity[] {
    const firstResource = resources[0]?.id ?? 'res-tech-001';
    const secondResource = resources[1]?.id ?? 'res-tech-002';
    const vehicleResource = resources[2]?.id ?? 'veh-001';
    const seed: Activity[] = [
      {
        id: 'act-1000',
        resourceId: firstResource,
        participantResourceIds: [secondResource],
        title: 'Wareneingang prüfen',
        start: '2025-03-01T07:30:00.000Z',
        end: '2025-03-01T09:00:00.000Z',
        type: 'inspection',
        locationId: 'loc-inbound',
        locationLabel: 'Inbound Dock',
        workRuleTags: ['shift-morning'],
      },
      {
        id: 'act-1001',
        resourceId: firstResource,
        title: 'Kommissionierung Auftrag 4711',
        start: '2025-03-01T09:15:00.000Z',
        end: '2025-03-01T11:00:00.000Z',
        type: 'assembly',
        locationId: 'loc-zone-a',
        workRuleTags: ['shift-morning'],
        requiredQualifications: ['assembly-a'],
      },
      {
        id: 'act-1002',
        resourceId: secondResource,
        title: 'Ausgang vorbereiten',
        start: '2025-03-01T08:00:00.000Z',
        end: '2025-03-01T12:00:00.000Z',
        type: 'fulfillment',
        locationId: 'loc-zone-b',
        workRuleTags: ['shift-day'],
        requiredQualifications: ['forklift'],
      },
      {
        id: 'act-1003',
        resourceId: vehicleResource,
        title: 'Tour Innenstadt',
        start: '2025-03-01T12:30:00.000Z',
        end: '2025-03-01T14:00:00.000Z',
        type: 'transport',
        from: 'Hub Nord',
        to: 'City Depot',
        workRuleTags: ['drive'],
      },
    ];
    return seed.map((activity) => this.cloneActivity(activity));
  }

  private createSeedPersonnelServicePools(): PersonnelServicePool[] {
    const seed: PersonnelServicePool[] = [
      {
        id: 'psp-line-haul',
        name: 'Line Haul Crews',
        description: 'Teams for long-haul and night operations',
        serviceIds: ['svc-line-haul', 'svc-night-run'],
        shiftCoordinator: 'eva.mueller',
        contactEmail: 'linehaul@planning.local',
        attributes: { region: 'north' },
      },
      {
        id: 'psp-yard',
        name: 'Yard Services',
        description: 'Shunting and yard activities',
        serviceIds: ['svc-yard-mgmt'],
        shiftCoordinator: 'markus.schneider',
        attributes: { coverage: '24/7' },
      },
    ];
    return seed.map((pool) => this.clonePersonnelServicePool(pool));
  }

  private createSeedPersonnelPools(): PersonnelPool[] {
    const seed: PersonnelPool[] = [
      {
        id: 'pp-maintenance',
        name: 'Maintenance Crew',
        description: 'Technicians with electrical focus',
        personnelIds: ['per-100', 'per-101', 'per-102'],
        locationCode: 'LOC-MAINT',
        attributes: { skills: ['assembly', 'inspection'] },
      },
      {
        id: 'pp-shift-b',
        name: 'Shift Team B',
        personnelIds: ['per-120', 'per-121'],
        locationCode: 'LOC-ZONE-B',
      },
    ];
    return seed.map((pool) => this.clonePersonnelPool(pool));
  }

  private createSeedVehicleServicePools(): VehicleServicePool[] {
    const seed: VehicleServicePool[] = [
      {
        id: 'vsp-last-mile',
        name: 'Last Mile Deliveries',
        description: 'City and regional deliveries',
        serviceIds: ['svc-last-mile', 'svc-city'],
        dispatcher: 'dispatcher-1',
        attributes: { region: 'city' },
      },
      {
        id: 'vsp-heavy',
        name: 'Heavy Transport',
        serviceIds: ['svc-heavy-haul'],
        dispatcher: 'dispatcher-heavy',
        attributes: { maxPayloadTons: 40 },
      },
    ];
    return seed.map((pool) => this.cloneVehicleServicePool(pool));
  }

  private createSeedVehiclePools(): VehiclePool[] {
    const seed: VehiclePool[] = [
      {
        id: 'vp-urban',
        name: 'Urban Fleet',
        description: 'Vehicles stationed at City Depot',
        vehicleIds: ['veh-001', 'veh-002', 'veh-003'],
        depotManager: 'lena.schmidt',
        attributes: { depot: 'City Depot' },
      },
      {
        id: 'vp-long-haul',
        name: 'Long Haul Fleet',
        vehicleIds: ['veh-010', 'veh-011'],
        depotManager: 'stefan.berger',
        attributes: { depot: 'Hub Nord' },
      },
    ];
    return seed.map((pool) => this.cloneVehiclePool(pool));
  }

  private createSeedVehicleTypes(): VehicleType[] {
    const seed: VehicleType[] = [
      {
        id: 'vt-tractor',
        label: 'Heavy Tractor',
        category: 'heavy',
        capacity: 40,
        maxSpeed: 90,
        maintenanceIntervalDays: 90,
        energyType: 'diesel',
        manufacturer: 'MAN',
        lengthMeters: 7.5,
        weightTons: 18,
        brakeType: 'air',
        brakePercentage: 85,
        tiltingCapability: 'none',
        powerSupplySystems: ['15kv-ac'],
        trainProtectionSystems: ['LZB'],
        etcsLevel: '2',
        gaugeProfile: 'G2',
        maxAxleLoad: 22.5,
        noiseCategory: 'N2',
        remarks: 'Standard tractor for long haul',
        attributes: { emissionClass: 'Euro VI' },
      },
      {
        id: 'vt-van',
        label: 'City Van',
        category: 'light',
        capacity: 2,
        maxSpeed: 120,
        maintenanceIntervalDays: 60,
        energyType: 'electric',
        manufacturer: 'Mercedes',
        lengthMeters: 5.2,
        weightTons: 3.5,
        tiltingCapability: 'none',
        powerSupplySystems: [],
        trainProtectionSystems: [],
        remarks: 'Used for last mile',
        attributes: { rangeKm: 220 },
      },
    ];
    return seed.map((type) => this.cloneVehicleType(type));
  }

  private createSeedVehicleCompositions(): VehicleComposition[] {
    const seed: VehicleComposition[] = [
      {
        id: 'vc-duo',
        name: 'Duo Combination',
        entries: [
          { typeId: 'vt-tractor', quantity: 1 },
          { typeId: 'vt-van', quantity: 1 },
        ],
        turnaroundBuffer: 'PT30M',
        remark: 'Combines heavy tractor with support van',
        attributes: { defaultRoute: 'city-connect' },
      },
      {
        id: 'vc-heavy-haul',
        name: 'Heavy Haul Train',
        entries: [
          { typeId: 'vt-tractor', quantity: 2 },
        ],
        turnaroundBuffer: 'PT1H',
        attributes: { escortRequired: true },
      },
    ];
    return seed.map((composition) => this.cloneVehicleComposition(composition));
  }

  private cloneResource(resource: Resource): Resource {
    return {
      ...resource,
      attributes: resource.attributes
        ? { ...resource.attributes }
        : undefined,
    };
  }

  private cloneActivity(activity: Activity): Activity {
    return {
      ...activity,
      participantResourceIds: activity.participantResourceIds
        ? [...activity.participantResourceIds]
        : undefined,
      requiredQualifications: activity.requiredQualifications
        ? [...activity.requiredQualifications]
        : undefined,
      assignedQualifications: activity.assignedQualifications
        ? [...activity.assignedQualifications]
        : undefined,
      workRuleTags: activity.workRuleTags ? [...activity.workRuleTags] : undefined,
      attributes: activity.attributes ? { ...activity.attributes } : undefined,
      meta: activity.meta ? { ...activity.meta } : undefined,
    };
  }

  private clonePersonnelServicePool(pool: PersonnelServicePool): PersonnelServicePool {
    return {
      ...pool,
      serviceIds: [...(pool.serviceIds ?? [])],
      attributes: pool.attributes ? { ...pool.attributes } : undefined,
    };
  }

  private clonePersonnelPool(pool: PersonnelPool): PersonnelPool {
    return {
      ...pool,
      personnelIds: [...(pool.personnelIds ?? [])],
      attributes: pool.attributes ? { ...pool.attributes } : undefined,
    };
  }

  private cloneVehicleServicePool(pool: VehicleServicePool): VehicleServicePool {
    return {
      ...pool,
      serviceIds: [...(pool.serviceIds ?? [])],
      attributes: pool.attributes ? { ...pool.attributes } : undefined,
    };
  }

  private cloneVehiclePool(pool: VehiclePool): VehiclePool {
    return {
      ...pool,
      vehicleIds: [...(pool.vehicleIds ?? [])],
      attributes: pool.attributes ? { ...pool.attributes } : undefined,
    };
  }

  private cloneVehicleType(type: VehicleType): VehicleType {
    return {
      ...type,
      powerSupplySystems: type.powerSupplySystems
        ? [...type.powerSupplySystems]
        : type.powerSupplySystems,
      trainProtectionSystems: type.trainProtectionSystems
        ? [...type.trainProtectionSystems]
        : type.trainProtectionSystems,
      attributes: type.attributes ? { ...type.attributes } : undefined,
    };
  }

  private cloneVehicleComposition(
    composition: VehicleComposition,
  ): VehicleComposition {
    return {
      ...composition,
      entries: (composition.entries ?? []).map((entry) => ({ ...entry })),
      attributes: composition.attributes ? { ...composition.attributes } : undefined,
    };
  }

  private getStageEventSubject(stageId: StageId): Subject<PlanningStageRealtimeEvent> {
    const existing = this.stageEventSubjects.get(stageId);
    if (existing) {
      return existing;
    }
    const subject = new Subject<PlanningStageRealtimeEvent>();
    this.stageEventSubjects.set(stageId, subject);
    return subject;
  }

  private emitStageEvent(stageId: StageId, event: PlanningStageRealtimeEvent): void {
    const subject = this.getStageEventSubject(stageId);
    subject.next(event);
  }

  private emitTimelineEvent(stage: StageState, sourceContext?: SourceContext): void {
    this.emitStageEvent(stage.stageId, this.createTimelineEvent(stage, sourceContext));
  }

  private createTimelineEvent(
    stage: StageState,
    sourceContext?: SourceContext,
  ): PlanningStageRealtimeEvent {
    return {
      stageId: stage.stageId,
      scope: 'timeline',
      version: stage.version,
      sourceClientId: sourceContext?.userId,
      sourceConnectionId: sourceContext?.connectionId,
      timelineRange: { ...stage.timelineRange },
    };
  }

  private collectResourceSnapshots(stage: StageState, ids: string[]): Resource[] {
    return ids
      .map((id) => stage.resources.find((resource) => resource.id === id))
      .filter((resource): resource is Resource => Boolean(resource))
      .map((resource) => this.cloneResource(resource));
  }

  private collectActivitySnapshots(stage: StageState, ids: string[]): Activity[] {
    return ids
      .map((id) => stage.activities.find((activity) => activity.id === id))
      .filter((activity): activity is Activity => Boolean(activity))
      .map((activity) => this.cloneActivity(activity));
  }

  private extractSourceContext(clientRequestId?: string): SourceContext {
    if (!clientRequestId) {
      return {};
    }
    const segments = clientRequestId.split('|');
    const [userId, connectionId] = segments;
    return {
      userId: userId || undefined,
      connectionId: connectionId || undefined,
    };
  }

  private getStage(stageIdValue: string): StageState {
    if (!isStageId(stageIdValue)) {
      throw new NotFoundException(`Stage ${stageIdValue} ist unbekannt.`);
    }
    const stage = this.stages.get(stageIdValue);
    if (!stage) {
      throw new NotFoundException(`Stage ${stageIdValue} ist nicht initialisiert.`);
    }
    return stage;
  }

  private applyActivityFilters(
    activities: Activity[],
    filters: ActivityFilters = {},
  ): Activity[] {
    const fromMs = this.parseIso(filters.from);
    const toMs = this.parseIso(filters.to);
    const resourceFilter = filters.resourceIds?.length
      ? new Set(filters.resourceIds)
      : undefined;

    return activities.filter((activity) => {
      if (resourceFilter && !resourceFilter.has(activity.resourceId)) {
        return false;
      }

      const startMs = this.parseIso(activity.start);
      const endMs = this.parseIso(activity.end ?? activity.start ?? '');

      if (fromMs !== undefined && endMs !== undefined && endMs <= fromMs) {
        return false;
      }

      if (toMs !== undefined && startMs !== undefined && startMs >= toMs) {
        return false;
      }

      return true;
    });
  }

  private upsertActivity(stage: StageState, incoming: Activity): void {
    const clone = this.cloneActivity(incoming);
    const index = stage.activities.findIndex(
      (activity) => activity.id === incoming.id,
    );
    if (index >= 0) {
      stage.activities[index] = clone;
    } else {
      stage.activities.push(clone);
    }
  }

  private upsertResource(stage: StageState, incoming: Resource): void {
    const clone = this.cloneResource(incoming);
    const index = stage.resources.findIndex((resource) => resource.id === incoming.id);
    if (index >= 0) {
      stage.resources[index] = clone;
    } else {
      stage.resources.push(clone);
    }
  }

  private detectOverlapIssues(activities: Activity[]): ActivityValidationIssue[] {
    const byResource = new Map<string, Activity[]>();
    activities.forEach((activity) => {
      const collection = byResource.get(activity.resourceId) ?? [];
      collection.push(activity);
      byResource.set(activity.resourceId, collection);
    });

    const issues: ActivityValidationIssue[] = [];
    byResource.forEach((list, resourceId) => {
      const sorted = [...list].sort(
        (a, b) => (this.parseIso(a.start) ?? 0) - (this.parseIso(b.start) ?? 0),
      );
      for (let i = 1; i < sorted.length; i += 1) {
        const previous = sorted[i - 1];
        const current = sorted[i];
        if (this.activitiesOverlap(previous, current)) {
          this.validationIssueCounter += 1;
          issues.push({
            id: `working-time-${resourceId}-${this.validationIssueCounter}`,
            rule: 'working-time',
            severity: 'warning',
            message: `Aktivitäten ${previous.id} und ${current.id} überschneiden sich auf Ressource ${resourceId}.`,
            activityIds: [previous.id, current.id],
            meta: { resourceId },
          });
        }
      }
    });
    return issues;
  }

  private activitiesOverlap(a: Activity, b: Activity): boolean {
    const aStart = this.parseIso(a.start) ?? 0;
    const aEnd = this.parseIso(a.end ?? a.start ?? '') ?? aStart;
    const bStart = this.parseIso(b.start) ?? 0;
    const bEnd = this.parseIso(b.end ?? b.start ?? '') ?? bStart;

    return aStart < bEnd && bStart < aEnd;
  }

  private computeTimelineRange(
    activities: Activity[],
    fallback: TimelineRange,
  ): TimelineRange {
    if (!activities.length) {
      return { ...fallback };
    }
    const starts: number[] = [];
    const ends: number[] = [];
    activities.forEach((activity) => {
      const startMs = this.parseIso(activity.start);
      if (startMs !== undefined) {
        starts.push(startMs);
      }
      const endMs = this.parseIso(activity.end ?? activity.start ?? '');
      if (endMs !== undefined) {
        ends.push(endMs);
      }
    });
    if (!starts.length || !ends.length) {
      return { ...fallback };
    }
    const min = Math.min(...starts);
    const max = Math.max(...ends);
    return { start: new Date(min).toISOString(), end: new Date(max).toISOString() };
  }

  private defaultTimelineRange(): TimelineRange {
    return {
      start: '2025-03-01T06:00:00.000Z',
      end: '2025-03-01T18:00:00.000Z',
    };
  }

  private parseIso(value?: string): number | undefined {
    if (!value) {
      return undefined;
    }
    const timestamp = Date.parse(value);
    return Number.isNaN(timestamp) ? undefined : timestamp;
  }

  private nextVersion(): string {
    return new Date().toISOString();
  }
}
