import {
  Injectable,
  Logger,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
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
  TrainRun,
  TrainSegment,
  OperationalPoint,
  OperationalPointListRequest,
  OperationalPointListResponse,
  LatLng,
  SectionOfLine,
  SectionOfLineListRequest,
  SectionOfLineListResponse,
  TopologyAttribute,
  PersonnelSite,
  PersonnelSiteListRequest,
  PersonnelSiteListResponse,
  ReplacementStop,
  ReplacementStopListRequest,
  ReplacementStopListResponse,
  ReplacementRoute,
  ReplacementRouteListRequest,
  ReplacementRouteListResponse,
  ReplacementEdge,
  ReplacementEdgeListRequest,
  ReplacementEdgeListResponse,
  OpReplacementStopLink,
  OpReplacementStopLinkListRequest,
  OpReplacementStopLinkListResponse,
  TransferNode,
  TransferEdge,
  TransferEdgeListRequest,
  TransferEdgeListResponse,
  TopologyImportRequest,
  TopologyImportResponse,
  TopologyImportKind,
  TopologyImportEventRequest,
  TopologyImportRealtimeEvent,
} from './planning.types';
import { PlanningRepository } from './planning.repository';

interface StageState {
  stageId: StageId;
  resources: Resource[];
  activities: Activity[];
  trainRuns: TrainRun[];
  trainSegments: TrainSegment[];
  timelineRange: TimelineRange;
  version: string | null;
}

interface SourceContext {
  userId?: string;
  connectionId?: string;
}

@Injectable()
export class PlanningService implements OnModuleInit {
  private readonly logger = new Logger(PlanningService.name);
  private readonly stages = new Map<StageId, StageState>();
  private validationIssueCounter = 0;
  private readonly stageEventSubjects = new Map<
    StageId,
    Subject<PlanningStageRealtimeEvent>
  >();
  private readonly heartbeatIntervalMs = 30000;
  private personnelServicePools: PersonnelServicePool[] = [];
  private personnelPools: PersonnelPool[] = [];
  private vehicleServicePools: VehicleServicePool[] = [];
  private vehiclePools: VehiclePool[] = [];
  private vehicleTypes: VehicleType[] = [];
  private vehicleCompositions: VehicleComposition[] = [];
  private operationalPoints: OperationalPoint[] = [];
  private sectionsOfLine: SectionOfLine[] = [];
  private personnelSites: PersonnelSite[] = [];
  private replacementStops: ReplacementStop[] = [];
  private replacementRoutes: ReplacementRoute[] = [];
  private replacementEdges: ReplacementEdge[] = [];
  private opReplacementStopLinks: OpReplacementStopLink[] = [];
  private transferEdges: TransferEdge[] = [];
  private readonly topologyImportEvents = new Subject<TopologyImportRealtimeEvent>();

  private readonly usingDatabase: boolean;

  constructor(private readonly repository: PlanningRepository) {
    this.usingDatabase = this.repository.isEnabled;
    if (!this.usingDatabase) {
      STAGE_IDS.forEach((stageId) => {
        this.stages.set(stageId, this.createEmptyStage(stageId));
      });
    }
  }

  async onModuleInit(): Promise<void> {
    if (!this.usingDatabase) {
      return;
    }
    await this.initializeStagesFromDatabase();
    await this.initializeMasterDataFromDatabase();
  }

  getStageSnapshot(stageId: string): PlanningStageSnapshot {
    const stage = this.getStage(stageId);
    return {
      stageId: stage.stageId,
      resources: stage.resources.map((resource) =>
        this.cloneResource(resource),
      ),
      activities: stage.activities.map((activity) =>
        this.cloneActivity(activity),
      ),
      trainRuns: stage.trainRuns.map((run) => this.cloneTrainRun(run)),
      trainSegments: stage.trainSegments.map((segment) =>
        this.cloneTrainSegment(segment),
      ),
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

  async mutateActivities(
    stageId: string,
    request?: ActivityMutationRequest,
  ): Promise<ActivityMutationResponse> {
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

    const activitySnapshots = appliedUpserts.length
      ? this.collectActivitySnapshots(stage, appliedUpserts)
      : [];

    const sourceContext = this.extractSourceContext(request?.clientRequestId);
    if (appliedUpserts.length || deletedIds.length) {
      this.emitStageEvent(stage.stageId, {
        stageId: stage.stageId,
        scope: 'activities',
        version: stage.version,
        sourceClientId: sourceContext.userId,
        sourceConnectionId: sourceContext.connectionId,
        upserts: activitySnapshots.length ? activitySnapshots : undefined,
        deleteIds: deletedIds.length ? [...deletedIds] : undefined,
      });
    }
    if (timelineChanged) {
      this.emitTimelineEvent(stage, sourceContext);
    }

    if (this.usingDatabase) {
      await this.repository.applyActivityMutations(
        stage.stageId,
        activitySnapshots,
        deletedIds,
      );
      await this.repository.updateStageMetadata(
        stage.stageId,
        stage.timelineRange,
        stage.version,
      );
    }

    return {
      appliedUpserts,
      deletedIds,
      version: stage.version,
    };
  }

  listPersonnelServicePools(): PersonnelServicePoolListResponse {
    return this.personnelServicePools.map((pool) =>
      this.clonePersonnelServicePool(pool),
    );
  }

  async savePersonnelServicePools(
    request?: PersonnelServicePoolListRequest,
  ): Promise<PersonnelServicePoolListResponse> {
    const incoming = request?.items ?? [];
    this.personnelServicePools = incoming.map((pool) =>
      this.clonePersonnelServicePool(pool),
    );
    if (this.usingDatabase) {
      await this.repository.replacePersonnelServicePools(
        this.personnelServicePools,
      );
    }
    return this.listPersonnelServicePools();
  }

  listPersonnelPools(): PersonnelPoolListResponse {
    return this.personnelPools.map((pool) => this.clonePersonnelPool(pool));
  }

  async savePersonnelPools(
    request?: PersonnelPoolListRequest,
  ): Promise<PersonnelPoolListResponse> {
    const incoming = request?.items ?? [];
    this.personnelPools = incoming.map((pool) => this.clonePersonnelPool(pool));
    if (this.usingDatabase) {
      await this.repository.replacePersonnelPools(this.personnelPools);
    }
    return this.listPersonnelPools();
  }

  listVehicleServicePools(): VehicleServicePoolListResponse {
    return this.vehicleServicePools.map((pool) =>
      this.cloneVehicleServicePool(pool),
    );
  }

  async saveVehicleServicePools(
    request?: VehicleServicePoolListRequest,
  ): Promise<VehicleServicePoolListResponse> {
    const incoming = request?.items ?? [];
    this.vehicleServicePools = incoming.map((pool) =>
      this.cloneVehicleServicePool(pool),
    );
    if (this.usingDatabase) {
      await this.repository.replaceVehicleServicePools(
        this.vehicleServicePools,
      );
    }
    return this.listVehicleServicePools();
  }

  listVehiclePools(): VehiclePoolListResponse {
    return this.vehiclePools.map((pool) => this.cloneVehiclePool(pool));
  }

  async saveVehiclePools(
    request?: VehiclePoolListRequest,
  ): Promise<VehiclePoolListResponse> {
    const incoming = request?.items ?? [];
    this.vehiclePools = incoming.map((pool) => this.cloneVehiclePool(pool));
    if (this.usingDatabase) {
      await this.repository.replaceVehiclePools(this.vehiclePools);
    }
    return this.listVehiclePools();
  }

  listVehicleTypes(): VehicleTypeListResponse {
    return this.vehicleTypes.map((type) => this.cloneVehicleType(type));
  }

  async saveVehicleTypes(
    request?: VehicleTypeListRequest,
  ): Promise<VehicleTypeListResponse> {
    const incoming = request?.items ?? [];
    this.vehicleTypes = incoming.map((type) => this.cloneVehicleType(type));
    if (this.usingDatabase) {
      await this.repository.replaceVehicleTypes(this.vehicleTypes);
    }
    return this.listVehicleTypes();
  }

  listVehicleCompositions(): VehicleCompositionListResponse {
    return this.vehicleCompositions.map((composition) =>
      this.cloneVehicleComposition(composition),
    );
  }

  async saveVehicleCompositions(
    request?: VehicleCompositionListRequest,
  ): Promise<VehicleCompositionListResponse> {
    const incoming = request?.items ?? [];
    this.vehicleCompositions = incoming.map((composition) =>
      this.cloneVehicleComposition(composition),
    );
    if (this.usingDatabase) {
      await this.repository.replaceVehicleCompositions(
        this.vehicleCompositions,
      );
    }
    return this.listVehicleCompositions();
  }

  listOperationalPoints(): OperationalPointListResponse {
    return this.operationalPoints.map((point) =>
      this.cloneOperationalPoint(point),
    );
  }

  async saveOperationalPoints(
    request?: OperationalPointListRequest,
  ): Promise<OperationalPointListResponse> {
    const incoming = request?.items ?? [];
    this.operationalPoints = incoming.map((point) =>
      this.cloneOperationalPoint(point),
    );
    if (this.usingDatabase) {
      await this.repository.replaceOperationalPoints(this.operationalPoints);
    }
    return this.listOperationalPoints();
  }

  listSectionsOfLine(): SectionOfLineListResponse {
    return this.sectionsOfLine.map((section) =>
      this.cloneSectionOfLine(section),
    );
  }

  async saveSectionsOfLine(
    request?: SectionOfLineListRequest,
  ): Promise<SectionOfLineListResponse> {
    const incoming = request?.items ?? [];
    this.sectionsOfLine = incoming.map((section) =>
      this.cloneSectionOfLine(section),
    );
    if (this.usingDatabase) {
      await this.repository.replaceSectionsOfLine(this.sectionsOfLine);
    }
    return this.listSectionsOfLine();
  }

  listPersonnelSites(): PersonnelSiteListResponse {
    return this.personnelSites.map((site) => this.clonePersonnelSite(site));
  }

  async savePersonnelSites(
    request?: PersonnelSiteListRequest,
  ): Promise<PersonnelSiteListResponse> {
    const incoming = request?.items ?? [];
    this.personnelSites = incoming.map((site) => this.clonePersonnelSite(site));
    if (this.usingDatabase) {
      await this.repository.replacePersonnelSites(this.personnelSites);
    }
    return this.listPersonnelSites();
  }

  listReplacementStops(): ReplacementStopListResponse {
    return this.replacementStops.map((stop) => this.cloneReplacementStop(stop));
  }

  async saveReplacementStops(
    request?: ReplacementStopListRequest,
  ): Promise<ReplacementStopListResponse> {
    const incoming = request?.items ?? [];
    this.replacementStops = incoming.map((stop) =>
      this.cloneReplacementStop(stop),
    );
    if (this.usingDatabase) {
      await this.repository.replaceReplacementStops(this.replacementStops);
    }
    return this.listReplacementStops();
  }

  listReplacementRoutes(): ReplacementRouteListResponse {
    return this.replacementRoutes.map((route) =>
      this.cloneReplacementRoute(route),
    );
  }

  async saveReplacementRoutes(
    request?: ReplacementRouteListRequest,
  ): Promise<ReplacementRouteListResponse> {
    const incoming = request?.items ?? [];
    this.replacementRoutes = incoming.map((route) =>
      this.cloneReplacementRoute(route),
    );
    if (this.usingDatabase) {
      await this.repository.replaceReplacementRoutes(this.replacementRoutes);
    }
    return this.listReplacementRoutes();
  }

  listReplacementEdges(): ReplacementEdgeListResponse {
    return this.replacementEdges.map((edge) => this.cloneReplacementEdge(edge));
  }

  async saveReplacementEdges(
    request?: ReplacementEdgeListRequest,
  ): Promise<ReplacementEdgeListResponse> {
    const incoming = request?.items ?? [];
    this.replacementEdges = incoming.map((edge) =>
      this.cloneReplacementEdge(edge),
    );
    if (this.usingDatabase) {
      await this.repository.replaceReplacementEdges(this.replacementEdges);
    }
    return this.listReplacementEdges();
  }

  listOpReplacementStopLinks(): OpReplacementStopLinkListResponse {
    return this.opReplacementStopLinks.map((link) =>
      this.cloneOpReplacementStopLink(link),
    );
  }

  async saveOpReplacementStopLinks(
    request?: OpReplacementStopLinkListRequest,
  ): Promise<OpReplacementStopLinkListResponse> {
    const incoming = request?.items ?? [];
    this.opReplacementStopLinks = incoming.map((link) =>
      this.cloneOpReplacementStopLink(link),
    );
    if (this.usingDatabase) {
      await this.repository.replaceOpReplacementStopLinks(
        this.opReplacementStopLinks,
      );
    }
    return this.listOpReplacementStopLinks();
  }

  listTransferEdges(): TransferEdgeListResponse {
    return this.transferEdges.map((edge) => this.cloneTransferEdge(edge));
  }

  async saveTransferEdges(
    request?: TransferEdgeListRequest,
  ): Promise<TransferEdgeListResponse> {
    const incoming = request?.items ?? [];
    this.transferEdges = incoming.map((edge) => this.cloneTransferEdge(edge));
    if (this.usingDatabase) {
      await this.repository.replaceTransferEdges(this.transferEdges);
    }
    return this.listTransferEdges();
  }

  async triggerTopologyImport(
    request?: TopologyImportRequest,
  ): Promise<TopologyImportResponse> {
    const requestedKinds = this.normalizeTopologyKinds(request?.kinds);
    const startedAt = new Date().toISOString();
    requestedKinds.forEach((kind) => {
      this.logger.log(
        `Topologie-Import für ${kind} angefordert – Integration folgt.`,
      );
    });
    if (!requestedKinds.length) {
      this.logger.warn(
        'Topologie-Import wurde ohne gültige Typen angefragt und wird ignoriert.',
      );
      this.publishTopologyImportEvent({
        status: 'ignored',
        kinds: [],
        message: 'Keine gültigen Topologie-Typen übergeben.',
        source: 'backend',
      });
      return {
        startedAt,
        requestedKinds,
        message:
          'Import-Anfrage ignoriert – keine gültigen Typen. Migration oder Konfiguration prüfen.',
      };
    }
    this.publishTopologyImportEvent({
      status: 'queued',
      kinds: requestedKinds,
      message: 'Import angefordert.',
      source: 'backend',
    });
    return {
      startedAt,
      requestedKinds,
      message:
        'Import-Ausführung protokolliert. Python-Brücken folgen in einem späteren Schritt.',
    };
  }

  streamTopologyImportEvents(): Observable<TopologyImportRealtimeEvent> {
    return this.topologyImportEvents.asObservable();
  }

  publishTopologyImportEvent(
    request: TopologyImportEventRequest,
  ): TopologyImportRealtimeEvent {
    const event: TopologyImportRealtimeEvent = {
      timestamp: new Date().toISOString(),
      ...request,
    };
    this.topologyImportEvents.next(event);
    return event;
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

  async mutateResources(
    stageId: string,
    request?: ResourceMutationRequest,
  ): Promise<ResourceMutationResponse> {
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
      stage.activities = stage.activities.filter((activity) => {
        const participants = activity.participants ?? [];
        if (
          participants.length > 0 &&
          participants.every((participant) =>
            deletedSet.has(participant.resourceId),
          )
        ) {
          orphanedActivityIds.push(activity.id);
          return false;
        }
        return true;
      });
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

    const resourceSnapshots = appliedUpserts.length
      ? this.collectResourceSnapshots(stage, appliedUpserts)
      : [];

    const sourceContext = this.extractSourceContext(request?.clientRequestId);
    if (appliedUpserts.length || deletedIds.length) {
      this.emitStageEvent(stage.stageId, {
        stageId: stage.stageId,
        scope: 'resources',
        version: stage.version,
        sourceClientId: sourceContext.userId,
        sourceConnectionId: sourceContext.connectionId,
        upserts: resourceSnapshots.length ? resourceSnapshots : undefined,
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

    if (this.usingDatabase) {
      await this.repository.applyResourceMutations(
        stage.stageId,
        resourceSnapshots,
        deletedIds,
      );
      if (orphanedActivityIds.length) {
        await this.repository.deleteActivities(
          stage.stageId,
          orphanedActivityIds,
        );
      }
      await this.repository.updateStageMetadata(
        stage.stageId,
        stage.timelineRange,
        stage.version,
      );
    }

    return {
      appliedUpserts,
      deletedIds,
      version: stage.version,
    };
  }

  private async initializeStagesFromDatabase(): Promise<void> {
    for (const stageId of STAGE_IDS) {
      await this.loadStageFromDatabase(stageId);
    }
  }

  private async initializeMasterDataFromDatabase(): Promise<void> {
    try {
      const masterData = await this.repository.loadMasterData();
      this.personnelServicePools = masterData.personnelServicePools.map(
        (pool) => this.clonePersonnelServicePool(pool),
      );
      this.personnelPools = masterData.personnelPools.map((pool) =>
        this.clonePersonnelPool(pool),
      );
      this.vehicleServicePools = masterData.vehicleServicePools.map((pool) =>
        this.cloneVehicleServicePool(pool),
      );
      this.vehiclePools = masterData.vehiclePools.map((pool) =>
        this.cloneVehiclePool(pool),
      );
      this.vehicleTypes = masterData.vehicleTypes.map((type) =>
        this.cloneVehicleType(type),
      );
      this.vehicleCompositions = masterData.vehicleCompositions.map(
        (composition) => this.cloneVehicleComposition(composition),
      );
      this.operationalPoints = masterData.operationalPoints.map((point) =>
        this.cloneOperationalPoint(point),
      );
      this.sectionsOfLine = masterData.sectionsOfLine.map((section) =>
        this.cloneSectionOfLine(section),
      );
      this.personnelSites = masterData.personnelSites.map((site) =>
        this.clonePersonnelSite(site),
      );
      this.replacementStops = masterData.replacementStops.map((stop) =>
        this.cloneReplacementStop(stop),
      );
      this.replacementRoutes = masterData.replacementRoutes.map((route) =>
        this.cloneReplacementRoute(route),
      );
      this.replacementEdges = masterData.replacementEdges.map((edge) =>
        this.cloneReplacementEdge(edge),
      );
      this.opReplacementStopLinks = masterData.opReplacementStopLinks.map(
        (link) => this.cloneOpReplacementStopLink(link),
      );
      this.transferEdges = masterData.transferEdges.map((edge) =>
        this.cloneTransferEdge(edge),
      );
    } catch (error) {
      this.logger.error(
        'Stammdaten konnten nicht aus der Datenbank geladen werden – verwende leere Sammlungen.',
        (error as Error).stack ?? String(error),
      );
      this.personnelServicePools = [];
      this.personnelPools = [];
      this.vehicleServicePools = [];
      this.vehiclePools = [];
      this.vehicleTypes = [];
      this.vehicleCompositions = [];
      this.operationalPoints = [];
      this.sectionsOfLine = [];
      this.personnelSites = [];
      this.replacementStops = [];
      this.replacementRoutes = [];
      this.replacementEdges = [];
      this.opReplacementStopLinks = [];
      this.transferEdges = [];
    }
  }

  private async loadStageFromDatabase(stageId: StageId): Promise<void> {
    try {
      const data = await this.repository.loadStageData(stageId);
      if (!data) {
        const emptyStage = this.createEmptyStage(stageId);
        this.stages.set(stageId, emptyStage);
        await this.repository.updateStageMetadata(
          stageId,
          emptyStage.timelineRange,
          emptyStage.version,
        );
        return;
      }

      const timelineRange = this.computeTimelineRange(
        data.activities,
        data.timelineRange ?? this.defaultTimelineRange(),
      );
      const version = data.version ?? this.nextVersion();
      const stage: StageState = {
        stageId,
        resources: data.resources.map((resource) =>
          this.cloneResource(resource),
        ),
        activities: data.activities.map((activity) =>
          this.cloneActivity(activity),
        ),
        trainRuns: data.trainRuns.map((run) => this.cloneTrainRun(run)),
        trainSegments: data.trainSegments.map((segment) =>
          this.cloneTrainSegment(segment),
        ),
        timelineRange,
        version,
      };
      this.stages.set(stageId, stage);

      if (
        !data.timelineRange ||
        data.timelineRange.start !== timelineRange.start ||
        data.timelineRange.end !== timelineRange.end ||
        data.version !== version
      ) {
        await this.repository.updateStageMetadata(
          stageId,
          timelineRange,
          version,
        );
      }
    } catch (error) {
      this.logger.error(
        `Stage ${stageId} konnte nicht aus der Datenbank geladen werden – verwende eine leere Stage.`,
        (error as Error).stack ?? String(error),
      );
      this.stages.set(stageId, this.createEmptyStage(stageId));
    }
  }

  private createEmptyStage(stageId: StageId): StageState {
    return {
      stageId,
      resources: [],
      activities: [],
      trainRuns: [],
      trainSegments: [],
      timelineRange: this.defaultTimelineRange(),
      version: this.nextVersion(),
    };
  }

  private cloneResource(resource: Resource): Resource {
    return {
      ...resource,
      attributes: resource.attributes ? { ...resource.attributes } : undefined,
    };
  }

  private cloneActivity(activity: Activity): Activity {
    return {
      ...activity,
      requiredQualifications: activity.requiredQualifications
        ? [...activity.requiredQualifications]
        : undefined,
      assignedQualifications: activity.assignedQualifications
        ? [...activity.assignedQualifications]
        : undefined,
      workRuleTags: activity.workRuleTags
        ? [...activity.workRuleTags]
        : undefined,
      participants: activity.participants
        ? activity.participants.map((participant) => ({ ...participant }))
        : undefined,
      attributes: activity.attributes ? { ...activity.attributes } : undefined,
      meta: activity.meta ? { ...activity.meta } : undefined,
    };
  }

  private cloneTrainRun(run: TrainRun): TrainRun {
    return {
      ...run,
      attributes: run.attributes ? { ...run.attributes } : undefined,
    };
  }

  private cloneTrainSegment(segment: TrainSegment): TrainSegment {
    return {
      ...segment,
      attributes: segment.attributes ? { ...segment.attributes } : undefined,
    };
  }

  private clonePersonnelServicePool(
    pool: PersonnelServicePool,
  ): PersonnelServicePool {
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

  private cloneVehicleServicePool(
    pool: VehicleServicePool,
  ): VehicleServicePool {
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
      attributes: composition.attributes
        ? { ...composition.attributes }
        : undefined,
    };
  }

  private cloneOperationalPoint(point: OperationalPoint): OperationalPoint {
    return {
      ...point,
      position: this.cloneLatLngOptional(point.position),
      attributes: this.cloneTopologyAttributes(point.attributes),
    };
  }

  private cloneSectionOfLine(section: SectionOfLine): SectionOfLine {
    return {
      ...section,
      polyline: section.polyline?.map((entry) => this.cloneLatLng(entry)),
      attributes: this.cloneTopologyAttributes(section.attributes),
    };
  }

  private clonePersonnelSite(site: PersonnelSite): PersonnelSite {
    return {
      ...site,
      position: this.cloneLatLng(site.position),
      attributes: this.cloneTopologyAttributes(site.attributes),
    };
  }

  private cloneReplacementStop(stop: ReplacementStop): ReplacementStop {
    return {
      ...stop,
      position: this.cloneLatLng(stop.position),
      attributes: this.cloneTopologyAttributes(stop.attributes),
    };
  }

  private cloneReplacementRoute(route: ReplacementRoute): ReplacementRoute {
    return {
      ...route,
      attributes: this.cloneTopologyAttributes(route.attributes),
    };
  }

  private cloneReplacementEdge(edge: ReplacementEdge): ReplacementEdge {
    return {
      ...edge,
      polyline: edge.polyline?.map((entry) => this.cloneLatLng(entry)),
      attributes: this.cloneTopologyAttributes(edge.attributes),
    };
  }

  private cloneOpReplacementStopLink(
    link: OpReplacementStopLink,
  ): OpReplacementStopLink {
    return {
      ...link,
      attributes: this.cloneTopologyAttributes(link.attributes),
    };
  }

  private cloneTransferEdge(edge: TransferEdge): TransferEdge {
    return {
      ...edge,
      from: this.cloneTransferNode(edge.from),
      to: this.cloneTransferNode(edge.to),
      attributes: this.cloneTopologyAttributes(edge.attributes),
    };
  }

  private cloneTransferNode(node: TransferNode): TransferNode {
    switch (node.kind) {
      case 'OP':
        return { kind: 'OP', uniqueOpId: node.uniqueOpId };
      case 'PERSONNEL_SITE':
        return { kind: 'PERSONNEL_SITE', siteId: node.siteId };
      case 'REPLACEMENT_STOP':
        return { kind: 'REPLACEMENT_STOP', replacementStopId: node.replacementStopId };
      default: {
        const exhaustive: never = node;
        return exhaustive;
      }
    }
  }

  private cloneLatLng(value: LatLng): LatLng {
    return { lat: value.lat, lng: value.lng };
  }

  private cloneLatLngOptional(value?: LatLng | null): LatLng | undefined {
    return value ? this.cloneLatLng(value) : undefined;
  }

  private cloneTopologyAttributes(
    attributes?: TopologyAttribute[],
  ): TopologyAttribute[] | undefined {
    return attributes?.map((attribute) => ({ ...attribute }));
  }

  private getStageEventSubject(
    stageId: StageId,
  ): Subject<PlanningStageRealtimeEvent> {
    const existing = this.stageEventSubjects.get(stageId);
    if (existing) {
      return existing;
    }
    const subject = new Subject<PlanningStageRealtimeEvent>();
    this.stageEventSubjects.set(stageId, subject);
    return subject;
  }

  private emitStageEvent(
    stageId: StageId,
    event: PlanningStageRealtimeEvent,
  ): void {
    const subject = this.getStageEventSubject(stageId);
    subject.next(event);
  }

  private emitTimelineEvent(
    stage: StageState,
    sourceContext?: SourceContext,
  ): void {
    this.emitStageEvent(
      stage.stageId,
      this.createTimelineEvent(stage, sourceContext),
    );
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

  private collectResourceSnapshots(
    stage: StageState,
    ids: string[],
  ): Resource[] {
    return ids
      .map((id) => stage.resources.find((resource) => resource.id === id))
      .filter((resource): resource is Resource => Boolean(resource))
      .map((resource) => this.cloneResource(resource));
  }

  private collectActivitySnapshots(
    stage: StageState,
    ids: string[],
  ): Activity[] {
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
      throw new NotFoundException(
        `Stage ${stageIdValue} ist nicht initialisiert.`,
      );
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
      if (resourceFilter) {
        const participants = activity.participants ?? [];
        const matchesResource = participants.some((participant) =>
          resourceFilter.has(participant.resourceId),
        );
        if (!matchesResource) {
          return false;
        }
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
    const index = stage.resources.findIndex(
      (resource) => resource.id === incoming.id,
    );
    if (index >= 0) {
      stage.resources[index] = clone;
    } else {
      stage.resources.push(clone);
    }
  }

  private detectOverlapIssues(
    activities: Activity[],
  ): ActivityValidationIssue[] {
    const byResource = new Map<string, Activity[]>();
    activities.forEach((activity) => {
      const participants = activity.participants ?? [];
      participants.forEach((participant) => {
        const collection = byResource.get(participant.resourceId) ?? [];
        collection.push(activity);
        byResource.set(participant.resourceId, collection);
      });
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
    return {
      start: new Date(min).toISOString(),
      end: new Date(max).toISOString(),
    };
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

  private normalizeTopologyKinds(
    kinds?: TopologyImportKind[],
  ): TopologyImportKind[] {
    const allowed: TopologyImportKind[] = [
      'operational-points',
      'sections-of-line',
      'personnel-sites',
      'replacement-stops',
      'replacement-routes',
      'replacement-edges',
      'op-replacement-stop-links',
      'transfer-edges',
    ];
    if (!kinds?.length) {
      return [...allowed];
    }
    const allowedSet = new Set<TopologyImportKind>(allowed);
    const normalized: TopologyImportKind[] = [];
    kinds.forEach((kind) => {
      if (allowedSet.has(kind) && !normalized.includes(kind)) {
        normalized.push(kind);
      }
    });
    return normalized;
  }

  private nextVersion(): string {
    return new Date().toISOString();
  }
}
