import { Injectable, NotFoundException } from '@nestjs/common';
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

@Injectable()
export class PlanningService {
  private readonly stages = new Map<StageId, StageState>();
  private validationIssueCounter = 0;

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

    return {
      appliedUpserts,
      deletedIds,
      version: stage.version,
    };
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

  mutateResources(
    stageId: string,
    request?: ResourceMutationRequest,
  ): ResourceMutationResponse {
    const stage = this.getStage(stageId);
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
    if (deletedSet) {
      const originalLength = stage.activities.length;
      // Drop Aktivitäten ohne gültige Ressource, damit der Snapshot konsistent bleibt.
      stage.activities = stage.activities.filter(
        (activity) => !deletedSet.has(activity.resourceId),
      );
      activitiesChanged = stage.activities.length !== originalLength;
    }

    stage.version = this.nextVersion();
    if (activitiesChanged) {
      stage.timelineRange = this.computeTimelineRange(
        stage.activities,
        stage.timelineRange,
      );
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
      meta: activity.meta ? { ...activity.meta } : undefined,
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
