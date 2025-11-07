export type StageId = 'base' | 'operations' | 'dispatch';

export const STAGE_IDS: StageId[] = ['base', 'operations', 'dispatch'];

export function isStageId(value: string): value is StageId {
  return (STAGE_IDS as string[]).includes(value);
}

export interface TimelineRange {
  start: string;
  end: string;
}

export type ResourceKind =
  | 'personnel-service'
  | 'vehicle-service'
  | 'personnel'
  | 'vehicle';

export interface Resource {
  id: string;
  name: string;
  kind: ResourceKind;
  dailyServiceCapacity?: number;
  attributes?: Record<string, unknown>;
}

export interface Activity {
  id: string;
  resourceId: string;
  participantResourceIds?: string[];
  clientId?: string | null;
  title: string;
  start: string;
  end?: string | null;
  type?: string;
  from?: string | null;
  to?: string | null;
  remark?: string | null;
  serviceId?: string | null;
  serviceTemplateId?: string | null;
  serviceDate?: string | null;
  serviceCategory?: string | null;
  serviceRole?: string | null;
  locationId?: string | null;
  locationLabel?: string | null;
  capacityGroupId?: string | null;
  requiredQualifications?: string[];
  assignedQualifications?: string[];
  workRuleTags?: string[];
  meta?: Record<string, unknown>;
}

export interface PlanningStageSnapshot {
  stageId: StageId;
  resources: Resource[];
  activities: Activity[];
  timelineRange: TimelineRange;
  version?: string | null;
}

export interface ActivityMutationRequest {
  upserts?: Activity[];
  deleteIds?: string[];
  clientRequestId?: string;
}

export interface ActivityMutationResponse {
  appliedUpserts: string[];
  deletedIds: string[];
  version?: string | null;
}

export interface ResourceMutationRequest {
  upserts?: Resource[];
  deleteIds?: string[];
  clientRequestId?: string;
}

export interface ResourceMutationResponse {
  appliedUpserts: string[];
  deletedIds: string[];
  version?: string | null;
}

export interface ActivityValidationRequest {
  activityIds?: string[];
  windowStart?: string;
  windowEnd?: string;
  resourceIds?: string[];
  clientRequestId?: string;
}

export type ValidationRule =
  | 'location-conflict'
  | 'capacity-conflict'
  | 'working-time'
  | 'qualification'
  | 'custom';

export type ValidationSeverity = 'info' | 'warning' | 'error';

export interface ActivityValidationIssue {
  id: string;
  rule: ValidationRule;
  severity: ValidationSeverity;
  message: string;
  activityIds: string[];
  meta?: Record<string, unknown>;
}

export interface ActivityValidationResponse {
  generatedAt: string;
  issues: ActivityValidationIssue[];
}

export interface ActivityFilters {
  from?: string;
  to?: string;
  resourceIds?: string[];
}
