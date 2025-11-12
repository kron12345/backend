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

export type PlanWeekResourceKind = 'vehicle-service' | 'personnel-service';

export interface PlanWeekTemplate {
  id: string;
  label: string;
  description?: string | null;
  baseWeekStartIso: string;
  variant?: string | null;
  slices: PlanWeekSlice[];
  createdAtIso: string;
  updatedAtIso: string;
  version: string;
}

export interface PlanWeekSlice {
  id: string;
  templateId: string;
  label?: string | null;
  startIso: string;
  endIso: string;
}

export interface PlanWeekActivity {
  id: string;
  templateId: string;
  resourceId: string;
  title: string;
  startIso: string;
  endIso: string;
  type?: string | null;
  remark?: string | null;
  attributes?: Record<string, unknown>;
}

export interface PlanWeekTemplateListResponse {
  items: PlanWeekTemplate[];
}

export interface PlanWeekActivityListResponse {
  items: PlanWeekActivity[];
}

export type PlanWeekValidityStatus = 'draft' | 'approved' | 'rolled-out';

export interface PlanWeekValidity {
  id: string;
  templateId: string;
  validFromIso: string;
  validToIso: string;
  includeWeekNumbers?: number[];
  excludeWeekNumbers?: number[];
  status: PlanWeekValidityStatus;
}

export interface PlanWeekValidityListResponse {
  items: PlanWeekValidity[];
}

export interface PlanWeekRolloutRequest {
  templateId: string;
  version: string;
  weekStartIso: string;
  weekCount: number;
  skipWeekCodes?: string[];
}

export interface WeekInstanceSummary {
  id: string;
  weekStartIso: string;
  status: WeekInstanceStatus;
}

export interface PlanWeekRolloutResponse {
  createdInstances: WeekInstanceSummary[];
}

export type WeekInstanceStatus = 'planned' | 'released' | 'in-progress' | 'archived';

export interface ScheduledService {
  id: string;
  instanceId: string;
  sliceId: string;
  startIso: string;
  endIso: string;
  attributes?: Record<string, unknown>;
}

export type ServiceAssignmentResourceKind = 'vehicle' | 'personnel';

export interface ServiceAssignment {
  id: string;
  scheduledServiceId: string;
  resourceId: string;
  resourceKind: ServiceAssignmentResourceKind;
  assignedAtIso: string;
  assignedBy?: string | null;
}

export interface WeekInstance {
  id: string;
  templateId: string;
  weekStartIso: string;
  templateVersion: string;
  services: ScheduledService[];
  assignments: ServiceAssignment[];
  status: WeekInstanceStatus;
}

export interface WeekInstanceListResponse {
  items: WeekInstance[];
}

export type PlanWeekRealtimeScope =
  | 'template'
  | 'slice'
  | 'activity'
  | 'validity'
  | 'rollout';

export interface PlanWeekRealtimeEvent {
  scope: PlanWeekRealtimeScope;
  templateId?: string | null;
  upserts?: (
    | PlanWeekTemplate
    | PlanWeekSlice
    | PlanWeekActivity
    | PlanWeekValidity
    | WeekInstanceSummary
  )[];
  deleteIds?: string[];
  version?: string | null;
  sourceClientId?: string | null;
  sourceConnectionId?: string | null;
  timestamp?: string | null;
}

export interface ListPayload<T> {
  items: T[];
}

export interface PersonnelServicePool {
  id: string;
  name: string;
  description?: string | null;
  serviceIds: string[];
  shiftCoordinator?: string | null;
  contactEmail?: string | null;
  attributes?: Record<string, unknown>;
}

export type PersonnelServicePoolListRequest = ListPayload<PersonnelServicePool>;
export type PersonnelServicePoolListResponse = ListPayload<PersonnelServicePool>;

export interface PersonnelPool {
  id: string;
  name: string;
  description?: string | null;
  personnelIds: string[];
  locationCode?: string | null;
  attributes?: Record<string, unknown>;
}

export type PersonnelPoolListRequest = ListPayload<PersonnelPool>;
export type PersonnelPoolListResponse = ListPayload<PersonnelPool>;

export interface VehicleServicePool {
  id: string;
  name: string;
  description?: string | null;
  serviceIds: string[];
  dispatcher?: string | null;
  attributes?: Record<string, unknown>;
}

export type VehicleServicePoolListRequest = ListPayload<VehicleServicePool>;
export type VehicleServicePoolListResponse = ListPayload<VehicleServicePool>;

export interface VehiclePool {
  id: string;
  name: string;
  description?: string | null;
  vehicleIds: string[];
  depotManager?: string | null;
  attributes?: Record<string, unknown>;
}

export type VehiclePoolListRequest = ListPayload<VehiclePool>;
export type VehiclePoolListResponse = ListPayload<VehiclePool>;

export interface VehicleType {
  id: string;
  label: string;
  category?: string | null;
  capacity?: number | null;
  maxSpeed?: number | null;
  maintenanceIntervalDays?: number | null;
  energyType?: string | null;
  manufacturer?: string | null;
  trainTypeCode?: string | null;
  lengthMeters?: number | null;
  weightTons?: number | null;
  brakeType?: string | null;
  brakePercentage?: number | null;
  tiltingCapability?: 'none' | 'passive' | 'active' | null;
  powerSupplySystems?: string[];
  trainProtectionSystems?: string[];
  etcsLevel?: string | null;
  gaugeProfile?: string | null;
  maxAxleLoad?: number | null;
  noiseCategory?: string | null;
  remarks?: string | null;
  attributes?: Record<string, unknown>;
}

export type VehicleTypeListRequest = ListPayload<VehicleType>;
export type VehicleTypeListResponse = ListPayload<VehicleType>;

export interface VehicleCompositionEntry {
  typeId: string;
  quantity: number;
}

export interface VehicleComposition {
  id: string;
  name: string;
  entries: VehicleCompositionEntry[];
  turnaroundBuffer?: string | null;
  remark?: string | null;
  attributes?: Record<string, unknown>;
}

export type VehicleCompositionListRequest = ListPayload<VehicleComposition>;
export type VehicleCompositionListResponse = ListPayload<VehicleComposition>;

export type PlanningStageRealtimeScope = 'resources' | 'activities' | 'timeline';

export interface PlanningStageRealtimeEvent {
  stageId: StageId;
  scope: PlanningStageRealtimeScope;
  version?: string | null;
  sourceClientId?: string | null;
  sourceConnectionId?: string | null;
  upserts?: (Resource | Activity)[];
  deleteIds?: string[];
  timelineRange?: TimelineRange;
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
  attributes?: Record<string, unknown>;
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
