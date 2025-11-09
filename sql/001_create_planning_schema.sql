-- Planning schema for Postgres.
-- Run this after ensuring the database exists (e.g. CREATE DATABASE planning;).

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS planning_stage (
  stage_id TEXT PRIMARY KEY,
  version TIMESTAMPTZ NOT NULL DEFAULT now(),
  timeline_start TIMESTAMPTZ NOT NULL,
  timeline_end TIMESTAMPTZ NOT NULL
);

INSERT INTO planning_stage (stage_id, timeline_start, timeline_end)
VALUES
  ('base', '2025-03-01T06:00:00Z', '2025-03-01T18:00:00Z'),
  ('operations', '2025-03-01T06:00:00Z', '2025-03-01T18:00:00Z'),
  ('dispatch', '2025-03-01T06:00:00Z', '2025-03-01T18:00:00Z')
ON CONFLICT (stage_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS planning_resource (
  id TEXT PRIMARY KEY,
  stage_id TEXT NOT NULL REFERENCES planning_stage(stage_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  daily_service_capacity INTEGER,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_planning_resource_stage
  ON planning_resource(stage_id);

CREATE TABLE IF NOT EXISTS planning_activity (
  id TEXT PRIMARY KEY,
  stage_id TEXT NOT NULL REFERENCES planning_stage(stage_id) ON DELETE CASCADE,
  resource_id TEXT NOT NULL REFERENCES planning_resource(id) ON DELETE CASCADE,
  participant_resource_ids TEXT[],
  client_id TEXT,
  title TEXT NOT NULL,
  start TIMESTAMPTZ NOT NULL,
  "end" TIMESTAMPTZ,
  type TEXT,
  "from" TEXT,
  "to" TEXT,
  remark TEXT,
  service_id TEXT,
  service_template_id TEXT,
  service_date DATE,
  service_category TEXT,
  service_role TEXT,
  location_id TEXT,
  location_label TEXT,
  capacity_group_id TEXT,
  required_qualifications TEXT[],
  assigned_qualifications TEXT[],
  work_rule_tags TEXT[],
  attributes JSONB,
  meta JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_planning_activity_stage
  ON planning_activity(stage_id);

CREATE INDEX IF NOT EXISTS idx_planning_activity_resource
  ON planning_activity(resource_id);

CREATE TABLE IF NOT EXISTS personnel_service_pool (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  service_ids TEXT[] NOT NULL,
  shift_coordinator TEXT,
  contact_email TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS personnel_pool (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  personnel_ids TEXT[] NOT NULL,
  location_code TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle_service_pool (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  service_ids TEXT[] NOT NULL,
  dispatcher TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle_pool (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  vehicle_ids TEXT[] NOT NULL,
  depot_manager TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle_type (
  id TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  category TEXT,
  capacity INTEGER,
  max_speed INTEGER,
  maintenance_interval_days INTEGER,
  energy_type TEXT,
  manufacturer TEXT,
  train_type_code TEXT,
  length_meters NUMERIC,
  weight_tons NUMERIC,
  brake_type TEXT,
  brake_percentage INTEGER,
  tilting_capability TEXT,
  power_supply_systems TEXT[],
  train_protection_systems TEXT[],
  etcs_level TEXT,
  gauge_profile TEXT,
  max_axle_load NUMERIC,
  noise_category TEXT,
  remarks TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle_composition (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  turnaround_buffer INTERVAL,
  remark TEXT,
  attributes JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicle_composition_entry (
  composition_id TEXT NOT NULL REFERENCES vehicle_composition(id) ON DELETE CASCADE,
  type_id TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  PRIMARY KEY (composition_id, type_id)
);
