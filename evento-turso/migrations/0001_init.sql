-- evento schema for Turso: the final shape of evento-sql's migrations (M0001..M0008),
-- in SQLite types. No SQL-side time defaults: `created_at` / `updated_at` are unix
-- seconds set by the executor from its clock (Turso documents no CURRENT_TIMESTAMP
-- default), and composite keys are UNIQUE indexes (Turso documents composite
-- PRIMARY KEYs only for WITHOUT ROWID tables).
CREATE TABLE event (
  id TEXT NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  aggregator_type TEXT NOT NULL,
  aggregator_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  data BLOB NOT NULL,
  metadata BLOB NOT NULL,
  routing_key TEXT NULL,
  timestamp INTEGER NOT NULL,
  timestamp_subsec INTEGER NOT NULL DEFAULT 0,
  tenant TEXT NOT NULL DEFAULT ''
);
-- Optimistic concurrency: one version per stream. `tenant` is NOT NULL on purpose:
-- NULLs are distinct in a UNIQUE index and would stop it catching conflicts.
CREATE UNIQUE INDEX idx_event_type_id_tenant_version ON event (aggregator_type, aggregator_id, tenant, version);
CREATE INDEX idx_event_type_id_tenant_cursor ON event (aggregator_type, aggregator_id, tenant, timestamp, timestamp_subsec, version, id);
CREATE INDEX idx_event_type_tenant_cursor ON event (aggregator_type, tenant, timestamp, timestamp_subsec, version, id);
CREATE INDEX idx_event_type_routing_cursor ON event (aggregator_type, routing_key, timestamp, timestamp_subsec, version, id);
CREATE INDEX idx_event_type_cursor ON event (aggregator_type, timestamp, timestamp_subsec, version, id);

CREATE TABLE snapshot (
  tenant TEXT NOT NULL DEFAULT '',
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  projection TEXT NOT NULL,
  cursor TEXT NOT NULL,
  revision TEXT NOT NULL,
  data BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NULL
);
CREATE UNIQUE INDEX idx_snapshot_key ON snapshot (tenant, type, projection, id);

CREATE TABLE subscriber (
  key TEXT NOT NULL PRIMARY KEY,
  worker_id TEXT NOT NULL,
  cursor TEXT NULL,
  lag INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NULL
);

-- Store metadata: the monotonic commit clock (`last_stamp`), as evento-fjall's
-- `meta` partition.
CREATE TABLE meta (
  k TEXT NOT NULL PRIMARY KEY,
  v BLOB NOT NULL
);
