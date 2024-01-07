CREATE TABLE IF NOT EXISTS ev_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idx_ev_event_aggregate_id ON ev_event(aggregate_id);
-- CREATE INDEX idx_ev_event_metadata ON ev_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX idx_ev_event_aggregate_id_version ON ev_event(aggregate_id, version);

CREATE TABLE IF NOT EXISTS concurrency_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idx_concurrency_event_aggregate_id ON concurrency_event(aggregate_id);
-- CREATE INDEX ON concurrency_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX idx_concurrency_event_aggregate_id_version ON concurrency_event(aggregate_id, version);

CREATE TABLE IF NOT EXISTS save_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idx_save_event_aggregate_id ON save_event(aggregate_id);
-- CREATE INDEX ON save_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX idx_save_event_aggregate_id_version ON save_event(aggregate_id, version);

CREATE TABLE IF NOT EXISTS wrong_version_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idx_wrong_version_event_aggregate_id ON wrong_version_event(aggregate_id);
-- CREATE INDEX ON wrong_version_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX idx_wrong_version_event_aggregate_id_version ON wrong_version_event(aggregate_id, version);

CREATE TABLE IF NOT EXISTS insert_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idx_insert_event_aggregate_id ON insert_event(aggregate_id);
-- CREATE INDEX ON insert_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX idx_insert_event_aggregate_id_version ON insert_event(aggregate_id, version);
