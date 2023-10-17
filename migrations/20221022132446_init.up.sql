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

CREATE INDEX ON ev_event (aggregate_id);
CREATE INDEX ON ev_event USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS ev_deadletter AS
TABLE ev_event
WITH NO DATA;

CREATE TABLE IF NOT EXISTS ev_queue
(
    id uuid NOT NULL PRIMARY KEY,
    consumer_id uuid NOT NULL,
    rule varchar(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor TEXT NULL,
    updated_at timestamptz NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX ON ev_queue (rule);
