-- Add up migration script here
CREATE TABLE IF NOT EXISTS _evento_events
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idk_aggregate_id ON _evento_events (aggregate_id);
CREATE INDEX idk_metadata ON _evento_events USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS _evento_deadletters
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS _evento_subscriptions
(
    id uuid NOT NULL PRIMARY KEY,
    consumer_id uuid NOT NULL,
    key varchar(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor uuid NULL,
    cursor_updated_at timestamptz NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX idk_key ON _evento_subscriptions (key);
