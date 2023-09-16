-- Add up migration script here
CREATE TABLE IF NOT EXISTS evento_events
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idk_aggregate_id ON evento_events (aggregate_id);
CREATE INDEX idk_metadata ON evento_events USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS evento_deadletters
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS evento_subscriptions
(
    id uuid NOT NULL PRIMARY KEY,
    consumer_id uuid NOT NULL,
    key varchar(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor uuid NULL,
    updated_at timestamptz NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX idk_key ON evento_subscriptions (key);
