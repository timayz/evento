-- Add up migration script here
CREATE TABLE IF NOT EXISTS _evento_events
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata json DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idk_aggregate_id ON _evento_events (aggregate_id);

CREATE TABLE IF NOT EXISTS _evento_deadletters
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata json DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS _evento_consumers
(
    id uuid NOT NULL PRIMARY KEY,
    key varchar(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor uuid NULL,
    processing_ids json NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idk_key ON _evento_consumers (key);
