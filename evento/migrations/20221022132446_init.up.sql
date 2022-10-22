-- Add up migration script here
CREATE TABLE IF NOT EXISTS evento_events
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata json DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX idk_aggregate_id ON evento_events (aggregate_id);
