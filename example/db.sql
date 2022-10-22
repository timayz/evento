CREATE TABLE IF NOT EXISTS evento_events
(
    id varchar(36) NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data text DEFAULT NULL,
    metadata text DEFAULT NULL,
    created_at timestamp NOT NULL
);

CREATE INDEX idk_aggregate_id ON evento_events (aggregate_id);
