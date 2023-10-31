CREATE TABLE IF NOT EXISTS ev_event (
    id uuid NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    data JSON NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON ev_event (aggregate_id);
CREATE INDEX ON ev_event USING gin (metadata jsonb_path_ops);
CREATE UNIQUE INDEX ON ev_event (aggregate_id, version);

CREATE TABLE IF NOT EXISTS ev_deadletter_event AS TABLE ev_event WITH NO DATA;

CREATE TABLE IF NOT EXISTS ev_queue (
    id UUID NOT NULL PRIMARY KEY,
    consumer_id UUID NOT NULL,
    rule VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor TEXT NULL,
    updated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX ON ev_queue (rule);

CREATE TABLE IF NOT EXISTS iv_product (
    id VARCHAR(10) NOT NULL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL,
    name VARCHAR(50) NOT NULL,
    description TEXT NULL,
    stock INT NOT NULL DEFAULT 0,
    price REAL NULL,
    visible BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL
);