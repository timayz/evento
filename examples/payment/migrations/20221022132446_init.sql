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

CREATE TABLE IF NOT EXISTS ev_deadletter AS TABLE ev_event WITH NO DATA;

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

CREATE TYPE pm_order_status AS ENUM (
    'Draft',
    'Pending',
    'Delivered',
    'Canceled',
    'Deleted'
);

CREATE TABLE IF NOT EXISTS pm_order (
    id VARCHAR(26) NOT NULL PRIMARY KEY,
    shipping_address TEXT NOT NULL,
    status pm_order_status NOT NULL,
    updated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pm_order_item (
    product_id VARCHAR(10) NOT NULL,
    order_id VARCHAR(26) NOT NULL,
    name VARCHAR(100) NOT NULL,
    price REAL NOT NULL,
    quantity SMALLINT NOT NULL,
    updated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (product_id, order_id)
);

CREATE INDEX ON pm_order_item (order_id);