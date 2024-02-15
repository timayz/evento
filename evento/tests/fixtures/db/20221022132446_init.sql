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
CREATE UNIQUE INDEX ON ev_event (aggregate_id, version);

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

DO
$$
DECLARE
  table_prefixes  text[] = array[
    'multiple_consumer',
    'no_cdc', 'no_cdc_with_name',
    'external_store', 'external_store_with_name',
    'external_store_ext', 'external_store_with_name_ext',
    'filter', 'filter_with_name',
    'deadletter', 'deadletter_with_name',
    'post_handler', 'post_handler_with_name'
  ];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %1$s_event AS
    TABLE ev_event
    WITH NO DATA;

    CREATE INDEX ON %1$s_event (aggregate_id);
    CREATE INDEX ON %1$s_event USING gin (metadata jsonb_path_ops);
    CREATE UNIQUE INDEX ON %1$s_event (aggregate_id, version);

    CREATE TABLE IF NOT EXISTS %1$s_deadletter_event AS
    TABLE ev_event
    WITH NO DATA;

    CREATE TABLE IF NOT EXISTS %1$s_queue AS
    TABLE ev_queue
    WITH NO DATA;
  
    CREATE UNIQUE INDEX ON %1$s_queue (rule);
    ', table_prefix);
  END LOOP;
END;
$$;
