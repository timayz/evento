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
    cursor uuid NULL,
    updated_at timestamptz NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX ON ev_queue (key);

DO
$$
DECLARE
  table_prefixes  text[] = array['concurrency', 'save', 'wrong_version', 'insert'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %1$s_event AS
    TABLE ev_event
    WITH NO DATA;

    CREATE INDEX ON %1$s_event (aggregate_id);
    CREATE INDEX ON %1$s_event USING gin (metadata jsonb_path_ops);

    CREATE TABLE IF NOT EXISTS %1$s_deadletter_event AS
    TABLE ev_event
    WITH NO DATA;

    CREATE TABLE IF NOT EXISTS %1$s_queue AS
    TABLE ev_queue
    WITH NO DATA;
  
    CREATE UNIQUE INDEX ON %1$s_queue (key);
    ', table_prefix);
  END LOOP;
END;
$$;
