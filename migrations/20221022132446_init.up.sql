-- Add up migration script here
CREATE TABLE IF NOT EXISTS evento_event
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX ON evento_event (aggregate_id);
CREATE INDEX ON evento_event USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS evento_deadletter AS
TABLE evento_event
WITH NO DATA;

CREATE TABLE IF NOT EXISTS evento_queue
(
    id uuid NOT NULL PRIMARY KEY,
    consumer_id uuid NOT NULL,
    rule varchar(255) NOT NULL,
    enabled BOOLEAN NOT NULL,
    cursor uuid NULL,
    updated_at timestamptz NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX ON evento_queue (key);

-- Test only

DO
$$
DECLARE
  table_prefixes  text[] = array['save','load_save', 'lib_publish', 'lib_from_last', 'lib_filter', 'lib_deadletter'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS evento_%1$s_event AS
    TABLE evento_event
    WITH NO DATA;

    CREATE INDEX ON evento_%1$s_event (aggregate_id);
    CREATE INDEX ON evento_%1$s_event USING gin (metadata jsonb_path_ops);

    CREATE TABLE IF NOT EXISTS evento_%1$s_queue AS
    TABLE evento_queue
    WITH NO DATA;

    CREATE UNIQUE INDEX ON evento_%1$s_queue (key);

    CREATE TABLE IF NOT EXISTS evento_%1$s_deadletter AS
    TABLE evento_deadletter
    WITH NO DATA;

    ', table_prefix);
  END LOOP;
END;
$$;
