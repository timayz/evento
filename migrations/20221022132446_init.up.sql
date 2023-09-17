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

CREATE INDEX ON evento_events (aggregate_id);
CREATE INDEX ON evento_events USING gin (metadata jsonb_path_ops);

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

CREATE UNIQUE INDEX ON evento_subscriptions (key);

-- Test only

DO
$$
DECLARE
  table_prefixes  text[] = array['save','load_save', 'lib_publish', 'lib_filter', 'lib_deadletter'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS evento_%1$s_events AS
    TABLE evento_events
    WITH NO DATA;

    CREATE INDEX ON evento_%1$s_events (aggregate_id);
    CREATE INDEX ON evento_%1$s_events USING gin (metadata jsonb_path_ops);

    CREATE TABLE IF NOT EXISTS evento_%1$s_subscriptions AS
    TABLE evento_subscriptions
    WITH NO DATA;

    CREATE UNIQUE INDEX ON evento_%1$s_subscriptions (key);

    CREATE TABLE IF NOT EXISTS evento_%1$s_deadletters AS
    TABLE evento_deadletters
    WITH NO DATA;

    ', table_prefix);
  END LOOP;
END;
$$;
