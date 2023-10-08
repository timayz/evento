CREATE TABLE IF NOT EXISTS ev_events
(
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    aggregate_id varchar(255) NOT NULL,
    version int NOT NULL,
    data json NOT NULL,
    metadata jsonb DEFAULT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX ON ev_events (aggregate_id);
CREATE INDEX ON ev_events USING gin (metadata jsonb_path_ops);

DO
$$
DECLARE
  table_prefixes  text[] = array['concurrency', 'save', 'wrong_version'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %1$s_events AS
    TABLE ev_events
    WITH NO DATA;

    CREATE INDEX ON %1$s_events (aggregate_id);
    CREATE INDEX ON %1$s_events USING gin (metadata jsonb_path_ops);

    ', table_prefix);
  END LOOP;
END;
$$;
