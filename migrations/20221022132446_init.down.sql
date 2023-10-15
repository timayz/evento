-- Add down migration script here
DROP TABLE IF EXISTS evento_event;
DROP TABLE IF EXISTS evento_deadletter;
DROP TABLE IF EXISTS evento_queue;

-- Test only
DO
$$
DECLARE
  table_prefixes  text[] = array['save','load_save', 'lib_publish', 'lib_from_last', 'lib_filter', 'lib_deadletter'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_event;', table_prefix);
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_subscription;', table_prefix);
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_deadletter;', table_prefix);
  END LOOP;
END;
$$;
