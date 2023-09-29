-- Add down migration script here
DROP TABLE IF EXISTS evento_events;
DROP TABLE IF EXISTS evento_deadletters;
DROP TABLE IF EXISTS evento_subscriptions;

-- Test only
DO
$$
DECLARE
  table_prefixes  text[] = array['save','load_save', 'lib_publish', 'lib_from_last', 'lib_filter', 'lib_deadletter'];
  table_prefix     text;
BEGIN
  FOREACH table_prefix IN ARRAY table_prefixes LOOP
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_events;', table_prefix);
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_subscriptions;', table_prefix);
    EXECUTE format('DROP TABLE IF EXISTS evento_%s_deadletters;', table_prefix);
  END LOOP;
END;
$$;
