-- A subscription reads by (aggregator_type, name) — one pair per event it handles —
-- across every tenant, and takes MAX(timestamp) under the same filter for its lag.
-- With no index carrying `name`, both read every row of the type whatever the
-- cursor: a pass costs time proportional to the store (#478). This index seeks to
-- the pair, then to the cursor, and covers the sort. `idx_event_type_cursor` stays
-- for the reads by type alone (`.strict()`, `#[subscription_all]`).
CREATE INDEX idx_event_type_name_cursor ON event (aggregator_type, name, timestamp, timestamp_subsec, version, id);
