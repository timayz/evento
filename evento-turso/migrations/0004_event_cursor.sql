-- The join copy (evento-cluster's `snapshot_page`) reads the whole log — no
-- filters, every routing key, every tenant — in cursor order, a page at a time.
-- Every index on `event` led with `aggregator_type`, so an unfiltered page had no
-- index to walk: each one scanned and sorted the table, and the copy cost grew
-- with the square of the store (#505). This index is the cursor order itself:
-- a page seeks to its cursor and reads the next rows in index order.
CREATE INDEX idx_event_cursor ON event (timestamp, timestamp_subsec, version, id);
