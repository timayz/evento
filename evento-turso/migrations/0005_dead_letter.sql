-- The events a subscription gave up on: still failing after their retries, they
-- are recorded here and passed over instead of stopping the worker (#509). One row
-- per (subscriber, event_id), beside the subscriber's cursor; the row names the
-- event, which stays in its stream. `timestamp` is unix seconds, set by the
-- subscription.
CREATE TABLE dead_letter (
  subscriber TEXT NOT NULL,
  event_id TEXT NOT NULL,
  aggregator_type TEXT NOT NULL,
  aggregator_id TEXT NOT NULL,
  name TEXT NOT NULL,
  tenant TEXT NOT NULL DEFAULT '',
  routing_key TEXT NULL,
  version INTEGER NOT NULL,
  error TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  timestamp INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_dead_letter_key ON dead_letter (subscriber, event_id);
