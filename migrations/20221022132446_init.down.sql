-- Add down migration script here
DROP TABLE IF EXISTS ev_event;
DROP TABLE IF EXISTS ev_deadletter;
DROP TABLE IF EXISTS ev_queue;
