-- The accord consensus journal (feature `accord`): the same four tables as
-- evento-sql's `SqlJournal` / `AccordMigration`, in SQLite types. Applied under
-- the app name `evento-accord`, independently of the event store's schema.
CREATE TABLE accord_commands (
  txn BLOB NOT NULL PRIMARY KEY,
  data BLOB NOT NULL
);
CREATE TABLE accord_meta (
  k TEXT NOT NULL PRIMARY KEY,
  v BLOB NOT NULL
);
CREATE TABLE accord_metadata_log (
  epoch INTEGER NOT NULL PRIMARY KEY,
  layout BLOB NOT NULL
);
CREATE TABLE accord_acceptors (
  epoch INTEGER NOT NULL PRIMARY KEY,
  state BLOB NOT NULL
);
