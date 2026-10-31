-- The accord consensus journal (feature `accord`): the same four tables as
-- evento-sql's `SqlJournal` / `AccordMigration`, in SQLite types. `IF NOT EXISTS`
-- so `TursoJournal::migrate` can run this file on its own.
CREATE TABLE IF NOT EXISTS accord_commands (
  txn BLOB NOT NULL PRIMARY KEY,
  data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS accord_meta (
  k TEXT NOT NULL PRIMARY KEY,
  v BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS accord_metadata_log (
  epoch INTEGER NOT NULL PRIMARY KEY,
  layout BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS accord_acceptors (
  epoch INTEGER NOT NULL PRIMARY KEY,
  state BLOB NOT NULL
);
