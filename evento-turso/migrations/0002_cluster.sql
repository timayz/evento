-- The cluster consensus journal (feature `cluster`): the same four tables as
-- evento-sql's `SqlJournal` / `ClusterMigration`, in SQLite types. Applied under
-- the app name `evento-cluster`, independently of the event store's schema.
CREATE TABLE cluster_commands (
  txn BLOB NOT NULL PRIMARY KEY,
  data BLOB NOT NULL
);
CREATE TABLE cluster_meta (
  k TEXT NOT NULL PRIMARY KEY,
  v BLOB NOT NULL
);
CREATE TABLE cluster_metadata_log (
  epoch INTEGER NOT NULL PRIMARY KEY,
  layout BLOB NOT NULL
);
CREATE TABLE cluster_acceptors (
  epoch INTEGER NOT NULL PRIMARY KEY,
  state BLOB NOT NULL
);
