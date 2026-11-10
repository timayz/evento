//! Optional schema for the **evento-cluster** SQL-backed consensus journal
//! (`SqlJournal<DB>`), behind the `cluster` feature.
//!
//! These four tables back a replica's durable consensus state — the per-transaction
//! command log, the truncation watermark, the metadata (epoch→layout) log, and the
//! config-Paxos acceptor records. The table and column **names are a contract** with
//! `evento_sql::SqlJournal`; keep them in sync. The migration is pure DDL, so this
//! crate needs no dependency on `evento-cluster`.

mod tables;

use sea_query::Iden;
use sqlx_migrator::vec_box;

/// Column identifiers for `cluster_commands` — one row per transaction's
/// [`CommandState`](https://docs.rs/evento-cluster), keyed by an order-preserving
/// 20-byte `TxnId`.
#[derive(Iden)]
pub(crate) enum ClusterCommands {
    Table,
    Txn,
    Data,
}

/// Column identifiers for `cluster_meta` — a small key/value table (the truncation
/// watermark lives under `k = 'redundant_before'`).
#[derive(Iden)]
pub(crate) enum ClusterMeta {
    Table,
    K,
    V,
}

/// Column identifiers for `cluster_metadata_log` — the decided `(epoch, layout)` log.
#[derive(Iden)]
pub(crate) enum ClusterMetadataLog {
    Table,
    Epoch,
    Layout,
}

/// Column identifiers for `cluster_acceptors` — config-Paxos acceptor state per epoch.
#[derive(Iden)]
pub(crate) enum ClusterAcceptors {
    Table,
    Epoch,
    State,
}

/// Creates the evento-cluster consensus-journal tables. Independent of the event
/// schema (no migration dependencies), so it can run on a database that holds only
/// the consensus state. It is its own `cluster` app: the event schema is app `main`,
/// and sqlx_migrator refuses an app with two heads, which a parentless migration
/// in `main` would be.
pub struct ClusterMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    ClusterMigration,
    "cluster",
    "cluster_journal",
    vec_box![],
    vec_box![tables::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    ClusterMigration,
    "cluster",
    "cluster_journal",
    vec_box![],
    vec_box![tables::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    ClusterMigration,
    "cluster",
    "cluster_journal",
    vec_box![],
    vec_box![tables::Operation]
);
