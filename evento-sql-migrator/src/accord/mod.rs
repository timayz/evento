//! Optional schema for the **evento-accord** SQL-backed consensus journal
//! (`SqlJournal<DB>`), behind the `accord` feature.
//!
//! These four tables back a replica's durable consensus state — the per-transaction
//! command log, the truncation watermark, the metadata (epoch→layout) log, and the
//! config-Paxos acceptor records. The table and column **names are a contract** with
//! `evento_sql::SqlJournal`; keep them in sync. The migration is pure DDL, so this
//! crate needs no dependency on `evento-accord`.

mod tables;

use sea_query::Iden;
use sqlx_migrator::vec_box;

/// Column identifiers for `accord_commands` — one row per transaction's
/// [`CommandState`](https://docs.rs/evento-accord), keyed by an order-preserving
/// 20-byte `TxnId`.
#[derive(Iden)]
pub(crate) enum AccordCommands {
    Table,
    Txn,
    Data,
}

/// Column identifiers for `accord_meta` — a small key/value table (the truncation
/// watermark lives under `k = 'redundant_before'`).
#[derive(Iden)]
pub(crate) enum AccordMeta {
    Table,
    K,
    V,
}

/// Column identifiers for `accord_metadata_log` — the decided `(epoch, layout)` log.
#[derive(Iden)]
pub(crate) enum AccordMetadataLog {
    Table,
    Epoch,
    Layout,
}

/// Column identifiers for `accord_acceptors` — config-Paxos acceptor state per epoch.
#[derive(Iden)]
pub(crate) enum AccordAcceptors {
    Table,
    Epoch,
    State,
}

/// Creates the evento-accord consensus-journal tables. Independent of the event
/// schema (no migration dependencies), so it can run on a database that holds only
/// the consensus state.
pub struct AccordMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    AccordMigration,
    "main",
    "accord_journal",
    vec_box![],
    vec_box![tables::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    AccordMigration,
    "main",
    "accord_journal",
    vec_box![],
    vec_box![tables::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    AccordMigration,
    "main",
    "accord_journal",
    vec_box![],
    vec_box![tables::Operation]
);
