//! [Turso](https://github.com/tursodatabase/turso) backend for
//! [evento](https://docs.rs/evento): SQLite's schema and queries — the same
//! tables as `evento-sql`'s SQLite backend — in a pure-Rust, in-process, async
//! engine, with no `sqlx` and no C library.
//!
//! - [`Turso`] — the [`Executor`](evento_core::Executor): events, snapshots,
//!   subscriptions, keyset-paginated reads.
//! - [`MIGRATIONS`] / [`migrate`] — the schema, applied by [`Turso::open`] or
//!   by hand, recorded per app so an application's own migrations can share
//!   the database.
//! - [`TursoJournal`] (feature `cluster`) — the cluster consensus journal, in the
//!   same database as the events or in one of its own, with its own
//!   [`CLUSTER_MIGRATIONS`].
//! - [`Reader`], [`Bind`], [`FromRow`], [`execute`] / [`fetch_all`] — your own
//!   read models in the same database, with sea-query statements and keyset
//!   pagination, as `evento_sql::Reader` offers on sqlx.
//! - [`WriteMode`], [`Turso::open_concurrent`], [`transaction`] — several
//!   writers in one database (the store, the journal, your read models) on
//!   Turso's MVCC, with `BEGIN CONCURRENT` transactions retried on conflict
//!   (see [`mod@write`]).
//!
//! ```rust,no_run
//! # async fn run() -> anyhow::Result<()> {
//! let executor = evento_turso::Turso::open("./events.db").await?;
//! # let _ = executor;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `rw` (default) — `evento_core::Rw` / `Sidecar` wrappers and [`RwTurso`].
//! - `macro` (default) — the [`FromRow`](derive@FromRow) derive.
//! - `fts` — Turso's full-text search: `CREATE INDEX … USING fts(cols)` on a
//!   table, `WHERE (cols) MATCH ?` to filter, `fts_match(cols…, ?)` and
//!   `fts_score(cols…, ?)` (every column of the index, then the query) to
//!   filter and rank. Databases this crate opens then have Turso's
//!   index-method support switched on (see [`Turso::builder`]).
//! - `cluster` — [`TursoJournal`] and [`CLUSTER_MIGRATIONS`].
//! - `tracing` — logs from the migration runner.

#![forbid(unsafe_code)]

/// The Turso driver this crate is built on, re-exported for callers that open
/// databases themselves or write [`up_fn!`] migrations.
pub use turso;

mod executor;
pub mod migrate;
mod query;
mod reader;
mod row;
pub mod write;

#[cfg(feature = "cluster")]
mod cluster_journal;

#[cfg(feature = "cluster")]
pub use cluster_journal::TursoJournal;
pub use executor::{Clock, Turso, BUSY_TIMEOUT};
pub use query::{
    execute, fetch_all, fetch_one, fetch_optional, EventTable, MetaTable, SnapshotTable, Statement,
    SubscriberTable,
};
pub use reader::{Bind, Reader};
pub use row::{FromRow, FromValue, Row, Text};
pub use write::{enable_concurrent_writes, is_retryable, transaction, write_mode_of, WriteMode};

/// Derives [`FromRow`]: one field per column of the same name, read with
/// [`FromValue`]; `#[turso(default)]` lets a field default when its column
/// is not selected, `#[turso(rename = "column")]` reads another column.
#[cfg(feature = "macro")]
pub use evento_macro::TursoFromRow as FromRow;

use migrate::{Migration, Migrations};

/// Read-write executor pair over two Turso handles (`evento_core::Rw`).
#[cfg(feature = "rw")]
pub type RwTurso = evento_core::Rw<Turso, Turso>;

/// The event store schema: `event`, `snapshot`, `subscriber` and `meta`.
const INIT: Migration<'static> = up_file!("../migrations/0001_init.sql");

/// `idx_event_type_name_cursor`: a subscription's reads by event name seek
/// instead of scanning the type.
const EVENT_TYPE_NAME_CURSOR: Migration<'static> =
    up_file!("../migrations/0003_event_type_name_cursor.sql");

/// The cluster journal's tables (feature `cluster`).
#[cfg(feature = "cluster")]
const CLUSTER: Migration<'static> = up_file!("../migrations/0002_cluster.sql");

/// The event store's schema migrations, under the app name `evento`;
/// [`Turso::open`] applies the pending ones. Each applied migration is
/// recorded in [`migrate::MIGRATIONS_TABLE`], so an application's own
/// [`Migrations`] (under its own name) can share the database.
pub const MIGRATIONS: Migrations<'static> =
    Migrations::new("evento", &[INIT, EVENT_TYPE_NAME_CURSOR]);

/// The cluster journal's schema migrations, under the app name
/// `evento-cluster`; every [`TursoJournal`] constructor applies the pending
/// ones. Independent of [`MIGRATIONS`], so a journal can live in the store's
/// database or in one of its own.
#[cfg(feature = "cluster")]
pub const CLUSTER_MIGRATIONS: Migrations<'static> = Migrations::new("evento-cluster", &[CLUSTER]);
