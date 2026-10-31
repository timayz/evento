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
//! - [`TursoJournal`] (feature `accord`) — the accord consensus journal, in the
//!   same database as the events or in one of its own, with its own
//!   [`ACCORD_MIGRATIONS`].
//! - [`Reader`], [`Bind`], [`FromRow`], [`execute`] / [`fetch_all`] — your own
//!   read models in the same database, with sea-query statements and keyset
//!   pagination, as `evento_sql::Reader` offers on sqlx.
//! - [`Reader`], [`Bind`], [`FromRow`], [`execute`] / [`fetch_all`] — your own
//!   read models in the same database, with sea-query statements and keyset
//!   pagination, as `evento_sql::Reader` offers on sqlx (see
//!   [`Reader`]).
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
//! - `macro` (default) — the [`FromRow`](derive@FromRow) derive.
//! - `fts` — Turso's full-text search: `CREATE INDEX … USING fts(cols)` on a
//!   table, `WHERE (cols) MATCH ?` to filter, `fts_match(cols…, ?)` and
//!   `fts_score(cols…, ?)` (every column of the index, then the query) to
//!   filter and rank. Databases this crate opens then have Turso's
//!   index-method support switched on (see [`Turso::builder`]).
//! - `accord` — [`TursoJournal`] and [`ACCORD_MIGRATIONS`].
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

#[cfg(feature = "accord")]
mod accord_journal;

#[cfg(feature = "accord")]
pub use accord_journal::TursoJournal;
pub use executor::{Clock, Turso};
pub use query::{
    execute, fetch_all, fetch_one, fetch_optional, EventTable, MetaTable, SnapshotTable, Statement,
    SubscriberTable,
};
pub use reader::{Bind, Reader};
pub use row::{FromRow, FromValue, Row, Text};

/// Derives [`FromRow`]: one field per column of the same name, read with
/// [`FromValue`]; `#[turso(default)]` lets a field default when its column
/// is not selected.
#[cfg(feature = "macro")]
pub use evento_macro::TursoFromRow as FromRow;

use migrate::{Migration, Migrations};

/// Read-write executor pair over two Turso handles (`evento_core::Rw`).
#[cfg(feature = "rw")]
pub type RwTurso = evento_core::Rw<Turso, Turso>;

/// The event store schema: `event`, `snapshot`, `subscriber` and `meta`.
const INIT: Migration<'static> = up_file!("../migrations/0001_init.sql");

/// The accord journal's tables (feature `accord`).
#[cfg(feature = "accord")]
const ACCORD: Migration<'static> = up_file!("../migrations/0002_accord.sql");

/// The event store's schema migrations, under the app name `evento`;
/// [`Turso::open`] applies the pending ones. Each applied migration is
/// recorded in [`migrate::MIGRATIONS_TABLE`], so an application's own
/// [`Migrations`] (under its own name) can share the database.
pub const MIGRATIONS: Migrations<'static> = Migrations::new("evento", &[INIT]);

/// The accord journal's schema migrations, under the app name
/// `evento-accord`; every [`TursoJournal`] constructor applies the pending
/// ones. Independent of [`MIGRATIONS`], so a journal can live in the store's
/// database or in one of its own.
#[cfg(feature = "accord")]
pub const ACCORD_MIGRATIONS: Migrations<'static> = Migrations::new("evento-accord", &[ACCORD]);
