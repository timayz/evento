//! [Turso](https://github.com/tursodatabase/turso) backend for
//! [evento](https://docs.rs/evento): SQLite's schema and queries — the same
//! tables as `evento-sql`'s SQLite backend — in a pure-Rust, in-process, async
//! engine, with no `sqlx` and no C library.
//!
//! - [`Turso`] — the [`Executor`](evento_core::Executor): events, snapshots,
//!   subscriptions, keyset-paginated reads.
//! - [`MIGRATIONS`] / [`migrate`] — the schema, applied by [`Turso::open`] or
//!   by hand, tracked in the database's `PRAGMA user_version`.
//! - [`TursoJournal`] (feature `accord`) — the accord consensus journal, in the
//!   same database as the events or in one of its own.
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
//! - `accord` — [`TursoJournal`], and the accord tables in [`MIGRATIONS`].
//! - `tracing` — logs from the migration runner.

#![forbid(unsafe_code)]

/// The Turso driver this crate is built on, re-exported for callers that open
/// databases themselves or write [`up_fn!`] migrations.
pub use turso;

mod executor;
pub mod migrate;
mod query;

#[cfg(feature = "accord")]
mod accord_journal;

#[cfg(feature = "accord")]
pub use accord_journal::TursoJournal;
pub use executor::{Clock, Turso};
pub use query::{EventTable, MetaTable, SnapshotTable, SubscriberTable};

use migrate::{Migration, Migrations};

/// Read-write executor pair over two Turso handles (`evento_core::Rw`).
#[cfg(feature = "rw")]
pub type RwTurso = evento_core::Rw<Turso, Turso>;

/// The event store schema: `event`, `snapshot`, `subscriber` and `meta`.
const INIT: Migration<'static> = up_file!("../migrations/0001_init.sql");

/// The accord journal's tables, as a migration (feature `accord`).
#[cfg(feature = "accord")]
const ACCORD: Migration<'static> = up_file!("../migrations/0002_accord.sql");

/// The accord journal's DDL (`IF NOT EXISTS`), run by
/// [`TursoJournal::migrate`] on its own.
#[cfg(feature = "accord")]
pub(crate) const ACCORD_DDL: &str = include_str!("../migrations/0002_accord.sql");

/// The schema migrations, in order; [`Turso::open`] applies the pending ones.
/// Each one is tracked in the database's `PRAGMA user_version`.
///
/// With the `accord` feature the list also holds the consensus journal's
/// tables, so a database migrated by an `accord` build is at `user_version`
/// 2: opening it from a build without the feature fails with
/// [`MigrationError::InvalidUserVersion`](migrate::MigrationError::InvalidUserVersion).
#[cfg(not(feature = "accord"))]
pub const MIGRATIONS: Migrations<'static> = Migrations::new(&[INIT]);

/// The schema migrations, in order; [`Turso::open`] applies the pending ones.
/// Each one is tracked in the database's `PRAGMA user_version`.
///
/// With the `accord` feature the list also holds the consensus journal's
/// tables, so a database migrated by an `accord` build is at `user_version`
/// 2: opening it from a build without the feature fails with
/// [`MigrationError::InvalidUserVersion`](migrate::MigrationError::InvalidUserVersion).
#[cfg(feature = "accord")]
pub const MIGRATIONS: Migrations<'static> = Migrations::new(&[INIT, ACCORD]);
