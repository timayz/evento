#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! SQL database migrations for the Evento event sourcing library.
//!
//! This crate provides database schema migrations required for storing events, snapshots,
//! and subscriber state in SQL databases. It supports SQLite, MySQL, and PostgreSQL through
//! feature flags.
//!
//! # Features
//!
//! - **`sqlite`** - Enables SQLite database support
//! - **`mysql`** - Enables MySQL database support
//! - **`postgres`** - Enables PostgreSQL database support
//!
//! All features are enabled by default. You can selectively enable only the databases you need:
//!
//! ```toml
//! [dependencies]
//! evento-sql-migrator = { version = "1.8", default-features = false, features = ["postgres"] }
//! ```
//!
//! # Usage
//!
//! The main entry point is the [`new`] function, which creates a [`Migrator`]
//! instance configured with all Evento migrations.
//!
//! ```rust,no_run
//! use sqlx_migrator::{Migrate, Plan};
//!
//! # async fn run(pool: sqlx::SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
//! // Acquire a database connection
//! let mut conn = pool.acquire().await?;
//!
//! // Create the migrator for your database type
//! let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
//!
//! // Run all pending migrations
//! migrator.run(&mut *conn, &Plan::apply_all()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! When using the main `evento` crate, the migrator is re-exported:
//!
//! ```rust,no_run
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let migrator = evento::sql_migrator::new::<sqlx::Sqlite>()?;
//! # let _ = migrator;
//! # Ok(())
//! # }
//! ```
//!
//! # Migrations
//!
//! The crate includes the following migrations:
//!
//! - [`InitMigration`] - Creates the initial database schema (event, snapshot, subscriber tables)
//! - [`M0002`] - Adds `timestamp_subsec` column for sub-second precision timestamps
//! - [`M0003`] - Widens the event `name` column (no-op on databases created at the current schema)
//! - [`M0004`] - Replaces `idx_event_type` with a composite cursor-scan index
//! - [`M0005`] - Adds a leading-cursor index for no-routing-key subscription scans
//! - [`M0006`] - Repairs schema drift from early alphas (recreates `snapshot`, widens columns)
//!   and aligns `event` indexes with the hot read paths (aggregate-scoped cursor index, drops
//!   the redundant `(type, id)` prefix index, adds `id` to the routing cursor index)
//!
//! # Database Schema
//!
//! After running all migrations, the database will contain:
//!
//! ## Event Table
//!
//! Stores all domain events:
//!
//! | Column | Type | Description |
//! |--------|------|-------------|
//! | `id` | VARCHAR(26) | Event ID (ULID format) |
//! | `name` | VARCHAR(50) | Event type name |
//! | `aggregator_type` | VARCHAR(50) | Aggregate root type |
//! | `aggregator_id` | VARCHAR(64) | Aggregate root instance ID |
//! | `version` | INTEGER | Event sequence number |
//! | `data` | BLOB | Serialized event data |
//! | `metadata` | BLOB | Serialized event metadata |
//! | `routing_key` | VARCHAR(50) | Optional routing key |
//! | `timestamp` | BIGINT | Event timestamp (seconds) |
//! | `timestamp_subsec` | BIGINT | Sub-second precision |
//!
//! ## Subscriber Table
//!
//! Tracks event subscription progress:
//!
//! | Column | Type | Description |
//! |--------|------|-------------|
//! | `key` | VARCHAR(255) | Subscriber identifier (primary key) |
//! | `worker_id` | VARCHAR(26) | Associated worker ID |
//! | `cursor` | TEXT | Current event stream position |
//! | `lag` | INTEGER | Seconds behind the newest matching event |
//! | `enabled` | BOOLEAN | Whether subscription is active |
//! | `created_at` | TIMESTAMP | Creation timestamp |
//! | `updated_at` | TIMESTAMP | Last update timestamp |

use sqlx_migrator::{Info, Migrator};

#[cfg(feature = "accord")]
mod accord;
mod m0001;
mod m0002;
mod m0003;
mod m0004;
mod m0005;
mod m0006;

#[cfg(feature = "accord")]
pub use accord::AccordMigration;
pub use m0001::InitMigration;
pub use m0002::M0002;
pub use m0003::M0003;
pub use m0004::M0004;
pub use m0005::M0005;
pub use m0006::M0006;

/// Creates a new [`Migrator`] instance with all Evento migrations registered.
///
/// The migrator is generic over the database type and works with SQLite, MySQL, and PostgreSQL
/// when the corresponding feature is enabled.
///
/// # Example
///
/// ```rust,no_run
/// use sqlx_migrator::{Migrate, Plan};
///
/// # async fn run(
/// #     conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// // For SQLite
/// let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
///
/// // For PostgreSQL
/// let migrator = evento_sql_migrator::new::<sqlx::Postgres>()?;
///
/// // For MySQL
/// let migrator = evento_sql_migrator::new::<sqlx::MySql>()?;
///
/// // Run migrations
/// migrator.run(&mut **conn, &Plan::apply_all()).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if migration registration fails.
// Two definitions: the `accord` build adds the consensus-journal migration (and its
// extra trait bound); the default build is unchanged — so the `AccordMigration` bound
// never leaks onto callers (e.g. evento-sql's generic test harness) that don't opt in.
#[cfg(not(feature = "accord"))]
pub fn new<DB: sqlx::Database>() -> Result<Migrator<DB>, sqlx_migrator::Error>
where
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    M0005: sqlx_migrator::Migration<DB>,
    M0006: sqlx_migrator::Migration<DB>,
{
    let mut migrator = Migrator::default();
    migrator.add_migration(Box::new(InitMigration))?;
    migrator.add_migration(Box::new(M0002))?;
    migrator.add_migration(Box::new(M0003))?;
    migrator.add_migration(Box::new(M0004))?;
    migrator.add_migration(Box::new(M0005))?;
    migrator.add_migration(Box::new(M0006))?;
    Ok(migrator)
}

/// Creates a new [`Migrator`] instance with all Evento migrations registered.
///
/// This is the `accord` variant: identical to the default build, plus the
/// consensus-journal migration ([`AccordMigration`]) required by
/// `evento-accord`'s SQL journal.
///
/// # Example
///
/// ```rust,no_run
/// use sqlx_migrator::{Migrate, Plan};
///
/// # async fn run(
/// #     conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// // For SQLite
/// let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
///
/// // For PostgreSQL
/// let migrator = evento_sql_migrator::new::<sqlx::Postgres>()?;
///
/// // For MySQL
/// let migrator = evento_sql_migrator::new::<sqlx::MySql>()?;
///
/// // Run migrations
/// migrator.run(&mut **conn, &Plan::apply_all()).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if migration registration fails.
#[cfg(feature = "accord")]
pub fn new<DB: sqlx::Database>() -> Result<Migrator<DB>, sqlx_migrator::Error>
where
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    M0005: sqlx_migrator::Migration<DB>,
    M0006: sqlx_migrator::Migration<DB>,
    AccordMigration: sqlx_migrator::Migration<DB>,
{
    let mut migrator = Migrator::default();
    migrator.add_migration(Box::new(InitMigration))?;
    migrator.add_migration(Box::new(M0002))?;
    migrator.add_migration(Box::new(M0003))?;
    migrator.add_migration(Box::new(M0004))?;
    migrator.add_migration(Box::new(M0005))?;
    migrator.add_migration(Box::new(M0006))?;
    // The optional evento-accord consensus-journal tables.
    migrator.add_migration(Box::new(AccordMigration))?;
    Ok(migrator)
}
