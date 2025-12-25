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
//! ```rust,ignore
//! use sqlx_migrator::{Migrate, Plan};
//!
//! // Acquire a database connection
//! let mut conn = pool.acquire().await?;
//!
//! // Create the migrator for your database type
//! let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
//!
//! // Run all pending migrations
//! migrator.run(&mut *conn, &Plan::apply_all()).await?;
//! ```
//!
//! When using the main `evento` crate, the migrator is re-exported:
//!
//! ```rust,ignore
//! let migrator = evento::sql_migrator::new::<sqlx::Sqlite>()?;
//! ```
//!
//! # Migrations
//!
//! The crate includes the following migrations:
//!
//! - [`InitMigration`] - Creates the initial database schema (event, snapshot, subscriber tables)
//! - [`M0002`] - Adds `timestamp_subsec` column for sub-second precision timestamps
//! - [`M0003`] - Drops the snapshot table and extends the event name column length
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
//! | `aggregator_id` | VARCHAR(26) | Aggregate root instance ID |
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
//! | `key` | VARCHAR(50) | Subscriber identifier (primary key) |
//! | `worker_id` | VARCHAR(26) | Associated worker ID |
//! | `cursor` | TEXT | Current event stream position |
//! | `lag` | INTEGER | Subscription lag counter |
//! | `enabled` | BOOLEAN | Whether subscription is active |
//! | `created_at` | TIMESTAMP | Creation timestamp |
//! | `updated_at` | TIMESTAMP | Last update timestamp |

use sqlx_migrator::{Info, Migrator};

mod m0001;
mod m0002;
mod m0003;

pub use m0001::InitMigration;
pub use m0002::M0002;
pub use m0003::M0003;

/// Creates a new [`Migrator`] instance with all Evento migrations registered.
///
/// The migrator is generic over the database type and works with SQLite, MySQL, and PostgreSQL
/// when the corresponding feature is enabled.
///
/// # Example
///
/// ```rust,ignore
/// use sqlx_migrator::{Migrate, Plan};
///
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
/// migrator.run(&mut *conn, &Plan::apply_all()).await?;
/// ```
///
/// # Errors
///
/// Returns an error if migration registration fails.
pub fn new<DB: sqlx::Database>() -> Result<Migrator<DB>, sqlx_migrator::Error>
where
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
{
    let mut migrator = Migrator::default();
    migrator.add_migration(Box::new(InitMigration))?;
    migrator.add_migration(Box::new(M0002))?;
    migrator.add_migration(Box::new(M0003))?;

    Ok(migrator)
}
