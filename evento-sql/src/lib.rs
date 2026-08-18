#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! SQL database implementations for the Evento event sourcing library.
//!
//! This crate provides SQL-based persistence for events and subscriber state, supporting
//! SQLite, MySQL, and PostgreSQL through feature flags.
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
//! evento-sql = { version = "1.8", default-features = false, features = ["postgres"] }
//! ```
//!
//! # Usage
//!
//! The main type is [`Sql<DB>`], a generic wrapper around a SQLx connection pool that
//! implements the [`Executor`](evento_core::Executor) trait for event sourcing operations.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use evento_sql::Sql;
//! use sqlx::sqlite::SqlitePoolOptions;
//!
//! # async fn run() -> anyhow::Result<()> {
//! // Create a connection pool
//! let pool = SqlitePoolOptions::new()
//!     .connect(":memory:")
//!     .await?;
//!
//! // Wrap it in the Sql executor
//! let executor: Sql<sqlx::Sqlite> = pool.into();
//!
//! // Use with Evento for event sourcing operations
//! # let _ = executor;
//! # Ok(())
//! # }
//! ```
//!
//! ## Type Aliases
//!
//! Convenience type aliases are provided for each database:
//!
//! - [`Sqlite`] - `Sql<sqlx::Sqlite>`
//! - [`MySql`] - `Sql<sqlx::MySql>`
//! - [`Postgres`] - `Sql<sqlx::Postgres>`
//!
//! Read-write pair types for CQRS patterns:
//!
//! - [`RwSqlite`] - Read/write SQLite executor pair
//! - [`RwMySql`] - Read/write MySQL executor pair
//! - [`RwPostgres`] - Read/write PostgreSQL executor pair
//!
//! # Core Components
//!
//! - [`Sql`] - The main executor type wrapping a SQLx connection pool
//! - [`Reader`] - Query builder for reading events with cursor-based pagination
//! - [`Bind`] - Trait for cursor serialization in paginated queries
//! - [`Event`], [`Subscriber`], [`Snapshot`] - Sea-Query column identifiers
//!
//! # Serialization
//!
//! The `sql_types` module (SQLite only, feature `sqlite`) provides the
//! `Bitcode` wrapper for compact binary serialization of event data using the
//! bitcode format.

mod sql;
#[cfg(feature = "sqlite")]
pub mod sql_types;

pub use sql::*;

/// A SQL-backed [`evento_accord::Journal`] (the Accord consensus log), behind the
/// optional `accord` feature.
#[cfg(feature = "accord")]
mod accord_journal;
#[cfg(feature = "accord")]
pub use accord_journal::SqlJournal;
