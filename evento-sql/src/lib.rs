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
//! ```rust,ignore
//! use evento_sql::Sql;
//! use sqlx::sqlite::SqlitePoolOptions;
//!
//! // Create a connection pool
//! let pool = SqlitePoolOptions::new()
//!     .connect(":memory:")
//!     .await?;
//!
//! // Wrap it in the Sql executor
//! let executor: Sql<sqlx::Sqlite> = pool.into();
//!
//! // Use with Evento for event sourcing operations
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
//! The [`sql_types`] module provides the [`Rkyv`](sql_types::Rkyv) wrapper for zero-copy
//! serialization of event data using the rkyv format.

mod sql;
pub mod sql_types;

pub use sql::*;
