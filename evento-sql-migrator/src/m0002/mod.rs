//! Migration adding sub-second timestamp precision.
//!
//! This module adds the `timestamp_subsec` column to support microsecond/nanosecond
//! precision for event timestamps.

mod event;

use sqlx_migrator::vec_box;

/// Migration that adds sub-second precision to event timestamps.
///
/// This migration adds the `timestamp_subsec` column to the event table, allowing
/// events to be stored with microsecond or nanosecond precision instead of just
/// second-level precision.
///
/// ## Changes
///
/// - Adds `timestamp_subsec` column (BIGINT, NOT NULL, DEFAULT 0) to the event table
///
/// ## Dependencies
///
/// This migration depends on [`InitMigration`](crate::InitMigration).
pub struct M0002;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::InitMigration],
    vec_box![event::add_column_timestamp_subsec::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::InitMigration],
    vec_box![event::add_column_timestamp_subsec::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::InitMigration],
    vec_box![event::add_column_timestamp_subsec::Operation]
);
