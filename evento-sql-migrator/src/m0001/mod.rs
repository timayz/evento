//! Initial database schema migration.
//!
//! This module contains the first migration that creates the foundational database
//! schema for Evento event sourcing.

mod event;
mod snapshot;
mod subscriber;

use sqlx_migrator::vec_box;

/// Initial migration that creates the core database schema for Evento.
///
/// This migration creates the following database objects:
///
/// ## Event Table
///
/// The main table for storing domain events with the following columns:
/// - `id` - Event identifier (ULID format, VARCHAR(26))
/// - `name` - Event type name (VARCHAR(50))
/// - `aggregator_type` - Aggregate root type (VARCHAR(50))
/// - `aggregator_id` - Aggregate root instance ID (VARCHAR(26))
/// - `version` - Event sequence number within the aggregate
/// - `data` - Serialized event payload (BLOB)
/// - `metadata` - Serialized event metadata (BLOB)
/// - `routing_key` - Optional routing key for partitioning (VARCHAR(50))
/// - `timestamp` - Event timestamp in seconds (BIGINT)
///
/// ## Event Indexes
///
/// Several indexes are created for efficient querying:
/// - `idx_event_type` - Index on `aggregator_type`
/// - `idx_event_type_id` - Composite index on `(aggregator_type, aggregator_id)`
/// - `idx_event_routing_key_type` - Composite index on `(routing_key, aggregator_type)`
/// - Unique constraint on `(aggregator_type, aggregator_id, version)`
///
/// ## Snapshot Table
///
/// Table for storing aggregate snapshots (note: dropped in [`M0003`](crate::M0003)):
/// - `id` - Snapshot identifier
/// - `type` - Snapshot type
/// - `cursor` - Event stream cursor
/// - `revision` - Revision identifier
/// - `data` - Serialized snapshot data
/// - Primary key: composite `(type, id)`
///
/// ## Subscriber Table
///
/// Table for tracking event subscription state:
/// - `key` - Subscriber identifier (primary key)
/// - `worker_id` - Associated worker ID
/// - `cursor` - Current event stream position
/// - `lag` - Subscription lag counter
/// - `enabled` - Whether subscription is active
/// - `created_at` / `updated_at` - Timestamps
pub struct InitMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);
