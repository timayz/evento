//! Repair migration for databases upgraded through early alpha releases.
//!
//! Two earlier migrations were edited in place after being released, so a
//! database that ran them at an old alpha differs from a fresh install:
//!
//! - `m0003` originally dropped the `snapshot` table; the drop was later
//!   removed when snapshot support returned, but already-migrated databases
//!   kept no `snapshot` table.
//! - `m0001` originally created `event.aggregator_id` / `snapshot.id` as
//!   VARCHAR(26) and `event.name` as VARCHAR(20); the definitions were later
//!   widened in place (64 / 50), but already-migrated databases kept the
//!   narrow columns — overflowing on multi-id aggregates (silently truncating
//!   on non-strict MySQL).
//!
//! This migration converges every database to the current schema: it recreates
//! the `snapshot` table (`IF NOT EXISTS`, so it is a no-op on fresh installs)
//! and widens the affected columns. It also widens `subscriber.key` and
//! `event.routing_key` to VARCHAR(255): the subscriber key is
//! `{routing_key}.{name}`, which easily exceeds 50 chars for ULID-per-tenant
//! routing keys.

mod repair_columns;
mod snapshot_create;

use sqlx_migrator::vec_box;

/// Migration that repairs schema drift from in-place-edited early migrations
/// and widens `subscriber.key` / `event.routing_key`.
///
/// ## Changes
///
/// 1. Recreates the `snapshot` table (`IF NOT EXISTS`) for databases migrated
///    while `m0003` still dropped it.
/// 2. Widens `event.aggregator_id` and `snapshot.id` to VARCHAR(64),
///    `event.name` to VARCHAR(50), and `event.routing_key` /
///    `subscriber.key` to VARCHAR(255) (MySQL/PostgreSQL; SQLite ignores
///    VARCHAR lengths, so no alteration is needed there).
///
/// ## Dependencies
///
/// This migration depends on [`M0005`](crate::M0005).
pub struct M0006;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0006,
    "main",
    "m0006",
    vec_box![crate::M0005],
    vec_box![
        snapshot_create::Operation,
        repair_columns::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0006,
    "main",
    "m0006",
    vec_box![crate::M0005],
    vec_box![
        snapshot_create::Operation,
        repair_columns::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0006,
    "main",
    "m0006",
    vec_box![crate::M0005],
    vec_box![
        snapshot_create::Operation,
        repair_columns::Operation,
    ]
);
