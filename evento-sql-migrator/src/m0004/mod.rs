//! Migration adding a composite index for subscription cursor scans.
//!
//! Subscription cursor queries filter on `aggregator_type` (optionally `name`,
//! always with a `routing_key` predicate) and order by
//! `(timestamp, timestamp_subsec, version, id)`. The original `idx_event_type`
//! covers only the filter, so the planner loads every matching row and sorts
//! externally; this dominates polling cost as the event table grows.
//!
//! This migration replaces the single-column `idx_event_type` with one composite
//! index that covers both filter and sort columns, allowing the planner to walk
//! the index in sorted order without an external sort.

mod event;

use sqlx_migrator::vec_box;

/// Migration that swaps `idx_event_type` for a composite cursor-scan index.
///
/// ## Changes
///
/// - Creates `idx_event_type_routing_cursor` on
///   `(aggregator_type, routing_key, timestamp, timestamp_subsec, version)`.
/// - Drops `idx_event_type` (redundant once the composite index exists — same
///   leading column, planner will use the new one).
///
/// ## Notes
///
/// - `name` is intentionally not part of the index: it is only ever a
///   per-aggregator-branch residual predicate inside an OR, and with the typical
///   small set of event names per aggregator type the residual scan stays cheap.
/// - The `ORDER BY` tail stops at `version`; `(timestamp, timestamp_subsec,
///   version)` is unique in practice, so the trailing `id` adds storage without
///   eliminating any sort work.
///
/// ## Dependencies
///
/// This migration depends on [`M0003`](crate::M0003).
pub struct M0004;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0004,
    "main",
    "m0004",
    vec_box![crate::M0003],
    vec_box![
        event::create_type_routing_cursor_idx::Operation,
        event::drop_type_idx::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0004,
    "main",
    "m0004",
    vec_box![crate::M0003],
    vec_box![
        event::create_type_routing_cursor_idx::Operation,
        event::drop_type_idx::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0004,
    "main",
    "m0004",
    vec_box![crate::M0003],
    vec_box![
        event::create_type_routing_cursor_idx::Operation,
        event::drop_type_idx::Operation,
    ]
);
