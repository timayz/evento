//! Migration adding composite indexes for subscription cursor scans.
//!
//! This module adds two composite indexes on the event table that cover both the
//! filter and the `ORDER BY` columns used by subscription cursor queries, so the
//! database can satisfy them with an index scan instead of a temp B-tree sort.

mod event;

use sqlx_migrator::vec_box;

/// Migration that adds composite cursor-scan indexes to the event table.
///
/// Subscription cursor queries filter on `aggregator_type` (optionally also `name`)
/// and `routing_key`, ordering by `(timestamp, timestamp_subsec, version, id)`.
/// The original indexes (`idx_event_type`, `idx_event_type_id`,
/// `idx_event_routing_key_type`) cover only the filter, so the planner must load
/// every matching row and sort. As the event table grows this becomes the
/// dominant cost of subscription polling.
///
/// ## Changes
///
/// Two composite indexes are added. The leading columns match the filter; the
/// trailing columns match the `ORDER BY`, allowing a forward index scan (read in
/// reverse for `DESC`) without an external sort:
///
/// - `idx_event_type_routing_cursor` on
///   `(aggregator_type, routing_key, timestamp, timestamp_subsec, version, id)`
/// - `idx_event_type_name_routing_cursor` on
///   `(aggregator_type, name, routing_key, timestamp, timestamp_subsec, version, id)`
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
        event::create_type_name_routing_cursor_idx::Operation,
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
        event::create_type_name_routing_cursor_idx::Operation,
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
        event::create_type_name_routing_cursor_idx::Operation,
    ]
);
