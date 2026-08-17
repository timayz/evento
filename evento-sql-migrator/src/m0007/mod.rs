//! Migration tuning the `event` table's indexes for the hot read paths.
//!
//! Every aggregate-scoped read (`ORDER BY timestamp, timestamp_subsec,
//! version, id` filtered on `(aggregator_type, aggregator_id)`) previously had
//! no index covering its sort: `idx_event_type_id` stops at the id, so the
//! planner read all of an aggregate's rows and sorted them to return one page.
//! That path runs on every projection load and every version-1 routing-key
//! lookup.

mod event;

use sqlx_migrator::vec_box;

/// Migration that aligns the `event` indexes with the query shapes.
///
/// ## Changes
///
/// - Creates `idx_event_type_id_cursor` on
///   `(aggregator_type, aggregator_id, timestamp, timestamp_subsec, version, id)`,
///   so aggregate-scoped reads seek instead of scanning + sorting.
/// - Drops `idx_event_type_id`: it is a strict prefix of both the new index and
///   the unique `idx_event_type_id_version`, so it only cost write
///   amplification.
/// - Recreates `idx_event_type_routing_cursor` with a trailing `id` column, so
///   the routing-key subscription path fully covers the keyset tiebreaker and
///   needs no residual sort (matching m0005's `idx_event_type_cursor`).
///
/// ## Dependencies
///
/// This migration depends on [`M0006`](crate::M0006).
pub struct M0007;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0007,
    "main",
    "m0007",
    vec_box![crate::M0006],
    vec_box![
        event::create_type_id_cursor_idx::Operation,
        event::drop_type_id_idx::Operation,
        event::recreate_type_routing_cursor_idx::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0007,
    "main",
    "m0007",
    vec_box![crate::M0006],
    vec_box![
        event::create_type_id_cursor_idx::Operation,
        event::drop_type_id_idx::Operation,
        event::recreate_type_routing_cursor_idx::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0007,
    "main",
    "m0007",
    vec_box![crate::M0006],
    vec_box![
        event::create_type_id_cursor_idx::Operation,
        event::drop_type_id_idx::Operation,
        event::recreate_type_routing_cursor_idx::Operation,
    ]
);
