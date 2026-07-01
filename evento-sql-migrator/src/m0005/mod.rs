//! Migration adding a leading-cursor index for no-routing-key subscription scans.
//!
//! The m0004 index `idx_event_type_routing_cursor` places `routing_key` between
//! `aggregator_type` and the cursor/sort columns, so it only helps subscriptions
//! that constrain `routing_key`. A subscription reading with `.all()` has no
//! `routing_key` predicate, so the planner can only use that index up to
//! `aggregator_type=?` — the `timestamp` range becomes unreachable, forcing a full
//! scan of every matching row plus an external sort on every poll.
//!
//! This migration adds a second, complementary index whose cursor columns come
//! immediately after `aggregator_type`, letting the `timestamp > ?` cursor bound
//! push into the index seek so a caught-up poll returns without a scan or sort.

mod event;

use sqlx_migrator::vec_box;

/// Migration that adds a leading-cursor index for `.all()` subscription scans.
///
/// ## Changes
///
/// - Creates `idx_event_type_cursor` on
///   `(aggregator_type, timestamp, timestamp_subsec, version, id)`.
///
/// ## Notes
///
/// - This is additive: `idx_event_type_routing_cursor` (m0004) is kept, since it
///   still serves subscriptions that filter by `routing_key`.
/// - The trailing `id` is included so the index fully covers the keyset tiebreaker
///   (`id` is the final `ORDER BY` / cursor column), avoiding any residual sort.
/// - It also lets the `latest_timestamp()` query (`MAX(timestamp)` over the same
///   filter) read the index tail.
///
/// ## Dependencies
///
/// This migration depends on [`M0004`](crate::M0004).
pub struct M0005;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0005,
    "main",
    "m0005",
    vec_box![crate::M0004],
    vec_box![event::create_type_cursor_idx::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0005,
    "main",
    "m0005",
    vec_box![crate::M0004],
    vec_box![event::create_type_cursor_idx::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0005,
    "main",
    "m0005",
    vec_box![crate::M0004],
    vec_box![event::create_type_cursor_idx::Operation]
);
