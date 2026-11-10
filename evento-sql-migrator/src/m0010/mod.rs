//! Adds the event cursor index.
//!
//! The join copy (`evento-cluster`'s `snapshot_page`) reads the whole log —
//! no filters, every routing key, every tenant — in cursor order, a page at a
//! time. Every index on `event` led with `aggregator_type`, so an unfiltered
//! page had no index to walk: each one scanned and sorted the table, and the
//! copy cost grew with the square of the store (#505, the SQL half).
//! `idx_event_cursor` on `(timestamp, timestamp_subsec, version, id)` is the
//! cursor order itself: a page seeks to its cursor and reads the next rows in
//! index order.

use sea_query::Index;
use sqlx_migrator::vec_box;

use evento_sql::Event;

use crate::step::{step_operation, Backend, Step};

/// Migration that indexes every event by cursor.
///
/// ## Changes
///
/// 1. Creates `idx_event_cursor` on `(timestamp, timestamp_subsec, version,
///    id)`.
///
/// ## Dependencies
///
/// This migration depends on [`M0009`](crate::M0009).
pub struct M0010;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0010,
    "main",
    "m0010",
    vec_box![crate::M0009],
    vec_box![CreateCursorIdx]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0010,
    "main",
    "m0010",
    vec_box![crate::M0009],
    vec_box![CreateCursorIdx]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0010,
    "main",
    "m0010",
    vec_box![crate::M0009],
    vec_box![CreateCursorIdx]
);

/// Creates `idx_event_cursor`.
pub struct CreateCursorIdx;

impl Step for CreateCursorIdx {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::create()
                .name("idx_event_cursor")
                .table(Event::Table)
                .col(Event::Timestamp)
                .col(Event::TimestampSubsec)
                .col(Event::Version)
                .col(Event::Id),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(Index::drop().name("idx_event_cursor").table(Event::Table))]
    }
}

step_operation!(CreateCursorIdx);
