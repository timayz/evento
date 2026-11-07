//! Adds the event-name cursor index.
//!
//! A subscription reads by `(aggregator_type, name)`, one filter per handled
//! event, and nothing indexed `name`: a name that was never written, and a
//! subscription's lag `MAX(timestamp)`, read every row of the type — a pass
//! cost time proportional to the store, whatever its cursor (#478, the SQL
//! half). `idx_event_type_name_cursor` on `(aggregator_type, name, timestamp,
//! timestamp_subsec, version, id)` serves a `(type, name)` arm the way
//! `idx_event_type_cursor` serves a `(type)` one: a seek to the cursor, the
//! page read in index order, `MAX(timestamp)` from the arm's last entry.

use sea_query::Index;
use sqlx_migrator::vec_box;

use evento_sql::Event;

use crate::step::{step_operation, Backend, Step};

/// Migration that indexes the events of a type by name and cursor.
///
/// ## Changes
///
/// 1. Creates `idx_event_type_name_cursor` on `(aggregator_type, name,
///    timestamp, timestamp_subsec, version, id)`.
///
/// ## Dependencies
///
/// This migration depends on [`M0008`](crate::M0008).
pub struct M0009;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0009,
    "main",
    "m0009",
    vec_box![crate::M0008],
    vec_box![CreateTypeNameCursorIdx]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0009,
    "main",
    "m0009",
    vec_box![crate::M0008],
    vec_box![CreateTypeNameCursorIdx]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0009,
    "main",
    "m0009",
    vec_box![crate::M0008],
    vec_box![CreateTypeNameCursorIdx]
);

/// Creates `idx_event_type_name_cursor`.
pub struct CreateTypeNameCursorIdx;

impl Step for CreateTypeNameCursorIdx {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::create()
                .name("idx_event_type_name_cursor")
                .table(Event::Table)
                .col(Event::AggregatorType)
                .col(Event::Name)
                .col(Event::Timestamp)
                .col(Event::TimestampSubsec)
                .col(Event::Version)
                .col(Event::Id),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::drop()
                .name("idx_event_type_name_cursor")
                .table(Event::Table),
        )]
    }
}

step_operation!(CreateTypeNameCursorIdx);
