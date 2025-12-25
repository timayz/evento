//! Migration for schema refinements.
//!
//! This module removes the snapshot table (no longer used) and extends the event
//! name column for longer event type names.

mod event;
mod snapshot;

use sqlx_migrator::vec_box;

/// Migration that drops the snapshot table and extends the event name column.
///
/// This migration makes two changes to optimize the schema:
///
/// ## Changes
///
/// 1. **Drops the snapshot table** - The snapshot functionality has been removed
///    from Evento, so this table is no longer needed.
///
/// 2. **Extends event.name column** - Changes the `name` column from VARCHAR(20)
///    to VARCHAR(50) to accommodate longer event type names.
///
/// ## Database-Specific Notes
///
/// - **SQLite**: The column alteration is a no-op since SQLite doesn't support
///   `ALTER COLUMN`. The column was already created with sufficient length.
/// - **MySQL**: Uses `MODIFY COLUMN` syntax.
/// - **PostgreSQL**: Uses standard `ALTER COLUMN` syntax.
///
/// ## Dependencies
///
/// This migration depends on [`M0002`](crate::M0002).
pub struct M0003;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);
