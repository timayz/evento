//! Keys projection snapshots by projection name.
//!
//! Until now the `snapshot` primary key was `(type, id)`: nothing said which
//! projection a row belonged to, so two snapshotted projections of one
//! aggregate overwrote each other and then failed to decode each other's
//! bytes. The key becomes `(type, projection, id)`.

mod recreate_snapshot;

use sqlx_migrator::vec_box;

/// Migration that adds the projection name to the `snapshot` primary key.
///
/// ## Changes
///
/// 1. Drops and recreates the `snapshot` table with a
///    `projection VARCHAR(255) NOT NULL` column and primary key
///    `(type, projection, id)`.
///
/// Existing snapshots are discarded: they are a cache, and every projection
/// rebuilds from events on its next load.
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
    vec_box![recreate_snapshot::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0007,
    "main",
    "m0007",
    vec_box![crate::M0006],
    vec_box![recreate_snapshot::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0007,
    "main",
    "m0007",
    vec_box![crate::M0006],
    vec_box![recreate_snapshot::Operation]
);
