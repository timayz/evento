//! Migration adding ACCORD transaction tracking for crash recovery.
//!
//! This module adds the `accord_txn` table to track executed transactions,
//! enabling nodes to recover their state after a crash.

mod accord_txn;

use sqlx_migrator::vec_box;

/// Migration that adds the ACCORD transaction tracking table.
///
/// This migration creates the `accord_txn` table for tracking which transactions
/// have been executed locally. This enables crash recovery - when a node restarts,
/// it can load the executed transaction IDs and skip re-executing them.
///
/// ## Changes
///
/// - Creates `accord_txn` table with columns:
///   - `txn_id` (VARCHAR(50), PRIMARY KEY) - Transaction identifier
///   - `status` (INTEGER) - Transaction status (3 = Executed, 4 = ExecutionFailed)
///   - `executed_at` (TIMESTAMP) - When the transaction was executed
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
    vec_box![accord_txn::create_table::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0004,
    "main",
    "m0004",
    vec_box![crate::M0003],
    vec_box![accord_txn::create_table::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0004,
    "main",
    "m0004",
    vec_box![crate::M0003],
    vec_box![accord_txn::create_table::Operation]
);
