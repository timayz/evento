//! Adds the `dead_letter` table.
//!
//! A subscription in dead-letter mode records the events it gave up on —
//! still failing after their retries — and goes on instead of stopping
//! (#509). One row per `(subscriber, event_id)`, beside the subscriber's
//! cursor: it names the event, which stays in its stream.

use sea_query::{ColumnDef, Index, Table};
use sqlx_migrator::vec_box;

use evento_sql::DeadLetter;

use crate::step::{step_operation, Backend, Step};

/// Migration that creates the `dead_letter` table.
///
/// ## Changes
///
/// 1. Creates `dead_letter` with primary key `(subscriber, event_id)`.
///
/// ## Dependencies
///
/// This migration depends on [`M0010`](crate::M0010).
pub struct M0011;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0011,
    "main",
    "m0011",
    vec_box![crate::M0010],
    vec_box![CreateDeadLetter]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0011,
    "main",
    "m0011",
    vec_box![crate::M0010],
    vec_box![CreateDeadLetter]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0011,
    "main",
    "m0011",
    vec_box![crate::M0010],
    vec_box![CreateDeadLetter]
);

/// Creates the `dead_letter` table.
pub struct CreateDeadLetter;

impl Step for CreateDeadLetter {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Table::create()
                .table(DeadLetter::Table)
                // Re-runnable on MySQL, whose DDL is not transactional.
                .if_not_exists()
                .col(
                    ColumnDef::new(DeadLetter::Subscriber)
                        .string()
                        .string_len(255)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::EventId)
                        .string()
                        .string_len(26)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::AggregatorType)
                        .string()
                        .string_len(255)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::AggregatorId)
                        .string()
                        .string_len(255)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::Name)
                        .string()
                        .string_len(255)
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::Tenant)
                        .string()
                        .string_len(255)
                        .not_null()
                        .default(""),
                )
                .col(
                    ColumnDef::new(DeadLetter::RoutingKey)
                        .string()
                        .string_len(255)
                        .null(),
                )
                // 64-bit like `timestamp`: the executor reads every integer
                // of a row as one type, and Postgres's is exact about widths.
                .col(ColumnDef::new(DeadLetter::Version).big_integer().not_null())
                .col(ColumnDef::new(DeadLetter::Error).text().not_null())
                .col(
                    ColumnDef::new(DeadLetter::Attempts)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeadLetter::Timestamp)
                        .big_integer()
                        .not_null(),
                )
                .primary_key(
                    Index::create()
                        .col(DeadLetter::Subscriber)
                        .col(DeadLetter::EventId),
                ),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(Table::drop().table(DeadLetter::Table).if_exists())]
    }
}

step_operation!(CreateDeadLetter);
