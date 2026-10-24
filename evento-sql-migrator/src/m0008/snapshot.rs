//! Recreates the `snapshot` table with the tenant in its primary key:
//! `(tenant, type, projection, id)` instead of `(type, projection, id)`.
//!
//! Dropped rather than altered, as in `m0007`: SQLite cannot change a primary
//! key in place, and the rows are a cache that every projection rebuilds from
//! events on its next load. `DROP ... IF EXISTS` keeps `up` re-runnable on
//! MySQL, whose DDL is not transactional.

use sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement};

use evento_sql::Snapshot;

use super::{Backend, Step};

/// The `snapshot` table, with or without the tenant (the `m0007` shape).
fn create_statement(with_tenant: bool) -> TableCreateStatement {
    let mut statement = Table::create();
    statement.table(Snapshot::Table);
    if with_tenant {
        statement.col(
            ColumnDef::new(Snapshot::Tenant)
                .string()
                .string_len(255)
                .not_null()
                .default(""),
        );
    }
    statement
        .col(
            ColumnDef::new(Snapshot::Id)
                .string()
                .not_null()
                .string_len(64),
        )
        .col(
            ColumnDef::new(Snapshot::Type)
                .string()
                .string_len(50)
                .not_null(),
        )
        .col(
            ColumnDef::new(Snapshot::Projection)
                .string()
                .string_len(255)
                .not_null(),
        )
        .col(ColumnDef::new(Snapshot::Cursor).string().not_null())
        .col(ColumnDef::new(Snapshot::Revision).string().not_null())
        .col(ColumnDef::new(Snapshot::Data).blob().not_null())
        .col(
            ColumnDef::new(Snapshot::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .col(
            ColumnDef::new(Snapshot::UpdatedAt)
                .timestamp_with_time_zone()
                .null(),
        );

    let mut primary_key = Index::create();
    if with_tenant {
        primary_key.col(Snapshot::Tenant);
    }
    primary_key
        .col(Snapshot::Type)
        .col(Snapshot::Projection)
        .col(Snapshot::Id);

    statement.primary_key(&mut primary_key).to_owned()
}

/// Recreates `snapshot` keyed by `(tenant, type, projection, id)`.
pub struct RecreateWithTenant;

impl Step for RecreateWithTenant {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![
            backend.schema(Table::drop().table(Snapshot::Table).if_exists()),
            backend.schema(&create_statement(true)),
        ]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![
            backend.schema(Table::drop().table(Snapshot::Table).if_exists()),
            backend.schema(&create_statement(false)),
        ]
    }
}

step_operation!(RecreateWithTenant);
