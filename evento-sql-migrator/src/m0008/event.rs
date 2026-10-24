//! The `event` table: the tenant column and the indexes keyed by it.

use sea_query::{ColumnDef, Index, Table};

use evento_sql::Event;

use super::{Backend, Step};

/// Adds `event.tenant VARCHAR(255) NOT NULL DEFAULT ''`: every existing row
/// lands in the default tenant.
pub struct AddTenant;

impl Step for AddTenant {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Table::alter().table(Event::Table).add_column(
                ColumnDef::new(Event::Tenant)
                    .string()
                    .string_len(255)
                    .not_null()
                    .default(""),
            ),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Table::alter()
                .table(Event::Table)
                .drop_column(Event::Tenant),
        )]
    }
}

step_operation!(AddTenant);

/// The unique version index, the tenant inserted before the version: a
/// version conflict is per `(type, id, tenant)` stream.
fn type_id_tenant_version_uk() -> sea_query::IndexCreateStatement {
    Index::create()
        .name("idx_event_type_id_tenant_version")
        .table(Event::Table)
        .unique()
        .col(Event::AggregatorType)
        .col(Event::AggregatorId)
        .col(Event::Tenant)
        .col(Event::Version)
        .to_owned()
}

/// The unique version index it replaces (`m0001`).
fn type_id_version_uk() -> sea_query::IndexCreateStatement {
    Index::create()
        .name("idx_event_type_id_version")
        .table(Event::Table)
        .unique()
        .col(Event::AggregatorType)
        .col(Event::AggregatorId)
        .col(Event::Version)
        .to_owned()
}

/// Creates the unique `(aggregator_type, aggregator_id, tenant, version)`
/// index — before the old one goes, so versions stay guarded throughout.
pub struct CreateTypeIdTenantVersionUk;

impl Step for CreateTypeIdTenantVersionUk {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(&type_id_tenant_version_uk())]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::drop()
                .name("idx_event_type_id_tenant_version")
                .table(Event::Table),
        )]
    }
}

step_operation!(CreateTypeIdTenantVersionUk);

/// Drops the unique `(aggregator_type, aggregator_id, version)` index: two
/// tenants may now hold the same aggregate id.
pub struct DropTypeIdVersionUk;

impl Step for DropTypeIdVersionUk {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::drop()
                .name("idx_event_type_id_version")
                .table(Event::Table),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(&type_id_version_uk())]
    }
}

step_operation!(DropTypeIdVersionUk);

/// An aggregate cursor index on `(aggregator_type, aggregator_id[, tenant],
/// timestamp, timestamp_subsec, version, id)`.
fn type_id_cursor_idx(name: &str, with_tenant: bool) -> sea_query::IndexCreateStatement {
    let mut index = Index::create();
    index
        .name(name)
        .table(Event::Table)
        .col(Event::AggregatorType)
        .col(Event::AggregatorId);
    if with_tenant {
        index.col(Event::Tenant);
    }
    index
        .col(Event::Timestamp)
        .col(Event::TimestampSubsec)
        .col(Event::Version)
        .col(Event::Id)
        .to_owned()
}

/// Replaces `idx_event_type_id_cursor` with `idx_event_type_id_tenant_cursor`,
/// the tenant after the aggregate id: every projection load names its tenant.
pub struct RecreateTypeIdCursorIdx;

impl Step for RecreateTypeIdCursorIdx {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![
            backend.schema(&type_id_cursor_idx("idx_event_type_id_tenant_cursor", true)),
            backend.schema(
                Index::drop()
                    .name("idx_event_type_id_cursor")
                    .table(Event::Table),
            ),
        ]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![
            backend.schema(&type_id_cursor_idx("idx_event_type_id_cursor", false)),
            backend.schema(
                Index::drop()
                    .name("idx_event_type_id_tenant_cursor")
                    .table(Event::Table),
            ),
        ]
    }
}

step_operation!(RecreateTypeIdCursorIdx);

/// Creates `idx_event_type_tenant_cursor` on `(aggregator_type, tenant,
/// timestamp, timestamp_subsec, version, id)`, for one tenant's scans.
pub struct CreateTypeTenantCursorIdx;

impl Step for CreateTypeTenantCursorIdx {
    fn up(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::create()
                .name("idx_event_type_tenant_cursor")
                .table(Event::Table)
                .col(Event::AggregatorType)
                .col(Event::Tenant)
                .col(Event::Timestamp)
                .col(Event::TimestampSubsec)
                .col(Event::Version)
                .col(Event::Id),
        )]
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        vec![backend.schema(
            Index::drop()
                .name("idx_event_type_tenant_cursor")
                .table(Event::Table),
        )]
    }
}

step_operation!(CreateTypeTenantCursorIdx);
