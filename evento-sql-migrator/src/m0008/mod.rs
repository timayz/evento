//! Adds the tenant to the `event` and `snapshot` tables.
//!
//! A stream is identified by `(tenant, aggregator_type, aggregator_id)`: the
//! same aggregate id in two tenants names two streams, so one store can hold
//! every tenant's events and a single subscription can serve them all. The
//! default tenant is `''`, where every row written before this migration
//! lives, so a single-tenant application sees no change. The column is
//! `NOT NULL DEFAULT ''` rather than nullable: NULLs are distinct in a UNIQUE
//! index, which would stop the version index from catching a conflict.
//!
//! Index plan. A read of one aggregate always names its tenant; a scan across
//! aggregates names one tenant, or every tenant for a subscription:
//!
//! - the version index and the aggregate cursor index gain the tenant after the
//!   aggregate id, so a per-tenant singleton id (`"settings"` in every tenant)
//!   still seeks exactly, and a by-id read of every tenant still seeks on
//!   `(type, id)`;
//! - a new `(aggregator_type, tenant, …cursor)` index serves one tenant's
//!   scans — every evento-remote client of a session serving a tenant reads
//!   through it — with the routing key and event name as residual filters;
//! - the routing-key and type cursor indexes are unchanged: they serve the
//!   scans of every tenant, which the tenant in front would turn into sorts.

use sqlx_migrator::vec_box;

/// Migration that adds the tenant to the event store.
///
/// ## Changes
///
/// 1. Adds `event.tenant VARCHAR(255) NOT NULL DEFAULT ''`.
/// 2. Creates the unique `idx_event_type_id_tenant_version` on
///    `(aggregator_type, aggregator_id, tenant, version)`, then drops the
///    unique `idx_event_type_id_version` it replaces.
/// 3. Replaces `idx_event_type_id_cursor` with `idx_event_type_id_tenant_cursor`
///    on `(aggregator_type, aggregator_id, tenant, timestamp, timestamp_subsec,
///    version, id)`.
/// 4. Creates `idx_event_type_tenant_cursor` on `(aggregator_type, tenant,
///    timestamp, timestamp_subsec, version, id)`.
/// 5. Recreates the `snapshot` table with a `tenant` column and the primary
///    key `(tenant, type, projection, id)`. Existing snapshots are discarded:
///    they are a cache, rebuilt from events on the next load.
///
/// The `subscriber` table is unchanged: a subscription reads every tenant, so
/// its row stays keyed by the subscription key alone.
///
/// Reverting refuses while a tenant other than the default one holds events:
/// the previous schema cannot tell them apart.
///
/// ## Dependencies
///
/// This migration depends on [`M0007`](crate::M0007).
pub struct M0008;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0008,
    "main",
    "m0008",
    vec_box![crate::M0007],
    vec_box![
        event::AddTenant,
        event::CreateTypeIdTenantVersionUk,
        event::DropTypeIdVersionUk,
        event::RecreateTypeIdCursorIdx,
        event::CreateTypeTenantCursorIdx,
        snapshot::RecreateWithTenant,
        guard::RevertGuard,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0008,
    "main",
    "m0008",
    vec_box![crate::M0007],
    vec_box![
        event::AddTenant,
        event::CreateTypeIdTenantVersionUk,
        event::DropTypeIdVersionUk,
        event::RecreateTypeIdCursorIdx,
        event::CreateTypeTenantCursorIdx,
        snapshot::RecreateWithTenant,
        guard::RevertGuard,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0008,
    "main",
    "m0008",
    vec_box![crate::M0007],
    vec_box![
        event::AddTenant,
        event::CreateTypeIdTenantVersionUk,
        event::DropTypeIdVersionUk,
        event::RecreateTypeIdCursorIdx,
        event::CreateTypeTenantCursorIdx,
        snapshot::RecreateWithTenant,
        guard::RevertGuard,
    ]
);

mod event;
mod guard;
mod snapshot;
