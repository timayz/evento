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

/// The database an operation renders its statements for.
#[derive(Clone, Copy)]
enum Backend {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "mysql")]
    MySql,
    #[cfg(feature = "postgres")]
    Postgres,
}

impl Backend {
    /// Renders a schema statement (table or index DDL) for this backend.
    fn schema(self, statement: &impl sea_query::SchemaStatementBuilder) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => statement.to_string(sea_query::SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            Backend::MySql => statement.to_string(sea_query::MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            Backend::Postgres => statement.to_string(sea_query::PostgresQueryBuilder),
        }
    }
}

/// An operation made of plain statements, run in order.
trait Step: Send + Sync {
    /// The statements that apply the operation.
    fn up(&self, backend: Backend) -> Vec<String>;
    /// The statements that revert it.
    fn down(&self, backend: Backend) -> Vec<String>;
}

/// Implements `sqlx_migrator::Operation` for a [`Step`] on every enabled
/// backend.
macro_rules! step_operation {
    ($step:ty) => {
        #[cfg(feature = "sqlite")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::Sqlite> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::SqliteConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::up(self, $crate::m0008::Backend::Sqlite) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::SqliteConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::down(self, $crate::m0008::Backend::Sqlite) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }

        #[cfg(feature = "mysql")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::MySql> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::MySqlConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::up(self, $crate::m0008::Backend::MySql) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::MySqlConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::down(self, $crate::m0008::Backend::MySql) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }

        #[cfg(feature = "postgres")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::Postgres> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::PgConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::up(self, $crate::m0008::Backend::Postgres) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::PgConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::m0008::Step::down(self, $crate::m0008::Backend::Postgres) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }
    };
}

// Declared after `step_operation!`, which they invoke.
mod event;
mod guard;
mod snapshot;
