//! Keys the `subscriber` table by `(tenant, key)`, keeping every row.
//!
//! A subscription's cursor lives in its tenant: the same key in two tenants
//! names two subscriptions, and a client confined to one tenant cannot reach
//! another's row. Existing rows land in the default tenant (`''`), so every
//! running subscription resumes where it left off.
//!
//! SQLite cannot change a primary key in place, so the table is rebuilt:
//! created beside, filled from the old one, then renamed over it. MySQL and
//! PostgreSQL swap the primary key in place — in one `ALTER` on MySQL, whose
//! DDL is not transactional, so no crash can leave the table without one.

#[cfg(feature = "sqlite")]
use sea_query::{Alias, ColumnDef, Expr, Index, Query, Table, TableCreateStatement};

#[cfg(feature = "sqlite")]
use evento_sql::Subscriber;

use super::{Backend, Step};

/// The table a SQLite rebuild fills before renaming it over `subscriber`.
#[cfg(feature = "sqlite")]
const REBUILT: &str = "subscriber_m0008";

/// The `subscriber` table as `m0001` and `m0006` left it, keyed by `key`, or
/// with the tenant and keyed by `(tenant, key)`.
#[cfg(feature = "sqlite")]
fn create_statement(with_tenant: bool) -> TableCreateStatement {
    let mut statement = Table::create();
    statement.table(Alias::new(REBUILT));
    if with_tenant {
        statement.col(
            ColumnDef::new(Subscriber::Tenant)
                .string()
                .string_len(255)
                .not_null()
                .default(""),
        );
    }
    statement
        .col(
            ColumnDef::new(Subscriber::Key)
                .string()
                .string_len(255)
                .not_null(),
        )
        .col(
            ColumnDef::new(Subscriber::WorkerId)
                .string()
                .not_null()
                .string_len(26),
        )
        .col(ColumnDef::new(Subscriber::Cursor).string())
        .col(ColumnDef::new(Subscriber::Lag).integer().not_null())
        .col(
            ColumnDef::new(Subscriber::Enabled)
                .boolean()
                .not_null()
                .default(true),
        )
        .col(
            ColumnDef::new(Subscriber::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .col(
            ColumnDef::new(Subscriber::UpdatedAt)
                .timestamp_with_time_zone()
                .null(),
        );
    let mut primary_key = Index::create();
    if with_tenant {
        primary_key.col(Subscriber::Tenant);
    }
    primary_key.col(Subscriber::Key);
    statement.primary_key(&mut primary_key).to_owned()
}

/// The SQLite rebuild: create beside, copy, drop, rename. Every copied row
/// lands in the default tenant (up), or leaves it (down — the revert guard
/// has checked no other tenant holds a row).
#[cfg(feature = "sqlite")]
fn sqlite_rebuild(backend: Backend, with_tenant: bool) -> Vec<String> {
    let columns = || {
        [
            Subscriber::Key,
            Subscriber::WorkerId,
            Subscriber::Cursor,
            Subscriber::Lag,
            Subscriber::Enabled,
            Subscriber::CreatedAt,
            Subscriber::UpdatedAt,
        ]
    };
    let mut select = Query::select();
    if with_tenant {
        select.expr(Expr::val(""));
    }
    select.columns(columns()).from(Subscriber::Table);

    // One `columns` call: a second one replaces the list instead of adding.
    let mut inserted = Vec::from(columns());
    if with_tenant {
        inserted.insert(0, Subscriber::Tenant);
    }
    let mut insert = Query::insert();
    insert
        .into_table(Alias::new(REBUILT))
        .columns(inserted)
        .select_from(select)
        .expect("the select lists one value per inserted column");

    vec![
        backend.schema(&create_statement(with_tenant)),
        backend.query(&insert),
        backend.schema(Table::drop().table(Subscriber::Table)),
        backend.schema(Table::rename().table(Alias::new(REBUILT), Subscriber::Table)),
    ]
}

/// Adds `subscriber.tenant` and makes `(tenant, key)` the primary key.
pub struct KeyByTenant;

impl Step for KeyByTenant {
    fn up(&self, backend: Backend) -> Vec<String> {
        match backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => sqlite_rebuild(backend, true),
            #[cfg(feature = "mysql")]
            Backend::MySql => vec!["ALTER TABLE `subscriber` \
                 ADD COLUMN `tenant` VARCHAR(255) NOT NULL DEFAULT '', \
                 DROP PRIMARY KEY, \
                 ADD PRIMARY KEY (`tenant`, `key`)"
                .to_owned()],
            #[cfg(feature = "postgres")]
            Backend::Postgres => vec![
                r#"ALTER TABLE "subscriber" ADD COLUMN "tenant" VARCHAR(255) NOT NULL DEFAULT ''"#
                    .to_owned(),
                r#"ALTER TABLE "subscriber" DROP CONSTRAINT "subscriber_pkey""#.to_owned(),
                r#"ALTER TABLE "subscriber" ADD PRIMARY KEY ("tenant", "key")"#.to_owned(),
            ],
        }
    }

    fn down(&self, backend: Backend) -> Vec<String> {
        match backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => sqlite_rebuild(backend, false),
            #[cfg(feature = "mysql")]
            Backend::MySql => vec!["ALTER TABLE `subscriber` \
                 DROP PRIMARY KEY, \
                 ADD PRIMARY KEY (`key`), \
                 DROP COLUMN `tenant`"
                .to_owned()],
            #[cfg(feature = "postgres")]
            Backend::Postgres => vec![
                r#"ALTER TABLE "subscriber" DROP CONSTRAINT "subscriber_pkey""#.to_owned(),
                r#"ALTER TABLE "subscriber" ADD PRIMARY KEY ("key")"#.to_owned(),
                r#"ALTER TABLE "subscriber" DROP COLUMN "tenant""#.to_owned(),
            ],
        }
    }
}

step_operation!(KeyByTenant);
