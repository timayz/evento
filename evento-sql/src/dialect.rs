//! The one place the crate maps a `sqlx` driver to its sea-query builder.
//!
//! Every statement the crate renders goes through here, so a dialect is
//! added, feature-gated or given a new error message once.

#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;
use sea_query_sqlx::SqlxBinder;
use sqlx::Database;

/// Renders a value-bound statement for `DB`'s dialect.
pub(crate) fn build_sqlx<DB: Database, S: SqlxBinder>(
    statement: &S,
) -> (String, sea_query_sqlx::SqlxValues) {
    match DB::NAME {
        #[cfg(feature = "sqlite")]
        "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
        #[cfg(feature = "mysql")]
        "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
        #[cfg(feature = "postgres")]
        "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
        name => unsupported(name),
    }
}

/// Renders a (value-free) schema statement for `DB`'s dialect.
#[cfg(feature = "accord")]
pub(crate) fn build_ddl<DB: Database>(statement: &sea_query::TableCreateStatement) -> String {
    match DB::NAME {
        #[cfg(feature = "sqlite")]
        "SQLite" => statement.to_string(SqliteQueryBuilder),
        #[cfg(feature = "mysql")]
        "MySQL" => statement.to_string(MysqlQueryBuilder),
        #[cfg(feature = "postgres")]
        "PostgreSQL" => statement.to_string(PostgresQueryBuilder),
        name => unsupported(name),
    }
}

/// The crate's one message for a driver it has no dialect for.
pub(crate) fn unsupported(name: &str) -> ! {
    panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL")
}
