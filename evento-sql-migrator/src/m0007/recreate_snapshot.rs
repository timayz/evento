//! Recreates the `snapshot` table with the projection name in its primary
//! key: `(type, projection, id)` instead of `(type, id)`.
//!
//! The table is dropped rather than altered: SQLite cannot change a primary
//! key in place, and the rows are a cache — every projection rebuilds from
//! events on its next load. `DROP ... IF EXISTS` keeps `up` re-runnable on
//! MySQL, whose DDL is not transactional (a crash between the drop and the
//! create leaves no table).
//!
//! `down` restores the previous shape instead of only dropping: the table
//! belongs to `m0001`, whose own `down` drops it unconditionally.

use sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement, TableDropStatement};

use evento_sql::Snapshot;

pub struct Operation;

fn drop_statement() -> TableDropStatement {
    Table::drop().table(Snapshot::Table).if_exists().to_owned()
}

fn create_statement(with_projection: bool) -> TableCreateStatement {
    let mut statement = Table::create();
    statement
        .table(Snapshot::Table)
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
        );

    if with_projection {
        statement.col(
            ColumnDef::new(Snapshot::Projection)
                .string()
                .string_len(255)
                .not_null(),
        );
    }

    statement
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
    primary_key.col(Snapshot::Type);
    if with_projection {
        primary_key.col(Snapshot::Projection);
    }
    primary_key.col(Snapshot::Id);

    statement.primary_key(&mut primary_key).to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        for statement in [
            drop_statement().to_string(sea_query::SqliteQueryBuilder),
            create_statement(true).to_string(sea_query::SqliteQueryBuilder),
        ] {
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
        for statement in [
            drop_statement().to_string(sea_query::SqliteQueryBuilder),
            create_statement(false).to_string(sea_query::SqliteQueryBuilder),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::MySql> for Operation {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in [
            drop_statement().to_string(sea_query::MysqlQueryBuilder),
            create_statement(true).to_string(sea_query::MysqlQueryBuilder),
        ] {
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
        for statement in [
            drop_statement().to_string(sea_query::MysqlQueryBuilder),
            create_statement(false).to_string(sea_query::MysqlQueryBuilder),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Postgres> for Operation {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in [
            drop_statement().to_string(sea_query::PostgresQueryBuilder),
            create_statement(true).to_string(sea_query::PostgresQueryBuilder),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in [
            drop_statement().to_string(sea_query::PostgresQueryBuilder),
            create_statement(false).to_string(sea_query::PostgresQueryBuilder),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

        Ok(())
    }
}
