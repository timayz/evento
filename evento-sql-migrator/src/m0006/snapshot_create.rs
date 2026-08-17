//! Recreates the `snapshot` table for databases that ran the original `m0003`
//! (which dropped it). `IF NOT EXISTS` makes this a no-op on fresh installs.
//! `down` is deliberately a no-op: on a fresh install the table belongs to
//! `m0001`, so reverting this repair must never drop it.

use sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement};

use evento_sql::Snapshot;

pub struct Operation;

fn up_statement() -> TableCreateStatement {
    Table::create()
        .table(Snapshot::Table)
        .if_not_exists()
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
        )
        .primary_key(Index::create().col(Snapshot::Type).col(Snapshot::Id))
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statement = up_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(
        &self,
        _connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::MySql> for Operation {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statement = up_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(
        &self,
        _connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Postgres> for Operation {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statement = up_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(&self, _connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }
}
