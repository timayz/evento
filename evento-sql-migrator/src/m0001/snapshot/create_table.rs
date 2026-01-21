use sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement, TableDropStatement};

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

fn down_statement() -> TableDropStatement {
    Table::drop().table(Snapshot::Table).to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = up_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = down_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::MySql> for Operation {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = up_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = down_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Postgres> for Operation {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = up_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = down_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}
