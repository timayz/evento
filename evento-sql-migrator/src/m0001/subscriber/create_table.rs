use sea_query::{ColumnDef, Expr, Table, TableCreateStatement, TableDropStatement};

use evento_sql::Subscriber;

pub struct Operation;

fn up_statement() -> TableCreateStatement {
    Table::create()
        .table(Subscriber::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Subscriber::Key)
                .string()
                .string_len(50)
                .not_null()
                .primary_key(),
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
        )
        .to_owned()
}

fn down_statement() -> TableDropStatement {
    Table::drop().table(Subscriber::Table).to_owned()
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
