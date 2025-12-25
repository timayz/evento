use sea_query::{ColumnDef, Table, TableCreateStatement, TableDropStatement};

use crate::sql::Event;

pub struct Operation;

fn up_statement() -> TableCreateStatement {
    Table::create()
        .table(Event::Table)
        .col(
            ColumnDef::new(Event::Id)
                .string()
                .not_null()
                .string_len(26)
                .primary_key(),
        )
        .col(
            ColumnDef::new(Event::Name)
                .string()
                .string_len(20)
                .not_null(),
        )
        .col(
            ColumnDef::new(Event::AggregatorType)
                .string()
                .string_len(50)
                .not_null(),
        )
        .col(
            ColumnDef::new(Event::AggregatorId)
                .string()
                .string_len(26)
                .not_null(),
        )
        .col(ColumnDef::new(Event::Version).integer().not_null())
        .col(ColumnDef::new(Event::Data).blob().not_null())
        .col(ColumnDef::new(Event::Metadata).blob().not_null())
        .col(ColumnDef::new(Event::RoutingKey).string().string_len(50))
        .col(ColumnDef::new(Event::Timestamp).big_integer().not_null())
        .to_owned()
}

fn down_statement() -> TableDropStatement {
    Table::drop().table(Event::Table).to_owned()
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
