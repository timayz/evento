use sea_query::{ColumnDef, Table, TableAlterStatement};

use evento_sql::Event;

pub struct Operation;

fn up_statement() -> TableAlterStatement {
    Table::alter()
        .table(Event::Table)
        .modify_column(
            ColumnDef::new(Event::Name)
                .string()
                .string_len(50)
                .not_null(),
        )
        .to_owned()
}

fn down_statement() -> TableAlterStatement {
    Table::alter()
        .table(Event::Table)
        .modify_column(
            ColumnDef::new(Event::Name)
                .string()
                .string_len(20)
                .not_null(),
        )
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        _connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        // SQLite does not support ALTER COLUMN
        Ok(())
    }

    async fn down(
        &self,
        _connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        // SQLite does not support ALTER COLUMN
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
