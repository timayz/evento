use sea_query::{Index, IndexCreateStatement, IndexDropStatement};

use evento_sql::Event;

pub struct Operation;

fn drop_statement() -> IndexDropStatement {
    Index::drop()
        .name("idx_event_type_routing_cursor")
        .table(Event::Table)
        .to_owned()
}

/// The m0004 columns plus a trailing `id`, so the index fully covers the
/// keyset tiebreaker (the final `ORDER BY` / cursor column) and the
/// routing-key subscription path needs no residual sort — matching m0005's
/// `idx_event_type_cursor`.
fn create_new_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_type_routing_cursor")
        .table(Event::Table)
        .col(Event::AggregatorType)
        .col(Event::RoutingKey)
        .col(Event::Timestamp)
        .col(Event::TimestampSubsec)
        .col(Event::Version)
        .col(Event::Id)
        .to_owned()
}

/// The original m0004 shape, for rollback.
fn create_old_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_type_routing_cursor")
        .table(Event::Table)
        .col(Event::AggregatorType)
        .col(Event::RoutingKey)
        .col(Event::Timestamp)
        .col(Event::TimestampSubsec)
        .col(Event::Version)
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_new_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_old_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::MySql> for Operation {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_new_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_old_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Postgres> for Operation {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_new_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(&mut *connection)
            .await?;
        let statment = create_old_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(sqlx::AssertSqlSafe(statment.as_str()))
            .execute(connection)
            .await?;

        Ok(())
    }
}
