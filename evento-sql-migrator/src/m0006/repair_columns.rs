//! Widens columns that were narrowed on databases migrated before the
//! in-place edits to `m0001`/`m0003`, and widens `subscriber.key` /
//! `event.routing_key` for long routing-key-prefixed subscription keys.
//!
//! SQLite ignores VARCHAR lengths entirely, so it needs no alteration.
//! `down` is a no-op: shrinking columns is destructive and the widened
//! definitions are the canonical schema.

use sea_query::{ColumnDef, Table, TableAlterStatement};

use evento_sql::{Event, Snapshot, Subscriber};

pub struct Operation;

fn up_statements() -> Vec<TableAlterStatement> {
    vec![
        Table::alter()
            .table(Event::Table)
            .modify_column(
                ColumnDef::new(Event::AggregatorId)
                    .string()
                    .string_len(64)
                    .not_null(),
            )
            .to_owned(),
        Table::alter()
            .table(Event::Table)
            .modify_column(
                ColumnDef::new(Event::Name)
                    .string()
                    .string_len(50)
                    .not_null(),
            )
            .to_owned(),
        Table::alter()
            .table(Event::Table)
            .modify_column(ColumnDef::new(Event::RoutingKey).string().string_len(255))
            .to_owned(),
        Table::alter()
            .table(Snapshot::Table)
            .modify_column(
                ColumnDef::new(Snapshot::Id)
                    .string()
                    .string_len(64)
                    .not_null(),
            )
            .to_owned(),
        Table::alter()
            .table(Subscriber::Table)
            .modify_column(
                ColumnDef::new(Subscriber::Key)
                    .string()
                    .string_len(255)
                    .not_null(),
            )
            .to_owned(),
    ]
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(
        &self,
        _connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        // SQLite ignores VARCHAR lengths; nothing to widen.
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
        for statement in up_statements() {
            let statement = statement.to_string(sea_query::MysqlQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

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
        for statement in up_statements() {
            let statement = statement.to_string(sea_query::PostgresQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                .execute(&mut *connection)
                .await?;
        }

        Ok(())
    }

    async fn down(&self, _connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }
}
