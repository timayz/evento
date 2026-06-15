//! The single create/drop operation for the four accord-journal tables.

use sea_query::{ColumnDef, Table, TableCreateStatement, TableDropStatement};

use super::{AccordAcceptors, AccordCommands, AccordMetadataLog, AccordMeta};

pub struct Operation;

/// `CREATE TABLE` statements for the four tables. Types are chosen to be portable
/// across SQLite/MySQL/PostgreSQL: the `txn` key is `VARBINARY(20)` (a BLOB primary
/// key is invalid on MySQL) — the 20-byte order-preserving `(micros, logical, node)`
/// pack `SqlJournal` writes — and the `meta` key is `VARCHAR(64)`. **Must match
/// `evento_sql::SqlJournal::migrate`.**
fn up_statements() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(AccordCommands::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(AccordCommands::Txn)
                    .var_binary(20)
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(AccordCommands::Data).blob().not_null())
            .to_owned(),
        Table::create()
            .table(AccordMeta::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(AccordMeta::K)
                    .string_len(64)
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(AccordMeta::V).blob().not_null())
            .to_owned(),
        Table::create()
            .table(AccordMetadataLog::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(AccordMetadataLog::Epoch)
                    .big_integer()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(AccordMetadataLog::Layout).blob().not_null())
            .to_owned(),
        Table::create()
            .table(AccordAcceptors::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(AccordAcceptors::Epoch)
                    .big_integer()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(AccordAcceptors::State).blob().not_null())
            .to_owned(),
    ]
}

fn down_statements() -> Vec<TableDropStatement> {
    vec![
        Table::drop().table(AccordCommands::Table).to_owned(),
        Table::drop().table(AccordMeta::Table).to_owned(),
        Table::drop().table(AccordMetadataLog::Table).to_owned(),
        Table::drop().table(AccordAcceptors::Table).to_owned(),
    ]
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for Operation {
    async fn up(&self, connection: &mut sqlx::SqliteConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in up_statements() {
            let sql = statement.to_string(sea_query::SqliteQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        for statement in down_statements() {
            let sql = statement.to_string(sea_query::SqliteQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
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
        for statement in up_statements() {
            let sql = statement.to_string(sea_query::MysqlQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in down_statements() {
            let sql = statement.to_string(sea_query::MysqlQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
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
        for statement in up_statements() {
            let sql = statement.to_string(sea_query::PostgresQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        for statement in down_statements() {
            let sql = statement.to_string(sea_query::PostgresQueryBuilder);
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }
}
