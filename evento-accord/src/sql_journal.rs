//! A SQL-backed [`Journal`] over `sqlx` + `sea-query`, portable across SQLite,
//! MySQL, and PostgreSQL — parity with [`FjallJournal`](crate::fjall_journal::FjallJournal)
//! for deployments that already run evento on SQL (`evento-sql`) and want their
//! consensus state in the **same** database as their events.
//!
//! Mirrors `evento-sql`'s `Sql<DB>`: statements are built with `sea-query` and bound
//! via [`sea_query_sqlx::SqlxBinder`], then executed against a generic
//! [`sqlx::Pool`]. Values reuse the crate's tagged-bitcode encoding
//! ([`crate::format`]), so the SQL journal shares the format-versioning / upgrade
//! story with the fjall one.
//!
//! **Group commit.** [`stage`](Journal::stage) buffers a command in memory;
//! [`flush`](Journal::flush) writes every buffered row in **one transaction**, so a
//! batch of consensus messages costs a single commit. (Staged rows become durable —
//! and query-visible — at `flush`, which satisfies the trait contract; recovery
//! reads via [`load_all`](Journal::load_all) only after a restart, i.e. post-flush.)

use std::sync::Mutex;

#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, Query, Table};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Database, Pool};

use async_trait::async_trait;

use crate::api::{AcceptorRecord, Journal};
use crate::clock::{NodeId, Timestamp, TxnId};
use crate::format::{decode_tagged, encode_tagged, RecordKind};
use crate::message::CommandState;

/// `k` value under which the truncation watermark is stored in `accord_meta`.
const WATERMARK_KEY: &str = "redundant_before";

#[derive(Iden)]
enum AccordCommands {
    Table,
    Txn,
    Data,
}

#[derive(Iden)]
enum AccordMeta {
    Table,
    K,
    V,
}

#[derive(Iden)]
enum AccordMetadataLog {
    Table,
    Epoch,
    Layout,
}

#[derive(Iden)]
enum AccordAcceptors {
    Table,
    Epoch,
    State,
}

/// An order-preserving key for a timestamp: big-endian `(micros, logical, node)`,
/// matching [`Timestamp`]'s field-order `Ord`, so a byte-wise `<` on the BLOB key
/// equals `<` on the timestamp — letting `truncate` be a single range `DELETE`.
fn ts_key(ts: Timestamp) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&ts.micros.to_be_bytes());
    key.extend_from_slice(&ts.logical.to_be_bytes());
    key.extend_from_slice(&ts.node.0.to_be_bytes());
    key
}

fn txn_key(txn: TxnId) -> Vec<u8> {
    ts_key(txn.0)
}

/// A [`Journal`] persisting consensus state to a SQL database via `sqlx`.
pub struct SqlJournal<DB: Database> {
    pool: Pool<DB>,
    /// Commands staged since the last [`flush`](Journal::flush), as `(key, value)`.
    staged: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl<DB: Database> SqlJournal<DB> {
    /// Wraps a `sqlx` pool as a journal. Call [`migrate`](Self::migrate) once to
    /// create the tables.
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            pool,
            staged: Mutex::new(Vec::new()),
        }
    }

    /// Closes the underlying pool, waiting for connections to finish — a clean
    /// shutdown (so the next open sees a fully-flushed database).
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Renders a value-bound statement for this pool's dialect.
    fn build_sqlx<S: SqlxBinder>(statement: &S) -> (String, sea_query_sqlx::SqlxValues) {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }

    /// Renders a (value-free) schema statement for this pool's dialect.
    fn build_ddl(statement: &sea_query::TableCreateStatement) -> String {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => statement.to_string(SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            "MySQL" => statement.to_string(MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => statement.to_string(PostgresQueryBuilder),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }
}

impl<DB> SqlJournal<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    /// Creates the journal's tables if absent (idempotent). Safe to call on every
    /// startup.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        let tables = [
            Table::create()
                .table(AccordCommands::Table)
                .if_not_exists()
                .col(ColumnDef::new(AccordCommands::Txn).blob().primary_key())
                .col(ColumnDef::new(AccordCommands::Data).blob().not_null())
                .to_owned(),
            Table::create()
                .table(AccordMeta::Table)
                .if_not_exists()
                .col(ColumnDef::new(AccordMeta::K).text().primary_key())
                .col(ColumnDef::new(AccordMeta::V).blob().not_null())
                .to_owned(),
            Table::create()
                .table(AccordMetadataLog::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(AccordMetadataLog::Epoch)
                        .big_integer()
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
                        .primary_key(),
                )
                .col(ColumnDef::new(AccordAcceptors::State).blob().not_null())
                .to_owned(),
        ];
        for table in &tables {
            let sql = Self::build_ddl(table);
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql)).execute(&self.pool).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<DB> Journal for SqlJournal<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage(state).await?;
        self.flush().await
    }

    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        let key = txn_key(state.txn);
        let value = encode_tagged(RecordKind::Command, state)?;
        self.staged
            .lock()
            .expect("journal poisoned")
            .push((key, value));
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let batch = std::mem::take(&mut *self.staged.lock().expect("journal poisoned"));
        if batch.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        for (key, value) in batch {
            // Upsert: a command's state advances and is re-recorded under its txn.
            let statement = Query::insert()
                .into_table(AccordCommands::Table)
                .columns([AccordCommands::Txn, AccordCommands::Data])
                .values_panic([key.into(), value.into()])
                .on_conflict(
                    OnConflict::column(AccordCommands::Txn)
                        .update_column(AccordCommands::Data)
                        .to_owned(),
                )
                .to_owned();
            let (sql, values) = Self::build_sqlx(&statement);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        let bound = ts_key(before);
        let watermark = encode_tagged(RecordKind::Watermark, &before)?;
        let mut tx = self.pool.begin().await?;

        // Single range delete — the order-preserving key makes `txn < before`
        // a byte-wise comparison the database can do directly.
        let delete = Query::delete()
            .from_table(AccordCommands::Table)
            .and_where(Expr::col(AccordCommands::Txn).lt(Expr::value(bound)))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&delete);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&mut *tx)
            .await?;

        let upsert = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .values_panic([WATERMARK_KEY.into(), watermark.into()])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&upsert);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        let statement = Query::select()
            .column(AccordMeta::V)
            .from(AccordMeta::Table)
            .and_where(Expr::col(AccordMeta::K).eq(Expr::value(WATERMARK_KEY)))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let row = sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some((bytes,)) => Ok(Some(decode_tagged(RecordKind::Watermark, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        let statement = Query::select()
            .column(AccordCommands::Data)
            .from(AccordCommands::Table)
            .and_where(Expr::col(AccordCommands::Txn).eq(Expr::value(txn_key(txn))))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let row = sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some((bytes,)) => Ok(Some(decode_tagged(RecordKind::Command, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        let statement = Query::select()
            .column(AccordCommands::Data)
            .from(AccordCommands::Table)
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let rows = sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|(bytes,)| decode_tagged(RecordKind::Command, &bytes))
            .collect()
    }

    async fn append_metadata(&self, epoch: u64, layout: &[Vec<NodeId>]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::MetadataEntry, &layout.to_vec())?;
        // Idempotent: the first decided layout for an epoch wins.
        let statement = Query::insert()
            .into_table(AccordMetadataLog::Table)
            .columns([AccordMetadataLog::Epoch, AccordMetadataLog::Layout])
            .values_panic([(epoch as i64).into(), value.into()])
            .on_conflict(OnConflict::column(AccordMetadataLog::Epoch).do_nothing().to_owned())
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
        let statement = Query::select()
            .columns([AccordMetadataLog::Epoch, AccordMetadataLog::Layout])
            .from(AccordMetadataLog::Table)
            .order_by(AccordMetadataLog::Epoch, sea_query::Order::Asc)
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let rows =
            sqlx::query_as_with::<DB, (i64, Vec<u8>), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(epoch, bytes)| Ok((epoch as u64, decode_tagged(RecordKind::MetadataEntry, &bytes)?)))
            .collect()
    }

    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::AcceptorState, state)?;
        let statement = Query::insert()
            .into_table(AccordAcceptors::Table)
            .columns([AccordAcceptors::Epoch, AccordAcceptors::State])
            .values_panic([(epoch as i64).into(), value.into()])
            .on_conflict(
                OnConflict::column(AccordAcceptors::Epoch)
                    .update_column(AccordAcceptors::State)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        let statement = Query::select()
            .columns([AccordAcceptors::Epoch, AccordAcceptors::State])
            .from(AccordAcceptors::Table)
            .order_by(AccordAcceptors::Epoch, sea_query::Order::Asc)
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let rows =
            sqlx::query_as_with::<DB, (i64, Vec<u8>), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(epoch, bytes)| Ok((epoch as u64, decode_tagged(RecordKind::AcceptorState, &bytes)?)))
            .collect()
    }
}
