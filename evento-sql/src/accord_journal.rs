//! A SQL-backed [`Journal`] over `sqlx` + `sea-query`, portable across SQLite,
//! MySQL, and PostgreSQL — parity with `evento_fjall::FjallJournal` for deployments
//! that already run evento on SQL (`evento-sql`) and want their consensus state in the
//! **same** database as their events.
//!
//! Mirrors this crate's `Sql<DB>`: statements are built with `sea-query` and bound
//! via [`sea_query_sqlx::SqlxBinder`], then executed against a generic
//! [`sqlx::Pool`]. Values reuse evento-accord's tagged-bitcode encoding
//! (`evento_accord::format`), so the SQL journal shares the format-versioning /
//! upgrade story with the fjall one.
//!
//! **Group commit.** [`stage`](Journal::stage) buffers a command in memory;
//! [`flush`](Journal::flush) writes every buffered row in **one transaction**, so a
//! batch of consensus messages costs a single commit. (Staged rows become durable —
//! and query-visible — at `flush`, which satisfies the trait contract; recovery
//! reads via [`load_all`](Journal::load_all) only after a restart, i.e. post-flush.)
//!
//! A multi-shard transaction's route and a recovery promise for a transaction
//! never witnessed are rows of `accord_meta` keyed `route/` / `promise/` + the
//! hex of the order-preserving transaction key — no schema change — staged and
//! flushed with the commands, and truncated with them. A compacted multi-shard
//! transaction's decision record is a `decision/` row, staged the same way but
//! kept by `truncate` until the node drops it. The node's clock bound is the
//! `accord_meta` row `clock_bound`, written durably on its own and never
//! truncated.

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

use evento_accord::format::{decode_tagged, encode_tagged, RecordKind};
use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, ConfigEntry, DecisionRecord, FenceRecord,
    Journal, Key, NodeId, Timestamp, TxnId,
};

/// `k` value under which the truncation watermark is stored in `accord_meta`.
const WATERMARK_KEY: &str = "redundant_before";
/// `k` value under which the rejoin bounds are stored in `accord_meta`.
const REJOIN_BOUNDS_KEY: &str = "rejoin_above";
/// `k` value under which the node's clock bound is stored in `accord_meta`.
const CLOCK_BOUND_KEY: &str = "clock_bound";
/// `k` value under which the replica's amnesia fence is stored in
/// `accord_meta`. Untouched by `truncate`.
const AMNESIA_FENCE_KEY: &str = "amnesia_fence";
/// `k` value under which the replica's transition fences are stored in
/// `accord_meta`. Untouched by `truncate`.
const FENCES_KEY: &str = "fences";
/// `k` prefix of a multi-shard transaction's route in `accord_meta`.
const ROUTE_PREFIX: &str = "route/";
/// `k` prefix of a blind recovery promise in `accord_meta`.
const PROMISE_PREFIX: &str = "promise/";
/// `k` prefix of a decision record in `accord_meta` (49 characters with the
/// key). Untouched by `truncate`.
const DECISION_PREFIX: &str = "decision/";

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

/// The `accord_meta` key of a per-transaction record: `prefix` + the hex of
/// [`txn_key`] (46/48 characters, under the 64 of the `k` column).
fn meta_txn_key(prefix: &str, txn: TxnId) -> String {
    let mut key = String::with_capacity(prefix.len() + 40);
    key.push_str(prefix);
    for byte in txn_key(txn) {
        key.push_str(&format!("{byte:02x}"));
    }
    key
}

/// A `LIKE` pattern matching every `accord_meta` key under `prefix`.
fn like_prefix(prefix: &str) -> String {
    let mut pattern = prefix.to_string();
    pattern.push('%');
    pattern
}

/// A [`Journal`] persisting consensus state to a SQL database via `sqlx`.
pub struct SqlJournal<DB: Database> {
    pool: Pool<DB>,
    /// Commands staged since the last [`flush`](Journal::flush), as `(key, value)`.
    staged: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    /// `accord_meta` rows (routes, promises, decision records) staged since
    /// the last flush, as `(txn, k, v)`.
    staged_meta: Mutex<Vec<(TxnId, String, Vec<u8>)>>,
    /// Serializes flushes so staged entries are only drained once their
    /// transaction has committed — a failed flush must leave them staged.
    flush_lock: tokio::sync::Mutex<()>,
}

impl<DB: Database> SqlJournal<DB> {
    /// Wraps a `sqlx` pool as a journal. Call [`migrate`](Self::migrate) once to
    /// create the tables.
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            pool,
            staged: Mutex::new(Vec::new()),
            staged_meta: Mutex::new(Vec::new()),
            flush_lock: tokio::sync::Mutex::new(()),
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
    /// The value stored under `key` in `accord_meta`, if any.
    async fn load_meta(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let statement = Query::select()
            .column(AccordMeta::V)
            .from(AccordMeta::Table)
            .and_where(Expr::col(AccordMeta::K).eq(Expr::value(key)))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let row =
            sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(bytes,)| bytes))
    }

    /// The values of every `accord_meta` row whose key starts with `prefix`.
    async fn load_meta_prefixed(
        conn: &mut DB::Connection,
        prefix: &str,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let statement = Query::select()
            .column(AccordMeta::V)
            .from(AccordMeta::Table)
            .and_where(Expr::col(AccordMeta::K).like(like_prefix(prefix)))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let rows =
            sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&mut *conn)
                .await?;
        Ok(rows.into_iter().map(|(bytes,)| bytes).collect())
    }

    /// Every persisted route, read over `conn`.
    async fn load_routes_in(
        &self,
        conn: &mut DB::Connection,
    ) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        Self::load_meta_prefixed(conn, ROUTE_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Route, bytes))
            .collect()
    }

    /// Every persisted blind promise, read over `conn`.
    async fn load_promises_in(
        &self,
        conn: &mut DB::Connection,
    ) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        Self::load_meta_prefixed(conn, PROMISE_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Promise, bytes))
            .collect()
    }

    /// Creates the journal's tables if absent (idempotent) — a self-contained
    /// convenience for standalone/test use.
    ///
    /// The **production** path is `evento-sql-migrator` with its `accord` feature
    /// (versioned, reversible, tracked migrations alongside the event schema). This
    /// method and that migration **must produce the same schema** — the table and
    /// column names and types here are the contract (`VARBINARY(20)` txn key,
    /// `VARCHAR(64)` meta key, so a BLOB/TEXT primary key never breaks MySQL).
    pub async fn migrate(&self) -> anyhow::Result<()> {
        let tables = [
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
        ];
        for table in &tables {
            let sql = Self::build_ddl(table);
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(&self.pool)
                .await?;
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
        // Serialize flushes; entries leave `staged` only after their
        // transaction commits, so a failed flush keeps the batch for retry and
        // a concurrent `record` cannot observe "flushed" state that was never
        // made durable.
        let _guard = self.flush_lock.lock().await;

        let batch = self.staged.lock().expect("journal poisoned").clone();
        let meta_batch = self.staged_meta.lock().expect("journal poisoned").clone();
        if batch.is_empty() && meta_batch.is_empty() {
            return Ok(());
        }

        // A command's state advances and is re-staged under the same txn, and a
        // multi-row upsert must carry each key at most once (Postgres rejects
        // `ON CONFLICT DO UPDATE` affecting one row twice) — dedupe keeping the
        // LAST staged value, which is what the per-row upserts converged to.
        let mut seen = std::collections::HashSet::new();
        let mut rows: Vec<&(Vec<u8>, Vec<u8>)> = batch
            .iter()
            .rev()
            .filter(|(key, _)| seen.insert(key.as_slice()))
            .collect();
        rows.reverse();

        // One multi-row upsert per chunk, all chunks in one transaction: a
        // batch of N staged commands costs one commit and ⌈N/8000⌉ statements
        // instead of N round trips. 2 binds per row keeps a chunk well under
        // every backend's bind limit (SQLite 32766, Postgres/MySQL 65535).
        const ROWS_PER_STATEMENT: usize = 8_000;
        let mut tx = self.pool.begin().await?;
        for chunk in rows.chunks(ROWS_PER_STATEMENT) {
            let mut statement = Query::insert()
                .into_table(AccordCommands::Table)
                .columns([AccordCommands::Txn, AccordCommands::Data])
                .on_conflict(
                    OnConflict::column(AccordCommands::Txn)
                        .update_column(AccordCommands::Data)
                        .to_owned(),
                )
                .to_owned();
            for (key, value) in chunk {
                statement.values_panic([key.clone().into(), value.clone().into()]);
            }
            let (sql, values) = Self::build_sqlx(&statement);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await?;
        }
        // The staged `accord_meta` rows, deduped the same way (last wins).
        let mut seen = std::collections::HashSet::new();
        let mut meta_rows: Vec<&(TxnId, String, Vec<u8>)> = meta_batch
            .iter()
            .rev()
            .filter(|(_, key, _)| seen.insert(key.as_str()))
            .collect();
        meta_rows.reverse();
        for chunk in meta_rows.chunks(ROWS_PER_STATEMENT) {
            let mut statement = Query::insert()
                .into_table(AccordMeta::Table)
                .columns([AccordMeta::K, AccordMeta::V])
                .on_conflict(
                    OnConflict::column(AccordMeta::K)
                        .update_column(AccordMeta::V)
                        .to_owned(),
                )
                .to_owned();
            for (_, key, value) in chunk {
                statement.values_panic([key.clone().into(), value.clone().into()]);
            }
            let (sql, values) = Self::build_sqlx(&statement);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;

        // Drop exactly what was flushed; entries staged mid-transaction stay.
        // By entry, not by position: a concurrent `truncate` may have purged
        // part of `staged` while the transaction ran, so the batch is no
        // longer a prefix of it (and draining `batch.len()` entries would
        // overrun — or worse, drop entries never flushed).
        let flushed: std::collections::HashSet<&(Vec<u8>, Vec<u8>)> = batch.iter().collect();
        self.staged
            .lock()
            .expect("journal poisoned")
            .retain(|entry| !flushed.contains(entry));
        let flushed: std::collections::HashSet<&(TxnId, String, Vec<u8>)> =
            meta_batch.iter().collect();
        self.staged_meta
            .lock()
            .expect("journal poisoned")
            .retain(|entry| !flushed.contains(entry));
        Ok(())
    }

    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        let bound = ts_key(before);
        let watermark = encode_tagged(RecordKind::Watermark, &before)?;

        // Also purge staged (unflushed) commands below the bound, or a later
        // flush would resurrect rows this truncation just deleted.
        self.staged
            .lock()
            .expect("journal poisoned")
            .retain(|(key, _)| key.as_slice() >= bound.as_slice());
        // Decision records outlive their commands.
        self.staged_meta
            .lock()
            .expect("journal poisoned")
            .retain(|(txn, key, _)| txn.0 >= before || key.starts_with(DECISION_PREFIX));

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

        // Routes and blind promises go with the commands — selected by key
        // prefix, compared by the transaction their value carries (never by
        // the column's collation).
        let mut redundant: Vec<String> = Vec::new();
        for (txn, _) in self.load_routes_in(&mut *tx).await? {
            if txn.0 < before {
                redundant.push(meta_txn_key(ROUTE_PREFIX, txn));
            }
        }
        for (txn, _) in self.load_promises_in(&mut *tx).await? {
            if txn.0 < before {
                redundant.push(meta_txn_key(PROMISE_PREFIX, txn));
            }
        }
        for chunk in redundant.chunks(8_000) {
            let delete = Query::delete()
                .from_table(AccordMeta::Table)
                .and_where(Expr::col(AccordMeta::K).is_in(chunk.iter().cloned()))
                .to_owned();
            let (sql, values) = Self::build_sqlx(&delete);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await?;
        }

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
        match self.load_meta(WATERMARK_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::Watermark, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        if bounds.is_empty() {
            let delete = Query::delete()
                .from_table(AccordMeta::Table)
                .and_where(Expr::col(AccordMeta::K).eq(Expr::value(REJOIN_BOUNDS_KEY)))
                .to_owned();
            let (sql, values) = Self::build_sqlx(&delete);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&self.pool)
                .await?;
            return Ok(());
        }
        let value = encode_tagged(RecordKind::RejoinBounds, &bounds.to_vec())?;
        let upsert = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .values_panic([REJOIN_BOUNDS_KEY.into(), value.into()])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&upsert);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        match self.load_meta(REJOIN_BOUNDS_KEY).await? {
            Some(bytes) => Ok(decode_tagged(RecordKind::RejoinBounds, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::ClockBound, &micros)?;
        let upsert = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .values_panic([CLOCK_BOUND_KEY.into(), value.into()])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&upsert);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        match self.load_meta(CLOCK_BOUND_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::ClockBound, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::AmnesiaFence, fence)?;
        let upsert = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .values_panic([AMNESIA_FENCE_KEY.into(), value.into()])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&upsert);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
        match self.load_meta(AMNESIA_FENCE_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::AmnesiaFence, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_fences(&self, fences: &[FenceRecord]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Fences, &fences.to_vec())?;
        let upsert = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .values_panic([FENCES_KEY.into(), value.into()])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&upsert);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_fences(&self) -> anyhow::Result<Vec<FenceRecord>> {
        match self.load_meta(FENCES_KEY).await? {
            Some(bytes) => Ok(decode_tagged(RecordKind::Fences, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_route(&self, txn: TxnId, route: &[Key]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Route, &(txn, route.to_vec()))?;
        self.staged_meta.lock().expect("journal poisoned").push((
            txn,
            meta_txn_key(ROUTE_PREFIX, txn),
            value,
        ));
        Ok(())
    }

    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        let mut conn = self.pool.acquire().await?;
        self.load_routes_in(&mut *conn).await
    }

    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Promise, &(txn, ballot))?;
        self.staged_meta.lock().expect("journal poisoned").push((
            txn,
            meta_txn_key(PROMISE_PREFIX, txn),
            value,
        ));
        Ok(())
    }

    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        let mut conn = self.pool.acquire().await?;
        self.load_promises_in(&mut *conn).await
    }

    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Decision, record)?;
        self.staged_meta.lock().expect("journal poisoned").push((
            record.txn,
            meta_txn_key(DECISION_PREFIX, record.txn),
            value,
        ));
        Ok(())
    }

    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        let mut conn = self.pool.acquire().await?;
        Self::load_meta_prefixed(&mut *conn, DECISION_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Decision, bytes))
            .collect()
    }

    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        let keys: Vec<String> = txns
            .iter()
            .map(|txn| meta_txn_key(DECISION_PREFIX, *txn))
            .collect();
        {
            let dropped: std::collections::HashSet<&str> =
                keys.iter().map(String::as_str).collect();
            self.staged_meta
                .lock()
                .expect("journal poisoned")
                .retain(|(_, key, _)| !dropped.contains(key.as_str()));
        }
        // A flush already under way may still write one back: it is then
        // only kept longer, and dropped again after a restart.
        for chunk in keys.chunks(8_000) {
            let delete = Query::delete()
                .from_table(AccordMeta::Table)
                .and_where(Expr::col(AccordMeta::K).is_in(chunk.iter().cloned()))
                .to_owned();
            let (sql, values) = Self::build_sqlx(&delete);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        let statement = Query::select()
            .column(AccordCommands::Data)
            .from(AccordCommands::Table)
            .and_where(Expr::col(AccordCommands::Txn).eq(Expr::value(txn_key(txn))))
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        let row =
            sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
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
        let rows =
            sqlx::query_as_with::<DB, (Vec<u8>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(bytes,)| decode_tagged(RecordKind::Command, &bytes))
            .collect()
    }

    async fn append_metadata(&self, epoch: u64, entry: &ConfigEntry) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::MetadataEntry, entry)?;
        // Idempotent: the first decided entry for an epoch wins.
        let statement = Query::insert()
            .into_table(AccordMetadataLog::Table)
            .columns([AccordMetadataLog::Epoch, AccordMetadataLog::Layout])
            .values_panic([(epoch as i64).into(), value.into()])
            // `do_nothing_on` (not bare `do_nothing`) so MySQL emits a valid
            // `ON DUPLICATE KEY UPDATE epoch = epoch` no-op rather than the invalid
            // `ON DUPLICATE KEY IGNORE`; Postgres/SQLite still render `DO NOTHING`.
            .on_conflict(
                OnConflict::column(AccordMetadataLog::Epoch)
                    .do_nothing_on([AccordMetadataLog::Epoch])
                    .to_owned(),
            )
            .to_owned();
        let (sql, values) = Self::build_sqlx(&statement);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn append_metadata_batch(&self, entries: &[(u64, ConfigEntry)]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // One multi-row insert with the same first-decided-wins conflict
        // handling as `append_metadata`. Entries arrive as distinct ascending
        // epochs; dedupe defensively anyway (first wins) so a malformed batch
        // cannot break the multi-row statement.
        let mut seen = std::collections::HashSet::new();
        let mut statement = Query::insert()
            .into_table(AccordMetadataLog::Table)
            .columns([AccordMetadataLog::Epoch, AccordMetadataLog::Layout])
            .on_conflict(
                OnConflict::column(AccordMetadataLog::Epoch)
                    .do_nothing_on([AccordMetadataLog::Epoch])
                    .to_owned(),
            )
            .to_owned();
        for (epoch, entry) in entries {
            if !seen.insert(*epoch) {
                continue;
            }
            let value = encode_tagged(RecordKind::MetadataEntry, entry)?;
            statement.values_panic([(*epoch as i64).into(), value.into()]);
        }
        let (sql, values) = Self::build_sqlx(&statement);
        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
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
            .map(|(epoch, bytes)| {
                Ok((
                    epoch as u64,
                    decode_tagged(RecordKind::MetadataEntry, &bytes)?,
                ))
            })
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
            .map(|(epoch, bytes)| {
                Ok((
                    epoch as u64,
                    decode_tagged(RecordKind::AcceptorState, &bytes)?,
                ))
            })
            .collect()
    }
}
