//! SQL-backed durable storage for crash recovery.
//!
//! This module provides `SqlDurableStore`, an implementation of `DurableStore`
//! that persists transaction state to SQL databases (SQLite, MySQL, PostgreSQL).

use crate::error::Result;
use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};
use async_trait::async_trait;
use evento_sql::AccordTxn;
use sea_query::{Expr, ExprTrait, OnConflict, Query};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Database, Pool};

use super::DurableStore;

/// SQL-backed durable store for ACCORD transactions.
///
/// Persists executed transaction IDs to the database so nodes can recover
/// their state after a crash. When a node restarts, it loads the executed
/// transaction IDs and skips re-executing them.
///
/// # Example
///
/// ```ignore
/// use evento_accord::SqlDurableStore;
/// use sqlx::sqlite::SqlitePoolOptions;
///
/// let pool = SqlitePoolOptions::new()
///     .connect("sqlite:my_db.sqlite")
///     .await?;
///
/// let durable_store = SqlDurableStore::new(pool);
/// ```
pub struct SqlDurableStore<DB: Database> {
    pool: Pool<DB>,
}

impl<DB: Database> SqlDurableStore<DB> {
    /// Create a new SQL durable store backed by the given connection pool.
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    fn build_sqlx<S: SqlxBinder>(statement: S) -> (String, sea_query_sqlx::SqlxValues) {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => statement.build_sqlx(sea_query::SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            "MySQL" => statement.build_sqlx(sea_query::MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => statement.build_sqlx(sea_query::PostgresQueryBuilder),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }
}

impl<DB: Database> Clone for SqlDurableStore<DB> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

#[async_trait]
impl<DB> DurableStore for SqlDurableStore<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i32: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    async fn persist(&self, txn: &Transaction) -> Result<()> {
        // We only persist when transaction reaches Executed or ExecutionFailed status
        if !txn.status.is_terminal() {
            return Ok(());
        }

        let statement = Query::insert()
            .into_table(AccordTxn::Table)
            .columns([AccordTxn::TxnId, AccordTxn::Status])
            .values_panic([txn.id.to_string().into(), (txn.status as i32).into()])
            .on_conflict(
                OnConflict::column(AccordTxn::TxnId)
                    .update_column(AccordTxn::Status)
                    .value(AccordTxn::ExecutedAt, Expr::current_timestamp())
                    .to_owned(),
            )
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.pool)
            .await
            .map_err(|e| crate::error::AccordError::Internal(anyhow::anyhow!("{}", e)))?;

        Ok(())
    }

    async fn load_committed(&self) -> Result<Vec<Transaction>> {
        // We don't store full transactions, just IDs and status.
        // This is sufficient for our use case - we just need to know
        // which transactions have been executed.
        //
        // For full transaction recovery, we would need to store the
        // events_data, keys, deps, etc. But for now, we only need
        // to track executed IDs to prevent re-execution.
        Ok(vec![])
    }

    async fn get_last_executed(&self) -> Result<Option<TxnId>> {
        // Query the most recently executed transaction by executed_at timestamp
        let statement = Query::select()
            .columns([AccordTxn::TxnId])
            .from(AccordTxn::Table)
            .and_where(Expr::col(AccordTxn::Status).eq(TxnStatus::Executed as i32))
            .order_by(AccordTxn::ExecutedAt, sea_query::Order::Desc)
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let result = sqlx::query_as_with::<DB, (String,), _>(&sql, values)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| crate::error::AccordError::Internal(anyhow::anyhow!("{}", e)))?;

        match result {
            Some((txn_id_str,)) => {
                let txn_id = txn_id_str.parse::<TxnId>().map_err(|e| {
                    crate::error::AccordError::Internal(anyhow::anyhow!(
                        "invalid txn_id in database: {}",
                        e
                    ))
                })?;
                Ok(Some(txn_id))
            }
            None => Ok(None),
        }
    }

    async fn mark_executed(&self, txn_id: TxnId) -> Result<()> {
        // Create a minimal transaction just to persist the executed status
        let txn = Transaction {
            id: txn_id,
            events_data: vec![],
            keys: vec![],
            deps: vec![],
            status: TxnStatus::Executed,
            execute_at: txn_id.timestamp,
            ballot: Ballot::initial(0),
        };
        self.persist(&txn).await
    }
}

/// Load all executed transaction IDs from the database.
///
/// This is used during node startup to populate the ConsensusState
/// with already-executed transaction IDs, preventing re-execution.
pub async fn load_executed_txn_ids<DB>(pool: &Pool<DB>) -> Result<Vec<TxnId>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let statement = Query::select()
        .columns([AccordTxn::TxnId])
        .from(AccordTxn::Table)
        .to_owned();

    let (sql, values) = match DB::NAME {
        #[cfg(feature = "sqlite")]
        "SQLite" => statement.build_sqlx(sea_query::SqliteQueryBuilder),
        #[cfg(feature = "mysql")]
        "MySQL" => statement.build_sqlx(sea_query::MysqlQueryBuilder),
        #[cfg(feature = "postgres")]
        "PostgreSQL" => statement.build_sqlx(sea_query::PostgresQueryBuilder),
        name => panic!("'{name}' not supported"),
    };

    let rows = sqlx::query_as_with::<DB, (String,), _>(&sql, values)
        .fetch_all(pool)
        .await
        .map_err(|e| crate::error::AccordError::Internal(anyhow::anyhow!("{}", e)))?;

    let mut txn_ids = Vec::with_capacity(rows.len());
    for (txn_id_str,) in rows {
        if let Ok(txn_id) = txn_id_str.parse::<TxnId>() {
            txn_ids.push(txn_id);
        }
    }

    Ok(txn_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_status_as_i32() {
        assert_eq!(TxnStatus::Executed as i32, 3);
        assert_eq!(TxnStatus::ExecutionFailed as i32, 4);
    }
}
