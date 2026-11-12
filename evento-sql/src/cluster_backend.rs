//! [`Sql`] as a [`ClusterBackend`]: its consensus journal is a [`SqlJournal`]
//! on the same pool, and emptying the store for a join deletes the events,
//! snapshots and subscription cursors in one transaction while the
//! `cluster_*` tables stay.

use std::sync::Arc;

use async_trait::async_trait;
use evento_cluster::{ClusterBackend, Journal};
use sea_query::{Iden, Query};
use sqlx::Database;

use crate::cluster_journal::{
    ClusterAcceptors, ClusterCommands, ClusterMeta, ClusterMetadataLog, SqlJournal,
};
use crate::dialect::build_sqlx;
use crate::sql::{DeadLetter, Event, RowsAffected, Snapshot, Sql, SqlEvent, Subscriber};

/// The table names, taken before any await: a `dyn Iden` is not `Sync`.
fn names(tables: &[&dyn Iden]) -> Vec<String> {
    tables.iter().map(|table| table.to_string()).collect()
}

impl<DB> Sql<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
{
    /// Deletes every row of the tables named, in one transaction.
    async fn delete_all(&self, tables: Vec<String>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        for table in tables {
            let statement = Query::delete()
                .from_table(sea_query::Alias::new(table))
                .to_owned();
            let (sql, values) = build_sqlx::<DB, _>(&statement);
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl<DB> ClusterBackend for Sql<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    bool: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
    DB::QueryResult: RowsAffected,
{
    async fn open_journal(&self) -> anyhow::Result<Arc<dyn Journal>> {
        let journal = SqlJournal::new(self.pool.clone());
        journal.migrate().await?;
        Ok(Arc::new(journal))
    }

    async fn empty_for_join(&self) -> anyhow::Result<()> {
        self.delete_all(names(&[
            &Event::Table,
            &Snapshot::Table,
            &Subscriber::Table,
            // With the cursors: the subscriptions start over, and give up
            // again on what they must.
            &DeadLetter::Table,
        ]))
        .await
    }

    async fn discard_journal(&self) -> anyhow::Result<()> {
        self.delete_all(names(&[
            &ClusterCommands::Table,
            &ClusterMeta::Table,
            &ClusterMetadataLog::Table,
            &ClusterAcceptors::Table,
        ]))
        .await
    }
}
