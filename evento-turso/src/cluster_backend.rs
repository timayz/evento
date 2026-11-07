//! [`Turso`] as a [`ClusterBackend`]: its consensus journal is a
//! [`TursoJournal`] over the same database file, and emptying the store for a
//! join deletes the events, snapshots and subscription cursors in one
//! transaction while the `cluster_*` tables stay.

use std::sync::Arc;

use async_trait::async_trait;
use evento_cluster::{ClusterBackend, Journal};
use sea_query::{Alias, Iden, Query};

use crate::query::{self, EventTable, Prepared, SnapshotTable, SubscriberTable};
use crate::{Turso, TursoJournal};

/// The consensus journal's tables, as `migrations/0002_cluster.sql` creates
/// them.
const JOURNAL_TABLES: [&str; 4] = [
    "cluster_commands",
    "cluster_meta",
    "cluster_metadata_log",
    "cluster_acceptors",
];

fn delete_all(table: impl Iden + 'static) -> Prepared {
    query::prepare(&Query::delete().from_table(table).to_owned())
}

impl Turso {
    /// Runs `statements` in one transaction on an application connection,
    /// in the database's write mode.
    async fn run_all(&self, statements: Vec<Prepared>) -> anyhow::Result<()> {
        self.transaction(&statements, |tx, statements| {
            Box::pin(async move {
                for q in statements {
                    query::execute_prepared(tx, q).await?;
                }
                Ok(())
            })
        })
        .await
    }
}

#[async_trait]
impl ClusterBackend for Turso {
    async fn open_journal(&self) -> anyhow::Result<Arc<dyn Journal>> {
        Ok(Arc::new(TursoJournal::new(self.database().clone()).await?))
    }

    async fn empty_for_join(&self) -> anyhow::Result<()> {
        self.run_all(vec![
            delete_all(EventTable::Table),
            delete_all(SnapshotTable::Table),
            delete_all(SubscriberTable::Table),
        ])
        .await
    }

    async fn discard_journal(&self) -> anyhow::Result<()> {
        self.run_all(
            JOURNAL_TABLES
                .iter()
                .map(|table| delete_all(Alias::new(*table)))
                .collect(),
        )
        .await
    }
}
