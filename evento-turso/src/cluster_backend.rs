//! [`Turso`] as a [`ClusterBackend`]: its consensus journal is a
//! [`TursoJournal`] over the same database file, and emptying the store for a
//! join drops the event, snapshot and subscription cursor tables and creates
//! them again, in one transaction, while the `cluster_*` tables stay.

use std::sync::Arc;

use async_trait::async_trait;
use evento_cluster::{ClusterBackend, Journal};
use sea_query::{Alias, Iden, Query};

use crate::query::{self, Prepared};
use crate::write::{self, WriteMode};
use crate::{Turso, TursoJournal};

/// The consensus journal's tables, as `migrations/0002_cluster.sql` creates
/// them.
const JOURNAL_TABLES: [&str; 4] = [
    "cluster_commands",
    "cluster_meta",
    "cluster_metadata_log",
    "cluster_acceptors",
];

/// What a join empties: the events, the snapshots and the subscription
/// cursors. `meta` (the commit clock) stays.
const STORE_TABLES: [&str; 3] = ["event", "snapshot", "subscriber"];

fn delete_all(table: impl Iden + 'static) -> Prepared {
    query::prepare(&Query::delete().from_table(table).to_owned())
}

/// The statements that create `table` and its indexes as the database holds
/// them — the store's migrations' and any an application added —, the table
/// first. An index the table's own constraints imply has no statement: it
/// comes back with the table.
async fn schema_of(conn: &turso::Connection, table: &str) -> anyhow::Result<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT sql FROM sqlite_schema WHERE tbl_name = ? AND sql IS NOT NULL \
             ORDER BY type = 'table' DESC, rowid",
            [table],
        )
        .await?;
    let mut schema = Vec::new();
    while let Some(row) = rows.next().await? {
        schema.push(row.get::<String>(0)?);
    }
    Ok(schema)
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

    /// Drops the tables and creates them again from the schema the database
    /// records for them, indexes included, rather than deleting their rows:
    /// in MVCC mode a delete is a new version of every row until the next
    /// checkpoint, so emptying a large store row by row wrote it once more
    /// to the log before the copy began (#510). A drop is one schema change
    /// per table, and the copy takes the pages it frees.
    async fn empty_for_join(&self) -> anyhow::Result<()> {
        let mut conn = self.connect()?;
        // DDL needs a plain transaction, in MVCC mode too (as the migration
        // runner's); a writer in the way is waited out.
        write::transaction(&mut conn, WriteMode::Serial, &(), |tx, _| {
            Box::pin(async move {
                for table in STORE_TABLES {
                    let schema = schema_of(tx, table).await?;
                    anyhow::ensure!(
                        !schema.is_empty(),
                        "the store has no `{table}` table to empty"
                    );
                    tx.execute(&format!("DROP TABLE {table}"), ()).await?;
                    for sql in schema {
                        tx.execute(&sql, ()).await?;
                    }
                }
                Ok(())
            })
        })
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
