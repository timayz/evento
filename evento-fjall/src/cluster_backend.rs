//! [`Fjall`] as a [`ClusterBackend`]: its consensus journal is a
//! [`FjallJournal`] inside the same database (fjall locks a database per
//! directory, so the two cannot be opened apart), and emptying the store for
//! a join clears the event, index, cursor, snapshot and dead-letter keyspaces while the
//! `cluster_*` keyspaces stay.

use std::sync::Arc;

use async_trait::async_trait;
use evento_cluster::{ClusterBackend, Journal};
use fjall::{Keyspace, PersistMode};

use crate::{Fjall, FjallJournal};

/// Clears `keyspaces` and makes the clearing durable, off the async runtime.
async fn clear_all(db: fjall::Database, keyspaces: Vec<Keyspace>) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        for keyspace in &keyspaces {
            keyspace.clear()?;
        }
        db.persist(PersistMode::SyncAll)?;
        Ok(())
    })
    .await?
}

#[async_trait]
impl ClusterBackend for Fjall {
    async fn open_journal(&self) -> anyhow::Result<Arc<dyn Journal>> {
        Ok(Arc::new(FjallJournal::from_database(self.db.clone())?))
    }

    async fn empty_for_join(&self) -> anyhow::Result<()> {
        // `meta` stays: the commit clock and the index version are this
        // store's own, not derived from the events.
        clear_all(
            self.db.clone(),
            vec![
                self.events.clone(),
                self.agg_index.clone(),
                self.cursor_all.clone(),
                self.cursor_agg.clone(),
                self.cursor_agg_name.clone(),
                self.cursor_type.clone(),
                self.cursor_type_name.clone(),
                self.cursor_type_tenant.clone(),
                self.cursor_type_name_tenant.clone(),
                self.cursor_routing.clone(),
                self.subscribers.clone(),
                self.snapshots.clone(),
                // With the cursors: the subscriptions start over.
                self.dead_letters.clone(),
            ],
        )
        .await
    }

    async fn discard_journal(&self) -> anyhow::Result<()> {
        let journal = FjallJournal::from_database(self.db.clone())?;
        clear_all(self.db.clone(), journal.keyspaces()).await
    }
}
