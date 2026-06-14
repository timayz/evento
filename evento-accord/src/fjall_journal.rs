//! A disk-backed [`Journal`] over [fjall], so a node's consensus state survives a
//! real process restart (not just an in-process rebuild).
//!
//! Each [`CommandState`] is bitcode-serialized and stored keyed by its `TxnId`;
//! [`load_all`](Journal::load_all) replays them all on startup via
//! [`Node::recover_state`](crate::node::Node::recover_state). Writes are
//! fsync'd on every record — durable but slow; production should batch these
//! (group commit) rather than sync per consensus message.
//!
//! [fjall]: https://crates.io/crates/fjall

use std::path::Path;

use async_trait::async_trait;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::api::Journal;
use crate::clock::TxnId;
use crate::message::CommandState;

/// A [`Journal`] persisting command states to a fjall database on disk.
#[derive(Clone)]
pub struct FjallJournal {
    db: Database,
    commands: Keyspace,
}

impl FjallJournal {
    /// Opens (creating if absent) a journal at `path`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::builder(path).open()?;
        let commands = db.keyspace("accord_commands", KeyspaceCreateOptions::default)?;
        Ok(Self { db, commands })
    }
}

#[async_trait]
impl Journal for FjallJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        let key = bitcode::serialize(&state.txn)?;
        let value = bitcode::serialize(state)?;
        let commands = self.commands.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            commands.insert(key, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        let key = bitcode::serialize(&txn)?;
        let commands = self.commands.clone();
        let bytes = tokio::task::spawn_blocking(move || commands.get(key)).await??;
        match bytes {
            Some(bytes) => Ok(Some(bitcode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        let commands = self.commands.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<CommandState>> {
            let mut out = Vec::new();
            for guard in commands.iter() {
                let (_, value) = guard.into_inner()?;
                out.push(bitcode::deserialize(&value)?);
            }
            Ok(out)
        })
        .await?
    }
}
