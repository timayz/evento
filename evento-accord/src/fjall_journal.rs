//! A disk-backed [`Journal`] over [fjall], so a node's consensus state survives a
//! real process restart (not just an in-process rebuild).
//!
//! Each [`CommandState`] is bitcode-serialized and stored keyed by its `TxnId`;
//! [`load_all`](Journal::load_all) replays them all on startup via
//! [`Node::recover_state`](crate::node::Node::recover_state).
//!
//! **Group commit.** [`stage`](Journal::stage) inserts a record without an fsync;
//! [`flush`](Journal::flush) does a single `persist(SyncAll)` covering every
//! staged insert. The node's inbox loop drains a batch of messages, stages each,
//! then flushes once — so a burst of consensus messages costs one fsync, not one
//! per message. [`record`](Journal::record) (stage + flush) remains for callers
//! that need a write durable immediately.
//!
//! [fjall]: https://crates.io/crates/fjall

use std::path::Path;

use async_trait::async_trait;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::api::Journal;
use crate::clock::{Timestamp, TxnId};
use crate::message::CommandState;

/// Key under which the truncation watermark is stored in the `meta` keyspace.
const WATERMARK_KEY: &[u8] = b"redundant_before";

/// A [`Journal`] persisting command states to a fjall database on disk.
#[derive(Clone)]
pub struct FjallJournal {
    db: Database,
    commands: Keyspace,
    /// Small keyspace for journal metadata (currently the truncation watermark).
    meta: Keyspace,
}

impl FjallJournal {
    /// Opens (creating if absent) a journal at `path`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::builder(path).open()?;
        let commands = db.keyspace("accord_commands", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("accord_meta", KeyspaceCreateOptions::default)?;
        Ok(Self { db, commands, meta })
    }
}

#[async_trait]
impl Journal for FjallJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage(state).await?;
        self.flush().await
    }

    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        let key = bitcode::serialize(&state.txn)?;
        let value = bitcode::serialize(state)?;
        let commands = self.commands.clone();
        // Insert into the keyspace (write-ahead) but do not fsync; the next
        // `flush` makes this — and every other staged write — durable at once.
        tokio::task::spawn_blocking(move || commands.insert(key, value)).await??;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.persist(PersistMode::SyncAll)).await??;
        Ok(())
    }

    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        let commands = self.commands.clone();
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Collect the redundant keys first, then remove them (serialized keys
            // are not order-preserving, so we cannot range-delete; the keyspace is
            // bounded to in-flight work in steady state, so the scan is cheap).
            let mut redundant = Vec::new();
            for guard in commands.iter() {
                let (key, _) = guard.into_inner()?;
                let txn: TxnId = bitcode::deserialize(&key)?;
                if txn.0 < before {
                    redundant.push(key);
                }
            }
            for key in redundant {
                commands.remove(key)?;
            }
            meta.insert(WATERMARK_KEY, bitcode::serialize(&before)?)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(WATERMARK_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(Some(bitcode::deserialize(&bytes)?)),
            None => Ok(None),
        }
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
