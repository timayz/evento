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

use crate::api::{AcceptorRecord, Journal};
use crate::clock::{NodeId, Timestamp, TxnId};
use crate::format::{decode_tagged, encode_tagged, RecordKind};
use crate::message::CommandState;

/// Key under which the truncation watermark is stored in the `meta` keyspace.
const WATERMARK_KEY: &[u8] = b"redundant_before";
/// Prefix for metadata-log entry keys (`mlog/` + 8-byte big-endian epoch).
const METADATA_PREFIX: &[u8] = b"mlog/";
/// Prefix for acceptor-state keys (`acc/` + 8-byte big-endian epoch).
const ACCEPTOR_PREFIX: &[u8] = b"acc/";

/// Builds a `prefix`-namespaced key for `epoch`, big-endian so the keyspace's
/// lexicographic order matches epoch order (a prefix scan returns them ascending).
fn epoch_key(prefix: &[u8], epoch: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&epoch.to_be_bytes());
    key
}

/// Recovers the epoch from a `prefix`-namespaced key, if it matches.
fn epoch_of(prefix: &[u8], key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(prefix)?;
    let bytes: [u8; 8] = rest.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

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
        // Keys stay bare (their `TxnId` ordering drives the truncation scan); only the
        // value is version-tagged, since that is the schema that evolves.
        let key = bitcode::serialize(&state.txn)?;
        let value = encode_tagged(RecordKind::Command, state)?;
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
            meta.insert(
                WATERMARK_KEY,
                encode_tagged(RecordKind::Watermark, &before)?,
            )?;
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
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::Watermark, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        let key = bitcode::serialize(&txn)?;
        let commands = self.commands.clone();
        let bytes = tokio::task::spawn_blocking(move || commands.get(key)).await??;
        match bytes {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::Command, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        let commands = self.commands.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<CommandState>> {
            let mut out = Vec::new();
            for guard in commands.iter() {
                let (_, value) = guard.into_inner()?;
                out.push(decode_tagged(RecordKind::Command, &value)?);
            }
            Ok(out)
        })
        .await?
    }

    async fn append_metadata(&self, epoch: u64, layout: &[Vec<NodeId>]) -> anyhow::Result<()> {
        let key = epoch_key(METADATA_PREFIX, epoch);
        let value = encode_tagged(RecordKind::MetadataEntry, &layout.to_vec())?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Idempotent: the first decided layout for an epoch wins; a re-commit is a
            // no-op (never overwrite the durable chosen value).
            if meta.contains_key(&key)? {
                return Ok(());
            }
            meta.insert(key, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
            let mut out = Vec::new();
            for guard in meta.iter() {
                let (key, value) = guard.into_inner()?;
                if let Some(epoch) = epoch_of(METADATA_PREFIX, &key) {
                    out.push((epoch, decode_tagged(RecordKind::MetadataEntry, &value)?));
                }
            }
            // Big-endian keys already sort ascending, but be explicit.
            out.sort_by_key(|(epoch, _)| *epoch);
            Ok(out)
        })
        .await?
    }

    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        let key = epoch_key(ACCEPTOR_PREFIX, epoch);
        let value = encode_tagged(RecordKind::AcceptorState, state)?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            meta.insert(key, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
            let mut out = Vec::new();
            for guard in meta.iter() {
                let (key, value) = guard.into_inner()?;
                if let Some(epoch) = epoch_of(ACCEPTOR_PREFIX, &key) {
                    out.push((epoch, decode_tagged(RecordKind::AcceptorState, &value)?));
                }
            }
            Ok(out)
        })
        .await?
    }
}
