//! A disk-backed [`Journal`] over [fjall], so an evento-accord node's consensus state
//! survives a real process restart (not just an in-process rebuild).
//!
//! Each [`CommandState`] is bitcode-serialized and stored keyed by its `TxnId`;
//! [`load_all`](Journal::load_all) replays them all on startup via
//! `Node::recover_state`. A multi-shard transaction's route and a recovery
//! promise for a transaction never witnessed live in the `meta` keyspace under
//! `route/` / `promise/` + the order-preserving transaction id, staged like a
//! command and truncated with the commands. A compacted multi-shard
//! transaction's decision record lives under `decision/`, staged the same way
//! but kept by `truncate` until the node drops it. The node's clock bound is the
//! `meta` key `clock_bound`, persisted on its own and never truncated. The
//! replica's vote fence is the `meta` key `vote_fence`, staged like a command
//! and never truncated.
//!
//! **Group commit.** [`stage`](Journal::stage) inserts a record without an fsync;
//! [`flush`](Journal::flush) does a single `persist(SyncAll)` covering every staged
//! insert. The node's inbox loop drains a batch of messages, stages each, then flushes
//! once — so a burst of consensus messages costs one fsync, not one per message.
//! [`record`](Journal::record) (stage + flush) remains for callers that need a write
//! durable immediately.
//!
//! [fjall]: https://crates.io/crates/fjall

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use evento_accord::format::{decode_tagged, encode_tagged, RecordKind};
use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, ConfigEntry, DecisionRecord, FenceRecord,
    Journal, Key, NodeId, Timestamp, TxnId,
};

/// Key under which the truncation watermark is stored in the `meta` keyspace.
const WATERMARK_KEY: &[u8] = b"redundant_before";
/// Meta key under which the rejoin bounds are stored.
const REJOIN_BOUNDS_KEY: &[u8] = b"rejoin_above";
/// Meta key under which the node's clock bound is stored.
const CLOCK_BOUND_KEY: &[u8] = b"clock_bound";
/// Meta key under which the replica's amnesia fence is stored. Untouched by
/// `truncate`.
const AMNESIA_FENCE_KEY: &[u8] = b"amnesia_fence";
/// Meta key under which the replica's transition fences are stored. Untouched
/// by `truncate`.
const FENCES_KEY: &[u8] = b"fences";
/// Meta key under which the replica's vote fence is stored. Staged; untouched
/// by `truncate`.
const VOTE_FENCE_KEY: &[u8] = b"vote_fence";
/// Prefix for metadata-log entry keys (`mlog/` + 8-byte big-endian epoch).
const METADATA_PREFIX: &[u8] = b"mlog/";
/// Prefix for acceptor-state keys (`acc/` + 8-byte big-endian epoch).
const ACCEPTOR_PREFIX: &[u8] = b"acc/";
/// Prefix for multi-shard route keys (`route/` + order-preserving txn).
const ROUTE_PREFIX: &[u8] = b"route/";
/// Prefix for blind recovery-promise keys (`promise/` + order-preserving txn).
const PROMISE_PREFIX: &[u8] = b"promise/";
/// Prefix for decision-record keys (`decision/` + order-preserving txn).
/// Untouched by `truncate`.
const DECISION_PREFIX: &[u8] = b"decision/";

/// Builds a `prefix`-namespaced key for `txn`: big-endian `(micros, logical,
/// node)`, so byte order is timestamp order and truncation can compare keys.
fn txn_key(prefix: &[u8], txn: TxnId) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 20);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&txn.0.micros.to_be_bytes());
    key.extend_from_slice(&txn.0.logical.to_be_bytes());
    key.extend_from_slice(&txn.0.node.0.to_be_bytes());
    key
}

/// Recovers the transaction from a `prefix`-namespaced key, if it matches.
fn txn_of(prefix: &[u8], key: &[u8]) -> Option<TxnId> {
    let rest = key.strip_prefix(prefix)?;
    if rest.len() != 20 {
        return None;
    }
    Some(TxnId(Timestamp {
        micros: u64::from_be_bytes(rest[0..8].try_into().ok()?),
        logical: u32::from_be_bytes(rest[8..12].try_into().ok()?),
        node: NodeId(u64::from_be_bytes(rest[12..20].try_into().ok()?)),
    }))
}

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
    /// Whether anything was staged since the last flush. Lets `flush` skip the
    /// fsync entirely for inbox batches that staged nothing (gossip, response
    /// routing, probes), which otherwise cost a full `SyncAll` each.
    dirty: Arc<AtomicBool>,
    /// Held across a flush's check of `dirty` and its fsync, so a flush
    /// arriving while another's fsync is under way waits for it (see
    /// [`flush`](Journal::flush)). Shared by clones, like `dirty`.
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    /// How long each fsync is held back, so a test can keep one under way.
    #[cfg(test)]
    persist_delay: Arc<std::sync::Mutex<std::time::Duration>>,
}

impl FjallJournal {
    /// Opens (creating if absent) a journal at `path`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::builder(path).open()?;
        Self::from_database(db)
    }

    /// Creates a journal inside an existing database.
    ///
    /// fjall takes a directory lock per database, so this is the only way to
    /// colocate the consensus journal with a [`crate::Fjall`] event store in
    /// one database (`Fjall::database()` exposes the handle). The journal uses
    /// its own `accord_*`-prefixed keyspaces, so it never collides with the
    /// event store's.
    pub fn from_database(db: Database) -> anyhow::Result<Self> {
        let commands = db.keyspace("accord_commands", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("accord_meta", KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            commands,
            meta,
            dirty: Arc::new(AtomicBool::new(false)),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            persist_delay: Arc::default(),
        })
    }
}

impl FjallJournal {
    /// Inserts a per-transaction `meta` record without an fsync — durable at
    /// the next `flush`, like a staged command.
    async fn stage_meta(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || meta.insert(key, value)).await??;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Every per-transaction `meta` record under `prefix`, decoded by `decode`
    /// (the value carries its transaction, as in the SQL journal).
    async fn load_by_txn<T: Send + 'static>(
        &self,
        prefix: &'static [u8],
        decode: fn(&[u8]) -> anyhow::Result<(TxnId, T)>,
    ) -> anyhow::Result<Vec<(TxnId, T)>> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(TxnId, T)>> {
            let mut out = Vec::new();
            for guard in meta.prefix(prefix) {
                let (_, value) = guard.into_inner()?;
                out.push(decode(&value)?);
            }
            Ok(out)
        })
        .await?
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
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // One flush at a time. A flush arriving while another's fsync is under
        // way waits for it, then finds `dirty` clear (that fsync covered what
        // was staged before it) or set again (the fsync failed, or more was
        // staged meanwhile) and fsyncs itself. Without the lock it would find
        // `dirty` already cleared and return at once — and the inbox loop
        // would send a vote whose fsync had not completed, or had failed
        // (#308).
        let _guard = self.flush_lock.lock().await;
        // Nothing staged since the last flush → no fsync needed.
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let db = self.db.clone();
        #[cfg(test)]
        let delay = *self.persist_delay.lock().expect("delay poisoned");
        let result = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            std::thread::sleep(delay);
            db.persist(PersistMode::SyncAll)
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            other => {
                // The staged data may not be durable yet — make the next
                // flush try again rather than silently skipping.
                self.dirty.store(true, Ordering::Release);
                other??;
                Ok(())
            }
        }
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
            // Routes and blind promises go with the commands; decision
            // records outlive them.
            for prefix in [ROUTE_PREFIX, PROMISE_PREFIX] {
                let mut redundant = Vec::new();
                for guard in meta.prefix(prefix) {
                    let (key, _) = guard.into_inner()?;
                    if txn_of(prefix, &key).is_some_and(|txn| txn.0 < before) {
                        redundant.push(key);
                    }
                }
                for key in redundant {
                    meta.remove(key)?;
                }
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

    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        let value = if bounds.is_empty() {
            None
        } else {
            Some(encode_tagged(RecordKind::RejoinBounds, &bounds.to_vec())?)
        };
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            match value {
                Some(value) => meta.insert(REJOIN_BOUNDS_KEY, value)?,
                None => meta.remove(REJOIN_BOUNDS_KEY)?,
            }
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(REJOIN_BOUNDS_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(decode_tagged(RecordKind::RejoinBounds, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::ClockBound, &micros)?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            meta.insert(CLOCK_BOUND_KEY, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(CLOCK_BOUND_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::ClockBound, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_vote_fence(&self, fence: Timestamp) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::VoteFence, &fence)?;
        self.stage_meta(VOTE_FENCE_KEY.to_vec(), value).await
    }

    async fn load_vote_fence(&self) -> anyhow::Result<Option<Timestamp>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(VOTE_FENCE_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::VoteFence, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::AmnesiaFence, fence)?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            meta.insert(AMNESIA_FENCE_KEY, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(AMNESIA_FENCE_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::AmnesiaFence, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_fences(&self, fences: &[FenceRecord]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Fences, &fences.to_vec())?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            meta.insert(FENCES_KEY, value)?;
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_fences(&self) -> anyhow::Result<Vec<FenceRecord>> {
        let meta = self.meta.clone();
        let bytes = tokio::task::spawn_blocking(move || meta.get(FENCES_KEY)).await??;
        match bytes {
            Some(bytes) => Ok(decode_tagged(RecordKind::Fences, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_route(&self, txn: TxnId, route: &[Key]) -> anyhow::Result<()> {
        self.stage_meta(
            txn_key(ROUTE_PREFIX, txn),
            encode_tagged(RecordKind::Route, &(txn, route.to_vec()))?,
        )
        .await
    }

    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        self.load_by_txn(ROUTE_PREFIX, |bytes| {
            decode_tagged(RecordKind::Route, bytes)
        })
        .await
    }

    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        self.stage_meta(
            txn_key(PROMISE_PREFIX, txn),
            encode_tagged(RecordKind::Promise, &(txn, ballot))?,
        )
        .await
    }

    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        self.load_by_txn(PROMISE_PREFIX, |bytes| {
            decode_tagged(RecordKind::Promise, bytes)
        })
        .await
    }

    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        self.stage_meta(
            txn_key(DECISION_PREFIX, record.txn),
            encode_tagged(RecordKind::Decision, record)?,
        )
        .await
    }

    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<DecisionRecord>> {
            let mut out = Vec::new();
            for guard in meta.prefix(DECISION_PREFIX) {
                let (_, value) = guard.into_inner()?;
                out.push(decode_tagged(RecordKind::Decision, &value)?);
            }
            Ok(out)
        })
        .await?
    }

    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        let keys: Vec<Vec<u8>> = txns
            .iter()
            .map(|txn| txn_key(DECISION_PREFIX, *txn))
            .collect();
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            for key in keys {
                meta.remove(key)?;
            }
            Ok(())
        })
        .await??;
        // Durable with the next flush; a record dropped late is only kept longer.
        self.dirty.store(true, Ordering::Release);
        Ok(())
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

    async fn append_metadata(&self, epoch: u64, entry: &ConfigEntry) -> anyhow::Result<()> {
        let key = epoch_key(METADATA_PREFIX, epoch);
        let value = encode_tagged(RecordKind::MetadataEntry, entry)?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Idempotent: the first decided entry for an epoch wins; a re-commit is a
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

    async fn append_metadata_batch(&self, entries: &[(u64, ConfigEntry)]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let encoded = entries
            .iter()
            .map(|(epoch, entry)| {
                Ok((
                    epoch_key(METADATA_PREFIX, *epoch),
                    encode_tagged(RecordKind::MetadataEntry, entry)?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let meta = self.meta.clone();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Same first-decided-wins idempotency as `append_metadata`, but one
            // fsync covers the whole run (a metadata catch-up otherwise pays one
            // per entry).
            let mut inserted = false;
            for (key, value) in encoded {
                if meta.contains_key(&key)? {
                    continue;
                }
                meta.insert(key, value)?;
                inserted = true;
            }
            if inserted {
                db.persist(PersistMode::SyncAll)?;
            }
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
        let meta = self.meta.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
            let mut out = Vec::new();
            // Big-endian epoch keys sort ascending, so the prefix scan already
            // yields entries in epoch order.
            for guard in meta.prefix(METADATA_PREFIX) {
                let (key, value) = guard.into_inner()?;
                if let Some(epoch) = epoch_of(METADATA_PREFIX, &key) {
                    out.push((epoch, decode_tagged(RecordKind::MetadataEntry, &value)?));
                }
            }
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
            for guard in meta.prefix(ACCEPTOR_PREFIX) {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use evento_accord::Status;
    use evento_core::Event;

    use super::*;

    fn command(micros: u64) -> CommandState {
        let txn = TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(0),
        });
        CommandState {
            epoch: 0,
            txn,
            status: Status::PreAccepted,
            promised: Ballot(txn.0),
            accepted: Ballot(txn.0),
            execute_at: txn.0,
            deps: vec![],
            keys: vec![Key("acc".into())],
            events: vec![Event {
                aggregate_type: "test/Account".into(),
                aggregate_id: "acc".into(),
                version: 1,
                name: "Bumped".into(),
                ..Default::default()
            }],
            reply_to: NodeId(0),
            decision: None,
            applied_conflict: None,
        }
    }

    /// A flush returns only once everything staged before it is durable,
    /// even while another flush is under way: it waits for that one's fsync
    /// rather than finding `dirty` cleared and returning at once. The node's
    /// inbox loop stages a vote and flushes while the sweep may be flushing
    /// the same journal; returning early, it would send the vote before its
    /// fsync completed — or after it failed (#308).
    #[tokio::test(flavor = "current_thread")]
    async fn a_flush_waits_for_the_fsync_under_way() {
        let dir = tempfile::tempdir().unwrap();
        let journal = FjallJournal::open(dir.path()).unwrap();
        *journal.persist_delay.lock().unwrap() = Duration::from_millis(200);
        journal.stage(&command(100)).await.unwrap();

        let first = {
            let journal = journal.clone();
            tokio::spawn(async move { journal.flush().await })
        };
        // The first flush clears `dirty` and hands its fsync to the blocking
        // pool, where it stays for the delay.
        tokio::task::yield_now().await;
        journal.flush().await.unwrap();
        assert!(
            first.is_finished(),
            "a flush returned while the fsync covering what was staged before it was under way"
        );
        first.await.unwrap().unwrap();
    }
}
