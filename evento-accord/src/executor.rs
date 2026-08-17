//! Wiring Accord into evento's [`Executor`] trait.
//!
//! [`AccordExecutor`] is a drop-in `evento_core::Executor` whose **writes** are
//! coordinated through an Accord cluster — replicated, strictly serializable, and
//! atomic across aggregates — while **reads, subscriptions, and snapshots** are
//! served from a local evento backend (`Fjall`/`Sql`) that each replica keeps up
//! to date by applying committed transactions.
//!
//! [`ExecutorDataStore`] is the bridge in the other direction: the Accord
//! [`DataStore`](crate::api::DataStore) that a replica uses to read an
//! aggregate's version (for the commit condition) and to append committed events
//! — both delegated to that same local backend. Share one backend instance
//! (it is `Clone`/`Arc`-backed) between the two so a node's writes are visible to
//! its reads.
//!
//! ## Scope
//!
//! This targets a **single-shard** cluster (every node replicates everything),
//! so a node's local backend holds the whole log and serves complete reads.
//! Routing reads to owning shards in a multi-shard cluster is a later layer.

use async_trait::async_trait;
use evento_core::{
    cursor::{Args, ReadResult, Value},
    Event, EventFilter, Executor, RoutingKey, WriteError,
};
use ulid::Ulid;

use crate::api::DataStore;
use crate::clock::{Timestamp, TxnId};
use crate::message::Key;
use crate::node::Node;

/// Page size for the full-store scans behind [`ExecutorDataStore::version`] and
/// [`ExecutorDataStore::snapshot`]. Both walk the backend page by page until
/// exhausted, so an aggregate (or store) of any size is handled — no longer capped at
/// one `u16::MAX`-sized page. Sized to balance round-trips against per-page memory.
const SNAPSHOT_PAGE_SIZE: u16 = 4096;

/// The single key a read targets, if it can be pinned to one shard: an explicit
/// routing key, or a query for one aggregate by id. Broad scans (multiple
/// aggregates, or by event type) return `None` and are served locally.
///
/// **Caveat:** the write side derives the shard key from `routing_key` falling
/// back to the aggregate id ([`Key::of`]). A by-id pin is therefore only
/// correct for aggregates written **without** a routing key; a read of a
/// *routed* aggregate must pass its routing key too, or the pin lands on the
/// wrong key — a forwarded read goes to the wrong shard, and a read barrier
/// fences the wrong conflict set.
fn target_key(
    aggregators: &Option<Vec<EventFilter>>,
    routing_key: &Option<RoutingKey>,
) -> Option<Key> {
    if let Some(RoutingKey::Value(Some(key))) = routing_key {
        return Some(Key(key.clone()));
    }
    match aggregators.as_deref() {
        Some([only]) => only.aggregate_id.clone().map(Key),
        _ => None,
    }
}

/// An Accord [`DataStore`] backed by a local evento [`Executor`].
///
/// `version` reads the aggregate's current version from the backend (the
/// optimistic-concurrency input to Accord's Read phase); `apply` appends the
/// committed events (or does nothing on abort).
pub struct ExecutorDataStore<E: Executor> {
    local: E,
}

impl<E: Executor> ExecutorDataStore<E> {
    /// Wraps a local evento backend as an Accord data store.
    pub fn new(local: E) -> Self {
        Self { local }
    }
}

#[async_trait]
impl<E: Executor> DataStore for ExecutorDataStore<E> {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        // The aggregate's current version is the highest among its events. Walk the
        // backend a page at a time and keep the running max, so an aggregate with more
        // than one page of events is handled correctly (not capped at one page).
        let mut max = 0u16;
        let mut after = None;
        loop {
            let result = self
                .local
                .read(
                    Some(vec![EventFilter::by_id(aggregate_type, aggregate_id)]),
                    None,
                    Args::forward(SNAPSHOT_PAGE_SIZE, after),
                )
                .await?;
            if let Some(page_max) = result.edges.iter().map(|e| e.node.version).max() {
                max = max.max(page_max);
            }
            if !result.page_info.has_next_page {
                break;
            }
            // Defensive: a "more pages" claim with no cursor can't advance — stop
            // rather than loop forever (evento sets the cursor whenever edges exist).
            match result.page_info.end_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(max)
    }

    async fn apply(
        &self,
        _txn: TxnId,
        _execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if !commit || events.is_empty() {
            return Ok(());
        }
        // `replicate`, not `write`: the replication layer owns ordering, so
        // the events' timestamps must be persisted verbatim (a re-stamping
        // backend would desync this replica's cursors from its peers').
        match self.local.replicate(events).await {
            // Accord already validated the condition at this serial point;
            // a version conflict here means the events are already present
            // (idempotent re-apply), which is fine.
            Ok(()) | Err(WriteError::InvalidOriginalVersion) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn read(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.local.read(aggregators, routing_key, args).await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        // The materialised state is the full event log; a joining node re-applies it
        // to reconstruct the prefix that journal truncation removed. Walk the backend
        // a page at a time so a store of any size is captured (no longer capped at one
        // page). The whole log is materialised in memory — fine for the bootstrap path;
        // a streaming snapshot for very large stores is a future refinement.
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let result = self
                .local
                .read(None, None, Args::forward(SNAPSHOT_PAGE_SIZE, after))
                .await?;
            let has_next = result.page_info.has_next_page;
            let cursor = result.page_info.end_cursor;
            out.extend(result.edges.into_iter().map(|edge| edge.node));
            if !has_next {
                break;
            }
            match cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(out)
    }
}

/// An evento [`Executor`] whose writes are coordinated through an Accord cluster
/// and whose reads/subscriptions/snapshots are served from a local backend.
#[derive(Clone)]
pub struct AccordExecutor<E: Executor + Clone> {
    node: Node,
    local: E,
}

impl<E: Executor + Clone> AccordExecutor<E> {
    /// Builds an executor over `node` (for coordinating writes) and `local`
    /// (the backend serving reads — the same instance the node's
    /// [`ExecutorDataStore`] applies to).
    pub fn new(node: Node, local: E) -> Self {
        Self { node, local }
    }

    /// The underlying node, e.g. to run recovery.
    pub fn node(&self) -> &Node {
        &self.node
    }
}

#[async_trait]
impl<E: Executor + Clone> Executor for AccordExecutor<E> {
    fn default_routing_key(&self) -> Option<&str> {
        self.local.default_routing_key()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Err(WriteError::MissingData);
        }
        let outcome = self.node.write(events).await.map_err(WriteError::Unknown)?;
        if outcome.conflict {
            Err(WriteError::InvalidOriginalVersion)
        } else {
            Ok(())
        }
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        // Subscriptions read from the local backend, and committed writes are
        // applied to it via the node's `ExecutorDataStore`. That apply calls the
        // local executor's `write`, which bumps the local signal — so forwarding
        // here delivers a wakeup once a write is locally visible.
        self.local.write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // Gate subscriptions by the node's stability watermark. Events are
        // applied to each replica's local store in Accord's `(execute_at, txn)`
        // order — which can differ from the subscription's wall-clock cursor —
        // so without this gate a late, lower-cursor event applied out of order
        // would be skipped by the forward read. See `Node::stable_micros`.
        Ok(Some(self.node.stable_micros()))
    }

    async fn read(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        // A single-shard read for a key this node does not own is forwarded to an
        // owner. Everything else (owned keys, and broad scans that can't be pinned
        // to one shard) is served from the local backend.
        if let Some(key) = target_key(&aggregators, &routing_key) {
            if self.node.owns_key(&key) {
                // Linearizable reads: fence the local backend with a read barrier
                // (a read-only consensus round) so it reflects every write that
                // committed before this read began, then serve locally. Off by
                // default — reads stay local-only (serializable, not linearizable).
                if self.node.linearizable_reads() {
                    self.node.read_barrier(key).await?;
                }
            } else if let Some(owner) = self.node.an_owner_of(&key) {
                // NOTE: a forwarded (non-owned, multi-shard) read is not yet
                // linearized — the owner would need to barrier before serving.
                return self
                    .node
                    .forward_read(owner, aggregators, routing_key, args)
                    .await;
            }
        }
        self.local.read(aggregators, routing_key, args).await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        self.local.latest_timestamp(aggregators, routing_key).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.local.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.local.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.local.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.local.acknowledge(key, worker_id, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        aggregate_type: String,
        aggregator_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.local
            .get_snapshot(aggregate_type, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.local
            .save_snapshot(aggregate_type, aggregator_revision, id, data, cursor)
            .await
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        self.local.delete_snapshot(aggregate_type, id).await
    }
}
