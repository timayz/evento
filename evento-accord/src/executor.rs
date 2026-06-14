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
    Event, Executor, ReadAggregator, RoutingKey, WriteError,
};
use ulid::Ulid;

use crate::api::DataStore;
use crate::clock::{Timestamp, TxnId};
use crate::message::Key;
use crate::node::Node;

/// The single key a read targets, if it can be pinned to one shard: an explicit
/// routing key, or a query for one aggregate by id. Broad scans (multiple
/// aggregates, or by event type) return `None` and are served locally.
fn target_key(
    aggregators: &Option<Vec<ReadAggregator>>,
    routing_key: &Option<RoutingKey>,
) -> Option<Key> {
    if let Some(RoutingKey::Value(Some(key))) = routing_key {
        return Some(Key(key.clone()));
    }
    match aggregators.as_deref() {
        Some([only]) => only.aggregator_id.clone().map(Key),
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
    async fn version(&self, aggregator_type: &str, aggregator_id: &str) -> anyhow::Result<u16> {
        // The aggregate's current version is the highest among its events.
        // (Reads up to u16::MAX-1 events; aggregates beyond that need snapshot
        // compaction, which evento provides — a refinement for this adapter.)
        let result = self
            .local
            .read(
                Some(vec![ReadAggregator::id(aggregator_type, aggregator_id)]),
                None,
                Args::forward(u16::MAX - 1, None),
            )
            .await?;
        Ok(result
            .edges
            .iter()
            .map(|e| e.node.version)
            .max()
            .unwrap_or(0))
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
        match self.local.write(events).await {
            // Accord already validated the condition at this serial point;
            // a version conflict here means the events are already present
            // (idempotent re-apply), which is fine.
            Ok(()) | Err(WriteError::InvalidOriginalVersion) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.local.read(aggregators, routing_key, args).await
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

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        // A single-shard read for a key this node does not own is forwarded to an
        // owner. Everything else (owned keys, and broad scans that can't be pinned
        // to one shard) is served from the local backend.
        if let Some(key) = target_key(&aggregators, &routing_key) {
            if !self.node.owns_key(&key) {
                if let Some(owner) = self.node.an_owner_of(&key) {
                    return self
                        .node
                        .forward_read(owner, aggregators, routing_key, args)
                        .await;
                }
            }
        }
        self.local.read(aggregators, routing_key, args).await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
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

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.local.acknowledge(key, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.local
            .get_snapshot(aggregator_type, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.local
            .save_snapshot(aggregator_type, aggregator_revision, id, data, cursor)
            .await
    }

    async fn delete_snapshot(&self, aggregator_type: String, id: String) -> anyhow::Result<()> {
        self.local.delete_snapshot(aggregator_type, id).await
    }
}
