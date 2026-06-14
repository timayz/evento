//! Host-facing trait interfaces, modelled on `accord-core`'s `accord/api`
//! package. The protocol core depends only on these traits; each has a
//! deterministic in-process implementation for the simulation harness and a
//! production implementation (real transport, static-config membership,
//! Fjall/SQL-backed storage).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use async_trait::async_trait;
use evento_core::{
    cursor::{Args, ReadResult},
    Event, ReadAggregator, RoutingKey,
};

use crate::{
    clock::{NodeId, Timestamp, TxnId},
    message::{CommandState, Key, Message},
};

/// Identifies a shard — a partition of the key space with its own replica set.
pub type ShardId = usize;

/// Outbound transport. The protocol core sends a [`Message`] to a peer by
/// [`NodeId`]; correlation of responses is by `TxnId` inside the message, not by
/// the transport. Implementations: in-memory channels (tests) and
/// length-delimited framed TCP (production).
#[async_trait]
pub trait MessageSink: Send + Sync + 'static {
    /// Sends `message` to node `to`. Returns an error only on a local failure to
    /// enqueue; delivery is best-effort and unacknowledged (the protocol
    /// tolerates loss via quorums and recovery).
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()>;
}

/// Cluster membership and key→replica-set mapping for the current epoch.
///
/// Implementations: [`StaticTopology`] (a fixed node list, single shard) and
/// [`ShardedTopology`] (disjoint, hash-partitioned shards). Epochs and dynamic
/// membership arrive in M4.
pub trait Topology: Send + Sync + 'static {
    /// Monotonic topology version. Bumped on every membership change.
    fn epoch(&self) -> u64;

    /// The node this process is running as.
    fn this_node(&self) -> NodeId;

    /// All nodes in the cluster.
    fn nodes(&self) -> Vec<NodeId>;

    /// Replicas that own `key` — the set a transaction touching `key` must reach
    /// quorum within.
    fn replicas(&self, key: &Key) -> Vec<NodeId>;

    /// Fast-path quorum size for `key`'s replica set: `⌈3f/2⌉ + 1` where the
    /// replica set has size `2f + 1`.
    fn fast_quorum(&self, key: &Key) -> usize;

    /// Slow-path / recovery quorum size for `key`'s replica set: `f + 1`.
    fn slow_quorum(&self, key: &Key) -> usize;

    /// The shard that owns `key`. Single-shard topologies return `0`.
    fn shard_of(&self, _key: &Key) -> ShardId {
        0
    }

    /// The shard `node` replicates, if any. Shards are disjoint, so a node
    /// belongs to at most one. Single-shard topologies return `Some(0)`.
    fn node_shard(&self, _node: NodeId) -> Option<ShardId> {
        Some(0)
    }

    /// Whether `node` replicates `key` (i.e. owns its shard). Determines which
    /// events of a multi-shard transaction a replica reads and applies.
    fn owns(&self, node: NodeId, key: &Key) -> bool {
        self.node_shard(node) == Some(self.shard_of(key))
    }

    /// Installs a later epoch's shard layout. The default is a no-op (fixed
    /// topologies do not change); [`DynamicTopology`] overrides it.
    fn install(&self, _epoch: u64, _shards: Vec<Vec<NodeId>>) {}
}

/// Durable command log. Each replica persists its [`CommandState`] per
/// transaction so it survives restarts and can be replayed during recovery.
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    /// Durably records (upserts) a replica's state for one transaction.
    async fn record(&self, state: &CommandState) -> anyhow::Result<()>;

    /// Loads the persisted state for `txn`, if any.
    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>>;

    /// All persisted command states, for rebuilding replica state after a
    /// process restart. The default returns nothing (a non-durable journal).
    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        Ok(Vec::new())
    }
}

/// Applied state — the materialised event store on a replica. Backed by an
/// existing evento backend (`Fjall` / `Sql`). This is where committed
/// transactions land in execution-timestamp order and where reads,
/// subscriptions, and snapshots are served.
#[async_trait]
pub trait DataStore: Send + Sync + 'static {
    /// Current version of an aggregate. Returns 0 for an aggregate with no
    /// events. Used during the Read phase to evaluate the optimistic-version
    /// condition before any shard appends.
    async fn version(&self, aggregator_type: &str, aggregator_id: &str) -> anyhow::Result<u16>;

    /// Records a decided transaction at its execution timestamp. When
    /// `commit` is true the `events` are appended (advancing versions);
    /// otherwise the transaction is a no-op (an aborted conditional write).
    /// Implementations must apply in `execute_at` order to preserve the global
    /// serial order. `events` is only this replica's owned subset.
    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()>;

    /// Serves a read query from the local backend, so a node can answer reads
    /// forwarded to it for a key range it owns. The default returns nothing (a
    /// store that holds no queryable events, e.g. the in-memory test store).
    async fn read(
        &self,
        _aggregators: Option<Vec<ReadAggregator>>,
        _routing_key: Option<RoutingKey>,
        _args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        Ok(ReadResult::default())
    }
}

/// Static, single-shard [`Topology`]: every node replicates every key. The
/// starting point for M1; superseded by sharded/epoch-aware topologies in M3/M4.
pub struct StaticTopology {
    this: NodeId,
    nodes: Vec<NodeId>,
}

impl StaticTopology {
    /// Builds a topology from the full node list and this process's id.
    ///
    /// Panics if `this` is not in `nodes` — a configuration error.
    pub fn new(this: NodeId, nodes: Vec<NodeId>) -> Self {
        assert!(
            nodes.contains(&this),
            "this node {this:?} must be part of the cluster"
        );
        Self { this, nodes }
    }

    /// Failures tolerated: for `N = 2f + 1`, `f = (N - 1) / 2`.
    fn faults(&self) -> usize {
        self.nodes.len().saturating_sub(1) / 2
    }
}

impl Topology for StaticTopology {
    fn epoch(&self) -> u64 {
        0
    }

    fn this_node(&self) -> NodeId {
        self.this
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }

    fn replicas(&self, _key: &Key) -> Vec<NodeId> {
        // Single shard: every node owns every key.
        self.nodes.clone()
    }

    fn fast_quorum(&self, _key: &Key) -> usize {
        // ⌈3f/2⌉ + 1.
        let f = self.faults();
        (3 * f).div_ceil(2) + 1
    }

    fn slow_quorum(&self, _key: &Key) -> usize {
        self.faults() + 1
    }
}

/// Range-/hash-partitioned [`Topology`] with **disjoint** shards: the key space
/// is split across shards, each owned by its own replica set, and every node
/// belongs to exactly one shard. A transaction touching keys in several shards
/// must reach a quorum in each. Used for M3 multi-shard transactions.
pub struct ShardedTopology {
    this: NodeId,
    /// Shard id → replica set. Replica sets are disjoint.
    shards: Vec<Vec<NodeId>>,
}

impl ShardedTopology {
    /// Builds a topology from disjoint shard replica sets and this node's id.
    ///
    /// Panics if `this` is not in any shard, or if the shards are not disjoint.
    pub fn new(this: NodeId, shards: Vec<Vec<NodeId>>) -> Self {
        let mut seen = std::collections::HashSet::new();
        for shard in &shards {
            for &node in shard {
                assert!(
                    seen.insert(node),
                    "shards must be disjoint ({node:?} repeated)"
                );
            }
        }
        assert!(
            seen.contains(&this),
            "this node {this:?} must belong to a shard"
        );
        Self { this, shards }
    }

    /// Failures a replica set of size `n = 2f + 1` tolerates.
    fn faults(n: usize) -> usize {
        n.saturating_sub(1) / 2
    }
}

impl Topology for ShardedTopology {
    fn epoch(&self) -> u64 {
        0
    }

    fn this_node(&self) -> NodeId {
        self.this
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.shards.iter().flatten().copied().collect()
    }

    fn replicas(&self, key: &Key) -> Vec<NodeId> {
        self.shards[self.shard_of(key)].clone()
    }

    fn fast_quorum(&self, key: &Key) -> usize {
        let f = Self::faults(self.shards[self.shard_of(key)].len());
        (3 * f).div_ceil(2) + 1
    }

    fn slow_quorum(&self, key: &Key) -> usize {
        Self::faults(self.shards[self.shard_of(key)].len()) + 1
    }

    fn shard_of(&self, key: &Key) -> ShardId {
        // Deterministic across nodes: DefaultHasher uses fixed seeds.
        let mut hasher = DefaultHasher::new();
        key.0.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn node_shard(&self, node: NodeId) -> Option<ShardId> {
        self.shards.iter().position(|shard| shard.contains(&node))
    }
}

/// A [`Topology`] whose shard layout can be replaced atomically as the cluster
/// grows. Each layout is tagged with a monotonically increasing **epoch**.
///
/// Unlike [`ShardedTopology`], the running node need not yet belong to any shard
/// (a joining node holds the current layout to find a peer to bootstrap from,
/// then [`install`](DynamicTopology::install)s the new epoch that includes it).
/// Coordinating the epoch change across the cluster — and handling transactions
/// in flight across it — is future work; callers install an agreed layout.
pub struct DynamicTopology {
    this: NodeId,
    state: Mutex<DynamicState>,
}

struct DynamicState {
    epoch: u64,
    shards: Vec<Vec<NodeId>>,
}

impl DynamicTopology {
    /// Builds a topology at `epoch` from disjoint shard replica sets. `this` need
    /// not be a member yet.
    pub fn new(this: NodeId, epoch: u64, shards: Vec<Vec<NodeId>>) -> Self {
        Self {
            this,
            state: Mutex::new(DynamicState { epoch, shards }),
        }
    }

    fn shard_index(shards: &[Vec<NodeId>], key: &Key) -> ShardId {
        let mut hasher = DefaultHasher::new();
        key.0.hash(&mut hasher);
        (hasher.finish() as usize) % shards.len()
    }
}

impl Topology for DynamicTopology {
    fn epoch(&self) -> u64 {
        self.state.lock().expect("topology poisoned").epoch
    }

    fn this_node(&self) -> NodeId {
        self.this
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.state
            .lock()
            .expect("topology poisoned")
            .shards
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    fn replicas(&self, key: &Key) -> Vec<NodeId> {
        let state = self.state.lock().expect("topology poisoned");
        state.shards[Self::shard_index(&state.shards, key)].clone()
    }

    fn fast_quorum(&self, key: &Key) -> usize {
        let state = self.state.lock().expect("topology poisoned");
        let n = state.shards[Self::shard_index(&state.shards, key)].len();
        let f = n.saturating_sub(1) / 2;
        (3 * f).div_ceil(2) + 1
    }

    fn slow_quorum(&self, key: &Key) -> usize {
        let state = self.state.lock().expect("topology poisoned");
        let n = state.shards[Self::shard_index(&state.shards, key)].len();
        n.saturating_sub(1) / 2 + 1
    }

    fn shard_of(&self, key: &Key) -> ShardId {
        let state = self.state.lock().expect("topology poisoned");
        Self::shard_index(&state.shards, key)
    }

    fn node_shard(&self, node: NodeId) -> Option<ShardId> {
        self.state
            .lock()
            .expect("topology poisoned")
            .shards
            .iter()
            .position(|shard| shard.contains(&node))
    }

    /// Atomically replaces the layout with a later `epoch`. Ignored if `epoch`
    /// is not newer than the current one.
    fn install(&self, epoch: u64, shards: Vec<Vec<NodeId>>) {
        let mut state = self.state.lock().expect("topology poisoned");
        if epoch > state.epoch {
            state.epoch = epoch;
            state.shards = shards;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topo(n: u64) -> StaticTopology {
        let nodes: Vec<NodeId> = (0..n).map(NodeId).collect();
        StaticTopology::new(NodeId(0), nodes)
    }

    #[test]
    fn quorum_sizes_match_accord_table() {
        // N=3 (f=1): fast=3, slow=2.
        let k = Key("x".into());
        let t3 = topo(3);
        assert_eq!(t3.fast_quorum(&k), 3);
        assert_eq!(t3.slow_quorum(&k), 2);

        // N=5 (f=2): fast=4, slow=3.
        let t5 = topo(5);
        assert_eq!(t5.fast_quorum(&k), 4);
        assert_eq!(t5.slow_quorum(&k), 3);

        // N=7 (f=3): fast=⌈9/2⌉+1=5+1=... (3*3+1)/2+1 = 5+1 = 6? check: ⌈4.5⌉=5, +1=6
        let t7 = topo(7);
        assert_eq!(t7.fast_quorum(&k), 6);
        assert_eq!(t7.slow_quorum(&k), 4);
    }
}
