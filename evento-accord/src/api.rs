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
    Event, EventFilter, RoutingKey,
};
use serde::{Deserialize, Serialize};

use crate::{
    clock::{Ballot, NodeId, Timestamp, TxnId},
    message::{CommandState, Key, Message},
};

/// Identifies a shard — a partition of the key space with its own replica set.
pub type ShardId = usize;

/// Identifies a region / datacenter — a locality group of nodes. Used to prefer
/// same-region peers for lower-latency operations (e.g. read routing).
pub type RegionId = u16;

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

    /// The **fast-path electorate** for `key`'s shard — the subset of
    /// [`replicas`](Topology::replicas) whose `PreAccept` votes decide the
    /// one-round-trip fast path. It is a *fixed, cluster-agreed* property of the
    /// shard (every coordinator of the shard uses the same set), placed in one
    /// region so a co-located coordinator commits in a single local round-trip.
    ///
    /// The default is the whole replica set (no region favouring — the classic
    /// `⌈3f/2⌉ + 1` fast path). A configured electorate must be a subset of
    /// `replicas(key)` with `f + 1 ≤ |E| ≤ N` (asserted at construction): that
    /// bound keeps recovery sound — a recovery quorum (`f + 1`) always intersects
    /// a fast quorum, and any two fast quorums of the shared electorate intersect.
    fn fast_electorate(&self, key: &Key) -> Vec<NodeId> {
        self.replicas(key)
    }

    /// Fast-path quorum size for `key`: `⌊(e + f)/2⌋ + 1` where `e` is the
    /// [`fast_electorate`](Topology::fast_electorate) size and the replica set has
    /// size `N = 2f + 1`. Reduces to `⌈3f/2⌉ + 1` when the electorate is the whole
    /// replica set (`e = N`).
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

    /// The region / datacenter `node` lives in, if regions are configured. Used to
    /// prefer a same-region peer (e.g. for read routing). The default is `None`
    /// (no region awareness — all nodes treated as one locality).
    fn region(&self, _node: NodeId) -> Option<RegionId> {
        None
    }

    /// Installs a later epoch's shard layout. The default is a no-op (fixed
    /// topologies do not change); [`DynamicTopology`] overrides it.
    fn install(&self, _epoch: u64, _shards: Vec<Vec<NodeId>>) {}

    /// Whether this topology can change epoch (reconfigure). The default is
    /// `false` (fixed topologies); [`DynamicTopology`] overrides it to `true`. The
    /// node's metadata-log catch-up sweep only runs for dynamic topologies, so a
    /// fixed-membership cluster issues no control-plane traffic.
    fn dynamic(&self) -> bool {
        false
    }
}

/// A config-Paxos acceptor's durable state for one epoch — the highest ballot it
/// has promised and the highest-ballot layout it has accepted, if any. Persisted
/// (via [`Journal::record_acceptor`]) **before** the matching promise/accept reply
/// is sent, so a restarted acceptor restores its `promised` ballot and can never
/// regress to a lower one (the Paxos crash-safety requirement).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptorRecord {
    /// Highest ballot ever promised for this epoch.
    pub promised: Ballot,
    /// The highest-ballot `(ballot, layout)` accepted for this epoch, if any.
    pub accepted: Option<(Ballot, Vec<Vec<NodeId>>)>,
}

/// Durable command log. Each replica persists its [`CommandState`] per
/// transaction so it survives restarts and can be replayed during recovery.
///
/// Two write paths support **group commit**: [`stage`](Journal::stage) buffers a
/// write that must be durable no later than the next [`flush`](Journal::flush),
/// and `flush` makes every staged write durable in one fsync. The node's inbox
/// loop drains a batch of messages, stages each, then flushes once — amortizing
/// the fsync across the batch. [`record`](Journal::record) is the immediate
/// (stage-then-flush) path for callers that need durability right away.
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    /// Durably records (upserts) a replica's state for one transaction — durable
    /// by the time it returns (equivalent to [`stage`](Journal::stage) then
    /// [`flush`](Journal::flush)).
    async fn record(&self, state: &CommandState) -> anyhow::Result<()>;

    /// Buffers a write that must be made durable no later than the next
    /// [`flush`](Journal::flush). The default is the immediate [`record`] — fine
    /// for journals that do not (or need not) batch their fsync.
    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        self.record(state).await
    }

    /// Makes every write [`stage`](Journal::stage)d since the last flush durable,
    /// in one operation (a group-commit fsync). The default is a no-op, since a
    /// non-batching journal's [`record`]/[`stage`] are already durable.
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Discards every record whose transaction is below `before` (its
    /// transactions are redundant — applied on every replica) and persists
    /// `before` as the truncation watermark, so a restart sets its redundancy
    /// floor and trusts the data store for everything below it. Bounds the log to
    /// in-flight work. The default is a no-op (a non-truncating journal).
    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        let _ = before;
        Ok(())
    }

    /// The persisted truncation watermark, if any — the redundancy floor a restart
    /// restores. The default returns nothing.
    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        Ok(None)
    }

    /// Loads the persisted state for `txn`, if any.
    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>>;

    /// All persisted command states, for rebuilding replica state after a
    /// process restart. The default returns nothing (a non-durable journal).
    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        Ok(Vec::new())
    }

    /// Durably appends a decided metadata-log entry — one `epoch`'s committed
    /// topology `layout`. Durable by the time it returns. Idempotent: re-appending
    /// an epoch already present with the same layout is a no-op. The default is a
    /// no-op (a journal that does not back dynamic membership).
    async fn append_metadata(&self, epoch: u64, layout: &[Vec<NodeId>]) -> anyhow::Result<()> {
        let _ = (epoch, layout);
        Ok(())
    }

    /// All persisted metadata-log entries, ascending by epoch, for replaying the
    /// committed topology sequence after a restart. The default returns nothing.
    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
        Ok(Vec::new())
    }

    /// Durably persists this node's config-Paxos acceptor state for one `epoch`.
    /// Durable by the time it returns — this is the crash-safety gate (a restarted
    /// acceptor must not promise/accept a ballot lower than one already promised).
    /// The default is a no-op.
    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        let _ = (epoch, state);
        Ok(())
    }

    /// All persisted acceptor states, for restoring config-Paxos safety after a
    /// process restart. The default returns nothing.
    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
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
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16>;

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
        _aggregators: Option<Vec<EventFilter>>,
        _routing_key: Option<RoutingKey>,
        _args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        Ok(ReadResult::default())
    }

    /// All applied events materialised in this store — the bootstrap snapshot a
    /// joining node installs to reconstruct the state that log truncation has
    /// removed from the command journal. The default is empty (a store whose
    /// journal never truncates needs no snapshot).
    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        Ok(Vec::new())
    }
}

/// Fast-path quorum size: `⌊(electorate + faults)/2⌋ + 1`. With the electorate set
/// to the whole replica set (`electorate = 2f + 1`) this is the classic
/// `⌈3f/2⌉ + 1`; shrinking the electorate toward `f + 1` shrinks the quorum toward
/// `f + 1`, enabling a single-region one-round-trip commit.
pub(crate) fn fast_quorum_size(electorate: usize, faults: usize) -> usize {
    (electorate + faults) / 2 + 1
}

/// Asserts a configured fast-path electorate is well-formed for a replica set:
/// a subset of `replicas`, with `f + 1 ≤ |electorate| ≤ N` (the bound that keeps
/// recovery sound). Panics on violation — a configuration error.
pub(crate) fn assert_valid_electorate(electorate: &[NodeId], replicas: &[NodeId]) {
    let n = replicas.len();
    let f = n.saturating_sub(1) / 2;
    let min = f + 1; // the smallest electorate that keeps recovery sound
    assert!(
        electorate.len() >= min && electorate.len() <= n,
        "fast-path electorate of {} is out of range for N={n} (need f+1={min}..=N)",
        electorate.len(),
    );
    for node in electorate {
        assert!(
            replicas.contains(node),
            "fast-path electorate member {node:?} is not a replica of the shard"
        );
    }
}

/// Static, single-shard [`Topology`]: every node replicates every key. The
/// starting point for M1; superseded by sharded/epoch-aware topologies in M3/M4.
pub struct StaticTopology {
    this: NodeId,
    nodes: Vec<NodeId>,
    /// Optional fast-path electorate (a subset of `nodes`); `None` ⇒ all nodes.
    electorate: Option<Vec<NodeId>>,
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
        Self {
            this,
            nodes,
            electorate: None,
        }
    }

    /// Sets the fast-path electorate — the subset of nodes whose `PreAccept` votes
    /// decide the one-round-trip fast path (see [`Topology::fast_electorate`]).
    /// Panics unless it is a subset of the nodes with `f + 1 ≤ |E| ≤ N`.
    pub fn with_fast_electorate(mut self, electorate: Vec<NodeId>) -> Self {
        assert_valid_electorate(&electorate, &self.nodes);
        self.electorate = Some(electorate);
        self
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

    fn fast_electorate(&self, _key: &Key) -> Vec<NodeId> {
        self.electorate
            .clone()
            .unwrap_or_else(|| self.nodes.clone())
    }

    fn fast_quorum(&self, key: &Key) -> usize {
        // ⌊(e + f)/2⌋ + 1; ⌈3f/2⌉ + 1 when the electorate is the whole set.
        fast_quorum_size(self.fast_electorate(key).len(), self.faults())
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
    /// Shard id → optional fast-path electorate (a subset of that shard's
    /// replicas); `None` ⇒ the whole shard.
    electorates: Vec<Option<Vec<NodeId>>>,
    /// Optional node → region map for region-aware routing (empty ⇒ no regions).
    regions: std::collections::HashMap<NodeId, RegionId>,
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
        let electorates = vec![None; shards.len()];
        Self {
            this,
            shards,
            electorates,
            regions: std::collections::HashMap::new(),
        }
    }

    /// Tags nodes with regions for region-aware routing (a node absent from the
    /// map has no region).
    pub fn with_regions(mut self, regions: std::collections::HashMap<NodeId, RegionId>) -> Self {
        self.regions = regions;
        self
    }

    /// Sets `shard`'s fast-path electorate — the subset of its replicas whose
    /// `PreAccept` votes decide the one-round-trip fast path (see
    /// [`Topology::fast_electorate`]). Panics unless it is a subset of the shard's
    /// replicas with `f + 1 ≤ |E| ≤ N`.
    pub fn with_fast_electorate(mut self, shard: ShardId, electorate: Vec<NodeId>) -> Self {
        assert_valid_electorate(&electorate, &self.shards[shard]);
        self.electorates[shard] = Some(electorate);
        self
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

    fn fast_electorate(&self, key: &Key) -> Vec<NodeId> {
        let shard = self.shard_of(key);
        self.electorates[shard]
            .clone()
            .unwrap_or_else(|| self.shards[shard].clone())
    }

    fn fast_quorum(&self, key: &Key) -> usize {
        let f = Self::faults(self.shards[self.shard_of(key)].len());
        fast_quorum_size(self.fast_electorate(key).len(), f)
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

    fn region(&self, node: NodeId) -> Option<RegionId> {
        self.regions.get(&node).copied()
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
    /// Optional node → region map for region-aware routing and the region-derived
    /// fast-path electorate (empty ⇒ no regions ⇒ the classic whole-set fast path).
    /// Static cluster config — the operator supplies the **same** map to every node,
    /// so all nodes derive the identical electorate from the identical layout.
    regions: std::collections::HashMap<NodeId, RegionId>,
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
            regions: std::collections::HashMap::new(),
        }
    }

    /// Tags nodes with regions, enabling region-aware read routing and the
    /// **region-derived fast-path electorate** (a node absent from the map has no
    /// region). The map must be identical on every node.
    pub fn with_regions(mut self, regions: std::collections::HashMap<NodeId, RegionId>) -> Self {
        self.regions = regions;
        self
    }

    fn shard_index(shards: &[Vec<NodeId>], key: &Key) -> ShardId {
        let mut hasher = DefaultHasher::new();
        key.0.hash(&mut hasher);
        (hasher.finish() as usize) % shards.len()
    }

    /// Derives shard `idx`'s fast-path electorate from the layout + region tags:
    /// the **largest in-region group** of that shard's replicas (ties break to the
    /// lowest [`RegionId`], so every node — holding the same layout + map — derives
    /// the same set). Used iff that group has size in `[f+1, N]` (a recovery-sound
    /// electorate; `== N` when one region holds the shard, i.e. no shrink);
    /// otherwise (no regions, or the largest region is below `f+1`) the whole shard.
    fn electorate_for(
        shards: &[Vec<NodeId>],
        idx: usize,
        regions: &std::collections::HashMap<NodeId, RegionId>,
    ) -> Vec<NodeId> {
        let replicas = &shards[idx];
        if regions.is_empty() {
            return replicas.clone();
        }
        let f = replicas.len().saturating_sub(1) / 2;
        // Distinct regions present among the replicas, ascending so a size tie
        // resolves to the lowest RegionId (the `<=` keeps the incumbent).
        let mut present: Vec<RegionId> = replicas
            .iter()
            .filter_map(|n| regions.get(n).copied())
            .collect();
        present.sort_unstable();
        present.dedup();
        let mut best: Option<(RegionId, usize)> = None;
        for &r in &present {
            let count = replicas
                .iter()
                .filter(|n| regions.get(n) == Some(&r))
                .count();
            if best.is_none_or(|(_, bc)| count > bc) {
                best = Some((r, count));
            }
        }
        match best {
            // `count > f` ≡ `count >= f + 1`: the recovery-sound lower bound.
            Some((r, count)) if count > f => replicas
                .iter()
                .copied()
                .filter(|n| regions.get(n) == Some(&r))
                .collect(),
            _ => replicas.clone(),
        }
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

    /// The **region-derived** electorate for `key`'s shard (see
    /// [`electorate_for`](DynamicTopology::electorate_for)). A pure function of the
    /// installed layout + region tags, so it is recomputed correctly after every
    /// epoch change with no extra state crossing consensus.
    fn fast_electorate(&self, key: &Key) -> Vec<NodeId> {
        let state = self.state.lock().expect("topology poisoned");
        let idx = Self::shard_index(&state.shards, key);
        Self::electorate_for(&state.shards, idx, &self.regions)
    }

    fn fast_quorum(&self, key: &Key) -> usize {
        let state = self.state.lock().expect("topology poisoned");
        let idx = Self::shard_index(&state.shards, key);
        let f = state.shards[idx].len().saturating_sub(1) / 2;
        let e = Self::electorate_for(&state.shards, idx, &self.regions).len();
        fast_quorum_size(e, f)
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

    fn region(&self, node: NodeId) -> Option<RegionId> {
        self.regions.get(&node).copied()
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

    fn dynamic(&self) -> bool {
        true
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

    #[test]
    fn electorate_quorum_table_and_invariants() {
        let k = Key("x".into());
        // For each N, sweep the electorate size e over the legal range [f+1, N] and
        // check the formula plus the two recovery-safety invariants.
        for &n in &[3u64, 5, 7] {
            let f = (n as usize - 1) / 2;
            let nodes: Vec<NodeId> = (0..n).map(NodeId).collect();
            for e in (f + 1)..=(n as usize) {
                let topo = StaticTopology::new(NodeId(0), nodes.clone())
                    .with_fast_electorate(nodes[..e].to_vec());
                let fast = topo.fast_quorum(&k);
                let slow = topo.slow_quorum(&k);
                assert_eq!(fast, (e + f) / 2 + 1, "N={n} e={e}");
                assert_eq!(slow, f + 1, "slow quorum is f+1 regardless of electorate");
                // Invariant 1: a recovery quorum (f+1) always intersects a fast quorum.
                assert!(fast > f, "fast≥f+1 fails for N={n} e={e}");
                // Invariant 2: any two fast quorums of the electorate intersect.
                assert!(2 * fast > e, "2·fast>e fails for N={n} e={e}");
                // A fast quorum is reachable within the electorate.
                assert!(fast <= e, "fast≤e fails for N={n} e={e}");
            }
            // The full electorate (default) reproduces the classic ⌈3f/2⌉+1.
            assert_eq!(
                StaticTopology::new(NodeId(0), nodes.clone()).fast_quorum(&k),
                (3 * f).div_ceil(2) + 1,
                "default electorate is the classic fast quorum for N={n}"
            );
            // The smallest electorate (e=f+1) collapses fast to slow (f+1).
            let smallest = StaticTopology::new(NodeId(0), nodes.clone())
                .with_fast_electorate(nodes[..f + 1].to_vec());
            assert_eq!(
                smallest.fast_quorum(&k),
                f + 1,
                "e=f+1 ⇒ fast=slow for N={n}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn electorate_below_f_plus_one_panics() {
        // N=5, f=2: an electorate of 2 (< f+1=3) is unsafe.
        let nodes: Vec<NodeId> = (0..5).map(NodeId).collect();
        StaticTopology::new(NodeId(0), nodes.clone()).with_fast_electorate(nodes[..2].to_vec());
    }

    #[test]
    #[should_panic(expected = "not a replica")]
    fn electorate_with_a_non_replica_panics() {
        let nodes: Vec<NodeId> = (0..5).map(NodeId).collect();
        StaticTopology::new(NodeId(0), nodes).with_fast_electorate(vec![
            NodeId(0),
            NodeId(1),
            NodeId(99),
        ]);
    }

    #[test]
    fn sharded_per_shard_electorate() {
        // Two 3-node shards; shrink shard 0's electorate to 2 (region-local),
        // leave shard 1 at the default.
        let shards = vec![
            vec![NodeId(0), NodeId(1), NodeId(2)],
            vec![NodeId(3), NodeId(4), NodeId(5)],
        ];
        let topo = ShardedTopology::new(NodeId(0), shards)
            .with_fast_electorate(0, vec![NodeId(0), NodeId(1)]);
        // Find a key for each shard.
        let mut k0 = None;
        let mut k1 = None;
        for i in 0..1000 {
            let k = Key(format!("k{i}"));
            match topo.shard_of(&k) {
                0 if k0.is_none() => k0 = Some(k),
                1 if k1.is_none() => k1 = Some(k),
                _ => {}
            }
            if k0.is_some() && k1.is_some() {
                break;
            }
        }
        let (k0, k1) = (k0.unwrap(), k1.unwrap());
        // Shard 0: e=2, f=1 ⇒ fast=(2+1)/2+1=2. Shard 1: default e=3 ⇒ fast=3.
        assert_eq!(topo.fast_electorate(&k0).len(), 2);
        assert_eq!(topo.fast_quorum(&k0), 2);
        assert_eq!(topo.fast_quorum(&k1), 3);
    }

    fn regions(pairs: &[(u64, RegionId)]) -> std::collections::HashMap<NodeId, RegionId> {
        pairs.iter().map(|&(n, r)| (NodeId(n), r)).collect()
    }

    #[test]
    fn dynamic_electorate_is_region_derived() {
        let k = Key("x".into());
        let five = vec![vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]];

        // (a) No regions ⇒ whole shard ⇒ classic fast quorum (N=5,f=2 ⇒ 4).
        let plain = DynamicTopology::new(NodeId(0), 0, five.clone());
        assert_eq!(plain.fast_electorate(&k).len(), 5);
        assert_eq!(plain.fast_quorum(&k), 4);

        // (b) A={0,1,2}, B={3,4} ⇒ electorate {0,1,2}, fast=(3+2)/2+1=3.
        let split = DynamicTopology::new(NodeId(0), 0, five.clone()).with_regions(regions(&[
            (0, 0),
            (1, 0),
            (2, 0),
            (3, 1),
            (4, 1),
        ]));
        assert_eq!(
            split.fast_electorate(&k),
            vec![NodeId(0), NodeId(1), NodeId(2)]
        );
        assert_eq!(split.fast_quorum(&k), 3);

        // (e) All in one region ⇒ e == N, no shrink (whole shard, classic quorum).
        let one = DynamicTopology::new(NodeId(0), 0, five.clone()).with_regions(regions(&[
            (0, 7),
            (1, 7),
            (2, 7),
            (3, 7),
            (4, 7),
        ]));
        assert_eq!(one.fast_electorate(&k).len(), 5);
        assert_eq!(one.fast_quorum(&k), 4);

        // (d) Largest tagged region below f+1 (=3) ⇒ whole-shard fallback.
        let tiny = DynamicTopology::new(NodeId(0), 0, five).with_regions(regions(&[
            (0, 0),
            (1, 0),
            (2, 1),
        ])); // 3,4 untagged
        assert_eq!(tiny.fast_electorate(&k).len(), 5);
        assert_eq!(tiny.fast_quorum(&k), 4);
    }

    #[test]
    fn dynamic_electorate_tie_breaks_to_lowest_region() {
        // N=4 (f=1): two equal regions of 2. The lower RegionId must win on every
        // node so the derived electorate is identical cluster-wide.
        let k = Key("x".into());
        let shard = vec![vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)]];
        let topo = DynamicTopology::new(NodeId(0), 0, shard).with_regions(regions(&[
            (0, 5),
            (1, 5),
            (2, 2),
            (3, 2),
        ]));
        // Region 2 (the lower id) wins the tie ⇒ electorate {2,3}; f=1 ⇒ fast=2.
        assert_eq!(topo.fast_electorate(&k), vec![NodeId(2), NodeId(3)]);
        assert_eq!(topo.fast_quorum(&k), 2);
    }

    #[test]
    fn dynamic_electorate_survives_an_epoch_change() {
        // The electorate is a pure function of the installed layout, so an epoch
        // change re-derives it from the new layout — no extra state needed.
        let k = Key("x".into());
        let topo = DynamicTopology::new(
            NodeId(0),
            0,
            vec![vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]],
        )
        .with_regions(regions(&[(0, 0), (1, 0), (2, 0), (3, 1), (4, 1)]));
        // Epoch 0: N=5, electorate {0,1,2}, fast=3.
        assert_eq!(topo.fast_quorum(&k), 3);

        // Epoch 1: shard becomes {0,1,3} — region 0 holds {0,1}, region 1 {3}.
        // N=3, f=1; largest region {0,1} (e=2 ≥ f+1) ⇒ electorate {0,1}, fast=2.
        topo.install(1, vec![vec![NodeId(0), NodeId(1), NodeId(3)]]);
        assert_eq!(topo.fast_electorate(&k), vec![NodeId(0), NodeId(1)]);
        assert_eq!(topo.fast_quorum(&k), 2);
    }
}
