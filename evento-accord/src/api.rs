//! Host-facing trait interfaces, modelled on `accord-core`'s `accord/api`
//! package. The protocol core depends only on these traits; each has a
//! deterministic in-process implementation for the simulation harness and a
//! production implementation (real transport, static-config membership,
//! Fjall/SQL-backed storage).

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use evento_core::{
    cursor::{Args, ReadResult, Value},
    Event, EventFilter, RoutingKey, TenantFilter,
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

    /// Sends `message` to every node in `nodes`. The default clones the
    /// message per peer; transports that serialize should override it to
    /// encode once and share the buffer across peers (see `TcpTransport`).
    async fn broadcast(&self, nodes: &[NodeId], message: Message) -> anyhow::Result<()> {
        for &node in nodes {
            let _ = self.send(node, message.clone()).await;
        }
        Ok(())
    }
}

/// Cluster membership and key→replica-set mapping.
///
/// A topology hands out its current shard [`Layout`] as an immutable `Arc`
/// snapshot ([`layout`](Topology::layout)), and the layout it installed for
/// an earlier epoch ([`layout_at`](Topology::layout_at)). A coordination
/// reads one snapshot for its whole life, and a transaction is planned,
/// voted on and recovered under the layout of the epoch it was coordinated
/// in, however many epochs are installed meanwhile. The per-key methods read
/// the current layout, one snapshot per call.
///
/// Implementations: [`StaticTopology`] (a fixed node list, single shard),
/// [`ShardedTopology`] (disjoint, hash-partitioned shards) and
/// [`DynamicTopology`] (epoch-versioned layouts).
pub trait Topology: Send + Sync + 'static {
    /// The node this process is running as.
    fn this_node(&self) -> NodeId;

    /// The current layout, as an immutable snapshot.
    fn layout(&self) -> Arc<Layout>;

    /// The layout installed for `epoch`, if this topology holds it — the
    /// current one, or for a [`DynamicTopology`] any it installed before.
    /// `None` for an epoch this node never installed (it starts later, or
    /// the epoch is still to come).
    fn layout_at(&self, epoch: u64) -> Option<Arc<Layout>> {
        let layout = self.layout();
        (layout.epoch() == epoch).then_some(layout)
    }

    /// Monotonic topology version. Bumped on every membership change.
    fn epoch(&self) -> u64 {
        self.layout().epoch()
    }

    /// All nodes in the cluster.
    fn nodes(&self) -> Vec<NodeId> {
        self.layout().nodes()
    }

    /// Replicas that own `key` — the set a transaction touching `key` must reach
    /// quorum within.
    fn replicas(&self, key: &Key) -> Vec<NodeId> {
        self.layout().replicas(key).to_vec()
    }

    /// The **fast-path electorate** for `key`'s shard — the subset of
    /// [`replicas`](Topology::replicas) whose `PreAccept` votes decide the
    /// one-round-trip fast path (see [`Layout::electorate_of`]).
    fn fast_electorate(&self, key: &Key) -> Vec<NodeId> {
        let layout = self.layout();
        layout.electorate_of(layout.shard_of(key)).to_vec()
    }

    /// Number of replicas owning `key` — [`replicas`](Topology::replicas)
    /// `.len()` without the `Vec`.
    fn replica_count(&self, key: &Key) -> usize {
        self.layout().replicas(key).len()
    }

    /// Size of `key`'s [`fast_electorate`](Topology::fast_electorate), without
    /// materialising the set.
    fn fast_electorate_size(&self, key: &Key) -> usize {
        let layout = self.layout();
        layout.electorate_of(layout.shard_of(key)).len()
    }

    /// Fast-path quorum size for `key` (see [`Layout::fast_quorum_of`]).
    fn fast_quorum(&self, key: &Key) -> usize {
        let layout = self.layout();
        layout.fast_quorum_of(layout.shard_of(key))
    }

    /// Slow-path / recovery quorum size for `key`'s replica set: a strict
    /// majority, `⌊N/2⌋ + 1` (equal to `f + 1` for odd `N = 2f + 1`).
    fn slow_quorum(&self, key: &Key) -> usize {
        let layout = self.layout();
        layout.slow_quorum_of(layout.shard_of(key))
    }

    /// The shard that owns `key`. Single-shard topologies return `0`.
    fn shard_of(&self, key: &Key) -> ShardId {
        self.layout().shard_of(key)
    }

    /// The shard `node` replicates, if any. Shards are disjoint, so a node
    /// belongs to at most one.
    fn node_shard(&self, node: NodeId) -> Option<ShardId> {
        self.layout().node_shard(node)
    }

    /// Whether `node` replicates `key` (i.e. owns its shard). Determines which
    /// events of a multi-shard transaction a replica reads and applies.
    fn owns(&self, node: NodeId, key: &Key) -> bool {
        self.layout().owns(node, key)
    }

    /// The region / datacenter `node` lives in, if regions are configured. Used to
    /// prefer a same-region peer (e.g. for read routing). The default is `None`
    /// (no region awareness — all nodes treated as one locality).
    fn region(&self, _node: NodeId) -> Option<RegionId> {
        None
    }

    /// Installs a decided epoch's shard layout. The newest installed epoch is
    /// the current layout; an older one is only kept for
    /// [`layout_at`](Topology::layout_at). The default is a no-op (fixed
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

/// One epoch's shard layout, immutable once built: the replica set of every
/// shard, each shard's fast-path electorate, and the quorum sizes they imply.
/// Shards are disjoint, and a key's shard is a stable hash of the key modulo
/// the shard count, so every node holding the same layout maps every key the
/// same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    epoch: u64,
    shards: Vec<Vec<NodeId>>,
    /// Shard id → its fast-path electorate, a subset of its replicas.
    electorates: Vec<Vec<NodeId>>,
}

impl Layout {
    /// A layout at `epoch` whose electorates are the whole shards.
    pub fn new(epoch: u64, shards: Vec<Vec<NodeId>>) -> Self {
        let electorates = shards.clone();
        Self {
            epoch,
            shards,
            electorates,
        }
    }

    /// A layout with `electorates[shard]` as each shard's fast-path
    /// electorate (asserted well-formed by whoever configured it).
    fn with_electorates(
        epoch: u64,
        shards: Vec<Vec<NodeId>>,
        electorates: Vec<Vec<NodeId>>,
    ) -> Self {
        debug_assert_eq!(shards.len(), electorates.len());
        Self {
            epoch,
            shards,
            electorates,
        }
    }

    /// The epoch this layout was decided for.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Shard id → replica set.
    pub fn shards(&self) -> &[Vec<NodeId>] {
        &self.shards
    }

    /// Every node of every shard.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.shards.iter().flatten().copied().collect()
    }

    /// Whether `node` is in some shard of this layout.
    pub fn contains(&self, node: NodeId) -> bool {
        self.shards.iter().any(|shard| shard.contains(&node))
    }

    /// The shard that owns `key`.
    pub fn shard_of(&self, key: &Key) -> ShardId {
        if self.shards.len() <= 1 {
            return 0;
        }
        // Stable across nodes AND toolchains (unlike DefaultHasher).
        (stable_hash(key.0.as_bytes()) as usize) % self.shards.len()
    }

    /// `shard`'s replica set.
    pub fn replicas_of(&self, shard: ShardId) -> &[NodeId] {
        &self.shards[shard]
    }

    /// The replica set of `key`'s shard.
    pub fn replicas(&self, key: &Key) -> &[NodeId] {
        self.replicas_of(self.shard_of(key))
    }

    /// `shard`'s fast-path electorate: the subset of its replicas whose
    /// `PreAccept` votes decide the one-round-trip fast path. It is a fixed,
    /// cluster-agreed property of the shard, every coordinator of the shard
    /// uses the same set, with `f + 1 ≤ |E| ≤ N`: that bound keeps recovery
    /// sound — a recovery quorum (`f + 1`) always intersects a fast quorum,
    /// and any two fast quorums of the shared electorate intersect.
    pub fn electorate_of(&self, shard: ShardId) -> &[NodeId] {
        &self.electorates[shard]
    }

    /// `shard`'s fast-path quorum size: `⌊(e + f)/2⌋ + 1` where `e` is its
    /// electorate's size and its replica set has size `N = 2f + 1`. Reduces
    /// to `⌈3f/2⌉ + 1` when the electorate is the whole replica set.
    pub fn fast_quorum_of(&self, shard: ShardId) -> usize {
        let f = self.shards[shard].len().saturating_sub(1) / 2;
        fast_quorum_size(self.electorates[shard].len(), f)
    }

    /// `shard`'s slow-path / recovery quorum size: a strict majority.
    pub fn slow_quorum_of(&self, shard: ShardId) -> usize {
        slow_quorum_size(self.shards[shard].len())
    }

    /// The shard `node` replicates, if any.
    pub fn node_shard(&self, node: NodeId) -> Option<ShardId> {
        self.shards.iter().position(|shard| shard.contains(&node))
    }

    /// Whether `node` replicates `key`.
    pub fn owns(&self, node: NodeId, key: &Key) -> bool {
        self.replicas(key).contains(&node)
    }

    /// Whether `key` has the same replica set here and in `other`: the same
    /// nodes vote on, apply and answer for it under both layouts, whatever
    /// its shard's index.
    pub fn same_replicas(&self, other: &Layout, key: &Key) -> bool {
        let mut here = self.replicas(key).to_vec();
        let mut there = other.replicas(key).to_vec();
        here.sort_unstable();
        there.sort_unstable();
        here == there
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

/// What a replica keeps of an applied **multi-shard** transaction once it
/// compacted the command away: its decision, enough to answer a recovery
/// from another route shard that still holds the transaction undecided
/// (the redundancy watermark is per shard, so that shard may not have
/// applied it yet). Kept, and journaled ([`Journal::save_decision`]), until
/// every replica of every other route shard has applied it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRecord {
    /// The transaction.
    pub txn: TxnId,
    /// The epoch it was coordinated under: its route's shards are that
    /// epoch's.
    pub epoch: u64,
    /// Its execution timestamp — [`INVALIDATED_AT`](crate::message::INVALIDATED_AT)
    /// for one a recovery invalidated.
    pub execute_at: Timestamp,
    /// The decision: `true` for an abort, `false` for a commit.
    pub conflict: bool,
    /// Its route — the full key set, every shard it touches.
    pub route: Vec<Key>,
}

/// The fence of a replica that came back over an empty journal under its old
/// node id (#312): it forgot the votes, promises and reports it made, and it
/// joins again only through a contact that vouches past them. Journaled
/// ([`Journal::save_amnesia_fence`]) before the replica does anything, and
/// kept for good — it also marks the replica as an acceptor that abstains
/// from config Paxos under this id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmnesiaFence {
    /// Microseconds above every timestamp the replica can have voted on,
    /// promised, minted or reported before the loss: its wall clock when it
    /// reopened, plus three skews.
    pub fence_micros: u64,
    /// Whether the replica has joined through a contact that vouched at or
    /// above the fence. Until then it takes no part, and must open joining.
    pub joined: bool,
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
    /// [`flush`](Journal::flush). The default is the immediate
    /// [`record`](Journal::record) — fine for journals that do not (or need
    /// not) batch their fsync.
    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        self.record(state).await
    }

    /// Makes every write [`stage`](Journal::stage)d since the last flush durable,
    /// in one operation (a group-commit fsync). The default is a no-op, since a
    /// non-batching journal's [`record`](Journal::record)/[`stage`](Journal::stage)
    /// are already durable.
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

    /// Persists, for each peer this node stopped waiting for (see
    /// `NodeConfig::peer_absence_limit`), the highest compacted transaction that
    /// peer must prove it holds before it is counted again. Replaces the
    /// previous set; an empty slice clears it. A restart restores the set with
    /// [`load_rejoin_bounds`](Journal::load_rejoin_bounds), so a peer that
    /// returns after this node restarted is still told to rejoin. The default
    /// is a no-op (a journal that forgets them is merely less strict: the peer
    /// is counted again as soon as it reports).
    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        let _ = bounds;
        Ok(())
    }

    /// The persisted rejoin bounds, if any. The default returns none.
    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        Ok(Vec::new())
    }

    /// Buffers the route — full key set — of a multi-shard transaction this
    /// replica holds, durable no later than the next [`flush`](Journal::flush)
    /// (like [`stage`](Journal::stage)). A restarted replica that lost it could
    /// recover the transaction on its own shard only. Discarded by
    /// [`truncate`](Journal::truncate) with the commands. The default is a
    /// no-op (a journal that forgets routes recovers such a transaction from
    /// what the replies reveal, as before routes existed).
    async fn save_route(&self, txn: TxnId, route: &[Key]) -> anyhow::Result<()> {
        let _ = (txn, route);
        Ok(())
    }

    /// Every persisted route. The default returns none.
    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        Ok(Vec::new())
    }

    /// Buffers the recovery `ballot` promised for a transaction this replica
    /// never witnessed, durable no later than the next
    /// [`flush`](Journal::flush). A replica that forgot it after a restart
    /// would let the original coordinator's late PreAccept win a vote the
    /// recovery decided without. Discarded by [`truncate`](Journal::truncate)
    /// with the commands. The default is a no-op (promises then last only as
    /// long as the process).
    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        let _ = (txn, ballot);
        Ok(())
    }

    /// Every persisted promise. The default returns none.
    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        Ok(Vec::new())
    }

    /// Buffers the decision record of a multi-shard transaction this replica
    /// applied and compacted away, durable no later than the next
    /// [`flush`](Journal::flush). The node stages it, and flushes, before
    /// the [`truncate`](Journal::truncate) that deletes the command, so the
    /// decision is never lost in between. **Not** discarded by `truncate`:
    /// it outlives the command until [`drop_decisions`](Journal::drop_decisions).
    /// The default is a no-op (a journal that forgets them leaves another
    /// shard's recovery of such a transaction waiting on a replica that
    /// still holds it applied).
    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        let _ = record;
        Ok(())
    }

    /// Every persisted decision record. The default returns none.
    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        Ok(Vec::new())
    }

    /// Discards the decision records of `txns`: every route shard applied
    /// them. Need not be durable at once — a record dropped late is only
    /// kept longer. The default is a no-op.
    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        let _ = txns;
        Ok(())
    }

    /// Durably persists this node's **clock bound**: every timestamp it has
    /// minted (transaction ids, ballots) is below `micros`. Durable by the
    /// time it returns — the node raises it before minting past it, and a
    /// restart starts the clock from it ([`load_clock_bound`](Journal::load_clock_bound)),
    /// so a restarted node never mints an id it used before, not even after
    /// its wall clock stepped back. Only ever raised; survives
    /// [`truncate`](Journal::truncate). The default is a no-op (a restarted
    /// node then starts its clock past its own journaled transactions only).
    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        let _ = micros;
        Ok(())
    }

    /// The persisted clock bound, if any. The default returns none.
    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        Ok(None)
    }

    /// Durably persists the [`AmnesiaFence`] of a replica that came back over
    /// an empty journal (#312), replacing any previous one. Durable by the
    /// time it returns, and **never** discarded by
    /// [`truncate`](Journal::truncate). The default is a no-op: the node
    /// reads the fence back before relying on it, so a journal that cannot
    /// keep it refuses such a replica — which must then come back under a new
    /// node id.
    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        let _ = fence;
        Ok(())
    }

    /// The persisted [`AmnesiaFence`], if any. The default returns none.
    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
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

    /// Durably appends a run of decided metadata-log entries at once, with the
    /// same per-entry idempotency as [`append_metadata`](Journal::append_metadata).
    /// Journals that batch their fsync should override this to make the whole run
    /// durable with one sync instead of one per entry. The default loops
    /// [`append_metadata`](Journal::append_metadata).
    async fn append_metadata_batch(
        &self,
        entries: &[(u64, Vec<Vec<NodeId>>)],
    ) -> anyhow::Result<()> {
        for (epoch, layout) in entries {
            self.append_metadata(*epoch, layout).await?;
        }
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
    /// Current version of the stream `(tenant, aggregate_type,
    /// aggregate_id)`. Returns 0 for a stream with no events. Used during the
    /// Read phase to evaluate the optimistic-version condition before any
    /// shard appends.
    async fn version(
        &self,
        tenant: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<u16>;

    /// Records a decided transaction at its execution timestamp. When
    /// `commit` is true the `events` are appended (advancing versions);
    /// otherwise the transaction is a no-op (an aborted conditional write).
    /// Implementations must apply in `execute_at` order to preserve the global
    /// serial order. `events` is only this replica's owned subset.
    ///
    /// A committed transaction's events arrive already stamped from
    /// `execute_at`, floored to milliseconds — the same stamp on every replica,
    /// rising with the serial order on every key — so implementations must
    /// persist them **verbatim**: the stamps are the subscription cursor, and
    /// the node's stability watermark is computed from them. A bootstrap
    /// snapshot page arrives with the contact's stored stamps, also verbatim.
    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()>;

    /// Whether this store already holds every event of `events`: each
    /// aggregate's event at that version is that very event: the same id,
    /// tenant, aggregate, version, name, routing key, data and metadata (only
    /// its stamps may differ). It tells a transaction this replica applied before —
    /// and forgot: its
    /// journal record lost across a restart, or applied by an anti-entropy
    /// import, which is not journaled — from a conflict. Its own events in
    /// place, its condition held, and applying it again changes nothing.
    /// The default answers `false`: cannot tell.
    async fn holds(&self, _events: &[Event]) -> anyhow::Result<bool> {
        Ok(false)
    }

    /// Serves a read query from the local backend, so a node can answer reads
    /// forwarded to it for a key range it owns. `to_micros` is the forwarded
    /// exclusive stamp bound (see `evento_core::Executor::read`). The default
    /// returns nothing (a store that holds no queryable events, e.g. the
    /// in-memory test store).
    async fn read(
        &self,
        _aggregators: Option<Arc<[EventFilter]>>,
        _routing_key: Option<RoutingKey>,
        _tenant: TenantFilter,
        _args: Args,
        _to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        Ok(ReadResult::default())
    }

    /// All applied events materialised in this store, in one `Vec` — the
    /// state a joining node installs to reconstruct what log truncation has
    /// removed from the command journal. The default is empty (a store whose
    /// journal never truncates needs no snapshot).
    ///
    /// The node bootstraps joiners through [`snapshot_page`](Self::snapshot_page),
    /// which the default implementation pages over this method with an offset
    /// cursor: fine for a store that fits in memory, O(n) per page otherwise.
    /// A store that can grow past a few MiB should implement `snapshot_page`
    /// natively (as [`ExecutorDataStore`](crate::ExecutorDataStore) does).
    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        Ok(Vec::new())
    }

    /// One page of the bootstrap snapshot: the applied events after cursor
    /// `after` (`None` starts from the beginning), at most `max_events` of them
    /// and no more than `max_bytes` of [`snapshot_event_size`] in all — but
    /// always at least one event while any remain, so a single large event
    /// still crosses. `next` is `None` once nothing follows. Pages must be
    /// stable in order across calls (each aggregate's versions ascending), as
    /// the joiner applies them in sequence.
    async fn snapshot_page(
        &self,
        after: Option<Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<SnapshotPage> {
        let all = self.snapshot().await?;
        let offset = match after {
            None => 0,
            Some(cursor) => cursor
                .parse::<usize>()
                .map_err(|_| anyhow::anyhow!("not an offset snapshot cursor: {cursor:?}"))?,
        };
        let total = all.len();
        let remaining = all.into_iter().skip(offset);
        let mut events = Vec::new();
        let mut bytes = 0;
        for event in remaining {
            let size = snapshot_event_size(&event);
            if !events.is_empty()
                && (events.len() >= usize::from(max_events) || bytes + size > max_bytes)
            {
                break;
            }
            bytes += size;
            events.push(event);
            if events.len() >= usize::from(max_events) {
                break;
            }
        }
        let taken = offset + events.len();
        let next = (total > taken).then(|| Value(taken.to_string()));
        Ok(SnapshotPage { events, next })
    }
}

/// One page of a [`DataStore::snapshot_page`] bootstrap snapshot.
#[derive(Debug, Clone, Default)]
pub struct SnapshotPage {
    /// The events of this page, in the store's stable order.
    pub events: Vec<Event>,
    /// The cursor to pass as `after` for the next page; `None` after the last.
    pub next: Option<Value>,
}

/// The approximate size of `event` on the wire inside a
/// [`Message`](crate::Message): what a snapshot page's byte budget counts.
/// The fixed fields (id, version, timestamps, length prefixes) plus the
/// strings, the data and the bitcode-encoded metadata.
pub fn snapshot_event_size(event: &Event) -> usize {
    const FIXED: usize = 16 + 2 + 8 + 4 + 8 * 6;
    FIXED
        + event.aggregate_id.len()
        + event.aggregate_type.len()
        + event.name.len()
        + event.routing_key.as_ref().map_or(0, String::len)
        + event.tenant.len()
        + event.data.len()
        + bitcode::encode(&event.metadata).len()
}

/// Re-stamps a committed transaction's events from its agreed execution
/// timestamp, floored to **milliseconds** (`timestamp` seconds +
/// `timestamp_subsec` milliseconds — the precision every backend and the
/// `to_micros` bound assume).
///
/// The stamps an event carries in are its writing host's provisional ones,
/// and two hosts' skewed clocks could order an aggregate's v2 before its v1.
/// `execute_at` is agreed by consensus, identical on every replica, and rises
/// with the serial order of every key, so the stamp derived from it is too
/// (ties within a millisecond fall back to the version). Called at every
/// command apply — journal replay, execution, anti-entropy import — but not
/// at a snapshot install, whose events were already stamped by the replica
/// that applied them. The id (a ULID) is left alone.
pub(crate) fn stamp_events(execute_at: Timestamp, events: &mut [Event]) {
    let millis = execute_at.micros / 1_000;
    for event in events {
        event.timestamp = millis / 1_000;
        event.timestamp_subsec = (millis % 1_000) as u32;
    }
}

/// `micros` floored to a whole millisecond: the stamp [`stamp_events`] gives a
/// transaction executing at `micros`, in microseconds.
pub(crate) fn floor_millis(micros: u64) -> u64 {
    micros - micros % 1_000
}

/// Fast-path quorum size: `⌊(electorate + faults)/2⌋ + 1`. With the electorate set
/// to the whole replica set (`electorate = 2f + 1`) this is the classic
/// `⌈3f/2⌉ + 1`; shrinking the electorate toward `f + 1` shrinks the quorum toward
/// `f + 1`, enabling a single-region one-round-trip commit.
pub(crate) fn fast_quorum_size(electorate: usize, faults: usize) -> usize {
    (electorate + faults) / 2 + 1
}

/// Slow-path / recovery quorum size: a strict **majority** of the replica set.
///
/// This must be `⌊n/2⌋ + 1`, not `f + 1` with `f = (n-1)/2`: the two only agree
/// for odd `n`. For even `n` (e.g. a 4-node shard) `f + 1 = n/2`, and two
/// disjoint "quorums" of that size exist — recovery could then miss a value a
/// quorum already accepted, and two coordinators could decide differently.
pub(crate) fn slow_quorum_size(n: usize) -> usize {
    n / 2 + 1
}

/// A stable 64-bit FNV-1a hash for key→shard placement.
///
/// `DefaultHasher` is explicitly not guaranteed stable across Rust releases, so
/// two nodes built with different toolchains could derive different key→shard
/// maps — a silent split brain. FNV-1a is fixed forever.
pub(crate) fn stable_hash(bytes: &[u8]) -> u64 {
    stable_hash_extend(STABLE_HASH_SEED, bytes)
}

/// The FNV-1a offset basis: the state [`stable_hash`] starts from.
pub(crate) const STABLE_HASH_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// Folds `bytes` into a running [`stable_hash`] state, so a long sequence
/// can be hashed piecewise.
pub(crate) fn stable_hash_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Whether `stored` is `event` as a store keeps it: the same id, tenant,
/// aggregate, version, name, routing key, data and metadata. Only the stamps
/// may differ: a replica stamps a transaction's events from its agreed
/// `execute_at` as it applies them (see [`stamp_events`]).
pub(crate) fn same_event(stored: &Event, event: &Event) -> bool {
    stored.id == event.id
        && stored.tenant == event.tenant
        && stored.aggregate_type == event.aggregate_type
        && stored.aggregate_id == event.aggregate_id
        && stored.version == event.version
        && stored.name == event.name
        && stored.routing_key == event.routing_key
        && stored.data == event.data
        && stored.metadata == event.metadata
}

/// Checks that `shards` can serve as a shard layout: at least one shard, no
/// empty shard, and every node in exactly one shard, once. A layout without
/// shards leaves no shard for any key (and no acceptors for the next epoch);
/// an empty shard owns keys no replica can commit; a node listed twice counts
/// twice towards a quorum, and one in two shards has ambiguous ownership.
pub fn validate_layout(shards: &[Vec<NodeId>]) -> anyhow::Result<()> {
    anyhow::ensure!(!shards.is_empty(), "a layout needs at least one shard");
    let mut seen = std::collections::HashSet::new();
    for (shard, replicas) in shards.iter().enumerate() {
        anyhow::ensure!(!replicas.is_empty(), "shard {shard} has no replicas");
        for &node in replicas {
            anyhow::ensure!(
                seen.insert(node),
                "{node:?} is listed more than once (shards must be disjoint)"
            );
        }
    }
    Ok(())
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
    /// The one layout, at epoch 0: a single shard of every node.
    layout: Arc<Layout>,
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
            layout: Arc::new(Layout::new(0, vec![nodes])),
        }
    }

    /// Sets the fast-path electorate — the subset of nodes whose `PreAccept` votes
    /// decide the one-round-trip fast path (see [`Layout::electorate_of`]).
    /// Panics unless it is a subset of the nodes with `f + 1 ≤ |E| ≤ N`.
    pub fn with_fast_electorate(mut self, electorate: Vec<NodeId>) -> Self {
        let nodes = self.layout.shards()[0].clone();
        assert_valid_electorate(&electorate, &nodes);
        self.layout = Arc::new(Layout::with_electorates(0, vec![nodes], vec![electorate]));
        self
    }
}

impl Topology for StaticTopology {
    fn this_node(&self) -> NodeId {
        self.this
    }

    fn layout(&self) -> Arc<Layout> {
        Arc::clone(&self.layout)
    }
}

/// Range-/hash-partitioned [`Topology`] with **disjoint** shards: the key space
/// is split across shards, each owned by its own replica set, and every node
/// belongs to exactly one shard. A transaction touching keys in several shards
/// must reach a quorum in each. Used for M3 multi-shard transactions.
pub struct ShardedTopology {
    this: NodeId,
    /// The one layout, at epoch 0.
    layout: Arc<Layout>,
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
        Self {
            this,
            layout: Arc::new(Layout::new(0, shards)),
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
    /// [`Layout::electorate_of`]). Panics unless it is a subset of the shard's
    /// replicas with `f + 1 ≤ |E| ≤ N`.
    pub fn with_fast_electorate(mut self, shard: ShardId, electorate: Vec<NodeId>) -> Self {
        let shards = self.layout.shards().to_vec();
        assert_valid_electorate(&electorate, &shards[shard]);
        let mut electorates = self.layout.electorates.clone();
        electorates[shard] = electorate;
        self.layout = Arc::new(Layout::with_electorates(0, shards, electorates));
        self
    }
}

impl Topology for ShardedTopology {
    fn this_node(&self) -> NodeId {
        self.this
    }

    fn layout(&self) -> Arc<Layout> {
        Arc::clone(&self.layout)
    }

    fn region(&self, node: NodeId) -> Option<RegionId> {
        self.regions.get(&node).copied()
    }
}

/// A [`Topology`] whose shard layout can be replaced atomically as the cluster
/// grows. Each layout is tagged with a monotonically increasing **epoch**, and
/// every layout installed is kept ([`layout_at`](Topology::layout_at)), the
/// starting one included, so a transaction coordinated under an earlier epoch
/// is still planned, voted on and recovered under its own layout.
///
/// Unlike [`ShardedTopology`], the running node need not yet belong to any shard
/// (a joining node holds the current layout to find a peer to bootstrap from,
/// then [`install`](DynamicTopology::install)s the new epoch that includes it).
/// Coordinating the epoch change across the cluster is the node's job; callers
/// install an agreed layout.
pub struct DynamicTopology {
    this: NodeId,
    /// The installed layouts. Readers take the read lock only long enough to
    /// clone a layout's `Arc`, so a whole coordination reads one consistent
    /// layout without serialising on a lock.
    state: RwLock<DynamicState>,
    /// Optional node → region map for region-aware routing and the region-derived
    /// fast-path electorate (empty ⇒ no regions ⇒ the classic whole-set fast path).
    /// Static cluster config — the operator supplies the **same** map to every node,
    /// so all nodes derive the identical electorate from the identical layout.
    regions: std::collections::HashMap<NodeId, RegionId>,
}

struct DynamicState {
    /// The newest installed layout.
    current: Arc<Layout>,
    /// Every installed layout by epoch, `current` included.
    history: std::collections::BTreeMap<u64, Arc<Layout>>,
}

impl DynamicTopology {
    /// Builds a topology at `epoch` from disjoint shard replica sets. `this`
    /// need not be a member yet.
    ///
    /// Panics if the layout is invalid (see [`validate_layout`]) — a
    /// configuration error.
    pub fn new(this: NodeId, epoch: u64, shards: Vec<Vec<NodeId>>) -> Self {
        if let Err(err) = validate_layout(&shards) {
            panic!("invalid shard layout: {err}");
        }
        let regions = std::collections::HashMap::new();
        let current = Arc::new(Self::derive(epoch, shards, &regions));
        Self {
            this,
            state: RwLock::new(DynamicState {
                history: std::collections::BTreeMap::from([(epoch, Arc::clone(&current))]),
                current,
            }),
            regions,
        }
    }

    /// Tags nodes with regions, enabling region-aware read routing and the
    /// **region-derived fast-path electorate** (a node absent from the map has no
    /// region). The map must be identical on every node.
    pub fn with_regions(mut self, regions: std::collections::HashMap<NodeId, RegionId>) -> Self {
        self.regions = regions;
        let state = self.state.get_mut().expect("topology poisoned");
        for layout in state.history.values_mut() {
            *layout = Arc::new(Self::derive(
                layout.epoch(),
                layout.shards().to_vec(),
                &self.regions,
            ));
        }
        state.current = Arc::clone(state.history.values().next_back().expect("one layout"));
        self
    }

    /// The layout of `shards` at `epoch`, with each shard's electorate
    /// derived from the region tags (see `DynamicTopology::electorate_for`).
    fn derive(
        epoch: u64,
        shards: Vec<Vec<NodeId>>,
        regions: &std::collections::HashMap<NodeId, RegionId>,
    ) -> Layout {
        let electorates = (0..shards.len())
            .map(|idx| Self::electorate_for(&shards, idx, regions))
            .collect();
        Layout::with_electorates(epoch, shards, electorates)
    }

    /// Derives shard `idx`'s fast-path electorate from the layout + region tags:
    /// the **largest in-region group** of that shard's replicas (ties break to the
    /// lowest [`RegionId`], so every node — holding the same layout + map — derives
    /// the same set). Used iff that group has size in `[f+1, N]` (a recovery-sound
    /// electorate; `== N` when one region holds the shard, i.e. no shrink);
    /// otherwise (no regions, or the largest region is below `f+1`) the whole shard.
    /// A pure function of the layout + region tags, so it is recomputed
    /// correctly for every epoch with no extra state crossing consensus.
    fn electorate_for(
        shards: &[Vec<NodeId>],
        idx: usize,
        regions: &std::collections::HashMap<NodeId, RegionId>,
    ) -> Vec<NodeId> {
        let replicas = &shards[idx];
        match Self::electorate_region(replicas, regions) {
            Some(r) => replicas
                .iter()
                .copied()
                .filter(|n| regions.get(n) == Some(&r))
                .collect(),
            None => replicas.clone(),
        }
    }

    /// The region whose group forms the fast-path electorate, if any: the
    /// largest in-region group of the replicas, provided it has a
    /// recovery-sound size (`>= f + 1`). `None` ⇒ the whole replica set.
    fn electorate_region(
        replicas: &[NodeId],
        regions: &std::collections::HashMap<NodeId, RegionId>,
    ) -> Option<RegionId> {
        if regions.is_empty() {
            return None;
        }
        let f = replicas.len().saturating_sub(1) / 2;
        // Distinct regions present among the replicas, ascending so a size tie
        // resolves to the lowest RegionId (the strict `>` keeps the incumbent).
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
            Some((r, count)) if count > f => Some(r),
            _ => None,
        }
    }
}

impl Topology for DynamicTopology {
    fn this_node(&self) -> NodeId {
        self.this
    }

    fn layout(&self) -> Arc<Layout> {
        Arc::clone(&self.state.read().expect("topology poisoned").current)
    }

    fn layout_at(&self, epoch: u64) -> Option<Arc<Layout>> {
        self.state
            .read()
            .expect("topology poisoned")
            .history
            .get(&epoch)
            .cloned()
    }

    fn region(&self, node: NodeId) -> Option<RegionId> {
        self.regions.get(&node).copied()
    }

    /// Keeps `epoch`'s layout (the first one installed for an epoch wins) and
    /// makes it the current layout if it is newer than the current one.
    fn install(&self, epoch: u64, shards: Vec<Vec<NodeId>>) {
        let mut state = self.state.write().expect("topology poisoned");
        if state.history.contains_key(&epoch) {
            return;
        }
        let layout = Arc::new(Self::derive(epoch, shards, &self.regions));
        state.history.insert(epoch, Arc::clone(&layout));
        if epoch > state.current.epoch() {
            state.current = layout;
        }
    }

    fn dynamic(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default `snapshot_page` pages `snapshot()` with an offset cursor:
    /// stable pages, a `next` while anything remains, and a refusal of a
    /// cursor it did not mint.
    #[tokio::test]
    async fn default_snapshot_page_offsets_over_snapshot() {
        let store = crate::store::InMemoryDataStore::new();
        let events: Vec<Event> = (1..=5u16)
            .map(|version| Event {
                aggregate_type: "test/Account".into(),
                aggregate_id: "acc".into(),
                version,
                name: "Tick".into(),
                ..Default::default()
            })
            .collect();
        store
            .apply(TxnId(Timestamp::MIN), Timestamp::MIN, events, true)
            .await
            .unwrap();

        let first = store.snapshot_page(None, 2, usize::MAX).await.unwrap();
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.next.as_deref().map(String::as_str), Some("2"));
        let second = store
            .snapshot_page(first.next, 2, usize::MAX)
            .await
            .unwrap();
        assert_eq!(second.events[0].version, 3);
        assert_eq!(second.next.as_deref().map(String::as_str), Some("4"));
        let last = store
            .snapshot_page(second.next, 2, usize::MAX)
            .await
            .unwrap();
        assert_eq!(last.events.len(), 1);
        assert_eq!(last.events[0].version, 5);
        assert!(last.next.is_none());

        // A byte budget under one event still moves one event per page.
        let tiny = store.snapshot_page(None, 10, 1).await.unwrap();
        assert_eq!(tiny.events.len(), 1);
        assert_eq!(tiny.next.as_deref().map(String::as_str), Some("1"));

        // An empty store is one empty, final page.
        let empty = crate::store::InMemoryDataStore::new();
        let page = empty.snapshot_page(None, 10, usize::MAX).await.unwrap();
        assert!(page.events.is_empty() && page.next.is_none());

        assert!(store
            .snapshot_page(Some(Value("not-an-offset".into())), 2, usize::MAX)
            .await
            .is_err());
    }

    /// Events are stamped with `execute_at` floored to the millisecond:
    /// seconds + milliseconds, whatever the writer had put there, and the
    /// stamp in microseconds is exactly `floor_millis(execute_at)`.
    #[test]
    fn stamp_events_floors_execute_at_to_milliseconds() {
        let execute_at = Timestamp {
            micros: 1_700_000_123_456_789,
            logical: 7,
            node: NodeId(2),
        };
        let mut events = vec![
            Event {
                timestamp: 9_999_999_999,
                timestamp_subsec: 999,
                ..Default::default()
            },
            Event::default(),
        ];
        stamp_events(execute_at, &mut events);
        for event in &events {
            assert_eq!(event.timestamp, 1_700_000_123);
            assert_eq!(event.timestamp_subsec, 456);
            assert_eq!(
                event.timestamp * 1_000_000 + u64::from(event.timestamp_subsec) * 1_000,
                floor_millis(execute_at.micros)
            );
        }
        assert_eq!(floor_millis(1_999), 1_000);
        assert_eq!(floor_millis(2_000), 2_000);
    }

    fn topo(n: u64) -> StaticTopology {
        let nodes: Vec<NodeId> = (0..n).map(NodeId).collect();
        StaticTopology::new(NodeId(0), nodes)
    }

    /// Even replica-set sizes must still use a strict majority: `f + 1` with
    /// `f = (N-1)/2` would allow two disjoint "quorums" for N=4 (2+2) and a
    /// single-node quorum for N=2 — non-intersecting, hence unsafe.
    #[test]
    fn slow_quorum_is_a_strict_majority_for_even_n() {
        let k = Key("x".into());
        assert_eq!(topo(2).slow_quorum(&k), 2, "N=2 needs both nodes");
        assert_eq!(topo(4).slow_quorum(&k), 3, "N=4 needs 3, not 2");
        assert_eq!(topo(6).slow_quorum(&k), 4, "N=6 needs 4, not 3");

        let shards = vec![vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)]];
        let sharded = ShardedTopology::new(NodeId(0), shards.clone());
        assert_eq!(sharded.slow_quorum(&k), 3);
        let dynamic = DynamicTopology::new(NodeId(0), 0, shards);
        assert_eq!(dynamic.slow_quorum(&k), 3);
    }

    /// The key→shard hash must not depend on `DefaultHasher`'s unstable
    /// algorithm; FNV-1a is fixed forever. Golden values pin the placement so
    /// an accidental hash change fails loudly instead of splitting the cluster.
    #[test]
    fn shard_hash_is_stable() {
        assert_eq!(stable_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(stable_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
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
    fn validate_layout_accepts_disjoint_shards_only() {
        let n = |ids: &[u64]| ids.iter().copied().map(NodeId).collect::<Vec<_>>();
        assert!(validate_layout(&[n(&[0, 1, 2])]).is_ok());
        assert!(validate_layout(&[n(&[0, 1, 2]), n(&[3, 4, 5])]).is_ok());
        assert!(validate_layout(&[]).is_err(), "no shard");
        assert!(
            validate_layout(&[n(&[0, 1, 2]), n(&[])]).is_err(),
            "an empty shard"
        );
        assert!(
            validate_layout(&[n(&[0, 1]), n(&[1, 2])]).is_err(),
            "a node in two shards"
        );
        assert!(
            validate_layout(&[n(&[0, 0, 1])]).is_err(),
            "a node twice in one shard"
        );
    }

    #[test]
    #[should_panic(expected = "invalid shard layout")]
    fn dynamic_topology_refuses_an_invalid_layout() {
        DynamicTopology::new(NodeId(0), 0, vec![]);
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
