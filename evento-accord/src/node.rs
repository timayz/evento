//! A cluster node: one replica plus the coordinator logic for client writes and
//! for recovering transactions whose coordinator has failed.
//!
//! Each node runs a single inbox loop that processes incoming [`Message`]s
//! sequentially — so replica state mutates without data races — and routes
//! responses (tagged with the responder's id) back to whichever local
//! coordination awaits them.
//!
//! ## M3 scope
//!
//! Multi-shard: keys are partitioned across disjoint shards, and a transaction
//! touching several shards must reach a quorum in **each**. Execution is a
//! two-phase Read → Apply so a multi-aggregate write commits **atomically**: every
//! touched shard reads its owned keys' version condition, the coordinator commits
//! only if all hold, then every shard appends (or all abort). A replica only
//! reads/applies the events for keys it owns.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use evento_core::{
    cursor::{Args, Edge, ReadResult},
    Event, EventFilter, RoutingKey,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::api::{AcceptorRecord, DataStore, Journal, MessageSink, ShardId, Topology};
use crate::clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
use crate::failure_detector::FailureDetector;
use crate::message::{CommandState, Key, Message, Status, SyncKnown};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::replica::Replica;
use crate::transport::Envelope;

/// Tunable timing/sizing parameters for a [`Node`]. [`Default`] reproduces the
/// values the cluster shipped with; geo deployments raise the timeouts to match
/// cross-region round-trips. (The clock-skew bound lives on the injected
/// [`HybridLogicalClock`] — see `with_max_skew` — since only the clock reads it.)
#[derive(Debug, Clone, Copy)]
pub struct NodeConfig {
    /// Wait for the fast quorum before falling back to the slow path. Short so a
    /// down node barely delays a write.
    pub fast_timeout: Duration,
    /// Cap on waiting for a slow quorum at later phases (returns as soon as the
    /// quorum is reached; the cap only bounds a genuinely stuck phase).
    pub collect_timeout: Duration,
    /// How often the automatic-recovery sweep runs.
    pub recovery_interval: Duration,
    /// How often the anti-entropy repair round runs (hosted by the recovery
    /// sweep, but on its own — typically slower — cadence). Must stay well
    /// below [`compaction_margin`](Self::compaction_margin): a transaction a
    /// replica missed entirely can only be repaired until compaction drops it,
    /// so several anti-entropy rounds must fit inside the margin.
    pub anti_entropy_interval: Duration,
    /// A transaction unapplied this long is presumed stalled and recovered. Well
    /// above normal write latency so healthy in-flight writes are never disturbed.
    pub recovery_timeout: Duration,
    /// How far behind the present a node's reported redundancy point lags, so a
    /// not-yet-propagated commit is never skipped when state below the watermark is
    /// compacted away. Keep it above the clock-skew bound and propagation latency.
    pub compaction_margin: Duration,
    /// Most messages the inbox loop stages before forcing a group-commit flush.
    /// Caps the latency a deferred reply waits for its batch's fsync and the memory
    /// held for the batch's pending sends.
    pub max_journal_batch: usize,
    /// Suspicion threshold for the phi-accrual failure detector: a coordinator
    /// whose φ exceeds this is presumed dead and its stalled transactions are
    /// recovered (adaptively, ahead of `recovery_timeout`). Higher tolerates more
    /// latency variance before suspecting.
    pub phi_threshold: f64,
    /// Serve **linearizable** reads of a single owned key by coordinating a
    /// read-only barrier transaction (`Node::read_barrier`) before reading the
    /// local backend, instead of reading possibly-stale local state directly.
    /// Costs one consensus round per read; off by default (reads stay local and
    /// fast, but are only serializable — a read off a lagging replica can break
    /// real-time order).
    pub linearizable_reads: bool,
    /// Backpressure on the consensus backlog: if this node already holds at least
    /// this many un-compacted commands, it **refuses to coordinate a new write**
    /// (returning an error) rather than letting `Replica.commands` grow without bound
    /// — e.g. under a sustained partition where compaction stalls. Only the local
    /// coordinator's new-write entry point is gated; replica-side handling of peer
    /// consensus messages is never refused (that would break safety/liveness). Set
    /// high enough to never trip in a healthy cluster.
    pub max_commands: usize,
    /// Control-plane anti-dueling delay. An automatic config-change recovery defers
    /// by `rank * config_defer_step` (the **distinguished proposer** for an epoch has
    /// rank 0 and proposes first; others wait, then skip if it already committed), and
    /// a ballot duel backs off by a rank/attempt-derived multiple of this — so
    /// concurrent reconfigurations converge instead of livelocking. Deterministic
    /// (no RNG), so the simulation stays bit-reproducible.
    pub config_defer_step: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            fast_timeout: Duration::from_millis(50),
            collect_timeout: Duration::from_secs(5),
            recovery_interval: Duration::from_millis(100),
            anti_entropy_interval: Duration::from_millis(250),
            recovery_timeout: Duration::from_millis(300),
            compaction_margin: Duration::from_secs(1),
            max_journal_batch: 128,
            phi_threshold: 8.0,
            linearizable_reads: false,
            max_commands: 100_000,
            config_defer_step: Duration::from_millis(100),
        }
    }
}

/// The result of a coordinated (or recovered) write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitOutcome {
    /// The committed transaction's id.
    pub txn: TxnId,
    /// Whether the write was aborted by the optimistic-version condition
    /// (evento's `InvalidOriginalVersion`) — atomically, across all shards.
    pub conflict: bool,
}

/// The replica set and quorum sizes for one shard a transaction touches.
#[derive(Clone)]
struct ShardPlan {
    shard: ShardId,
    replicas: Vec<NodeId>,
    /// The fast-path electorate — the subset of `replicas` whose `PreAccept` votes
    /// decide the fast path. Equals `replicas` unless a smaller electorate is
    /// configured (region-favouring single-round-trip commits).
    electorate: Vec<NodeId>,
    fast_q: usize,
    slow_q: usize,
}

/// One replica's recovery report, extracted from a `RecoverOk`.
struct RecoverFields {
    known: bool,
    status: Status,
    accepted: Ballot,
    execute_at: Timestamp,
    deps: Vec<TxnId>,
    superseding_rejects: bool,
    keys: Vec<Key>,
    events: Vec<Event>,
}

/// A response to a `Recover`: a report, or a ballot rejection.
enum RecoverResp {
    Ok(RecoverFields),
    Nack(Ballot),
}

/// In-flight coordinations: transaction → channel delivering its tagged
/// responses `(responder, message)`.
type Pending = Arc<Mutex<HashMap<TxnId, mpsc::UnboundedSender<(NodeId, Message)>>>>;

/// An acceptor's single-decree-Paxos state for one config epoch.
#[derive(Clone)]
struct ConfigAcceptor {
    promised: Ballot,
    accepted: Option<(Ballot, Vec<Vec<NodeId>>)>,
}

impl Default for ConfigAcceptor {
    fn default() -> Self {
        Self {
            promised: Ballot(Timestamp {
                micros: 0,
                logical: 0,
                node: NodeId(0),
            }),
            accepted: None,
        }
    }
}

impl ConfigAcceptor {
    /// A durable snapshot of this acceptor for the journal.
    fn to_record(&self) -> AcceptorRecord {
        AcceptorRecord {
            promised: self.promised,
            accepted: self.accepted.clone(),
        }
    }

    /// Rebuilds in-memory acceptor state from a durable record (restart restore).
    fn from_record(record: AcceptorRecord) -> Self {
        Self {
            promised: record.promised,
            accepted: record.accepted,
        }
    }
}

/// Per-epoch config acceptor state.
type ConfigState = Arc<Mutex<HashMap<u64, ConfigAcceptor>>>;

/// Delivers config-Paxos responses to the in-flight epoch change per epoch, so
/// two concurrent proposals (e.g. a manual change and the sweep's recovery)
/// cannot steal each other's replies.
type ConfigInbox = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>;

/// The committed metadata log: decided `(epoch, layout)` entries, plus the highest
/// epoch actually installed into the [`Topology`]. The log is the source of truth
/// that drives `topology.install` in **strict, contiguous** epoch order — an entry
/// for a future epoch is held until every intervening epoch arrives, so no epoch is
/// ever skipped (the gap a behind node fills via [`Message::MetadataFetch`]).
struct MetadataLog {
    /// Decided entries by epoch (`BTreeMap` → deterministic ascending iteration).
    entries: BTreeMap<u64, Vec<Vec<NodeId>>>,
    /// Highest epoch installed into the topology so far (`== topology.epoch()`).
    installed_epoch: u64,
}

/// Shared metadata log.
type MetaLog = Arc<Mutex<MetadataLog>>;

/// A [`SyncData`](Message::SyncData) payload handed to an in-flight sync: the
/// contact's `(watermark, snapshot events, recent commands)`.
type SyncPayload = (Timestamp, Vec<Event>, Vec<CommandState>);

/// Delivers each sync response to its in-flight [`join`](Node::join)/anti-entropy
/// request, keyed by the request's correlation id — so a concurrent join and
/// anti-entropy round cannot steal each other's data.
type SyncInbox = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<SyncPayload>>>>;

/// A cluster node. Cheap to clone — all state is shared behind `Arc`.
#[derive(Clone)]
pub struct Node {
    id: NodeId,
    clock: Arc<HybridLogicalClock>,
    topology: Arc<dyn Topology>,
    sink: Arc<dyn MessageSink>,
    datastore: Arc<dyn DataStore>,
    journal: Arc<dyn Journal>,
    replica: Arc<Mutex<Replica>>,
    pending: Pending,
    /// Delivers the [`SyncData`](Message::SyncData) response to an in-flight
    /// [`join`](Node::join)/anti-entropy.
    sync_inbox: SyncInbox,
    /// True once this node may process consensus messages. A joining node clears
    /// it (via [`begin_join`](Node::begin_join)) so messages that arrive while it
    /// bootstraps are buffered and replayed, not dropped.
    bootstrapped: Arc<AtomicBool>,
    /// Consensus messages received while not yet bootstrapped, replayed by
    /// [`join`](Node::join) after the snapshot is imported.
    join_buffer: Arc<Mutex<Vec<Envelope>>>,
    /// This node's acceptor state per config epoch.
    config: ConfigState,
    /// Delivers config-Paxos responses to an in-flight epoch change.
    config_inbox: ConfigInbox,
    /// The committed metadata log — decided topology entries, installed in strict
    /// epoch order (drives [`Topology::install`]).
    meta_log: MetaLog,
    /// Monotonic correlation-id source, shared by forwarded reads, sync
    /// requests, and the anti-entropy/metadata peer rotation (each consumer
    /// only needs unique — not gap-free — values).
    correlation_seq: Arc<AtomicU64>,
    /// Channels awaiting forwarded-read replies, by correlation id.
    read_pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    /// Latest `applied_through` gossiped by each shard peer (and self); the
    /// recovery sweep compacts below the per-shard minimum of these.
    peer_watermarks: Arc<Mutex<HashMap<NodeId, Timestamp>>>,
    /// Phi-accrual liveness estimator: every inbound message is a heartbeat, and a
    /// coordinator suspected dead has its stalled transactions recovered.
    failure_detector: Arc<FailureDetector>,
    /// Bumped whenever local applied state advances (an apply or an
    /// anti-entropy import). Waiters (`read_barrier`, read-your-writes) select
    /// on it instead of polling the replica lock every millisecond.
    applied_gen: Arc<tokio::sync::watch::Sender<u64>>,
    /// When the last anti-entropy round ran, pacing it on
    /// [`NodeConfig::anti_entropy_interval`] independent of the recovery sweep.
    last_anti_entropy: Arc<Mutex<Option<Instant>>>,
    /// Runtime observability counters (writes, paths, recoveries, compactions, …).
    metrics: Arc<Metrics>,
    /// Tunable timing/sizing parameters (see [`NodeConfig`]).
    settings: NodeConfig,
}

impl Node {
    /// Builds a node from its injected dependencies.
    pub fn new(
        id: NodeId,
        topology: Arc<dyn Topology>,
        clock: Arc<HybridLogicalClock>,
        sink: Arc<dyn MessageSink>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
    ) -> Self {
        let start_epoch = topology.epoch();
        Self {
            id,
            clock,
            topology,
            sink,
            datastore,
            journal,
            replica: Arc::new(Mutex::new(Replica::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            sync_inbox: Arc::new(Mutex::new(HashMap::new())),
            bootstrapped: Arc::new(AtomicBool::new(true)),
            join_buffer: Arc::new(Mutex::new(Vec::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
            config_inbox: Arc::new(Mutex::new(HashMap::new())),
            meta_log: Arc::new(Mutex::new(MetadataLog {
                entries: BTreeMap::new(),
                installed_epoch: start_epoch,
            })),
            correlation_seq: Arc::new(AtomicU64::new(0)),
            read_pending: Arc::new(Mutex::new(HashMap::new())),
            peer_watermarks: Arc::new(Mutex::new(HashMap::new())),
            failure_detector: Arc::new(FailureDetector::new()),
            applied_gen: Arc::new(tokio::sync::watch::channel(0).0),
            last_anti_entropy: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Metrics::new()),
            settings: NodeConfig::default(),
        }
    }

    /// Overrides the timing/sizing [`NodeConfig`] (default reproduces the shipped
    /// values). Call before [`start`](Node::start); geo deployments raise the
    /// timeouts here.
    pub fn with_config(mut self, config: NodeConfig) -> Self {
        self.settings = config;
        self
    }

    /// This node's timing/sizing configuration.
    pub fn config(&self) -> NodeConfig {
        self.settings
    }

    /// Shares an external [`Metrics`] with this node, so counters this node records
    /// and counters a transport records (e.g. [`TcpTransport`](crate::tcp::TcpTransport)'s
    /// shed count) accumulate into one snapshot. Build the node and the transport with
    /// the *same* `Arc<Metrics>`, then read both through [`metrics`](Self::metrics).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// A handle to this node's shared [`Metrics`], e.g. to hand to a transport via
    /// [`TcpTransport::with_metrics`](crate::tcp::TcpTransport::with_metrics).
    pub fn metrics_handle(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Number of consensus commands currently held in memory — the state that
    /// compaction bounds. For tests and observability.
    pub fn command_count(&self) -> usize {
        self.replica
            .lock()
            .expect("replica poisoned")
            .command_count()
    }

    /// How many transactions this node has taken over via the recovery sweep — a
    /// healthy cluster recovers nothing. For tests and observability.
    pub fn recovery_count(&self) -> u64 {
        self.metrics.recoveries.load(Ordering::Relaxed)
    }

    /// Locally-safe subscription watermark, in microseconds since the Unix epoch:
    /// every committed transaction with a `t0` below this is already applied to
    /// the local data store, and no future transaction can be assigned a `t0`
    /// below it (future timestamps are `>= now - max_skew`, and this trails `now`
    /// by [`compaction_margin`](NodeConfig::compaction_margin)).
    ///
    /// It is the same `applied_through` point the compaction sweep trusts, so a
    /// subscription that only processes events below it cannot skip a late,
    /// lower-cursor event applied out of order from another node — provided
    /// propagation + clock skew stays within `compaction_margin` (the bound the
    /// node already assumes). Exposed to [`Executor::stable_timestamp`].
    pub fn stable_micros(&self) -> u64 {
        let now = self.clock.now().micros;
        let cutoff = Timestamp {
            micros: now.saturating_sub(self.settings.compaction_margin.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        };
        self.replica
            .lock()
            .expect("replica poisoned")
            .stable_event_micros(cutoff)
    }

    /// A point-in-time snapshot of this node's observability counters.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Spawns the inbox loop that drives this node until the network closes.
    ///
    /// **Group commit:** each iteration drains up to
    /// [`max_journal_batch`](NodeConfig::max_journal_batch) already-
    /// queued messages, stages each one's durable writes, flushes the journal
    /// **once** for the whole batch, then performs the deferred (durability-gated)
    /// sends. Under load a burst of consensus messages costs a single fsync; with
    /// one message in flight it behaves exactly as a per-message sync.
    pub fn start(&self, mut inbox: mpsc::Receiver<Envelope>) -> JoinHandle<()> {
        let node = self.clone();
        tokio::spawn(async move {
            while let Some(first) = inbox.recv().await {
                let mut sends = node.handle_staged(first).await;
                let mut batched = 1;
                while batched < node.settings.max_journal_batch {
                    match inbox.try_recv() {
                        Ok(env) => {
                            sends.extend(node.handle_staged(env).await);
                            batched += 1;
                        }
                        Err(_) => break,
                    }
                }
                // Only release the batch's durability-gated replies once the fsync
                // succeeds. If the flush fails (e.g. a disk error), withhold them —
                // a node that could not persist must not ack a decision as durable.
                // Dropping the replies is exactly the message loss quorums/recovery
                // already tolerate, so the cluster makes progress via durable peers.
                if node.journal.flush().await.is_ok() {
                    node.metrics.record_flush();
                    for (to, message) in sends {
                        node.send(to, message).await;
                    }
                } else {
                    tracing::warn!(
                        node = node.id.0,
                        "journal flush failed; withholding durability-gated sends"
                    );
                }
            }
        })
    }

    /// Spawns the automatic-recovery sweep: periodically takes over any
    /// transaction that has stalled (its coordinator presumed dead) and drives it
    /// to completion, so a crashed coordinator can never block progress forever.
    /// Opt-in — call alongside [`start`](Node::start) on a real deployment.
    pub fn start_recovery(&self) -> JoinHandle<()> {
        let node = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(node.settings.recovery_interval).await;
                node.recovery_sweep().await;
            }
        })
    }

    /// Rebuilds this node's state from the journal after a process restart:
    /// restores every command's consensus state (status, ballots, decision) and
    /// replays committed, applied transactions into the data store. Call once on
    /// startup before [`start`](Node::start). In-flight (unapplied) transactions
    /// resume via the recovery sweep.
    pub async fn recover_state(&self) -> anyhow::Result<()> {
        let watermark = self.journal.load_watermark().await?;
        let mut commands = self.journal.load_all().await?;
        commands.sort_by_key(|cmd| (cmd.execute_at, cmd.txn));

        // Restore the control plane: acceptor state (so a restarted acceptor keeps
        // its promised ballots — Paxos crash-safety) and the metadata log (so the
        // node re-installs the contiguous topology prefix it had decided).
        {
            let acceptors = self.journal.load_acceptors().await?;
            let mut config = self.config.lock().expect("config poisoned");
            for (epoch, record) in acceptors {
                config.insert(epoch, ConfigAcceptor::from_record(record));
            }
        }
        for (epoch, layout) in self.journal.load_metadata().await? {
            self.meta_log
                .lock()
                .expect("meta_log poisoned")
                .entries
                .insert(epoch, layout);
        }
        self.apply_log();

        {
            let mut replica = self.replica.lock().expect("replica poisoned");
            for cmd in &commands {
                replica.restore(cmd.clone());
            }
            // Restore the redundancy floor so a dependency on a transaction that
            // was compacted away before the restart (and is therefore absent from
            // the truncated journal) counts as already satisfied.
            if let Some(wm) = watermark {
                replica.compact(wm);
            }
        }
        // Replay the retained (post-truncation) applied tail into the data store.
        // The truncated prefix is not in the journal; a durable data store already
        // holds it, and re-applying the tail is idempotent.
        for cmd in &commands {
            if cmd.status == Status::Applied && cmd.applied_conflict == Some(false) {
                let _ = self
                    .datastore
                    .apply(cmd.txn, cmd.execute_at, cmd.events.clone(), true)
                    .await;
            }
        }
        Ok(())
    }

    /// Compacts consensus state below `before`: drops redundant commands from the
    /// in-memory replica and truncates the journal, bounding both to in-flight
    /// work. `before` must be a **cluster-safe** watermark — every replica has
    /// applied everything below it (the recovery sweep computes this as the
    /// per-shard minimum; supplied directly here and in tests). The journal is
    /// truncated to the replica's *actual* resulting watermark, which `compact`
    /// may clamp below `before` if a local transaction is not yet applied.
    pub async fn compact(&self, before: Timestamp) {
        let (previous, watermark) = {
            let mut replica = self.replica.lock().expect("replica poisoned");
            let previous = replica.redundant_before();
            replica.compact(before);
            (previous, replica.redundant_before())
        };
        if watermark > previous {
            self.metrics.record_compaction();
            tracing::debug!(node = self.id.0, ?watermark, "compacted consensus state");
        }
        let _ = self.journal.truncate(watermark).await;
    }

    /// Recovers stalled transactions whose coordinator is presumed dead, then runs
    /// one anti-entropy round against a peer.
    ///
    /// A candidate is any unapplied transaction pending past a short grace (one
    /// sweep interval). It is recovered when its coordinator is **suspected** by
    /// the phi-accrual failure detector (adaptive — fires as soon as the
    /// coordinator's heartbeats stop, ahead of the fixed timeout on a slow link),
    /// or when it has been stalled past `recovery_timeout` (the fixed fallback,
    /// which guarantees liveness even with no liveness history). Both paths are
    /// idempotent and ballot-fenced, so concurrent recoveries are safe.
    async fn recovery_sweep(&self) {
        let now = self.clock.now().micros;
        let micros_ago = |d: Duration| Timestamp {
            micros: now.saturating_sub(d.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        };
        let grace = micros_ago(self.settings.recovery_interval);
        let timeout = micros_ago(self.settings.recovery_timeout);

        let candidates = self.replica.lock().expect("replica poisoned").stuck(grace);
        for txn in candidates {
            let coordinator = txn.0.node;
            // Never recover a transaction this node is actively coordinating:
            // `recover` would replace the live coordination's response channel
            // and sabotage any write slower than `recovery_timeout`. (A crashed
            // -and-restarted coordinator has no registered channel, so its
            // stalled transactions are still recovered.)
            if coordinator == self.id
                && self
                    .pending
                    .lock()
                    .expect("pending poisoned")
                    .contains_key(&txn)
            {
                continue;
            }
            let suspected = coordinator != self.id
                && self
                    .failure_detector
                    .suspect(coordinator, self.settings.phi_threshold);
            let timed_out = txn.0 < timeout;
            if suspected || timed_out {
                self.metrics.record_recovery();
                tracing::debug!(
                    node = self.id.0,
                    ?txn,
                    suspected,
                    timed_out,
                    "recovering stalled transaction"
                );
                let _ = self.recover(txn).await;
            }
        }
        self.anti_entropy().await;
        self.advance_watermark().await;
        self.metadata_sweep().await;
    }

    /// Control-plane sweep: converge the metadata log without operator action.
    ///
    /// 1. **Gap-fill** — if this node holds a decided entry it cannot install
    ///    because an intervening epoch is missing (it was down/partitioned for an
    ///    epoch change), pull the missing run from a peer via [`Message::MetadataFetch`].
    /// 2. **Drive-to-completion** — if this node accepted a layout for an epoch that
    ///    was never committed (its coordinator died after the value was durable but
    ///    before commit), finish the change itself — the automatic equivalent of a
    ///    manual [`recover_topology`](Self::recover_topology).
    ///
    /// A no-op for static topologies (no epochs are ever decided), so a
    /// fixed-membership cluster issues no control-plane traffic.
    async fn metadata_sweep(&self) {
        if !self.topology.dynamic() {
            return;
        }

        // (1) Metadata anti-entropy: pull any entries above our installed epoch from
        // a rotating peer. Covers a node that missed the latest epochs entirely (no
        // local gap to detect) as well as one holding an uninstallable future entry;
        // the peer replies only if it has something newer.
        let peers: Vec<NodeId> = self
            .topology
            .nodes()
            .into_iter()
            .filter(|&n| n != self.id)
            .collect();
        if !peers.is_empty() {
            let next = self.correlation_seq.fetch_add(1, Ordering::Relaxed) as usize;
            let peer = peers[next % peers.len()];
            self.send(
                peer,
                Message::MetadataFetch {
                    after_epoch: self.installed_epoch(),
                },
            )
            .await;
        }

        // (2) Drive-to-completion: complete any accepted-but-uncommitted epoch.
        // Sort epochs so the order is deterministic (the in-memory map is a HashMap).
        let pending: Vec<u64> = {
            let installed = self.installed_epoch();
            let committed = self.meta_log.lock().expect("meta_log poisoned");
            let mut pending: Vec<u64> = self
                .config
                .lock()
                .expect("config poisoned")
                .iter()
                .filter(|(epoch, acc)| {
                    **epoch > installed
                        && acc.accepted.is_some()
                        && !committed.entries.contains_key(epoch)
                })
                .map(|(epoch, _)| *epoch)
                .collect();
            pending.sort_unstable();
            pending
        };
        for epoch in pending {
            let _ = self.recover_topology(epoch).await;
        }
    }

    /// Anti-entropy repair: pull any committed transactions a rotating peer has
    /// that this node missed (e.g. while it was briefly down). Recovery alone
    /// can't fix this — a transaction that completed on a quorum without this
    /// node is "stuck" nowhere — so a healed node converges via this sweep.
    async fn anti_entropy(&self) {
        // Paced on its own interval, independent of the (faster) recovery
        // sweep that hosts it.
        {
            let mut last = self
                .last_anti_entropy
                .lock()
                .expect("anti-entropy poisoned");
            if last.is_some_and(|at| at.elapsed() < self.settings.anti_entropy_interval) {
                return;
            }
            *last = Some(Instant::now());
        }
        let nodes = self.topology.nodes();
        let peers: Vec<NodeId> = nodes.into_iter().filter(|&n| n != self.id).collect();
        if peers.is_empty() {
            return;
        }
        let next = self.correlation_seq.fetch_add(1, Ordering::Relaxed) as usize;
        let peer = peers[next % peers.len()];
        // The digest lets the peer answer with only the commands this node is
        // missing, instead of its full applied set every round.
        let known = {
            let replica = self.replica.lock().expect("replica poisoned");
            let (since, txns) = replica.applied_digest();
            SyncKnown { since, txns }
        };
        let _ = self
            .import_from(
                peer,
                self.settings.anti_entropy_interval,
                false,
                Some(known),
            )
            .await;
    }

    /// Gossips this node's redundancy point to its shard peers and compacts below
    /// the per-shard minimum once every peer has reported. The point is its
    /// applied-through, lagged by [`compaction_margin`](NodeConfig::compaction_margin)
    /// so a not-yet-propagated
    /// commit is never skipped; the per-shard min ensures every replica has
    /// applied everything below the compaction watermark (a still-behind or
    /// partitioned peer holds the min down, and an unheard-from peer blocks
    /// compaction entirely), so dropping that state is safe.
    async fn advance_watermark(&self) {
        let now = self.clock.now().micros;
        let cutoff = Timestamp {
            micros: now.saturating_sub(self.settings.compaction_margin.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        };
        let mine = self
            .replica
            .lock()
            .expect("replica poisoned")
            .applied_through(cutoff);

        // The disjoint replica set this node belongs to.
        let all_nodes = self.topology.nodes();
        let my_shard = self.topology.node_shard(self.id);
        let shard_peers: Vec<NodeId> = all_nodes
            .iter()
            .copied()
            .filter(|&n| self.topology.node_shard(n) == my_shard)
            .collect();

        // Watermarks go to EVERY peer, not just shard peers: compaction only
        // consumes same-shard reports, but the message doubles as the periodic
        // heartbeat the phi-accrual failure detector needs — without it a quiet
        // cross-shard coordinator would be falsely suspected between writes.
        let peers: Vec<NodeId> = all_nodes
            .iter()
            .copied()
            .filter(|&n| n != self.id)
            .collect();
        self.broadcast(
            &peers,
            Message::Watermark {
                applied_through: mine,
            },
        )
        .await;

        let watermark = {
            let mut watermarks = self.peer_watermarks.lock().expect("watermarks poisoned");
            // Evict reports from nodes no longer in the topology, or a removed
            // peer's last (stale) watermark would hold compaction back forever.
            watermarks.retain(|node, _| all_nodes.contains(node));
            watermarks.insert(self.id, mine);
            let mut wm = mine;
            let mut complete = true;
            for &peer in &shard_peers {
                match watermarks.get(&peer) {
                    Some(&point) => wm = wm.min(point),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            complete.then_some(wm)
        };
        if let Some(watermark) = watermark {
            self.compact(watermark).await;
        }
    }

    // ----- inbox handling -------------------------------------------------

    /// Processes one inbound message in full: stages its durable writes, flushes
    /// the journal, then performs its (durability-gated) sends. Used by the
    /// bootstrap-replay path; the inbox loop calls
    /// [`handle_staged`](Self::handle_staged) directly so it can batch the flush
    /// across a whole drained batch of messages.
    async fn handle(&self, env: Envelope) {
        let sends = self.handle_staged(env).await;
        // Withhold the durability-gated sends if the fsync failed (see `start`).
        if self.journal.flush().await.is_ok() {
            self.metrics.record_flush();
            for (to, message) in sends {
                self.send(to, message).await;
            }
        } else {
            tracing::warn!(
                node = self.id.0,
                "journal flush failed; withholding durability-gated sends"
            );
        }
    }

    /// Applies one inbound message to local state, **staging** any journal writes,
    /// and returns the peer sends to perform once the batch is durable (the caller
    /// flushes the journal first). Local-channel responses and state changes run
    /// inline; only durability-gated peer sends are deferred so a reply is never
    /// observable before the decision behind it is durable.
    async fn handle_staged(&self, env: Envelope) -> Vec<(NodeId, Message)> {
        self.metrics.record_message();
        // Every inbound message is a liveness heartbeat from its sender.
        self.failure_detector.heartbeat(env.from);

        // While bootstrapping, buffer consensus messages for replay after the
        // snapshot is imported. Cluster-management messages (bootstrap + epoch
        // changes) pass through so the node can still sync and learn its layout.
        let control = matches!(
            env.message,
            Message::SyncRequest { .. }
                | Message::SyncData { .. }
                | Message::Watermark { .. }
                | Message::ConfigPrepare { .. }
                | Message::ConfigPromise { .. }
                | Message::ConfigAccept { .. }
                | Message::ConfigAccepted { .. }
                | Message::ConfigCommit { .. }
                | Message::ConfigNack { .. }
                | Message::MetadataFetch { .. }
                | Message::MetadataEntries { .. }
                | Message::ReadForward { .. }
                | Message::ReadReply { .. }
                | Message::ReadProbe { .. }
                | Message::ReadProbeOk { .. }
        );
        if !control {
            // Flag check and push under one lock: `join` flips the flag and
            // drains the buffer while holding it, so a message can never slip
            // into the buffer after the drain (it would be lost forever).
            let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
            if !self.bootstrapped.load(Ordering::Acquire) {
                buffer.push(env);
                return Vec::new();
            }
        }

        // Witness the peer's timestamp so this node's clock tracks the cluster
        // (bounded by `MAX_SKEW` in the clock) — keeping recovery/compaction
        // cutoffs valid for every node's transactions even under wall-clock drift.
        if let Some(txn) = env.message.txn() {
            self.clock.witness(txn.0);
        }

        let from = env.from;
        let mut out: Vec<(NodeId, Message)> = Vec::new();
        match env.message {
            Message::PreAccept { txn, keys, events } => {
                // Only handle the keys/events this node owns.
                let keys = self.owned_keys(keys);
                let events = self.owned_events(events);
                let (execute_at, deps) = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .preaccept(txn, keys, events);
                self.journal_stage(txn).await;
                out.push((
                    from,
                    Message::PreAcceptOk {
                        txn,
                        execute_at,
                        deps,
                    },
                ));
            }
            Message::Accept {
                txn,
                ballot,
                execute_at,
                deps,
                keys,
            } => {
                // A slow-path `execute_at` can exceed `t0`; witness it too.
                self.clock.witness(execute_at);
                let keys = self.owned_keys(keys);
                let reply = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .accept(txn, ballot, execute_at, deps, keys);
                self.journal_stage(txn).await;
                match reply {
                    Ok(deps) => out.push((from, Message::AcceptOk { txn, ballot, deps })),
                    Err(promised) => out.push((from, Message::Nack { txn, promised })),
                }
            }
            Message::Commit {
                txn,
                execute_at,
                deps,
                events,
                reply_to,
            } => {
                self.clock.witness(execute_at);
                // Keys derived from the FULL event set (then filtered to owned),
                // so a replica that missed PreAccept still indexes this
                // transaction in its conflict graph.
                let keys = self.owned_keys(Self::keys_of(&events));
                let events = self.owned_events(events);
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .commit(txn, execute_at, deps, events, keys, reply_to);
                self.journal_stage(txn).await;

                // If already applied (e.g. a second recovery), short-circuit the
                // read phase with the stored outcome.
                let applied = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .applied_result(txn);
                match applied {
                    Some(conflict) => {
                        out.push((reply_to, Message::ReadOk { txn, ok: !conflict }));
                    }
                    None => out.extend(self.drive_staged().await),
                }
            }
            Message::Apply { txn, commit } => {
                // Record the decision (idempotent) and acknowledge if it was
                // already applied; `drive` enacts it now if ready, or later once
                // dependencies clear (so a lagging replica still converges).
                let already_applied = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    let already = replica.applied_result(txn).is_some();
                    replica.record_decision(txn, commit);
                    already
                };
                if already_applied {
                    let reply_to = self.replica.lock().expect("replica poisoned").reply_to(txn);
                    if let Some(reply_to) = reply_to {
                        out.push((reply_to, Message::Applied { txn }));
                    }
                } else {
                    out.extend(self.drive_staged().await);
                }
            }
            Message::Recover { txn, ballot } => {
                let reply = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .recover(txn, ballot);
                let message = match reply {
                    Ok(state) => Message::RecoverOk {
                        txn,
                        ballot,
                        known: state.known,
                        status: state.status,
                        accepted: state.accepted,
                        execute_at: state.execute_at,
                        deps: state.deps,
                        superseding_rejects: state.superseding_rejects,
                        keys: state.keys,
                        events: state.events,
                    },
                    Err(promised) => Message::Nack { txn, promised },
                };
                out.push((from, message));
            }
            // Bootstrap / anti-entropy: a node asks for our committed state.
            Message::SyncRequest {
                id,
                snapshot,
                known,
            } => {
                let (watermark, commands) = {
                    let replica = self.replica.lock().expect("replica poisoned");
                    // With a digest (anti-entropy), ship only what the
                    // requester is missing — a healthy in-sync round clones
                    // and ships nothing. Bootstrap (no digest) still gets the
                    // full applied set.
                    let commands = match &known {
                        Some(known) => {
                            let have: HashSet<TxnId> = known.txns.iter().copied().collect();
                            replica.export_applied_missing(known.since, &have)
                        }
                        None => replica.export_applied(),
                    };
                    (replica.redundant_before(), commands)
                };
                // A bootstrapping joiner may be below our truncation watermark, so
                // it also needs the materialised state command replay no longer
                // covers; anti-entropy (snapshot = false) only needs the commands.
                let snapshot = if snapshot {
                    self.datastore.snapshot().await.unwrap_or_default()
                } else {
                    Vec::new()
                };
                out.push((
                    from,
                    Message::SyncData {
                        id,
                        watermark,
                        snapshot,
                        commands,
                    },
                ));
            }
            Message::SyncData {
                id,
                watermark,
                snapshot,
                commands,
            } => {
                let tx = self
                    .sync_inbox
                    .lock()
                    .expect("sync poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send((watermark, snapshot, commands));
                }
            }
            Message::Watermark { applied_through } => {
                self.peer_watermarks
                    .lock()
                    .expect("watermarks poisoned")
                    .insert(from, applied_through);
            }
            // Config Paxos — acceptor side.
            Message::ConfigPrepare { epoch, ballot } => {
                let (reply, record) = {
                    let mut config = self.config.lock().expect("config poisoned");
                    let acc = config.entry(epoch).or_default();
                    if ballot < acc.promised {
                        (
                            Message::ConfigNack {
                                epoch,
                                promised: acc.promised,
                            },
                            None,
                        )
                    } else {
                        acc.promised = ballot;
                        let (accepted_ballot, accepted_layout) = match &acc.accepted {
                            Some((b, l)) => (*b, Some(l.clone())),
                            None => (ConfigAcceptor::default().promised, None),
                        };
                        (
                            Message::ConfigPromise {
                                epoch,
                                ballot,
                                accepted_ballot,
                                accepted_layout,
                            },
                            Some(acc.to_record()),
                        )
                    }
                };
                // A promise must be durable before it is observable: a restarted
                // acceptor restores `promised` and so can never regress to a lower
                // ballot. Persist inline (before the deferred reply is sent).
                if let Some(record) = record {
                    let _ = self.journal.record_acceptor(epoch, &record).await;
                }
                out.push((from, reply));
            }
            Message::ConfigAccept {
                epoch,
                ballot,
                layout,
            } => {
                let (reply, record) = {
                    let mut config = self.config.lock().expect("config poisoned");
                    let acc = config.entry(epoch).or_default();
                    if ballot < acc.promised {
                        (
                            Message::ConfigNack {
                                epoch,
                                promised: acc.promised,
                            },
                            None,
                        )
                    } else {
                        acc.promised = ballot;
                        acc.accepted = Some((ballot, layout));
                        (
                            Message::ConfigAccepted { epoch, ballot },
                            Some(acc.to_record()),
                        )
                    }
                };
                if let Some(record) = record {
                    let _ = self.journal.record_acceptor(epoch, &record).await;
                }
                out.push((from, reply));
            }
            Message::ConfigCommit { epoch, layout } => {
                // Append to the durable log, then install every now-contiguous
                // epoch. If this commit arrived before an intervening epoch, ask the
                // sender (which decided it, so holds the full prefix) to fill the gap.
                let _ = self.journal.append_metadata(epoch, &layout).await;
                self.ingest_entry(epoch, layout);
                if self.has_metadata_gap() {
                    out.push((
                        from,
                        Message::MetadataFetch {
                            after_epoch: self.installed_epoch(),
                        },
                    ));
                }
            }
            Message::MetadataFetch { after_epoch } => {
                // Reply with the contiguous run of decided entries starting just
                // after `after_epoch` (never a set with our own holes, so the
                // requester can apply them in order).
                let entries = {
                    let log = self.meta_log.lock().expect("meta_log poisoned");
                    let mut entries = Vec::new();
                    let mut next = after_epoch + 1;
                    while let Some(layout) = log.entries.get(&next) {
                        entries.push((next, layout.clone()));
                        next += 1;
                    }
                    entries
                };
                if !entries.is_empty() {
                    out.push((from, Message::MetadataEntries { entries }));
                }
            }
            Message::MetadataEntries { entries } => {
                // Persist the whole run first (one group fsync for journals that
                // support it), then ingest in ascending order; `apply_log` installs
                // each now-contiguous epoch. Idempotent, so an overlapping pull is
                // harmless.
                let _ = self.journal.append_metadata_batch(&entries).await;
                for (epoch, layout) in entries {
                    self.ingest_entry(epoch, layout);
                }
            }
            // Config Paxos responses — route to the in-flight epoch change.
            response @ (Message::ConfigPromise { .. }
            | Message::ConfigAccepted { .. }
            | Message::ConfigNack { .. }) => {
                let epoch = match &response {
                    Message::ConfigPromise { epoch, .. }
                    | Message::ConfigAccepted { epoch, .. }
                    | Message::ConfigNack { epoch, .. } => *epoch,
                    _ => unreachable!(),
                };
                let tx = self
                    .config_inbox
                    .lock()
                    .expect("config poisoned")
                    .get(&epoch)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(response);
                }
            }
            // A forwarded read: serve it from the local backend and reply.
            Message::ReadForward {
                id,
                aggregators,
                routing_key,
                args,
                to_micros,
            } => {
                let result = self
                    .datastore
                    .read(aggregators.map(Arc::from), routing_key, args, to_micros)
                    .await
                    .unwrap_or_default();
                let page_info = result.page_info;
                let mut cursors = Vec::with_capacity(result.edges.len());
                let mut events = Vec::with_capacity(result.edges.len());
                for edge in result.edges {
                    cursors.push(edge.cursor);
                    events.push(edge.node);
                }
                out.push((
                    from,
                    Message::ReadReply {
                        id,
                        cursors,
                        events,
                        page_info,
                    },
                ));
            }
            // Read-index probe: a pure query of the conflict graph — report the
            // deps a read at `txn` would witness over `key`, storing nothing.
            Message::ReadProbe { txn, key } => {
                let (execute_at, deps) = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .read_probe(txn, std::slice::from_ref(&key));
                out.push((
                    from,
                    Message::ReadProbeOk {
                        txn,
                        execute_at,
                        deps,
                    },
                ));
            }
            reply @ Message::ReadReply { .. } => {
                let id = match &reply {
                    Message::ReadReply { id, .. } => *id,
                    _ => unreachable!(),
                };
                let tx = self
                    .read_pending
                    .lock()
                    .expect("reads poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(reply);
                }
            }
            // Responses: hand to the waiting coordinator, tagged with the sender.
            response => {
                if let Some(txn) = response.txn() {
                    let tx = self
                        .pending
                        .lock()
                        .expect("pending poisoned")
                        .get(&txn)
                        .cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send((from, response));
                    }
                }
            }
        }
        out
    }

    /// Drives execution in execution-timestamp order: applies any transaction
    /// whose decision is known and whose dependencies are satisfied, otherwise
    /// reads (and holds the slot for) the next ready undecided transaction. Re-run
    /// after any Commit or Apply, since either can unblock more work. Stages its
    /// journal writes and returns the (durability-gated) acks to send once the
    /// caller has flushed.
    async fn drive_staged(&self) -> Vec<(NodeId, Message)> {
        let mut out: Vec<(NodeId, Message)> = Vec::new();
        loop {
            // Prefer enacting a decided, ready transaction.
            let apply = self.replica.lock().expect("replica poisoned").next_apply();
            if let Some(apply) = apply {
                let _ = self
                    .datastore
                    .apply(apply.txn, apply.execute_at, apply.events, apply.commit)
                    .await;
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .mark_applied(apply.txn, !apply.commit);
                // Wake read barriers / read-your-writes waiters instead of
                // letting them poll the replica lock.
                self.applied_gen.send_modify(|v| *v += 1);
                self.journal_stage(apply.txn).await;
                out.push((apply.reply_to, Message::Applied { txn: apply.txn }));
                continue;
            }

            // Otherwise read the next ready, undecided transaction.
            let read = self.replica.lock().expect("replica poisoned").next_read();
            let Some(read) = read else { break };
            let ok = self.read_condition(&read.events).await;
            out.push((read.reply_to, Message::ReadOk { txn: read.txn, ok }));
        }
        out
    }

    /// Whether every owned event extends its aggregate (version strictly greater
    /// than what is stored) — this node's part of the atomic commit condition.
    async fn read_condition(&self, owned_events: &[Event]) -> bool {
        for event in owned_events {
            let current = self
                .datastore
                .version(&event.aggregate_type, &event.aggregate_id)
                .await
                .unwrap_or(0);
            if event.version <= current {
                return false;
            }
        }
        true
    }

    /// Stages the current durable state of `txn` to the journal — durable once the
    /// caller (the inbox batch loop, or the [`handle`](Self::handle) wrapper)
    /// flushes.
    async fn journal_stage(&self, txn: TxnId) {
        let snapshot = self.replica.lock().expect("replica poisoned").snapshot(txn);
        if let Some(state) = snapshot {
            let _ = self.journal.stage(&state).await;
        }
    }

    // ----- coordination ---------------------------------------------------

    /// Whether reads should be linearized through a [`read_barrier`](Self::read_barrier).
    pub fn linearizable_reads(&self) -> bool {
        self.settings.linearizable_reads
    }

    /// Coordinates appending `events` as one strictly-serializable, atomic
    /// transaction across every shard it touches.
    pub async fn write(&self, events: Vec<Event>) -> anyhow::Result<CommitOutcome> {
        // Backpressure: if the un-compacted backlog is already at the cap (e.g.
        // compaction stalled under a sustained partition), refuse new load rather
        // than grow `Replica.commands` without bound. We shed *new writes* only —
        // existing consensus state is never dropped, and peer messages are never
        // refused (gated only here, the local coordinator's entry point).
        let backlog = self.command_count();
        if backlog >= self.settings.max_commands {
            tracing::warn!(
                node = self.id.0,
                backlog,
                cap = self.settings.max_commands,
                "refusing new write: consensus backlog at cap (compaction stalled?)"
            );
            anyhow::bail!(
                "consensus backlog at cap ({backlog} >= {}); refusing new write",
                self.settings.max_commands
            );
        }

        let keys = Self::keys_of(&events);
        let (outcome, all_fast) = self.coordinate(keys, events).await?;
        self.metrics.record_outcome(outcome.conflict, all_fast);
        tracing::debug!(
            node = self.id.0,
            txn = ?outcome.txn,
            conflict = outcome.conflict,
            fast_path = all_fast,
            "write complete"
        );
        Ok(outcome)
    }

    /// A **linearizable read barrier** for one owned `key`, as a lightweight
    /// **read-index** (it stores nothing and is never journaled — a read never
    /// enters the conflict graph):
    ///
    /// 1. Probe a slow quorum (`f + 1`) of the key's replicas for the
    ///    dependencies a read at a fresh timestamp would witness ([`ReadProbe`]).
    ///    Any write that committed before this read is committed on a quorum,
    ///    which intersects the probe quorum, so that write is in some reply's
    ///    deps.
    /// 2. Wait until every witnessed dependency is **applied locally**
    ///    ([`Replica::deps_applied`]).
    ///
    /// A subsequent local read of `key` then reflects every write that committed
    /// before the barrier began — i.e. it is linearizable. The caller must own
    /// `key` (be a replica) so the local wait actually fences its reads. Errors
    /// (quorum unreachable, or a dependency that never applies in time) surface
    /// as an unavailable read rather than a stale one.
    ///
    /// [`ReadProbe`]: Message::ReadProbe
    pub async fn read_barrier(&self, key: Key) -> anyhow::Result<()> {
        let replicas = self.topology.replicas(&key);
        let slow_q = self.topology.slow_quorum(&key);
        let t0 = self.clock.now();
        let txn = TxnId(t0);
        let mut rx = self.register(txn);

        self.broadcast(
            &replicas,
            Message::ReadProbe {
                txn,
                key: key.clone(),
            },
        )
        .await;
        let probes = Self::collect_tagged(
            &mut rx,
            slow_q,
            Self::after(self.settings.collect_timeout),
            |m| match m {
                Message::ReadProbeOk {
                    execute_at, deps, ..
                } => Some((execute_at, deps)),
                _ => None,
            },
        )
        .await;
        self.deregister(txn);
        if probes.len() < slow_q {
            anyhow::bail!("read barrier quorum not reached for {key:?}");
        }

        // The union of every dependency the quorum witnessed: every write
        // conflicting on `key` that orders before this read.
        let deps: Vec<TxnId> = probes
            .into_iter()
            .flat_map(|(_, (_, deps))| deps)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        // Wait until they are all applied locally, then the caller reads.
        //
        // Deliberately conservative: every witnessed dependency is awaited,
        // even one whose *currently known* position orders after the probe's.
        // Probe replicas may hold a stale (pre-slow-path) view of a
        // dependency's timestamp, so a "orders after the read" escape computed
        // from probe data can skip a write that committed before the read
        // began — a real stale read observed under contention + churn.
        let deadline = Self::after(self.settings.collect_timeout);
        let mut applied_rx = self.applied_gen.subscribe();
        loop {
            if self
                .replica
                .lock()
                .expect("replica poisoned")
                .deps_applied(&deps)
            {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                anyhow::bail!("read barrier timed out: {} deps unapplied", deps.len());
            }
            // Woken by the next apply (local or imported); the timeout arm only
            // bounds a genuinely stuck dependency.
            if let Ok(Err(_)) = tokio::time::timeout(deadline - now, applied_rx.changed()).await {
                // Sender dropped (cannot happen while `self` lives) — degrade
                // to a bounded poll rather than busy-looping.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    /// The shared PreAccept → (Accept) → Commit → Read → Apply coordination for a
    /// transaction over `keys` appending `events` (empty for a read barrier).
    /// Returns the outcome and whether it took the one-round fast path.
    async fn coordinate(
        &self,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> anyhow::Result<(CommitOutcome, bool)> {
        let (plans, union) = self.plan(&keys);

        let t0 = self.clock.now();
        let txn = TxnId(t0);
        let mut rx = self.register(txn);

        // PreAccept: a fast quorum per shard, or whatever arrives before the
        // fast-path timeout.
        self.broadcast(
            &union,
            Message::PreAccept {
                txn,
                keys: keys.clone(),
                events: events.clone(),
            },
        )
        .await;
        // Stop as soon as each shard's **electorate** has returned a fast quorum
        // (the latency win: a coordinator co-located with the electorate need not
        // wait for remote, non-electorate replicas), or at the fast-path timeout.
        let electorate_responders = |p: &ShardPlan, got: &[(NodeId, (Timestamp, Vec<TxnId>))]| {
            got.iter()
                .filter(|(from, _)| p.electorate.contains(from))
                .count()
        };
        let mut pre = self
            .collect_by_shard_tagged(
                &mut rx,
                &plans,
                |p, got| electorate_responders(p, got) >= p.fast_q,
                Self::after(self.settings.fast_timeout),
                |m| match m {
                    Message::PreAcceptOk {
                        execute_at, deps, ..
                    } => Some((execute_at, deps)),
                    _ => None,
                },
            )
            .await;
        // The fast-path window may close before a slow quorum has witnessed the
        // txn (e.g. cold connections at startup, where the first handshake
        // outlasts `fast_timeout`). Keep collecting up to the slow-path budget
        // so a simple majority can still carry the write down the slow path.
        if plans.iter().any(|p| pre[&p.shard].len() < p.slow_q) {
            let remaining: HashMap<ShardId, usize> = plans
                .iter()
                .map(|p| (p.shard, p.slow_q.saturating_sub(pre[&p.shard].len())))
                .collect();
            let more = self
                .collect_by_shard_tagged(
                    &mut rx,
                    &plans,
                    |p, got| got.len() >= remaining[&p.shard],
                    Self::after(self.settings.collect_timeout),
                    |m| match m {
                        Message::PreAcceptOk {
                            execute_at, deps, ..
                        } => Some((execute_at, deps)),
                        _ => None,
                    },
                )
                .await;
            for (shard, mut items) in more {
                if let Some(bucket) = pre.get_mut(&shard) {
                    bucket.append(&mut items);
                }
            }
        }
        for plan in &plans {
            if pre[&plan.shard].len() < plan.slow_q {
                self.deregister(txn);
                anyhow::bail!("preaccept quorum not reached for shard {}", plan.shard);
            }
        }

        // Fast path iff every shard's electorate returned a fast quorum that all
        // agree `t0` may stand. Only electorate votes count toward the fast path;
        // non-electorate replicas still witnessed (for deps & recovery) but do not
        // vote, so their disagreement never blocks a fast commit.
        let all_fast = plans.iter().all(|p| {
            let agree = pre[&p.shard]
                .iter()
                .filter(|(from, _)| p.electorate.contains(from))
                .collect::<Vec<_>>();
            agree.len() >= p.fast_q && agree.iter().all(|(_, (e, _))| *e == t0)
        });
        // On the fast path the committed timestamp MUST be `t0`: `all_fast` is
        // judged on electorate votes only, and recovery reconstructs a
        // fast-path decision as `t0`. Taking the max over ALL responders here
        // would let a non-electorate replica raise the committed timestamp
        // above what recovery would later decide — two different positions in
        // the global order for one transaction.
        let execute_at = if all_fast {
            t0
        } else {
            pre.values()
                .flatten()
                .map(|(_, (e, _))| *e)
                .max()
                .unwrap_or(t0)
        };
        // Dependencies are kept **per shard** — each shard's deps come only from
        // its own replicas, so a replica never receives (and stalls on) a
        // dependency in a shard it cannot witness.
        let mut shard_deps: HashMap<ShardId, BTreeSet<TxnId>> = plans
            .iter()
            .map(|p| {
                let deps = pre[&p.shard]
                    .iter()
                    .flat_map(|(_, (_, d))| d.iter().copied())
                    .collect();
                (p.shard, deps)
            })
            .collect();

        // Slow path: Accept the raised timestamp and each shard's dependency set.
        if !all_fast {
            for plan in &plans {
                let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                for &node in &plan.replicas {
                    self.send(
                        node,
                        Message::Accept {
                            txn,
                            ballot: Ballot(t0),
                            execute_at,
                            deps: deps.clone(),
                            keys: keys.clone(),
                        },
                    )
                    .await;
                }
            }
            let acks = self
                .collect_by_shard(
                    &mut rx,
                    &plans,
                    |p| p.slow_q,
                    Self::after(self.settings.collect_timeout),
                    |m| match m {
                        Message::AcceptOk { ballot, deps, .. } if ballot == Ballot(t0) => {
                            Some(deps)
                        }
                        _ => None,
                    },
                )
                .await;
            for plan in &plans {
                if acks[&plan.shard].len() < plan.slow_q {
                    self.deregister(txn);
                    anyhow::bail!("accept quorum not reached for shard {}", plan.shard);
                }
            }
            for (shard, deps_lists) in acks {
                if let Some(set) = shard_deps.get_mut(&shard) {
                    for deps in deps_lists {
                        set.extend(deps);
                    }
                }
            }
        }

        let conflict = self
            .execute(&mut rx, &plans, txn, execute_at, shard_deps, events)
            .await?;
        self.deregister(txn);
        Ok((CommitOutcome { txn, conflict }, all_fast))
    }

    /// Commit → Read → Apply: drive the atomic decision across all touched
    /// shards and return whether the write was aborted.
    async fn execute(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        txn: TxnId,
        execute_at: Timestamp,
        shard_deps: HashMap<ShardId, BTreeSet<TxnId>>,
        events: Vec<Event>,
    ) -> anyhow::Result<bool> {
        let union = Self::union_of(plans);

        // Commit each shard with its own dependency set.
        for plan in plans {
            let deps: Vec<TxnId> = shard_deps
                .get(&plan.shard)
                .into_iter()
                .flatten()
                .copied()
                .filter(|d| *d != txn)
                .collect();
            for &node in &plan.replicas {
                self.send(
                    node,
                    Message::Commit {
                        txn,
                        execute_at,
                        deps: deps.clone(),
                        events: events.clone(),
                        reply_to: self.id,
                    },
                )
                .await;
            }
        }

        // Read: a slow quorum per shard reports its version condition.
        let reads = self
            .collect_by_shard(
                rx,
                plans,
                |p| p.slow_q,
                Self::after(self.settings.collect_timeout),
                |m| match m {
                    Message::ReadOk { ok, .. } => Some(ok),
                    _ => None,
                },
            )
            .await;
        for plan in plans {
            if reads[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("read quorum not reached for shard {}", plan.shard);
            }
        }
        // Atomic commit only if every shard's condition holds.
        let commit = reads.values().flatten().all(|ok| *ok);

        // Apply: enact the decision and wait for a slow quorum per shard.
        self.broadcast(&union, Message::Apply { txn, commit }).await;
        let acks = self
            .collect_by_shard(
                rx,
                plans,
                |p| p.slow_q,
                Self::after(self.settings.collect_timeout),
                |m| match m {
                    Message::Applied { .. } => Some(()),
                    _ => None,
                },
            )
            .await;
        for plan in plans {
            if acks[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("apply quorum not reached for shard {}", plan.shard);
            }
        }

        // Read-your-writes: if this coordinator is itself a replica, wait for its
        // own local apply so a subsequent read on this node sees the write.
        if union.contains(&self.id) {
            let deadline = Self::after(self.settings.collect_timeout);
            let mut applied_rx = self.applied_gen.subscribe();
            while self
                .replica
                .lock()
                .expect("replica poisoned")
                .applied_result(txn)
                .is_none()
            {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                // Woken by the next apply instead of polling the replica lock
                // every millisecond.
                if let Ok(Err(_)) = tokio::time::timeout(deadline - now, applied_rx.changed()).await
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        Ok(!commit)
    }

    /// Recovers `txn`: takes it over under a fresh ballot and drives it to an
    /// applied outcome consistent with whatever its failed coordinator decided.
    pub async fn recover(&self, txn: TxnId) -> anyhow::Result<CommitOutcome> {
        // A transaction this node is actively coordinating must not be
        // recovered by the same node: `register` would replace the live
        // coordination's response channel and starve it.
        if txn.0.node == self.id
            && self
                .pending
                .lock()
                .expect("pending poisoned")
                .contains_key(&txn)
        {
            anyhow::bail!("transaction {txn:?} is locally in flight; not recovering it");
        }

        let all_nodes = self.topology.nodes();
        let mut ballot = Ballot(self.clock.now());

        for _attempt in 0..3 {
            let mut rx = self.register(txn);
            self.broadcast(&all_nodes, Message::Recover { txn, ballot })
                .await;

            // Gather what every reachable replica knows within a short window,
            // tagged with the responder so deps can be grouped by shard.
            let resp = Self::collect_tagged(
                &mut rx,
                all_nodes.len(),
                Self::after(self.settings.fast_timeout),
                |m| match m {
                    // Only reports answering THIS attempt's ballot count — a
                    // stale reply from an earlier ballot must not fill the
                    // quorum (that could rest the decision on a sub-quorum
                    // sample and miss an existing Commit/Accept).
                    Message::RecoverOk {
                        ballot: b,
                        known,
                        status,
                        accepted,
                        execute_at,
                        deps,
                        superseding_rejects,
                        keys,
                        events,
                        ..
                    } if b == ballot => Some(RecoverResp::Ok(RecoverFields {
                        known,
                        status,
                        accepted,
                        execute_at,
                        deps,
                        superseding_rejects,
                        keys,
                        events,
                    })),
                    Message::Nack { promised, .. } => Some(RecoverResp::Nack(promised)),
                    _ => None,
                },
            )
            .await;

            if let Some(promised) = resp.iter().find_map(|(_, r)| match r {
                RecoverResp::Nack(p) => Some(*p),
                _ => None,
            }) {
                self.deregister(txn);
                ballot = self.higher_ballot(promised);
                continue;
            }

            // Every node that answered (whether or not it witnessed the txn). The
            // recovery decision must rest on a slow quorum (`f + 1`) per shard, so
            // it intersects any quorum that could already have decided — without
            // that gate a sub-quorum sample could miss a Commit/Accept (or, under a
            // shrunk electorate, raise `t0` while the txn already fast-committed at
            // `t0` on the electorate). A non-witness still counts: it attests the
            // txn was absent there.
            let responders: Vec<NodeId> = resp
                .iter()
                .filter_map(|(from, r)| matches!(r, RecoverResp::Ok(_)).then_some(*from))
                .collect();

            let known: Vec<(NodeId, RecoverFields)> = resp
                .into_iter()
                .filter_map(|(from, r)| match r {
                    RecoverResp::Ok(f) if f.known => Some((from, f)),
                    _ => None,
                })
                .collect();
            if known.is_empty() {
                self.deregister(txn);
                anyhow::bail!("transaction unknown to recovery quorum");
            }

            // Reconstruct the full key/event set and plan the touched shards.
            let mut keys: Vec<Key> = Vec::new();
            for k in known.iter().flat_map(|(_, f)| f.keys.iter()) {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
            let (plans, _union) = self.plan(&keys);

            // Recovery-quorum gate: bail (retry on the next sweep) unless a slow
            // quorum of each touched shard reported.
            for plan in &plans {
                let heard = responders
                    .iter()
                    .filter(|from| self.topology.node_shard(**from) == Some(plan.shard))
                    .count();
                if heard < plan.slow_q {
                    self.deregister(txn);
                    anyhow::bail!("recovery quorum not reached for shard {}", plan.shard);
                }
            }
            // Each shard reported only its owned events; reassemble the whole set
            // so every replica can extract its part from the Commit.
            let events_full = self.reassemble_events(&known);

            let max_status = known
                .iter()
                .map(|(_, f)| f.status)
                .max()
                .expect("non-empty");
            // The recovered execution timestamp (global), by max status.
            let execute_at = match max_status {
                Status::Committed | Status::Reading | Status::Applied => {
                    known
                        .iter()
                        .find(|(_, f)| f.status >= Status::Committed)
                        .expect("committed present")
                        .1
                        .execute_at
                }
                Status::Accepted => {
                    known
                        .iter()
                        .max_by_key(|(_, f)| f.accepted)
                        .expect("non-empty")
                        .1
                        .execute_at
                }
                Status::PreAccepted => {
                    if known.iter().any(|(_, f)| f.superseding_rejects) {
                        known
                            .iter()
                            .map(|(_, f)| f.execute_at)
                            .max()
                            .expect("non-empty")
                    } else {
                        txn.0
                    }
                }
            };

            // Per-shard deps: union of the deps reported by each shard's own
            // replicas, so no shard's barrier names a transaction from another.
            let mut shard_deps: HashMap<ShardId, BTreeSet<TxnId>> =
                plans.iter().map(|p| (p.shard, BTreeSet::new())).collect();
            for (from, f) in &known {
                if let Some(shard) = self.topology.node_shard(*from) {
                    if let Some(set) = shard_deps.get_mut(&shard) {
                        set.extend(f.deps.iter().copied());
                    }
                }
            }

            // Re-propose under our ballot (unless already committed), then drive
            // the atomic Commit → Read → Apply.
            if max_status < Status::Committed {
                for plan in &plans {
                    let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                    for &node in &plan.replicas {
                        self.send(
                            node,
                            Message::Accept {
                                txn,
                                ballot,
                                execute_at,
                                deps: deps.clone(),
                                keys: keys.clone(),
                            },
                        )
                        .await;
                    }
                }
                let acks = self
                    .collect_by_shard(
                        &mut rx,
                        &plans,
                        |p| p.slow_q,
                        Self::after(self.settings.collect_timeout),
                        |m| match m {
                            Message::AcceptOk {
                                ballot: b, deps, ..
                            } if b == ballot => Some(Ok(deps)),
                            Message::Nack { promised, .. } => Some(Err(promised)),
                            _ => None,
                        },
                    )
                    .await;
                if let Some(promised) = acks
                    .values()
                    .flatten()
                    .find_map(|a| a.as_ref().err().copied())
                {
                    self.deregister(txn);
                    ballot = self.higher_ballot(promised);
                    continue;
                }
                for (shard, lists) in acks {
                    if let Some(set) = shard_deps.get_mut(&shard) {
                        for a in lists.into_iter().flatten() {
                            set.extend(a);
                        }
                    }
                }
            }

            let conflict = self
                .execute(&mut rx, &plans, txn, execute_at, shard_deps, events_full)
                .await?;
            self.deregister(txn);
            return Ok(CommitOutcome { txn, conflict });
        }

        anyhow::bail!("recovery exhausted its ballot retries")
    }

    /// Runs only PreAccept and returns the transaction id, modelling a
    /// coordinator that crashed before committing. For tests of [`recover`].
    pub async fn coordinate_preaccept(&self, events: Vec<Event>) -> anyhow::Result<TxnId> {
        let keys = Self::keys_of(&events);
        let (plans, union) = self.plan(&keys);
        let t0 = self.clock.now();
        let txn = TxnId(t0);

        let mut rx = self.register(txn);
        self.broadcast(&union, Message::PreAccept { txn, keys, events })
            .await;
        let pre = self
            .collect_by_shard(
                &mut rx,
                &plans,
                |p| p.slow_q,
                Self::after(self.settings.collect_timeout),
                |m| match m {
                    Message::PreAcceptOk { .. } => Some(()),
                    _ => None,
                },
            )
            .await;
        self.deregister(txn);

        for plan in &plans {
            if pre[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("preaccept quorum not reached for shard {}", plan.shard);
            }
        }
        Ok(txn)
    }

    /// Marks this node as joining: until [`join`](Node::join) completes,
    /// consensus messages are buffered rather than processed. Call this before
    /// the epoch that adds this node is installed, so nothing is lost in the gap.
    pub fn begin_join(&self) {
        self.bootstrapped.store(false, Ordering::Release);
    }

    /// Bootstraps this (joining) node from `contact`: fetches its committed state,
    /// imports every command for a key this node now owns (recording it in the
    /// conflict graph and applying its events), then replays any consensus
    /// messages buffered since [`begin_join`](Node::begin_join) — so a transaction
    /// committed during the join still lands. Returns how many were imported.
    ///
    /// Coordinating the epoch change across the cluster (rather than each node
    /// installing an agreed layout) remains future work.
    pub async fn join(&self, contact: NodeId) -> anyhow::Result<usize> {
        let imported = self
            .import_from(contact, self.settings.collect_timeout, true, None)
            .await?;

        // Resume normal processing and replay anything buffered during bootstrap.
        // Flag flip and drain happen under the buffer lock (see `handle_staged`)
        // so no envelope can land in the buffer after the drain.
        let buffered = {
            let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
            self.bootstrapped.store(true, Ordering::Release);
            std::mem::take(&mut *buffer)
        };
        for env in buffered {
            Box::pin(self.handle(env)).await;
        }

        Ok(imported)
    }

    /// Fetches `contact`'s committed state and imports every command for a key
    /// this node owns that it does not already have. Used by both bootstrap
    /// ([`join`](Node::join), `want_snapshot = true`) and the anti-entropy repair
    /// sweep (`false`).
    ///
    /// With `want_snapshot`, the contact also ships its materialised data-store
    /// snapshot and redundancy watermark: a bootstrapping node may be below the
    /// cluster's truncation point, where command replay alone no longer
    /// reconstructs the state. It installs the snapshot, adopts the watermark
    /// (so a dependency on a compacted-away transaction counts as satisfied), and
    /// imports the recent commands into its conflict graph only — their events are
    /// already in the snapshot. Anti-entropy needs no snapshot (the watermark is
    /// the cluster-wide min, so an existing replica has already applied everything
    /// below it), so it applies each imported command's events as before.
    async fn import_from(
        &self,
        contact: NodeId,
        timeout: Duration,
        want_snapshot: bool,
        known: Option<SyncKnown>,
    ) -> anyhow::Result<usize> {
        let id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.sync_inbox
            .lock()
            .expect("sync poisoned")
            .insert(id, tx);
        self.send(
            contact,
            Message::SyncRequest {
                id,
                snapshot: want_snapshot,
                known,
            },
        )
        .await;

        let received = tokio::time::timeout(timeout, rx.recv()).await;
        self.sync_inbox.lock().expect("sync poisoned").remove(&id);
        let (watermark, snapshot, commands) = received
            .map_err(|_| anyhow::anyhow!("sync timed out"))?
            .ok_or_else(|| anyhow::anyhow!("sync channel closed"))?;

        // Install the materialised snapshot (the truncated prefix and beyond) for
        // owned keys under a synthetic, sub-watermark id — redundant for ordering
        // but it sets this node's versions and committed map.
        let owned_snapshot: Vec<Event> = snapshot
            .into_iter()
            .filter(|event| self.topology.owns(self.id, &Key::of(event)))
            .collect();
        if !owned_snapshot.is_empty() {
            let _ = self
                .datastore
                .apply(TxnId(watermark), watermark, owned_snapshot, true)
                .await;
        }

        let mut imported = 0;
        for cmd in commands {
            if !cmd.keys.iter().any(|k| self.topology.owns(self.id, k)) {
                continue;
            }
            let (txn, execute_at, events) = (cmd.txn, cmd.execute_at, cmd.events.clone());
            let commit = cmd.applied_conflict == Some(false);
            let inserted = self
                .replica
                .lock()
                .expect("replica poisoned")
                .import_applied(cmd);
            if inserted {
                // On bootstrap the snapshot already materialised these events.
                if !want_snapshot {
                    let _ = self.datastore.apply(txn, execute_at, events, commit).await;
                }
                imported += 1;
            }
        }

        // A bootstrapping node adopts the contact's (cluster-agreed) redundancy
        // floor. Anti-entropy must not: its peer's watermark is not the cluster
        // min, so advancing here could be premature — that is the sweep's job.
        if want_snapshot {
            self.replica
                .lock()
                .expect("replica poisoned")
                .compact(watermark);
        }

        if imported > 0 {
            // Imported applies advance local applied state too.
            self.applied_gen.send_modify(|v| *v += 1);
        }

        Ok(imported)
    }

    /// Coordinates installing `shards` as the new `epoch`'s layout, fault
    /// tolerantly: runs single-decree Paxos over the **current members** so the
    /// decision survives this coordinator failing, then commits it cluster-wide.
    /// Returns the decided layout (which differs from `shards` only if a prior,
    /// interrupted change for this epoch had already been accepted).
    ///
    /// The acceptor set is the members of the current epoch (a node proposing the
    /// next epoch is still on this one), so it tracks membership: changes keep
    /// working after the original founders have left.
    pub async fn change_topology(
        &self,
        epoch: u64,
        shards: Vec<Vec<NodeId>>,
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        let decided = self.run_config_paxos(epoch, shards, &acceptors).await?;
        self.commit_topology(epoch, &decided, &acceptors).await;
        Ok(decided)
    }

    /// Drives Paxos phases 1–2 for `epoch` (no commit), returning the decided
    /// layout. Exposed so tests can simulate a coordinator that fails after the
    /// decision is durable but before committing.
    pub async fn propose_topology(
        &self,
        epoch: u64,
        proposed: Vec<Vec<NodeId>>,
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        self.run_config_paxos(epoch, proposed, &acceptors).await
    }

    /// The committed layout for `epoch`, if it is already in the metadata log.
    fn committed_layout(&self, epoch: u64) -> Option<Vec<Vec<NodeId>>> {
        self.meta_log
            .lock()
            .expect("meta_log poisoned")
            .entries
            .get(&epoch)
            .cloned()
    }

    /// This node's **rank** for proposing `epoch` among `acceptors`: 0 for the
    /// *distinguished proposer* `members[epoch % n]` (members sorted by `NodeId` and
    /// deduplicated — `topology.nodes()` is layout order, which is not stable), then
    /// 1, 2, … going round the ring. A node absent from the set ranks last. Drives
    /// the anti-dueling deference: the rank-0 node proposes first, others wait
    /// `rank * config_defer_step` and skip if it already committed.
    fn config_rank(&self, epoch: u64, acceptors: &[NodeId]) -> usize {
        let mut members: Vec<NodeId> = acceptors.to_vec();
        members.sort_unstable();
        members.dedup();
        let n = members.len();
        if n == 0 {
            return 0;
        }
        let preferred = epoch as usize % n;
        match members.iter().position(|&m| m == self.id) {
            Some(mine) => (mine + n - preferred) % n,
            None => n, // not a member — defer behind everyone
        }
    }

    /// Deterministic, rank/attempt-asymmetric ballot-duel backoff: grows with the
    /// attempt and is offset by this node's rank, so two racing proposers wait
    /// *different* amounts and one pulls ahead instead of re-colliding every round.
    fn config_backoff(&self, attempt: usize, rank: usize) -> Duration {
        self.settings.config_defer_step * (attempt + rank + 1) as u32
    }

    /// The config-Paxos coordinator over an explicit `acceptors` set.
    async fn run_config_paxos(
        &self,
        epoch: u64,
        proposed: Vec<Vec<NodeId>>,
        acceptors: &[NodeId],
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let need = acceptors.len() / 2 + 1;
        let rank = self.config_rank(epoch, acceptors);
        let mut ballot = Ballot(self.clock.now());

        for attempt in 0..6 {
            // Yield: if the epoch committed (this round or while we were backing off
            // from a duel), adopt that decided layout — never our own `proposed`,
            // which may differ from the chosen value.
            if let Some(layout) = self.committed_layout(epoch) {
                self.config_inbox
                    .lock()
                    .expect("config poisoned")
                    .remove(&epoch);
                return Ok(layout);
            }
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.config_inbox
                .lock()
                .expect("config poisoned")
                .insert(epoch, tx);

            // Phase 1: Prepare → adopt the highest already-accepted value, if any.
            for &node in acceptors {
                self.send(node, Message::ConfigPrepare { epoch, ballot })
                    .await;
            }
            let mut promises = 0;
            let mut nack: Option<Ballot> = None;
            let mut adopted: Option<(Ballot, Vec<Vec<NodeId>>)> = None;
            let deadline = Self::after(self.settings.collect_timeout);
            while promises < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    // Only promises answering THIS ballot count; a stale reply
                    // from an earlier attempt (or another proposer's round)
                    // must not fill the quorum.
                    Ok(Some(Message::ConfigPromise {
                        ballot: b,
                        accepted_ballot,
                        accepted_layout,
                        ..
                    })) if b == ballot => {
                        promises += 1;
                        if let Some(layout) = accepted_layout {
                            if adopted.as_ref().is_none_or(|(b, _)| accepted_ballot > *b) {
                                adopted = Some((accepted_ballot, layout));
                            }
                        }
                    }
                    Ok(Some(Message::ConfigNack { promised, .. })) => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            if let Some(promised) = nack {
                self.config_inbox
                    .lock()
                    .expect("config poisoned")
                    .remove(&epoch);
                // Back off before re-preparing so duelling proposers desync (the
                // delay is rank/attempt-asymmetric, so two racers never re-collide in
                // lockstep) — then the top-of-loop committed-check lets the loser
                // adopt the winner's decision instead of escalating forever.
                tokio::time::sleep(self.config_backoff(attempt, rank)).await;
                ballot = self.higher_ballot(promised);
                continue;
            }
            if promises < need {
                self.config_inbox
                    .lock()
                    .expect("config poisoned")
                    .remove(&epoch);
                anyhow::bail!("config prepare quorum not reached for epoch {epoch}");
            }

            let layout = adopted.map(|(_, l)| l).unwrap_or(proposed.clone());

            // Phase 2: Accept the chosen layout.
            for &node in acceptors {
                self.send(
                    node,
                    Message::ConfigAccept {
                        epoch,
                        ballot,
                        layout: layout.clone(),
                    },
                )
                .await;
            }
            let mut accepts = 0;
            let mut nack: Option<Ballot> = None;
            let deadline = Self::after(self.settings.collect_timeout);
            while accepts < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    // Only accepts answering THIS ballot count — a stale accept
                    // from a lower ballot proves nothing about this round.
                    Ok(Some(Message::ConfigAccepted { ballot: b, .. })) if b == ballot => {
                        accepts += 1
                    }
                    Ok(Some(Message::ConfigNack { promised, .. })) => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            self.config_inbox
                .lock()
                .expect("config poisoned")
                .remove(&epoch);

            if let Some(promised) = nack {
                tokio::time::sleep(self.config_backoff(attempt, rank)).await;
                ballot = self.higher_ballot(promised);
                continue;
            }
            if accepts < need {
                anyhow::bail!("config accept quorum not reached for epoch {epoch}");
            }
            return Ok(layout);
        }
        // A winner may have committed during our final backoff — adopt it.
        if let Some(layout) = self.committed_layout(epoch) {
            return Ok(layout);
        }
        anyhow::bail!("config change for epoch {epoch} exhausted its ballots")
    }

    /// Recovers a possibly-interrupted change for `epoch`: completes whatever
    /// layout was already accepted (or, if none was, keeps the current layout),
    /// then commits it. Any node can call this after the original coordinator
    /// fails.
    pub async fn recover_topology(&self, epoch: u64) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        // Anti-dueling deference (this is the automatic path, run by the sweep on
        // every node): the distinguished proposer for `epoch` (rank 0) drives first;
        // others wait `rank * config_defer_step` and skip if it already committed, so
        // N concurrent sweeps don't duel. A dead preferred node just means the next
        // rank takes over after its delay (and the periodic sweep retries).
        let rank = self.config_rank(epoch, &acceptors);
        if rank > 0 {
            tokio::time::sleep(self.settings.config_defer_step * rank as u32).await;
            if let Some(layout) = self.committed_layout(epoch) {
                return Ok(layout);
            }
        }
        let fallback = self.current_layout();
        let decided = self.run_config_paxos(epoch, fallback, &acceptors).await?;
        self.commit_topology(epoch, &decided, &acceptors).await;
        Ok(decided)
    }

    /// Records a decided metadata-log entry and installs every now-contiguous epoch.
    ///
    /// The entry is held in [`MetadataLog::entries`] but only installed into the
    /// [`Topology`] once **every** intervening epoch is present — so a node that
    /// receives epoch N+2 before N+1 holds it until N+1 arrives, never skipping an
    /// epoch. Idempotent: re-ingesting a known entry is a no-op.
    fn ingest_entry(&self, epoch: u64, layout: Vec<Vec<NodeId>>) {
        {
            let mut log = self.meta_log.lock().expect("meta_log poisoned");
            log.entries.entry(epoch).or_insert(layout);
        }
        self.apply_log();
    }

    /// Installs each contiguous metadata-log entry above the installed epoch, in
    /// order. Only ever calls `install(installed_epoch + 1, …)`, so no epoch is
    /// skipped even if a far-future entry is already in the log.
    fn apply_log(&self) {
        loop {
            let next = {
                let log = self.meta_log.lock().expect("meta_log poisoned");
                let next = log.installed_epoch + 1;
                log.entries.get(&next).map(|layout| (next, layout.clone()))
            };
            let Some((next, layout)) = next else { break };
            self.topology.install(next, layout);
            self.meta_log
                .lock()
                .expect("meta_log poisoned")
                .installed_epoch = next;
        }
    }

    /// Whether the log holds a decided entry the topology cannot install yet because
    /// an intervening epoch is missing — the catch-up trigger.
    fn has_metadata_gap(&self) -> bool {
        let log = self.meta_log.lock().expect("meta_log poisoned");
        log.entries
            .keys()
            .next_back()
            .is_some_and(|&max| max > log.installed_epoch + 1)
    }

    /// The highest epoch this node has installed into its topology.
    fn installed_epoch(&self) -> u64 {
        self.meta_log
            .lock()
            .expect("meta_log poisoned")
            .installed_epoch
    }

    /// Installs the decided layout locally and broadcasts it to the acceptors and
    /// the new members so they install it too.
    async fn commit_topology(&self, epoch: u64, layout: &[Vec<NodeId>], acceptors: &[NodeId]) {
        let _ = self.journal.append_metadata(epoch, layout).await;
        self.ingest_entry(epoch, layout.to_vec());
        let mut recipients = acceptors.to_vec();
        for node in layout.iter().flatten() {
            if !recipients.contains(node) {
                recipients.push(*node);
            }
        }
        for &node in &recipients {
            self.send(
                node,
                Message::ConfigCommit {
                    epoch,
                    layout: layout.to_vec(),
                },
            )
            .await;
        }
    }

    /// This node's current shard layout, recovered shard-by-shard from the
    /// topology (used as the no-op fallback when recovering a config change).
    fn current_layout(&self) -> Vec<Vec<NodeId>> {
        // Rebuild the REAL per-shard layout: proposing a single flattened shard
        // here would remap every key's owner if it won (the exact opposite of a
        // no-op fallback).
        let mut shards: BTreeMap<ShardId, Vec<NodeId>> = BTreeMap::new();
        let mut unsharded: Vec<NodeId> = Vec::new();
        for node in self.topology.nodes() {
            match self.topology.node_shard(node) {
                Some(shard) => shards.entry(shard).or_default().push(node),
                None => unsharded.push(node),
            }
        }
        let mut layout: Vec<Vec<NodeId>> = shards.into_values().collect();
        if layout.is_empty() {
            layout.push(unsharded);
        }
        layout
    }

    // ----- read routing ---------------------------------------------------

    /// Whether this node replicates `key` (and so can serve reads for it locally).
    pub fn owns_key(&self, key: &Key) -> bool {
        self.topology.owns(self.id, key)
    }

    /// A replica that owns `key`, to forward a read to. Prefers another node but
    /// falls back to self.
    pub fn an_owner_of(&self, key: &Key) -> Option<NodeId> {
        let replicas = self.topology.replicas(key);
        let my_region = self.topology.region(self.id);
        // Prefer an owner in this node's region (lower-latency reads when regions
        // are configured), then any other owner, then self as a last resort.
        replicas
            .iter()
            .find(|&&n| n != self.id && my_region.is_some() && self.topology.region(n) == my_region)
            .copied()
            .or_else(|| replicas.iter().find(|&&n| n != self.id).copied())
            .or_else(|| replicas.first().copied())
    }

    /// Forwards a read to `to` (an owner of the queried range) and returns its
    /// result.
    pub async fn forward_read(
        &self,
        to: NodeId,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        let id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.read_pending
            .lock()
            .expect("reads poisoned")
            .insert(id, tx);
        self.send(
            to,
            Message::ReadForward {
                id,
                aggregators: aggregators.map(|a| a.to_vec()),
                routing_key,
                args,
                to_micros,
            },
        )
        .await;

        let reply = tokio::time::timeout(self.settings.collect_timeout, rx.recv()).await;
        self.read_pending
            .lock()
            .expect("reads poisoned")
            .remove(&id);
        match reply {
            Ok(Some(Message::ReadReply {
                cursors,
                events,
                page_info,
                ..
            })) => {
                let edges = events
                    .into_iter()
                    .zip(cursors)
                    .map(|(node, cursor)| Edge { cursor, node })
                    .collect();
                Ok(ReadResult { edges, page_info })
            }
            _ => anyhow::bail!("forwarded read to {to:?} failed"),
        }
    }

    // ----- helpers --------------------------------------------------------

    /// Re-assembles the full event set from the owned subsets each shard's
    /// replicas reported during recovery.
    fn reassemble_events(&self, known: &[(NodeId, RecoverFields)]) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::new();
        for event in known.iter().flat_map(|(_, f)| f.events.iter()) {
            // Dedupe by the event's ULID — `(aggregate_id, version)` alone
            // would collapse two aggregates of different types sharing an id.
            if !events.iter().any(|e| e.id == event.id) {
                events.push(event.clone());
            }
        }
        events
    }

    /// The shards a key set touches, with their replica sets and quorum sizes,
    /// plus the deduplicated union of all their replicas.
    fn plan(&self, keys: &[Key]) -> (Vec<ShardPlan>, Vec<NodeId>) {
        let mut shards: BTreeMap<ShardId, ShardPlan> = BTreeMap::new();
        for key in keys {
            let shard = self.topology.shard_of(key);
            shards.entry(shard).or_insert_with(|| ShardPlan {
                shard,
                replicas: self.topology.replicas(key),
                electorate: self.topology.fast_electorate(key),
                fast_q: self.topology.fast_quorum(key),
                slow_q: self.topology.slow_quorum(key),
            });
        }
        let plans: Vec<ShardPlan> = shards.into_values().collect();
        let union = Self::union_of(&plans);
        (plans, union)
    }

    /// The deduplicated union of every shard's replica set.
    fn union_of(plans: &[ShardPlan]) -> Vec<NodeId> {
        let mut union: Vec<NodeId> = Vec::new();
        for plan in plans {
            for &node in &plan.replicas {
                if !union.contains(&node) {
                    union.push(node);
                }
            }
        }
        union
    }

    /// The events this node owns (keys in this node's shard).
    fn owned_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .filter(|e| self.topology.owns(self.id, &Key::of(e)))
            .collect()
    }

    /// The keys this node owns.
    fn owned_keys(&self, keys: Vec<Key>) -> Vec<Key> {
        keys.into_iter()
            .filter(|k| self.topology.owns(self.id, k))
            .collect()
    }

    /// The distinct keys a set of events touches.
    fn keys_of(events: &[Event]) -> Vec<Key> {
        let mut keys: Vec<Key> = Vec::new();
        for event in events {
            let key = Key::of(event);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        keys
    }

    /// A ballot strictly greater than `at_least`, owned by this node.
    fn higher_ballot(&self, at_least: Ballot) -> Ballot {
        let now = self.clock.now();
        if now > at_least.0 {
            Ballot(now)
        } else {
            Ballot(Timestamp {
                micros: at_least.0.micros,
                logical: at_least.0.logical + 1,
                node: self.id,
            })
        }
    }

    /// A deadline `d` from now.
    fn after(d: Duration) -> Instant {
        Instant::now() + d
    }

    /// Registers a coordination for `txn` and returns its response channel.
    fn register(&self, txn: TxnId) -> mpsc::UnboundedReceiver<(NodeId, Message)> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(txn, tx);
        rx
    }

    /// Removes a finished coordination.
    fn deregister(&self, txn: TxnId) {
        self.pending.lock().expect("pending poisoned").remove(&txn);
    }

    /// Collects up to `want` items matching `extract`, each tagged with the
    /// responder's id (so the caller can group by shard).
    async fn collect_tagged<T>(
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        want: usize,
        deadline: Instant,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> Vec<(NodeId, T)> {
        let mut out = Vec::new();
        // One vote per responder: a duplicated response (retransmission, or a
        // stale reply routed to a fresh attempt's channel) must not fill a
        // quorum with fewer distinct nodes than it claims.
        let mut seen: HashSet<NodeId> = HashSet::new();
        while out.len() < want {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, msg))) => {
                    if let Some(item) = extract(msg) {
                        if seen.insert(from) {
                            out.push((from, item));
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        out
    }

    /// Like [`collect_by_shard`](Self::collect_by_shard) but keeps each item
    /// tagged with its responder, and stops once `done(plan, items)` holds for
    /// every planned shard (or at the deadline). The responder is needed to gate
    /// the fast path on the **electorate** subset of a shard's replicas.
    async fn collect_by_shard_tagged<T>(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        done: impl Fn(&ShardPlan, &[(NodeId, T)]) -> bool,
        deadline: Instant,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> HashMap<ShardId, Vec<(NodeId, T)>> {
        let mut got: HashMap<ShardId, Vec<(NodeId, T)>> =
            plans.iter().map(|p| (p.shard, Vec::new())).collect();
        // One vote per responder (see `collect_tagged`).
        let mut seen: HashSet<NodeId> = HashSet::new();

        loop {
            if plans.iter().all(|p| done(p, &got[&p.shard])) {
                break;
            }
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, msg))) => {
                    if let Some(item) = extract(msg) {
                        if let Some(shard) = self.topology.node_shard(from) {
                            if let Some(bucket) = got.get_mut(&shard) {
                                if seen.insert(from) {
                                    bucket.push((from, item));
                                }
                            }
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        got
    }

    /// Collects matching responses grouped by the responder's shard, stopping
    /// once every planned shard has `target(plan)` items (or at the deadline).
    async fn collect_by_shard<T>(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        target: impl Fn(&ShardPlan) -> usize,
        deadline: Instant,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> HashMap<ShardId, Vec<T>> {
        let mut got: HashMap<ShardId, Vec<T>> =
            plans.iter().map(|p| (p.shard, Vec::new())).collect();
        // One vote per responder (see `collect_tagged`).
        let mut seen: HashSet<NodeId> = HashSet::new();

        loop {
            if plans.iter().all(|p| got[&p.shard].len() >= target(p)) {
                break;
            }
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, msg))) => {
                    if let Some(item) = extract(msg) {
                        if let Some(shard) = self.topology.node_shard(from) {
                            if let Some(bucket) = got.get_mut(&shard) {
                                if seen.insert(from) {
                                    bucket.push(item);
                                }
                            }
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        got
    }

    /// Sends `message` to every node in `nodes`, ignoring transport-level loss.
    /// Delegated to the sink so a serializing transport encodes once for the
    /// whole fan-out instead of cloning and re-encoding per peer.
    async fn broadcast(&self, nodes: &[NodeId], message: Message) {
        let _ = self.sink.broadcast(nodes, message).await;
    }

    /// Sends a single message, ignoring transport-level loss.
    async fn send(&self, to: NodeId, message: Message) {
        let _ = self.sink.send(to, message).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use evento_core::Event;

    use crate::api::{DataStore, Journal, ShardedTopology, Topology};
    use crate::clock::{HybridLogicalClock, NodeId, Timestamp, TxnId};
    use crate::message::{CommandState, Key, Message};
    use crate::store::{InMemoryDataStore, InMemoryJournal};
    use crate::transport::InMemoryNetwork;
    use crate::{api::MessageSink, StaticTopology};

    use super::{Node, NodeConfig};

    /// A [`Journal`] that counts `stage`/`flush` calls so a test can observe
    /// group commit: many staged writes, few flushes.
    #[derive(Default)]
    struct CountingJournal {
        inner: InMemoryJournal,
        stages: AtomicUsize,
        flushes: AtomicUsize,
    }

    #[async_trait]
    impl Journal for CountingJournal {
        async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
            self.stage(state).await?;
            self.flush().await
        }
        async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
            self.stages.fetch_add(1, Ordering::SeqCst);
            self.inner.record(state).await
        }
        async fn flush(&self) -> anyhow::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
            self.inner.load(txn).await
        }
        async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
            self.inner.load_all().await
        }
    }

    /// A [`Journal`] whose `flush` fails — models a node that cannot fsync (disk
    /// error / full disk). `stage` still records so we can confirm the message was
    /// processed; only the durability barrier fails.
    #[derive(Default)]
    struct FlakyJournal {
        inner: InMemoryJournal,
        stages: AtomicUsize,
    }

    #[async_trait]
    impl Journal for FlakyJournal {
        async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
            self.stage(state).await?;
            self.flush().await
        }
        async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
            self.stages.fetch_add(1, Ordering::SeqCst);
            self.inner.record(state).await
        }
        async fn flush(&self) -> anyhow::Result<()> {
            anyhow::bail!("simulated fsync failure")
        }
        async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
            self.inner.load(txn).await
        }
        async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
            self.inner.load_all().await
        }
    }

    fn preaccept(i: u64) -> Message {
        let txn = TxnId(Timestamp {
            micros: i + 1,
            logical: 0,
            node: NodeId(9),
        });
        let agg = format!("acc{i}");
        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".into(),
            aggregate_id: agg.clone(),
            version: 1,
            name: "Bumped".into(),
            ..Default::default()
        };
        Message::PreAccept {
            txn,
            keys: vec![Key(agg)],
            events: vec![event],
        }
    }

    /// A burst of messages already queued in the inbox is drained as one batch and
    /// made durable with a **single** `flush` — group commit. Without batching this
    /// would be one fsync per message.
    #[tokio::test(start_paused = true)]
    async fn inbox_drains_a_burst_into_one_group_commit() {
        const N: u64 = 50;
        let id = NodeId(0);
        let ids = vec![id, NodeId(1), NodeId(2)];
        let net = InMemoryNetwork::new();
        let inbox = net.register(id);
        let journal = Arc::new(CountingJournal::default());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(NodeId(1))) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        );

        // Queue the whole burst *before* the loop starts, so its first iteration
        // drains all of them in one batch.
        let coordinator = net.sink(NodeId(1));
        for i in 0..N {
            coordinator.send(id, preaccept(i)).await.unwrap();
        }

        let _loop = node.start(inbox);

        // Let the loop drain + flush, then check the counters.
        for _ in 0..1000 {
            if journal.stages.load(Ordering::SeqCst) as u64 == N {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(
            journal.stages.load(Ordering::SeqCst) as u64,
            N,
            "every message should stage its record"
        );
        assert_eq!(
            journal.flushes.load(Ordering::SeqCst),
            1,
            "the whole queued burst should cost exactly one group-commit flush"
        );
    }

    /// Durability gate: if the journal `flush` (fsync) fails, the node must
    /// **withhold** the batch's durability-gated replies — it processed the message
    /// (staged it) but must not ack a decision it could not persist. The withheld
    /// reply is just the loss quorums/recovery already tolerate.
    #[tokio::test(start_paused = true)]
    async fn flush_failure_withholds_the_durability_gated_ack() {
        let node_id = NodeId(0);
        let coord = NodeId(1);
        let ids = vec![node_id, coord, NodeId(2)];
        let net = InMemoryNetwork::new();
        let inbox = net.register(node_id);
        let mut coord_inbox = net.register(coord);
        let journal = Arc::new(FlakyJournal::default());

        let node = Node::new(
            node_id,
            Arc::new(StaticTopology::new(node_id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(node_id)),
            Arc::new(net.sink(node_id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        );
        let _loop = node.start(inbox);

        // The coordinator sends a PreAccept; normally the node replies PreAcceptOk
        // once the record is durable.
        net.sink(coord).send(node_id, preaccept(0)).await.unwrap();

        // Wait until the node has processed (staged) it — so the flush was attempted.
        for _ in 0..1000 {
            if journal.stages.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            journal.stages.load(Ordering::SeqCst),
            1,
            "the message was processed and its record staged"
        );
        // Give the loop ample opportunity to (not) send the reply.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            coord_inbox.try_recv().is_err(),
            "a failed fsync must withhold the durability-gated PreAcceptOk"
        );
    }

    /// A custom [`NodeConfig`] is honoured end to end: a small group-commit batch
    /// cap splits the same queued burst into several flushes instead of one.
    #[tokio::test(start_paused = true)]
    async fn config_caps_the_group_commit_batch() {
        const N: u64 = 50;
        const BATCH: usize = 8;
        let id = NodeId(0);
        let ids = vec![id, NodeId(1), NodeId(2)];
        let net = InMemoryNetwork::new();
        let inbox = net.register(id);
        let journal = Arc::new(CountingJournal::default());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(NodeId(1))) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        )
        .with_config(NodeConfig {
            max_journal_batch: BATCH,
            ..NodeConfig::default()
        });
        assert_eq!(node.config().max_journal_batch, BATCH);

        let coordinator = net.sink(NodeId(1));
        for i in 0..N {
            coordinator.send(id, preaccept(i)).await.unwrap();
        }
        let _loop = node.start(inbox);

        for _ in 0..1000 {
            if journal.stages.load(Ordering::SeqCst) as u64 == N {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(journal.stages.load(Ordering::SeqCst) as u64, N);
        assert_eq!(
            journal.flushes.load(Ordering::SeqCst),
            N.div_ceil(BATCH as u64) as usize,
            "the configured batch cap splits the burst into multiple group commits"
        );
    }

    /// Backpressure: once the un-compacted command backlog reaches `max_commands`,
    /// the node refuses to coordinate a **new** write, while the existing consensus
    /// state it already holds is retained untouched (never evicted).
    #[tokio::test(start_paused = true)]
    async fn max_commands_refuses_new_writes_but_keeps_existing_state() {
        const CAP: usize = 3;
        let id = NodeId(0);
        let ids = vec![id, NodeId(1), NodeId(2)];
        let net = InMemoryNetwork::new();
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(NodeId(1))) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        )
        .with_config(NodeConfig {
            max_commands: CAP,
            ..NodeConfig::default()
        });

        // Fill the backlog to the cap via peer PreAccepts (the replica-side path,
        // which is never gated).
        let coordinator = net.sink(NodeId(1));
        for i in 0..CAP as u64 {
            coordinator.send(id, preaccept(i)).await.unwrap();
        }
        let _loop = node.start(inbox);
        for _ in 0..1000 {
            if node.command_count() == CAP {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(node.command_count(), CAP, "backlog filled to the cap");

        // A new local write is refused rather than growing the backlog further.
        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".into(),
            aggregate_id: "new".into(),
            version: 1,
            name: "Opened".into(),
            ..Default::default()
        };
        let err = node.write(vec![event]).await.unwrap_err();
        assert!(
            err.to_string().contains("backlog at cap"),
            "the refusal names the backpressure cause: {err}"
        );

        // Existing consensus state is intact — backpressure shed *new load*, it did
        // not drop anything already accepted.
        assert_eq!(
            node.command_count(),
            CAP,
            "existing commands are retained, never evicted"
        );
    }

    /// A read for a key this node doesn't own is routed to a **same-region** owner
    /// when regions are configured — even when a remote-region owner is listed
    /// first in the replica set.
    #[test]
    fn read_routing_prefers_a_same_region_owner() {
        use std::collections::HashMap;

        // Shard 1's owners are listed region-1 first, so a naive "first other
        // owner" would pick a remote node; region preference must pick node 2.
        let shards = vec![
            vec![NodeId(0), NodeId(1)],
            vec![NodeId(3), NodeId(4), NodeId(2)],
        ];
        let regions = HashMap::from([
            (NodeId(0), 0u16),
            (NodeId(1), 1),
            (NodeId(2), 0),
            (NodeId(3), 1),
            (NodeId(4), 1),
        ]);
        let topology = Arc::new(ShardedTopology::new(NodeId(0), shards).with_regions(regions));
        let net = InMemoryNetwork::new();
        let node = Node::new(
            NodeId(0),
            topology.clone() as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(NodeId(0))),
            Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );

        // A key node 0 does not own (it hashes to shard 1).
        let key = (0..)
            .map(|i| Key(format!("k{i}")))
            .find(|k| topology.shard_of(k) == 1)
            .expect("a shard-1 key");
        assert!(!node.owns_key(&key));

        let owner = node.an_owner_of(&key).expect("an owner");
        assert_eq!(
            topology.region(owner),
            Some(0),
            "read routed to a same-region owner"
        );
        assert_eq!(
            owner,
            NodeId(2),
            "the region-0 owner, not the region-1 nodes listed first"
        );
    }

    /// Config-Paxos crash safety: an acceptor that promised a ballot must, after a
    /// restart, still reject a lower ballot — its `promised` is durable.
    #[tokio::test]
    async fn restarted_acceptor_rejects_stale_ballot() {
        use crate::clock::Ballot;
        use crate::transport::Envelope;

        let id = NodeId(0);
        let ids = vec![id, NodeId(1), NodeId(2)];
        let net = InMemoryNetwork::new();
        // A durable journal handle kept across the simulated restart.
        let journal = Arc::new(InMemoryJournal::new());
        let build = || {
            Node::new(
                id,
                Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
                Arc::new(HybridLogicalClock::new(id)),
                Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
                Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
                Arc::clone(&journal) as Arc<dyn Journal>,
            )
        };
        let ballot = |micros: u64| {
            Ballot(Timestamp {
                micros,
                logical: 0,
                node: NodeId(9),
            })
        };

        // Acceptor promises a high ballot for epoch 1 (persisted before the reply).
        let node = build();
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::ConfigPrepare {
                    epoch: 1,
                    ballot: ballot(1000),
                },
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::ConfigPromise { .. })]),
            "high ballot is promised"
        );

        // Restart: a fresh node over the same durable journal restores `promised`.
        let restarted = build();
        restarted.recover_state().await.unwrap();
        let out = restarted
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::ConfigPrepare {
                    epoch: 1,
                    ballot: ballot(1), // lower than the promised 1000
                },
            })
            .await;
        match out.as_slice() {
            [(_, Message::ConfigNack { promised, .. })] => {
                assert_eq!(*promised, ballot(1000), "restored the promised ballot")
            }
            other => panic!("a restarted acceptor must reject a stale ballot: {other:?}"),
        }
    }
}
