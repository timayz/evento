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
    cursor::{Args, Edge, ReadResult, Value},
    Event, EventFilter, RoutingKey,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::api::{
    floor_millis, snapshot_event_size, stamp_events, validate_layout, AcceptorRecord, DataStore,
    Journal, MessageSink, ShardId, Topology,
};
use crate::clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
use crate::failure_detector::FailureDetector;
use crate::message::{CommandState, Key, Message, Status, SyncKnown, INVALIDATED_AT};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::replica::{command_size, Refusal, Replica, RoutedRecovery};
use crate::sweep::SweepPressure;
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
    /// sweep, but on its own — typically slower — cadence). Also paces how
    /// quickly a node re-proves the sync coverage that unclamps its compaction
    /// watermark reports (a replica only vouches for a window once syncs with
    /// enough shard peers have covered it), so keep it well below
    /// [`compaction_margin`](Self::compaction_margin) or compaction lags the
    /// margin by the extra coverage round-trips.
    pub anti_entropy_interval: Duration,
    /// A transaction unapplied this long is presumed stalled and recovered. Well
    /// above normal write latency so healthy in-flight writes are never disturbed.
    pub recovery_timeout: Duration,
    /// How far behind the present a node's reported redundancy point lags, so a
    /// not-yet-propagated commit is never skipped when state below the watermark is
    /// compacted away. Keep it above the clock-skew bound and propagation latency.
    pub compaction_margin: Duration,
    /// Least time between two journal truncations. The replica compacts in
    /// memory on every sweep, but a journal truncate is a write transaction
    /// (an fsync, and on SQLite the writer lock) that under steady writes
    /// would otherwise run every sweep and queue behind the event commits.
    /// The journal keeps at most this much of a redundant tail beyond the
    /// replica; a restart replays it idempotently. Default 5 s.
    pub truncate_interval: Duration,
    /// Most messages the inbox loop stages before forcing a group-commit flush.
    /// Caps the latency a deferred reply waits for its batch's fsync and the memory
    /// held for the batch's pending sends.
    pub max_journal_batch: usize,
    /// Suspicion threshold for the phi-accrual failure detector: a coordinator
    /// whose φ exceeds this is presumed dead and its stalled transactions are
    /// recovered (adaptively, ahead of `recovery_timeout`). Higher tolerates more
    /// latency variance before suspecting.
    pub phi_threshold: f64,
    /// How long a live peer may go quiet beyond its usual heartbeat interval
    /// before the failure detector's suspicion starts to rise: room for a
    /// scheduler stall, a slow fsync or a loaded machine. A coordinator is
    /// suspected roughly this much later than its rhythm alone would allow.
    pub acceptable_pause: Duration,
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
    /// How long a same-shard peer may stay silent before this node stops
    /// waiting for it. Compaction needs every shard peer's watermark, so a
    /// peer that is down holds every command on the others until it returns —
    /// and `max_commands` then refuses writes. Past this limit the peer is
    /// *absent*: compaction proceeds below the minimum of the peers still
    /// heard from. An absent peer is counted again as soon as it reports a
    /// watermark above everything compacted meanwhile (it was merely quiet,
    /// or down while nothing was written); if transactions it never applied
    /// were compacted, it is told to rejoin from a snapshot instead
    /// ([`Message::Rejoin`], [`Node::rejoin_required`]), since anti-entropy
    /// can no longer ship them. A node never stops waiting for so many peers
    /// that fewer than a slow quorum stay counted, so a partitioned minority
    /// cannot compact on its own. `None` (the default) waits forever.
    pub peer_absence_limit: Option<Duration>,
    /// Most events this node puts in one snapshot page when a joining peer
    /// bootstraps from it ([`Message::SnapshotPageRequest`]). With
    /// [`snapshot_page_bytes`](Self::snapshot_page_bytes), bounds the memory
    /// a page holds and the size of its frame.
    pub snapshot_page_events: u16,
    /// Byte budget of one snapshot page (the sum of the events' wire sizes,
    /// [`snapshot_event_size`]); a single event larger than it still crosses
    /// alone. Keep it far below the transport's frame cap (64 MiB on the
    /// TCP transport), and small enough that one group's bootstrap does not
    /// hold the link it shares with every other group for long: the default
    /// 4 MiB leaves sixteen times the headroom.
    pub snapshot_page_bytes: usize,
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
            truncate_interval: Duration::from_secs(5),
            max_journal_batch: 128,
            phi_threshold: 8.0,
            acceptable_pause: Duration::from_millis(500),
            linearizable_reads: false,
            max_commands: 100_000,
            peer_absence_limit: None,
            snapshot_page_events: 4096,
            snapshot_page_bytes: 4 * 1024 * 1024,
            config_defer_step: Duration::from_millis(100),
        }
    }
}

/// The node's view of the journal's truncation: see `Node::journal_mark`.
#[derive(Debug, Clone, Copy)]
struct JournalMark {
    /// The watermark the journal is known to have persisted; advanced on a
    /// successful truncate only, so a failed one is retried.
    persisted: Timestamp,
    /// When the journal was last asked to truncate, successful or not — the
    /// pacing clock, so a truncate that failed (say, timed out behind the
    /// event commits) is not hammered on every sweep either.
    last_attempt: Option<Instant>,
}

impl Default for JournalMark {
    fn default() -> Self {
        Self {
            persisted: Timestamp::MIN,
            last_attempt: None,
        }
    }
}

/// [`Node::write`] refused a new write: the un-compacted backlog reached
/// [`NodeConfig::max_commands`] (compaction stalled — a peer is down or
/// partitioned). Returned through `anyhow`; downcast to tell it apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogAtCap {
    /// Commands this node holds.
    pub backlog: usize,
    /// The configured cap.
    pub cap: usize,
}

impl std::fmt::Display for BacklogAtCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consensus backlog at cap ({} >= {}); refusing new write",
            self.backlog, self.cap
        )
    }
}

impl std::error::Error for BacklogAtCap {}

/// The most a single transaction's events may weigh on the wire
/// ([`snapshot_event_size`] summed): half the TCP transport's frame cap
/// ([`MAX_FRAME_LENGTH`](crate::tcp::MAX_FRAME_LENGTH)), leaving room for
/// the dependencies, keys and framing every message carrying it adds. A
/// command is the one unit no page can split: anti-entropy and a join ship
/// at least one per frame, so a heavier one could never be repaired onto a
/// replica that missed it.
pub const MAX_COMMAND_BYTES: usize = crate::tcp::MAX_FRAME_LENGTH / 2;

/// [`Node::write`] refused a new write: its events weigh more on the wire
/// than [`MAX_COMMAND_BYTES`]. Split it into several transactions. Returned
/// through `anyhow`; downcast to tell it apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandTooLarge {
    /// The transaction's wire size ([`snapshot_event_size`] summed).
    pub bytes: usize,
    /// The cap, [`MAX_COMMAND_BYTES`].
    pub cap: usize,
}

impl std::fmt::Display for CommandTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transaction too large to replicate ({} bytes > {}); split it",
            self.bytes, self.cap
        )
    }
}

impl std::error::Error for CommandTooLarge {}

/// [`Node::write`] refused a new write: a peer told this node to rejoin the
/// group from a snapshot (see [`NodeConfig::peer_absence_limit`]), so it no
/// longer takes part until its host does. Returned through `anyhow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejoinRequired;

impl std::fmt::Display for RejoinRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("this node fell behind its peers' compaction and must rejoin the group")
    }
}

impl std::error::Error for RejoinRequired {}

/// The node was closed by its host ([`GroupHost::close`](crate::GroupHost::close)):
/// it mints no timestamps any more, so it coordinates nothing — the group's
/// reopened node does. Returned through `anyhow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeClosed;

impl std::fmt::Display for NodeClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("this node was closed by its host")
    }
}

impl std::error::Error for NodeClosed {}

/// [`Node::join`] failed after at least one snapshot page was installed, so
/// the store holds a prefix no other contact can complete: a page that
/// overlaps existing events is skipped whole as an idempotent re-apply, which
/// would leave a gap. Empty the store (and journal) before joining again.
/// Attached as context on the error; downcast to tell it apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialSnapshot {
    /// Pages installed before the failure.
    pub pages: usize,
    /// Events installed before the failure.
    pub events: usize,
}

impl std::fmt::Display for PartialSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the join installed {} events over {} pages before failing; empty the store before joining again",
            self.events, self.pages
        )
    }
}

impl std::error::Error for PartialSnapshot {}

/// How many times a joiner asks for the same snapshot page before giving the
/// contact up: a page request and its reply are idempotent, so a frame lost
/// on a lossy link only costs one more round trip.
const SNAPSHOT_PAGE_ATTEMPTS: usize = 3;

/// What an [`import_from`](Node::import_from) round brought in.
#[derive(Debug, Clone, Copy, Default)]
struct Imported {
    /// Commands imported into the conflict graph.
    commands: usize,
    /// Snapshot pages installed (bootstrap only).
    pages: usize,
    /// Snapshot events installed (bootstrap only).
    events: usize,
    /// Wire size of the snapshot events installed (bootstrap only).
    bytes: usize,
}

/// A same-shard peer this node stopped waiting for; see
/// [`NodeConfig::peer_absence_limit`].
#[derive(Debug, Clone, Copy)]
struct AbsentPeer {
    /// What it last reported ([`Timestamp::MIN`] if never): a report is a
    /// proof that the peer holds every transaction with a lower `t0`.
    last_report: Timestamp,
    /// The newest `t0` compacted since at or above that report — what the peer
    /// may lack. It must report above this to be counted again; until then it
    /// is told to rejoin. Persisted through the journal.
    rejoin_above: Option<Timestamp>,
}

/// What to do with a message from an absent peer.
enum Screen {
    /// Not absent, or a join request: handle it.
    Pass,
    /// Reinstated by this message; the bounds changed if `persist`.
    Reinstated { persist: bool },
    /// Still behind: answer this instead.
    Reply(Message),
}

/// The [`Message::JoinProof`] slot of an in-flight join, dropped from
/// `Node::proof_pending` however the join ends.
struct PendingProof<'a> {
    node: &'a Node,
    id: u64,
}

impl PendingProof<'_> {
    /// The vouch the contact sent, if any arrived.
    fn take(self) -> Option<Vouch> {
        self.node
            .proof_pending
            .lock()
            .expect("proof poisoned")
            .get_mut(&self.id)
            .and_then(Option::take)
    }
}

impl Drop for PendingProof<'_> {
    fn drop(&mut self) {
        self.node
            .proof_pending
            .lock()
            .expect("proof poisoned")
            .remove(&self.id);
    }
}

/// A join this node is serving as the contact (see `Node::join_holds`).
///
/// The joiner's first report is what this node vouched for in its header.
/// A peer that gave the joiner up counts it again only on a report above
/// everything it compacted since, and it compacts below the minimum of the
/// reports it counts — this node's among them. Holding this node's report
/// at the vouched point until the joiner reports keeps every such peer's
/// compaction, and so its bound, below that first report however long the
/// pages take; otherwise a write load during a long join would have the
/// joiner told to rejoin again, every time.
struct JoinHold {
    vouched: Timestamp,
    /// The joiner's last header or page request. A joiner that stops asking
    /// for [`JOIN_HOLD_PAGE_INTERVALS`] page timeouts gave up (or died): the
    /// hold lapses, so a failed join cannot pin compaction.
    refreshed: Instant,
}

/// How many page timeouts (`NodeConfig::collect_timeout`, a joiner's wait
/// per page attempt) a join hold outlives its joiner's last request by: all
/// of a page's attempts plus one more for installing it.
const JOIN_HOLD_PAGE_INTERVALS: u32 = SNAPSHOT_PAGE_ATTEMPTS as u32 + 1;

/// One config-Paxos proposal's slot in `Node::config_inbox`, dropped
/// however the proposal ends.
struct ConfigRound<'a> {
    node: &'a Node,
    epoch: u64,
    id: u64,
}

impl<'a> ConfigRound<'a> {
    fn open(node: &'a Node, epoch: u64) -> (Self, mpsc::UnboundedReceiver<(NodeId, Message)>) {
        let id = node.correlation_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        node.config_inbox
            .lock()
            .expect("config poisoned")
            .entry(epoch)
            .or_default()
            .insert(id, tx);
        (Self { node, epoch, id }, rx)
    }
}

impl Drop for ConfigRound<'_> {
    fn drop(&mut self) {
        let mut inbox = self.node.config_inbox.lock().expect("config poisoned");
        if let Some(rounds) = inbox.get_mut(&self.epoch) {
            rounds.remove(&self.id);
            if rounds.is_empty() {
                inbox.remove(&self.epoch);
            }
        }
    }
}

/// Ends a write's coordination when dropped (see [`Node::write`]).
struct Coordinating<'a> {
    node: &'a Node,
    txn: TxnId,
}

impl Drop for Coordinating<'_> {
    fn drop(&mut self) {
        self.node.deregister(self.txn);
        self.node
            .coordinating
            .lock()
            .expect("coordinating poisoned")
            .remove(&self.txn);
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
    /// This shard's slice of the transaction's keys — what PreAccept / Accept /
    /// Commit carry to this shard's replicas instead of the full set.
    keys: Vec<Key>,
    /// This shard's slice of the transaction's events.
    events: Vec<Event>,
}

/// One replica's recovery report, extracted from a `RecoverOk`.
struct RecoverFields {
    known: bool,
    status: Status,
    accepted: Ballot,
    execute_at: Timestamp,
    deps: Vec<TxnId>,
    superseding_rejects: bool,
    wait: bool,
    keys: Vec<Key>,
    events: Vec<Event>,
    /// The transaction's route, when the report came routed
    /// ([`Message::Routed`]): the replica holds a multi-shard transaction.
    route: Option<Vec<Key>>,
}

impl RecoverFields {
    /// Whether the replica recorded the transaction as invalidated.
    fn invalidated(&self) -> bool {
        self.known && self.status == Status::Applied && self.execute_at == INVALIDATED_AT
    }
}

/// A response to a `Recover`: a report, a "never witnessed" (routed recovery
/// only), or a ballot rejection.
enum RecoverResp {
    Ok(RecoverFields),
    NotWitnessed { redundant: bool },
    Nack(Ballot),
}

/// How one recovery attempt ended (see [`Node::recover_with`]).
enum Attempt {
    /// Decided: the outcome.
    Done(CommitOutcome),
    /// A higher ballot was promised: retry above it.
    Retry(Ballot),
    /// A report revealed the transaction's multi-shard route: retry routed.
    Route(Vec<Key>),
}

/// One route shard's replies to a routed recovery, by kind.
#[derive(Default)]
struct ShardReports {
    /// Reports from replicas holding the transaction.
    known: usize,
    /// Replicas that never witnessed it and promised the ballot.
    blind: usize,
    /// Replicas holding nothing of it below their redundancy watermark.
    redundant: usize,
    /// Replies of any kind.
    heard: usize,
}

/// What a routed recovery's reports prove (see [`Node::route_verdict`]).
enum Verdict {
    /// The transaction never committed and never will: abort it everywhere.
    Invalidate,
    /// Recover it on these shards (the others applied it and compacted it).
    Recover(Vec<ShardId>),
    /// Not enough evidence yet; the sweep retries.
    Undecided(String),
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

/// Delivers config-Paxos responses to every in-flight proposal for their
/// epoch, keyed epoch → proposal id (see [`ConfigRound`]). Two concurrent
/// proposals on one node (e.g. a manual change and the sweep's recovery)
/// each hear every reply and keep only those answering their own ballot, so
/// neither can take or tear down the other's.
type ConfigInbox = Arc<Mutex<HashMap<u64, HashMap<u64, ConfigSender>>>>;
type ConfigSender = mpsc::UnboundedSender<(NodeId, Message)>;

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
/// contact's `(watermark, coverage, snapshot events, recent commands)`.
type SyncPayload = (Timestamp, Timestamp, Vec<Event>, Vec<CommandState>);

/// A join contact's vouch ([`Message::JoinProof`]): `(epoch, vouched)`.
type Vouch = (u64, Timestamp);

/// What this node's [`join`](Node::join) proved: the contact's [`Vouch`],
/// and `until`, this node's clock when the join completed (plus a skew). A transaction
/// committed before the join's inbox started but not yet applied by the
/// contact when it vouched is in neither the snapshot nor the replayed
/// inbox: until its own anti-entropy has covered everything below `until`,
/// this node holds provably only what was vouched for.
#[derive(Clone, Copy)]
struct JoinProven {
    epoch: u64,
    vouched: Timestamp,
    until: Timestamp,
}

/// Most bytes of an owner's error a [`Message::ReadFailed`] carries back.
const READ_FAILURE_MAX: usize = 256;

/// `err` as a [`Message::ReadFailed`] reason: its chain, cut to
/// [`READ_FAILURE_MAX`] bytes on a character boundary.
fn read_failure(err: &anyhow::Error) -> String {
    let mut reason = format!("{err:#}");
    if reason.len() > READ_FAILURE_MAX {
        let mut cut = READ_FAILURE_MAX;
        while !reason.is_char_boundary(cut) {
            cut -= 1;
        }
        reason.truncate(cut);
    }
    reason
}

/// Delivers each sync response to its in-flight [`join`](Node::join)/anti-entropy
/// request, keyed by the request's correlation id — so a concurrent join and
/// anti-entropy round cannot steal each other's data.
type SyncInbox = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<SyncPayload>>>>;

/// A cluster node. Cheap to clone — all state is shared behind `Arc`.
#[derive(Clone)]
pub struct Node {
    id: NodeId,
    clock: Arc<HybridLogicalClock>,
    /// Every timestamp this node has minted is below these microseconds, and
    /// the journal holds them ([`Journal::save_clock_bound`]); raised by
    /// [`mint`](Node::mint) before a stamp reaches them. 0 until the first.
    clock_bound: Arc<AtomicU64>,
    /// Serializes raising [`clock_bound`](Self::clock_bound), and
    /// [`retire`](Node::retire) against it.
    clock_bound_lock: Arc<tokio::sync::Mutex<()>>,
    /// Set once this node's host closed it: it mints nothing more.
    retired: Arc<AtomicBool>,
    topology: Arc<dyn Topology>,
    sink: Arc<dyn MessageSink>,
    datastore: Arc<dyn DataStore>,
    journal: Arc<dyn Journal>,
    replica: Arc<Mutex<Replica>>,
    pending: Pending,
    /// Transactions this node is coordinating, from their first PreAccept
    /// until their outcome is known. The watermark this node reports stays
    /// below the oldest, so no replica compacts a transaction its coordinator
    /// may still have to learn the outcome of through recovery.
    coordinating: Arc<Mutex<BTreeSet<TxnId>>>,
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
    /// requests and snapshot pages (each consumer only needs unique — not
    /// gap-free — values).
    correlation_seq: Arc<AtomicU64>,
    /// The anti-entropy and metadata catch-up rounds' turns, each picking the
    /// next peer. Counters of their own: drawn from `correlation_seq`, a turn
    /// advanced with every other id drawn in between — the sync round's own
    /// among them — so a rotation could keep landing on the same peer.
    anti_entropy_turn: Arc<AtomicU64>,
    metadata_turn: Arc<AtomicU64>,
    /// Channels awaiting forwarded-read replies, by correlation id.
    read_pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    /// Channels awaiting snapshot pages ([`Message::SnapshotPage`]) during a
    /// [`join`](Node::join), by the page request's correlation id.
    snapshot_pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    /// Latest `applied_through` gossiped by each shard peer (and self); the
    /// recovery sweep compacts below the per-shard minimum of these.
    peer_watermarks: Arc<Mutex<HashMap<NodeId, Timestamp>>>,
    /// What the journal has been asked to truncate: the sweep calls
    /// [`compact`](Node::compact) continuously, and a journal's `truncate` is
    /// a write transaction (an fsync, and on SQLite the writer lock) even when
    /// it has nothing to delete — so it is only asked when the watermark moved
    /// past what it persisted, and at most once per
    /// [`truncate_interval`](NodeConfig::truncate_interval).
    journal_mark: Arc<Mutex<JournalMark>>,
    /// Proven sync coverage per shard peer: a successful sync round with `peer`
    /// established that every transaction `peer` had applied below the recorded
    /// timestamp is in this node's store. Clamps the watermark this node
    /// reports — a replica must not vouch for a window it wasn't connected for
    /// (it cannot know what it never witnessed), only for one it has provably
    /// re-covered with enough peers to intersect every commit quorum.
    /// Max-merged per peer, keyed to the topology epoch it was accrued under
    /// (cleared on epoch change); never persisted — a restarted node re-proves
    /// its coverage, which is conservative.
    sync_coverage: Arc<Mutex<(u64, HashMap<NodeId, Timestamp>)>>,
    /// What the contact of this node's [`join`](Node::join) vouched for
    /// ([`Message::JoinProof`]), with the topology epoch it holds under: the
    /// joiner copied a state holding every committed transaction below it,
    /// so it counts as proven coverage next to
    /// [`sync_coverage`](Self::sync_coverage) — a fresh joiner cannot
    /// otherwise prove anything until anti-entropy rounds with enough peers
    /// have run. Ignored once the epoch changes; never persisted.
    join_proof: Arc<Mutex<Option<JoinProven>>>,
    /// The [`JoinProof`](Message::JoinProof) of an in-flight join, by its
    /// header request's correlation id (`None` until it arrives).
    proof_pending: Arc<Mutex<HashMap<u64, Option<Vouch>>>>,
    /// Same-shard joiners this node vouched for and is serving snapshot
    /// pages to. Until a joiner reports, this node's own report stays at what
    /// it vouched, so no peer compacts past the joiner's first report while
    /// its pages cross the wire (see [`JoinHold`]).
    join_holds: Arc<Mutex<HashMap<NodeId, JoinHold>>>,
    /// Phi-accrual liveness estimator: every inbound message is a heartbeat, and a
    /// coordinator suspected dead has its stalled transactions recovered.
    failure_detector: Arc<FailureDetector>,
    /// When this node was built: the silence of a peer never heard from is
    /// measured from here.
    started_at: Instant,
    /// Same-shard peers this node stopped waiting for (silent past
    /// [`NodeConfig::peer_absence_limit`]). Locked before `peer_watermarks`
    /// wherever both are held.
    absent: Arc<Mutex<HashMap<NodeId, AbsentPeer>>>,
    /// True once a peer told this node to rejoin: it then drops every message
    /// and refuses writes until its host replaces its store and opens the
    /// group by joining (`GroupHost::open_joining`).
    rejoin_required: Arc<tokio::sync::watch::Sender<bool>>,
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
            clock_bound: Arc::new(AtomicU64::new(0)),
            clock_bound_lock: Arc::new(tokio::sync::Mutex::new(())),
            retired: Arc::new(AtomicBool::new(false)),
            topology,
            sink,
            datastore,
            journal,
            replica: Arc::new(Mutex::new(Replica::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            coordinating: Arc::new(Mutex::new(BTreeSet::new())),
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
            anti_entropy_turn: Arc::new(AtomicU64::new(0)),
            metadata_turn: Arc::new(AtomicU64::new(0)),
            read_pending: Arc::new(Mutex::new(HashMap::new())),
            snapshot_pending: Arc::new(Mutex::new(HashMap::new())),
            peer_watermarks: Arc::new(Mutex::new(HashMap::new())),
            journal_mark: Arc::new(Mutex::new(JournalMark::default())),
            sync_coverage: Arc::new(Mutex::new((start_epoch, HashMap::new()))),
            join_proof: Arc::new(Mutex::new(None)),
            proof_pending: Arc::new(Mutex::new(HashMap::new())),
            join_holds: Arc::new(Mutex::new(HashMap::new())),
            failure_detector: Arc::new(FailureDetector::new()),
            started_at: Instant::now(),
            absent: Arc::new(Mutex::new(HashMap::new())),
            rejoin_required: Arc::new(tokio::sync::watch::channel(false).0),
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

    /// How many decision records of compacted multi-shard transactions this
    /// node keeps, waiting for the transactions' other shards to apply them.
    /// For tests and observability.
    pub fn decision_count(&self) -> usize {
        self.replica
            .lock()
            .expect("replica poisoned")
            .decision_count()
    }

    /// How far `txn` has progressed on this replica, if it holds the command.
    /// For tests and observability.
    pub fn command_status(&self, txn: TxnId) -> Option<Status> {
        self.replica.lock().expect("replica poisoned").status(txn)
    }

    /// How many transactions this node has taken over via the recovery sweep — a
    /// healthy cluster recovers nothing. For tests and observability.
    pub fn recovery_count(&self) -> u64 {
        self.metrics.recoveries.load(Ordering::Relaxed)
    }

    /// Same-shard peers this node has stopped waiting for (see
    /// [`NodeConfig::peer_absence_limit`]), sorted. For observability.
    pub fn absent_peers(&self) -> Vec<NodeId> {
        let mut peers: Vec<NodeId> = self
            .absent
            .lock()
            .expect("absent poisoned")
            .keys()
            .copied()
            .collect();
        peers.sort_unstable();
        peers
    }

    /// How long since the last message from `peer` — since this node started,
    /// if it never heard from it. For observability.
    pub fn silent_for(&self, peer: NodeId) -> Duration {
        let heard = self
            .failure_detector
            .last_heard(peer)
            .unwrap_or(self.started_at);
        Instant::now().saturating_duration_since(heard)
    }

    /// Whether a peer told this node to rejoin the group from a snapshot: it
    /// was absent while transactions it never applied were compacted, so it
    /// can no longer catch up. The node drops every message and refuses writes
    /// from then on; the host closes it, discards its store and journal, and
    /// opens the group by joining (`GroupHost::open_joining`).
    pub fn rejoin_required(&self) -> bool {
        *self.rejoin_required.borrow()
    }

    /// A watch that turns `true` when [`rejoin_required`](Self::rejoin_required)
    /// does.
    pub fn rejoin_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        self.rejoin_required.subscribe()
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
    /// node already assumes). Exposed to
    /// [`Executor::stable_timestamp`](evento_core::Executor::stable_timestamp).
    ///
    /// Like the watermark this node reports for compaction, it is clamped to
    /// the window anti-entropy has proven with enough shard peers
    /// ([`quorum_sync_coverage`](Self::quorum_sync_coverage)): a replica cut off
    /// from the transactions its peers commit (a partition, a lossy link)
    /// cannot vouch for them, and its subscriptions would advance past events
    /// still on their way to it, skipping them for good. Until a sync round
    /// re-proves the window — after a start, a restart or a heal — its
    /// subscriptions wait (a joined node starts from what its contact
    /// vouched for, [`Message::JoinProof`]). The clamp keeps the clock skew
    /// bound below the proven window: no longer load-bearing for the stamps
    /// (an event is stamped at apply from its `execute_at`, never below its
    /// `t0`), it stays as a conservative hedge on a point a peer computed from
    /// its own clock.
    ///
    /// The result is floored to a millisecond, the precision events are
    /// stamped with: a transaction drawing `t0 = 1.5 ms` just above the bound
    /// still lands at `1.0 ms`, so an unfloored `1.5 ms` watermark would have
    /// let a subscription pass it.
    pub fn stable_micros(&self) -> u64 {
        let cutoff = self.margin_cutoff();
        let local = self
            .replica
            .lock()
            .expect("replica poisoned")
            .stable_event_micros(cutoff);
        let bound = match self.proven_coverage(&self.shard_peers()) {
            Some(coverage) => {
                local.min(coverage.micros.saturating_sub(self.clock.max_skew_micros()))
            }
            None => local,
        };
        floor_millis(bound)
    }

    /// The present lagged by [`compaction_margin`](NodeConfig::compaction_margin):
    /// the newest point this node may vouch for, since a message sent within the
    /// margin (bounded propagation + clock skew) may still be in flight to it.
    fn margin_cutoff(&self) -> Timestamp {
        let now = self.clock.now().micros;
        Timestamp {
            micros: now.saturating_sub(self.settings.compaction_margin.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        }
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

    /// One iteration of the loop [`start_recovery`](Node::start_recovery) runs:
    /// recover stalled transactions, one anti-entropy round, watermark gossip +
    /// compaction, metadata catch-up. For a process hosting **many** nodes (one
    /// consensus group per tenant), drive this from a shared
    /// [`SweepScheduler`](crate::sweep::SweepScheduler) instead of starting a
    /// recovery task per node — it sweeps idle groups far less often. Not
    /// re-entrant: do not run two sweeps of one node concurrently.
    pub async fn sweep(&self) {
        self.recovery_sweep().await;
    }

    /// Cheap liveness signal for a shared [`SweepScheduler`](crate::sweep::SweepScheduler):
    /// whether this node has un-applied or in-flight work (must be swept at full
    /// cadence), and a monotone counter that moves only when applied state
    /// advances (an apply or an anti-entropy import — *not* on sweep gossip, so
    /// idle peers cannot keep each other awake).
    pub fn sweep_pressure(&self) -> SweepPressure {
        let busy = self
            .replica
            .lock()
            .expect("replica poisoned")
            .has_unapplied()
            || !self.pending.lock().expect("pending poisoned").is_empty();
        SweepPressure {
            busy,
            activity: *self.applied_gen.borrow(),
        }
    }

    /// Rebuilds this node's state from the journal after a process restart:
    /// restores every command's consensus state (status, ballots, decision) and
    /// replays committed, applied transactions into the data store. Call once on
    /// startup before [`start`](Node::start). In-flight (unapplied) transactions
    /// resume via the recovery sweep.
    pub async fn recover_state(&self) -> anyhow::Result<()> {
        let watermark = self.journal.load_watermark().await?;
        self.journal_mark
            .lock()
            .expect("journal mark poisoned")
            .persisted = watermark.unwrap_or(Timestamp::MIN);
        let mut commands = self.journal.load_all().await?;
        commands.sort_by_key(|cmd| (cmd.execute_at, cmd.txn));
        let routes = self.journal.load_routes().await?;
        let promises = self.journal.load_promises().await?;
        let decisions = self.journal.load_decisions().await?;
        self.restore_clock(&commands).await?;

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
            // After the floor: what lies below it is redundant.
            for (txn, route) in routes {
                replica.restore_route(txn, route);
            }
            // A recovery promise for a transaction never witnessed still
            // fences the original coordinator after the restart.
            for (txn, ballot) in promises {
                replica.restore_promise(txn, ballot);
            }
            // The decisions of compacted multi-shard transactions another
            // shard may still hold undecided.
            for record in decisions {
                replica.restore_decision(record);
            }
        }
        // Peers this node had stopped waiting for stay excluded until they
        // report above what was compacted past them (or rejoin).
        let bounds = self.journal.load_rejoin_bounds().await?;
        if !bounds.is_empty() {
            let mut absent = self.absent.lock().expect("absent poisoned");
            for (peer, bound) in bounds {
                absent.insert(
                    peer,
                    AbsentPeer {
                        last_report: bound,
                        rejoin_above: Some(bound),
                    },
                );
            }
        }
        // Replay the retained (post-truncation) applied tail into the data store.
        // The truncated prefix is not in the journal; a durable data store already
        // holds it, and re-applying the tail is idempotent. A replay the store
        // fails fails the restart: the journal says these are applied, so a node
        // starting without them would never apply them again. The events are
        // stamped from `execute_at` as at the first apply, so a replay over a
        // store that already holds them is an exact duplicate.
        for cmd in &commands {
            if cmd.status == Status::Applied && cmd.applied_conflict == Some(false) {
                let mut events = cmd.events.clone();
                stamp_events(cmd.execute_at, &mut events);
                self.datastore
                    .apply(cmd.txn, cmd.execute_at, events, true)
                    .await
                    .map_err(|err| {
                        err.context(format!(
                            "replaying applied transaction {:?} into the data store",
                            cmd.txn
                        ))
                    })?;
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
    /// may clamp below `before` if a local transaction is not yet applied — and
    /// only when that watermark is above what the journal already persisted,
    /// at most once per [`truncate_interval`](NodeConfig::truncate_interval):
    /// the sweep calls this every pass, and a truncate costs the journal a
    /// write transaction whether or not it has much to delete. The decision
    /// records the compaction leaves are journaled first: until they are,
    /// the journal is not truncated.
    pub async fn compact(&self, before: Timestamp) {
        let (previous, watermark, newest) = {
            let mut replica = self.replica.lock().expect("replica poisoned");
            let previous = replica.redundant_before();
            let newest = replica.compact(before);
            (previous, replica.redundant_before(), newest)
        };
        if watermark > previous {
            self.metrics.record_compaction();
            tracing::debug!(node = self.id.0, ?watermark, "compacted consensus state");
        }
        // Against the journal's watermark, not `previous`: a truncate that
        // failed is retried (next interval) rather than waiting for the next
        // advance.
        let due = {
            let mut mark = self.journal_mark.lock().expect("journal mark poisoned");
            let due = mark.persisted < watermark
                && mark
                    .last_attempt
                    .is_none_or(|at| at.elapsed() >= self.settings.truncate_interval);
            if due {
                mark.last_attempt = Some(Instant::now());
            }
            due
        };
        if due && self.save_decisions().await && self.journal.truncate(watermark).await.is_ok() {
            self.note_journal_watermark(watermark);
        }
        if let Some(newest) = newest {
            self.note_compacted_past_absent(newest).await;
        }
    }

    /// Journals the decision records the replica has not yet journaled,
    /// durable before the truncate that deletes their commands. Returns
    /// whether every one is durable — the truncate waits otherwise, and
    /// they are retried with it.
    async fn save_decisions(&self) -> bool {
        let unsaved = self
            .replica
            .lock()
            .expect("replica poisoned")
            .unsaved_decisions();
        if unsaved.is_empty() {
            return true;
        }
        for record in &unsaved {
            if let Err(err) = self.journal.save_decision(record).await {
                tracing::warn!(
                    node = self.id.0,
                    txn = ?record.txn,
                    %err,
                    "cannot stage a decision record; holding the journal truncate back"
                );
                return false;
            }
        }
        if let Err(err) = self.journal.flush().await {
            tracing::warn!(
                node = self.id.0,
                %err,
                "cannot flush decision records; holding the journal truncate back"
            );
            return false;
        }
        let txns: Vec<TxnId> = unsaved.iter().map(|record| record.txn).collect();
        self.replica
            .lock()
            .expect("replica poisoned")
            .decisions_saved(&txns);
        true
    }

    /// Drops the decision records no route shard needs any more: for every
    /// route shard but this node's own (which compacted the transaction, so
    /// every replica counted there applied it), every current replica has
    /// reported a watermark above the transaction — each applied it, so no
    /// recovery there will ask for its decision. Reports come from the
    /// watermark gossip every node sends every other. A replica that never
    /// reports again (permanently down) keeps its shard's records until it
    /// leaves the topology.
    async fn prune_decisions(&self) {
        let dropped = {
            let my_shard = self.topology.node_shard(self.id);
            let watermarks = self
                .peer_watermarks
                .lock()
                .expect("watermarks poisoned")
                .clone();
            self.replica
                .lock()
                .expect("replica poisoned")
                .prune_decisions(|record| {
                    record
                        .route
                        .iter()
                        .filter(|key| Some(self.topology.shard_of(key)) != my_shard)
                        .flat_map(|key| self.topology.replicas(key))
                        .all(|node| watermarks.get(&node).is_some_and(|w| *w > record.txn.0))
                })
        };
        if dropped.is_empty() {
            return;
        }
        tracing::debug!(
            node = self.id.0,
            records = dropped.len(),
            "dropped decision records every route shard applied"
        );
        // A record the journal keeps is dropped again after a restart.
        if let Err(err) = self.journal.drop_decisions(&dropped).await {
            tracing::warn!(node = self.id.0, %err, "cannot drop decision records from the journal");
        }
    }

    /// Records that the journal has persisted `watermark` (max-merged: `join`
    /// and the sweep may both land one).
    fn note_journal_watermark(&self, watermark: Timestamp) {
        let mut mark = self.journal_mark.lock().expect("journal mark poisoned");
        if watermark > mark.persisted {
            mark.persisted = watermark;
        }
    }

    /// After a compaction that dropped commands up to `newest`: every absent
    /// peer whose last report is at or below it may lack one of them (a
    /// report vouches only for lower `t0`s), so it must now report above
    /// `newest` to be counted again. The bounds are persisted, so a restart of
    /// this node does not let such a peer back in unproven.
    async fn note_compacted_past_absent(&self, newest: Timestamp) {
        let bounds = {
            let mut absent = self.absent.lock().expect("absent poisoned");
            let mut changed = false;
            for (&peer, state) in absent.iter_mut() {
                if newest >= state.last_report && state.rejoin_above.is_none_or(|b| newest > b) {
                    if state.rejoin_above.is_none() {
                        tracing::warn!(
                            node = self.id.0,
                            peer = peer.0,
                            ?newest,
                            "compacted past an absent peer's last report; it must rejoin on return"
                        );
                    }
                    state.rejoin_above = Some(newest);
                    changed = true;
                }
            }
            if !changed {
                return;
            }
            Self::bounds_of(&absent)
        };
        self.save_rejoin_bounds(&bounds).await;
    }

    fn bounds_of(absent: &HashMap<NodeId, AbsentPeer>) -> Vec<(NodeId, Timestamp)> {
        let mut bounds: Vec<(NodeId, Timestamp)> = absent
            .iter()
            .filter_map(|(&peer, state)| state.rejoin_above.map(|bound| (peer, bound)))
            .collect();
        bounds.sort_unstable();
        bounds
    }

    /// Appends decided metadata-log entries, the gate in front of installing
    /// them. An epoch is never installed unless the log took it: installed, it
    /// is acted on, yet a restart would fall back to the epoch before — and if
    /// the epoch retired every member of that one, nobody this node asks for
    /// the entry is left. The commit or the metadata sweep retries it.
    async fn persist_metadata(&self, entries: &[(u64, Vec<Vec<NodeId>>)]) -> bool {
        let persisted = match entries {
            [] => return true,
            [(epoch, layout)] => self.journal.append_metadata(*epoch, layout).await,
            entries => self.journal.append_metadata_batch(entries).await,
        };
        match persisted {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    node = self.id.0,
                    epochs = entries.len(),
                    %err,
                    "cannot persist decided epochs; not installing them"
                );
                false
            }
        }
    }

    /// Persists this node's config-acceptor state for `epoch`, the gate in
    /// front of every promise or accept it sends. On failure the vote is
    /// withheld, like a batch whose flush failed: the in-memory state may be
    /// ahead of the journal, which only makes this acceptor refuse more, and
    /// every later vote persists the whole state before it goes out.
    async fn persist_acceptor(&self, epoch: u64, record: &AcceptorRecord) -> bool {
        match self.journal.record_acceptor(epoch, record).await {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    node = self.id.0,
                    epoch,
                    %err,
                    "cannot persist a config vote; withholding it"
                );
                false
            }
        }
    }

    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) {
        if let Err(err) = self.journal.save_rejoin_bounds(bounds).await {
            tracing::warn!(node = self.id.0, %err, "cannot persist the rejoin bounds");
        }
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
        // A node told to rejoin holds state its peers compacted past: it
        // recovers, repairs and gossips nothing until its host rejoins it.
        // Nor does a node still joining: it holds part of its state, and a
        // report it gossips would end its contact's hold on compaction.
        if self.rejoin_required() || !self.bootstrapped.load(Ordering::Acquire) {
            return;
        }
        let now = self.clock.now().micros;
        let micros_ago = |d: Duration| Timestamp {
            micros: now.saturating_sub(d.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        };
        let grace = micros_ago(self.settings.recovery_interval);
        let timeout = micros_ago(self.settings.recovery_timeout);
        self.redrive().await;

        let candidates: Vec<TxnId> = {
            let replica = self.replica.lock().expect("replica poisoned");
            // A command committed here but waiting on a dependency is past
            // consensus: recovering it cannot apply it, and on a replica that
            // fell behind there are thousands. What it waits on is what needs
            // progress — recovered below if held here, imported by
            // anti-entropy if missed.
            replica
                .stuck(grace)
                .into_iter()
                .filter(|&txn| !replica.is_blocked(txn))
                .collect()
        };
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
                && self.failure_detector.suspect(
                    coordinator,
                    self.settings.phi_threshold,
                    self.settings.acceptable_pause,
                );
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
                // A recovery can wait out `collect_timeout`; anti-entropy
                // (paced on its own interval) must not wait for them all, or a
                // replica that fell behind never gets back what it missed.
                self.anti_entropy().await;
                let _ = self.recover(txn).await;
            }
        }
        self.anti_entropy().await;
        self.advance_watermark().await;
        self.metadata_sweep().await;
    }

    /// Retries local execution the inbox loop left behind: an apply or a read
    /// the data store failed is only attempted again by the next drive, and a
    /// quiet group may receive no message to trigger one. Like
    /// [`handle`](Self::handle), the acknowledgements wait for the journal.
    /// (`next_apply`/`next_read` reserve what they hand out, so this cannot
    /// race the inbox loop into executing anything twice.)
    async fn redrive(&self) {
        let sends = self.drive_staged().await;
        if sends.is_empty() {
            return;
        }
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
            let next = self.metadata_turn.fetch_add(1, Ordering::Relaxed) as usize;
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

    /// Anti-entropy repair: pull any committed transactions a rotating peer of
    /// this node's shard has that this node missed (e.g. while it was briefly down). Recovery alone
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
        // Only a replica of this node's shard holds what it is missing; another
        // shard's node would build and ship a page this node then discards. A
        // node outside every shard owns nothing to repair.
        if self.topology.node_shard(self.id).is_none() {
            return;
        }
        let peers: Vec<NodeId> = self
            .shard_peers()
            .into_iter()
            .filter(|&n| n != self.id)
            .collect();
        if peers.is_empty() {
            return;
        }
        let next = self.anti_entropy_turn.fetch_add(1, Ordering::Relaxed) as usize;
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

    /// Records `peer`'s proven sync coverage after a successful sync round:
    /// every transaction `peer` had applied below `coverage` is now in this
    /// node's store. Max-merged; the map is reset when the topology epoch
    /// changes, since coverage accrued under an old layout says nothing about
    /// the keys this node owns under the new one.
    fn record_sync_coverage(&self, peer: NodeId, coverage: Timestamp) {
        let epoch = self.topology.epoch();
        let mut guard = self.sync_coverage.lock().expect("coverage poisoned");
        let (at, map) = &mut *guard;
        if *at != epoch {
            *at = epoch;
            map.clear();
        }
        let entry = map.entry(peer).or_insert(Timestamp::MIN);
        *entry = (*entry).max(coverage);
    }

    /// The newest point this node has *proven* it holds every committed
    /// transaction below, via sync coverage of enough shard peers that
    /// `{self} ∪ covered` intersects every commit quorum: with `slow_q − 1`
    /// covered peers the set is a majority, every fast quorum meets every
    /// majority (`fast_q ≥ f + 1`), and the intersecting witness either still
    /// had the transaction unapplied when it reported (keeping its coverage
    /// below the transaction's `t0`) or had applied it and shipped it in that
    /// round. `None` when no clamp applies (a single-node shard witnesses
    /// everything itself); [`Timestamp::MIN`] when too few peers are covered
    /// yet (fresh start, restart, or epoch change).
    fn quorum_sync_coverage(&self, shard_peers: &[NodeId]) -> Option<Timestamp> {
        let needed = crate::api::slow_quorum_size(shard_peers.len()).saturating_sub(1);
        if needed == 0 {
            return None;
        }
        let epoch = self.topology.epoch();
        let guard = self.sync_coverage.lock().expect("coverage poisoned");
        let (at, map) = &*guard;
        if *at != epoch {
            return Some(Timestamp::MIN);
        }
        let mut covered: Vec<Timestamp> = shard_peers
            .iter()
            .filter(|&&peer| peer != self.id)
            .filter_map(|peer| map.get(peer).copied())
            .collect();
        if covered.len() < needed {
            return Some(Timestamp::MIN);
        }
        covered.sort_unstable_by(|a, b| b.cmp(a));
        Some(covered[needed - 1])
    }

    /// [`quorum_sync_coverage`](Self::quorum_sync_coverage), raised to what
    /// the contact of this node's join vouched for under the current epoch
    /// ([`Message::JoinProof`]): what clamps this node's watermark report and
    /// its subscriptions' stable point.
    fn proven_coverage(&self, shard_peers: &[NodeId]) -> Option<Timestamp> {
        let coverage = self.quorum_sync_coverage(shard_peers)?;
        match *self.join_proof.lock().expect("join proof poisoned") {
            Some(proof) if proof.epoch == self.topology.epoch() => {
                Some(coverage.max(proof.vouched))
            }
            _ => Some(coverage),
        }
    }

    /// The sync coverage this node may hand a peer, which records it as proof
    /// this node holds everything it applied below it: `coverage`, capped at
    /// [`proven_coverage`](Self::proven_coverage) while a join's gap is open
    /// ([`JoinProven`]) — applied-through cannot vouch for a transaction the
    /// join never brought in. Uncapped otherwise, as for any replica.
    fn exportable_coverage(&self, coverage: Timestamp, shard_peers: &[NodeId]) -> Timestamp {
        let Some(proof) = *self.join_proof.lock().expect("join proof poisoned") else {
            return coverage;
        };
        if proof.epoch != self.topology.epoch() {
            return coverage;
        }
        match self.quorum_sync_coverage(shard_peers) {
            Some(quorum) if quorum < proof.until => coverage.min(quorum.max(proof.vouched)),
            _ => coverage,
        }
    }

    /// Gossips this node's redundancy point to its shard peers and compacts below
    /// the per-shard minimum once every peer has reported. The point is its
    /// applied-through, lagged by [`compaction_margin`](NodeConfig::compaction_margin)
    /// so a not-yet-propagated
    /// commit is never skipped, and clamped to this node's proven sync coverage
    /// ([`quorum_sync_coverage`](Self::quorum_sync_coverage)) — applied-through
    /// alone cannot vouch for transactions this node never witnessed (e.g.
    /// while partitioned away), so until anti-entropy has provably re-covered
    /// the window, the clamp holds cluster compaction back and keeps those
    /// transactions repairable. The per-shard min ensures every replica has
    /// applied everything below the compaction watermark (a still-behind or
    /// partitioned peer holds the min down, and an unheard-from peer blocks
    /// compaction entirely), so dropping that state is safe.
    async fn advance_watermark(&self) {
        // The disjoint replica set this node belongs to.
        let all_nodes = self.topology.nodes();
        let shard_peers = self.shard_peers();
        let mine = self.report(&shard_peers);
        self.give_up_on_silent_peers(&shard_peers);

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
            let mut absent = self.absent.lock().expect("absent poisoned");
            let mut watermarks = self.peer_watermarks.lock().expect("watermarks poisoned");
            // Evict reports from nodes no longer in the topology, or a removed
            // peer's last (stale) watermark would hold compaction back forever.
            watermarks.retain(|node, _| all_nodes.contains(node));
            absent.retain(|node, _| all_nodes.contains(node));
            watermarks.insert(self.id, mine);
            let mut wm = mine;
            let mut complete = true;
            for &peer in &shard_peers {
                if absent.contains_key(&peer) {
                    continue;
                }
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
        self.prune_decisions().await;
    }

    /// The nodes of this node's shard, itself included.
    fn shard_peers(&self) -> Vec<NodeId> {
        let my_shard = self.topology.node_shard(self.id);
        self.topology
            .nodes()
            .into_iter()
            .filter(|&n| self.topology.node_shard(n) == my_shard)
            .collect()
    }

    /// The redundancy point this node vouches for: its applied-through lagged
    /// by the compaction margin, clamped to its proven coverage, to the
    /// oldest transaction it is still coordinating (see
    /// [`advance_watermark`](Self::advance_watermark)) and to what it vouched
    /// for a joiner it is still serving ([`JoinHold`]).
    fn report(&self, shard_peers: &[NodeId]) -> Timestamp {
        let replica = self.replica.lock().expect("replica poisoned");
        self.report_locked(&replica, shard_peers)
    }

    /// [`report`](Self::report) over an already-locked replica, so a join
    /// header can vouch for exactly the state it exports. Takes the coverage,
    /// join-proof, coordinating and hold locks under the replica lock —
    /// none of them is ever held while the replica lock is taken.
    fn report_locked(&self, replica: &Replica, shard_peers: &[NodeId]) -> Timestamp {
        let local = replica.applied_through(self.margin_cutoff());

        let mine = match self.proven_coverage(shard_peers) {
            Some(coverage) if coverage < local => {
                self.metrics.record_watermark_clamp();
                tracing::debug!(
                    node = self.id.0,
                    ?local,
                    ?coverage,
                    "watermark report clamped by sync coverage"
                );
                coverage
            }
            _ => local,
        };
        // Never past a transaction this node is still coordinating: if its
        // coordination fails, the node recovers it to learn the outcome, and
        // that needs every replica to still hold it.
        let mine = match self
            .coordinating
            .lock()
            .expect("coordinating poisoned")
            .first()
        {
            Some(oldest) if oldest.0 < mine => oldest.0,
            _ => mine,
        };
        // Never past what this node vouched for a joiner still pulling its
        // snapshot (see `JoinHold`); a lapsed hold is dropped here.
        let lapse = self.settings.collect_timeout * JOIN_HOLD_PAGE_INTERVALS;
        let mut holds = self.join_holds.lock().expect("join holds poisoned");
        holds.retain(|_, hold| hold.refreshed.elapsed() < lapse);
        holds
            .values()
            .map(|hold| hold.vouched)
            .fold(mine, Timestamp::min)
    }

    /// Stops waiting for same-shard peers silent past
    /// [`NodeConfig::peer_absence_limit`]: their report leaves the compaction
    /// minimum, and what they last reported is kept to tell, when they return,
    /// whether anything they lack was compacted meanwhile. Never leaves fewer
    /// than a slow quorum counted (this node included), so a partitioned
    /// minority keeps waiting.
    fn give_up_on_silent_peers(&self, shard_peers: &[NodeId]) {
        let Some(limit) = self.settings.peer_absence_limit else {
            return;
        };
        let now = Instant::now();
        let quorum = crate::api::slow_quorum_size(shard_peers.len());
        let mut absent = self.absent.lock().expect("absent poisoned");
        let mut watermarks = self.peer_watermarks.lock().expect("watermarks poisoned");
        for &peer in shard_peers {
            if peer == self.id || absent.contains_key(&peer) {
                continue;
            }
            let heard = self
                .failure_detector
                .last_heard(peer)
                .unwrap_or(self.started_at);
            let silent = now.saturating_duration_since(heard);
            if silent < limit {
                continue;
            }
            let counted = shard_peers.len() - absent.len();
            if counted <= quorum {
                tracing::debug!(
                    node = self.id.0,
                    peer = peer.0,
                    "a silent peer is still waited for: giving it up would leave less than a quorum"
                );
                continue;
            }
            let last_report = watermarks.remove(&peer).unwrap_or(Timestamp::MIN);
            absent.insert(
                peer,
                AbsentPeer {
                    last_report,
                    rejoin_above: None,
                },
            );
            self.metrics.record_peer_absent();
            tracing::warn!(
                node = self.id.0,
                peer = peer.0,
                silent_secs = silent.as_secs(),
                "stopped waiting for a silent peer; compaction proceeds without it"
            );
        }
    }

    /// What to do with a message from `from` if this node stopped waiting for
    /// it. A watermark report above everything compacted past it (or any
    /// message, if nothing was) proves it lacks nothing and counts it again; a
    /// join request is served, since joining is how it catches up; anything
    /// else while it is behind is answered with [`Message::Rejoin`] instead
    /// of being handled — a stale replica must not take part (its recovery of
    /// a compacted transaction would find no one who knows it).
    fn screen_absent(&self, from: NodeId, message: &Message) -> Screen {
        let mut absent = self.absent.lock().expect("absent poisoned");
        let Some(state) = absent.get(&from) else {
            return Screen::Pass;
        };
        let behind = match message {
            // A join is how a peer behind its bound gets back: always served.
            Message::SyncRequest { snapshot: true, .. }
            | Message::JoinHeaderRequest { .. }
            | Message::CommandPageRequest { .. }
            | Message::SnapshotRequest { .. }
            | Message::SnapshotPageRequest { .. }
            | Message::JoinKeepalive => return Screen::Pass,
            Message::Watermark { applied_through } => match state.rejoin_above {
                Some(bound) if *applied_through <= bound => Some(bound),
                _ => None,
            },
            _ => state.rejoin_above,
        };
        match behind {
            Some(above) => Screen::Reply(Message::Rejoin { above }),
            None => {
                let persist = state.rejoin_above.is_some();
                absent.remove(&from);
                tracing::info!(
                    node = self.id.0,
                    peer = from.0,
                    "an absent peer is back and lacks nothing compacted; waiting for it again"
                );
                Screen::Reinstated { persist }
            }
        }
    }

    /// A peer stopped waiting for this node and compacted past what it holds.
    /// If this node can nevertheless prove it holds everything up to `above`
    /// (its own report is higher: a stale bound, or a report that crossed the
    /// peer's message), it answers with that report; otherwise it stops taking
    /// part and waits for its host to rejoin it.
    async fn on_rejoin(&self, from: NodeId, above: Timestamp) {
        if self.rejoin_required() {
            return;
        }
        let mine = self.report(&self.shard_peers());
        if mine > above {
            tracing::debug!(
                node = self.id.0,
                peer = from.0,
                ?above,
                ?mine,
                "told to rejoin but holding everything compacted; reporting instead"
            );
            self.send(
                from,
                Message::Watermark {
                    applied_through: mine,
                },
            )
            .await;
            return;
        }
        self.metrics.record_rejoin_required();
        tracing::error!(
            node = self.id.0,
            peer = from.0,
            ?above,
            ?mine,
            "behind the peers' compaction; this node must rejoin the group from a snapshot"
        );
        self.rejoin_required.send_replace(true);
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

        // Told to rejoin: this node's state is stale, so it takes no part until
        // its host rejoins it (see `NodeConfig::peer_absence_limit`). A
        // forwarded read is still refused out loud: unanswered, its requester
        // would only fail at `collect_timeout`.
        if self.rejoin_required() {
            if let Message::ReadForward { id, .. } = env.message {
                return vec![(
                    env.from,
                    Message::ReadFailed {
                        id,
                        reason: "the owner must rejoin".into(),
                    },
                )];
            }
            return Vec::new();
        }
        if let Message::Rejoin { above } = env.message {
            // A node still joining is rejoining already: a peer that has it
            // marked absent answers whatever it hears from it with this
            // until the join's first report counts it again, and taking that
            // for a verdict would abort the join it is the cure for.
            if self.bootstrapped.load(Ordering::Acquire) {
                self.on_rejoin(env.from, above).await;
            }
            return Vec::new();
        }
        match self.screen_absent(env.from, &env.message) {
            Screen::Pass => {}
            Screen::Reinstated { persist } => {
                if persist {
                    let bounds = Self::bounds_of(&self.absent.lock().expect("absent poisoned"));
                    self.save_rejoin_bounds(&bounds).await;
                }
            }
            Screen::Reply(reply) => return vec![(env.from, reply)],
        }

        // While bootstrapping, buffer consensus messages for replay after the
        // snapshot is imported. Cluster-management messages (bootstrap + epoch
        // changes) pass through so the node can still sync and learn its layout.
        let control = matches!(
            env.message,
            Message::SyncRequest { .. }
                | Message::SyncData { .. }
                | Message::SnapshotRequest { .. }
                | Message::SnapshotPageRequest { .. }
                | Message::SnapshotPage { .. }
                | Message::JoinProof { .. }
                | Message::JoinHeaderRequest { .. }
                | Message::CommandPageRequest { .. }
                | Message::CommandPage { .. }
                | Message::JoinKeepalive
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
                | Message::ReadFailed { .. }
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
        // A multi-shard transaction's messages arrive wrapped with its route:
        // handled as the inner message, the route kept with the command.
        let (message, route) = match env.message {
            Message::Routed { route, inner } => (*inner, Some(route)),
            message => (message, None),
        };
        if route.is_some()
            && !matches!(
                message,
                Message::PreAccept { .. }
                    | Message::Accept { .. }
                    | Message::Commit { .. }
                    | Message::BeginRecovery { .. }
                    | Message::BeginRecoveryOk { .. }
            )
        {
            tracing::warn!(
                node = self.id.0,
                from = from.0,
                "dropping a malformed routed message"
            );
            return out;
        }
        match message {
            Message::PreAccept { txn, keys, events } => {
                // Only handle the keys/events this node owns.
                let keys = self.owned_keys(keys);
                let events = self.owned_events(events);
                let vote = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .preaccept(txn, keys, events);
                match vote {
                    Ok((execute_at, deps)) => {
                        // A vote the journal did not take is not sent.
                        let route_kept = self.keep_route(txn, route.as_deref()).await;
                        if !(self.journal_stage(txn).await && route_kept) {
                            return out;
                        }
                        out.push((
                            from,
                            Message::PreAcceptOk {
                                txn,
                                execute_at,
                                deps,
                            },
                        ));
                    }
                    // A recovery took the transaction over here.
                    Err(Refusal::Promised(promised)) => {
                        out.push((from, Message::Nack { txn, promised }))
                    }
                    Err(Refusal::Compacted) => {}
                }
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
                if reply == Err(Refusal::Compacted) {
                    return out;
                }
                let route_kept = reply.is_err() || self.keep_route(txn, route.as_deref()).await;
                if !(self.journal_stage(txn).await && route_kept) {
                    return out;
                }
                match reply {
                    Ok(deps) => out.push((from, Message::AcceptOk { txn, ballot, deps })),
                    Err(Refusal::Promised(promised)) => {
                        out.push((from, Message::Nack { txn, promised }))
                    }
                    Err(Refusal::Compacted) => {}
                }
            }
            Message::Commit {
                txn,
                execute_at,
                deps,
                keys,
                events,
                reply_to,
            } => {
                self.clock.witness(execute_at);
                // The coordinator ships this shard's keys explicitly, so a
                // replica that missed PreAccept still indexes this transaction
                // in its conflict graph. The owned filters stay as defense
                // against a stale-epoch coordinator's view of ownership.
                let keys = self.owned_keys(keys);
                let events = self.owned_events(events);
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .commit(txn, execute_at, deps, events, keys, reply_to);
                let route_kept = self.keep_route(txn, route.as_deref()).await;
                // A commit the journal did not take is not answered for; what
                // it unblocks still executes (each apply gates its own ack).
                let durable = self.journal_stage(txn).await && route_kept;

                // If already applied (e.g. a second recovery), short-circuit the
                // read phase with the stored outcome. If already read — the slot
                // is held, awaiting a decision — answer with the recorded read:
                // the replica never reads twice, so a recovery coordinator that
                // took over from one that never decided would otherwise wait
                // forever for a read nobody repeats.
                let (applied, read) = {
                    let replica = self.replica.lock().expect("replica poisoned");
                    (replica.applied_result(txn), replica.read_result(txn))
                };
                match (applied, read) {
                    (Some(conflict), _) => {
                        if durable {
                            out.push((reply_to, Message::ReadOk { txn, ok: !conflict }));
                        }
                    }
                    (None, Some(ok)) => {
                        if durable {
                            out.push((reply_to, Message::ReadOk { txn, ok }));
                        }
                        out.extend(self.drive_staged().await);
                    }
                    (None, None) => out.extend(self.drive_staged().await),
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
                    // Compacted away with a decision record: no coordinator
                    // is known, so the sender is told.
                    let reply_to = self
                        .replica
                        .lock()
                        .expect("replica poisoned")
                        .reply_to(txn)
                        .unwrap_or(from);
                    out.push((reply_to, Message::Applied { txn }));
                } else {
                    out.extend(self.drive_staged().await);
                }
            }
            // The legacy recovery request (no keys/events): answered as before,
            // but a replica that never witnessed the transaction still
            // remembers the promise.
            Message::Recover { txn, ballot } => {
                let reply = self.replica.lock().expect("replica poisoned").recover(
                    txn,
                    ballot,
                    Vec::new(),
                    Vec::new(),
                );
                // The promise must be durable before the reply that reveals it
                // (a restarted replica must not accept a lower ballot again).
                if !self.journal_stage(txn).await {
                    return out;
                }
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
            Message::BeginRecovery {
                txn,
                ballot,
                keys,
                events,
            } => {
                let keys = self.owned_keys(keys);
                let events = self.owned_events(events);
                let reply = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    match &route {
                        // Routed: never pre-accept what this replica never
                        // witnessed (see `Replica::recover_routed`).
                        Some(_) => replica.recover_routed(txn, ballot, keys, events),
                        None => replica.recover_unrouted(txn, ballot, keys, events),
                    }
                };
                let route_kept = !matches!(reply, Ok(RoutedRecovery::Known(_)))
                    || self.keep_route(txn, route.as_deref()).await;
                // Durable before the reply, like any promise or vote — the
                // promise of a transaction never witnessed included.
                if !(self.journal_stage(txn).await && route_kept) {
                    return out;
                }
                let message = match reply {
                    Ok(RoutedRecovery::Known(state)) => {
                        let report = Message::BeginRecoveryOk {
                            txn,
                            ballot,
                            known: state.known,
                            status: state.status,
                            accepted: state.accepted,
                            execute_at: state.execute_at,
                            deps: state.deps,
                            superseding_rejects: state.superseding_rejects,
                            wait: state.wait,
                            keys: state.keys,
                            events: state.events,
                        };
                        // A replica holding a multi-shard transaction always
                        // answers with its route, so a recovery coordinator
                        // that lacked it learns every shard to hear from.
                        let held = self.replica.lock().expect("replica poisoned").route(txn);
                        match held.or(route) {
                            Some(route) => Message::Routed {
                                route,
                                inner: Box::new(report),
                            },
                            None => report,
                        }
                    }
                    Ok(RoutedRecovery::NotWitnessed { redundant }) => Message::NotWitnessed {
                        txn,
                        ballot,
                        redundant,
                    },
                    Err(promised) => Message::Nack { txn, promised },
                };
                out.push((from, message));
            }
            Message::Invalidate { txn } => {
                let invalidated = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .invalidate(txn);
                if invalidated {
                    tracing::debug!(node = self.id.0, ?txn, "invalidated a transaction");
                    self.applied_gen.send_modify(|v| *v += 1);
                    // No reply rides on it; the next stage of `txn` retries.
                    self.journal_stage(txn).await;
                    // Its dependents may be free to execute now.
                    out.extend(self.drive_staged().await);
                }
            }
            // Bootstrap / anti-entropy: a node asks for our committed state.
            Message::SyncRequest {
                id,
                snapshot,
                known,
            } => {
                // A node still joining holds only part of the state: serving
                // its snapshot would bootstrap the requester into the same
                // gap. Staying silent sends the joiner to its next contact.
                // Nor does it answer anti-entropy: with nothing un-applied
                // yet, its coverage would be its whole margin-lagged clock,
                // vouching for transactions it never witnessed.
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                let (watermark, coverage, commands) = self.export_bootstrap(known.as_ref());
                // A bootstrapping joiner on the legacy protocol (before
                // `SnapshotRequest`) may be below our truncation watermark, so
                // it also needs the materialised state command replay no
                // longer covers — inline, in one frame (a current joiner pulls
                // it page by page instead). Anti-entropy (snapshot = false)
                // only needs the commands, already bounded. A snapshot this
                // node cannot read, or one that cannot fit a frame, is not
                // answered at all: an empty one would have the joiner adopt
                // our floor over a missing prefix.
                let snapshot = if snapshot {
                    let budget = crate::tcp::MAX_FRAME_LENGTH
                        .saturating_sub(commands.iter().map(command_size).sum());
                    match self.inline_snapshot(budget).await {
                        Ok(Some(snapshot)) => snapshot,
                        Ok(None) => {
                            tracing::warn!(
                                node = self.id.0,
                                from = from.0,
                                cap = crate::tcp::MAX_FRAME_LENGTH,
                                "a legacy joiner's inline snapshot would not fit a frame; \
                                 not answering (upgrade the joiner: it pulls pages)"
                            );
                            return out;
                        }
                        Err(err) => {
                            tracing::warn!(
                                node = self.id.0,
                                from = from.0,
                                %err,
                                "cannot read the bootstrap snapshot; not answering"
                            );
                            return out;
                        }
                    }
                } else {
                    Vec::new()
                };
                out.push((
                    from,
                    Message::SyncData {
                        id,
                        watermark,
                        coverage,
                        snapshot,
                        commands,
                    },
                ));
            }
            // The join header. A `SnapshotRequest` (a joiner predating
            // command pages) gets every applied command inline; a
            // `JoinHeaderRequest` gets none — it pulls them page by page
            // next, so a backlog of any size fits the frame cap. Either way
            // the snapshot follows page by page. A node still joining stays
            // silent (see above).
            request @ (Message::SnapshotRequest { .. } | Message::JoinHeaderRequest { .. }) => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                let (id, inline) = match request {
                    Message::SnapshotRequest { id } => (id, true),
                    Message::JoinHeaderRequest { id } => (id, false),
                    _ => unreachable!(),
                };
                let (watermark, coverage, commands, epoch, vouched) =
                    self.export_join_header(inline);
                // Hold this node's report at the vouch until the joiner's
                // first report (see `JoinHold`). Only a same-shard joiner's
                // report is ever compared against this shard's compaction.
                if vouched > Timestamp::MIN
                    && self.topology.node_shard(from) == self.topology.node_shard(self.id)
                {
                    self.join_holds.lock().expect("join holds poisoned").insert(
                        from,
                        JoinHold {
                            vouched,
                            refreshed: Instant::now(),
                        },
                    );
                }
                out.push((
                    from,
                    Message::SyncData {
                        id,
                        watermark,
                        coverage,
                        snapshot: Vec::new(),
                        commands,
                    },
                ));
                out.push((from, Message::JoinProof { id, epoch, vouched }));
            }
            // A joiner whose install spaces out its page requests: keep the
            // hold on this node's report alive.
            Message::JoinKeepalive => self.refresh_join_hold(from),
            Message::CommandPageRequest { id, after } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                self.refresh_join_hold(from);
                let (watermark, commands, next) = {
                    let replica = self.replica.lock().expect("replica poisoned");
                    let (commands, next) = replica.export_applied_page(
                        after,
                        self.settings.snapshot_page_events,
                        self.settings.snapshot_page_bytes,
                    );
                    (replica.redundant_before(), commands, next)
                };
                self.metrics.record_command_page();
                out.push((
                    from,
                    Message::CommandPage {
                        id,
                        watermark,
                        commands,
                        next,
                    },
                ));
            }
            Message::SnapshotPageRequest { id, after } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                self.refresh_join_hold(from);
                match self
                    .datastore
                    .snapshot_page(
                        after,
                        self.settings.snapshot_page_events,
                        self.settings.snapshot_page_bytes,
                    )
                    .await
                {
                    Ok(page) => {
                        self.metrics.record_snapshot_page();
                        out.push((
                            from,
                            Message::SnapshotPage {
                                id,
                                events: page.events,
                                next: page.next,
                            },
                        ));
                    }
                    // Not answered: the joiner asks again, then gives this
                    // contact up — never installs a page with a hole.
                    Err(err) => tracing::warn!(
                        node = self.id.0,
                        from = from.0,
                        %err,
                        "cannot read a snapshot page; not answering"
                    ),
                }
            }
            page @ (Message::SnapshotPage { .. } | Message::CommandPage { .. }) => {
                let id = match &page {
                    Message::SnapshotPage { id, .. } | Message::CommandPage { id, .. } => *id,
                    _ => unreachable!(),
                };
                let tx = self
                    .snapshot_pending
                    .lock()
                    .expect("snapshot poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(page);
                }
            }
            Message::SyncData {
                id,
                watermark,
                coverage,
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
                    let _ = tx.send((watermark, coverage, snapshot, commands));
                }
            }
            Message::JoinProof { id, epoch, vouched } => {
                if let Some(slot) = self
                    .proof_pending
                    .lock()
                    .expect("proof poisoned")
                    .get_mut(&id)
                {
                    *slot = Some((epoch, vouched));
                }
            }
            Message::Watermark { applied_through } => {
                // A joiner's first report ends the hold its contact kept for
                // it (see `JoinHold`): from here on it is counted itself.
                self.join_holds
                    .lock()
                    .expect("join holds poisoned")
                    .remove(&from);
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
                // ballot. Persist inline (before the deferred reply is sent), and
                // say nothing if that fails.
                if let Some(record) = record {
                    if !self.persist_acceptor(epoch, &record).await {
                        return out;
                    }
                }
                out.push((from, reply));
            }
            Message::ConfigAccept {
                epoch,
                ballot,
                layout,
            } => {
                // Never vote for a layout that would break the cluster, so a
                // majority of current nodes can never choose one (an older
                // proposer validates nothing).
                if let Err(err) = validate_layout(&layout) {
                    tracing::warn!(node = self.id.0, epoch, %err, "refusing an invalid layout");
                    return out;
                }
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
                    if !self.persist_acceptor(epoch, &record).await {
                        return out;
                    }
                }
                out.push((from, reply));
            }
            Message::ConfigCommit { epoch, layout } => {
                // Checked before the append: the log keeps the first entry per
                // epoch, so a persisted invalid one would shut out the real one.
                if let Err(err) = validate_layout(&layout) {
                    tracing::warn!(node = self.id.0, epoch, %err, "ignoring an invalid layout");
                    return out;
                }
                // Append to the durable log, then install every now-contiguous
                // epoch. If this commit arrived before an intervening epoch, ask the
                // sender (which decided it, so holds the full prefix) to fill the gap.
                // An epoch the log did not take is not installed (see
                // `persist_metadata`); the metadata sweep fetches it again.
                if !self.persist_metadata(&[(epoch, layout.clone())]).await {
                    return out;
                }
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
                // An invalid layout is dropped before the append, like a
                // `ConfigCommit`'s; the run stops there, so what is ingested
                // stays contiguous.
                let mut entries = entries;
                if let Some(bad) = entries
                    .iter()
                    .position(|(_, layout)| validate_layout(layout).is_err())
                {
                    tracing::warn!(
                        node = self.id.0,
                        epoch = entries[bad].0,
                        "ignoring an invalid layout and the entries after it"
                    );
                    entries.truncate(bad);
                }
                // Persist the whole run first (one group fsync for journals that
                // support it), then ingest in ascending order; `apply_log` installs
                // each now-contiguous epoch. Idempotent, so an overlapping pull is
                // harmless — and a failed one is simply pulled again.
                if !self.persist_metadata(&entries).await {
                    return out;
                }
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
                // Every proposal for the epoch hears it; each keeps only the
                // replies answering its own ballot.
                let rounds: Vec<ConfigSender> = self
                    .config_inbox
                    .lock()
                    .expect("config poisoned")
                    .get(&epoch)
                    .map(|rounds| rounds.values().cloned().collect())
                    .unwrap_or_default();
                for tx in rounds {
                    let _ = tx.send((from, response.clone()));
                }
            }
            // A forwarded read: serve it from the local backend and reply —
            // with the failure if the store refused it: an empty page would
            // read as "no such events".
            Message::ReadForward {
                id,
                aggregators,
                routing_key,
                args,
                to_micros,
            } => {
                // A node still joining holds part of its store: whatever it
                // answered could miss events committed long ago.
                if !self.bootstrapped.load(Ordering::Acquire) {
                    out.push((
                        from,
                        Message::ReadFailed {
                            id,
                            reason: "the owner is bootstrapping".into(),
                        },
                    ));
                    return out;
                }
                let reply = match self
                    .datastore
                    .read(aggregators.map(Arc::from), routing_key, args, to_micros)
                    .await
                {
                    Ok(result) => {
                        let page_info = result.page_info;
                        let mut cursors = Vec::with_capacity(result.edges.len());
                        let mut events = Vec::with_capacity(result.edges.len());
                        for edge in result.edges {
                            cursors.push(edge.cursor);
                            events.push(edge.node);
                        }
                        Message::ReadReply {
                            id,
                            cursors,
                            events,
                            page_info,
                        }
                    }
                    Err(err) => {
                        tracing::warn!(node = self.id.0, %err, "cannot serve a forwarded read");
                        Message::ReadFailed {
                            id,
                            reason: read_failure(&err),
                        }
                    }
                };
                out.push((from, reply));
            }
            // Read-index probe: a pure query of the conflict graph — report the
            // deps a read at `txn` would witness over `key`, storing nothing.
            // A node still joining stays silent: its conflict graph lacks the
            // commands the join is importing, so its deps could miss a
            // transaction, and the barrier counts only the replicas that
            // answer.
            Message::ReadProbe { txn, key } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
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
            reply @ (Message::ReadReply { .. } | Message::ReadFailed { .. }) => {
                let id = match &reply {
                    Message::ReadReply { id, .. } | Message::ReadFailed { id, .. } => *id,
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
            // Responses: hand to the waiting coordinator, tagged with the sender
            // (still wrapped, so it learns the route a report carries).
            response => {
                let response = match route {
                    Some(route) => Message::Routed {
                        route,
                        inner: Box::new(response),
                    },
                    None => response,
                };
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
            if let Some(mut apply) = apply {
                // The events carry their writer's provisional stamps; the
                // store gets the agreed ones (see `stamp_events`). The journal
                // keeps the provisional ones — every apply re-derives the same.
                stamp_events(apply.execute_at, &mut apply.events);
                if let Err(err) = self
                    .datastore
                    .apply(apply.txn, apply.execute_at, apply.events, apply.commit)
                    .await
                {
                    // Never mark (or acknowledge) what the store did not take:
                    // `Applied` is the promise that the effect is visible, and
                    // anti-entropy never ships an applied command again. Hand
                    // the transaction back and stop; the next drive (a message,
                    // or the sweep) retries it.
                    self.replica
                        .lock()
                        .expect("replica poisoned")
                        .release_apply(apply.txn);
                    self.metrics.record_execution_failure();
                    tracing::warn!(
                        node = self.id.0,
                        txn = ?apply.txn,
                        %err,
                        "cannot apply a decided transaction; retrying"
                    );
                    break;
                }
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .mark_applied(apply.txn, !apply.commit);
                // Wake read barriers / read-your-writes waiters instead of
                // letting them poll the replica lock.
                self.applied_gen.send_modify(|v| *v += 1);
                // An apply the journal did not take is not acknowledged; a
                // restart re-applies it (idempotently) from its commit.
                if self.journal_stage(apply.txn).await {
                    out.push((apply.reply_to, Message::Applied { txn: apply.txn }));
                }
                continue;
            }

            // Otherwise read the next ready, undecided transaction.
            let read = self.replica.lock().expect("replica poisoned").next_read();
            let Some(read) = read else { break };
            let ok = match self.read_condition(&read.events).await {
                Ok(ok) => ok,
                Err(err) => {
                    // A read that failed says nothing about the condition:
                    // answer nothing, and read it again on the next drive.
                    self.replica
                        .lock()
                        .expect("replica poisoned")
                        .unread(read.txn);
                    self.metrics.record_execution_failure();
                    tracing::warn!(
                        node = self.id.0,
                        txn = ?read.txn,
                        %err,
                        "cannot read a transaction's version condition; retrying"
                    );
                    break;
                }
            };
            // Record the result, and answer whoever drives the commit *now*: a
            // recovery's Commit may have taken the transaction over while the
            // read was in flight.
            let reply_to = {
                let mut replica = self.replica.lock().expect("replica poisoned");
                replica.record_read(read.txn, ok);
                replica.reply_to(read.txn).unwrap_or(read.reply_to)
            };
            out.push((reply_to, Message::ReadOk { txn: read.txn, ok }));
        }
        out
    }

    /// Whether every owned event extends its aggregate (version strictly greater
    /// than what is stored) — this node's part of the atomic commit condition.
    /// An error is not a vote either way.
    async fn read_condition(&self, owned_events: &[Event]) -> anyhow::Result<bool> {
        for event in owned_events {
            let current = self
                .datastore
                .version(&event.aggregate_type, &event.aggregate_id)
                .await?;
            if event.version <= current {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Stages the current durable state of `txn` to the journal — durable once the
    /// caller (the inbox batch loop, or the [`handle`](Self::handle) wrapper)
    /// flushes.
    ///
    /// Returns whether the stage succeeded (or there was nothing to stage). A
    /// reply that reveals the staged state is withheld when it did not, as the
    /// inbox loop withholds a batch whose flush failed: the in-memory state
    /// may be ahead of the journal, and the next stage of `txn` writes it whole.
    async fn journal_stage(&self, txn: TxnId) -> bool {
        let (snapshot, promise) = {
            let replica = self.replica.lock().expect("replica poisoned");
            (replica.snapshot(txn), replica.unknown_promise(txn))
        };
        let staged = if let Some(state) = snapshot {
            self.journal.stage(&state).await
        } else if let Some(ballot) = promise {
            // Never witnessed, but promised to a recovery: the promise is what
            // fences the original coordinator here, across a restart too.
            self.journal.save_promise(txn, ballot).await
        } else {
            return true;
        };
        match staged {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    node = self.id.0,
                    ?txn,
                    %err,
                    "cannot stage a transaction's state; withholding its reply"
                );
                false
            }
        }
    }

    /// Keeps `route` (when the message came routed) as `txn`'s route, staging
    /// it to the journal the first time — durable with the vote it travels
    /// with, so a restarted replica still recovers the transaction on every
    /// shard it touches.
    ///
    /// Returns whether the route is staged (or needed no staging). On failure
    /// the route is forgotten again, so the next routed message for `txn`
    /// retries it, and the vote it travels with is withheld.
    async fn keep_route(&self, txn: TxnId, route: Option<&[Key]>) -> bool {
        let Some(route) = route else { return true };
        let kept = self
            .replica
            .lock()
            .expect("replica poisoned")
            .set_route(txn, route);
        if !kept {
            return true;
        }
        match self.journal.save_route(txn, route).await {
            Ok(()) => true,
            Err(err) => {
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .forget_route(txn);
                tracing::warn!(
                    node = self.id.0,
                    ?txn,
                    %err,
                    "cannot stage a transaction's route; withholding its reply"
                );
                false
            }
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
        if self.rejoin_required() {
            return Err(RejoinRequired.into());
        }
        // A command is the unit no page can split: one over the cap could
        // never be repaired onto a replica that missed it.
        let bytes: usize = events.iter().map(snapshot_event_size).sum();
        if bytes > MAX_COMMAND_BYTES {
            tracing::warn!(
                node = self.id.0,
                bytes,
                cap = MAX_COMMAND_BYTES,
                "refusing new write: too large to replicate"
            );
            return Err(CommandTooLarge {
                bytes,
                cap: MAX_COMMAND_BYTES,
            }
            .into());
        }
        let backlog = self.command_count();
        if backlog >= self.settings.max_commands {
            tracing::warn!(
                node = self.id.0,
                backlog,
                cap = self.settings.max_commands,
                "refusing new write: consensus backlog at cap (compaction stalled?)"
            );
            return Err(BacklogAtCap {
                backlog,
                cap: self.settings.max_commands,
            }
            .into());
        }

        let keys = Self::keys_of(&events);
        let txn = TxnId(self.mint().await?);
        self.coordinating
            .lock()
            .expect("coordinating poisoned")
            .insert(txn);
        // Released however this ends — returned, or dropped mid-flight by a
        // caller that gave up (a hung-up client, a timeout): left behind, the
        // entry would pin this node's watermark report, and the response
        // channel would hide the transaction from its own recovery sweep.
        let _coordinating = Coordinating { node: self, txn };
        let (outcome, all_fast) = match self.coordinate(txn, &keys, &events).await {
            Ok(done) => Ok(done),
            Err(err) => self
                .settle(txn, err, &keys, &events)
                .await
                .map(|outcome| (outcome, false)),
        }?;
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
        let t0 = self.mint().await?;
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

        tracing::debug!(
            node = self.id.0,
            ?key,
            probes = slow_q,
            deps = deps.len(),
            "read barrier"
        );

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
        txn: TxnId,
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<(CommitOutcome, bool)> {
        let (plans, _union) = self.plan(keys, events);
        let route = Self::route_of(&plans, keys);
        let mut rx = self.register(txn);
        let (execute_at, shard_deps, all_fast) =
            self.agree(&mut rx, &plans, route.as_deref(), txn).await?;
        let conflict = self
            .execute(
                &mut rx,
                &plans,
                route.as_deref(),
                txn,
                execute_at,
                shard_deps,
                &[],
            )
            .await?;
        self.deregister(txn);
        Ok((CommitOutcome { txn, conflict }, all_fast))
    }

    /// PreAccept → (Accept): agrees `txn`'s execution timestamp and per-shard
    /// dependencies, and reports whether the fast path decided them.
    async fn agree(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        txn: TxnId,
    ) -> anyhow::Result<(Timestamp, HashMap<ShardId, BTreeSet<TxnId>>, bool)> {
        let t0 = txn.0;

        // PreAccept: a fast quorum per shard, or whatever arrives before the
        // fast-path timeout. Each shard's replicas receive only that shard's
        // keys/events — one encode per shard, no cross-shard payload.
        for plan in plans {
            self.broadcast(
                &plan.replicas,
                Self::routed(
                    route,
                    Message::PreAccept {
                        txn,
                        keys: plan.keys.clone(),
                        events: plan.events.clone(),
                    },
                ),
            )
            .await;
        }
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
                rx,
                plans,
                |p, got| electorate_responders(p, got) >= p.fast_q,
                Self::after(self.settings.fast_timeout),
                HashSet::new(),
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
            // A voter of the first round is not counted again: a duplicated
            // reply would otherwise complete the quorum with fewer distinct
            // replicas than it takes.
            let voted: HashSet<NodeId> = pre.values().flatten().map(|(from, _)| *from).collect();
            let more = self
                .collect_by_shard_tagged(
                    rx,
                    plans,
                    |p, got| got.len() >= remaining[&p.shard],
                    Self::after(self.settings.collect_timeout),
                    voted,
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
        for plan in plans {
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
            for plan in plans {
                let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                self.broadcast(
                    &plan.replicas,
                    Self::routed(
                        route,
                        Message::Accept {
                            txn,
                            ballot: Ballot(t0),
                            execute_at,
                            deps,
                            keys: plan.keys.clone(),
                        },
                    ),
                )
                .await;
            }
            let acks = self
                .collect_by_shard(
                    rx,
                    plans,
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
            for plan in plans {
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

        Ok((execute_at, shard_deps, all_fast))
    }

    /// Commit → Read → Apply: drive the atomic decision across all touched
    /// shards (each plan carries its shard's keys/events slice) and return
    /// whether the write was aborted.
    ///
    /// `applied` names replicas a recovery saw holding the transaction
    /// applied: each answers the Commit with the decision itself, and a
    /// shard's read is waited for from one of them. Their decision is final,
    /// and it may rest on a shard no longer planned (applied and compacted
    /// there): a fresh read of the planned shards alone could contradict it.
    /// An applied replica outside every plan therefore decides for its shard:
    /// it is sent the Commit, and its answer is waited for and counted like a
    /// read, but its shard needs no read quorum and is sent no Apply.
    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        txn: TxnId,
        execute_at: Timestamp,
        shard_deps: HashMap<ShardId, BTreeSet<TxnId>>,
        applied: &[NodeId],
    ) -> anyhow::Result<bool> {
        let union = Self::union_of(plans);
        // A shard skipped as compacted: its applied replicas heard from stand
        // in for its read, one answer enough. They hold the transaction
        // applied, so the Commit needs none of its keys, events or deps.
        let mut skipped: Vec<ShardPlan> = Vec::new();
        for n in applied
            .iter()
            .filter(|n| !plans.iter().any(|p| p.replicas.contains(n)))
        {
            let Some(shard) = self.topology.node_shard(*n) else {
                continue;
            };
            match skipped.iter_mut().find(|p| p.shard == shard) {
                Some(p) => p.replicas.push(*n),
                None => skipped.push(ShardPlan {
                    shard,
                    replicas: vec![*n],
                    electorate: vec![*n],
                    fast_q: 1,
                    slow_q: 1,
                    keys: Vec::new(),
                    events: Vec::new(),
                }),
            }
        }
        let read_plans: Vec<ShardPlan> = plans.iter().chain(&skipped).cloned().collect();
        self.broadcast_commit(&read_plans, route, txn, execute_at, &shard_deps)
            .await;

        // Read: a slow quorum per shard reports its version condition —
        // including one of the shard's applied replicas, if it has any.
        let heard_applied = |p: &ShardPlan, got: &[(NodeId, bool)]| {
            let mut here = applied.iter().filter(|n| p.replicas.contains(n)).peekable();
            here.peek().is_none() || here.any(|n| got.iter().any(|(from, _)| from == n))
        };
        let reads = self
            .collect_by_shard_tagged(
                rx,
                &read_plans,
                |p, got| got.len() >= p.slow_q && heard_applied(p, got),
                Self::after(self.settings.collect_timeout),
                HashSet::new(),
                |m| match m {
                    Message::ReadOk { ok, .. } => Some(ok),
                    _ => None,
                },
            )
            .await;
        for plan in &read_plans {
            let got = &reads[&plan.shard];
            if got.len() < plan.slow_q || !heard_applied(plan, got) {
                anyhow::bail!("read quorum not reached for shard {}", plan.shard);
            }
        }
        // Atomic commit only if every shard's condition holds.
        let commit = reads.values().flatten().all(|(_, ok)| *ok);

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

    /// Commits `txn` on each shard with its own dependency set and its own
    /// key/event slice — one encode per shard instead of one per replica.
    async fn broadcast_commit(
        &self,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        txn: TxnId,
        execute_at: Timestamp,
        shard_deps: &HashMap<ShardId, BTreeSet<TxnId>>,
    ) {
        for plan in plans {
            let deps: Vec<TxnId> = shard_deps
                .get(&plan.shard)
                .into_iter()
                .flatten()
                .copied()
                .filter(|d| *d != txn)
                .collect();
            self.broadcast(
                &plan.replicas,
                Self::routed(
                    route,
                    Message::Commit {
                        txn,
                        execute_at,
                        deps,
                        keys: plan.keys.clone(),
                        events: plan.events.clone(),
                        reply_to: self.id,
                    },
                ),
            )
            .await;
        }
    }

    /// Learns how a write this node failed to coordinate ended. Once its
    /// PreAccept went out, a failed coordination does not mean a failed
    /// write: another node may have taken the transaction over by recovery
    /// (it suspected this one, or saw the transaction late and took it for
    /// stalled) and committed it. Reporting `failure` then would tell the
    /// caller a committed write failed, and a retry would apply it twice.
    ///
    /// So the coordinator recovers its own transaction: that returns the
    /// outcome another recovery decided, or drives the transaction to one.
    /// When recovery cannot reach a quorum either, the transaction may still
    /// be in another node's hands, so this node waits for it to apply locally
    /// (every replica applies what any coordinator commits). Only when neither
    /// settles it is `failure` returned. The transaction cannot have been
    /// compacted meanwhile: it is in `coordinating` until this returns.
    async fn settle(
        &self,
        txn: TxnId,
        failure: anyhow::Error,
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<CommitOutcome> {
        self.deregister(txn);
        if let Some(conflict) = self.local_outcome(txn) {
            return Ok(CommitOutcome { txn, conflict });
        }
        match self.recover_with(txn, keys.to_vec(), events.to_vec()).await {
            Ok(outcome) => {
                tracing::debug!(node = self.id.0, ?txn, error = %failure, "write settled by recovery");
                return Ok(outcome);
            }
            Err(err) => {
                tracing::debug!(node = self.id.0, ?txn, %err, "cannot recover a failed write");
            }
        }
        let deadline = Self::after(self.settings.collect_timeout);
        let mut applied_rx = self.applied_gen.subscribe();
        loop {
            if let Some(conflict) = self.local_outcome(txn) {
                tracing::debug!(node = self.id.0, ?txn, error = %failure, "write settled by another node");
                return Ok(CommitOutcome { txn, conflict });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(failure);
            }
            if let Ok(Err(_)) = tokio::time::timeout(deadline - now, applied_rx.changed()).await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    /// The outcome of `txn` if this node has applied it: `Some(conflict)`.
    fn local_outcome(&self, txn: TxnId) -> Option<bool> {
        self.replica
            .lock()
            .expect("replica poisoned")
            .applied_result(txn)
    }

    /// Recovers `txn`: takes it over under a fresh ballot and drives it to an
    /// applied outcome consistent with whatever its failed coordinator decided.
    pub async fn recover(&self, txn: TxnId) -> anyhow::Result<CommitOutcome> {
        // What this node holds of the transaction (its owned part) is what it
        // can hand to replicas that never witnessed it.
        let (keys, events) = self
            .replica
            .lock()
            .expect("replica poisoned")
            .snapshot(txn)
            .map(|cmd| (cmd.keys, cmd.events))
            .unwrap_or_default();
        self.recover_with(txn, keys, events).await
    }

    /// The recovery reports a message carries, if it answers `ballot`.
    fn recover_report(message: Message, ballot: Ballot) -> Option<RecoverResp> {
        match message {
            // A routed report carries the transaction's route.
            Message::Routed { route, inner } => match Self::recover_report(*inner, ballot)? {
                RecoverResp::Ok(mut fields) => {
                    fields.route = Some(route);
                    Some(RecoverResp::Ok(fields))
                }
                other => Some(other),
            },
            // Only reports answering THIS attempt's ballot count — a stale
            // reply from an earlier ballot must not fill the quorum (that could
            // rest the decision on a sub-quorum sample and miss an existing
            // Commit/Accept).
            Message::BeginRecoveryOk {
                ballot: b,
                known,
                status,
                accepted,
                execute_at,
                deps,
                superseding_rejects,
                wait,
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
                wait,
                keys,
                events,
                route: None,
            })),
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
                wait: false,
                keys,
                events,
                route: None,
            })),
            Message::NotWitnessed {
                ballot: b,
                redundant,
                ..
            } if b == ballot => Some(RecoverResp::NotWitnessed { redundant }),
            Message::Nack { promised, .. } => Some(RecoverResp::Nack(promised)),
            _ => None,
        }
    }

    /// [`recover`](Self::recover), handing the replicas `keys`/`events` — all
    /// this node knows of the transaction (the coordinator settling its own
    /// write knows all of it).
    ///
    /// A multi-shard transaction is recovered **by its route** — kept with
    /// the command, carried by any report, or, for the coordinator settling
    /// its own write, its keys — so every shard it touches is heard from,
    /// including one that never witnessed it or answers late
    /// ([`recover_routed`](Self::recover_routed)). Without a route (a
    /// single-shard transaction) the touched shards are those the reports
    /// reveal ([`recover_unrouted`](Self::recover_unrouted)).
    async fn recover_with(
        &self,
        txn: TxnId,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> anyhow::Result<CommitOutcome> {
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

        let mut route = self
            .replica
            .lock()
            .expect("replica poisoned")
            .route(txn)
            .or_else(|| self.spans_shards(&keys).then(|| keys.clone()));
        let mut ballot = Ballot(self.mint().await?);

        for _attempt in 0..3 {
            let mut rx = self.register(txn);
            let attempt = match &route {
                Some(route) => {
                    self.recover_routed(&mut rx, txn, ballot, route, &keys, &events)
                        .await
                }
                None => {
                    self.recover_unrouted(&mut rx, txn, ballot, &keys, &events)
                        .await
                }
            };
            // Deregistered on failure too: a leaked response channel makes the
            // sweep take this node's own transaction for locally in flight,
            // and never recover it.
            self.deregister(txn);
            match attempt? {
                Attempt::Done(outcome) => return Ok(outcome),
                Attempt::Retry(promised) => ballot = self.higher_ballot(promised).await?,
                // The replicas already promised `ballot`: ask again under it.
                Attempt::Route(found) => route = Some(found),
            }
        }

        anyhow::bail!("recovery exhausted its ballot retries")
    }

    /// One recovery attempt for a transaction without a known route: the
    /// touched shards are reassembled from the reports.
    async fn recover_unrouted(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        txn: TxnId,
        ballot: Ballot,
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<Attempt> {
        let all_nodes = self.topology.nodes();
        // Every replica gets what this node holds: one that never
        // witnessed the transaction pre-accepts it under this ballot, so
        // its vote counts here and the original coordinator, its PreAccept
        // possibly still in flight, can no longer win it there.
        self.broadcast(
            &all_nodes,
            Message::BeginRecovery {
                txn,
                ballot,
                keys: keys.to_vec(),
                events: events.to_vec(),
            },
        )
        .await;

        // Gather what every reachable replica knows within a short window,
        // tagged with the responder so deps can be grouped by shard.
        let mut resp = Self::collect_tagged(
            rx,
            all_nodes.len(),
            Self::after(self.settings.fast_timeout),
            |m| Self::recover_report(m, ballot),
        )
        .await;
        if let Some(promised) = Self::nack_in(&resp) {
            return Ok(Attempt::Retry(promised));
        }
        // A replica holding the transaction with a multi-shard route: it must
        // be recovered by that route instead.
        if let Some(route) = resp
            .iter()
            .find_map(|(_, r)| match r {
                RecoverResp::Ok(f) => f.route.clone(),
                _ => None,
            })
            .filter(|route| self.spans_shards(route))
        {
            return Ok(Attempt::Route(route));
        }

        // Reconstruct the full key/event set (each shard reported only its
        // owned subset) and plan the touched shards — the plan re-partitions
        // both back into per-shard slices for Accept/Commit.
        let (all_keys, all_events) = self.reassemble(keys, events, &resp);

        // A replica that owns none of what this node sent (another shard's
        // part) answered blind; now that the reports revealed its part, ask
        // it again so it votes too.
        let blind: Vec<NodeId> = resp
            .iter()
            .filter(|(_, r)| matches!(r, RecoverResp::Ok(f) if !f.known))
            .map(|(from, _)| *from)
            .filter(|&node| {
                all_keys
                    .iter()
                    .any(|k| !keys.contains(k) && self.topology.owns(node, k))
            })
            .collect();
        if !blind.is_empty() {
            self.broadcast(
                &blind,
                Message::BeginRecovery {
                    txn,
                    ballot,
                    keys: all_keys.clone(),
                    events: all_events.clone(),
                },
            )
            .await;
            let again = Self::collect_tagged(
                rx,
                blind.len(),
                Self::after(self.settings.fast_timeout),
                |m| Self::recover_report(m, ballot),
            )
            .await;
            if let Some(promised) = Self::nack_in(&again) {
                return Ok(Attempt::Retry(promised));
            }
            for (from, report) in again {
                if let Some(slot) = resp.iter_mut().find(|(n, _)| *n == from) {
                    slot.1 = report;
                }
            }
        }

        let known: Vec<(NodeId, RecoverFields)> = resp
            .into_iter()
            .filter_map(|(from, r)| match r {
                RecoverResp::Ok(f) if f.known => Some((from, f)),
                _ => None,
            })
            .collect();
        if known.is_empty() {
            anyhow::bail!("transaction unknown to recovery quorum");
        }
        let (plans, _union) = self.plan(&all_keys, &all_events);

        // Recovery-quorum gate: the decision must rest on a slow quorum of
        // every touched shard's replicas — each one's report and vote — so
        // it intersects any quorum that could already have decided: without
        // it a sub-quorum sample could miss a Commit/Accept, or raise `t0`
        // while the transaction fast-committed at `t0`. A replica that
        // owns a key of the transaction never answers blind (it pre-accepts
        // under this ballot), so every one heard from counts. Otherwise
        // bail; the next sweep retries.
        for plan in &plans {
            let heard = known
                .iter()
                .filter(|(from, _)| self.topology.node_shard(*from) == Some(plan.shard))
                .count();
            if heard < plan.slow_q {
                anyhow::bail!("recovery quorum not reached for shard {}", plan.shard);
            }
        }
        self.drive_recovery(rx, txn, ballot, &plans, None, known)
            .await
    }

    /// One recovery attempt for a multi-shard transaction, by its `route`.
    ///
    /// Every replica of every route shard is asked (`BeginRecovery`, routed).
    /// One holding the transaction reports on it; one that never witnessed it
    /// does **not** pre-accept it — it promises the ballot (journaled) and
    /// says so ([`Message::NotWitnessed`]). The reports are gathered until
    /// each route shard has been heard from enough to decide (or the slow-path
    /// budget runs out — a shard slower than the fast-path window is waited
    /// for, never left out), then [`route_verdict`](Self::route_verdict)
    /// decides: invalidate — a shard never witnessed the transaction, so it
    /// never committed (abort it everywhere, [`Message::Invalidate`]) — or
    /// recover it as usual over the route's shards.
    async fn recover_routed(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        txn: TxnId,
        ballot: Ballot,
        route: &[Key],
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<Attempt> {
        let (shards, targets) = self.plan(route, &[]);
        self.broadcast(
            &targets,
            Self::routed(
                Some(route),
                Message::BeginRecovery {
                    txn,
                    ballot,
                    keys: keys.to_vec(),
                    events: events.to_vec(),
                },
            ),
        )
        .await;
        let resp = self.collect_route_reports(rx, &shards, ballot).await;
        if let Some(promised) = Self::nack_in(&resp) {
            return Ok(Attempt::Retry(promised));
        }

        let planned = match self.route_verdict(&shards, &resp) {
            Verdict::Undecided(why) => anyhow::bail!("recovery of {txn:?} undecided: {why}"),
            Verdict::Invalidate => {
                tracing::debug!(
                    node = self.id.0,
                    ?txn,
                    "invalidating a transaction a shard never witnessed"
                );
                self.broadcast(&targets, Message::Invalidate { txn }).await;
                return Ok(Attempt::Done(CommitOutcome {
                    txn,
                    conflict: true,
                }));
            }
            Verdict::Recover(planned) => planned,
        };

        let (_, all_events) = self.reassemble(keys, events, &resp);
        let planned_keys: Vec<Key> = route
            .iter()
            .filter(|k| planned.contains(&self.topology.shard_of(k)))
            .cloned()
            .collect();
        let (plans, _union) = self.plan(&planned_keys, &all_events);
        let known: Vec<(NodeId, RecoverFields)> = resp
            .into_iter()
            .filter_map(|(from, r)| match r {
                RecoverResp::Ok(f) if f.known => Some((from, f)),
                _ => None,
            })
            // A skipped shard's applied replica stays: it holds the decision
            // (see `execute`).
            .filter(|(from, f)| {
                f.status == Status::Applied
                    || self
                        .topology
                        .node_shard(*from)
                        .is_some_and(|shard| planned.contains(&shard))
            })
            .collect();
        self.drive_recovery(rx, txn, ballot, &plans, Some(route), known)
            .await
    }

    /// Gathers a routed recovery's reports — one per replica of the route's
    /// `shards` — until [`route_settled`](Self::route_settled) holds, a Nack
    /// arrives, or the slow-path budget runs out.
    async fn collect_route_reports(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        shards: &[ShardPlan],
        ballot: Ballot,
    ) -> Vec<(NodeId, RecoverResp)> {
        let deadline = Self::after(self.settings.collect_timeout);
        let mut resp: Vec<(NodeId, RecoverResp)> = Vec::new();
        // One report per responder (see `collect_tagged`).
        let mut seen: HashSet<NodeId> = HashSet::new();
        while Self::nack_in(&resp).is_none() && !self.route_settled(shards, &resp) {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, message))) => {
                    let ours = shards.iter().any(|s| s.replicas.contains(&from));
                    if let Some(report) = Self::recover_report(message, ballot) {
                        if ours && seen.insert(from) {
                            resp.push((from, report));
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        resp
    }

    /// One route shard's reports, counted by kind.
    fn shard_reports(&self, shard: ShardId, resp: &[(NodeId, RecoverResp)]) -> ShardReports {
        let mut counts = ShardReports::default();
        for (_, report) in resp
            .iter()
            .filter(|(from, _)| self.topology.node_shard(*from) == Some(shard))
        {
            counts.heard += 1;
            match report {
                RecoverResp::Ok(f) if f.known => counts.known += 1,
                RecoverResp::NotWitnessed { redundant: false } => counts.blind += 1,
                RecoverResp::NotWitnessed { redundant: true } => counts.redundant += 1,
                _ => {}
            }
        }
        counts
    }

    /// Whether a routed recovery has heard enough to decide: an invalidation
    /// is already proven (an invalidated report, or a slow quorum of one shard
    /// that never witnessed the transaction), or every route shard has a slow
    /// quorum of reports, a report from below its watermark, or has answered
    /// in full. A decided transaction one shard compacted needs a report from
    /// a replica that applied it (see [`route_verdict`](Self::route_verdict)):
    /// until one arrives, every other shard is heard out in full.
    fn route_settled(&self, shards: &[ShardPlan], resp: &[(NodeId, RecoverResp)]) -> bool {
        let known = || {
            resp.iter().filter_map(|(_, r)| match r {
                RecoverResp::Ok(f) if f.known => Some(f),
                _ => None,
            })
        };
        if known().any(RecoverFields::invalidated)
            || shards
                .iter()
                .any(|s| self.shard_reports(s.shard, resp).blind >= s.slow_q)
        {
            return true;
        }
        let counts: Vec<ShardReports> = shards
            .iter()
            .map(|s| self.shard_reports(s.shard, resp))
            .collect();
        let await_applied = counts.iter().any(|c| c.redundant > 0)
            && known().any(|f| f.status >= Status::Committed)
            && !known().any(|f| f.status == Status::Applied);
        shards.iter().zip(&counts).all(|(s, c)| {
            (c.known >= s.slow_q && !await_applied)
                || c.redundant > 0
                || c.heard == s.replicas.len()
        })
    }

    /// What a routed recovery's reports prove, in order:
    ///
    /// 1. **Invalidated** somewhere — a recovery already proved the
    ///    transaction never committed: abort it again.
    /// 2. A slow quorum of one shard **never witnessed** it, each having
    ///    durably promised this ballot: it never committed (a commit needs a
    ///    PreAccept quorum on every shard, which would meet that quorum), and
    ///    never will (those replicas refuse the original coordinator below the
    ///    promise, and no recovery can gather a witnessed quorum there either).
    ///    Invalidate.
    /// 3. A report at `Committed` or beyond: the decision exists. Recover it
    ///    on every shard still holding it — a shard reporting from below its
    ///    watermark applied it and compacted it away — gated on a slow quorum
    ///    of reports from each. Skipping such a shard drops its read, so the
    ///    decision is then taken from a replica that applied it (see
    ///    [`execute`](Self::execute)); with none heard from, nothing is decided.
    /// 4. A shard reporting from below its watermark while another shard's
    ///    slow quorum is still below `Committed`: the low shard never held it
    ///    un-applied (that pins the watermark), and did not apply it (an Apply
    ///    follows reads from a quorum of every shard, all `Committed` first) —
    ///    so it never witnessed it, and never will: no coordinator's message
    ///    outlives the compaction margin (see
    ///    [`compaction_margin`](NodeConfig::compaction_margin)). Invalidate.
    /// 5. A slow quorum of reports from every shard: recover as usual.
    /// 6. Otherwise, too little is known yet.
    fn route_verdict(&self, shards: &[ShardPlan], resp: &[(NodeId, RecoverResp)]) -> Verdict {
        let known = || {
            resp.iter().filter_map(|(_, r)| match r {
                RecoverResp::Ok(f) if f.known => Some(f),
                _ => None,
            })
        };
        if known().any(RecoverFields::invalidated) {
            return Verdict::Invalidate;
        }
        let counts: Vec<(&ShardPlan, ShardReports)> = shards
            .iter()
            .map(|s| (s, self.shard_reports(s.shard, resp)))
            .collect();
        if counts.iter().any(|(s, c)| c.blind >= s.slow_q) {
            return Verdict::Invalidate;
        }
        let lacking = |s: &ShardPlan, c: &ShardReports| {
            Verdict::Undecided(format!(
                "shard {}: {} of {} replicas reported it, {} never witnessed it",
                s.shard, c.known, s.slow_q, c.blind
            ))
        };
        if known().any(|f| f.status >= Status::Committed) {
            // A shard that compacted it applied the decision, and its read
            // is gone: the decision must come from a replica that applied it.
            let applied = known().any(|f| f.status == Status::Applied);
            let mut planned = Vec::new();
            for (s, c) in &counts {
                if c.redundant > 0 {
                    if !applied {
                        return Verdict::Undecided(format!(
                            "shard {} compacted it, and no replica heard from applied it",
                            s.shard
                        ));
                    }
                    continue;
                }
                if c.known < s.slow_q {
                    return lacking(s, c);
                }
                planned.push(s.shard);
            }
            if planned.is_empty() {
                return Verdict::Undecided("every shard compacted it away".into());
            }
            return Verdict::Recover(planned);
        }
        if counts.iter().any(|(_, c)| c.redundant > 0)
            && counts.iter().any(|(s, c)| c.known >= s.slow_q)
        {
            return Verdict::Invalidate;
        }
        match counts.iter().find(|(s, c)| c.known < s.slow_q) {
            Some((s, c)) => lacking(s, c),
            None => Verdict::Recover(shards.iter().map(|s| s.shard).collect()),
        }
    }

    /// Whether the replicas' own PreAccept votes rule out that `txn` was
    /// committed on the fast path at its `t0` (accord-core's
    /// `RecoveryTracker` / `Shard::rejectsFastPath`). A fast commit needs a
    /// fast quorum `F` of a shard's electorate (`e` replicas) voting `t0`, so
    /// once more than `e − F` of that electorate are heard voting anything
    /// else, no fast quorum formed there. Only electorate votes count, as on
    /// the coordinator's fast path (`agree`); a PreAccepted report's
    /// `execute_at` is its replica's vote. `known` holds one report per
    /// responder.
    ///
    /// The superseding check alone misses this: a later conflict that a
    /// replica holds only pre-accepted may have fast-committed elsewhere
    /// without `txn` among its dependencies, and a replica's local view of
    /// such a conflict proves nothing either way. The vote count is sound
    /// both ways for any electorate `f + 1 ≤ e ≤ n`:
    /// - a fast commit at `t0` leaves at most `e − F` votes elsewhere, so it
    ///   is never ruled out;
    /// - a later conflict fast-committed without `txn` was voted for by a
    ///   fast quorum, `F + S − n` of whose members are among any `S` reports
    ///   of the shard (more than `e − F`, since `2F > e + f`). Each of them
    ///   saw the conflict before `txn` — it would be among its dependencies
    ///   otherwise — so each voted above `t0`.
    fn votes_reject_fast_path(
        txn: TxnId,
        plans: &[ShardPlan],
        known: &[(NodeId, RecoverFields)],
    ) -> bool {
        plans.iter().any(|plan| {
            let rejects = known
                .iter()
                .filter(|(from, f)| plan.electorate.contains(from) && f.execute_at != txn.0)
                .count();
            rejects > plan.electorate.len().saturating_sub(plan.fast_q)
        })
    }

    /// Decides a recovered transaction from the `known` reports — a slow
    /// quorum of every shard in `plans` — and drives it: re-propose under
    /// `ballot` (unless already committed), then Commit → Read → Apply.
    async fn drive_recovery(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        txn: TxnId,
        ballot: Ballot,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        known: Vec<(NodeId, RecoverFields)>,
    ) -> anyhow::Result<Attempt> {
        // An invalidated report is decided by the routed path; the
        // timestamp it carries is no position to commit at.
        if known.iter().any(|(_, f)| f.invalidated()) {
            anyhow::bail!("transaction {txn:?} was invalidated, but its route is unknown here");
        }
        let max_status = known
            .iter()
            .map(|(_, f)| f.status)
            .max()
            .expect("non-empty");
        let applied: Vec<NodeId> = known
            .iter()
            .filter(|(_, f)| f.status == Status::Applied)
            .map(|(from, _)| *from)
            .collect();
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
            // The value accepted at the highest ballot — among the reports
            // that accepted one: a pre-accepted command carries `Ballot(t0)`
            // as a placeholder, which ties the original coordinator's own
            // Accept ballot and must not let a pre-accept vote win.
            Status::Accepted => {
                known
                    .iter()
                    .filter(|(_, f)| f.status == Status::Accepted)
                    .max_by_key(|(_, f)| f.accepted)
                    .expect("an accepted report")
                    .1
                    .execute_at
            }
            // Nothing accepted: if committed at all, it was on the fast path,
            // at `t0`. Once a conflict or the votes rule that out, re-propose
            // above every vote, as the slow path would; otherwise keep `t0`.
            Status::PreAccepted => {
                if known.iter().any(|(_, f)| f.superseding_rejects)
                    || Self::votes_reject_fast_path(txn, plans, &known)
                {
                    known
                        .iter()
                        .map(|(_, f)| f.execute_at)
                        .max()
                        .expect("non-empty")
                } else if known.iter().any(|(_, f)| f.wait) {
                    // An earlier conflict, accepted to execute after `t0`
                    // without this transaction, has yet to commit: only
                    // its committed dependencies tell whether the fast
                    // path was possible. Retry once it has.
                    anyhow::bail!("recovery waits for an earlier conflict of {txn:?} to commit");
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
            for plan in plans {
                let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                self.broadcast(
                    &plan.replicas,
                    Self::routed(
                        route,
                        Message::Accept {
                            txn,
                            ballot,
                            execute_at,
                            deps,
                            keys: plan.keys.clone(),
                        },
                    ),
                )
                .await;
            }
            let acks = self
                .collect_by_shard(
                    rx,
                    plans,
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
                return Ok(Attempt::Retry(promised));
            }
            // Commit only what a quorum of every shard accepted: a value
            // accepted by fewer may be overlooked by a later recovery,
            // which could then decide differently.
            for plan in plans {
                if acks[&plan.shard].iter().filter(|a| a.is_ok()).count() < plan.slow_q {
                    anyhow::bail!(
                        "recovery accept quorum not reached for shard {}",
                        plan.shard
                    );
                }
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
            .execute(rx, plans, route, txn, execute_at, shard_deps, &applied)
            .await?;
        Ok(Attempt::Done(CommitOutcome { txn, conflict }))
    }

    /// Runs only PreAccept and returns the transaction id, modelling a
    /// coordinator that crashed before committing. For tests of
    /// [`recover`](Self::recover).
    pub async fn coordinate_preaccept(&self, events: Vec<Event>) -> anyhow::Result<TxnId> {
        let keys = Self::keys_of(&events);
        let (plans, _union) = self.plan(&keys, &events);
        let route = Self::route_of(&plans, &keys);
        let txn = TxnId(self.mint().await?);

        let mut rx = self.register(txn);
        for plan in &plans {
            self.broadcast(
                &plan.replicas,
                Self::routed(
                    route.as_deref(),
                    Message::PreAccept {
                        txn,
                        keys: plan.keys.clone(),
                        events: plan.events.clone(),
                    },
                ),
            )
            .await;
        }
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

    /// Runs PreAccept → (Accept) → Commit and returns without collecting a
    /// read or sending the decision, modelling a coordinator that crashed
    /// between Commit and Apply: every replica reads, reports to a coordinator
    /// that is no longer listening, and holds its slot until a recovery
    /// decides the transaction. For tests of [`recover`](Self::recover).
    pub async fn coordinate_through_commit(&self, events: Vec<Event>) -> anyhow::Result<TxnId> {
        let keys = Self::keys_of(&events);
        let (plans, _union) = self.plan(&keys, &events);
        let route = Self::route_of(&plans, &keys);
        let txn = TxnId(self.mint().await?);
        let mut rx = self.register(txn);
        let agreed = self.agree(&mut rx, &plans, route.as_deref(), txn).await;
        self.deregister(txn);
        let (execute_at, shard_deps, _) = agreed?;
        self.broadcast_commit(&plans, route.as_deref(), txn, execute_at, &shard_deps)
            .await;
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
            .keeping_join_hold(
                contact,
                self.import_from(contact, self.settings.collect_timeout, true, None),
            )
            .await?;
        // Persist the adopted floor: a restart recovers it from the journal,
        // and without it a dependency on a compacted-away transaction would
        // never count as satisfied again. Failing here leaves the snapshot
        // installed without its floor: as unusable as a half-installed one.
        let floor = self
            .replica
            .lock()
            .expect("replica poisoned")
            .redundant_before();
        if !self.save_decisions().await {
            return Err(self.partial(
                anyhow::anyhow!("cannot journal the decision records"),
                &imported,
            ));
        }
        if let Err(err) = self
            .keeping_join_hold(contact, self.journal.truncate(floor))
            .await
        {
            return Err(self.partial(err, &imported));
        }
        self.note_journal_watermark(floor);
        tracing::info!(
            node = self.id.0,
            contact = contact.0,
            pages = imported.pages,
            events = imported.events,
            bytes = imported.bytes,
            commands = imported.commands,
            ?floor,
            "joined from a snapshot"
        );

        // Report first: a peer that had stopped waiting for this node counts it
        // again on a report above what it compacted. Links deliver in order,
        // so the report lands before anything this node sends as a replica or
        // coordinator, which that peer would otherwise answer with `Rejoin`.
        // It is at least what the contact vouched for (`JoinProof`), which
        // its hold kept every peer's compaction below meanwhile — however
        // many peers' coverage a report of this shard needs.
        let mine = self.report(&self.shard_peers());
        let peers: Vec<NodeId> = self
            .topology
            .nodes()
            .into_iter()
            .filter(|&n| n != self.id)
            .collect();
        self.broadcast(
            &peers,
            Message::Watermark {
                applied_through: mine,
            },
        )
        .await;

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

        Ok(imported.commands)
    }

    /// Runs `work`, a step of a join through `contact`, sending the contact a
    /// [`JoinKeepalive`](Message::JoinKeepalive) every `collect_timeout`
    /// meanwhile: its hold on its report lapses after a few of those without
    /// a page request, and a step slower than that (a slow disk, a large
    /// page) would otherwise let peers compact past what it vouched for —
    /// and send this node back to rejoin when it reports.
    async fn keeping_join_hold<T>(
        &self,
        contact: NodeId,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        let mut work = std::pin::pin!(work);
        let period = self.settings.collect_timeout;
        let mut keepalive = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                out = &mut work => return out,
                _ = keepalive.tick() => self.send(contact, Message::JoinKeepalive).await,
            }
        }
    }

    /// The error of a join that fails after `imported` installed at least one
    /// snapshot page, tagged [`PartialSnapshot`]; a failure before anything
    /// landed stays a plain error, so the caller may try another contact.
    fn partial(&self, err: anyhow::Error, imported: &Imported) -> anyhow::Error {
        if imported.pages == 0 {
            return err;
        }
        self.metrics.record_join_partial();
        err.context(PartialSnapshot {
            pages: imported.pages,
            events: imported.events,
        })
    }

    /// The bootstrap header a sync requester installs: this node's
    /// redundancy watermark, its margin-lagged applied-through (the coverage
    /// the requester may record), and the applied commands it needs — with a
    /// digest (anti-entropy), only the ones it is missing, so a healthy
    /// in-sync round ships nothing; without one (legacy bootstrap), the full
    /// set.
    ///
    /// An anti-entropy reply is bounded like a snapshot page
    /// ([`snapshot_page_events`](NodeConfig::snapshot_page_events) /
    /// [`snapshot_page_bytes`](NodeConfig::snapshot_page_bytes)): a requester
    /// back from a long partition would otherwise be sent its whole backlog
    /// in one frame — over the transport's cap, shed, and asked for again
    /// every round, forever. It gets an execution-order prefix instead, and
    /// its next digest asks for the rest. The coverage then stops below the
    /// oldest `t0` left out: the requester records it as proof it holds
    /// everything this node applied below it.
    fn export_bootstrap(
        &self,
        known: Option<&SyncKnown>,
    ) -> (Timestamp, Timestamp, Vec<CommandState>) {
        let cutoff = self.margin_cutoff();
        let replica = self.replica.lock().expect("replica poisoned");
        let (commands, omitted) = match known {
            Some(known) => {
                let have: HashSet<TxnId> = known.txns.iter().copied().collect();
                replica.export_applied_missing(
                    known.since,
                    &have,
                    self.settings.snapshot_page_events,
                    self.settings.snapshot_page_bytes,
                )
            }
            None => (replica.export_applied(), None),
        };
        // Everything this node holds below its margin-lagged applied-through
        // is applied — and, with `commands`, on its way to the requester,
        // which records it as sync coverage — except what the budget left
        // out, all at or above `omitted`.
        let coverage = replica.applied_through(cutoff);
        let coverage = omitted.map_or(coverage, |oldest| coverage.min(oldest));
        let floor = replica.redundant_before();
        drop(replica);
        let coverage = self.exportable_coverage(coverage, &self.shard_peers());
        (floor, coverage, commands)
    }

    /// A join header ([`export_bootstrap`](Self::export_bootstrap) without a
    /// digest; with every applied command if `inline`, with none otherwise)
    /// plus what this node vouches for the joiner: its own watermark report
    /// and the epoch it holds under ([`Message::JoinProof`]). Both are
    /// taken under one replica lock — and a sync round records its coverage
    /// only after importing its commands — so every applied command below
    /// the vouch is either below the exported floor (in the snapshot) or
    /// among the applied commands of this cut: the joiner ends up holding
    /// exactly what the vouch covers. Not `inline`, those commands follow in
    /// [`CommandPage`](Message::CommandPage)s cut later; one compacted in
    /// between is no longer walked, but it is then below the page's floor,
    /// which the joiner adopts, and its events are in the snapshot pages it
    /// pulls after the last command page — so the vouch still holds. An
    /// epoch change meanwhile voids the vouch (`MIN`).
    fn export_join_header(
        &self,
        inline: bool,
    ) -> (Timestamp, Timestamp, Vec<CommandState>, u64, Timestamp) {
        let epoch = self.topology.epoch();
        let shard_peers = self.shard_peers();
        let cutoff = self.margin_cutoff();
        let replica = self.replica.lock().expect("replica poisoned");
        let commands = if inline {
            replica.export_applied()
        } else {
            Vec::new()
        };
        let vouched = self.report_locked(&replica, &shard_peers);
        let header = (
            replica.redundant_before(),
            replica.applied_through(cutoff),
            commands,
        );
        drop(replica);
        let coverage = self.exportable_coverage(header.1, &shard_peers);
        let vouched = if self.topology.epoch() == epoch {
            vouched
        } else {
            Timestamp::MIN
        };
        (header.0, coverage, header.2, epoch, vouched)
    }

    /// One sync round trip: sends `request(id)` to `contact` and waits up to
    /// `timeout` for the matching [`SyncData`](Message::SyncData).
    async fn sync_round(
        &self,
        contact: NodeId,
        timeout: Duration,
        request: impl FnOnce(u64) -> Message,
    ) -> anyhow::Result<SyncPayload> {
        let id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.sync_inbox
            .lock()
            .expect("sync poisoned")
            .insert(id, tx);
        self.send(contact, request(id)).await;
        let received = tokio::time::timeout(timeout, rx.recv()).await;
        self.sync_inbox.lock().expect("sync poisoned").remove(&id);
        received
            .map_err(|_| anyhow::anyhow!("sync timed out"))?
            .ok_or_else(|| anyhow::anyhow!("sync channel closed"))
    }

    /// Pulls one join page from `contact` — the reply to `request(id)` —
    /// asking up to [`SNAPSHOT_PAGE_ATTEMPTS`] times with `timeout` each.
    /// Every attempt uses a fresh correlation id, so a reply to a timed-out
    /// request that arrives late is dropped rather than taken for the next
    /// page's.
    async fn fetch_page(
        &self,
        contact: NodeId,
        timeout: Duration,
        what: &str,
        request: impl Fn(u64) -> Message,
    ) -> anyhow::Result<Message> {
        for attempt in 1..=SNAPSHOT_PAGE_ATTEMPTS {
            let id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.snapshot_pending
                .lock()
                .expect("snapshot poisoned")
                .insert(id, tx);
            self.send(contact, request(id)).await;
            let received = tokio::time::timeout(timeout, rx.recv()).await;
            self.snapshot_pending
                .lock()
                .expect("snapshot poisoned")
                .remove(&id);
            match received {
                Ok(Some(page)) => return Ok(page),
                Ok(None) => anyhow::bail!("{what} channel closed"),
                Err(_) => tracing::debug!(
                    node = self.id.0,
                    contact = contact.0,
                    attempt,
                    "{what} timed out"
                ),
            }
        }
        anyhow::bail!("{what} timed out after {SNAPSHOT_PAGE_ATTEMPTS} attempts")
    }

    /// Pulls the snapshot page after `after` from `contact` (see
    /// [`fetch_page`](Self::fetch_page)).
    async fn fetch_snapshot_page(
        &self,
        contact: NodeId,
        timeout: Duration,
        after: Option<Value>,
    ) -> anyhow::Result<(Vec<Event>, Option<Value>)> {
        let page = self
            .fetch_page(contact, timeout, "snapshot page", |id| {
                Message::SnapshotPageRequest {
                    id,
                    after: after.clone(),
                }
            })
            .await?;
        match page {
            Message::SnapshotPage { events, next, .. } => Ok((events, next)),
            other => anyhow::bail!("not a snapshot page: {other:?}"),
        }
    }

    /// Pulls the command page after `after` from `contact` (see
    /// [`fetch_page`](Self::fetch_page)): its floor when it cut the page,
    /// the commands, and the next page's cursor.
    async fn fetch_command_page(
        &self,
        contact: NodeId,
        timeout: Duration,
        after: Option<TxnId>,
    ) -> anyhow::Result<(Timestamp, Vec<CommandState>, Option<TxnId>)> {
        let page = self
            .fetch_page(contact, timeout, "command page", |id| {
                Message::CommandPageRequest { id, after }
            })
            .await?;
        match page {
            Message::CommandPage {
                watermark,
                commands,
                next,
                ..
            } => Ok((watermark, commands, next)),
            other => anyhow::bail!("not a command page: {other:?}"),
        }
    }

    /// Fetches the join header from `contact`: its `(watermark, coverage,
    /// commands)` and the slot its [`JoinProof`](Message::JoinProof) lands
    /// in, plus whether the commands follow in pages. Asks with a
    /// [`JoinHeaderRequest`](Message::JoinHeaderRequest) first (no commands
    /// inline); a contact that does not answer it in `timeout` — one
    /// predating it sheds it — is asked again with a
    /// [`SnapshotRequest`](Message::SnapshotRequest), which it answers with
    /// every command inline, as before.
    async fn fetch_join_header(
        &self,
        contact: NodeId,
        timeout: Duration,
    ) -> anyhow::Result<(SyncPayload, PendingProof<'_>, bool)> {
        let mut slot = None;
        let paged = self
            .sync_round(contact, timeout, |id| {
                slot = Some(self.pending_proof(id));
                Message::JoinHeaderRequest { id }
            })
            .await;
        match (paged, slot) {
            (Ok(header), Some(slot)) => return Ok((header, slot, true)),
            (Err(err), _) => tracing::debug!(
                node = self.id.0,
                contact = contact.0,
                %err,
                "no paged join header; asking for the inline one"
            ),
            (Ok(_), None) => unreachable!("the request registers its proof slot"),
        }
        let mut slot = None;
        let header = self
            .sync_round(contact, timeout, |id| {
                slot = Some(self.pending_proof(id));
                Message::SnapshotRequest { id }
            })
            .await?;
        Ok((
            header,
            slot.expect("the request registers its proof slot"),
            false,
        ))
    }

    /// Registers the [`JoinProof`](Message::JoinProof) slot of the join
    /// header request `id`.
    fn pending_proof(&self, id: u64) -> PendingProof<'_> {
        self.proof_pending
            .lock()
            .expect("proof poisoned")
            .insert(id, None);
        PendingProof { node: self, id }
    }

    /// A joiner this node vouched for asked for another page: it is still
    /// at it, so its hold stays (see [`JoinHold`]).
    fn refresh_join_hold(&self, joiner: NodeId) {
        if let Some(hold) = self
            .join_holds
            .lock()
            .expect("join holds poisoned")
            .get_mut(&joiner)
        {
            hold.refreshed = Instant::now();
        }
    }

    /// The materialised snapshot a legacy joiner gets inline, read a page at
    /// a time, or `None` once it outgrows `budget` bytes
    /// ([`snapshot_event_size`]) — it could not cross the transport in one
    /// frame anyway, and stopping there bounds what this node reads for it.
    async fn inline_snapshot(&self, budget: usize) -> anyhow::Result<Option<Vec<Event>>> {
        let mut snapshot = Vec::new();
        let mut bytes = 0;
        let mut after = None;
        loop {
            let page = self
                .datastore
                .snapshot_page(
                    after.take(),
                    self.settings.snapshot_page_events,
                    self.settings.snapshot_page_bytes,
                )
                .await?;
            bytes += page.events.iter().map(snapshot_event_size).sum::<usize>();
            if bytes > budget {
                return Ok(None);
            }
            snapshot.extend(page.events);
            match page.next {
                Some(cursor) => after = Some(cursor),
                None => return Ok(Some(snapshot)),
            }
        }
    }

    /// Fetches `contact`'s committed state and imports every command for a key
    /// this node owns that it does not already have. Used by both bootstrap
    /// ([`join`](Node::join), `want_snapshot = true`) and the anti-entropy repair
    /// sweep (`false`).
    ///
    /// With `want_snapshot`, the contact also ships its materialised data-store
    /// snapshot and redundancy watermark: a bootstrapping node may be below the
    /// cluster's truncation point, where command replay alone no longer
    /// reconstructs the state. The header ([`Message::JoinHeaderRequest`],
    /// or [`Message::SnapshotRequest`] from a contact predating it) brings
    /// the watermark; the recent commands follow page by page
    /// ([`Message::CommandPageRequest`]; inline in the older header), then the
    /// snapshot ([`Message::SnapshotPageRequest`]), each snapshot page
    /// installed as it lands, so a store and a backlog of any size cross the
    /// wire in frames far under the transport's cap. The node adopts the
    /// highest watermark it was sent (so a dependency on a compacted-away
    /// transaction counts as satisfied) and imports the recent commands above
    /// it into its conflict graph only — their events are already in the
    /// snapshot. The header and the command pages are taken before the
    /// snapshot pages, so every command listed has its events in a page; a
    /// transaction applied on the contact meanwhile is in the join buffer too,
    /// and its replay finds the events already there.
    /// A failure once a page is installed is tagged [`PartialSnapshot`].
    /// Anti-entropy needs no snapshot (the watermark is the cluster-wide min,
    /// so an existing replica has already applied everything below it), so it
    /// applies each imported command's events as before.
    async fn import_from(
        &self,
        contact: NodeId,
        timeout: Duration,
        want_snapshot: bool,
        known: Option<SyncKnown>,
    ) -> anyhow::Result<Imported> {
        let mut imported = Imported::default();
        let mut apply_failed = false;
        let mut proof_slot: Option<PendingProof<'_>> = None;
        let (watermark, coverage, commands) = if want_snapshot {
            // A page overlapping events already here would be skipped whole
            // (an idempotent re-apply), leaving a gap: a join needs an empty
            // store, and says so before fetching anything.
            let probe = self.datastore.snapshot_page(None, 1, usize::MAX).await?;
            anyhow::ensure!(
                probe.events.is_empty(),
                "join only over an empty store; this one holds events"
            );
            // The contact's vouch follows its header on the same link
            // ([`Message::JoinProof`]); a contact predating it sends none.
            let ((mut watermark, coverage, _, mut commands), slot, paged) =
                self.fetch_join_header(contact, timeout).await?;
            proof_slot = Some(slot);
            // Then the applied commands, page by page, before any snapshot
            // page: every command listed has its events in a page fetched
            // after it. The contact may compact between pages; the highest
            // floor it sent is the one to adopt, and what it covers is in the
            // snapshot — dropped here, as compacting below it would.
            if paged {
                let mut after = None;
                loop {
                    let (floor, page, next) =
                        self.fetch_command_page(contact, timeout, after).await?;
                    watermark = watermark.max(floor);
                    commands.extend(page);
                    match next {
                        Some(cursor) => after = Some(cursor),
                        None => break,
                    }
                }
                commands.retain(|cmd| cmd.txn.0 >= watermark);
            }
            // Install the materialised snapshot (the truncated prefix and
            // beyond) for owned keys under a synthetic, sub-watermark id —
            // redundant for ordering but it sets this node's versions and
            // committed map. Page by page, in the contact's order, so an
            // aggregate split across two pages keeps its versions contiguous.
            let mut after = None;
            loop {
                let (events, next) = match self
                    .fetch_snapshot_page(contact, timeout, after.take())
                    .await
                {
                    Ok(page) => page,
                    Err(err) => return Err(self.partial(err, &imported)),
                };
                let owned: Vec<Event> = events
                    .into_iter()
                    .filter(|event| self.topology.owns(self.id, &Key::of(event)))
                    .collect();
                if !owned.is_empty() {
                    let bytes: usize = owned.iter().map(snapshot_event_size).sum();
                    let count = owned.len();
                    if let Err(err) = self
                        .datastore
                        .apply(TxnId(watermark), watermark, owned, true)
                        .await
                    {
                        // A joiner without its snapshot would adopt the
                        // contact's floor over a missing prefix — a permanent
                        // hole. Fail the join instead; the backend may have
                        // written part of the page, so it counts as installed.
                        imported.pages += 1;
                        return Err(self.partial(
                            err.context("cannot install the bootstrap snapshot"),
                            &imported,
                        ));
                    }
                    imported.events += count;
                    imported.bytes += bytes;
                }
                imported.pages += 1;
                match next {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }
            }
            (watermark, coverage, commands)
        } else {
            let (watermark, coverage, _, commands) = self
                .sync_round(contact, timeout, |id| Message::SyncRequest {
                    id,
                    snapshot: false,
                    known,
                })
                .await?;
            (watermark, coverage, commands)
        };

        // A transaction this node minted, shipped back by a peer (a node that
        // lost its journal, and with it its clock bound, rejoins this way):
        // never mint at or below it again.
        if let Some(minted) = commands
            .iter()
            .map(|cmd| cmd.txn.0)
            .filter(|t0| t0.node == self.id)
            .max()
        {
            self.clock.restore(minted);
        }
        for cmd in commands {
            if !cmd.keys.iter().any(|k| self.topology.owns(self.id, k)) {
                continue;
            }
            // Already applied here (or compacted away below the floor): nothing
            // to ship. A cheap pre-check; `import_applied` re-checks under the
            // lock, so a concurrent local apply only costs one idempotent
            // data-store write.
            if self
                .replica
                .lock()
                .expect("replica poisoned")
                .deps_applied(std::slice::from_ref(&cmd.txn))
            {
                continue;
            }
            let (txn, execute_at, mut events) = (cmd.txn, cmd.execute_at, cmd.events.clone());
            // Shipped with the provisional stamps; applied with the agreed
            // ones, exactly as the contact applied them.
            stamp_events(execute_at, &mut events);
            let commit = cmd.applied_conflict == Some(false);
            // Materialise the events in the data store **before** recording the
            // command as `Applied`. `Status::Applied` is the promise that a
            // transaction's effect is visible in the store: the read barrier
            // (`deps_applied`) and the execution gate (`is_ready`) both trust it.
            // Marking first — as this used to — opened a window in which a
            // linearizable read woken by any other apply served the local store
            // without this command's events (a stale read), and in which a
            // dependent transaction executed ahead of it, writing a version gap
            // the backend rejects and the store swallows as a duplicate — losing
            // every later event on that key here (a lasting divergence).
            //
            // On bootstrap the snapshot already materialised these events. A
            // failed apply leaves the command un-imported so the next
            // anti-entropy round ships it again — and ends this round: the
            // commands arrive in execution order, and one applied past a gap
            // would put a later version in the store ahead of an earlier one.
            // An invalidated command never executes: nothing to apply.
            if !want_snapshot
                && !cmd.invalidated()
                && self
                    .datastore
                    .apply(txn, execute_at, events, commit)
                    .await
                    .is_err()
            {
                apply_failed = true;
                break;
            }
            let inserted = self
                .replica
                .lock()
                .expect("replica poisoned")
                .import_applied(cmd);
            if inserted {
                imported.commands += 1;
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
            // Every page is installed: this node now holds what the contact
            // vouched for, if it shares the contact's shard and epoch.
            let proof = proof_slot.take().and_then(PendingProof::take);
            if let Some((epoch, vouched)) = proof {
                if epoch == self.topology.epoch()
                    && self.topology.node_shard(contact) == self.topology.node_shard(self.id)
                {
                    // A transaction minted elsewhere before the inbox started
                    // may sit up to a skew above this node's clock.
                    let mut until = self.clock.now();
                    until.micros = until.micros.saturating_add(self.clock.max_skew_micros());
                    let mut slot = self.join_proof.lock().expect("join proof poisoned");
                    if slot.is_none_or(|held| held.epoch != epoch || held.vouched < vouched) {
                        *slot = Some(JoinProven {
                            epoch,
                            vouched,
                            until,
                        });
                    }
                }
            }
        }

        // A completed round (even one that shipped nothing — the strongest
        // evidence) proves this node holds everything the contact had applied
        // below its reported coverage, clamping this node's watermark reports.
        // Only a same-shard contact witnesses this shard's transactions, and a
        // failed local apply voids the claim for this round.
        if !apply_failed && self.topology.node_shard(contact) == self.topology.node_shard(self.id) {
            self.record_sync_coverage(contact, coverage);
        }
        // Tripwire: an anti-entropy contact that already truncated above what
        // this node has applied can no longer ship the window in between —
        // compaction outran repair, the divergence the coverage clamp exists
        // to prevent (or an operator compacted manually). A bootstrapping
        // joiner is legitimately below the contact's floor: the snapshot
        // covers it.
        if !want_snapshot {
            let (applied, floor) = {
                let replica = self.replica.lock().expect("replica poisoned");
                (
                    replica.applied_through(self.margin_cutoff()),
                    replica.redundant_before(),
                )
            };
            let my_shard = self.topology.node_shard(self.id);
            let shard_peers: Vec<NodeId> = self
                .topology
                .nodes()
                .into_iter()
                .filter(|&n| self.topology.node_shard(n) == my_shard)
                .collect();
            let vouched = match self.proven_coverage(&shard_peers) {
                Some(coverage) => coverage.min(applied),
                None => applied,
            };
            if watermark > vouched.max(floor) {
                tracing::warn!(
                    node = self.id.0,
                    contact = contact.0,
                    ?watermark,
                    ?vouched,
                    "sync contact truncated above this node's proven coverage — \
                     possible unrepairable divergence"
                );
            }
        }

        if imported.commands > 0 || imported.events > 0 {
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
        validate_layout(&shards)?;
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
        validate_layout(&proposed)?;
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
        let mut ballot = Ballot(self.mint().await?);

        for attempt in 0..6 {
            // Yield: if the epoch committed (this round or while we were backing off
            // from a duel), adopt that decided layout — never our own `proposed`,
            // which may differ from the chosen value.
            if let Some(layout) = self.committed_layout(epoch) {
                return Ok(layout);
            }
            // This attempt's replies, until it ends; another proposal for the
            // epoch on this node has a round of its own.
            let (round, mut rx) = ConfigRound::open(self, epoch);

            // Phase 1: Prepare → adopt the highest already-accepted value, if any.
            for &node in acceptors {
                self.send(node, Message::ConfigPrepare { epoch, ballot })
                    .await;
            }
            // One vote per acceptor: a duplicated reply (a frame resent over a
            // fresh connection) must not complete a quorum of fewer acceptors.
            let mut promisers: HashSet<NodeId> = HashSet::new();
            let mut nack: Option<Ballot> = None;
            let mut adopted: Option<(Ballot, Vec<Vec<NodeId>>)> = None;
            let deadline = Self::after(self.settings.collect_timeout);
            while promisers.len() < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    // Only promises answering THIS ballot count; a stale reply
                    // from an earlier attempt (or another proposer's round)
                    // must not fill the quorum.
                    Ok(Some((
                        from,
                        Message::ConfigPromise {
                            ballot: b,
                            accepted_ballot,
                            accepted_layout,
                            ..
                        },
                    ))) if b == ballot => {
                        if !promisers.insert(from) {
                            continue;
                        }
                        if let Some(layout) = accepted_layout {
                            if adopted.as_ref().is_none_or(|(b, _)| accepted_ballot > *b) {
                                adopted = Some((accepted_ballot, layout));
                            }
                        }
                    }
                    // A refusal of this ballot names a higher promise; one at
                    // or below it refused another proposal's.
                    Ok(Some((_, Message::ConfigNack { promised, .. }))) if promised > ballot => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            if let Some(promised) = nack {
                // Back off before re-preparing so duelling proposers desync (the
                // delay is rank/attempt-asymmetric, so two racers never re-collide in
                // lockstep) — then the top-of-loop committed-check lets the loser
                // adopt the winner's decision instead of escalating forever.
                tokio::time::sleep(self.config_backoff(attempt, rank)).await;
                ballot = self.higher_ballot(promised).await?;
                continue;
            }
            if promisers.len() < need {
                anyhow::bail!("config prepare quorum not reached for epoch {epoch}");
            }

            let layout = adopted.map(|(_, l)| l).unwrap_or(proposed.clone());
            // The adopted value (an older proposer's) or a recovery's fallback
            // was never checked; an invalid one is not carried to a decision.
            if let Err(err) = validate_layout(&layout) {
                return Err(err.context(format!("refusing to decide epoch {epoch}")));
            }

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
            let mut accepters: HashSet<NodeId> = HashSet::new();
            let mut nack: Option<Ballot> = None;
            let deadline = Self::after(self.settings.collect_timeout);
            while accepters.len() < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    // Only accepts answering THIS ballot count — a stale accept
                    // from a lower ballot proves nothing about this round.
                    Ok(Some((from, Message::ConfigAccepted { ballot: b, .. }))) if b == ballot => {
                        accepters.insert(from);
                    }
                    // A refusal of this ballot names a higher promise; one at
                    // or below it refused another proposal's.
                    Ok(Some((_, Message::ConfigNack { promised, .. }))) if promised > ballot => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            drop(round);

            if let Some(promised) = nack {
                tokio::time::sleep(self.config_backoff(attempt, rank)).await;
                ballot = self.higher_ballot(promised).await?;
                continue;
            }
            if accepters.len() < need {
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
            // An entry persisted before layouts were checked: installing it
            // would break every key (no shard) or strand some (an empty one).
            // Stay on the last good epoch instead.
            if let Err(err) = validate_layout(&layout) {
                tracing::error!(
                    node = self.id.0,
                    epoch = next,
                    %err,
                    "not installing an invalid layout; staying on the previous epoch"
                );
                break;
            }
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
        // Not installed here if the log did not take it: the commit sent to
        // this node with the others, or the metadata sweep, retries it.
        if self.persist_metadata(&[(epoch, layout.to_vec())]).await {
            self.ingest_entry(epoch, layout.to_vec());
        }
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
            Ok(Some(Message::ReadFailed { reason, .. })) => {
                anyhow::bail!("forwarded read to {to:?} failed: {reason}")
            }
            _ => anyhow::bail!("forwarded read to {to:?} failed"),
        }
    }

    // ----- helpers --------------------------------------------------------

    /// The first recovery round's Nack, if any: the ballot to beat.
    fn nack_in(resp: &[(NodeId, RecoverResp)]) -> Option<Ballot> {
        resp.iter().find_map(|(_, r)| match r {
            RecoverResp::Nack(p) => Some(*p),
            _ => None,
        })
    }

    /// Re-assembles the transaction's full key and event sets from what this
    /// node holds and the owned subsets each shard's replicas reported during
    /// recovery.
    fn reassemble(
        &self,
        keys: &[Key],
        events: &[Event],
        resp: &[(NodeId, RecoverResp)],
    ) -> (Vec<Key>, Vec<Event>) {
        let reports = resp.iter().filter_map(|(_, r)| match r {
            RecoverResp::Ok(f) if f.known => Some(f),
            _ => None,
        });
        let mut all_keys: Vec<Key> = keys.to_vec();
        // Dedupe events by the event's ULID — `(aggregate_id, version)` alone
        // would collapse two aggregates of different types sharing an id.
        let mut seen: HashSet<ulid::Ulid> = events.iter().map(|e| e.id).collect();
        let mut all_events: Vec<Event> = events.to_vec();
        for report in reports {
            for k in &report.keys {
                if !all_keys.contains(k) {
                    all_keys.push(k.clone());
                }
            }
            for event in &report.events {
                if seen.insert(event.id) {
                    all_events.push(event.clone());
                }
            }
        }
        (all_keys, all_events)
    }

    /// The shards a key set touches — replica sets, quorum sizes, and each
    /// shard's slice of `keys`/`events` (one partition pass, so PreAccept /
    /// Accept / Commit ship each shard only what it owns) — plus the
    /// deduplicated union of all their replicas.
    fn plan(&self, keys: &[Key], events: &[Event]) -> (Vec<ShardPlan>, Vec<NodeId>) {
        let mut shards: BTreeMap<ShardId, ShardPlan> = BTreeMap::new();
        let mut shard_of: HashMap<&str, ShardId> = HashMap::with_capacity(keys.len());
        for key in keys {
            let shard = self.topology.shard_of(key);
            shard_of.insert(key.0.as_str(), shard);
            shards
                .entry(shard)
                .or_insert_with(|| ShardPlan {
                    shard,
                    replicas: self.topology.replicas(key),
                    electorate: self.topology.fast_electorate(key),
                    fast_q: self.topology.fast_quorum(key),
                    slow_q: self.topology.slow_quorum(key),
                    keys: Vec::new(),
                    events: Vec::new(),
                })
                .keys
                .push(key.clone());
        }
        for event in events {
            // Every event's key is in `keys` (they are derived from the events,
            // or reassembled alongside them in recovery); compute defensively
            // for one that is not.
            let shard = match shard_of.get(Key::str_of(event)) {
                Some(shard) => *shard,
                None => self.topology.shard_of(&Key::of(event)),
            };
            if let Some(plan) = shards.get_mut(&shard) {
                plan.events.push(event.clone());
            }
        }
        let plans: Vec<ShardPlan> = shards.into_values().collect();
        let union = Self::union_of(&plans);
        (plans, union)
    }

    /// A transaction's route — its full key set — when it spans more than one
    /// shard; `None` for a single-shard one, whose messages go unwrapped.
    fn route_of(plans: &[ShardPlan], keys: &[Key]) -> Option<Vec<Key>> {
        (plans.len() > 1).then(|| keys.to_vec())
    }

    /// `message`, wrapped with `route` if the transaction has one.
    fn routed(route: Option<&[Key]>, message: Message) -> Message {
        match route {
            Some(route) => Message::Routed {
                route: route.to_vec(),
                inner: Box::new(message),
            },
            None => message,
        }
    }

    /// Whether `keys` span more than one shard.
    fn spans_shards(&self, keys: &[Key]) -> bool {
        let mut shards = keys.iter().map(|k| self.topology.shard_of(k));
        shards
            .next()
            .is_some_and(|first| shards.any(|shard| shard != first))
    }

    /// The deduplicated union of every shard's replica set (sorted — the order
    /// is deterministic, callers only use it as a broadcast target set).
    fn union_of(plans: &[ShardPlan]) -> Vec<NodeId> {
        let mut union: Vec<NodeId> = plans
            .iter()
            .flat_map(|p| p.replicas.iter().copied())
            .collect();
        union.sort_unstable();
        union.dedup();
        union
    }

    /// The events this node owns (keys in this node's shard). Ownership is
    /// resolved once per distinct key, not once per event.
    fn owned_events(&self, events: Vec<Event>) -> Vec<Event> {
        let mut owned: HashMap<String, bool> = HashMap::new();
        events
            .into_iter()
            .filter(|e| {
                let key = Key::str_of(e);
                match owned.get(key) {
                    Some(&v) => v,
                    None => {
                        let v = self.topology.owns(self.id, &Key(key.to_string()));
                        owned.insert(key.to_string(), v);
                        v
                    }
                }
            })
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
        let mut seen: HashSet<&str> = HashSet::new();
        let mut keys: Vec<Key> = Vec::new();
        for event in events {
            let key = Key::str_of(event);
            if seen.insert(key) {
                keys.push(Key(key.to_string()));
            }
        }
        keys
    }

    /// A ballot strictly greater than `at_least`, owned by this node.
    async fn higher_ballot(&self, at_least: Ballot) -> anyhow::Result<Ballot> {
        // Witnessed, a ballot within the skew bound puts the clock past it,
        // so a fresh mint outbids it — covered by the clock bound like any.
        self.clock.witness(at_least.0);
        let now = self.mint().await?;
        if now > at_least.0 {
            return Ok(Ballot(now));
        }
        // Only a ballot from a clock beyond the skew bound (a faulty peer far
        // in the future) gets here. It is outbid directly, and that ballot is
        // not covered by the clock bound: covering it would drag this node's
        // own clock out with the faulty peer's after a restart.
        tracing::warn!(
            node = self.id.0,
            ?at_least,
            "outbidding a ballot beyond the clock-skew bound"
        );
        let peer = at_least.0;
        Ok(Ballot(match peer.logical.checked_add(1) {
            Some(logical) => Timestamp {
                micros: peer.micros,
                logical,
                node: self.id,
            },
            None => Timestamp {
                micros: peer.micros.saturating_add(1),
                logical: 0,
                node: self.id,
            },
        }))
    }

    /// Mints a timestamp this node never used and never will again, for
    /// whatever one names — a transaction, a ballot, a read probe — as
    /// opposed to a local cut-off, which reads the clock directly.
    ///
    /// It is never below the group's compaction floor, where a transaction
    /// would count as applied at a replica that never saw it (a wall clock
    /// lagging the group, or a fresh joiner). And it is always below the
    /// persisted clock bound: once a stamp reaches it, the bound is raised —
    /// durably, before the stamp is used — to `2 × max_skew` past the **wall
    /// clock**, not past the stamp, so restarting the clock from it again and
    /// again cannot push the clock ever further ahead. At most one journal
    /// write per `max_skew` of wall time, even with the clock held at its
    /// skew cap by a peer.
    async fn mint(&self) -> anyhow::Result<Timestamp> {
        if self.retired.load(Ordering::Acquire) {
            return Err(NodeClosed.into());
        }
        let floor = self
            .replica
            .lock()
            .expect("replica poisoned")
            .redundant_before();
        self.clock.restore(floor);
        let stamp = self.clock.now();
        if stamp.micros < self.clock_bound.load(Ordering::Acquire) {
            return Ok(stamp);
        }
        let _raising = self.clock_bound_lock.lock().await;
        if self.retired.load(Ordering::Acquire) {
            return Err(NodeClosed.into());
        }
        if stamp.micros < self.clock_bound.load(Ordering::Acquire) {
            return Ok(stamp);
        }
        let ahead = self.clock.max_skew_micros().saturating_mul(2);
        let bound = stamp
            .micros
            .saturating_add(1)
            .max(self.clock.physical_micros().saturating_add(ahead));
        self.journal
            .save_clock_bound(bound)
            .await
            .map_err(|err| err.context("cannot persist the clock bound"))?;
        self.clock_bound.store(bound, Ordering::Release);
        Ok(stamp)
    }

    /// Stops this node minting, for good: its host closed it, and the node
    /// reopening its journal starts from the clock bound persisted so far.
    /// Taken under the lock that raises the bound, so a bound being raised is
    /// persisted before this returns, and every later [`mint`](Self::mint)
    /// fails with [`NodeClosed`] — a clone the application still holds can
    /// never mint an id the reopened node mints too.
    pub(crate) async fn retire(&self) {
        let _raising = self.clock_bound_lock.lock().await;
        self.retired.store(true, Ordering::Release);
    }

    /// Starts the clock after a restart past every timestamp this node
    /// minted before: its persisted clock bound, or — for a journal written
    /// before there was one — the transactions it coordinated that the
    /// journal still holds. A restart within a skew of the last mint could
    /// leave the clock further ahead of the wall clock than the skew bound the
    /// margins are sized for: that excess (at most one skew) is waited out.
    /// More means the wall clock stepped back; the clock then leads it until
    /// it catches up, as it would have without the restart.
    async fn restore_clock(&self, commands: &[CommandState]) -> anyhow::Result<()> {
        let bound = self
            .journal
            .load_clock_bound()
            .await?
            .map(|micros| Timestamp {
                micros,
                logical: 0,
                node: self.id,
            });
        let minted = commands
            .iter()
            .map(|cmd| cmd.txn.0)
            .filter(|t0| t0.node == self.id)
            .max();
        let Some(floor) = bound.max(minted) else {
            return Ok(());
        };
        self.clock.restore(floor);
        let skew = self.clock.max_skew_micros();
        let lead = floor.micros.saturating_sub(self.clock.physical_micros());
        if lead > skew {
            let excess = lead - skew;
            if excess <= skew {
                tokio::time::sleep(Duration::from_micros(excess)).await;
            } else {
                tracing::warn!(
                    node = self.id.0,
                    lead_micros = lead,
                    "the wall clock is behind this node's clock bound (stepped back?); \
                     stamps lead it until it catches up"
                );
            }
        }
        Ok(())
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
    /// `seen` holds responders already counted (by an earlier round of the
    /// same phase): their further replies are ignored.
    async fn collect_by_shard_tagged<T>(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        done: impl Fn(&ShardPlan, &[(NodeId, T)]) -> bool,
        deadline: Instant,
        mut seen: HashSet<NodeId>,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> HashMap<ShardId, Vec<(NodeId, T)>> {
        let mut got: HashMap<ShardId, Vec<(NodeId, T)>> =
            plans.iter().map(|p| (p.shard, Vec::new())).collect();
        // One vote per responder (see `collect_tagged`).

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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use evento_core::Event;

    use crate::api::{DataStore, Journal, ShardedTopology, Topology};
    use crate::clock::{HybridLogicalClock, NodeId, Timestamp, TxnId};
    use crate::message::{CommandState, Key, Message};
    use crate::store::{InMemoryDataStore, InMemoryJournal};
    use crate::transport::InMemoryNetwork;
    use crate::{api::MessageSink, Envelope, StaticTopology};

    use super::{Node, NodeConfig};

    /// A failed read's reason is cut short on a character boundary, keeping
    /// the start of the error chain.
    #[test]
    fn a_read_failure_reason_is_cut_on_a_char_boundary() {
        let err = anyhow::anyhow!("é".repeat(super::READ_FAILURE_MAX)).context("query failed");
        let reason = super::read_failure(&err);
        assert!(reason.len() <= super::READ_FAILURE_MAX);
        assert!(reason.starts_with("query failed: é"));
        assert_eq!(super::read_failure(&anyhow::anyhow!("short")), "short");
    }

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

    /// `plan` partitions the transaction's keys and events into per-shard
    /// slices in one pass: every shard's slice hashes to that shard, and the
    /// slices cover the full input (nothing dropped, nothing duplicated).
    #[tokio::test]
    async fn plan_partitions_keys_and_events_per_shard() {
        let id = NodeId(0);
        let shards = vec![vec![NodeId(0), NodeId(1)], vec![NodeId(2), NodeId(3)]];
        let topology = Arc::new(ShardedTopology::new(id, shards));
        let net = InMemoryNetwork::new();
        let node = Node::new(
            id,
            Arc::clone(&topology) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );

        // Enough distinct aggregates to land keys in both shards.
        let events: Vec<Event> = (0..8)
            .map(|i| Event {
                id: ulid::Ulid::generate(),
                aggregate_type: "test/Account".into(),
                aggregate_id: format!("acc{i}"),
                version: 1,
                name: "Bumped".into(),
                ..Default::default()
            })
            .collect();
        let keys = Node::keys_of(&events);
        assert!(
            (0..2).all(|s| keys.iter().any(|k| topology.shard_of(k) == s)),
            "test aggregates must span both shards"
        );

        let (plans, union) = node.plan(&keys, &events);

        assert_eq!(plans.len(), 2, "both shards are planned");
        let mut keys_covered = 0;
        let mut events_covered = 0;
        for plan in &plans {
            assert!(
                plan.keys.iter().all(|k| topology.shard_of(k) == plan.shard),
                "a shard's key slice must be exactly its own keys"
            );
            assert!(
                plan.events
                    .iter()
                    .all(|e| topology.shard_of(&Key::of(e)) == plan.shard),
                "a shard's event slice must be exactly its own events"
            );
            keys_covered += plan.keys.len();
            events_covered += plan.events.len();
        }
        assert_eq!(keys_covered, keys.len(), "key slices cover the full set");
        assert_eq!(
            events_covered,
            events.len(),
            "event slices cover the full set"
        );
        let mut expected_union: Vec<NodeId> = (0..4).map(NodeId).collect();
        expected_union.sort_unstable();
        assert_eq!(union, expected_union);
    }

    /// Recovery rules the fast path out from the replicas' own votes the way
    /// accord-core's `RecoveryTracker` does: votes above `t0` count only from
    /// the fast-path electorate, and only past its slack, `e − F`.
    #[tokio::test]
    async fn the_fast_path_is_ruled_out_past_the_electorates_slack() {
        use super::RecoverFields;
        use crate::clock::Ballot;
        use crate::message::Status;

        let txn = TxnId(Timestamp {
            micros: 10,
            logical: 0,
            node: NodeId(9),
        });
        let raised = Timestamp {
            micros: 20,
            logical: 0,
            node: NodeId(9),
        };
        let key = Key("acc".into());
        // Whether these PreAccept votes, one per replica, rule it out.
        let rejects = |topology: StaticTopology, votes: &[(u64, Timestamp)]| {
            let net = InMemoryNetwork::new();
            let node = Node::new(
                NodeId(0),
                Arc::new(topology) as Arc<dyn Topology>,
                Arc::new(HybridLogicalClock::new(NodeId(0))),
                Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
                Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
                Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
            );
            let (plans, _) = node.plan(std::slice::from_ref(&key), &[]);
            let known: Vec<(NodeId, RecoverFields)> = votes
                .iter()
                .map(|&(id, vote)| {
                    let report = RecoverFields {
                        known: true,
                        status: Status::PreAccepted,
                        accepted: Ballot(txn.0),
                        execute_at: vote,
                        deps: Vec::new(),
                        superseding_rejects: false,
                        wait: false,
                        keys: vec![key.clone()],
                        events: Vec::new(),
                        route: None,
                    };
                    (NodeId(id), report)
                })
                .collect();
            Node::votes_reject_fast_path(txn, &plans, &known)
        };
        let five = || StaticTopology::new(NodeId(0), (0..5).map(NodeId).collect());

        // All five vote on the fast path: F = 4 leaves one vote of slack.
        assert!(!rejects(five(), &[(0, raised), (1, txn.0), (2, txn.0)]));
        assert!(rejects(five(), &[(0, raised), (1, raised), (2, txn.0)]));

        // Electorate {0, 1, 2}: e = F = 3, no slack. One electorate vote
        // above `t0` rules it out; votes outside the electorate never count.
        let shrunk = || five().with_fast_electorate(vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert!(rejects(shrunk(), &[(1, raised), (3, txn.0), (4, txn.0)]));
        assert!(!rejects(shrunk(), &[(3, raised), (4, raised), (0, txn.0)]));
    }

    /// A replica that never saw PreAccept indexes the transaction from the
    /// explicit `keys` on Commit and still applies it — the conflict-graph
    /// guarantee the coordinator's per-shard subsetting must preserve.
    #[tokio::test(start_paused = true)]
    async fn commit_with_explicit_keys_applies_without_preaccept() {
        let id = NodeId(0);
        let coord = NodeId(1);
        let ids = vec![id, coord, NodeId(2)];
        let net = InMemoryNetwork::new();
        let inbox = net.register(id);
        let _coord_inbox = net.register(coord);
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids)) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );
        let _loop = node.start(inbox);

        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".into(),
            aggregate_id: "acc-commit-only".into(),
            version: 1,
            name: "Bumped".into(),
            ..Default::default()
        };
        let txn = TxnId(Timestamp {
            micros: 100,
            logical: 0,
            node: coord,
        });
        let sink = net.sink(coord);
        sink.send(
            id,
            Message::Commit {
                txn,
                execute_at: txn.0,
                deps: vec![],
                keys: vec![Key("acc-commit-only".into())],
                events: vec![event],
                reply_to: coord,
            },
        )
        .await
        .unwrap();
        sink.send(id, Message::Apply { txn, commit: true })
            .await
            .unwrap();

        for _ in 0..1000 {
            if !store.applied_log().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store.applied_log().len(),
            1,
            "the commit-only replica applied the transaction"
        );
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

    /// A [`Journal`] whose `stage` and `save_route` can be made to fail — a
    /// write the journal refused before any flush.
    #[derive(Default)]
    struct StageFailJournal {
        inner: InMemoryJournal,
        failing: AtomicBool,
        routes: AtomicUsize,
    }

    impl StageFailJournal {
        fn check(&self) -> anyhow::Result<()> {
            if self.failing.load(Ordering::SeqCst) {
                anyhow::bail!("simulated stage failure");
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Journal for StageFailJournal {
        async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
            self.stage(state).await
        }
        async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
            self.check()?;
            self.inner.record(state).await
        }
        async fn save_route(&self, _txn: TxnId, _route: &[Key]) -> anyhow::Result<()> {
            self.check()?;
            self.routes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
            self.inner.load(txn).await
        }
        async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
            self.inner.load_all().await
        }
    }

    fn stage_fail_node(journal: &Arc<StageFailJournal>) -> Node {
        let id = NodeId(0);
        let net = InMemoryNetwork::new();
        Node::new(
            id,
            Arc::new(StaticTopology::new(id, vec![id, NodeId(1), NodeId(2)])) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(journal) as Arc<dyn Journal>,
        )
    }

    /// A vote whose stage failed is withheld, though the flush that follows
    /// may succeed; once the journal takes it again, the vote goes out.
    #[tokio::test]
    async fn stage_failure_withholds_the_vote() {
        let journal = Arc::new(StageFailJournal::default());
        let node = stage_fail_node(&journal);
        let from = NodeId(1);

        journal.failing.store(true, Ordering::SeqCst);
        let out = node
            .handle_staged(Envelope {
                from,
                message: preaccept(0),
            })
            .await;
        assert!(out.is_empty(), "an unstaged vote is withheld: {out:?}");

        journal.failing.store(false, Ordering::SeqCst);
        let out = node
            .handle_staged(Envelope {
                from,
                message: preaccept(0),
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::PreAcceptOk { .. })]),
            "the vote goes out once staged: {out:?}"
        );
    }

    /// A route the journal refused is forgotten, so the next routed message
    /// stages it again — and the vote it travelled with is withheld.
    #[tokio::test]
    async fn a_route_the_journal_refused_is_retried() {
        let journal = Arc::new(StageFailJournal::default());
        let node = stage_fail_node(&journal);
        let routed = || {
            let Message::PreAccept { keys, .. } = preaccept(0) else {
                unreachable!()
            };
            Message::Routed {
                route: keys.into_iter().chain([Key("elsewhere".into())]).collect(),
                inner: Box::new(preaccept(0)),
            }
        };
        let txn = preaccept(0).txn().unwrap();

        // The command stages; only the route fails.
        node.handle_staged(Envelope {
            from: NodeId(1),
            message: preaccept(0),
        })
        .await;
        journal.failing.store(true, Ordering::SeqCst);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(1),
                message: routed(),
            })
            .await;
        assert!(out.is_empty(), "the vote is withheld: {out:?}");
        assert!(
            node.replica.lock().unwrap().route(txn).is_none(),
            "an unstaged route is forgotten"
        );

        journal.failing.store(false, Ordering::SeqCst);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(1),
                message: routed(),
            })
            .await;
        assert!(matches!(out.as_slice(), [(_, Message::PreAcceptOk { .. })]));
        assert!(node.replica.lock().unwrap().route(txn).is_some());
        assert_eq!(journal.routes.load(Ordering::SeqCst), 1, "staged on retry");
    }

    /// An apply whose stage failed is enacted but not acknowledged.
    #[tokio::test]
    async fn stage_failure_withholds_the_applied_ack() {
        let journal = Arc::new(StageFailJournal::default());
        let node = stage_fail_node(&journal);
        let coord = NodeId(1);
        let Message::PreAccept { txn, keys, events } = preaccept(0) else {
            unreachable!()
        };
        let out = node
            .handle_staged(Envelope {
                from: coord,
                message: Message::Commit {
                    txn,
                    execute_at: txn.0,
                    deps: Vec::new(),
                    keys,
                    events,
                    reply_to: coord,
                },
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::ReadOk { ok: true, .. })]),
            "the commit is read: {out:?}"
        );

        journal.failing.store(true, Ordering::SeqCst);
        let out = node
            .handle_staged(Envelope {
                from: coord,
                message: Message::Apply { txn, commit: true },
            })
            .await;
        assert!(
            out.is_empty(),
            "an unstaged apply is not acknowledged: {out:?}"
        );
        assert_eq!(
            node.replica.lock().unwrap().applied_result(txn),
            Some(false),
            "the apply itself took effect"
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
