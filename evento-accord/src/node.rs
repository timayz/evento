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

use anyhow::Context as _;
use evento_core::{
    cursor::{Args, Edge, ReadResult, Value},
    Event, EventFilter, RoutingKey, TenantFilter,
};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::api::{
    floor_millis, slot_of, snapshot_event_size, stamp_events, stamp_micros, validate_layout,
    AcceptorRecord, AmnesiaFence, ConfigEntry, CopyPhase, CopyState, DataStore, Drained,
    DurableMark, FenceRecord, Journal, Layout, MessageSink, ShardId, ShardMap, Topology, SLOTS,
};
use crate::clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
use crate::failure_detector::FailureDetector;
use crate::message::{
    CommandState, Key, Message, RecoveryRound, Status, SyncKnown, INVALIDATED_AT,
};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::reconfig::{self, Fence, Floor, Transition};
use crate::replica::{Refusal, Replica, RoutedRecovery};
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
    /// Also the original coordinator's deadline: past it, its PreAccept, or its
    /// Accept at the transaction's own ballot, gets no vote from a replica that
    /// never witnessed the transaction (see `Replica::late`), and a recovery
    /// settles such a replica in two rounds (see [`RecoveryRound`]).
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
    /// TCP transport), and small enough that a bootstrap does not hold the
    /// link for long: the default 4 MiB leaves sixteen times the headroom.
    pub snapshot_page_bytes: usize,
    /// Control-plane anti-dueling delay. An automatic config-change recovery defers
    /// by `rank * config_defer_step` (the **distinguished proposer** for an epoch has
    /// rank 0 and proposes first; others wait, then skip if it already committed), and
    /// a ballot duel backs off by a rank/attempt-derived multiple of this — so
    /// concurrent reconfigurations converge instead of livelocking. Deterministic
    /// (no RNG), so the simulation stays bit-reproducible.
    pub config_defer_step: Duration,
    /// Lets a recovery invalidate a transaction one replica compacted past
    /// while the others fenced it or hold it uncommitted (#310; see the
    /// replicas' vote fence, `Replica::fenced`). Sound only once every
    /// replica of the group fences its votes: during a rolling upgrade from a
    /// build without the fence, set it `false` until the last node runs a
    /// build with it. Default `true`.
    pub fence_invalidation: bool,
    /// How long a node joining over a lost journal waits for its contact's
    /// watermark report to pass its amnesia fence before it gives up with
    /// [`FenceNotCovered`] (#312). The report passes the fence about
    /// `compaction_margin` + three clock skews after the node reopened, plus
    /// the anti-entropy rounds the contact needs to prove its coverage.
    /// Default 30 s.
    pub amnesia_join_timeout: Duration,
    /// How long [`change_topology`](Node::change_topology) waits for the
    /// nodes a change gives slots to copy them **before** it proposes the
    /// change (#373), while writes still flow, so the copy under the write
    /// fence has only the tail left. Past it the change is proposed anyway:
    /// the pre-seed only shortens the pause, the fenced copy brings whatever
    /// is left. Default 10 s; raise it for a large range.
    pub preseed_timeout: Duration,
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
            fence_invalidation: true,
            amnesia_join_timeout: Duration::from_secs(30),
            preseed_timeout: Duration::from_secs(10),
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

/// [`Node::write`] refused a write: a key it touches is moving to other
/// replicas — a layout transition first decided at epoch `since` is draining
/// (see [`Node::change_topology`]). Transient: the change installs its layout
/// within about the compaction margin and a few anti-entropy rounds. Returned
/// through `anyhow`; downcast to tell it apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopologyChanging {
    /// The epoch the transition was first decided at.
    pub since: u64,
}

impl std::fmt::Display for TopologyChanging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a key this write touches is moving (layout transition since epoch {}); retry",
            self.since
        )
    }
}

impl std::error::Error for TopologyChanging {}

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

/// [`Node::write`] refused a new write: the node opened over a journal that
/// holds something, and its peers have not confirmed yet that the journal is
/// not an older copy of itself (#339, [`Node::confirmed`]). It takes no part
/// meanwhile. Returned through `anyhow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenUnconfirmed;

impl std::fmt::Display for OpenUnconfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("this node's peers have not confirmed its journal yet")
    }
}

impl std::error::Error for OpenUnconfirmed {}

/// [`Node::write`] refused a new write: the node is still joining
/// ([`Node::is_joining`]). It buffers every reply a coordination of its own
/// would need until its join ends, so the write could not complete — and
/// what it left pre-accepted on a minority of a shard no recovery could
/// decide while it stays silent (#388). Returned through `anyhow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Joining;

impl std::fmt::Display for Joining {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("this node is joining: it coordinates nothing until its snapshot is in")
    }
}

impl std::error::Error for Joining {}

/// [`Node::write`] refused a new write: a peer saw more of this node's
/// journal than it holds — the journal was rolled back to an older copy
/// (#339, [`Node::rolled_back`]). The node takes no part any more; its host
/// opens it again over an empty journal and store, joining. Returned through
/// `anyhow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalRolledBack;

impl std::fmt::Display for JournalRolledBack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this node's journal was rolled back to an older copy; \
             it must be opened again over an empty journal and store, joining",
        )
    }
}

impl std::error::Error for JournalRolledBack {}

/// A node's liveness signal ([`Node::sweep_pressure`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepPressure {
    /// Un-applied or in-flight transactions exist.
    pub busy: bool,
    /// A monotone counter that moves only when applied state advances.
    pub activity: u64,
}

/// The node was retired by its host ([`Node::retire`]): it mints no
/// timestamps any more, so it coordinates nothing — the node reopened over its
/// journal does. Returned through `anyhow`.
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
/// would leave a gap. Empty the store before joining again — keep the
/// journal: the join wrote nothing there that a retry cannot redo.
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

/// [`Node::recover_state`] refused to open the node: its journal holds
/// nothing, yet it cannot be a fresh replica. Either its data store holds
/// events (`join_pending: false`) — the journal that went with them was lost
/// or deleted — or a join it started over a lost journal never completed
/// (`join_pending: true`). Over a lost journal the node forgot the votes,
/// promises and reports it made, so it may only come back by **joining**
/// ([`Node::recover_state_for_join`] + [`begin_join`](Node::begin_join) +
/// [`join`](Node::join)) over an empty
/// data store: the join waits for a contact that vouches past everything it
/// may have forgotten (#312). Returned through `anyhow`; downcast to tell it
/// apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LostJournal {
    /// The node already holds an amnesia fence: a join it started over a lost
    /// journal never completed.
    pub join_pending: bool,
}

impl std::fmt::Display for LostJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.join_pending {
            f.write_str(
                "this node's join after a lost journal never completed; empty its data store and open it joining",
            )
        } else {
            f.write_str(
                "this node's journal is empty but its data store is not: the journal was lost; \
                 empty the data store and open it joining",
            )
        }
    }
}

impl std::error::Error for LostJournal {}

/// [`Node::join`] over a lost journal found no contact vouching for
/// everything the node may have forgotten (#312): within
/// [`NodeConfig::amnesia_join_timeout`], the contact's watermark report never
/// reached the node's fence, or the vouch it gave with its join header
/// ([`Message::JoinProof`]) was below it, under another epoch, or from
/// another shard. Nothing was installed: join again, through the same
/// contact or another. A contact's report stays below the fence while a
/// transaction below it is still undecided there, or while too few of its
/// peers are reachable to prove its coverage. Returned through `anyhow`;
/// downcast to tell it apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceNotCovered {
    /// The node's fence.
    pub fence: Timestamp,
    /// What the contact vouched for, if it sent a vouch at all.
    pub vouched: Option<Timestamp>,
}

impl std::fmt::Display for FenceNotCovered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.vouched {
            Some(vouched) => write!(
                f,
                "the contact vouched for {vouched:?}, below this node's amnesia fence {:?}; nothing was installed",
                self.fence
            ),
            None => write!(
                f,
                "no contact vouched past this node's amnesia fence {:?}; nothing was installed",
                self.fence
            ),
        }
    }
}

impl std::error::Error for FenceNotCovered {}

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
    /// An import failed to journal or apply (anti-entropy only): the round
    /// stopped there, and the next one ships the rest again.
    stopped: bool,
}

/// How many pages an anti-entropy round pulls from its peer at most, while
/// each brings something in (see `Node::anti_entropy`).
const ANTI_ENTROPY_PAGES: usize = 16;

/// How many store pages a node reads at most to answer one
/// [`RangePageRequest`](Message::RangePageRequest): a reply waits for no more
/// than that, however sparse the slots asked for.
const RANGE_PAGE_SCANS: usize = 8;

/// How many [`collect_timeout`](NodeConfig::collect_timeout)s
/// [`change_topology`](Node::change_topology) waits for a transition to drain
/// before it returns, leaving the recovery sweep to complete it: a drain takes
/// about the compaction margin and a few anti-entropy rounds.
const TRANSITION_ROUNDS: u32 = 6;

/// How many passes a pre-seed makes at most (see `Node::preseed`): each
/// copies what landed at the sources since the previous one, and they shrink
/// fast; what is left after the last is copied under the fence.
const PRESEED_PASSES: u32 = 3;

/// What a range copy brought in (`Node::copy_slots`).
#[derive(Debug, Default, Clone, Copy)]
struct CopyOutcome {
    /// The events installed.
    events: usize,
    /// The contact's stable point before its first page, if it reported one.
    stable: Option<u64>,
    /// The lowest and highest stamps (microseconds) among the events
    /// installed, if any.
    stamps: Option<(u64, u64)>,
    /// How many of the events installed are stamped below this node's
    /// stable point as it stood when they landed: history this node's
    /// subscriptions, which read forward from a stamp cursor, already read
    /// past and will not see (#374).
    below_stable: usize,
}

impl CopyOutcome {
    /// Folds `other` into this outcome.
    fn add(&mut self, other: CopyOutcome) {
        self.events += other.events;
        self.below_stable += other.below_stable;
        self.stable = match (self.stable, other.stable) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) => a,
            (None, b) => b,
        };
        self.stamps = match (self.stamps, other.stamps) {
            (Some((lo, hi)), Some((l, h))) => Some((lo.min(l), hi.max(h))),
            (a, None) => a,
            (None, b) => b,
        };
    }
}

/// How a layout transition stands on a node (see [`reconfig`]).
enum TransitionOutcome {
    /// In progress: its moved keys are draining.
    Draining,
    /// Ended by installing its layout.
    Installed(Vec<Vec<NodeId>>),
    /// Ended by another change.
    Replaced,
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
    /// The vouch the contact sent, if one arrived yet, leaving it in place.
    fn peek(&self) -> Option<Vouch> {
        self.node
            .proof_pending
            .lock()
            .expect("proof poisoned")
            .get(&self.id)
            .copied()
            .flatten()
    }

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
///
/// What this node vouches for ignores its holds (`Node::vouch_locked`): a
/// joiner that joins again, its last join's hold still in place, is vouched
/// for afresh, and its new hold replaces the old one.
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

/// The [`Message::ConfigReadReply`] slot of a config-vote restoration
/// (#338), dropped from `Node::config_read_pending` however it ends.
struct PendingConfigRead<'a> {
    node: &'a Node,
    id: u64,
}

impl<'a> PendingConfigRead<'a> {
    fn open(node: &'a Node) -> (Self, mpsc::UnboundedReceiver<(NodeId, Message)>) {
        let id = node.correlation_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        node.config_read_pending
            .lock()
            .expect("config read poisoned")
            .insert(id, tx);
        (Self { node, id }, rx)
    }
}

impl Drop for PendingConfigRead<'_> {
    fn drop(&mut self) {
        self.node
            .config_read_pending
            .lock()
            .expect("config read poisoned")
            .remove(&self.id);
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
    /// The epoch of the layout this plan was drawn from: every message the
    /// plan sends carries it.
    epoch: u64,
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
#[derive(Clone)]
struct RecoverFields {
    /// The epoch the replica holds the transaction under.
    epoch: u64,
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

/// A response to a `Recover`: a report (`known` false: the replica holds
/// nothing of it and promised the ballot, and votes once handed its shard's
/// part), a "never witnessed" for good, a replica that lost its journal
/// ([`Message::Amnesiac`]), one past the original coordinator's deadline
/// that neither votes nor refuses yet ([`Message::Late`]), or a ballot
/// rejection.
enum RecoverResp {
    Ok(RecoverFields),
    /// The replica never witnessed it and never votes for it: below its
    /// vote fence, refused for good, or (`redundant`) below its watermark.
    NotWitnessed {
        redundant: bool,
    },
    /// The replica holds nothing of it, may have voted for it before it lost
    /// its journal, and never votes for it again.
    Amnesiac,
    /// The replica never witnessed it, is past its original coordinator's
    /// deadline, and neither voted for it nor refused it ([`Message::Late`]):
    /// it may still do either.
    Late,
    Nack(Ballot),
}

/// How one recovery attempt ended (see [`Node::recover_with`]).
enum Attempt {
    /// Decided: the outcome.
    Done(CommitOutcome),
    /// A higher ballot was promised: retry above it, if it is this node's
    /// own, else leave the transaction to the node that holds it.
    Retry(Ballot),
    /// A report revealed the transaction's multi-shard route: retry routed.
    Route(Vec<Key>),
}

/// What drives a transaction through Commit → Read → Apply
/// ([`Node::execute`]), which decides when it stops waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Driver {
    /// Its coordinator: it reports the outcome once a quorum of every shard
    /// acknowledged the apply.
    Coordinator,
    /// A recovery that reports an outcome, to the caller of
    /// [`Node::recover`] or to a coordinator settling its own write: done
    /// once this node applied the transaction, whoever collected the acks.
    Recovery,
    /// A recovery the sweep started: like [`Recovery`](Self::Recovery), but
    /// it gives up a read this replica holds back on a stalled transaction
    /// ([`ReadWaitsOn`]), for the sweep to take that one over first.
    Sweep,
}

/// A sweep recovery's read, held back on this replica by a stalled
/// transaction it depends on (see [`Node::recover_stalled`]).
#[derive(Debug)]
struct ReadWaitsOn(TxnId);

impl std::fmt::Display for ReadWaitsOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the read waits on the stalled {:?}", self.0)
    }
}

impl std::error::Error for ReadWaitsOn {}

/// One route shard's replies to a routed recovery, by kind.
#[derive(Default)]
struct ShardReports {
    /// Reports from replicas holding the transaction.
    known: usize,
    /// Replicas that never witnessed it and promised the ballot.
    blind: usize,
    /// How many of `blind` are in the shard's fast-path electorate.
    blind_electorate: usize,
    /// Replicas holding nothing of it below their redundancy watermark.
    redundant: usize,
    /// Replicas past its coordinator's deadline that never witnessed it and
    /// promised the ballot: they vote or refuse once asked (#377).
    late: usize,
    /// Replicas within the deadline that never witnessed it and promised
    /// the ballot: they vote once handed their shard's part (#377).
    promised: usize,
    /// Replies of any kind.
    heard: usize,
}

impl ShardReports {
    /// Whether the replicas of `shard` that never witnessed the transaction
    /// meet every quorum that could have committed it — every slow quorum,
    /// and every fast quorum of the electorate: then it never committed, and
    /// with their promises it never will. A slow quorum of them in a shard of
    /// odd size; two of a shard of four, whose quorums are three.
    fn rule_out_a_commit(&self, shard: &ShardPlan) -> bool {
        self.blind + shard.slow_q > shard.replicas.len()
            && self.blind_electorate + shard.fast_q > shard.electorate.len()
    }
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

/// Signals that some local state advanced — applied state (an apply, an
/// invalidation, or an anti-entropy import), or an absent peer counted
/// again. A [`Notify`] rather than a `watch` channel: a
/// `watch` parks each waiter on one of several internal notifiers picked by
/// tokio's unseeded RNG, so waiters woken by one apply would resume in a
/// random order — several writes waiting on one coordinator would then
/// complete in a different order run to run, and the deterministic
/// simulation could no longer replay a seed.
#[derive(Default)]
struct Signal {
    generation: AtomicU64,
    notify: Notify,
}

impl Signal {
    /// Records an advance and wakes every waiter, in a fixed order.
    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// How many advances there have been.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Waits until `ready` holds, re-checking it after every advance, or
    /// until `deadline`. Returns whether it held.
    async fn wait_until(&self, deadline: Instant, mut ready: impl FnMut() -> bool) -> bool {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Registered before the check, so an advance between the check
            // and the wait still wakes this waiter.
            notified.as_mut().enable();
            if ready() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let _ = tokio::time::timeout(deadline - now, notified).await;
        }
    }
}

/// An acceptor's single-decree-Paxos state for one config epoch.
#[derive(Clone)]
struct ConfigAcceptor {
    promised: Ballot,
    accepted: Option<(Ballot, ConfigEntry)>,
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
/// neither can take or tear down the other's. The proposals of an epoch are
/// ordered: a reply reaches them, and wakes their tasks, in id order — a hash
/// order would differ run to run, and the simulation would no longer replay.
type ConfigInbox = Arc<Mutex<HashMap<u64, BTreeMap<u64, ConfigSender>>>>;
type ConfigSender = mpsc::UnboundedSender<(NodeId, Message)>;

/// The committed metadata log: decided `(epoch, layout)` entries, plus the highest
/// epoch actually installed into the [`Topology`]. The log is the source of truth
/// that drives `topology.install` in **strict, contiguous** epoch order — an entry
/// for a future epoch is held until every intervening epoch arrives, so no epoch is
/// ever skipped (the gap a behind node fills via [`Message::MetadataFetch`]).
///
/// It holds the layout the node started from too (`start_epoch`), journaled
/// with the first entry decided after it: a transaction coordinated under it
/// is recovered under it, and the node's topology may be rebuilt from a later
/// layout after a restart.
struct MetadataLog {
    /// Decided entries by epoch (`BTreeMap` → deterministic ascending iteration).
    entries: BTreeMap<u64, ConfigEntry>,
    /// Highest epoch installed into the topology so far (`== topology.epoch()`).
    installed_epoch: u64,
    /// The epoch the node started from, whose layout it was built with.
    start_epoch: u64,
    /// Whether the journal holds the starting layout's entry.
    start_persisted: bool,
}

/// Shared metadata log.
type MetaLog = Arc<Mutex<MetadataLog>>;

/// The watermark reports peers gossiped, under the live layout they were
/// made under ([`Message::Watermark`]'s `live_since`): a report vouches for
/// its sender's keys under that layout only, so one made under another is
/// dropped, and never counted.
#[derive(Default)]
struct PeerReports {
    /// The epoch the live layout the reports were made under became live at.
    live_since: u64,
    by_node: HashMap<NodeId, Timestamp>,
    /// The applied-through each peer gossiped with its report
    /// ([`Message::Watermark`]'s `applied`), under the same layout: what
    /// the shard's applied floor is drawn from (see
    /// [`Node::applied_floor`]).
    applied: HashMap<NodeId, Timestamp>,
}

impl PeerReports {
    /// The reports made under the live layout that became live at
    /// `live_since`: any made under another are dropped first.
    fn under(&mut self, live_since: u64) -> &mut HashMap<NodeId, Timestamp> {
        self.made_under(live_since);
        &mut self.by_node
    }

    /// The applied-throughs gossiped under the live layout that became live
    /// at `live_since`, as [`under`](Self::under).
    fn applied_under(&mut self, live_since: u64) -> &mut HashMap<NodeId, Timestamp> {
        self.made_under(live_since);
        &mut self.applied
    }

    /// Drops everything gossiped under another live layout than the one
    /// that became live at `live_since`.
    fn made_under(&mut self, live_since: u64) {
        if self.live_since != live_since {
            self.live_since = live_since;
            self.by_node.clear();
            self.applied.clear();
        }
    }
}

/// The highest [`DurableMark`] each peer reported (#339): what this node
/// answers a peer's [open](Message::Opened) from.
#[derive(Default)]
struct PeerMarks {
    by_node: HashMap<NodeId, DurableMark>,
    /// Raised since last staged (`Journal::save_peer_marks`).
    unsaved: bool,
}

impl PeerMarks {
    /// Raises `node`'s mark to `mark` (see [`DurableMark::max`]).
    fn observe(&mut self, node: NodeId, mark: DurableMark) {
        let seen = self.by_node.entry(node).or_default();
        let raised = seen.max(mark);
        if raised != *seen {
            *seen = raised;
            self.unsaved = true;
        }
    }
}

/// A node's open over a journal that holds something (#339): what the
/// journal held, and the peers that confirmed it.
struct Opening {
    mark: DurableMark,
    /// When the node first asked.
    since: Instant,
    /// The peers that answered [`OpenedOk`](Message::OpenedOk).
    confirmed_by: HashSet<NodeId>,
}

/// What a node knows of layout transitions (see [`reconfig`]). Locked after
/// the replica lock wherever both are held: a fence is installed, and a vote
/// checked against the fences, under the replica lock, so no vote slips
/// between a fence point and its fence.
#[derive(Default)]
struct Reconfig {
    /// The transition installed and not ended yet.
    transition: Option<Transition>,
    /// Every fence installed, oldest first. Never lifted.
    fences: Vec<Fence>,
    /// The clock floors of the activations that ended a transition.
    floors: Vec<Floor>,
    /// The fence each node, this one included, last reported.
    reported: HashMap<NodeId, FenceRecord>,
    /// The fences the journal held at restart, by `since`.
    restored: HashMap<u64, Timestamp>,
    /// The epoch the live layout became live at: proven coverage, join
    /// proofs and vouches hold for as long as it stays live, transitions
    /// included.
    live_since: u64,
    /// The transition (its `since`) for which this node copied every slot it
    /// gains (see [`copy_gained_slots`](Node::copy_gained_slots)).
    copied: Option<u64>,
    /// The transition each node, this one included, last reported it copied
    /// its gained slots for.
    copies: HashMap<NodeId, u64>,
    /// How far this node got copying the slots a transition gives it, as
    /// the journal keeps it ([`Journal::save_copy_state`]): `copied` is set
    /// once it says `Copied`, and no sooner.
    copy_state: Option<CopyState>,
    /// The epoch through which this node dropped the slots it does not own
    /// under the installed layout ([`gc_lost_slots`](Node::gc_lost_slots)).
    slot_gc: Option<u64>,
    /// Until when a pre-seed ([`CopyPhase::Preseeded`]) is waited out: the
    /// change it was for is expected by then; past it, the slots it brought
    /// in go like any this node does not own. `None` with no pre-seed
    /// pending — or after a restart, which forgets when it was made.
    preseed_expires: Option<Instant>,
}

impl Reconfig {
    /// Ends the fence of the transition first decided at `since`, at `epoch`.
    fn end_fence(&mut self, since: u64, epoch: u64) {
        for fence in self.fences.iter_mut().filter(|f| f.since == since) {
            fence.end.get_or_insert(epoch);
        }
    }
}

/// What installing decided entries left to make durable
/// ([`persist_installed`](Node::persist_installed)): the commands an
/// activation invalidated. Fences are journaled with them.
#[derive(Default)]
#[must_use]
struct Installed {
    invalidated: Vec<TxnId>,
}

/// A [`SyncData`](Message::SyncData) payload handed to an in-flight sync: the
/// contact's `(watermark, coverage, recent commands)`.
/// A [`SyncData`](Message::SyncData) reply: `(watermark, coverage,
/// live_since, commands)`.
type SyncPayload = (Timestamp, Timestamp, u64, Vec<CommandState>);

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

/// How many dependencies never witnessed here a recovery sweep learns the
/// decision of, at most (see [`Node::learn_decision`]): enough for the few a
/// stalled execution hangs on, while a replica that fell behind, missing
/// thousands, catches up through anti-entropy, run between them.
const MISSING_DEPENDENCY_LOOKUPS: usize = 4;

/// How many recoveries the sweep makes for one stalled transaction, its own
/// and those of what its read turns out to wait on, at most (see
/// [`Node::recover_stalled`]).
const STALLED_CHAIN_ATTEMPTS: usize = 16;

/// How far down a chain of stalled transactions, each waiting on the next,
/// the sweep follows one before it gives up on it until its next pass.
const STALLED_CHAIN_DEPTH: usize = 8;

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
    /// This node's amnesia fence, if it came back over a lost journal
    /// (#312): restored from or set in the journal by
    /// [`restore_journal`](Node::restore_journal). While not `joined`, its
    /// join waits for a contact vouching past the fence; until
    /// `votes_restored`, it abstains from config Paxos (#338).
    amnesia: Arc<Mutex<Option<AmnesiaFence>>>,
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
    /// Layout transitions: the one in progress, the fences and floors
    /// installed, the fences peers reported.
    reconfig: Arc<Mutex<Reconfig>>,
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
    /// Channels awaiting the other acceptors' state
    /// ([`Message::ConfigReadReply`]) during a config-vote restoration
    /// (#338), by the read's correlation id.
    config_read_pending: Arc<Mutex<HashMap<u64, ConfigSender>>>,
    /// Channels awaiting snapshot pages ([`Message::SnapshotPage`]) during a
    /// [`join`](Node::join), by the page request's correlation id.
    snapshot_pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    /// Held while this node copies a range into its store — a pre-seed, or
    /// the copy under the fence — so the two never interleave: the fenced
    /// copy waits for a pre-seed still running, then resumes where it ended.
    copying: Arc<tokio::sync::Mutex<()>>,
    /// Latest `applied_through` gossiped by each shard peer (and self); the
    /// recovery sweep compacts below the per-shard minimum of these.
    peer_watermarks: Arc<Mutex<PeerReports>>,
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
    /// Max-merged per peer, keyed to the epoch the live layout became live at
    /// ([`live_since`](Node::live_since); cleared when another layout goes
    /// live); never persisted — a restarted node re-proves its coverage, which
    /// is conservative.
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
    /// Serializes staging the vote fence ([`stage_vote_fence`](Node::stage_vote_fence)):
    /// each stager stages the fence as it stands once it holds this, so the
    /// journal never ends on an older fence than one staged before it.
    vote_fence_lock: Arc<tokio::sync::Mutex<()>>,
    /// The highest vote fence this node staged (see
    /// [`Journal::save_vote_fence`]).
    staged_fence: Arc<Mutex<Timestamp>>,
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
    /// and refuses writes until its host discards its event store (keeping
    /// the journal) and opens it again by joining
    /// ([`recover_state_for_join`](Self::recover_state_for_join)).
    rejoin_required: Arc<tokio::sync::watch::Sender<bool>>,
    /// The peer whose `Rejoin` set `rejoin_required` (see
    /// [`rejoin_contact`](Self::rejoin_contact)).
    rejoin_from: Arc<Mutex<Option<NodeId>>>,
    /// False while an open over a journal that holds something awaits its
    /// peers' confirmation (#339, see [`confirmed`](Self::confirmed)).
    confirmed: Arc<AtomicBool>,
    /// The open this node asked its peers to confirm, kept once confirmed:
    /// a [`RolledBack`](Message::RolledBack) answering it, however late,
    /// still stops the node.
    opening: Arc<Mutex<Option<Opening>>>,
    /// The highest durable mark each peer reported (#339).
    peer_marks: Arc<Mutex<PeerMarks>>,
    /// True once a peer found this node's journal rolled back (#339, see
    /// [`rolled_back`](Self::rolled_back)).
    rolled_back: Arc<tokio::sync::watch::Sender<bool>>,
    /// Bumped whenever local applied state advances (an apply or an
    /// anti-entropy import). Waiters (`read_barrier`, read-your-writes) wait
    /// on it instead of polling the replica lock every millisecond.
    applied: Arc<Signal>,
    /// Bumped whenever an absent peer is counted again (see
    /// [`screen_absent`](Self::screen_absent)): a join waits on it for its
    /// contact's proof ([`await_reinstated`](Self::await_reinstated)).
    reinstated: Arc<Signal>,
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
        let start = topology.layout();
        let start_epoch = start.epoch();
        let start_layout = start.map();
        Self {
            id,
            clock,
            clock_bound: Arc::new(AtomicU64::new(0)),
            clock_bound_lock: Arc::new(tokio::sync::Mutex::new(())),
            retired: Arc::new(AtomicBool::new(false)),
            amnesia: Arc::new(Mutex::new(None)),
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
                entries: BTreeMap::from([(
                    start_epoch,
                    ConfigEntry::Active {
                        layout: start_layout,
                        drained: None,
                    },
                )]),
                installed_epoch: start_epoch,
                start_epoch,
                start_persisted: false,
            })),
            reconfig: Arc::new(Mutex::new(Reconfig {
                live_since: start_epoch,
                ..Reconfig::default()
            })),
            correlation_seq: Arc::new(AtomicU64::new(0)),
            anti_entropy_turn: Arc::new(AtomicU64::new(0)),
            metadata_turn: Arc::new(AtomicU64::new(0)),
            read_pending: Arc::new(Mutex::new(HashMap::new())),
            config_read_pending: Arc::new(Mutex::new(HashMap::new())),
            snapshot_pending: Arc::new(Mutex::new(HashMap::new())),
            copying: Arc::new(tokio::sync::Mutex::new(())),
            peer_watermarks: Arc::new(Mutex::new(PeerReports::default())),
            journal_mark: Arc::new(Mutex::new(JournalMark::default())),
            sync_coverage: Arc::new(Mutex::new((start_epoch, HashMap::new()))),
            join_proof: Arc::new(Mutex::new(None)),
            proof_pending: Arc::new(Mutex::new(HashMap::new())),
            join_holds: Arc::new(Mutex::new(HashMap::new())),
            vote_fence_lock: Arc::new(tokio::sync::Mutex::new(())),
            staged_fence: Arc::new(Mutex::new(Timestamp::MIN)),
            failure_detector: Arc::new(FailureDetector::new()),
            started_at: Instant::now(),
            absent: Arc::new(Mutex::new(HashMap::new())),
            rejoin_required: Arc::new(tokio::sync::watch::channel(false).0),
            rejoin_from: Arc::new(Mutex::new(None)),
            confirmed: Arc::new(AtomicBool::new(true)),
            opening: Arc::new(Mutex::new(None)),
            peer_marks: Arc::new(Mutex::new(PeerMarks::default())),
            rolled_back: Arc::new(tokio::sync::watch::channel(false).0),
            applied: Arc::new(Signal::default()),
            reinstated: Arc::new(Signal::default()),
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

    /// This node's amnesia fence, if it came back over a lost journal under
    /// its node id (#312): see [`Node::recover_state_for_join`].
    pub fn amnesia_fence(&self) -> Option<AmnesiaFence> {
        *self.amnesia.lock().expect("amnesia poisoned")
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

    /// Whether this node must rejoin the group from a snapshot: a peer told it
    /// to, as it was absent while transactions it never applied were
    /// compacted; or its own recovery found it holding a transaction its peers
    /// compacted past, which no recovery can decide any more (see
    /// `stranded_below`). Either way it can no longer catch up. The node drops
    /// every message and refuses writes from then on; the host
    /// [retires](Self::retire) it, stops its inbox loop and recovery task,
    /// discards its event store — keeping its journal, whose votes and
    /// promises the node must not forget — and opens it again by joining
    /// ([`recover_state_for_join`](Self::recover_state_for_join),
    /// [`begin_join`](Self::begin_join), [`join`](Self::join)).
    pub fn rejoin_required(&self) -> bool {
        *self.rejoin_required.borrow()
    }

    /// The peer that told this node to rejoin, or that its recovery found
    /// compacted past a transaction it holds, once
    /// [`rejoin_required`](Self::rejoin_required): it compacted past what
    /// this node holds, so rejoining **through it** drops everything below
    /// its floor ([`recover_state_for_join`](Self::recover_state_for_join)
    /// keeps the journal above the contact's). Through a contact whose floor
    /// lags, the node could keep a transaction the told peer compacted past,
    /// and be told to rejoin again.
    pub fn rejoin_contact(&self) -> Option<NodeId> {
        *self.rejoin_from.lock().expect("rejoin poisoned")
    }

    /// A watch that turns `true` when [`rejoin_required`](Self::rejoin_required)
    /// does.
    pub fn rejoin_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        self.rejoin_required.subscribe()
    }

    /// Whether a peer found this node's journal rolled back to an older copy
    /// of itself (#339): it saw a [`DurableMark`] from this node above the
    /// one the journal held when the node opened. The node forgot votes and
    /// promises it made since, so it drops every message and refuses writes
    /// from then on ([`JournalRolledBack`]). The host [retires](Self::retire)
    /// it, discards its journal and its event store, and opens it again over
    /// empty ones by joining ([`recover_state_for_join`](Self::recover_state_for_join),
    /// [`begin_join`](Self::begin_join), [`join`](Self::join)): the path of
    /// a lost journal (#312).
    pub fn rolled_back(&self) -> bool {
        *self.rolled_back.borrow()
    }

    /// A watch that turns `true` when [`rolled_back`](Self::rolled_back)
    /// does.
    pub fn rolled_back_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        self.rolled_back.subscribe()
    }

    /// Whether this node takes part. One opened over a journal that holds
    /// something, in a group of more than one node, first asks every other
    /// node whether the journal is at or above the [`DurableMark`] it last
    /// saw from it ([`Message::Opened`]): a journal rolled back to an older
    /// copy would have it forget votes and promises it made (#339). Until a
    /// slow quorum of its shard, itself included, confirmed it, and every
    /// other node answered or `collect_timeout` passed, the node buffers
    /// consensus messages, refuses writes ([`OpenUnconfirmed`]), and votes,
    /// vouches and mints nothing.
    pub fn confirmed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }

    /// The highest [`DurableMark`] `peer` reported to this node, if any: what
    /// this node confirms an open of `peer` against (#339). For tests and
    /// observability.
    pub fn peer_mark(&self, peer: NodeId) -> Option<DurableMark> {
        self.peer_marks
            .lock()
            .expect("peer marks poisoned")
            .by_node
            .get(&peer)
            .copied()
    }

    /// What this node's journal durably holds (see [`DurableMark`]), as its
    /// watermark reports carry it.
    pub fn durable_mark(&self) -> DurableMark {
        let life = self
            .amnesia
            .lock()
            .expect("amnesia poisoned")
            .map_or(0, |fence| fence.fence_micros);
        DurableMark {
            life,
            clock_bound: self.clock_bound.load(Ordering::Acquire),
            vote_fence: self
                .replica
                .lock()
                .expect("replica poisoned")
                .durable_fence(),
        }
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
    ///
    /// Nor does it pass the journaled vote fence (see [`Replica::fenced`]):
    /// a subscription that read past a point relies on this node never
    /// voting below it for a transaction it does not hold (#337). The sweep
    /// raises the fence to the point it computes, so the stable point trails
    /// it by a sweep at most. A cluster of this node alone is exempt: every
    /// transaction is its own, and it holds each one it votes for.
    pub fn stable_micros(&self) -> u64 {
        let shard_peers = self.shard_peers();
        let alone = self.topology.nodes() == [self.id];
        let cutoff = self.margin_cutoff();
        let replica = self.replica.lock().expect("replica poisoned");
        let bound = self.stable_locked(&replica, cutoff, &shard_peers);
        // Read under the same lock as the store's bound: a fence raised past
        // a transaction held unapplied here never lets the point pass it.
        let fence = replica.durable_fence().micros;
        drop(replica);
        floor_millis(if alone { bound } else { bound.min(fence) })
    }

    /// The stable point over an already-locked replica, before the vote
    /// fence caps it (see [`stable_micros`](Self::stable_micros)), unfloored.
    /// Takes the coverage and join-proof locks under the replica lock.
    fn stable_locked(&self, replica: &Replica, cutoff: Timestamp, shard_peers: &[NodeId]) -> u64 {
        let local = replica.stable_event_micros(cutoff);
        match self.proven_coverage(shard_peers) {
            Some(coverage) => {
                local.min(coverage.micros.saturating_sub(self.clock.max_skew_micros()))
            }
            None => local,
        }
    }

    /// This node's vote fence, as far as its journal holds it: no vote for a
    /// transaction it does not hold below it, and no vouch past it (see
    /// [`Replica::fenced`]). For tests and observability.
    pub fn vote_fence(&self) -> Timestamp {
        self.replica
            .lock()
            .expect("replica poisoned")
            .durable_fence()
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

    /// The original coordinator's deadline (see [`Replica::late`]): this
    /// node's clock less the compaction margin. Its PreAccept, and its
    /// Accept at `Ballot(t0)`, for a transaction this node does not hold, get
    /// no vote past it — so a replica that voted for a transaction on its
    /// coordinator's word did so within the margin, while its applied-through
    /// still lay below the transaction, and its gossip says it holds it (see
    /// [`applied_floor`](Self::applied_floor)). Read under the replica lock,
    /// against the fence a report raises there.
    fn coordinator_cutoff(&self) -> Timestamp {
        Timestamp {
            micros: self
                .clock
                .reading_micros()
                .saturating_sub(self.settings.compaction_margin.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        }
    }

    /// The applied-through each peer last gossiped (see
    /// [`Message::Watermark`]'s `applied`), under the live layout.
    fn gossiped_applied(&self) -> HashMap<NodeId, Timestamp> {
        let live_since = self.live_since();
        self.peer_watermarks
            .lock()
            .expect("watermarks poisoned")
            .applied_under(live_since)
            .clone()
    }

    /// The shard's **applied floor**: the slow-quorum-th lowest
    /// applied-through among the replicas of `shard_peers` — this node's
    /// `own`, and what the others gossiped (`gossiped`), a replica not
    /// heard from counting as nothing applied. While a slow quorum holds a
    /// transaction unapplied — one that may have committed — the floor stays
    /// below it, and this node hands no peer sync coverage past it: its vote
    /// fence, which covers that coverage, stays below the transaction, and a
    /// recovery that needs its vote still gets it (#337). Coverage itself
    /// never clamps an applied-through, so the floor rises with the shard
    /// from its very first gossip. In a healthy shard it trails the
    /// applied-through of its slowest quorum by one gossip round, and so do
    /// the reports and stable points built on the coverage it caps.
    fn applied_floor(
        &self,
        own: Timestamp,
        shard_peers: &[NodeId],
        gossiped: &HashMap<NodeId, Timestamp>,
    ) -> Timestamp {
        if shard_peers.is_empty() {
            return own;
        }
        let mut applied: Vec<Timestamp> = shard_peers
            .iter()
            .map(|peer| {
                if *peer == self.id {
                    own
                } else {
                    gossiped.get(peer).copied().unwrap_or(Timestamp::MIN)
                }
            })
            .collect();
        applied.sort_unstable();
        applied[crate::api::slow_quorum_size(applied.len()) - 1]
    }

    /// The most sync coverage this node may hand a peer now, over an
    /// already-locked replica: its applied-through, never past the shard's
    /// [applied floor](Self::applied_floor). Before its vote fence covers it.
    fn export_ceiling(
        &self,
        replica: &Replica,
        shard_peers: &[NodeId],
        gossiped: &HashMap<NodeId, Timestamp>,
    ) -> Timestamp {
        let own = replica.applied_through(self.margin_cutoff());
        own.min(self.applied_floor(own, shard_peers, gossiped))
    }

    /// Whether `txn` is dated more than two clock skews past this node's
    /// clock — its coordinator's clock runs beyond the skew bound — so this
    /// node casts no vote and makes no promise for it (counted). Every
    /// honest `t0` is within the bound: a coordinator's clock reads at most
    /// one skew past its wall clock, which is within one skew of this one's.
    /// The bound is what lets a node that lost its journal fence everything
    /// it ever voted on (#312, [`AmnesiaFence`]): its votes all lie below its
    /// clock's reading at the time plus two skews, and the reading below its
    /// wall clock plus one.
    fn future_dated(&self, txn: TxnId) -> bool {
        let horizon = self
            .clock
            .reading_micros()
            .saturating_add(self.clock.max_skew_micros().saturating_mul(2));
        if txn.0.micros <= horizon {
            return false;
        }
        self.metrics.record_vote_future_dated();
        tracing::debug!(
            node = self.id.0,
            ?txn,
            horizon,
            "refusing a vote for a transaction dated past the clock-skew bound"
        );
        true
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
            // An open over a journal that holds something asks the other
            // nodes to confirm it at once (#339); the sweep asks again.
            if !node.confirmed() {
                node.ask_to_confirm().await;
            }
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
    /// compaction, metadata catch-up. Not re-entrant: do not run two sweeps
    /// of one node concurrently.
    pub async fn sweep(&self) {
        self.recovery_sweep().await;
    }

    /// Cheap liveness signal: whether this node has un-applied or in-flight
    /// work, and a monotone counter that moves only when applied state
    /// advances (an apply or an anti-entropy import — *not* on sweep gossip).
    /// A node that is not busy and whose counter stands still is quiescent.
    pub fn sweep_pressure(&self) -> SweepPressure {
        let busy = self
            .replica
            .lock()
            .expect("replica poisoned")
            .has_unapplied()
            || !self.pending.lock().expect("pending poisoned").is_empty();
        SweepPressure {
            busy,
            activity: self.applied.generation(),
        }
    }

    /// Rebuilds this node's state from the journal after a process restart:
    /// restores every command's consensus state (status, ballots, decision) and
    /// replays committed, applied transactions into the data store. Call once on
    /// startup before [`start`](Node::start). In-flight (unapplied) transactions
    /// resume via the recovery sweep.
    ///
    /// Over a journal that holds nothing at all, the node is a fresh replica
    /// — unless its data store holds events: then the journal that went with
    /// them was lost, and the node, which forgot what it voted, is refused
    /// with [`LostJournal`]; it must come back by joining (#312,
    /// [`recover_state_for_join`](Self::recover_state_for_join)). A cluster
    /// of this node alone holds nobody's votes but its own, and reopens over
    /// its store. A fresh replica's first open leaves its clock bound in the
    /// journal, so the journal is never empty again while the store holds
    /// what the node applied. A node whose join after a lost journal never
    /// completed is refused too (`LostJournal { join_pending: true }`).
    pub async fn recover_state(&self) -> anyhow::Result<()> {
        let commands = self.restore_journal(false).await?;
        // Replay the retained (post-truncation) applied tail into the data store.
        // The truncated prefix is not in the journal; a durable data store already
        // holds it, and re-applying the tail is idempotent. The events are
        // stamped from `execute_at` as at the first apply, so a replay over a
        // store that already holds them is an exact duplicate.
        for cmd in &commands {
            if cmd.status == Status::Applied && cmd.applied_conflict == Some(false) {
                let mut events = self.owned_now(cmd.events.clone());
                stamp_events(cmd.execute_at, &mut events);
                if let Err(err) = self
                    .datastore
                    .apply(cmd.txn, cmd.execute_at, events, true)
                    .await
                {
                    // The store neither holds it nor takes it now: it lost
                    // the write (a store that acknowledged before it
                    // persisted, then crashed), or misses an earlier one the
                    // journal never recorded (an anti-entropy import, which
                    // went unrecorded before #331). Hand the transaction back
                    // decided but un-applied, as a join does: execution
                    // applies it again once what precedes it is in place —
                    // anti-entropy brings a missing import back — rather than
                    // the node running on a store that lacks it, or not
                    // starting at all.
                    tracing::warn!(
                        node = self.id.0,
                        txn = ?cmd.txn,
                        %err,
                        "a replayed transaction is not in the data store; it will be applied again"
                    );
                    let mut cmd = cmd.clone();
                    cmd.status = Status::Committed;
                    cmd.decision = Some(true);
                    cmd.applied_conflict = None;
                    self.replica.lock().expect("replica poisoned").restore(cmd);
                }
            }
        }
        // After the replay: a slot the node lost before it went down, whose
        // drop it did not finish (or record), goes now.
        self.gc_lost_slots().await;
        Ok(())
    }

    /// Rebuilds this node's state from its journal for a **rejoin**: its
    /// host discarded the event store but kept the journal, and
    /// [`join`](Self::join) rebuilds the store from a contact's snapshot.
    /// Restores everything [`recover_state`](Self::recover_state) does —
    /// each command's votes and ballots, the blind promises, routes and
    /// decision records, the config-Paxos acceptor state, the metadata log,
    /// the rejoin bounds and the clock bound — so the node never votes
    /// against what it voted or promised before, and never mints an id it
    /// minted before. But nothing is replayed into the store, which must
    /// stay empty for the snapshot. A transaction the journal holds applied
    /// as a commit is restored decided but un-applied, since its events are
    /// in the snapshot only if the contact applied it too: the contact then
    /// ships it applied (or it lies below the contact's floor, and is
    /// dropped); otherwise it is applied again once the join completes. Call
    /// once, before [`begin_join`](Self::begin_join) and
    /// [`start`](Self::start).
    ///
    /// Over a journal that holds nothing at all — a lost journal, or a new
    /// member's — the node forgot (or never cast) the votes, promises and
    /// reports it made. It sets an [`AmnesiaFence`] above all of them (#312):
    /// its wall clock plus three clock skews, journaled before anything else
    /// (a journal that cannot keep it is refused), with its clock floored
    /// past every id it can have minted. Its [`join`](Self::join) then waits
    /// for a contact that vouches past the fence — every write that committed
    /// below it is in that contact's state, and none that did not can commit
    /// any more — and it abstains from config Paxos until it restored its
    /// votes (#338). It begins the join itself.
    pub async fn recover_state_for_join(&self) -> anyhow::Result<()> {
        self.restore_journal(true).await.map(|_| ())
    }

    /// Restores the replica and the control plane from the journal, for
    /// [`recover_state`](Self::recover_state) and
    /// [`recover_state_for_join`](Self::recover_state_for_join). Returns the
    /// journaled commands in execution order. `joining`: the data store was
    /// discarded, so a transaction applied as a commit is restored decided
    /// but un-applied.
    async fn restore_journal(&self, joining: bool) -> anyhow::Result<Vec<CommandState>> {
        let watermark = self.journal.load_watermark().await?;
        let mut commands = self.journal.load_all().await?;
        commands.sort_by_key(|cmd| (cmd.execute_at, cmd.txn));
        let routes = self.journal.load_routes().await?;
        let promises = self.journal.load_promises().await?;
        let decisions = self.journal.load_decisions().await?;
        let clock_bound = self.journal.load_clock_bound().await?;
        let acceptors = self.journal.load_acceptors().await?;
        let metadata = self.journal.load_metadata().await?;
        let bounds = self.journal.load_rejoin_bounds().await?;
        let fence = self.journal.load_amnesia_fence().await?;
        let fences = self.journal.load_fences().await?;
        let vote_fence = self.journal.load_vote_fence().await?;
        let peer_marks = self.journal.load_peer_marks().await?;
        let copy_state = self.journal.load_copy_state().await?;
        let slot_gc = self.journal.load_slot_gc().await?;
        // Nothing durable at all: the node never voted, promised, reported
        // or minted anything under this journal — a new member's, or a
        // replacement for one that was lost.
        let pristine = watermark.is_none()
            && commands.is_empty()
            && routes.is_empty()
            && promises.is_empty()
            && decisions.is_empty()
            && clock_bound.is_none()
            && acceptors.is_empty()
            && metadata.is_empty()
            && bounds.is_empty()
            && fence.is_none()
            && fences.is_empty()
            && vote_fence.is_none()
            && peer_marks.is_empty();
        // The vote fence comes back where the journal left it (#337). A
        // journal that keeps none restarts it at the clock bound less the
        // margin, where the fence stood before it was journaled: the bound is
        // persisted past every reading a vouch was cut at.
        let margin = self.settings.compaction_margin.as_micros() as u64;
        let vote_fence = vote_fence.or_else(|| {
            clock_bound.filter(|_| !pristine).map(|bound| Timestamp {
                micros: bound.saturating_sub(margin),
                logical: 0,
                node: NodeId(0),
            })
        });
        if let Some(vote_fence) = vote_fence {
            let mut replica = self.replica.lock().expect("replica poisoned");
            replica.raise_vote_fence(vote_fence);
            replica.fence_durable(vote_fence);
            *self.staged_fence.lock().expect("fence poisoned") = vote_fence;
        }
        let fence = match fence {
            Some(fence) if !fence.joined && !joining => {
                return Err(LostJournal { join_pending: true }.into());
            }
            Some(fence) => Some(fence),
            None if pristine && joining => Some(self.set_amnesia_fence().await?),
            None if pristine => {
                self.open_over_an_empty_journal().await?;
                None
            }
            None => None,
        };
        *self.amnesia.lock().expect("amnesia poisoned") = fence;
        if let Some(fence) = fence {
            self.floor_votes_at(fence);
            if !fence.joined {
                // Its join waits for a contact that vouches past the fence;
                // until then the node takes no part (see
                // `recover_state_for_join`).
                self.begin_join();
            }
        }
        self.journal_mark
            .lock()
            .expect("journal mark poisoned")
            .persisted = watermark.unwrap_or(Timestamp::MIN);
        self.restore_clock(&commands, clock_bound).await?;

        // Restore the control plane: acceptor state (so a restarted acceptor keeps
        // its promised ballots — Paxos crash-safety) and the metadata log (so the
        // node re-installs the contiguous topology prefix it had decided).
        {
            let mut config = self.config.lock().expect("config poisoned");
            for (epoch, record) in acceptors {
                config.insert(epoch, ConfigAcceptor::from_record(record));
            }
        }
        // A transition's fence comes back at the point it was journaled at:
        // a point recomputed now could lie below votes it refused.
        {
            let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
            reconfig
                .restored
                .extend(fences.into_iter().map(|f| (f.since, f.cap)));
            // A copy the journal holds complete is complete: the store took
            // every page before it was recorded. One it holds started is
            // resumed, not dropped again (`copy_gained_slots`).
            if let Some(state) = &copy_state {
                if state.phase == CopyPhase::Copied {
                    reconfig.copied = Some(state.since);
                }
            }
            reconfig.copy_state = copy_state;
            reconfig.slot_gc = slot_gc;
        }
        {
            let mut log = self.meta_log.lock().expect("meta_log poisoned");
            for (epoch, entry) in metadata {
                if epoch == log.start_epoch {
                    log.start_persisted = true;
                }
                // Below the epoch the topology was built at, the layout is
                // history only: kept to recover what was planned under it.
                if epoch < log.installed_epoch && entry.validate().is_ok() {
                    self.topology.install_map(epoch, entry.live().clone());
                }
                log.entries.insert(epoch, entry);
            }
        }

        {
            let mut replica = self.replica.lock().expect("replica poisoned");
            for cmd in &commands {
                let mut cmd = cmd.clone();
                if joining && cmd.status == Status::Applied && cmd.applied_conflict == Some(false) {
                    // Its events went with the store: decided, to be applied
                    // again unless the join brings them back. An abort, or an
                    // invalidation, changed nothing and stays applied.
                    cmd.status = Status::Committed;
                    cmd.decision = Some(true);
                    cmd.applied_conflict = None;
                }
                replica.restore(cmd);
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
        // Installed over the restored commands: a fence point covers every
        // vote they hold, and an activation sees what it invalidates.
        let installed = self.apply_log();
        self.persist_installed(installed).await;
        // The marks this node saw from its peers, which it answers their
        // opens from (#339).
        {
            let mut marks = self.peer_marks.lock().expect("peer marks poisoned");
            for (peer, mark) in peer_marks {
                marks.observe(peer, mark);
            }
            marks.unsaved = false;
        }
        // An open over a journal that holds something waits for the other
        // nodes to confirm the journal is no older copy of itself (#339).
        if !pristine && self.topology.nodes().len() > 1 {
            if clock_bound.is_some() {
                *self.opening.lock().expect("opening poisoned") = Some(Opening {
                    mark: self.durable_mark(),
                    since: Instant::now(),
                    confirmed_by: HashSet::new(),
                });
                self.confirmed.store(false, Ordering::Release);
            } else {
                tracing::warn!(
                    node = self.id.0,
                    "this journal keeps no clock bound (Journal::save_clock_bound): \
                     an older copy of it would go unnoticed"
                );
            }
        }
        // Peers this node had stopped waiting for stay excluded until they
        // report above what was compacted past them (or rejoin).
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
        Ok(commands)
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
    /// every replica counted there applied it), every replica has reported a
    /// watermark above the transaction — each applied it, so no recovery
    /// there will ask for its decision. The shards are those of the layout the
    /// transaction was coordinated under, the ones its recoveries ask.
    /// Reports come from the watermark gossip every node sends every other. A
    /// replica that never reports again (permanently down) keeps its shard's
    /// records until it leaves the topology.
    async fn prune_decisions(&self) {
        let dropped = {
            let current = self.topology.layout();
            let live_since = self.live_since();
            let watermarks = self
                .peer_watermarks
                .lock()
                .expect("watermarks poisoned")
                .under(live_since)
                .clone();
            self.replica
                .lock()
                .expect("replica poisoned")
                .prune_decisions(|record| {
                    let layout = self
                        .topology
                        .layout_at(record.epoch)
                        .unwrap_or_else(|| Arc::clone(&current));
                    let my_shard = layout.node_shard(self.id);
                    record
                        .route
                        .iter()
                        .filter(|key| Some(layout.shard_of(key)) != my_shard)
                        .flat_map(|key| layout.replicas(key).to_vec())
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
    async fn persist_metadata(&self, entries: &[(u64, ConfigEntry)]) -> bool {
        if entries.is_empty() {
            return true;
        }
        // The starting layout goes with the first entry decided after it, so a
        // restart rebuilt on a later layout still holds it.
        let start = {
            let log = self.meta_log.lock().expect("meta_log poisoned");
            (!log.start_persisted)
                .then(|| {
                    log.entries
                        .get(&log.start_epoch)
                        .map(|l| (log.start_epoch, l.clone()))
                })
                .flatten()
                .filter(|(epoch, _)| entries.iter().all(|(e, _)| e != epoch))
        };
        let batch: Vec<(u64, ConfigEntry)>;
        let entries = match start {
            Some(start) => {
                batch = std::iter::once(start)
                    .chain(entries.iter().cloned())
                    .collect();
                &batch[..]
            }
            None => entries,
        };
        let persisted = match entries {
            [(epoch, entry)] => self.journal.append_metadata(*epoch, entry).await,
            entries => self.journal.append_metadata_batch(entries).await,
        };
        match persisted {
            Ok(()) => {
                self.meta_log
                    .lock()
                    .expect("meta_log poisoned")
                    .start_persisted = true;
                true
            }
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
    /// idempotent and ballot-fenced, so concurrent recoveries are safe. What a
    /// recovered transaction's read waits on is recovered first
    /// ([`recover_stalled`](Self::recover_stalled)). One this node holds
    /// decided is left to the redrive (see [`stalled`](Self::stalled)).
    async fn recovery_sweep(&self) {
        // A node whose journal was rolled back takes no part (#339); one whose
        // journal is not confirmed yet asks again, and does nothing else.
        if self.rolled_back() {
            return;
        }
        if !self.confirmed() {
            self.ask_to_confirm().await;
            return;
        }
        // A node told to rejoin holds state its peers compacted past: it
        // recovers, repairs and gossips nothing until its host rejoins it.
        // Nor does a node still joining: it holds part of its state, and a
        // report it gossips would end its contact's hold on compaction — but
        // one a layout transition gives slots copies them meanwhile, and so
        // joins. It keeps learning the decided epochs: a new node hears of
        // the change that adds it from a commit, which a lost frame takes
        // away for good.
        if self.rejoin_required() {
            return;
        }
        if !self.bootstrapped.load(Ordering::Acquire) {
            if self.topology.dynamic() {
                self.fetch_metadata().await;
                // What it holds for slots it does not own goes here too: a
                // pre-seed for a change that never came (#373) would
                // otherwise stand in the way of its join, which needs an
                // empty store.
                self.gc_lost_slots().await;
            }
            self.copy_if_gaining().await;
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
            // anti-entropy if missed, or its decision learned from its
            // holders if never witnessed here (#330).
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
            if self.stalled(txn, timeout) {
                self.recover_stalled(txn).await;
            }
        }
        // A dependency this replica never witnessed (#330): anti-entropy
        // imports it only once it is applied somewhere. But where it is held,
        // it may wait on this very replica — on the read of an earlier
        // transaction this replica holds blocked on it — and there it is
        // committed and blocked, so its holders never recover it, and its
        // Commit never comes. Nothing would ever apply. So its decision is
        // learned from its holders: a few per sweep, the oldest first, with
        // anti-entropy, which brings in what is applied, in between.
        let missing = self
            .replica
            .lock()
            .expect("replica poisoned")
            .missing_deps(timeout);
        for dep in missing.into_iter().take(MISSING_DEPENDENCY_LOOKUPS) {
            self.anti_entropy().await;
            // Imported by that round, or committed here meanwhile.
            if self.command_status(dep).is_some() {
                continue;
            }
            if let Err(err) = self.learn_decision(dep).await {
                tracing::debug!(node = self.id.0, ?dep, %err, "cannot learn a missing dependency");
            }
        }
        self.anti_entropy().await;
        self.advance_watermark().await;
        self.metadata_sweep().await;
    }

    /// Whether `txn`'s coordinator is presumed gone, so that this node takes
    /// the transaction over: it began before `timeout`, `recovery_timeout`
    /// ago, or its coordinator is a peer the failure detector suspects.
    /// Never one this node holds [decided](Replica::decided), such as a
    /// replayed write its store refused (#351): only its apply is left,
    /// which the redrive retries on every sweep, and a recovery decides
    /// nothing more.
    fn stalled(&self, txn: TxnId, timeout: Timestamp) -> bool {
        if self.replica.lock().expect("replica poisoned").decided(txn) {
            return false;
        }
        let coordinator = txn.0.node;
        txn.0 < timeout
            || (coordinator != self.id
                && self.failure_detector.suspect(
                    coordinator,
                    self.settings.phi_threshold,
                    self.settings.acceptable_pause,
                ))
    }

    /// Recovers `txn`, a transaction the sweep found stalled, and first what
    /// its read turns out to wait on (#326).
    ///
    /// Once committed, a transaction is read on each replica only after
    /// everything it depends on is applied there, or committed to execute
    /// after it. A dependency can still be stalled itself: its coordinator
    /// gone with it undecided, or read and never applied. And it is not
    /// always recovered first: the sweep goes by `t0`, and a transaction
    /// whose `t0` the slow path raised depends on others above its own.
    /// Waiting for such a read, a recovery would only end at
    /// `collect_timeout`, one transaction at a time, while what it waits on
    /// stays stalled.
    ///
    /// So a sweep recovery gives up a read this replica holds back on a
    /// stalled transaction ([`ReadWaitsOn`]), and that one is taken over
    /// first: recovered if held here, its decision learned from its holders
    /// if never witnessed here (see [`learn_decision`](Self::learn_decision)).
    /// Then the recovery that waited is made again. Each transaction is
    /// followed once per chain, so a cycle ends; a chain longer than
    /// [`STALLED_CHAIN_DEPTH`], or recoveries past
    /// [`STALLED_CHAIN_ATTEMPTS`], wait for the next pass.
    async fn recover_stalled(&self, txn: TxnId) {
        let mut chain = vec![txn];
        let mut followed = HashSet::from([txn]);
        for _ in 0..STALLED_CHAIN_ATTEMPTS {
            let Some(&next) = chain.last() else {
                break;
            };
            // A recovery can wait out `collect_timeout`; anti-entropy (paced
            // on its own interval) must not wait for them all, or a replica
            // that fell behind never gets back what it missed.
            self.anti_entropy().await;
            let waits_on = match self.command_status(next) {
                // Applied meanwhile: by another recovery, or imported.
                Some(Status::Applied) => None,
                // Decided meanwhile: only its apply is left (see `stalled`).
                Some(_) if self.replica.lock().expect("replica poisoned").decided(next) => None,
                Some(_) => {
                    self.metrics.record_recovery();
                    tracing::debug!(node = self.id.0, txn = ?next, "recovering stalled transaction");
                    match self.recover_driven(next, Driver::Sweep).await {
                        Ok(_) => None,
                        Err(err) => {
                            tracing::debug!(node = self.id.0, txn = ?next, %err, "cannot recover a stalled transaction");
                            err.downcast_ref::<ReadWaitsOn>().map(|waits| waits.0)
                        }
                    }
                }
                // A dependency never witnessed here.
                None if next != txn
                    && !self
                        .replica
                        .lock()
                        .expect("replica poisoned")
                        .compacted(next) =>
                {
                    if let Err(err) = self.learn_decision(next).await {
                        tracing::debug!(node = self.id.0, dep = ?next, %err, "cannot learn a stalled dependency");
                    }
                    None
                }
                // Compacted meanwhile: applied here, and on its way out.
                None => None,
            };
            match waits_on {
                Some(dep) if chain.len() < STALLED_CHAIN_DEPTH && followed.insert(dep) => {
                    chain.push(dep);
                }
                _ => {
                    chain.pop();
                }
            }
        }
    }

    /// The transaction this replica holds `txn`'s read back on, if it is
    /// stalled ([`stalled`](Self::stalled)) and not in this node's hands.
    /// Past a dependency decided here, never stalled itself, it is what that
    /// one waits on ([`Replica::undecided_blocker`]).
    fn stalled_dependency(&self, txn: TxnId) -> Option<TxnId> {
        let dep = self
            .replica
            .lock()
            .expect("replica poisoned")
            .undecided_blocker(txn)?;
        if self
            .pending
            .lock()
            .expect("pending poisoned")
            .contains_key(&dep)
        {
            return None;
        }
        let timeout = Timestamp {
            micros: self
                .clock
                .now()
                .micros
                .saturating_sub(self.settings.recovery_timeout.as_micros() as u64),
            logical: 0,
            node: NodeId(0),
        };
        self.stalled(dep, timeout).then_some(dep)
    }

    /// Waits until this replica holds `txn`'s read back on a stalled
    /// transaction, and returns it; `None` at `deadline`. Checked every
    /// `fast_timeout`, the time a replica that can read takes to answer.
    async fn stalled_dependency_by(&self, txn: TxnId, deadline: Instant) -> Option<TxnId> {
        loop {
            let next = Instant::now() + self.settings.fast_timeout;
            if next >= deadline {
                return None;
            }
            tokio::time::sleep_until(next).await;
            if let Some(dep) = self.stalled_dependency(txn) {
                return Some(dep);
            }
        }
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

    /// Metadata anti-entropy: pulls any entries above the installed epoch from
    /// a rotating peer. Covers a node that missed the latest epochs entirely
    /// (no local gap to detect) as well as one holding an uninstallable future
    /// entry; the peer replies only if it has something newer.
    async fn fetch_metadata(&self) {
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
    /// 3. **Activation** — once the layout transition in progress has drained,
    ///    decide the entry that installs its layout (the distinguished proposer
    ///    first), whether or not the node that decided the transition is alive.
    ///
    /// A no-op for static topologies (no epochs are ever decided), so a
    /// fixed-membership cluster issues no control-plane traffic.
    async fn metadata_sweep(&self) {
        if !self.topology.dynamic() {
            return;
        }

        // (1) Metadata anti-entropy.
        self.fetch_metadata().await;

        // (1a) The slots a change took from this node go, once it installed.
        self.gc_lost_slots().await;

        // (1b) A node that lost its journal reads the other acceptors' state
        // back before it votes in config Paxos again (#338); retried each
        // sweep until a majority of the voting ones answered.
        if self
            .amnesia
            .lock()
            .expect("amnesia poisoned")
            .is_some_and(|fence| fence.joined && !fence.votes_restored)
        {
            self.restore_config_votes().await;
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

        // (3) Activation: a transition in progress that drained installs its
        // layout, even if the node that decided it is gone.
        self.sweep_transition().await;
    }

    /// Anti-entropy repair: pull any committed transactions a rotating peer of
    /// this node's shard has that this node missed (e.g. while it was briefly down). Recovery alone
    /// can't fix this — a transaction that completed on a quorum without this
    /// node is "stuck" nowhere — so a healed node converges via this sweep.
    ///
    /// The peer answers with a page of what this node misses, within its
    /// snapshot page budget (see [`export_missing`](Self::export_missing)).
    /// While a page brings something in, the round asks the same peer for
    /// the next at once, up to [`ANTI_ENTROPY_PAGES`] pages and for one
    /// `anti_entropy_interval`: at one page a round, a replica back from an
    /// absence took a round for every page it missed (#348).
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
        let started = Instant::now();
        for _ in 0..ANTI_ENTROPY_PAGES {
            // The digest lets the peer answer with only the commands this
            // node is missing, instead of its full applied set every page.
            let known = {
                let replica = self.replica.lock().expect("replica poisoned");
                let (since, txns) = replica.applied_digest();
                SyncKnown { since, txns }
            };
            match self
                .import_from(peer, self.settings.anti_entropy_interval, Some(known))
                .await
            {
                Ok(imported) if imported.commands > 0 && !imported.stopped => {}
                _ => break,
            }
            // A slow link or disk must not hold the sweep's recoveries back
            // for more than about a round.
            if started.elapsed() >= self.settings.anti_entropy_interval {
                break;
            }
        }
    }

    /// Records `peer`'s proven sync coverage after a successful sync round:
    /// every transaction `peer` had applied below `coverage` is now in this
    /// node's store. Max-merged; the map is reset when another layout goes
    /// live, since coverage accrued under an old layout says nothing about the
    /// keys this node owns under the new one.
    fn record_sync_coverage(&self, peer: NodeId, coverage: Timestamp) {
        let epoch = self.live_since();
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
        let epoch = self.live_since();
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
            Some(proof) if proof.epoch == self.live_since() => Some(coverage.max(proof.vouched)),
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
        if proof.epoch != self.live_since() {
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
        // The live layout the report is made, and the reports counted, under.
        let live_since = self.live_since();
        // The disjoint replica set this node belongs to.
        let mut all_nodes = self.topology.nodes();
        let shard_peers = self.shard_peers();
        let (mine, applied) = self.report(&shard_peers);
        self.give_up_on_silent_peers(&shard_peers);

        // Watermarks go to EVERY peer, not just shard peers: compaction only
        // consumes same-shard reports, but the message doubles as the periodic
        // heartbeat the phi-accrual failure detector needs — without it a quiet
        // cross-shard coordinator would be falsely suspected between writes.
        // During a layout transition, to the nodes of the layout it installs
        // too: a node gaining slots picks the replica it copies them from by
        // these reports and fences, and reports its copy to the others.
        if let Some(transition) = self
            .reconfig
            .lock()
            .expect("reconfig poisoned")
            .transition
            .as_ref()
        {
            for node in transition.pending.nodes() {
                if !all_nodes.contains(&node) {
                    all_nodes.push(node);
                }
            }
        }
        let peers: Vec<NodeId> = all_nodes
            .iter()
            .copied()
            .filter(|&n| n != self.id)
            .collect();
        // The marks its peers reported ride the same flush (#339).
        self.stage_peer_marks(&all_nodes).await;
        // A peer may compact up to `mine` past what this node does not hold,
        // trusting its vote fence, and past what it imported: make both
        // survive a restart first.
        if !self.vouch_durably().await {
            return;
        }
        // A fence not journaled yet is retried here, and reported once it is.
        self.persist_fences().await;
        let fenced = self.reportable_fence();
        let copied = self.copied_report();
        self.broadcast(
            &peers,
            Message::Watermark {
                applied_through: mine,
                applied,
                mark: self.durable_mark(),
                fenced,
                copied,
                live_since,
            },
        )
        .await;

        let watermark = {
            let mut absent = self.absent.lock().expect("absent poisoned");
            let mut reports = self.peer_watermarks.lock().expect("watermarks poisoned");
            // Evict reports from nodes no longer in the topology, or a removed
            // peer's last (stale) watermark would hold compaction back forever.
            // A peer merely silent keeps its last applied-through: what it
            // held unapplied then may still need a vote (see `applied_floor`).
            reports
                .applied_under(live_since)
                .retain(|node, _| all_nodes.contains(node));
            let watermarks = reports.under(live_since);
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
        // Below its floor a write counts as applied, and anti-entropy ships
        // none. A node the transition in progress gives slots holds, of them,
        // every write up to the drain point once it copied them, and none
        // after the activation, which it may install late: it compacts no
        // further than the drain point, and not at all before that is known.
        // A node in no shard of its live layout (dropped) compacts nothing.
        let ceiling = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            match reconfig
                .transition
                .as_ref()
                .filter(|t| t.gainers.contains(&self.id))
            {
                Some(transition) => reconfig::drain_point(transition, &reconfig.reported),
                None if shard_peers.is_empty() => None,
                None => watermark,
            }
        };
        if let Some(watermark) = watermark.zip(ceiling).map(|(w, c)| w.min(c)) {
            self.compact(watermark).await;
        }
        self.prune_decisions().await;
    }

    /// The nodes of this node's shard, itself included (none while it is in
    /// no shard), read from one layout.
    fn shard_peers(&self) -> Vec<NodeId> {
        let layout = self.topology.layout();
        layout
            .node_shard(self.id)
            .map(|shard| layout.replicas_of(shard).to_vec())
            .unwrap_or_default()
    }

    /// The redundancy point this node vouches for: its applied-through lagged
    /// by the compaction margin, clamped to its proven coverage, to the
    /// oldest transaction it is still coordinating (see
    /// [`advance_watermark`](Self::advance_watermark)) and to what it vouched
    /// for a joiner it is still serving ([`JoinHold`]) — and its
    /// applied-through unclamped, which its report gossips too (the shard's
    /// [applied floor](Self::applied_floor) is drawn from those). Takes the
    /// hold lock under the replica lock, never held while the replica lock
    /// is taken.
    ///
    /// Raises the vote fence, in the same critical section, to the report
    /// and to what this node may hand out until its next one: the stable
    /// point its subscriptions read and the sync coverage it exports, which
    /// are capped at the fence once it is journaled (#337). A vote decided
    /// after this lock is released sees the raised fence, so none slips in
    /// below the report. The caller makes the fence durable
    /// ([`vouch_durably`](Self::vouch_durably)) before the report leaves.
    fn report(&self, shard_peers: &[NodeId]) -> (Timestamp, Timestamp) {
        let gossiped = self.gossiped_applied();
        let cutoff = self.margin_cutoff();
        let mut replica = self.replica.lock().expect("replica poisoned");
        let applied = replica.applied_through(cutoff);
        let vouched = self.vouch_locked(&replica, shard_peers);
        // Never past what this node vouched for a joiner still pulling its
        // snapshot (see `JoinHold`); a lapsed hold is dropped here.
        let lapse = self.settings.collect_timeout * JOIN_HOLD_PAGE_INTERVALS;
        let mine = {
            let mut holds = self.join_holds.lock().expect("join holds poisoned");
            holds.retain(|_, hold| hold.refreshed.elapsed() < lapse);
            holds
                .values()
                .map(|hold| hold.vouched)
                .fold(vouched, Timestamp::min)
        };
        let stable = Timestamp {
            micros: self.stable_locked(&replica, cutoff, shard_peers),
            logical: 0,
            node: NodeId(0),
        };
        let exported = applied.min(self.applied_floor(applied, shard_peers, &gossiped));
        replica.raise_vote_fence(vouched.max(stable).max(exported));
        (mine, applied)
    }

    /// What this node vouches it holds, over an already-locked replica, so
    /// a join header vouches for exactly the state it exports: its
    /// [`report`](Self::report) before the holds it keeps for joiners. A
    /// hold keeps the peers from compacting past a joiner's first report;
    /// it says nothing of what this node holds. Capped by the hold kept for
    /// a joiner's last join, the vouch for its next could never rise above
    /// the last one, and a joiner vouched for below a peer's bound would be
    /// told to rejoin on every rejoin (#349). Takes the coverage,
    /// join-proof and coordinating locks under the replica lock — none of
    /// them is ever held while the replica lock is taken.
    fn vouch_locked(&self, replica: &Replica, shard_peers: &[NodeId]) -> Timestamp {
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
        match self
            .coordinating
            .lock()
            .expect("coordinating poisoned")
            .first()
        {
            Some(oldest) if oldest.0 < mine => oldest.0,
            _ => mine,
        }
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
        let live_since = self.live_since();
        let mut absent = self.absent.lock().expect("absent poisoned");
        let mut reports = self.peer_watermarks.lock().expect("watermarks poisoned");
        let watermarks = reports.under(live_since);
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
            // A join is how a peer behind its bound gets back: always served
            // — and the decided epochs it may need first, for a vouch to
            // count under the epoch it installed.
            Message::JoinHeaderRequest { .. }
            | Message::CommandPageRequest { .. }
            | Message::SnapshotPageRequest { .. }
            | Message::RangePageRequest { .. }
            | Message::MetadataFetch { .. }
            | Message::JoinKeepalive
            | Message::Preseed { .. }
            | Message::Preseeded { .. } => return Screen::Pass,
            // Its recovery of a transaction it alone holds gathers the vote
            // fences that let it invalidate one no one else can commit (see
            // `Node::never_commits`); answered, never counted as a sign the
            // peer is back.
            Message::BeginRecovery { .. } => return Screen::Pass,
            Message::Watermark {
                applied_through, ..
            } => match state.rejoin_above {
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
                self.reinstated.bump();
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
        let (mine, applied) = self.report(&self.shard_peers());
        if mine > above {
            tracing::debug!(
                node = self.id.0,
                peer = from.0,
                ?above,
                ?mine,
                "told to rejoin but holding everything compacted; reporting instead"
            );
            if !self.vouch_durably().await {
                return;
            }
            self.send(
                from,
                Message::Watermark {
                    applied_through: mine,
                    applied,
                    mark: self.durable_mark(),
                    fenced: self.reportable_fence(),
                    copied: self.copied_report(),
                    live_since: self.live_since(),
                },
            )
            .await;
            return;
        }
        tracing::error!(
            node = self.id.0,
            peer = from.0,
            ?above,
            ?mine,
            "behind the peers' compaction; this node must rejoin the group from a snapshot"
        );
        self.require_rejoin(from);
    }

    /// Stops this node taking part until its host rejoins it through
    /// `contact`, a peer that compacted past what it holds (see
    /// [`rejoin_contact`](Self::rejoin_contact)).
    fn require_rejoin(&self, contact: NodeId) {
        self.metrics.record_rejoin_required();
        *self.rejoin_from.lock().expect("rejoin poisoned") = Some(contact);
        self.rejoin_required.send_replace(true);
    }

    /// Whether `message` manages the cluster — a join, an anti-entropy round,
    /// a report, config Paxos, the metadata log, a forwarded read — rather
    /// than taking part in a transaction's consensus. A node joining handles
    /// those at once and buffers the rest until it joined.
    fn is_control(message: &Message) -> bool {
        matches!(
            message,
            Message::SyncRequest { .. }
                | Message::SyncData { .. }
                | Message::SnapshotPageRequest { .. }
                | Message::RangePageRequest { .. }
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
                | Message::ConfigRead { .. }
                | Message::ConfigReadReply { .. }
                | Message::MetadataFetch { .. }
                | Message::MetadataEntries { .. }
                | Message::ReadForward { .. }
                | Message::ReadReply { .. }
                | Message::ReadFailed { .. }
                | Message::ReadProbe { .. }
                | Message::ReadProbeOk { .. }
                | Message::Preseed { .. }
                | Message::Preseeded { .. }
                | Message::RangePage { .. }
        )
    }

    /// Answers `from`'s open over a journal holding `mark` (#339): at or
    /// above the highest mark this node saw from it, the journal lost
    /// nothing it saw, and the open is confirmed; below it, the journal was
    /// rolled back to an older copy. Answered from the marks this node's own
    /// journal kept, whether it is confirmed itself or not: when every node
    /// restarted, they confirm one another.
    fn on_opened(&self, from: NodeId, mark: DurableMark) -> Message {
        let mut marks = self.peer_marks.lock().expect("peer marks poisoned");
        match marks.by_node.get(&from).copied() {
            Some(seen) if !mark.covers(&seen) => {
                tracing::error!(
                    node = self.id.0,
                    peer = from.0,
                    ?mark,
                    ?seen,
                    "a peer opened over a journal holding less than it reported: \
                     an older copy of its journal"
                );
                Message::RolledBack { mark, seen }
            }
            _ => {
                marks.observe(from, mark);
                Message::OpenedOk { mark }
            }
        }
    }

    /// `from` confirmed this node's open over a journal holding `mark`
    /// (#339).
    async fn on_opened_ok(&self, from: NodeId, mark: DurableMark) {
        match self.opening.lock().expect("opening poisoned").as_mut() {
            Some(opening) if opening.mark == mark => {
                opening.confirmed_by.insert(from);
            }
            _ => return,
        }
        self.confirm_if_due().await;
    }

    /// `from` saw `seen` from this node, above the `mark` of its open (#339):
    /// the journal it opened over is an older copy of itself. However late,
    /// the node stops taking part (see [`rolled_back`](Self::rolled_back)).
    fn on_rolled_back(&self, from: NodeId, mark: DurableMark, seen: DurableMark) {
        let opened = self
            .opening
            .lock()
            .expect("opening poisoned")
            .as_ref()
            .map(|opening| opening.mark);
        // An answer to another open: an earlier one of this node's id.
        if opened != Some(mark) || self.rolled_back.send_replace(true) {
            return;
        }
        self.metrics.record_journal_rolled_back();
        tracing::error!(
            node = self.id.0,
            peer = from.0,
            ?mark,
            ?seen,
            "a peer saw more of this node's journal than it holds: an older copy of it. \
             This node takes no part any more; open it again over an empty journal and \
             store, joining"
        );
    }

    /// Asks every other node to confirm this node's open (#339), unless it
    /// is confirmed already — or now is (see [`confirmed`](Self::confirmed)).
    async fn ask_to_confirm(&self) {
        self.confirm_if_due().await;
        if self.confirmed() || self.rolled_back() {
            return;
        }
        let Some(mark) = self
            .opening
            .lock()
            .expect("opening poisoned")
            .as_ref()
            .map(|opening| opening.mark)
        else {
            return;
        };
        let others: Vec<NodeId> = self
            .topology
            .nodes()
            .into_iter()
            .filter(|&node| node != self.id)
            .collect();
        self.broadcast(&others, Message::Opened { mark }).await;
    }

    /// Confirms this node's open if enough nodes confirmed it (see
    /// [`confirmed`](Self::confirmed)), and handles what it buffered
    /// meanwhile — unless it still joins, whose join does.
    async fn confirm_if_due(&self) {
        if self.confirmed() || self.rolled_back() {
            return;
        }
        let due = self
            .opening
            .lock()
            .expect("opening poisoned")
            .as_ref()
            .is_some_and(|opening| self.open_confirmed(opening));
        if !due {
            return;
        }
        // Flag flip and drain under the buffer lock (see `handle_staged`).
        let buffered = {
            let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
            self.confirmed.store(true, Ordering::Release);
            if self.bootstrapped.load(Ordering::Acquire) {
                std::mem::take(&mut *buffer)
            } else {
                Vec::new()
            }
        };
        tracing::info!(
            node = self.id.0,
            "the other nodes confirmed this node's journal"
        );
        for env in buffered {
            Box::pin(self.handle(env)).await;
        }
    }

    /// Whether `opening` is confirmed: a slow quorum of this node's shard,
    /// itself included, confirmed it, and every other node did or
    /// `collect_timeout` passed — time for one that saw a higher mark to
    /// say so.
    fn open_confirmed(&self, opening: &Opening) -> bool {
        let layout = self.topology.layout();
        let quorum = layout.node_shard(self.id).is_none_or(|shard| {
            let confirmed = layout
                .replicas_of(shard)
                .iter()
                .filter(|&&node| node != self.id && opening.confirmed_by.contains(&node))
                .count();
            confirmed + 1 >= layout.slow_quorum_of(shard)
        });
        quorum
            && (opening.since.elapsed() >= self.settings.collect_timeout
                || self
                    .topology
                    .nodes()
                    .iter()
                    .all(|&node| node == self.id || opening.confirmed_by.contains(&node)))
    }

    /// Waits, for at most [`NodeConfig::amnesia_join_timeout`], until this
    /// node's open is confirmed (#339), asking again meanwhile.
    async fn await_confirmed(&self) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.settings.amnesia_join_timeout;
        loop {
            if self.rolled_back() {
                return Err(JournalRolledBack.into());
            }
            if self.confirmed() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(OpenUnconfirmed.into());
            }
            self.ask_to_confirm().await;
            tokio::time::sleep(self.settings.recovery_interval).await;
        }
    }

    /// Stages the marks this node saw from its peers (#339), if any rose
    /// since they were last staged, for the next journal flush. Nodes that
    /// left the topology are forgotten first.
    async fn stage_peer_marks(&self, nodes: &[NodeId]) {
        let marks = {
            let mut marks = self.peer_marks.lock().expect("peer marks poisoned");
            let before = marks.by_node.len();
            marks.by_node.retain(|node, _| nodes.contains(node));
            if marks.by_node.len() != before {
                marks.unsaved = true;
            }
            if !marks.unsaved {
                return;
            }
            marks.unsaved = false;
            let mut kept: Vec<(NodeId, DurableMark)> = marks
                .by_node
                .iter()
                .map(|(node, mark)| (*node, *mark))
                .collect();
            kept.sort_unstable_by_key(|(node, _)| *node);
            kept
        };
        if let Err(err) = self.journal.save_peer_marks(&marks).await {
            tracing::warn!(node = self.id.0, %err, "cannot stage the peers' marks");
            self.peer_marks.lock().expect("peer marks poisoned").unsaved = true;
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

        // Its journal rolled back to an older copy (#339): this node takes no
        // part any more. A forwarded read is still refused out loud.
        if self.rolled_back() {
            if let Message::ReadForward { id, .. } = env.message {
                return vec![(
                    env.from,
                    Message::ReadFailed {
                        id,
                        reason: "the owner's journal was rolled back".into(),
                    },
                )];
            }
            return Vec::new();
        }
        // The open handshake (#339) is answered and heard whatever else this
        // node is doing.
        match env.message {
            Message::Opened { mark } => return vec![(env.from, self.on_opened(env.from, mark))],
            Message::OpenedOk { mark } => {
                self.on_opened_ok(env.from, mark).await;
                return Vec::new();
            }
            Message::RolledBack { mark, seen } => {
                self.on_rolled_back(env.from, mark, seen);
                return Vec::new();
            }
            _ => {}
        }
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
        // Its journal not confirmed yet (#339): a consensus message waits for
        // the confirmation, the rest — which their senders repeat — are
        // dropped. A forwarded read is refused out loud, and a report still
        // tells the sender's mark.
        if !self.confirmed() {
            match &env.message {
                Message::ReadForward { id, .. } => {
                    return vec![(
                        env.from,
                        Message::ReadFailed {
                            id: *id,
                            reason: "the owner's journal is not confirmed yet".into(),
                        },
                    )];
                }
                Message::Watermark { mark, .. } => self
                    .peer_marks
                    .lock()
                    .expect("peer marks poisoned")
                    .observe(env.from, *mark),
                _ => {}
            }
            if Self::is_control(&env.message) {
                return Vec::new();
            }
            // Flag check and push under one lock, as for a join (see below).
            let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
            if !self.confirmed() {
                buffer.push(env);
                return Vec::new();
            }
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
        let mut joiner_answers = false;
        if !Self::is_control(&env.message) {
            // A node joining over a lost journal answers a recovery of a
            // transaction below its fence at once: it holds nothing of it,
            // and never votes for it (see `Message::Amnesiac`). Silence
            // would leave a transaction only it could rule out undecided —
            // and the contact's report, which the join waits on, below it.
            if let Some(answer) = self.amnesiac_answer(&env.message) {
                return vec![(env.from, answer)];
            }
            // And one of a transaction it does not hold, above the fence or
            // over a kept journal, as a late replica would (#390): handled
            // below, not buffered — a vote excepted, which waits for the join.
            joiner_answers = self.joiner_answers(&env.message);
            if !joiner_answers {
                // Flag check and push under one lock: `join` flips the flag
                // and drains the buffer while holding it, so a message can
                // never slip into the buffer after the drain (it would be
                // lost forever).
                let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
                if !self.bootstrapped.load(Ordering::Acquire) {
                    buffer.push(env);
                    return Vec::new();
                }
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
            Message::PreAccept {
                txn,
                epoch,
                keys,
                events,
            } => {
                if self.future_dated(txn) {
                    return out;
                }
                let Some(layout) = self.layout_for(epoch, from, &mut out) else {
                    return out;
                };
                let touched = route.clone().unwrap_or_else(|| keys.clone());
                if self.retired(txn, &layout, &touched) {
                    return out;
                }
                // Only handle the keys/events this node owns, under the
                // layout the transaction was planned under.
                let carried = !keys.is_empty();
                let keys = self.owned_keys(&layout, keys);
                if self.not_a_replica_of(txn, carried, &keys) {
                    return out;
                }
                let events = self.owned_events(&layout, events);
                let vote = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    if self
                        .change_refusal(&replica, txn, epoch, &touched)
                        .is_some()
                    {
                        return out;
                    }
                    // Read under the lock a report raises the vote fence
                    // under: a vote decided here comes after it, or before
                    // it was cut.
                    replica.set_coordinator_cutoff(self.coordinator_cutoff());
                    replica.preaccept(txn, epoch, keys, events)
                };
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
                    Err(Refusal::Fenced) => self.metrics.record_vote_fenced(),
                    Err(Refusal::Late) => self.metrics.record_vote_late(),
                }
            }
            Message::Accept {
                txn,
                ballot,
                epoch,
                execute_at,
                deps,
                keys,
            } => {
                if self.future_dated(txn) {
                    return out;
                }
                // A slow-path `execute_at` can exceed `t0`; witness it too.
                self.clock.witness(execute_at);
                let Some(layout) = self.layout_for(epoch, from, &mut out) else {
                    return out;
                };
                let touched = route.clone().unwrap_or_else(|| keys.clone());
                let carried = !keys.is_empty();
                let keys = self.owned_keys(&layout, keys);
                if self.not_a_replica_of(txn, carried, &keys) {
                    return out;
                }
                let reply = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    if self
                        .change_refusal(&replica, txn, epoch, &touched)
                        .is_some()
                    {
                        return out;
                    }
                    // The original coordinator's own Accept is held to its
                    // deadline; a recovery's, which decided to commit, to the
                    // vote fence alone.
                    replica.set_coordinator_cutoff(self.coordinator_cutoff());
                    replica.accept(txn, ballot, epoch, execute_at, deps, keys)
                };
                if reply == Err(Refusal::Compacted) {
                    return out;
                }
                if reply == Err(Refusal::Fenced) {
                    self.metrics.record_vote_fenced();
                    return out;
                }
                if reply == Err(Refusal::Late) {
                    self.metrics.record_vote_late();
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
                    Err(Refusal::Compacted | Refusal::Fenced | Refusal::Late) => {}
                }
            }
            Message::Commit {
                txn,
                epoch,
                execute_at,
                deps,
                keys,
                events,
                reply_to,
            } => {
                self.clock.witness(execute_at);
                let Some(layout) = self.layout_for(epoch, from, &mut out) else {
                    return out;
                };
                // The coordinator ships this shard's keys explicitly, so a
                // replica that missed PreAccept still indexes this transaction
                // in its conflict graph. The owned filters, under the layout
                // the transaction was planned under, stay as defense against
                // a misrouted Commit.
                let carried = !keys.is_empty();
                let keys = self.owned_keys(&layout, keys);
                if self.not_a_replica_of(txn, carried, &keys) {
                    return out;
                }
                let events = self.owned_events(&layout, events);
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .commit(txn, epoch, execute_at, deps, events, keys, reply_to);
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
                if self.future_dated(txn) {
                    return out;
                }
                // Nothing to pre-accept, so no layout to judge by: a known
                // command keeps its own epoch.
                let reply = self.replica.lock().expect("replica poisoned").recover(
                    txn,
                    ballot,
                    self.topology.epoch(),
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
                        epoch: state.epoch,
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
                epoch,
                keys,
                events,
                round,
            } => {
                if self.future_dated(txn) {
                    return out;
                }
                let Some(layout) = self.layout_for(epoch, from, &mut out) else {
                    return out;
                };
                let touched = route.clone().unwrap_or_else(|| keys.clone());
                let keys = self.owned_keys(&layout, keys);
                let events = self.owned_events(&layout, events);
                let reply = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    // A layout change's fence: no pre-accept either, and the
                    // answer it makes ("never witnessed", for good) is a
                    // promise only a journaled fence keeps across a restart.
                    // Handed none of its keys, a replica pre-accepts nothing
                    // whatever the fence.
                    let refused = (!keys.is_empty())
                        .then(|| self.change_refusal(&replica, txn, epoch, &touched))
                        .flatten();
                    match refused {
                        Some(false) => return out,
                        Some(true) => replica.refuse_blind(txn, ballot),
                        None => {
                            // Past the original coordinator's deadline, a
                            // transaction this replica does not hold gets no
                            // vote on a first round: the recovery, once it
                            // heard every reply, asks for one or for a
                            // refusal (#337, `settle_late`) — a routed one
                            // handing it its shard's part (#377).
                            replica.set_coordinator_cutoff(self.coordinator_cutoff());
                            if joiner_answers {
                                // Still joining, this node answers for a
                                // transaction it does not hold as a late
                                // replica (#390).
                                replica.recover_round_joining(txn, ballot, round)
                            } else {
                                replica.recover_round(
                                    txn,
                                    ballot,
                                    epoch,
                                    keys,
                                    events,
                                    round,
                                    route.is_some(),
                                )
                            }
                        }
                    }
                };
                // A recovery is told "never witnessed" only below the vote
                // fence, or once refused for good: an answer a recovery counts
                // as this replica's refusal for good, so the fence it rests on
                // must survive a restart — journaled with this batch, before
                // the answer leaves (`stage_vote_fence`), and under the clock
                // bound a journal that keeps no fence restarts it from
                // (`bound_clock`).
                let fenced = matches!(reply, Ok(RoutedRecovery::NotWitnessed { redundant: false }));
                if fenced {
                    self.metrics.record_vote_fenced();
                    if !self.bound_clock().await || self.stage_vote_fence().await.is_none() {
                        return out;
                    }
                }
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
                            epoch: state.epoch,
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
                    Ok(RoutedRecovery::Late) => Message::Late { txn, ballot },
                    Err(promised) => Message::Nack { txn, promised },
                };
                out.push((from, message));
            }
            Message::Invalidate { txn } => {
                let epoch = self.topology.epoch();
                let invalidated = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .invalidate(txn, epoch);
                if invalidated {
                    tracing::debug!(node = self.id.0, ?txn, "invalidated a transaction");
                    self.applied.bump();
                    // No reply rides on it; the next stage of `txn` retries.
                    self.journal_stage(txn).await;
                    // Its dependents may be free to execute now.
                    out.extend(self.drive_staged().await);
                }
            }
            // Anti-entropy: a peer asks for the applied commands it lacks.
            Message::SyncRequest { id, known } => {
                // A node still joining holds only part of the state: with
                // nothing un-applied yet, its coverage would be its whole
                // margin-lagged clock, vouching for transactions it never
                // witnessed. Staying silent sends the peer elsewhere.
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                // Nor does a node the requester is no shard peer of under
                // this node's layout: dropped from the layout, or moved to
                // another shard, it compacts without the requester's
                // reports, and may have compacted away what the requester
                // lacks — which its coverage would then vouch for.
                let layout = self.topology.layout();
                if layout
                    .node_shard(self.id)
                    .is_none_or(|shard| layout.node_shard(from) != Some(shard))
                {
                    return out;
                }
                // Under one live layout: one that goes live meanwhile leaves
                // the requester to ask again.
                let live_since = self.live_since();
                let (watermark, coverage, commands) = self.export_missing(&known);
                if self.live_since() != live_since {
                    return out;
                }
                // The coverage vouches for what a peer may compact past.
                if !self.bound_clock().await {
                    return out;
                }
                out.push((
                    from,
                    Message::SyncData {
                        id,
                        watermark,
                        coverage,
                        live_since,
                        commands,
                    },
                ));
            }
            // The join header: no commands — the joiner pulls them page by
            // page next, then the snapshot, so a backlog and a store of any
            // size fit the frame cap. A node still joining holds only part
            // of the state: serving it would bootstrap the joiner into the
            // same gap, so it stays silent and the joiner tries its next
            // contact.
            Message::JoinHeaderRequest { id } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                let (watermark, coverage, epoch, vouched) = self.export_join_header();
                // The coverage and the vouch let peers compact past them.
                if !self.bound_clock().await {
                    return out;
                }
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
                        live_since: epoch,
                        commands: Vec::new(),
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
            // A node gaining slots in a transition copies them through this
            // one: its store's next page, holding only those slots' events.
            // At most `RANGE_PAGE_SCANS` store pages are read for one reply,
            // so sparse slots cannot hold a request past its timeout; a reply
            // may then come back empty, with the cursor to go on from.
            Message::RangePageRequest {
                id,
                slots,
                after,
                since,
            } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                // Read before the page: every transaction stamped below it
                // was applied here when the page was read, so a copy that
                // runs while writes flow may resume from it (#373).
                let stable = self.stable_micros();
                let mut wanted = vec![false; crate::api::SLOTS];
                for slot in slots {
                    if let Some(wanted) = wanted.get_mut(slot as usize) {
                        *wanted = true;
                    }
                }
                let mut after = after;
                let mut events = Vec::new();
                let mut bytes = 0usize;
                let mut next = None;
                for _ in 0..RANGE_PAGE_SCANS {
                    let page = match self
                        .datastore
                        .snapshot_page_since(
                            after.take(),
                            since,
                            self.settings.snapshot_page_events,
                            self.settings.snapshot_page_bytes,
                        )
                        .await
                    {
                        Ok(page) => page,
                        // Not answered: the copier asks again.
                        Err(err) => {
                            tracing::warn!(node = self.id.0, from = from.0, %err, "cannot serve a range page");
                            return out;
                        }
                    };
                    for event in page.events {
                        if wanted[slot_of(&Key::of(&event))] {
                            bytes += snapshot_event_size(&event);
                            events.push(event);
                        }
                    }
                    next = page.next;
                    if next.is_none()
                        || events.len() >= self.settings.snapshot_page_events as usize
                        || bytes >= self.settings.snapshot_page_bytes
                    {
                        break;
                    }
                    after = next.clone();
                }
                self.metrics.record_snapshot_page();
                out.push((
                    from,
                    Message::RangePage {
                        id,
                        events,
                        next,
                        stable: Some(stable),
                    },
                ));
            }
            // A node the change gives slots copies them now, while writes
            // still flow (#373), off the inbox: a copy takes as long as the
            // range is large. Answered when it ends, done or not.
            Message::Preseed {
                id,
                live_epoch,
                map,
            } => {
                let node = self.clone();
                tokio::spawn(async move {
                    if node.installed_epoch() == live_epoch {
                        node.preseed(map).await;
                    }
                    node.send(from, Message::Preseeded { id }).await;
                });
            }
            Message::Preseeded { id } => {
                let tx = self
                    .snapshot_pending
                    .lock()
                    .expect("snapshot poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(Message::Preseeded { id });
                }
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
            page @ (Message::SnapshotPage { .. }
            | Message::CommandPage { .. }
            | Message::RangePage { .. }) => {
                let id = match &page {
                    Message::SnapshotPage { id, .. }
                    | Message::CommandPage { id, .. }
                    | Message::RangePage { id, .. } => *id,
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
                live_since,
                commands,
            } => {
                let tx = self
                    .sync_inbox
                    .lock()
                    .expect("sync poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send((watermark, coverage, live_since, commands));
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
            Message::Watermark {
                applied_through,
                applied,
                mark,
                fenced,
                copied,
                live_since,
            } => {
                // What the sender's journal holds, which it must hold again
                // when it reopens (#339).
                self.peer_marks
                    .lock()
                    .expect("peer marks poisoned")
                    .observe(from, mark);
                // A joiner's first report ends the hold its contact kept for
                // it (see `JoinHold`): from here on it is counted itself.
                self.join_holds
                    .lock()
                    .expect("join holds poisoned")
                    .remove(&from);
                // Counted under this node's live layout only: one made under
                // another vouches for other keys — a gainer's, before the
                // activation, for none.
                let mine = self.live_since();
                if live_since == mine {
                    let mut reports = self.peer_watermarks.lock().expect("watermarks poisoned");
                    reports.under(mine).insert(from, applied_through);
                    reports.applied_under(mine).insert(from, applied);
                }
                let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
                if let Some(fenced) = fenced {
                    reconfig.reported.insert(from, fenced);
                }
                if let Some(copied) = copied {
                    reconfig.copies.insert(from, copied);
                }
            }
            // Config Paxos — acceptor side.
            Message::ConfigPrepare { epoch, ballot } => {
                if self.abstains_from_config() {
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
                        let (accepted_ballot, accepted_entry) = match &acc.accepted {
                            Some((b, e)) => (*b, Some(e.clone())),
                            None => (ConfigAcceptor::default().promised, None),
                        };
                        (
                            Message::ConfigPromise {
                                epoch,
                                ballot,
                                accepted_ballot,
                                accepted_entry,
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
                entry,
            } => {
                if self.abstains_from_config() {
                    return out;
                }
                // Never vote for a layout that would break the cluster, so a
                // majority of current nodes can never choose one (an older
                // proposer validates nothing).
                if let Err(err) = entry.validate() {
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
                        acc.accepted = Some((ballot, entry));
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
            // An acceptor that lost its journal reads this node's state back
            // (#338): everything it holds, and whether it votes itself. Answered
            // while joining too (a control message), never while unconfirmed.
            Message::ConfigRead { id } => {
                let acceptors: Vec<(u64, AcceptorRecord)> = self
                    .config
                    .lock()
                    .expect("config poisoned")
                    .iter()
                    .map(|(epoch, acc)| (*epoch, acc.to_record()))
                    .collect();
                out.push((
                    from,
                    Message::ConfigReadReply {
                        id,
                        acceptors,
                        voting: self.config_voting(),
                    },
                ));
            }
            reply @ Message::ConfigReadReply { .. } => {
                let Message::ConfigReadReply { id, .. } = &reply else {
                    unreachable!()
                };
                let tx = self
                    .config_read_pending
                    .lock()
                    .expect("config read poisoned")
                    .get(id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send((from, reply));
                }
            }
            Message::ConfigCommit { epoch, entry } => {
                // Checked before the append: the log keeps the first entry per
                // epoch, so a persisted invalid one would shut out the real one.
                if let Err(err) = entry.validate() {
                    tracing::warn!(node = self.id.0, epoch, %err, "ignoring an invalid layout");
                    return out;
                }
                // Append to the durable log, then install every now-contiguous
                // epoch. If this commit arrived before an intervening epoch, ask the
                // sender (which decided it, so holds the full prefix) to fill the gap.
                // An epoch the log did not take is not installed (see
                // `persist_metadata`); the metadata sweep fetches it again.
                if !self.persist_metadata(&[(epoch, entry.clone())]).await {
                    return out;
                }
                let installed = self.ingest_entry(epoch, entry);
                self.persist_installed(installed).await;
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
                    while let Some(entry) = log.entries.get(&next) {
                        entries.push((next, entry.clone()));
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
                    .position(|(_, entry)| entry.validate().is_err())
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
                let mut installed = Installed::default();
                for (epoch, entry) in entries {
                    installed
                        .invalidated
                        .extend(self.ingest_entry(epoch, entry).invalidated);
                }
                self.persist_installed(installed).await;
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
                tenant,
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
                // A key this node does not own under the layout it installed
                // is not here: a change took it away and its slot was
                // dropped (or is about to be). A requester behind on the
                // layout gets a failure, not a page that reads as "no such
                // events".
                let filters: Option<Arc<[EventFilter]>> = aggregators.as_deref().map(Arc::from);
                let routed = crate::executor::target_key(&filters, &routing_key, &tenant);
                if let Some(key) = routed.filter(|key| !self.topology.owns(self.id, key)) {
                    let epoch = self.topology.epoch();
                    tracing::debug!(
                        node = self.id.0,
                        from = from.0,
                        ?key,
                        epoch,
                        "a forwarded read of a key this node does not own"
                    );
                    out.push((
                        from,
                        Message::ReadFailed {
                            id,
                            reason: format!("not an owner of the key under epoch {epoch}"),
                        },
                    ));
                    return out;
                }
                let reply = match self
                    .datastore
                    .read(
                        aggregators.map(Arc::from),
                        routing_key,
                        tenant,
                        args,
                        to_micros,
                    )
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
            Message::ReadProbe { txn, epoch, key } => {
                if !self.bootstrapped.load(Ordering::Acquire) {
                    return out;
                }
                // A coordinator that probed under a layout retired since for
                // `key` counts a quorum of replicas that no longer all hold it.
                let Some(layout) = self.layout_for(epoch, from, &mut out) else {
                    return out;
                };
                if self.retired(txn, &layout, std::slice::from_ref(&key)) {
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
                // Journaled first (#331): from the mark below on, a report may
                // count it as applied, and a restart that lost its record would
                // bring it back committed, its decision lost with it —
                // undecidable once its peers compacted it. Staged ahead of the
                // store, which keeps the store and the mark adjacent: a record
                // ahead of a failed apply is replayed into the store on a
                // restart, as any applied record is, and staged again by the
                // retry meanwhile.
                let applied = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .snapshot(apply.txn)
                    .map(|mut state| {
                        state.status = Status::Applied;
                        state.applied_conflict = Some(!apply.commit);
                        state
                    });
                let staged = match applied {
                    Some(state) => match self.journal.stage(&state).await {
                        Ok(()) => true,
                        Err(err) => {
                            tracing::warn!(
                                node = self.id.0,
                                txn = ?apply.txn,
                                %err,
                                "cannot stage a transaction's state; withholding its reply"
                            );
                            false
                        }
                    },
                    None => true,
                };
                // The events carry their writer's provisional stamps; the
                // store gets the agreed ones (see `stamp_events`). The journal
                // keeps the provisional ones — every apply re-derives the same.
                // Only the keys this node owns now land in the store: a key a
                // layout change took from it was dropped, and stays dropped.
                apply.events = self.owned_now(apply.events);
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
                self.applied.bump();
                // An apply the journal did not take is not acknowledged; a
                // restart re-applies it (idempotently) from its commit.
                if staged {
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
                .version(&event.tenant, &event.aggregate_type, &event.aggregate_id)
                .await?;
            if event.version <= current {
                // Its own events already in place, the transaction was
                // applied here before — and forgotten: its journal record
                // lost across a restart, or applied by an import, which is
                // not journaled. Its condition held then; reading its own
                // write as a conflict would vote to abort a decided commit
                // (#309).
                return self.datastore.holds(owned_events).await;
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

    /// Whether this node is no replica of the shard a PreAccept, Accept or
    /// Commit addressed it as: the message `carried` keys, it owns none of
    /// them (`owned`) under the layout it installed, and it holds nothing of
    /// `txn`. A coordinator that planned on an older layout must not count
    /// it, so it casts no vote, answers nothing and records nothing — before,
    /// it recorded an empty command and voted `t0` with no dependencies, a
    /// vote that could complete a quorum of replicas none of which hold the
    /// transaction's keys. A transaction it already holds, witnessed while it
    /// owned them, is finished as before; one below the redundancy watermark
    /// is answered as before (it records nothing, and a decision record
    /// answers a Commit).
    fn not_a_replica_of(&self, txn: TxnId, carried: bool, owned: &[Key]) -> bool {
        if !carried || !owned.is_empty() {
            return false;
        }
        let replica = self.replica.lock().expect("replica poisoned");
        replica.status(txn).is_none() && !replica.compacted(txn)
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
        if self.rolled_back() {
            return Err(JournalRolledBack.into());
        }
        if !self.confirmed() {
            return Err(OpenUnconfirmed.into());
        }
        // A node still joining coordinates nothing (#388): it buffers the
        // replies its coordination would need until its join ends, so the
        // write could not complete, and what it left pre-accepted on a
        // minority of a shard no recovery could decide — it buffers the
        // recovery's question too — while the shard's watermark, which its
        // own join may wait on, stayed pinned below it.
        if self.is_joining() {
            return Err(Joining.into());
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
        // A key moving to other replicas takes no new write until its
        // transition installed the layout it moves to (see `reconfig`).
        if let Some(since) = self.moving(&keys) {
            self.metrics.record_write_topology_changing();
            return Err(TopologyChanging { since }.into());
        }
        // One layout for the whole coordination: an epoch installed meanwhile
        // changes neither the replicas it counts nor those it recovers through.
        let layout = self.topology.layout();
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
        let (outcome, all_fast) = match self.coordinate(&layout, txn, &keys, &events).await {
            Ok(done) => Ok(done),
            Err(err) => self
                .settle(txn, err, layout.epoch(), &keys, &events)
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
        // A node still joining holds part of its store, whatever its probes
        // say: a read here could miss writes committed long ago (as a
        // forwarded read would, which it refuses too).
        anyhow::ensure!(
            self.bootstrapped.load(Ordering::Acquire),
            "this node is joining: its store is partial"
        );
        // One layout for the probe: the replicas it asks are the ones it
        // counts, and a replica that installed a change to `key` since does
        // not answer it (see `retired`).
        let layout = self.topology.layout();
        let replicas = layout.replicas(&key).to_vec();
        let slow_q = layout.slow_quorum_of(layout.shard_of(&key));
        let t0 = self.mint().await?;
        let txn = TxnId(t0);
        let mut rx = self.register(txn);

        self.broadcast(
            &replicas,
            Message::ReadProbe {
                txn,
                epoch: layout.epoch(),
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
        // Woken by every apply (local or imported); the deadline only bounds
        // a genuinely stuck dependency.
        let deadline = Self::after(self.settings.collect_timeout);
        let applied = self
            .applied
            .wait_until(deadline, || {
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .deps_applied(&deps)
            })
            .await;
        if !applied {
            anyhow::bail!("read barrier timed out: {} deps unapplied", deps.len());
        }
        Ok(())
    }

    /// The shared PreAccept → (Accept) → Commit → Read → Apply coordination for a
    /// transaction over `keys` appending `events` (empty for a read barrier).
    /// Returns the outcome and whether it took the one-round fast path.
    async fn coordinate(
        &self,
        layout: &Layout,
        txn: TxnId,
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<(CommitOutcome, bool)> {
        let (plans, _union) = Self::plan(layout, keys, events);
        let route = Self::route_of(&plans, keys);
        let mut rx = self.register(txn);
        let (execute_at, shard_deps, all_fast) =
            self.agree(&mut rx, &plans, route.as_deref(), txn).await?;
        let conflict = self
            .execute(
                &mut rx,
                layout,
                &plans,
                route.as_deref(),
                txn,
                execute_at,
                shard_deps,
                &[],
                Driver::Coordinator,
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
                        epoch: plan.epoch,
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
                            epoch: plan.epoch,
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
    ///
    /// A replica answers the reads and the apply to whoever committed the
    /// transaction last: a coordinator a recovery took over from, or one of
    /// two recoveries racing, may hear none of them. So the reads are waited
    /// for only until this node applies the transaction — another driver
    /// decided it then, final — and, for a recovery, the acks too. A sweep
    /// recovery stops waiting as well once this replica holds the read back on
    /// a stalled transaction (see [`Driver::Sweep`]).
    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        txn: TxnId,
        execute_at: Timestamp,
        shard_deps: HashMap<ShardId, BTreeSet<TxnId>>,
        applied: &[NodeId],
        driver: Driver,
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
            let Some(shard) = layout.node_shard(*n) else {
                continue;
            };
            match skipped.iter_mut().find(|p| p.shard == shard) {
                Some(p) => p.replicas.push(*n),
                None => skipped.push(ShardPlan {
                    epoch: layout.epoch(),
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
        let deadline = Self::after(self.settings.collect_timeout);
        let collect = self.collect_by_shard_tagged(
            rx,
            &read_plans,
            |p, got| got.len() >= p.slow_q && heard_applied(p, got),
            deadline,
            HashSet::new(),
            |m| match m {
                Message::ReadOk { ok, .. } => Some(ok),
                _ => None,
            },
        );
        let decided_here = self
            .applied
            .wait_until(deadline, || self.local_outcome(txn).is_some());
        let held_back = async {
            match driver {
                Driver::Sweep => self.stalled_dependency_by(txn, deadline).await,
                Driver::Coordinator | Driver::Recovery => None,
            }
        };
        // Biased: a replay runs the branches in one order.
        let reads = tokio::select! {
            biased;
            reads = collect => reads,
            true = decided_here => {
                let conflict = self.local_outcome(txn).expect("applied here");
                // Again, for a replica the decision's Apply missed.
                self.broadcast(&union, Message::Apply { txn, commit: !conflict }).await;
                return Ok(conflict);
            }
            Some(dep) = held_back => return Err(ReadWaitsOn(dep).into()),
        };
        for plan in &read_plans {
            let got = &reads[&plan.shard];
            if got.len() < plan.slow_q || !heard_applied(plan, got) {
                anyhow::bail!("read quorum not reached for shard {}", plan.shard);
            }
        }
        // A replica that applied the transaction answers with its decision,
        // which is final: adopt it. ANDing it with the fresh reads would let
        // one replica's read overturn a decided commit — one whose store
        // already holds the transaction's own events, its apply forgotten
        // across a restart, reads them as a conflict — and replicas yet to
        // apply would enact an abort the others committed (#309). Only with
        // no applied replica heard is the decision made here: commit only if
        // every shard's condition holds.
        let decided: Vec<bool> = reads
            .values()
            .flatten()
            .filter(|(from, _)| applied.contains(from))
            .map(|(_, ok)| *ok)
            .collect();
        let commit = match decided.first() {
            Some(&decision) => {
                anyhow::ensure!(
                    decided.iter().all(|&d| d == decision),
                    "replicas applied {txn:?} with different decisions"
                );
                decision
            }
            None => reads.values().flatten().all(|(_, ok)| *ok),
        };

        // Apply: enact the decision and wait for a slow quorum per shard.
        self.broadcast(&union, Message::Apply { txn, commit }).await;
        let deadline = Self::after(self.settings.collect_timeout);
        let collect = self.collect_by_shard(
            rx,
            plans,
            |p| p.slow_q,
            deadline,
            |m| match m {
                Message::Applied { .. } => Some(()),
                _ => None,
            },
        );
        let applied_here = async {
            match driver {
                Driver::Recovery | Driver::Sweep => {
                    self.applied
                        .wait_until(deadline, || self.local_outcome(txn).is_some())
                        .await
                }
                Driver::Coordinator => false,
            }
        };
        let acks = tokio::select! {
            biased;
            acks = collect => acks,
            true = applied_here => return Ok(!commit),
        };
        for plan in plans {
            if acks[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("apply quorum not reached for shard {}", plan.shard);
            }
        }

        // Read-your-writes: if this coordinator is itself a replica, wait for its
        // own local apply so a subsequent read on this node sees the write.
        if union.contains(&self.id) {
            let deadline = Self::after(self.settings.collect_timeout);
            self.applied
                .wait_until(deadline, || {
                    self.replica
                        .lock()
                        .expect("replica poisoned")
                        .applied_result(txn)
                        .is_some()
                })
                .await;
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
                        epoch: plan.epoch,
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
        epoch: u64,
        keys: &[Key],
        events: &[Event],
    ) -> anyhow::Result<CommitOutcome> {
        self.deregister(txn);
        if let Some(conflict) = self.local_outcome(txn) {
            return Ok(CommitOutcome { txn, conflict });
        }
        match self
            .recover_with(txn, epoch, keys.to_vec(), events.to_vec(), Driver::Recovery)
            .await
        {
            Ok(outcome) => {
                tracing::debug!(node = self.id.0, ?txn, error = %failure, "write settled by recovery");
                return Ok(outcome);
            }
            Err(err) => {
                tracing::debug!(node = self.id.0, ?txn, %err, "cannot recover a failed write");
            }
        }
        let deadline = Self::after(self.settings.collect_timeout);
        let mut outcome = None;
        self.applied
            .wait_until(deadline, || {
                outcome = self.local_outcome(txn);
                outcome.is_some()
            })
            .await;
        match outcome {
            Some(conflict) => {
                tracing::debug!(node = self.id.0, ?txn, error = %failure, "write settled by another node");
                Ok(CommitOutcome { txn, conflict })
            }
            None => Err(failure),
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
        self.recover_driven(txn, Driver::Recovery).await
    }

    /// [`recover`](Self::recover), driven by `driver`.
    async fn recover_driven(&self, txn: TxnId, driver: Driver) -> anyhow::Result<CommitOutcome> {
        // What this node holds of the transaction (its owned part) is what it
        // can hand to replicas that never witnessed it.
        // It is recovered under the layout it was planned under.
        let (epoch, keys, events) = self
            .replica
            .lock()
            .expect("replica poisoned")
            .snapshot(txn)
            .map(|cmd| (cmd.epoch, cmd.keys, cmd.events))
            .unwrap_or_else(|| (self.topology.epoch(), Vec::new(), Vec::new()));
        self.recover_with(txn, epoch, keys, events, driver).await
    }

    /// Learns the decision of `txn`, a transaction this replica never
    /// witnessed but a command committed here waits on (#330), and commits it
    /// here as its holders did: a decision is final, so one shard peer's
    /// committed report carries it whole — its execution timestamp,
    /// dependencies, keys and events. The peers are asked as a recovery asks
    /// them, under a fresh ballot, with nothing to pre-accept: one that never
    /// witnessed it only promises the ballot. Returns whether a decision was
    /// learned. A transaction no peer reports committed is left to the
    /// replicas holding it, which recover it; one invalidated is applied
    /// there, so anti-entropy brings it in.
    ///
    /// One no replica heard from holds at all was held only by a replica
    /// that lost its journal since (#337): nobody recovers it, and what waits
    /// on it would wait for good. The dependency was scoped to this node's
    /// shard, so its replicas answer for it there without its keys
    /// ([`RecoveryRound::Dependency`]); if, with those past the deadline
    /// refusing it ([`settle_late`](Self::settle_late)), they prove it never
    /// committed ([`never_commits`](Self::never_commits)), every replica of
    /// the shard records it aborted, held or not, and what waits on it goes
    /// on.
    async fn learn_decision(&self, txn: TxnId) -> anyhow::Result<bool> {
        let ballot = Ballot(self.mint().await?);
        let layout = self.topology.layout();
        let peers = self.shard_peers();
        let mut rx = self.register(txn);
        // With nothing to pre-accept, the epoch only has to be one the peers
        // hold; each report carries the epoch its holder planned under.
        self.broadcast(
            &peers,
            Message::BeginRecovery {
                txn,
                ballot,
                epoch: layout.epoch(),
                keys: Vec::new(),
                events: Vec::new(),
                round: RecoveryRound::Dependency,
            },
        )
        .await;
        let mut reports = Self::collect_tagged(
            &mut rx,
            peers.len(),
            Self::after(self.settings.fast_timeout),
            |m| Self::recover_report(m, ballot),
        )
        .await;
        let decided = reports.iter().find_map(|(_, report)| match report {
            RecoverResp::Ok(f) if f.status >= Status::Committed && f.known && !f.invalidated() => {
                Some(f.clone())
            }
            _ => None,
        });
        let Some(f) = decided else {
            let orphaned = self
                .invalidate_orphan(&mut rx, &layout, txn, ballot, &peers, &mut reports)
                .await;
            self.deregister(txn);
            return Ok(orphaned);
        };
        self.deregister(txn);
        self.metrics.record_dependency_learned();
        tracing::debug!(
            node = self.id.0,
            ?txn,
            execute_at = ?f.execute_at,
            "learned the decision of a dependency never witnessed here"
        );
        // Handled as any Commit is, by the inbox loop: journaled, then
        // executed when ready. Its read is this node's own: a recovery of
        // `txn` later gets it from the recorded read.
        let commit = Message::Commit {
            txn,
            epoch: f.epoch,
            execute_at: f.execute_at,
            deps: f.deps,
            keys: f.keys,
            events: f.events,
            reply_to: self.id,
        };
        self.send(self.id, Self::routed(f.route.as_deref(), commit))
            .await;
        Ok(true)
    }

    /// The part of [`learn_decision`](Self::learn_decision) for a dependency
    /// `txn` no shard peer heard from (`reports`) holds: invalidates it on
    /// every one of them, `peers`, if they prove it never committed. Returns
    /// whether it was.
    async fn invalidate_orphan(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        txn: TxnId,
        ballot: Ballot,
        peers: &[NodeId],
        reports: &mut Vec<(NodeId, RecoverResp)>,
    ) -> bool {
        if reports
            .iter()
            .any(|(_, r)| matches!(r, RecoverResp::Ok(f) if f.known))
        {
            // A holder recovers it.
            return false;
        }
        let Some(shard) = layout.node_shard(self.id) else {
            return false;
        };
        let plan = ShardPlan {
            epoch: layout.epoch(),
            shard,
            replicas: peers.to_vec(),
            electorate: layout.electorate_of(shard).to_vec(),
            fast_q: layout.fast_quorum_of(shard),
            slow_q: layout.slow_quorum_of(shard),
            keys: Vec::new(),
            events: Vec::new(),
        };
        let plans = [plan];
        // Even refused by those past the deadline, it may have committed:
        // nothing to do without its keys.
        if !self.never_commits(&plans, reports, true)
            || self
                .settle_late(rx, layout, txn, ballot, &plans, &[], &[], reports, None)
                .await
                .is_some()
            || !self.never_commits(&plans, reports, false)
        {
            return false;
        }
        tracing::debug!(
            node = self.id.0,
            ?txn,
            "invalidating a dependency no replica holds, which never committed"
        );
        self.broadcast(peers, Message::Invalidate { txn }).await;
        true
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
                epoch,
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
                epoch,
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
                epoch,
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
                epoch,
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
            Message::Amnesiac { ballot: b, .. } if b == ballot => Some(RecoverResp::Amnesiac),
            Message::Late { ballot: b, .. } if b == ballot => Some(RecoverResp::Late),
            // A replica refuses a ballot only for a higher one it promised:
            // a Nack naming one at or below this attempt's answers an
            // earlier attempt late, and refuses nothing here (#441).
            Message::Nack { promised, .. } if promised > ballot => {
                Some(RecoverResp::Nack(promised))
            }
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
        epoch: u64,
        keys: Vec<Key>,
        events: Vec<Event>,
        driver: Driver,
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

        // Planned, counted and decided under the layout the transaction was
        // coordinated under: its replicas then are those that voted for it,
        // whatever this node installed since (#288).
        let Some(layout) = self.topology.layout_at(epoch) else {
            anyhow::bail!("no layout for epoch {epoch} to recover {txn:?} under");
        };
        let mut route = self
            .replica
            .lock()
            .expect("replica poisoned")
            .route(txn)
            .or_else(|| Self::spans_shards(&layout, &keys).then(|| keys.clone()));
        let mut ballot = Ballot(self.mint().await?);

        for _attempt in 0..3 {
            let mut rx = self.register(txn);
            let attempt = match &route {
                Some(route) => {
                    self.recover_routed(
                        &mut rx, &layout, txn, ballot, route, &keys, &events, driver,
                    )
                    .await
                }
                None => {
                    self.recover_unrouted(&mut rx, &layout, txn, ballot, &keys, &events, driver)
                        .await
                }
            };
            // Deregistered on failure too: a leaked response channel makes the
            // sweep take this node's own transaction for locally in flight,
            // and never recover it.
            self.deregister(txn);
            let what = match &attempt {
                Ok(Attempt::Done(o)) => format!("done conflict={}", o.conflict),
                Ok(Attempt::Retry(p)) => format!("retry promised={p:?}"),
                Ok(Attempt::Route(_)) => "route".to_string(),
                Err(e) => format!("err {e:#}"),
            };
            tracing::trace!(node = self.id.0, ?txn, ?ballot, ?driver, %what, "recovery attempt");
            match attempt? {
                Attempt::Done(outcome) => return Ok(outcome),
                // Outbid by another node's recovery: that one holds the
                // higher ballot, and decides the transaction. Outbidding
                // it back would have it outbid this one again, and two
                // sweeps in step would go on like that for ever (#441).
                // Its ballot is witnessed, so a later recovery here, should
                // that one not finish, outbids it on its first attempt.
                // (The transaction's own ballot, its coordinator's, is no
                // recovery's: a mint below it is outbid at once.)
                Attempt::Retry(promised)
                    if promised.0.node != self.id && promised != Ballot(txn.0) =>
                {
                    self.clock.witness(promised.0);
                    anyhow::bail!(
                        "recovery of {txn:?} outbid by node {}'s, under {promised:?}",
                        promised.0.node.0
                    );
                }
                // Outbid by a ballot this node minted, an earlier attempt's
                // promised late: retry above it.
                Attempt::Retry(promised) => ballot = self.higher_ballot(promised).await?,
                // The replicas already promised `ballot`: ask again under it.
                Attempt::Route(found) => route = Some(found),
            }
        }

        anyhow::bail!("recovery exhausted its ballot retries")
    }

    /// One recovery attempt for a transaction without a known route: the
    /// touched shards are reassembled from the reports.
    #[allow(clippy::too_many_arguments)]
    async fn recover_unrouted(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        txn: TxnId,
        ballot: Ballot,
        keys: &[Key],
        events: &[Event],
        driver: Driver,
    ) -> anyhow::Result<Attempt> {
        let all_nodes = layout.nodes();
        // Every replica gets what this node holds: one that never
        // witnessed the transaction pre-accepts it under this ballot, so
        // its vote counts here and the original coordinator, its PreAccept
        // possibly still in flight, can no longer win it there.
        self.broadcast(
            &all_nodes,
            Message::BeginRecovery {
                txn,
                ballot,
                epoch: layout.epoch(),
                keys: keys.to_vec(),
                events: events.to_vec(),
                round: RecoveryRound::Ask,
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
            .filter(|route| Self::spans_shards(layout, route))
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
                    .any(|k| !keys.contains(k) && layout.owns(node, k))
            })
            .collect();
        if !blind.is_empty() {
            self.broadcast(
                &blind,
                Message::BeginRecovery {
                    txn,
                    ballot,
                    epoch: layout.epoch(),
                    keys: all_keys.clone(),
                    events: all_events.clone(),
                    round: RecoveryRound::Ask,
                },
            )
            .await;
            let again =
                Self::collect_asked(rx, &blind, Self::after(self.settings.fast_timeout), ballot)
                    .await;
            if let Some(promised) = Self::nack_in(&again) {
                return Ok(Attempt::Retry(promised));
            }
            Self::merge_reports(&mut resp, again);
        }

        let (plans, union) = Self::plan(layout, &all_keys, &all_events);
        // Replicas past the original coordinator's deadline that never
        // witnessed it said so, and neither voted nor refused: ask them to
        // refuse it for good if that proves it never committed, or to vote
        // for it if it may have (#337).
        if let Some(promised) = self
            .settle_late(
                rx,
                layout,
                txn,
                ballot,
                &plans,
                &all_keys,
                &all_events,
                &mut resp,
                None,
            )
            .await
        {
            return Ok(Attempt::Retry(promised));
        }
        if self.never_commits(&plans, &resp, false) {
            tracing::debug!(
                node = self.id.0,
                ?txn,
                "invalidating a transaction its replicas' vote fences rule out"
            );
            self.broadcast(&union, Message::Invalidate { txn }).await;
            return Ok(Attempt::Done(CommitOutcome {
                txn,
                conflict: true,
            }));
        }
        if let Some(contact) = self.stranded_below(txn, &plans, &resp) {
            tracing::error!(
                node = self.id.0,
                peer = contact.0,
                ?txn,
                "holding a transaction a peer compacted past, which no recovery can decide; \
                 this node must rejoin the group from a snapshot"
            );
            self.require_rejoin(contact);
            anyhow::bail!("{txn:?} lies below a peer's compaction: this node must rejoin");
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
        // An invalidated report: a recovery decided it was never committed,
        // and this replica's report shows it. Finish that recovery's job on
        // any replica that missed its Invalidate, rather than retry for ever.
        if known.iter().any(|(_, f)| f.invalidated()) {
            self.broadcast(&union, Message::Invalidate { txn }).await;
            return Ok(Attempt::Done(CommitOutcome {
                txn,
                conflict: true,
            }));
        }

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
                .filter(|(from, _)| plan.replicas.contains(from))
                .count();
            if heard < plan.slow_q {
                anyhow::bail!("recovery quorum not reached for shard {}", plan.shard);
            }
        }
        self.drive_recovery(rx, layout, txn, ballot, &plans, None, known, driver)
            .await
    }

    /// Asks the replicas that answered [`Late`](RecoverResp::Late) — past
    /// the original coordinator's deadline, never witnessing `txn` — to
    /// settle (#337), and puts their answers in `resp`. Were they to refuse
    /// it for good, would the reports prove it never committed
    /// ([`never_commits`](Self::never_commits))? Then they are asked to
    /// refuse ([`RecoveryRound::Refuse`]), and the recovery invalidates it at
    /// once, as it did when every replica past the deadline refused. If the
    /// reports show it may have committed instead
    /// ([`may_have_committed`](Self::may_have_committed)) — a replica that
    /// accepted it lost its journal — their votes may be what deciding it
    /// takes: they are asked to vote ([`RecoveryRound::Vote`]), which each
    /// does unless it vouched past it. Either way they are asked with every
    /// key and event the reports revealed — wrapped in `route` for a
    /// multi-shard transaction (#377), each replica taking its shard's
    /// part; one whose shard's part no holder reported has nothing to vote
    /// for, and is not asked to. Otherwise what the replicas not heard from
    /// hold decides it, and a next attempt hears them: the late ones are
    /// asked nothing, so a write too few replicas hold is not committed on
    /// their votes. The promise a Nack names, if one came back.
    #[allow(clippy::too_many_arguments)]
    async fn settle_late(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        txn: TxnId,
        ballot: Ballot,
        plans: &[ShardPlan],
        keys: &[Key],
        events: &[Event],
        resp: &mut Vec<(NodeId, RecoverResp)>,
        route: Option<&[Key]>,
    ) -> Option<Ballot> {
        let mut late: Vec<NodeId> = resp
            .iter()
            .filter(|(_, r)| matches!(r, RecoverResp::Late))
            .map(|(from, _)| *from)
            .collect();
        if late.is_empty() {
            return None;
        }
        let round = if self.never_commits(plans, resp, true) {
            RecoveryRound::Refuse
        } else if self.may_have_committed(plans, resp) {
            RecoveryRound::Vote
        } else {
            return None;
        };
        if round == RecoveryRound::Vote {
            let (parts, _) = Self::plan(layout, keys, events);
            late.retain(|node| {
                parts
                    .iter()
                    .any(|part| part.replicas.contains(node) && !part.events.is_empty())
            });
            if late.is_empty() {
                return None;
            }
        }
        tracing::debug!(
            node = self.id.0,
            ?txn,
            ?round,
            late = late.len(),
            "settling replicas past the deadline"
        );
        self.broadcast(
            &late,
            Self::routed(
                route,
                Message::BeginRecovery {
                    txn,
                    ballot,
                    epoch: layout.epoch(),
                    keys: keys.to_vec(),
                    events: events.to_vec(),
                    round,
                },
            ),
        )
        .await;
        // A routed round crosses shards: the slow-path budget, as its
        // reports had.
        let window = if route.is_some() {
            self.settings.collect_timeout
        } else {
            self.settings.fast_timeout
        };
        let again = Self::collect_asked(rx, &late, Self::after(window), ballot).await;
        if let Some(promised) = Self::nack_in(&again) {
            return Some(promised);
        }
        Self::merge_reports(resp, again);
        None
    }

    /// Collects the reports answering `ballot` until every node in `asked`
    /// answered, or `deadline` passed: one per responder, a straggler's to
    /// an earlier round of the same attempt included (the first round may
    /// have ended before it answered; counting it as an answer to this one
    /// would leave the answer asked for unread).
    async fn collect_asked(
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        asked: &[NodeId],
        deadline: Instant,
        ballot: Ballot,
    ) -> Vec<(NodeId, RecoverResp)> {
        let mut out: Vec<(NodeId, RecoverResp)> = Vec::new();
        let mut pending: HashSet<NodeId> = asked.iter().copied().collect();
        while !pending.is_empty() {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, message))) => {
                    if let Some(report) = Self::recover_report(message, ballot) {
                        if out.iter().all(|(n, _)| *n != from) {
                            pending.remove(&from);
                            out.push((from, report));
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        out
    }

    /// Puts a later round's reports in `resp`: a responder's newest answer
    /// replaces its earlier one, a straggler's is added.
    fn merge_reports(resp: &mut Vec<(NodeId, RecoverResp)>, again: Vec<(NodeId, RecoverResp)>) {
        for (from, report) in again {
            match resp.iter_mut().find(|(n, _)| *n == from) {
                Some(slot) => slot.1 = report,
                None => resp.push((from, report)),
            }
        }
    }

    /// Whether the reports show `txn` may have committed: in every shard it
    /// touches (a commit needs a quorum of each), the replicas that
    /// accepted it make a slow quorum, or the electors that pre-accepted it
    /// a fast one — a replica that lost its journal counted as one that did
    /// ([`RecoverResp::Amnesiac`]). One past the deadline never witnessed
    /// it, one that promised the ballot holding nothing neither, and one
    /// not heard from is heard on a next attempt: none counts.
    fn may_have_committed(&self, plans: &[ShardPlan], resp: &[(NodeId, RecoverResp)]) -> bool {
        plans.iter().all(|plan| {
            let (mut accepted, mut electors) = (0, 0);
            for (from, report) in resp {
                if !plan.replicas.contains(from) {
                    continue;
                }
                let (accepts, holds) = match report {
                    RecoverResp::Ok(f) if f.known => (f.status >= Status::Accepted, true),
                    RecoverResp::Amnesiac => (true, true),
                    _ => continue,
                };
                accepted += usize::from(accepts);
                electors += usize::from(holds && plan.electorate.contains(from));
            }
            accepted >= plan.slow_q || electors >= plan.fast_q
        })
    }

    /// Whether an unrouted recovery's reports prove `txn` never committed and
    /// never will (#310), so it can be invalidated. A replica that answers
    /// "never witnessed" to an unrouted recovery does so only below its vote
    /// fence (see [`Replica::fenced`]): it never voted for the transaction,
    /// and never will. Per shard of `slow_q` out of `n`:
    ///
    /// - **More than `n − slow_q` fenced:** every commit needs `slow_q`
    ///   voters, and too few replicas are left that ever could vote.
    /// - **A replica compacted past it, and more than `n − slow_q` fenced or
    ///   holding it uncommitted:** the compacted replica's floor rests on a
    ///   counted set `S` of at least `slow_q` replicas, each of which reported
    ///   above the transaction's `t0`. A replica holding it uncommitted
    ///   reported below it, so it is not in `S`. With too few replicas
    ///   outside the fenced and the holders to contain `S`, a fenced replica
    ///   is in `S`. A member of `S` that never held the transaction proves it
    ///   had no voter in any majority, or that member's sync coverage would
    ///   have brought it in. So it never committed. This rests on every
    ///   replica fencing its votes, so it needs
    ///   [`NodeConfig::fence_invalidation`].
    ///
    /// - **A replica that lost its journal answered, and no replica compacted
    ///   past it** (#312, [`Message::Amnesiac`]): that replica may have
    ///   voted for the transaction before the loss, but never votes for it
    ///   again, nor does a fenced one. Counting every replica not heard from
    ///   as one that may have voted, and may vote: if too few may have
    ///   accepted it for a slow commit, too few of the electorate may have
    ///   pre-accepted it for a fast one, and too few replicas are left that
    ///   could vote for it, it never committed and never will. A holder that
    ///   accepted it may hold half a decision, so it counts among those that
    ///   accepted; one that only pre-accepted it promised this recovery's
    ///   ballot, so the original coordinator can no longer complete an
    ///   accept there. Needs [`NodeConfig::fence_invalidation`] too.
    ///
    /// A report of the transaction committed anywhere rules all of them out:
    /// a decision exists.
    ///
    /// A replica that answered [`Late`](RecoverResp::Late) neither voted nor
    /// refused yet: it counts as one that may vote, unless `late_refuse` —
    /// what the reports would prove were it to refuse for good (see
    /// [`settle_late`](Self::settle_late)).
    fn never_commits(
        &self,
        plans: &[ShardPlan],
        resp: &[(NodeId, RecoverResp)],
        late_refuse: bool,
    ) -> bool {
        if resp.iter().any(
            |(_, r)| matches!(r, RecoverResp::Ok(f) if f.known && f.status >= Status::Committed),
        ) {
            return false;
        }
        plans.iter().any(|plan| {
            let (mut fenced, mut redundant, mut holders) = (0, 0, 0);
            let (mut accepted, mut amnesiac) = (0, 0);
            // The same, among the fast-path electorate.
            let (mut answered_e, mut holders_e, mut amnesiac_e) = (0, 0, 0);
            for (from, report) in resp {
                if !plan.replicas.contains(from) {
                    continue;
                }
                let elector = plan.electorate.contains(from);
                match report {
                    RecoverResp::NotWitnessed { redundant: false } => fenced += 1,
                    RecoverResp::Late if late_refuse => fenced += 1,
                    RecoverResp::NotWitnessed { redundant: true } => redundant += 1,
                    RecoverResp::Ok(f) if f.known => {
                        holders += 1;
                        holders_e += usize::from(elector);
                        accepted += usize::from(f.status >= Status::Accepted);
                    }
                    RecoverResp::Amnesiac => {
                        amnesiac += 1;
                        amnesiac_e += usize::from(elector);
                    }
                    _ => continue,
                }
                answered_e += usize::from(elector);
            }
            let spare = plan.replicas.len() - plan.slow_q;
            // Not heard from, or no answer that says anything of it: may
            // have voted, may vote.
            let silent = plan
                .replicas
                .len()
                .saturating_sub(fenced + redundant + holders + amnesiac);
            let silent_e = plan.electorate.len().saturating_sub(answered_e);
            let amnesia_rules_it_out = amnesiac > 0
                && redundant == 0
                && accepted + amnesiac + silent < plan.slow_q
                && holders_e + amnesiac_e + silent_e < plan.fast_q
                && holders + silent < plan.slow_q;
            fenced > spare
                || (self.settings.fence_invalidation && redundant > 0 && fenced + holders > spare)
                || (self.settings.fence_invalidation && amnesia_rules_it_out)
        })
    }

    /// Whether this node holds a transaction no recovery can decide again,
    /// because a replica compacted past it (#310): the replica to rejoin
    /// through, if so.
    ///
    /// A replica that reports it from below its floor ("redundant") never
    /// votes for it, nor does a fenced one. With fewer than `slow_q` replicas
    /// of a shard left that could, no recovery gathers the quorum it needs;
    /// and [`never_commits`](Self::never_commits) found no proof it never
    /// committed — the compacted replica may have applied it and dropped it.
    /// Below that replica's floor it was applied on every replica the floor
    /// counted, or can no longer commit (see `Replica::forget_below`). Either
    /// way this node, holding it undecided, is stale. Rejoining through that
    /// replica forgets it, and the snapshot carries its effect if it had one.
    ///
    /// This needs the transaction held here, undecided. A node recovering
    /// only a dependency is not stuck: its own floor follows its peers' past
    /// it. Nor is one holding the transaction [decided](Replica::decided),
    /// such as a replayed write its store refused (#351): the decision its
    /// peers applied before they compacted it is here, and all it lacks is
    /// the apply.
    fn stranded_below(
        &self,
        txn: TxnId,
        plans: &[ShardPlan],
        resp: &[(NodeId, RecoverResp)],
    ) -> Option<NodeId> {
        let held = resp
            .iter()
            .any(|(from, r)| *from == self.id && matches!(r, RecoverResp::Ok(f) if f.known));
        if !held || self.replica.lock().expect("replica poisoned").decided(txn) {
            return None;
        }
        plans.iter().find_map(|plan| {
            let (mut fenced, mut compacted) = (0, None::<(usize, NodeId)>);
            for (from, report) in resp {
                if !plan.replicas.contains(from) {
                    continue;
                }
                match report {
                    RecoverResp::NotWitnessed { redundant: false } => fenced += 1,
                    RecoverResp::NotWitnessed { redundant: true } => {
                        let (count, first) = compacted.unwrap_or((0, *from));
                        compacted = Some((count + 1, first));
                    }
                    _ => {}
                }
            }
            let (redundant, contact) = compacted?;
            (plan.replicas.len() - fenced - redundant < plan.slow_q).then_some(contact)
        })
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
    /// recover it as usual over the route's shards. What it leaves undecided
    /// once the late replicas settled, their vote fences may rule out
    /// ([`never_commits`](Self::never_commits)), as for an unrouted recovery
    /// (#386).
    #[allow(clippy::too_many_arguments)]
    async fn recover_routed(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        txn: TxnId,
        ballot: Ballot,
        route: &[Key],
        keys: &[Key],
        events: &[Event],
        driver: Driver,
    ) -> anyhow::Result<Attempt> {
        let (shards, targets) = Self::plan(layout, route, &[]);
        self.broadcast(
            &targets,
            Self::routed(
                Some(route),
                Message::BeginRecovery {
                    txn,
                    ballot,
                    epoch: layout.epoch(),
                    keys: keys.to_vec(),
                    events: events.to_vec(),
                    round: RecoveryRound::Ask,
                },
            ),
        )
        .await;
        let mut resp = self.collect_route_reports(rx, &shards, ballot).await;
        if let Some(promised) = Self::nack_in(&resp) {
            return Ok(Attempt::Retry(promised));
        }
        let mut verdict = self.route_verdict(&shards, &resp);
        if matches!(verdict, Verdict::Undecided(_)) {
            // Too few holders of some shard answered. The replicas that
            // never witnessed it were handed none of its part — this node
            // holds its own shard's at most — so they promised the ballot
            // and hold nothing, or, past the deadline, are late. Hand each
            // its shard's part, taken from a holder's report, so it votes
            // (#377): the promised ones now, the late ones through the
            // rounds the unrouted recovery settles them with (#337).
            let (_, all_events) = self.reassemble(keys, events, &resp);
            let (parts, _) = Self::plan(layout, route, &all_events);
            let has_part = |node: NodeId| {
                parts
                    .iter()
                    .any(|part| part.replicas.contains(&node) && !part.events.is_empty())
            };
            let promised: Vec<NodeId> = resp
                .iter()
                .filter(|(_, r)| matches!(r, RecoverResp::Ok(f) if !f.known))
                .map(|(from, _)| *from)
                .filter(|&node| has_part(node))
                .collect();
            if !promised.is_empty() {
                self.broadcast(
                    &promised,
                    Self::routed(
                        Some(route),
                        Message::BeginRecovery {
                            txn,
                            ballot,
                            epoch: layout.epoch(),
                            keys: route.to_vec(),
                            events: all_events.clone(),
                            round: RecoveryRound::Ask,
                        },
                    ),
                )
                .await;
                let again = Self::collect_asked(
                    rx,
                    &promised,
                    Self::after(self.settings.collect_timeout),
                    ballot,
                )
                .await;
                if let Some(promised) = Self::nack_in(&again) {
                    return Ok(Attempt::Retry(promised));
                }
                Self::merge_reports(&mut resp, again);
            }
            if let Some(promised) = self
                .settle_late(
                    rx,
                    layout,
                    txn,
                    ballot,
                    &parts,
                    route,
                    &all_events,
                    &mut resp,
                    Some(route),
                )
                .await
            {
                return Ok(Attempt::Retry(promised));
            }
            verdict = self.route_verdict(&shards, &resp);
        }
        // What the verdict leaves undecided, the replicas' vote fences may
        // rule out (#386), as for an unrouted recovery: one refusal meets no
        // quorum of a shard of three by itself, but beside a replica that
        // lost its journal and a holder that never accepted, it proves no
        // commit — the refusal `settle_late` asked for on that very proof.
        if matches!(verdict, Verdict::Undecided(_)) && self.never_commits(&shards, &resp, false) {
            verdict = Verdict::Invalidate;
        }

        let planned = match verdict {
            Verdict::Undecided(why) => anyhow::bail!("recovery of {txn:?} undecided: {why}"),
            Verdict::Invalidate => {
                tracing::debug!(
                    node = self.id.0,
                    ?txn,
                    "invalidating a transaction its route's replicas rule out"
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
            .filter(|k| planned.contains(&layout.shard_of(k)))
            .cloned()
            .collect();
        // The same layout the reports were gathered and counted under.
        let (plans, _union) = Self::plan(layout, &planned_keys, &all_events);
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
                    || Self::planned_shard(&plans, *from).is_some_and(|s| planned.contains(&s))
            })
            .collect();
        if let Some(shard) = Self::unreported_part(&plans, &known) {
            anyhow::bail!(
                "recovery of {txn:?} undecided: no holder of shard {shard} reported its part"
            );
        }
        self.drive_recovery(rx, layout, txn, ballot, &plans, Some(route), known, driver)
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

    /// One route shard's reports, counted by kind — from the replicas the
    /// recovery planned for it (see [`planned_shard`](Self::planned_shard)).
    fn shard_reports(shard: &ShardPlan, resp: &[(NodeId, RecoverResp)]) -> ShardReports {
        let mut counts = ShardReports::default();
        for (from, report) in resp
            .iter()
            .filter(|(from, _)| shard.replicas.contains(from))
        {
            counts.heard += 1;
            match report {
                RecoverResp::Ok(f) if f.known => counts.known += 1,
                RecoverResp::NotWitnessed { redundant: false } => {
                    counts.blind += 1;
                    if shard.electorate.contains(from) {
                        counts.blind_electorate += 1;
                    }
                }
                RecoverResp::NotWitnessed { redundant: true } => counts.redundant += 1,
                RecoverResp::Late => counts.late += 1,
                RecoverResp::Ok(_) => counts.promised += 1,
                _ => {}
            }
        }
        counts
    }

    /// The planned shard whose part no report carried (#377): no holder of
    /// it answered, and its replicas that would vote have nothing to vote
    /// for. A shard a known report shows committed is excepted: the
    /// decision exists, and a replica that applied it carries it.
    fn unreported_part(plans: &[ShardPlan], known: &[(NodeId, RecoverFields)]) -> Option<ShardId> {
        plans
            .iter()
            .find(|plan| {
                plan.events.is_empty()
                    && !known.iter().any(|(from, f)| {
                        plan.replicas.contains(from) && f.status >= Status::Committed
                    })
            })
            .map(|plan| plan.shard)
    }

    /// Whether a routed recovery has heard enough to decide: an invalidation
    /// is already proven (an invalidated report, or replicas of one shard
    /// that never witnessed the transaction ruling out its commit), or every
    /// route shard has a slow quorum of reports, a report from below its
    /// watermark, or has answered in full. A decided transaction one shard
    /// compacted needs a report from a replica that applied it (see
    /// [`route_verdict`](Self::route_verdict)): until one arrives, every
    /// other shard is heard out in full.
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
                .any(|s| Self::shard_reports(s, resp).rule_out_a_commit(s))
        {
            return true;
        }
        let counts: Vec<ShardReports> = shards
            .iter()
            .map(|s| Self::shard_reports(s, resp))
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
    /// 2. Replicas of one shard that **never witnessed** it, each having
    ///    durably promised this ballot, meet every quorum that could commit it
    ///    there ([`ShardReports::rule_out_a_commit`]: a slow quorum of them in
    ///    a shard of odd size, two in a shard of four): it never committed (a
    ///    commit needs a PreAccept quorum on every shard, which would meet
    ///    them), and never will (those replicas refuse the original
    ///    coordinator below the promise, and no recovery can gather a
    ///    witnessed quorum there either). Invalidate.
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
            .map(|s| (s, Self::shard_reports(s, resp)))
            .collect();
        if counts.iter().any(|(s, c)| c.rule_out_a_commit(s)) {
            return Verdict::Invalidate;
        }
        let lacking = |s: &ShardPlan, c: &ShardReports| {
            Verdict::Undecided(format!(
                "shard {}: {} of {} replicas reported it, {} never witnessed it, \
                 {} are late, {} promised it",
                s.shard, c.known, s.slow_q, c.blind, c.late, c.promised
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
    #[allow(clippy::too_many_arguments)]
    async fn drive_recovery(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        layout: &Layout,
        txn: TxnId,
        ballot: Ballot,
        plans: &[ShardPlan],
        route: Option<&[Key]>,
        known: Vec<(NodeId, RecoverFields)>,
        driver: Driver,
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
            if let Some(shard) = Self::planned_shard(plans, *from) {
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
                            epoch: plan.epoch,
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
                        // One below this ballot answers an earlier attempt
                        // (see `recover_report`).
                        Message::Nack { promised, .. } if promised > ballot => Some(Err(promised)),
                        _ => None,
                    },
                )
                .await;
            // Retry above the highest promise any shard reported: one ballot
            // that clears them all, and the same one whatever order the
            // shards are iterated in.
            if let Some(promised) = acks
                .values()
                .flatten()
                .filter_map(|a| a.as_ref().err().copied())
                .max()
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
            .execute(
                rx, layout, plans, route, txn, execute_at, shard_deps, &applied, driver,
            )
            .await?;
        Ok(Attempt::Done(CommitOutcome { txn, conflict }))
    }

    /// Runs only PreAccept and returns the transaction id, modelling a
    /// coordinator that crashed before committing. For tests of
    /// [`recover`](Self::recover).
    pub async fn coordinate_preaccept(&self, events: Vec<Event>) -> anyhow::Result<TxnId> {
        let keys = Self::keys_of(&events);
        let (plans, _union) = Self::plan(&self.topology.layout(), &keys, &events);
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
                        epoch: plan.epoch,
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
        let (plans, _union) = Self::plan(&self.topology.layout(), &keys, &events);
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

    /// Whether this node is still joining: opened with
    /// [`begin_join`](Node::begin_join), and neither joined yet nor holding
    /// the slots a layout transition gave it.
    pub fn is_joining(&self) -> bool {
        !self.bootstrapped.load(Ordering::Acquire)
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
    /// A node rejoining over its kept journal
    /// ([`recover_state_for_join`](Self::recover_state_for_join)) keeps its
    /// votes and promises at or above the contact's floor, drops what it holds
    /// below it ([`Replica::forget_below`]), and applies again, once joined,
    /// what it had applied that the snapshot lacks. If that journal holds the
    /// contact absent past a compaction, the contact must first prove it holds
    /// what was compacted ([`await_reinstated`](Self::await_reinstated)).
    ///
    /// `contact` must be a replica of this node's shard: only it holds what
    /// this node is to copy, and its floor is this node's shard's (#360). The
    /// join waits for the layout that makes this node a replica
    /// ([`await_membership`](Self::await_membership)). A node that already
    /// copied its slots during the layout transition that made it a replica
    /// has nothing to join: `Ok(0)`.
    pub async fn join(&self, contact: NodeId) -> anyhow::Result<usize> {
        // A rejoin over a kept journal waits until the other nodes confirmed
        // that journal (#339).
        self.await_confirmed().await?;
        let (layout, shard) = self.await_membership(contact).await?;
        if self.copied_for_live() {
            return Ok(0);
        }
        anyhow::ensure!(
            layout.node_shard(contact) == Some(shard),
            "{contact:?} is no replica of this node's shard under epoch {}: join through one",
            layout.epoch()
        );
        let bound = self
            .absent
            .lock()
            .expect("absent poisoned")
            .get(&contact)
            .and_then(|state| state.rejoin_above);
        if let Some(bound) = bound {
            self.await_reinstated(contact, bound).await?;
        }
        if let Some(fence) = self.pending_fence() {
            self.await_report_past(contact, fence).await?;
        }
        let imported = self
            .keeping_join_hold(
                contact,
                self.import_from(contact, self.settings.collect_timeout, None),
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
        // Joined past the fence: from here on the node is an ordinary
        // replica, and a restart opens it plainly. The imports are only
        // staged; durable first, so a plain restart never comes back without
        // what the join brought in.
        if self.pending_fence().is_some() {
            if let Err(err) = self.mark_joined().await {
                return Err(self.partial(err, &imported));
            }
        }
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
        let live_since = self.live_since();
        let (mine, applied) = self.report(&self.shard_peers());
        let peers: Vec<NodeId> = self
            .topology
            .nodes()
            .into_iter()
            .filter(|&n| n != self.id)
            .collect();
        if self.vouch_durably().await {
            self.broadcast(
                &peers,
                Message::Watermark {
                    applied_through: mine,
                    applied,
                    mark: self.durable_mark(),
                    fenced: self.reportable_fence(),
                    copied: self.copied_report(),
                    live_since,
                },
            )
            .await;
        }

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
        // A node rejoining over its kept journal applies again what it had
        // applied but the snapshot lacks (see `recover_state_for_join`),
        // without waiting for a message or a sweep to drive it.
        self.redrive().await;

        Ok(imported.commands)
    }

    /// Runs `work`, a step of a join through `contact`, sending the contact a
    /// [`JoinKeepalive`](Message::JoinKeepalive) every `collect_timeout`
    /// meanwhile: its hold on its report lapses after a few of those without
    /// a page request, and a step slower than that (a slow disk, a large
    /// page) would otherwise let peers compact past what it vouched for —
    /// and send this node back to rejoin when it reports.
    /// The [`Message::Amnesiac`] answer of a node joining over a lost
    /// journal to `message`, a recovery of a transaction below its fence;
    /// `None` for anything else, which the join buffers as usual.
    fn amnesiac_answer(&self, message: &Message) -> Option<Message> {
        let fence = self.pending_fence()?;
        let recovery = match message {
            Message::Routed { inner, .. } => inner.as_ref(),
            message => message,
        };
        match *recovery {
            Message::BeginRecovery { txn, ballot, .. } if txn.0 < fence => {
                Some(Message::Amnesiac { txn, ballot })
            }
            _ => None,
        }
    }

    /// Whether this node, still joining, answers `message` — a recovery of a
    /// transaction it does not hold, routed or not — now, rather than
    /// buffering the question until its join ends (#390). It has not voted
    /// for the transaction (a node joining votes on nothing), so it answers
    /// as a replica past the coordinator's deadline does
    /// ([`Replica::recover_round_joining`]): asked, it promises the ballot
    /// and says it is late, which the recovery settles with the shard's
    /// other replicas — a refusal for good where that proves the transaction
    /// never committed, which it gives at once, the PreAccept it may replay
    /// after the join then meeting a fenced vote. Buffered, the question
    /// would wait on the join; the join on the shard's watermark; and the
    /// watermark on that very transaction, or on one whose read waits for
    /// it. A vote is deferred: a `Vote` round is buffered, and pre-accepts
    /// once the snapshot is in, as the buffered PreAccept would — refusing
    /// for good instead would cost the shard a voter it may need, should
    /// another replica lose its journal meanwhile. A transaction this node
    /// holds — a rejoin over a kept journal voted for it — is buffered as
    /// before, and reported once joined. One below an amnesia fence is
    /// answered first (`amnesiac_answer`).
    fn joiner_answers(&self, message: &Message) -> bool {
        if !self.is_joining() {
            return false;
        }
        let recovery = match message {
            Message::Routed { inner, .. } => inner.as_ref(),
            message => message,
        };
        match recovery {
            Message::BeginRecovery { txn, round, .. } if *round != RecoveryRound::Vote => self
                .replica
                .lock()
                .expect("replica poisoned")
                .status(*txn)
                .is_none(),
            _ => false,
        }
    }

    /// Whether this node votes in config Paxos: it never lost a journal
    /// under its node id, or it restored its votes since (#338).
    fn config_voting(&self) -> bool {
        self.amnesia
            .lock()
            .expect("amnesia poisoned")
            .is_none_or(|fence| fence.votes_restored)
    }

    /// Whether this node lost a journal under its node id (#312) and has not
    /// restored its config votes yet (#338): it forgot the config-Paxos
    /// promises and accepts it made, which no fence in time bounds (an
    /// epoch's proposals may span any time), so it neither promises nor
    /// accepts — counted — until it read the other acceptors' state back
    /// ([`restore_config_votes`](Node::restore_config_votes)). It still
    /// learns decided epochs; config changes meanwhile need a majority of
    /// the other acceptors.
    fn abstains_from_config(&self) -> bool {
        if self.config_voting() {
            return false;
        }
        self.metrics.record_config_vote_withheld();
        true
    }

    /// Reads the other acceptors' state back and votes in config Paxos
    /// again (#338): a node that lost its journal forgot every promise and
    /// accept it made. Returns whether it is restored.
    ///
    /// It asks every other acceptor for its whole state, and needs answers
    /// from a majority of **all** the acceptors among those that vote
    /// themselves (one that lost its journal and has not restored its votes
    /// answers `voting: false`, and bounds nothing: counting it would let
    /// two such replicas each read past the other's forgotten promise). Any
    /// majority that promised or accepted a ballot meets those answers in a
    /// sound acceptor, so the merged state
    /// ([`merge_acceptors`](Self::merge_acceptors)) lies above everything
    /// this node forgot as part of a majority — and a vote of its own that
    /// no majority yet shared is covered by **when** it reads: no earlier
    /// than `collect_timeout` past its fence, since a proposer counts no
    /// reply older than that ([`run_config_paxos`](Self::run_config_paxos)),
    /// so every round that counted a reply this node made before the loss
    /// ended before the read, and whatever it accepted is journaled at a
    /// sound acceptor the read meets. The adopted state is journaled before
    /// the fence records the restoration, so a restart keeps it. Retried
    /// by the metadata sweep until a majority of the voting others answer;
    /// a group that lost a majority of its journals at once never restores
    /// (those nodes come back under new ids), nor does one of two nodes.
    async fn restore_config_votes(&self) -> bool {
        let Some(fence) = *self.amnesia.lock().expect("amnesia poisoned") else {
            return true;
        };
        if fence.votes_restored {
            return true;
        }
        if !fence.joined {
            return false;
        }
        let collect_micros = self.settings.collect_timeout.as_micros() as u64;
        if self.clock.physical_micros() < fence.fence_micros.saturating_add(collect_micros) {
            return false;
        }
        let acceptors = self.topology.nodes();
        let need = acceptors.len() / 2 + 1;
        let others: Vec<NodeId> = acceptors
            .iter()
            .copied()
            .filter(|&node| node != self.id)
            .collect();
        if others.len() < need {
            tracing::warn!(
                node = self.id.0,
                acceptors = acceptors.len(),
                "too few other acceptors to restore config votes from: abstaining for good"
            );
            return false;
        }

        let (pending, mut rx) = PendingConfigRead::open(self);
        self.broadcast(&others, Message::ConfigRead { id: pending.id })
            .await;
        let deadline = Self::after(self.settings.collect_timeout);
        // One answer per acceptor (a frame resent over a fresh connection
        // must not fill the majority twice over).
        let mut heard: HashSet<NodeId> = HashSet::new();
        let mut voting: Vec<Vec<(u64, AcceptorRecord)>> = Vec::new();
        let mut abstaining = 0usize;
        while voting.len() < need {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((
                    from,
                    Message::ConfigReadReply {
                        acceptors,
                        voting: votes,
                        ..
                    },
                ))) => {
                    if !others.contains(&from) || !heard.insert(from) {
                        continue;
                    }
                    if votes {
                        voting.push(acceptors);
                    } else {
                        abstaining += 1;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        drop(pending);
        if voting.len() < need {
            tracing::warn!(
                node = self.id.0,
                heard = heard.len(),
                abstaining,
                need,
                "too few voting acceptors answered to restore config votes; retrying"
            );
            return false;
        }

        let merged = Self::merge_acceptors(voting.iter().map(Vec::as_slice));
        for (epoch, read) in merged {
            let record = {
                let mut config = self.config.lock().expect("config poisoned");
                let acc = config.entry(epoch).or_default();
                acc.promised = acc.promised.max(read.promised);
                if let Some((ballot, entry)) = read.accepted {
                    if acc.accepted.as_ref().is_none_or(|(b, _)| ballot > *b) {
                        acc.accepted = Some((ballot, entry));
                    }
                }
                acc.to_record()
            };
            if !self.persist_acceptor(epoch, &record).await {
                return false;
            }
        }
        let restored = AmnesiaFence {
            votes_restored: true,
            ..fence
        };
        if let Err(err) = self.journal.flush().await {
            tracing::warn!(node = self.id.0, %err, "cannot flush the journal before recording the restored config votes");
            return false;
        }
        self.metrics.record_flush();
        if let Err(err) = self.journal.save_amnesia_fence(&restored).await {
            tracing::warn!(node = self.id.0, %err, "cannot record the restored config votes");
            return false;
        }
        *self.amnesia.lock().expect("amnesia poisoned") = Some(restored);
        self.metrics.record_config_votes_restored();
        tracing::info!(
            node = self.id.0,
            read_from = heard.len(),
            "restored the config votes lost with the journal"
        );
        true
    }

    /// Merges the acceptor state read from the other acceptors, per epoch
    /// (#338): the promised ballot is the highest any of them promised or
    /// accepted, and the accepted value is the one accepted under the
    /// highest ballot. An epoch none of them holds state for is absent.
    fn merge_acceptors<'a>(
        answers: impl IntoIterator<Item = &'a [(u64, AcceptorRecord)]>,
    ) -> BTreeMap<u64, AcceptorRecord> {
        let mut merged: BTreeMap<u64, AcceptorRecord> = BTreeMap::new();
        for answer in answers {
            for (epoch, record) in answer {
                let state = merged
                    .entry(*epoch)
                    .or_insert_with(|| ConfigAcceptor::default().to_record());
                state.promised = state.promised.max(record.promised);
                if let Some((ballot, entry)) = &record.accepted {
                    state.promised = state.promised.max(*ballot);
                    if state.accepted.as_ref().is_none_or(|(b, _)| *ballot > *b) {
                        state.accepted = Some((*ballot, entry.clone()));
                    }
                }
            }
        }
        merged
    }

    /// Raises the replica's vote fence to `fence`, for good: a node that
    /// lost its journal never votes for a transaction below it that it does
    /// not hold (#312) — it may have voted for it before, and once joined,
    /// anything below it that committed is in its store. It answers such a
    /// recovery "never witnessed": one that can still commit is held here.
    fn floor_votes_at(&self, fence: AmnesiaFence) {
        let fence = Timestamp {
            micros: fence.fence_micros,
            logical: 0,
            node: NodeId(0),
        };
        let mut replica = self.replica.lock().expect("replica poisoned");
        replica.raise_vote_fence(fence);
        // Journaled with the amnesia fence itself.
        replica.fence_durable(fence);
    }

    /// Asks `contact`, which this node stopped waiting for and compacted past
    /// `bound` without, to prove it holds what was compacted, and waits up to
    /// [`collect_timeout`](NodeConfig::collect_timeout) for the proof: its
    /// report above `bound`, which counts it again
    /// ([`screen_absent`](Self::screen_absent)). Until then, whatever it
    /// answers the join with is answered with [`Message::Rejoin`] instead —
    /// rightly so: this node is about to copy its state, and a contact
    /// lacking a write compacted past it would hand over a store without
    /// it, which no peer holds as a command any more. The `Rejoin` sent here
    /// has it report at once ([`on_rejoin`](Self::on_rejoin)), or rejoin
    /// itself if it cannot; then the attempt fails, and the host tries its
    /// next contact.
    async fn await_reinstated(&self, contact: NodeId, bound: Timestamp) -> anyhow::Result<()> {
        self.send(contact, Message::Rejoin { above: bound }).await;
        let deadline = Instant::now() + self.settings.collect_timeout;
        let proven = self
            .reinstated
            .wait_until(deadline, || {
                !self
                    .absent
                    .lock()
                    .expect("absent poisoned")
                    .contains_key(&contact)
            })
            .await;
        anyhow::ensure!(
            proven,
            "join contact {} reported nothing above {bound:?}, which this node compacted past it",
            contact.0
        );
        Ok(())
    }

    /// Waits, for at most [`TRANSITION_ROUNDS`] collect timeouts, until the
    /// installed layout makes this node a replica — the epoch that adds it
    /// may still be on its way when the join starts, or wait on a transition
    /// draining — fetching `contact`'s decided epochs meanwhile. Returns that
    /// layout and this node's shard in it. A join under a layout without this
    /// node would own no key: it would install none of the contact's state,
    /// adopt its floor, and then serve without everything below it.
    async fn await_membership(&self, contact: NodeId) -> anyhow::Result<(Arc<Layout>, ShardId)> {
        let deadline = Self::after(self.settings.collect_timeout * TRANSITION_ROUNDS);
        loop {
            let layout = self.topology.layout();
            if let Some(shard) = layout.node_shard(self.id) {
                return Ok((layout, shard));
            }
            anyhow::ensure!(
                self.topology.dynamic() && Instant::now() < deadline,
                "{:?} is no replica under epoch {}: join once a layout that adds it is installed",
                self.id,
                layout.epoch()
            );
            self.send(
                contact,
                Message::MetadataFetch {
                    after_epoch: self.installed_epoch(),
                },
            )
            .await;
            tokio::time::sleep(self.settings.recovery_interval).await;
        }
    }

    /// This node's amnesia fence while its join is pending (#312): a
    /// contact must vouch at or above it before the join installs anything.
    fn pending_fence(&self) -> Option<Timestamp> {
        self.amnesia
            .lock()
            .expect("amnesia poisoned")
            .filter(|fence| !fence.joined)
            .map(|fence| Timestamp {
                micros: fence.fence_micros,
                logical: 0,
                node: NodeId(0),
            })
    }

    /// Waits, for at most [`NodeConfig::amnesia_join_timeout`], until
    /// `contact`'s own watermark report (gossiped to every node, and heard
    /// while joining) reaches `fence`: only then is its join header asked
    /// for, since the header installs a [`JoinHold`] that would pin the
    /// contact's report at an earlier vouch. Under a topology that changes,
    /// the contact's decided epochs are fetched meanwhile, since a vouch
    /// counts only under the epoch this node has installed.
    async fn await_report_past(&self, contact: NodeId, fence: Timestamp) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.settings.amnesia_join_timeout;
        let mut reported = None;
        loop {
            let live_since = self.live_since();
            reported = self
                .peer_watermarks
                .lock()
                .expect("watermarks poisoned")
                .under(live_since)
                .get(&contact)
                .copied()
                .or(reported);
            if reported.is_some_and(|report| report >= fence) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.metrics.record_amnesia_join_refused();
                return Err(FenceNotCovered {
                    fence,
                    vouched: reported,
                }
                .into());
            }
            if self.topology.dynamic() {
                self.send(
                    contact,
                    Message::MetadataFetch {
                        after_epoch: self.installed_epoch(),
                    },
                )
                .await;
            }
            tokio::time::sleep(self.settings.recovery_interval).await;
        }
    }

    /// Records this node's join past its amnesia fence: the journal flushed
    /// first, so everything the join staged survives the plain restart the
    /// record allows.
    async fn mark_joined(&self) -> anyhow::Result<()> {
        let fence = self
            .amnesia
            .lock()
            .expect("amnesia poisoned")
            .map(|fence| AmnesiaFence {
                joined: true,
                ..fence
            });
        let Some(fence) = fence else {
            return Ok(());
        };
        self.journal.flush().await?;
        self.metrics.record_flush();
        self.journal
            .save_amnesia_fence(&fence)
            .await
            .map_err(|err| err.context("cannot record the join past the amnesia fence"))?;
        *self.amnesia.lock().expect("amnesia poisoned") = Some(fence);
        self.floor_votes_at(fence);
        tracing::info!(
            node = self.id.0,
            fence = fence.fence_micros,
            "joined past the amnesia fence"
        );
        Ok(())
    }

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
            // Biased: with both ready, the work's result wins. Unbiased,
            // tokio picks a branch from an RNG that is not seeded, and the
            // simulation would no longer replay.
            tokio::select! {
                biased;
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

    /// An anti-entropy reply: this node's redundancy watermark, its
    /// margin-lagged applied-through (the coverage the requester may record),
    /// and the applied commands the requester's digest says it is missing,
    /// so a healthy in-sync round ships nothing.
    ///
    /// An anti-entropy reply is bounded like a snapshot page
    /// ([`snapshot_page_events`](NodeConfig::snapshot_page_events) /
    /// [`snapshot_page_bytes`](NodeConfig::snapshot_page_bytes)): a requester
    /// back from a long partition would otherwise be sent its whole backlog
    /// in one frame — over the transport's cap, shed, and asked for again
    /// every round, forever. It gets an execution-order prefix instead, and
    /// its next digest — in the same round, while pages bring something in
    /// ([`anti_entropy`](Self::anti_entropy)) — asks for the rest. The
    /// coverage then stops below the oldest `t0` left out: the requester
    /// records it as proof it holds everything this node applied below it.
    ///
    /// The coverage is a vouch: it never passes the shard's
    /// [applied floor](Self::applied_floor), nor the journaled vote fence
    /// that covers it (#337).
    fn export_missing(&self, known: &SyncKnown) -> (Timestamp, Timestamp, Vec<CommandState>) {
        let shard_peers = self.shard_peers();
        let gossiped = self.gossiped_applied();
        let have: HashSet<TxnId> = known.txns.iter().copied().collect();
        let replica = self.replica.lock().expect("replica poisoned");
        let (commands, omitted) = replica.export_applied_missing(
            known.since,
            &have,
            self.settings.snapshot_page_events,
            self.settings.snapshot_page_bytes,
        );
        // Everything this node holds below its margin-lagged applied-through
        // is applied — and, with `commands`, on its way to the requester,
        // which records it as sync coverage — except what the budget left
        // out, all at or above `omitted`.
        let coverage = self.export_ceiling(&replica, &shard_peers, &gossiped);
        let coverage = omitted.map_or(coverage, |oldest| coverage.min(oldest));
        let coverage = coverage.min(replica.durable_fence());
        let floor = replica.redundant_before();
        drop(replica);
        let coverage = self.exportable_coverage(coverage, &shard_peers);
        (floor, coverage, commands)
    }

    /// A join header — this node's redundancy watermark and the coverage an
    /// [`export_missing`](Self::export_missing) would report, without its
    /// commands — plus what this node vouches for the joiner: its own
    /// watermark report before the holds it keeps for joiners
    /// ([`vouch_locked`](Self::vouch_locked)) and the epoch it holds under
    /// ([`Message::JoinProof`]). Both are
    /// taken under one replica lock — and a sync round records its coverage
    /// only after importing its commands — so every applied command below
    /// the vouch is either below the exported floor (in the snapshot) or
    /// among the applied commands of this cut: the joiner ends up holding
    /// exactly what the vouch covers. Those commands follow in
    /// [`CommandPage`](Message::CommandPage)s cut later; one compacted in
    /// between is no longer walked, but it is then below the page's floor,
    /// which the joiner adopts, and its events are in the snapshot pages it
    /// pulls after the last command page — so the vouch still holds. An
    /// epoch change meanwhile voids the vouch (`MIN`). Like every vouch,
    /// neither the coverage nor the vouch passes the journaled vote fence
    /// (#337), nor the coverage the shard's [applied floor](Self::applied_floor).
    fn export_join_header(&self) -> (Timestamp, Timestamp, u64, Timestamp) {
        let epoch = self.live_since();
        let shard_peers = self.shard_peers();
        let gossiped = self.gossiped_applied();
        let replica = self.replica.lock().expect("replica poisoned");
        let fence = replica.durable_fence();
        let vouched = self.vouch_locked(&replica, &shard_peers).min(fence);
        let watermark = replica.redundant_before();
        let applied = self
            .export_ceiling(&replica, &shard_peers, &gossiped)
            .min(fence);
        drop(replica);
        let coverage = self.exportable_coverage(applied, &shard_peers);
        let vouched = if self.live_since() == epoch {
            vouched
        } else {
            Timestamp::MIN
        };
        (watermark, coverage, epoch, vouched)
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

    /// Fetches the join header from `contact` with a
    /// [`JoinHeaderRequest`](Message::JoinHeaderRequest): its `(watermark,
    /// coverage)` (no commands: they follow in pages) and the slot its
    /// [`JoinProof`](Message::JoinProof) lands in.
    async fn fetch_join_header(
        &self,
        contact: NodeId,
        timeout: Duration,
    ) -> anyhow::Result<(SyncPayload, PendingProof<'_>)> {
        let mut slot = None;
        let header = self
            .sync_round(contact, timeout, |id| {
                slot = Some(self.pending_proof(id));
                Message::JoinHeaderRequest { id }
            })
            .await?;
        Ok((header, slot.expect("the request registers its proof slot")))
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

    /// Fetches `contact`'s committed state and imports every command for a key
    /// this node owns that it does not already have. Used by both bootstrap
    /// ([`join`](Node::join), `known = None`) and the anti-entropy repair
    /// sweep (this node's digest, so the contact ships only what it lacks).
    ///
    /// On bootstrap, the contact also ships its materialised data-store
    /// snapshot and redundancy watermark: a bootstrapping node may be below the
    /// cluster's truncation point, where command replay alone no longer
    /// reconstructs the state. The header ([`Message::JoinHeaderRequest`])
    /// brings the watermark; the recent commands follow page by page
    /// ([`Message::CommandPageRequest`]), then the
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
        known: Option<SyncKnown>,
    ) -> anyhow::Result<Imported> {
        let want_snapshot = known.is_none();
        let mut imported = Imported::default();
        let mut apply_failed = false;
        let mut proof_slot: Option<PendingProof<'_>> = None;
        let (watermark, coverage, live_since, commands) = if let Some(known) = known {
            self.sync_round(contact, timeout, |id| Message::SyncRequest { id, known })
                .await?
        } else {
            // A page overlapping events already here would be skipped whole
            // (an idempotent re-apply), leaving a gap: a join needs an empty
            // store, and says so before fetching anything.
            let probe = self.datastore.snapshot_page(None, 1, usize::MAX).await?;
            anyhow::ensure!(
                probe.events.is_empty(),
                "join only over an empty store; this one holds events"
            );
            // The contact's vouch follows its header on the same link
            // ([`Message::JoinProof`]).
            let ((mut watermark, coverage, live_since, mut commands), slot) =
                self.fetch_join_header(contact, timeout).await?;
            proof_slot = Some(slot);
            // Then the applied commands, page by page, before any snapshot
            // page: every command listed has its events in a page fetched
            // after it. The contact may compact between pages; the highest
            // floor it sent is the one to adopt, and what it covers is in the
            // snapshot — dropped here, as compacting below it would.
            let mut after = None;
            loop {
                let (floor, page, next) = self.fetch_command_page(contact, timeout, after).await?;
                watermark = watermark.max(floor);
                commands.extend(page);
                match next {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }
            }
            commands.retain(|cmd| cmd.txn.0 >= watermark);
            // A node that lost its journal installs nothing below a contact's
            // vouch past its amnesia fence (#312): only then is every write
            // that committed below the fence in the contact's state, and does
            // none that did not stand a chance any more. The vouch follows the
            // header on the same link, so it is here by now.
            if let Some(fence) = self.pending_fence() {
                let vouch = proof_slot.as_ref().and_then(PendingProof::peek);
                let covered = vouch.is_some_and(|(epoch, vouched)| {
                    epoch == self.live_since()
                        && self.topology.node_shard(contact) == self.topology.node_shard(self.id)
                        && vouched >= fence
                });
                if !covered {
                    // Release the hold the header made the contact keep at
                    // its vouch: this node's own report, until it joins, is
                    // that it holds nothing.
                    self.send(
                        contact,
                        Message::Watermark {
                            applied_through: Timestamp::MIN,
                            applied: Timestamp::MIN,
                            mark: self.durable_mark(),
                            fenced: None,
                            copied: None,
                            live_since: self.live_since(),
                        },
                    )
                    .await;
                    self.metrics.record_amnesia_join_refused();
                    return Err(FenceNotCovered {
                        fence,
                        vouched: vouch.map(|(_, vouched)| vouched),
                    }
                    .into());
                }
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
            (watermark, coverage, live_since, commands)
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
        let installed = self.installed_epoch();
        for cmd in commands {
            if !cmd.keys.iter().any(|k| self.topology.owns(self.id, k)) {
                continue;
            }
            // Planned under a layout this node has not installed: which of
            // its keys this node owns, it cannot tell yet — judged under the
            // layout it has, a key it is about to own would be left out of
            // the store, and the command marked applied never shipped again.
            // The round stops here, as at a failed apply: what follows is in
            // execution order, and may be planned under that epoch too, and
            // a round that stops proves no coverage (the peer would compact
            // what was not imported). The metadata sweep installs the epoch,
            // and the next round ships the command again.
            if cmd.epoch > installed {
                tracing::debug!(
                    node = self.id.0,
                    txn = ?cmd.txn,
                    epoch = cmd.epoch,
                    installed,
                    "an import planned under an epoch not installed here waits"
                );
                apply_failed = true;
                break;
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
            let (txn, execute_at, events) = (cmd.txn, cmd.execute_at, cmd.events.clone());
            // Journaled first (#331): once recorded here, a report of this node
            // may count it as applied, and a peer compact it away on that
            // report. A restart that forgot it would restore only what the
            // journal held of it — nothing, or a Commit — and a command
            // waiting on it would wait for good. Staged ahead of the store,
            // as a local apply is, and only staged: a report flushes the
            // journal before it leaves (`vouch_durably`).
            let mut applied = cmd.clone();
            applied.status = Status::Applied;
            if let Err(err) = self.journal.stage(&applied).await {
                tracing::warn!(node = self.id.0, ?txn, %err, "cannot journal an import");
                apply_failed = true;
                break;
            }
            // Shipped with the provisional stamps; applied with the agreed
            // ones, exactly as the contact applied them — for the keys this
            // node owns now: a peer behind on the layout may still ship a
            // key this node dropped.
            let mut events = self.owned_now(events);
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
        // A node rejoining over its kept journal first drops what it holds
        // below that floor (see `Replica::forget_below`): an un-applied
        // command there would otherwise clamp the floor, and nothing would
        // ever apply it.
        if want_snapshot {
            let mut replica = self.replica.lock().expect("replica poisoned");
            replica.forget_below(watermark);
            replica.compact(watermark);
            drop(replica);
            // Every page is installed: this node now holds what the contact
            // vouched for, if it shares the contact's shard and epoch.
            let proof = proof_slot.take().and_then(PendingProof::take);
            if let Some((epoch, vouched)) = proof {
                if epoch == self.live_since()
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
        // Only a same-shard contact under the same live layout witnesses this
        // shard's transactions: under another, this node skipped those on keys
        // it does not own here, and may own one next. A failed local apply
        // voids the claim for this round.
        if !apply_failed
            && live_since == self.live_since()
            && self.topology.node_shard(contact) == self.topology.node_shard(self.id)
        {
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
            let shard_peers = self.shard_peers();
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
            self.applied.bump();
        }

        imported.stopped = apply_failed;
        Ok(imported)
    }

    /// Changes the layout to `shards`, starting at `epoch` (the next one),
    /// fault tolerantly: each step is decided by single-decree Paxos over the
    /// **current members**, so it survives this coordinator failing, then
    /// committed cluster-wide. Returns the layout installed in the end (which
    /// differs from `shards` only if an interrupted change had already been
    /// decided for `epoch`).
    ///
    /// `shards` is each shard's replica set; the slots move as little as they
    /// can ([`ShardMap::rebalanced`]): none with as many shards, only those a
    /// new shard needs, or a removed one leaves, otherwise.
    ///
    /// A change that adds a replica to a shard of three installs `shards` at
    /// `epoch`: a new replica then joins through one its shard kept
    /// ([`join`](Node::join)). Any other change — removing or replacing a
    /// replica, adding two, adding or removing shards, moving a replica to
    /// another shard — takes two epochs (see `reconfig`):
    /// `epoch` decides a transition, during which writes to the keys it moves
    /// are refused with [`TopologyChanging`] while the writes in flight on them
    /// drain and every node gaining slots copies them; the next epoch installs
    /// `shards` once that is done — after about the compaction margin, a few
    /// anti-entropy rounds and the copies, all run by the recovery sweep
    /// ([`start_recovery`](Node::start_recovery)) of every node, the new ones
    /// included. A change while a transition is in progress replaces it; one
    /// back to the live layout cancels it. A change may give a node a slot it
    /// replicated before: the node drops what it still holds for it before
    /// copying it, and a node drops the slots a change takes from it once
    /// the change installs.
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
        self.ensure_next_epoch(epoch)?;
        let decided = match self.committed_entry(epoch) {
            Some(decided) => decided,
            None => {
                let live = self.topology.layout();
                let map = self.change_map(&live, shards)?;
                // The change already in progress is carried through, not
                // decided again: its fence and drain stand.
                if let Some(since) = self.transition_to(&map) {
                    return self.complete_transition(since).await;
                }
                // The nodes the change gives slots copy them first, while
                // writes still flow (#373), so the copy under the fence has
                // only the tail left. Not for a change that installs at
                // once: its new replica joins, over an empty store.
                let next = Layout::from_map(epoch, map.clone());
                if !reconfig::installs_at_once(&live, &next) {
                    self.preseed_gainers(&live, &next).await;
                }
                let acceptors = self.topology.nodes();
                let proposal = Self::change_entry(epoch, &live, map);
                let decided = self.run_config_paxos(epoch, proposal, &acceptors).await?;
                self.commit_topology(epoch, &decided, &acceptors).await;
                decided
            }
        };
        match decided {
            ConfigEntry::Active { layout, .. } => Ok(layout.shards),
            ConfigEntry::Transition { since, .. } => self.complete_transition(since).await,
        }
    }

    /// The shard map a change from `live` to `shards` installs: `live`'s slot
    /// table rebalanced over `shards` ([`ShardMap::rebalanced`]: with another
    /// shard count, only the slots a new shard needs, or a removed one leaves,
    /// move). A node may get a slot back it replicated before: it drops what
    /// it holds for the slot before copying it ([`copy_gained_slots`](Self::copy_gained_slots)).
    fn change_map(&self, live: &Layout, shards: Vec<Vec<NodeId>>) -> anyhow::Result<ShardMap> {
        let map = live.map().rebalanced(shards);
        map.validate()?;
        Ok(map)
    }

    /// The entry a change from `live` to `map` decides at `epoch`: the new
    /// layout at once when it can be installed so ([`reconfig::installs_at_once`]),
    /// a transition otherwise.
    fn change_entry(epoch: u64, live: &Layout, map: ShardMap) -> ConfigEntry {
        let next = Layout::from_map(epoch, map.clone());
        if reconfig::installs_at_once(live, &next) {
            ConfigEntry::Active {
                layout: map,
                drained: None,
            }
        } else {
            ConfigEntry::Transition {
                active: live.map(),
                pending: map,
                since: epoch,
            }
        }
    }

    /// The `since` of the transition in progress, if it installs `map`.
    fn transition_to(&self, map: &ShardMap) -> Option<u64> {
        let reconfig = self.reconfig.lock().expect("reconfig poisoned");
        reconfig
            .transition
            .as_ref()
            .filter(|t| t.pending.map() == *map)
            .map(|t| t.since)
    }

    /// Carries the transition first decided at `since` through: waits until it
    /// drained, decides the entry that installs its layout, and returns that
    /// layout — or fails if another change replaced it, or if it is still
    /// draining after a while (the recovery sweep then completes it).
    async fn complete_transition(&self, since: u64) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let deadline = Self::after(self.settings.collect_timeout * TRANSITION_ROUNDS);
        loop {
            match self.transition_outcome(since) {
                TransitionOutcome::Draining => {}
                TransitionOutcome::Installed(layout) => return Ok(layout),
                TransitionOutcome::Replaced => {
                    anyhow::bail!(
                        "the transition decided at epoch {since} was replaced by another change"
                    )
                }
            }
            if let Some(point) = self.ready_to_activate(since) {
                if let Err(err) = self.activate(since, point).await {
                    tracing::debug!(node = self.id.0, since, %err, "activation not decided yet");
                }
                continue;
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "the transition decided at epoch {since} is still draining; the recovery sweep completes it"
                );
            }
            tokio::time::sleep(self.settings.recovery_interval).await;
        }
    }

    /// How the transition first decided at `since` stands on this node.
    fn transition_outcome(&self, since: u64) -> TransitionOutcome {
        let ended = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            if reconfig
                .transition
                .as_ref()
                .is_some_and(|t| t.since == since)
            {
                return TransitionOutcome::Draining;
            }
            reconfig
                .fences
                .iter()
                .find(|f| f.since == since)
                .and_then(|f| f.end)
        };
        let log = self.meta_log.lock().expect("meta_log poisoned");
        let pending = match log.entries.get(&since) {
            Some(ConfigEntry::Transition { pending, .. }) => pending,
            _ => return TransitionOutcome::Replaced,
        };
        match ended.and_then(|end| log.entries.get(&end)) {
            Some(ConfigEntry::Active { layout, .. }) if layout == pending => {
                TransitionOutcome::Installed(layout.shards.clone())
            }
            _ => TransitionOutcome::Replaced,
        }
    }

    /// The drain point of the transition first decided at `since`, once it is
    /// in progress here, has drained (see [`reconfig::drained`]), every node
    /// keeping a moved slot reported past it, and every node gaining slots
    /// reported it copied them.
    fn ready_to_activate(&self, since: u64) -> Option<Timestamp> {
        let live_since = self.live_since();
        let reports = self
            .peer_watermarks
            .lock()
            .expect("watermarks poisoned")
            .under(live_since)
            .clone();
        let reconfig = self.reconfig.lock().expect("reconfig poisoned");
        let transition = reconfig.transition.as_ref().filter(|t| t.since == since)?;
        let point = reconfig::drain_point(transition, &reconfig.reported)?;
        let copied = transition.gainers.iter().all(|node| {
            if *node == self.id {
                reconfig.copied == Some(since)
            } else {
                reconfig.copies.get(node) == Some(&since)
            }
        });
        let kept = transition
            .keepers
            .iter()
            .all(|node| reports.get(node).is_some_and(|&report| report > point));
        (copied && kept && reconfig::drained(&transition.live, point, &reports)).then_some(point)
    }

    /// Copies the slots this node gains in the transition in progress, if it
    /// gains any and has not copied them yet. The transition's `since` once
    /// this node holds every slot it gains (or gains none); `None` while there
    /// is no transition, or the copy is not done.
    async fn copy_if_gaining(&self) -> Option<u64> {
        let (since, copying) = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            let transition = reconfig.transition.as_ref()?;
            (
                transition.since,
                transition.gainers.contains(&self.id) && reconfig.copied != Some(transition.since),
            )
        };
        if copying && !self.copy_gained_slots(since).await {
            return None;
        }
        Some(since)
    }

    /// Copies the slots this node gains in the transition first decided at
    /// `since` (see `reconfig`): from each shard of the live layout that holds
    /// some, those slots' events, through a replica whose watermark report is
    /// past that shard's own drain point ([`reconfig::shard_drain_point`]).
    /// The keys take no new write while the transition drains, so the copy is
    /// final. Installed like a join's snapshot, under a synthetic id at that
    /// point, as the continuation of what the store holds.
    ///
    /// Before the first page, the gained slots are dropped from the store:
    /// this node does not own them under the live layout, so whatever it
    /// holds for them is stale — the events from when it replicated them
    /// before, or a copy for a transition replaced since — and a copy over a
    /// stale prefix would skip it (#372). The drop is journaled once done
    /// ([`CopyPhase::Dropped`]), so a copy interrupted — by a failed page,
    /// or a restart — resumes over the contiguous prefix it left, and drops
    /// nothing again. The copy is journaled complete, and flushed, before it
    /// is reported ([`CopyPhase::Copied`]): the activation goes by the
    /// report, and the sources drop the slots once it installs, so a
    /// restarted gainer must never drop and copy again what it reported.
    /// Returns whether every gained slot is in; a shard not drained yet, or a
    /// contact that failed, is retried by the next sweep.
    async fn copy_gained_slots(&self, since: u64) -> bool {
        let (live, pending, fences, dropped) = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            let Some(transition) = reconfig.transition.as_ref().filter(|t| t.since == since) else {
                return false;
            };
            let dropped = reconfig
                .copy_state
                .as_ref()
                .is_some_and(|state| state.since == since && state.map == transition.pending.map());
            (
                Arc::clone(&transition.live),
                Arc::clone(&transition.pending),
                reconfig.reported.clone(),
                dropped,
            )
        };
        // Not while a pre-seed is still copying: it ends, and the copy below
        // resumes where it did.
        let _copying = self.copying.lock().await;
        let resume = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            match reconfig.copy_state.as_ref().map(|state| &state.phase) {
                Some(CopyPhase::Preseeded { resume }) if dropped => *resume,
                _ => None,
            }
        };
        if !dropped && !self.drop_gained_slots(since, &live, &pending).await {
            return false;
        }
        let live_since = self.live_since();
        let reports = self
            .peer_watermarks
            .lock()
            .expect("watermarks poisoned")
            .under(live_since)
            .clone();
        let mut gained: Vec<(usize, Vec<usize>)> = reconfig::gained_slots(&live, &pending, self.id)
            .into_iter()
            .collect();
        gained.sort_unstable();
        for (source, slots) in gained {
            let Some(point) = reconfig::shard_drain_point(&live, source, since, &fences) else {
                return false;
            };
            let Some(contact) = live
                .replicas_of(source)
                .iter()
                .copied()
                .filter(|&node| node != self.id)
                .find(|node| reports.get(node).is_some_and(|&report| report > point))
            else {
                return false;
            };
            let slots: Vec<u16> = slots.into_iter().map(|slot| slot as u16).collect();
            let copied = match self.copy_slots(contact, &slots, resume, TxnId(point)).await {
                Ok(copied) => copied,
                Err(err) => {
                    tracing::debug!(node = self.id.0, contact = contact.0, %err, "cannot copy a range yet");
                    return false;
                }
            };
            self.metrics.record_range_copy_events(copied.events);
            tracing::info!(
                node = self.id.0,
                since,
                source,
                contact = contact.0,
                slots = slots.len(),
                events = copied.events,
                stamps = ?copied.stamps,
                preseeded = resume.is_some(),
                "copied the slots a transition moves here"
            );
            self.note_copied_history(since, &copied);
        }
        let state = CopyState {
            since,
            map: pending.map().clone(),
            phase: CopyPhase::Copied,
        };
        if let Err(err) = self.journal.save_copy_state(&state).await {
            tracing::warn!(node = self.id.0, since, %err, "cannot journal a finished copy; not reporting it");
            return false;
        }
        if let Err(err) = self.journal.flush().await {
            tracing::warn!(node = self.id.0, since, %err, "cannot flush a finished copy; not reporting it");
            return false;
        }
        self.metrics.record_flush();
        {
            let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
            reconfig.copy_state = Some(state);
            reconfig.copied = Some(since);
        }
        // A new node opened joining holds its slots now: it serves, and what
        // it buffered meanwhile is handled.
        if !self.bootstrapped.load(Ordering::Acquire) && self.pending_fence().is_none() {
            let buffered = {
                let mut buffer = self.join_buffer.lock().expect("buffer poisoned");
                self.bootstrapped.store(true, Ordering::Release);
                std::mem::take(&mut *buffer)
            };
            for env in buffered {
                Box::pin(self.handle(env)).await;
            }
        }
        true
    }

    /// Drops from the store the slots this node gains in the transition
    /// first decided at `since`, and journals that it did
    /// ([`CopyPhase::Dropped`]). Returns whether both are done; a failure is
    /// retried by the next sweep, the drop first again (what a drop that
    /// failed partway left is dropped again).
    async fn drop_gained_slots(&self, since: u64, live: &Layout, pending: &Layout) -> bool {
        let mut gained: Vec<u16> = reconfig::gained_slots(live, pending, self.id)
            .into_values()
            .flatten()
            .map(|slot| slot as u16)
            .collect();
        gained.sort_unstable();
        let dropped = match self.datastore.drop_slots(&gained).await {
            Ok(dropped) => dropped,
            Err(err) => {
                tracing::warn!(node = self.id.0, since, %err, "cannot drop the slots a transition gives this node");
                return false;
            }
        };
        if dropped > 0 {
            self.metrics.record_streams_dropped(dropped);
            tracing::info!(
                node = self.id.0,
                since,
                slots = gained.len(),
                streams = dropped,
                "dropped what the store held for the slots a transition gives this node"
            );
        }
        let state = CopyState {
            since,
            map: pending.map().clone(),
            phase: CopyPhase::Dropped,
        };
        if let Err(err) = self.journal.save_copy_state(&state).await {
            tracing::warn!(node = self.id.0, since, %err, "cannot journal a drop; the copy waits");
            return false;
        }
        self.reconfig.lock().expect("reconfig poisoned").copy_state = Some(state);
        true
    }

    /// Copies `slots`' events from `contact`, page by page — those stamped
    /// at or above `since`, every one with `None`
    /// ([`Message::RangePageRequest`]) — into the store as the continuation
    /// of what it holds ([`not_held`](Self::not_held)), under the synthetic
    /// id `txn`. Returns how many events landed, and the contact's stable
    /// point as it stood before the first page.
    async fn copy_slots(
        &self,
        contact: NodeId,
        slots: &[u16],
        since: Option<u64>,
        txn: TxnId,
    ) -> anyhow::Result<CopyOutcome> {
        let mut after: Option<Value> = None;
        let mut outcome = CopyOutcome::default();
        let mut first = true;
        loop {
            let page = self
                .fetch_page(contact, self.settings.collect_timeout, "range page", |id| {
                    Message::RangePageRequest {
                        id,
                        slots: slots.to_vec(),
                        after: after.clone(),
                        since,
                    }
                })
                .await?;
            let (events, next, stable) = match page {
                Message::RangePage {
                    events,
                    next,
                    stable,
                    ..
                } => (events, next, stable),
                other => anyhow::bail!("not a range page: {other:?}"),
            };
            if first {
                outcome.stable = stable;
                first = false;
            }
            let events = self
                .not_held(events)
                .await
                .context("cannot read the store's versions")?;
            if !events.is_empty() {
                outcome.events += events.len();
                // What this node's subscriptions already read past (#374).
                let point = self.stable_micros();
                for event in &events {
                    let stamp = stamp_micros(event);
                    outcome.stamps = Some(match outcome.stamps {
                        Some((lo, hi)) => (lo.min(stamp), hi.max(stamp)),
                        None => (stamp, stamp),
                    });
                    if stamp < point {
                        outcome.below_stable += 1;
                    }
                }
                self.datastore
                    .apply(txn, txn.0, events, true)
                    .await
                    .context("cannot install a copied range")?;
            }
            match next {
                Some(cursor) => after = Some(cursor),
                None => return Ok(outcome),
            }
        }
    }

    /// Notes what a copy for the transition first decided at `since` brought
    /// in that this node's subscriptions will not see (#374): copied events
    /// keep their original stamps, and a subscription reads forward from a
    /// stamp cursor, so history stamped below the stable point it already
    /// passed is never delivered to it. Counted
    /// (`accord_copied_below_stable_total`) and warned about; a projection
    /// on this node that must hold the gained range is rebuilt, or its
    /// cursor rewound, by its operator (OPERATIONS §6).
    fn note_copied_history(&self, since: u64, copied: &CopyOutcome) {
        if copied.below_stable == 0 {
            return;
        }
        self.metrics.record_copied_below_stable(copied.below_stable);
        tracing::warn!(
            node = self.id.0,
            since,
            events = copied.events,
            below_stable = copied.below_stable,
            stamps = ?copied.stamps,
            "copied history lies below this node's stable point: its subscriptions already read past it (#374)"
        );
    }

    /// Copies the slots the change to `map` would give this node, before
    /// the change is decided and while writes still flow (#373), so the copy
    /// under the write fence has only the tail left. In passes: the first
    /// from the start, each later one from the sources' stable point as it
    /// stood before the previous pass — an event applied late at a source
    /// lands below a page already read, but never below the point every
    /// transaction stamped under it was applied at — until a pass brings
    /// nothing new, or [`PRESEED_PASSES`] ran. The gained slots are dropped
    /// first, as under the fence, unless this node pre-seeded this very map
    /// already. Journaled as [`CopyPhase::Preseeded`] with the point the
    /// fenced copy resumes from. A source that fails is skipped: the fenced
    /// copy brings whatever is left.
    async fn preseed(&self, map: ShardMap) {
        let _copying = self.copying.lock().await;
        let live = self.topology.layout();
        let since = live.epoch() + 1;
        let pending = Layout::from_map(since, map.clone());
        let mut gained: Vec<(usize, Vec<u16>)> = reconfig::gained_slots(&live, &pending, self.id)
            .into_iter()
            .map(|(source, slots)| (source, slots.into_iter().map(|s| s as u16).collect()))
            .collect();
        gained.sort_unstable();
        if gained.is_empty() {
            return;
        }
        let state = self
            .reconfig
            .lock()
            .expect("reconfig poisoned")
            .copy_state
            .clone()
            .filter(|state| state.since == since && state.map == map);
        match state.as_ref().map(|state| &state.phase) {
            // Done before, for this very change.
            Some(CopyPhase::Preseeded { .. }) | Some(CopyPhase::Copied) => return,
            Some(CopyPhase::Dropped) => {}
            None => {
                if !self.drop_gained_slots(since, &live, &pending).await {
                    return;
                }
            }
        }
        let txn = TxnId(Timestamp::MIN);
        let mut resume: Option<u64> = None;
        for pass in 1..=PRESEED_PASSES {
            let mut pass_outcome = CopyOutcome::default();
            for (source, slots) in &gained {
                let contacts: Vec<NodeId> = live
                    .replicas_of(*source)
                    .iter()
                    .copied()
                    .filter(|&node| node != self.id)
                    .collect();
                let mut done = false;
                for contact in contacts {
                    match self.copy_slots(contact, slots, resume, txn).await {
                        Ok(outcome) => {
                            pass_outcome.add(outcome);
                            done = true;
                            break;
                        }
                        Err(err) => {
                            tracing::debug!(node = self.id.0, contact = contact.0, %err, "a pre-seed source failed");
                        }
                    }
                }
                if !done {
                    tracing::info!(
                        node = self.id.0,
                        source,
                        "no source answered a pre-seed; the fenced copy brings the rest"
                    );
                    return;
                }
            }
            let copied = pass_outcome.events;
            self.metrics.record_preseed_pass(copied);
            tracing::info!(
                node = self.id.0,
                since,
                pass,
                events = copied,
                stamps = ?pass_outcome.stamps,
                stable = ?pass_outcome.stable,
                "pre-seeded the slots a change would give this node"
            );
            self.note_copied_history(since, &pass_outcome);
            // Nothing to resume from: the fenced copy starts over, skipping
            // what is held.
            let Some(stable) = pass_outcome.stable else {
                resume = None;
                break;
            };
            resume = Some(stable);
            if copied == 0 {
                break;
            }
        }
        let state = CopyState {
            since,
            map,
            phase: CopyPhase::Preseeded { resume },
        };
        if let Err(err) = self.journal.save_copy_state(&state).await {
            tracing::warn!(node = self.id.0, since, %err, "cannot journal a pre-seed; the fenced copy starts over");
            return;
        }
        let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
        reconfig.copy_state = Some(state);
        reconfig.preseed_expires = Some(Self::after(self.preseed_grace()));
    }

    /// How long a pre-seed is kept for the change it was made for: the
    /// driver proposes once every gainer answered or
    /// [`preseed_timeout`](NodeConfig::preseed_timeout) passed, and the
    /// transition reaches this node within a round or two of that.
    fn preseed_grace(&self) -> Duration {
        self.settings.preseed_timeout + 2 * self.settings.collect_timeout
    }

    /// Asks every node the change from `live` to `next` gives slots to copy
    /// them now ([`Message::Preseed`]), and waits for their answers up to
    /// [`preseed_timeout`](NodeConfig::preseed_timeout). This node, if it
    /// gains slots, copies them inline. Past the timeout the change goes on:
    /// the fenced copy brings whatever is left.
    async fn preseed_gainers(&self, live: &Layout, next: &Layout) {
        let gainers = reconfig::gainers(live, next);
        if gainers.is_empty() {
            return;
        }
        let map = next.map();
        let deadline = Self::after(self.settings.preseed_timeout);
        if gainers.contains(&self.id) {
            self.preseed(map.clone()).await;
        }
        let mut waits = Vec::new();
        for &gainer in gainers.iter().filter(|&&g| g != self.id) {
            let id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = mpsc::unbounded_channel();
            self.snapshot_pending
                .lock()
                .expect("snapshot poisoned")
                .insert(id, tx);
            self.send(
                gainer,
                Message::Preseed {
                    id,
                    live_epoch: live.epoch(),
                    map: map.clone(),
                },
            )
            .await;
            waits.push((id, gainer, rx));
        }
        for (id, gainer, mut rx) in waits {
            let left = deadline.saturating_duration_since(Instant::now());
            let answered = tokio::time::timeout(left, rx.recv()).await.is_ok();
            self.snapshot_pending
                .lock()
                .expect("snapshot poisoned")
                .remove(&id);
            if !answered {
                self.metrics.record_preseed_timed_out();
                tracing::info!(
                    node = self.id.0,
                    gainer = gainer.0,
                    "a pre-seed did not answer in time; the change goes on"
                );
            }
        }
    }

    /// `events` without those the store already holds, and without a gap: a
    /// stream's events continue its current version, each the next (the
    /// store applied the ones at or below it, verbatim). An event past a gap
    /// is left out too — a page that ran ahead of a late apply at its source
    /// — rather than put into the store ahead of what precedes it; the gap is
    /// filled by a later page.
    async fn not_held(&self, events: Vec<Event>) -> anyhow::Result<Vec<Event>> {
        let mut versions: HashMap<(String, String, String), u16> = HashMap::new();
        let mut kept = Vec::with_capacity(events.len());
        for event in events {
            let stream = (
                event.tenant.clone(),
                event.aggregate_type.clone(),
                event.aggregate_id.clone(),
            );
            let version = match versions.get(&stream) {
                Some(&version) => version,
                None => {
                    let version = self
                        .datastore
                        .version(&stream.0, &stream.1, &stream.2)
                        .await?;
                    versions.insert(stream.clone(), version);
                    version
                }
            };
            if event.version == version + 1 {
                versions.insert(stream, event.version);
                kept.push(event);
            }
        }
        Ok(kept)
    }

    /// Drops from the store the slots this node does not own under the
    /// layout it has installed — the slots a layout change took from it, or
    /// gave it and then went back on — once, per installed epoch, and
    /// journals the epoch ([`Journal::save_slot_gc`]). The slots this node is
    /// copying for a transition in progress, or pre-seeded for the change
    /// about to be proposed, are not its own yet and stay. A
    /// drop that fails, or a node that goes down partway, drops again at the
    /// next sweep or restart: dropping what is already gone is a no-op. Only
    /// after an activation installed does a node stop owning a slot, and by
    /// then every write on it that committed is applied at its new owners
    /// (`reconfig`): nothing a replica alone holds goes.
    async fn gc_lost_slots(&self) {
        let layout = self.topology.layout();
        let epoch = layout.epoch();
        let (copying, expired): (Vec<usize>, Option<CopyState>) = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            // A pre-seed for the next change, waited out: the change never
            // came (or is late enough to copy under the fence instead), and
            // what it brought in goes with the rest.
            let preseeded = reconfig.copy_state.as_ref().filter(|state| {
                state.since == epoch + 1 && matches!(state.phase, CopyPhase::Preseeded { .. })
            });
            let expired = preseeded
                .filter(|_| {
                    reconfig
                        .preseed_expires
                        .is_none_or(|until| Instant::now() >= until)
                })
                .cloned();
            if expired.is_none() && reconfig.slot_gc.is_some_and(|through| through >= epoch) {
                return;
            }
            // The layout is installed before the transition it belongs to
            // is (`apply_log`): seen in between, from the sweep, a transition
            // entry has its layout up and its gainers not yet known, and the
            // slots they copy would go. Wait for the install to finish.
            let installing = {
                let log = self.meta_log.lock().expect("meta_log poisoned");
                matches!(
                    log.entries.get(&epoch),
                    Some(ConfigEntry::Transition { .. })
                )
            } && reconfig
                .transition
                .as_ref()
                .is_none_or(|t| t.live.epoch() != epoch);
            if installing {
                return;
            }
            let mut copying: Vec<usize> = reconfig
                .transition
                .as_ref()
                .filter(|t| t.gainers.contains(&self.id))
                .map(|t| {
                    reconfig::gained_slots(&t.live, &t.pending, self.id)
                        .into_values()
                        .flatten()
                        .collect()
                })
                .unwrap_or_default();
            // The slots a pre-seed for the next change brought in (#373) are
            // not this node's yet either, and stay while the change is
            // expected: it is about to be proposed. Waited out, they go.
            if let Some(state) = preseeded.filter(|_| expired.is_none()) {
                let next = Layout::from_map(state.since, state.map.clone());
                copying.extend(
                    reconfig::gained_slots(&layout, &next, self.id)
                        .into_values()
                        .flatten(),
                );
            }
            (copying, expired)
        };
        let unowned: Vec<u16> = (0..SLOTS)
            .filter(|&slot| {
                !layout
                    .replicas_of(layout.slots()[slot] as usize)
                    .contains(&self.id)
                    && !copying.contains(&slot)
            })
            .map(|slot| slot as u16)
            .collect();
        let dropped = match self.datastore.drop_slots(&unowned).await {
            Ok(dropped) => dropped,
            Err(err) => {
                tracing::warn!(node = self.id.0, epoch, %err, "cannot drop the slots this node no longer owns");
                return;
            }
        };
        if dropped > 0 {
            self.metrics.record_streams_dropped(dropped);
            tracing::info!(
                node = self.id.0,
                epoch,
                slots = unowned.len(),
                streams = dropped,
                "dropped the slots this node does not own under the installed layout"
            );
        }
        if let Some(state) = expired {
            // Dropped, for that change: should it come after all, its copy
            // starts over under the fence, without dropping again.
            let state = CopyState {
                phase: CopyPhase::Dropped,
                ..state
            };
            if let Err(err) = self.journal.save_copy_state(&state).await {
                tracing::warn!(node = self.id.0, epoch, %err, "cannot journal an expired pre-seed; it is dropped again");
                return;
            }
            tracing::info!(
                node = self.id.0,
                since = state.since,
                "dropped a pre-seed for a change that did not come in time"
            );
            let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
            reconfig.copy_state = Some(state);
            reconfig.preseed_expires = None;
        }
        if let Err(err) = self.journal.save_slot_gc(epoch).await {
            tracing::warn!(node = self.id.0, epoch, %err, "cannot journal the slot drop; it runs again");
            return;
        }
        let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
        reconfig.slot_gc = Some(reconfig.slot_gc.map_or(epoch, |through| through.max(epoch)));
    }

    /// Decides, and commits, the entry that ends the transition first decided
    /// at `since` by installing its layout, drained at `point`: its clock floor
    /// is this node's reading plus three skews, rounded up to a millisecond, and
    /// this node waits for its clock to pass it first (every node raises its
    /// clock to it as it installs the entry).
    async fn activate(&self, since: u64, point: Timestamp) -> anyhow::Result<()> {
        let skew = self.clock.max_skew_micros();
        let floor_micros = (self.clock.reading_micros() + 3 * skew).div_ceil(1_000) * 1_000;
        loop {
            let reading = self.clock.reading_micros();
            if reading >= floor_micros {
                break;
            }
            tokio::time::sleep(Duration::from_micros(floor_micros - reading)).await;
        }
        let pending = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            match reconfig.transition.as_ref().filter(|t| t.since == since) {
                Some(t) => t.pending.map(),
                None => return Ok(()),
            }
        };
        let entry = ConfigEntry::Active {
            layout: pending,
            drained: Some(Drained {
                point,
                floor: Timestamp {
                    micros: floor_micros,
                    logical: 0,
                    node: NodeId(0),
                },
            }),
        };
        let epoch = self.installed_epoch() + 1;
        let acceptors = self.topology.nodes();
        let decided = self.run_config_paxos(epoch, entry, &acceptors).await?;
        self.commit_topology(epoch, &decided, &acceptors).await;
        Ok(())
    }

    /// Activates the transition in progress once it has drained, if this node
    /// is the distinguished proposer of the next epoch — or no proposer before
    /// it did so in time (the sweep's half of [`complete_transition`](Self::complete_transition)).
    async fn sweep_transition(&self) {
        let Some(since) = self.copy_if_gaining().await else {
            return;
        };
        let Some(point) = self.ready_to_activate(since) else {
            return;
        };
        let next = self.installed_epoch() + 1;
        let acceptors = self.topology.nodes();
        let rank = self.config_rank(next, &acceptors);
        if rank > 0 {
            tokio::time::sleep(self.settings.config_defer_step * rank as u32).await;
            if self.committed_entry(next).is_some() {
                return;
            }
        }
        if let Err(err) = self.activate(since, point).await {
            tracing::debug!(node = self.id.0, since, %err, "activation not decided yet");
        }
    }

    /// Drives Paxos phases 1–2 for `epoch` (no commit), returning the decided
    /// entry: what [`change_topology`](Self::change_topology) would decide for
    /// `proposed` first. Exposed so tests can simulate a coordinator that fails
    /// after the decision is durable but before committing.
    pub async fn propose_topology(
        &self,
        epoch: u64,
        proposed: Vec<Vec<NodeId>>,
    ) -> anyhow::Result<ConfigEntry> {
        validate_layout(&proposed)?;
        self.ensure_next_epoch(epoch)?;
        let live = self.topology.layout();
        let map = self.change_map(&live, proposed)?;
        let acceptors = self.topology.nodes();
        let proposal = Self::change_entry(epoch, &live, map);
        self.run_config_paxos(epoch, proposal, &acceptors).await
    }

    /// Refuses a change for an epoch past the next one. Its acceptors would
    /// be the members of the installed epoch, not of the one before it, and
    /// its entry would wait for good for the epochs it skips. An epoch already
    /// decided is not refused: the change returns its decided layout.
    fn ensure_next_epoch(&self, epoch: u64) -> anyhow::Result<()> {
        let next = self.installed_epoch() + 1;
        anyhow::ensure!(
            epoch <= next,
            "epoch {epoch} skips past the next one, {next}: install the epochs before it first"
        );
        Ok(())
    }

    /// The committed entry for `epoch`, if it is already in the metadata log.
    fn committed_entry(&self, epoch: u64) -> Option<ConfigEntry> {
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
    ///
    /// Each phase counts only the replies that reach it within
    /// `collect_timeout` of its start: a reply older than that fills no
    /// quorum. An acceptor that lost its journal relies on it
    /// ([`restore_config_votes`](Self::restore_config_votes)): a vote it
    /// made before the loss can complete no round once that long has passed.
    async fn run_config_paxos(
        &self,
        epoch: u64,
        proposed: ConfigEntry,
        acceptors: &[NodeId],
    ) -> anyhow::Result<ConfigEntry> {
        let need = acceptors.len() / 2 + 1;
        let rank = self.config_rank(epoch, acceptors);
        let mut ballot = Ballot(self.mint().await?);

        for attempt in 0..6 {
            // Yield: if the epoch committed (this round or while we were backing off
            // from a duel), adopt that decided entry — never our own `proposed`,
            // which may differ from the chosen value.
            if let Some(entry) = self.committed_entry(epoch) {
                return Ok(entry);
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
            let mut adopted: Option<(Ballot, ConfigEntry)> = None;
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
                            accepted_entry,
                            ..
                        },
                    ))) if b == ballot => {
                        if !promisers.insert(from) {
                            continue;
                        }
                        if let Some(entry) = accepted_entry {
                            if adopted.as_ref().is_none_or(|(b, _)| accepted_ballot > *b) {
                                adopted = Some((accepted_ballot, entry));
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

            let entry = adopted.map(|(_, e)| e).unwrap_or(proposed.clone());
            // The adopted value (an older proposer's) or a recovery's fallback
            // was never checked; an invalid one is not carried to a decision.
            if let Err(err) = entry.validate() {
                return Err(err.context(format!("refusing to decide epoch {epoch}")));
            }

            // Phase 2: Accept the chosen entry.
            for &node in acceptors {
                self.send(
                    node,
                    Message::ConfigAccept {
                        epoch,
                        ballot,
                        entry: entry.clone(),
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
            return Ok(entry);
        }
        // A winner may have committed during our final backoff — adopt it.
        if let Some(entry) = self.committed_entry(epoch) {
            return Ok(entry);
        }
        anyhow::bail!("config change for epoch {epoch} exhausted its ballots")
    }

    /// Recovers a possibly-interrupted change for `epoch`: completes whatever
    /// entry was already accepted (or, if none was, repeats the installed one,
    /// changing nothing), then commits it, and returns it. Any node can call
    /// this after the original coordinator fails; a transition it commits is
    /// then completed by the recovery sweep.
    pub async fn recover_topology(&self, epoch: u64) -> anyhow::Result<ConfigEntry> {
        self.ensure_next_epoch(epoch)?;
        let acceptors = self.topology.nodes();
        // Anti-dueling deference (this is the automatic path, run by the sweep on
        // every node): the distinguished proposer for `epoch` (rank 0) drives first;
        // others wait `rank * config_defer_step` and skip if it already committed, so
        // N concurrent sweeps don't duel. A dead preferred node just means the next
        // rank takes over after its delay (and the periodic sweep retries).
        let rank = self.config_rank(epoch, &acceptors);
        if rank > 0 {
            tokio::time::sleep(self.settings.config_defer_step * rank as u32).await;
            if let Some(entry) = self.committed_entry(epoch) {
                return Ok(entry);
            }
        }
        // Never an activation: one is decided only once its transition
        // drained (`activate`).
        let fallback = {
            let log = self.meta_log.lock().expect("meta_log poisoned");
            let installed = log
                .entries
                .get(&log.installed_epoch)
                .expect("the installed epoch's entry is in the log");
            reconfig::repeat(installed)
        };
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
    fn ingest_entry(&self, epoch: u64, entry: ConfigEntry) -> Installed {
        {
            let mut log = self.meta_log.lock().expect("meta_log poisoned");
            // Below the installed epoch an entry is history only (one the
            // node started past): kept to recover what was planned under it.
            if epoch < log.installed_epoch && entry.validate().is_ok() {
                self.topology.install_map(epoch, entry.live().clone());
            }
            log.entries.entry(epoch).or_insert(entry);
        }
        self.apply_log()
    }

    /// Installs each contiguous metadata-log entry above the installed epoch, in
    /// order. Only ever calls `install(installed_epoch + 1, …)`, so no epoch is
    /// skipped even if a far-future entry is already in the log. The caller
    /// makes what it returns durable ([`persist_installed`](Self::persist_installed)).
    fn apply_log(&self) -> Installed {
        let mut installed = Installed::default();
        loop {
            let next = {
                let log = self.meta_log.lock().expect("meta_log poisoned");
                let next = log.installed_epoch + 1;
                log.entries.get(&next).map(|entry| (next, entry.clone()))
            };
            let Some((next, entry)) = next else { break };
            // An entry persisted before layouts were checked: installing it
            // would break every key (no shard) or strand some (an empty one).
            // Stay on the last good epoch instead.
            if let Err(err) = entry.validate() {
                tracing::error!(
                    node = self.id.0,
                    epoch = next,
                    %err,
                    "not installing an invalid layout; staying on the previous epoch"
                );
                break;
            }
            // An activation's clock floor is in place before anything is
            // planned under the layout it installs.
            if let ConfigEntry::Active {
                drained: Some(drained),
                ..
            } = &entry
            {
                self.clock.restore(drained.floor);
            }
            self.topology.install_map(next, entry.live().clone());
            self.install_entry(next, &entry, &mut installed);
            self.meta_log
                .lock()
                .expect("meta_log poisoned")
                .installed_epoch = next;
        }
        installed
    }

    /// What installing `entry` at `epoch` changes besides the topology: a
    /// transition installs its fence (computed under the replica lock, so no
    /// vote slips between the fence point and the fence), and ends a
    /// different one still in progress; an activation ends the transition,
    /// sets its clock floor, and invalidates what can never commit (see
    /// [`reconfig`]).
    fn install_entry(&self, epoch: u64, entry: &ConfigEntry, installed: &mut Installed) {
        let live = self.topology.layout_at(epoch).expect("installed just now");
        match entry {
            ConfigEntry::Transition { pending, since, .. } => {
                let transition = Transition::new(
                    *since,
                    live,
                    Arc::new(Layout::from_map(epoch, pending.clone())),
                );
                let replica = self.replica.lock().expect("replica poisoned");
                let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
                if let Some(old) = reconfig.transition.take().filter(|old| old.since != *since) {
                    // Replaced by another change: it ends here.
                    reconfig.end_fence(old.since, epoch);
                }
                if !reconfig.fences.iter().any(|f| f.since == *since) {
                    let (cap, journaled) = match reconfig.restored.get(since) {
                        Some(&cap) => (cap, true),
                        None => (
                            Fence::cap_for(
                                replica.highest_t0(),
                                self.vote_horizon_micros(),
                                self.amnesia_fence_micros(),
                            ),
                            false,
                        ),
                    };
                    tracing::info!(
                        node = self.id.0,
                        epoch,
                        since,
                        ?cap,
                        "fenced the keys a layout transition moves"
                    );
                    reconfig
                        .fences
                        .push(Fence::new(&transition, cap, journaled));
                }
                reconfig.transition = Some(transition);
            }
            ConfigEntry::Active { layout, drained } => {
                let mut replica = self.replica.lock().expect("replica poisoned");
                let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
                let previous = self
                    .topology
                    .layout_at(epoch.saturating_sub(1))
                    .map(|l| l.map());
                if previous.as_ref() != Some(layout) {
                    reconfig.live_since = epoch;
                }
                let Some(transition) = reconfig.transition.take() else {
                    return;
                };
                reconfig.end_fence(transition.since, epoch);
                let Some(drained) = drained else {
                    // Ended without a move (the change went back to the live
                    // layout): nothing changed replicas, nothing to drain.
                    return;
                };
                reconfig
                    .floors
                    .push(Floor::new(epoch, drained.floor, &transition));
                // A transaction on a moved key above the drain point never
                // commits: no fenced replica votes for it.
                for (txn, planned, keys) in replica.uncommitted() {
                    if planned < epoch
                        && txn.0 > drained.point
                        && keys.iter().any(|key| transition.moves(key))
                        && replica.invalidate(txn, planned)
                    {
                        installed.invalidated.push(txn);
                    }
                }
                tracing::info!(
                    node = self.id.0,
                    epoch,
                    since = transition.since,
                    point = ?drained.point,
                    invalidated = installed.invalidated.len(),
                    "installed the layout a transition drained for"
                );
            }
        }
    }

    /// Makes what installing entries left durable: stages the commands an
    /// activation invalidated, and journals the fences not journaled yet.
    async fn persist_installed(&self, installed: Installed) {
        if !installed.invalidated.is_empty() {
            for txn in &installed.invalidated {
                self.journal_stage(*txn).await;
            }
            if self.journal.flush().await.is_ok() {
                self.metrics.record_flush();
            }
            self.applied.bump();
        }
        self.persist_fences().await;
        self.gc_lost_slots().await;
    }

    /// Journals the fences, if one is not journaled yet, then reads them
    /// back: only a fence the journal keeps is reported (a journal that
    /// cannot keep them holds every change that moves a key at its
    /// transition). Retried by the sweep until it succeeds.
    async fn persist_fences(&self) {
        let records: Vec<FenceRecord> = {
            let reconfig = self.reconfig.lock().expect("reconfig poisoned");
            if reconfig.fences.iter().all(|f| f.journaled) {
                return;
            }
            reconfig.fences.iter().map(Fence::record).collect()
        };
        if let Err(err) = self.journal.save_fences(&records).await {
            tracing::warn!(node = self.id.0, %err, "cannot journal a transition fence; not reporting it");
            return;
        }
        let kept = match self.journal.load_fences().await {
            Ok(kept) => kept,
            Err(err) => {
                tracing::warn!(node = self.id.0, %err, "cannot read back the transition fences");
                return;
            }
        };
        let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
        for fence in reconfig.fences.iter_mut() {
            if kept.contains(&fence.record()) {
                fence.journaled = true;
            }
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
    async fn commit_topology(&self, epoch: u64, entry: &ConfigEntry, acceptors: &[NodeId]) {
        // Not installed here if the log did not take it: the commit sent to
        // this node with the others, or the metadata sweep, retries it.
        if self.persist_metadata(&[(epoch, entry.clone())]).await {
            let installed = self.ingest_entry(epoch, entry.clone());
            self.persist_installed(installed).await;
        }
        let mut recipients = acceptors.to_vec();
        let named = match entry {
            ConfigEntry::Active { layout, .. } => vec![layout],
            ConfigEntry::Transition {
                active, pending, ..
            } => vec![active, pending],
        };
        for node in named
            .into_iter()
            .flat_map(|map| map.shards.iter().flatten())
        {
            if !recipients.contains(node) {
                recipients.push(*node);
            }
        }
        for &node in &recipients {
            self.send(
                node,
                Message::ConfigCommit {
                    epoch,
                    entry: entry.clone(),
                },
            )
            .await;
        }
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
        tenant: TenantFilter,
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
                tenant,
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

    /// The shards a key set touches under `layout` — replica sets, quorum
    /// sizes, and each shard's slice of `keys`/`events` (one partition pass,
    /// so PreAccept / Accept / Commit ship each shard only what it owns) —
    /// plus the deduplicated union of all their replicas.
    fn plan(layout: &Layout, keys: &[Key], events: &[Event]) -> (Vec<ShardPlan>, Vec<NodeId>) {
        let mut shards: BTreeMap<ShardId, ShardPlan> = BTreeMap::new();
        let mut shard_of: HashMap<&str, ShardId> = HashMap::with_capacity(keys.len());
        for key in keys {
            let shard = layout.shard_of(key);
            shard_of.insert(key.0.as_str(), shard);
            shards
                .entry(shard)
                .or_insert_with(|| ShardPlan {
                    epoch: layout.epoch(),
                    shard,
                    replicas: layout.replicas_of(shard).to_vec(),
                    electorate: layout.electorate_of(shard).to_vec(),
                    fast_q: layout.fast_quorum_of(shard),
                    slow_q: layout.slow_quorum_of(shard),
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
            let shard = match shard_of.get(Key::str_of(event).as_ref()) {
                Some(shard) => *shard,
                None => layout.shard_of(&Key::of(event)),
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

    /// Whether `keys` span more than one shard of `layout`.
    fn spans_shards(layout: &Layout, keys: &[Key]) -> bool {
        let mut shards = keys.iter().map(|k| layout.shard_of(k));
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

    /// The events this node owns under `layout` (keys in this node's shard
    /// there). Ownership is resolved once per distinct key, not once per event.
    fn owned_events(&self, layout: &Layout, events: Vec<Event>) -> Vec<Event> {
        let mut owned: HashMap<String, bool> = HashMap::new();
        events
            .into_iter()
            .filter(|e| {
                let key = Key::str_of(e);
                match owned.get(key.as_ref()) {
                    Some(&v) => v,
                    None => {
                        let v = layout.owns(self.id, &Key(key.to_string()));
                        owned.insert(key.into_owned(), v);
                        v
                    }
                }
            })
            .collect()
    }

    /// The events this node owns under the layout it has installed — what
    /// may land in its store. A transaction is voted on and recovered under
    /// the layout it was planned under ([`owned_events`](Self::owned_events)),
    /// but its events are stored under the installed one: a key a layout
    /// change took from this node was dropped from its store
    /// ([`gc_lost_slots`](Self::gc_lost_slots)), and a late apply, an
    /// anti-entropy import from a peer behind on the layout, or a restart's
    /// replay must not bring a fragment of it back. The transaction is still
    /// marked applied: its status is what the watermark, the read barrier
    /// and anti-entropy go by, and every committed write on a moved key is
    /// applied at its new owners before the layout that moves it installs.
    fn owned_now(&self, events: Vec<Event>) -> Vec<Event> {
        let before = events.len();
        let owned = self.owned_events(&self.topology.layout(), events);
        if owned.len() < before {
            self.metrics.record_applies_not_owned();
        }
        owned
    }

    /// The keys this node owns under `layout`.
    fn owned_keys(&self, layout: &Layout, keys: Vec<Key>) -> Vec<Key> {
        keys.into_iter()
            .filter(|k| layout.owns(self.id, k))
            .collect()
    }

    /// The layout a message about a transaction planned under `epoch` is
    /// judged by: that epoch's, which the topology keeps. `None`, and the
    /// message goes unanswered, when this node does not hold it: an epoch
    /// still to come here, for which it asks `from` (which planned under it,
    /// so holds it) for the metadata it misses, or one from before this node
    /// started, when it was no replica of anything.
    fn layout_for(
        &self,
        epoch: u64,
        from: NodeId,
        out: &mut Vec<(NodeId, Message)>,
    ) -> Option<Arc<Layout>> {
        if let Some(layout) = self.topology.layout_at(epoch) {
            return Some(layout);
        }
        if epoch > self.topology.epoch() {
            self.metrics.record_message_future_epoch();
            tracing::debug!(
                node = self.id.0,
                from = from.0,
                epoch,
                "a message planned under an epoch not installed here; fetching it"
            );
            out.push((
                from,
                Message::MetadataFetch {
                    after_epoch: self.installed_epoch(),
                },
            ));
        }
        None
    }

    /// The fence this node reports for the transition in progress, once the
    /// journal keeps it — recorded as its own report too.
    fn reportable_fence(&self) -> Option<FenceRecord> {
        let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
        let since = reconfig.transition.as_ref()?.since;
        let record = reconfig
            .fences
            .iter()
            .find(|f| f.since == since && f.journaled)?
            .record();
        reconfig.reported.insert(self.id, record);
        Some(record)
    }

    /// Whether this node copied its slots during the transition the live
    /// layout ended.
    fn copied_for_live(&self) -> bool {
        let reconfig = self.reconfig.lock().expect("reconfig poisoned");
        reconfig.copied.is_some_and(|since| {
            reconfig
                .fences
                .iter()
                .any(|f| f.since == since && f.end == Some(reconfig.live_since))
        })
    }

    /// The transition in progress this node copied its gained slots for, if
    /// it did — recorded as its own report too.
    fn copied_report(&self) -> Option<u64> {
        let mut reconfig = self.reconfig.lock().expect("reconfig poisoned");
        let since = reconfig.transition.as_ref()?.since;
        (reconfig.copied == Some(since)).then(|| {
            reconfig.copies.insert(self.id, since);
            since
        })
    }

    /// The `since` of the transition in progress, if it moves one of `keys`.
    fn moving(&self, keys: &[Key]) -> Option<u64> {
        let reconfig = self.reconfig.lock().expect("reconfig poisoned");
        reconfig
            .transition
            .as_ref()
            .filter(|t| keys.iter().any(|k| t.moves(k)))
            .map(|t| t.since)
    }

    /// The epoch the live layout became live at: sync coverage, join proofs
    /// and vouches hold for as long as it stays live — through a transition
    /// too, which changes no replicas until it ends.
    fn live_since(&self) -> u64 {
        self.reconfig.lock().expect("reconfig poisoned").live_since
    }

    /// The highest `t0` a coordinator may date a transaction this node votes
    /// for at (see [`future_dated`](Self::future_dated)), in microseconds.
    fn vote_horizon_micros(&self) -> u64 {
        self.clock
            .reading_micros()
            .saturating_add(self.clock.max_skew_micros().saturating_mul(2))
    }

    /// This node's amnesia fence, if it came back over a lost journal: above
    /// everything it may have voted on before (#312).
    fn amnesia_fence_micros(&self) -> Option<u64> {
        self.amnesia
            .lock()
            .expect("amnesia poisoned")
            .map(|fence| fence.fence_micros)
    }

    /// Whether a layout change refuses this node's vote for `txn`, which
    /// `replica` (locked by the caller, so no fence goes in meanwhile) does not
    /// hold, planned under `epoch`, touching `keys` (its route when it has
    /// one): a transition's fence, or an activation's clock floor (see
    /// [`reconfig`]). `Some(journaled)` when refused: whether the refusal
    /// rests on what the journal keeps (a fence not journaled yet could be
    /// recomputed after a restart, at another point).
    fn change_refusal(
        &self,
        replica: &Replica,
        txn: TxnId,
        epoch: u64,
        keys: &[Key],
    ) -> Option<bool> {
        if replica.status(txn).is_some() {
            return None;
        }
        let reconfig = self.reconfig.lock().expect("reconfig poisoned");
        let refused = if reconfig
            .floors
            .iter()
            .any(|f| f.refuses(epoch, txn.0, keys))
        {
            Some(true)
        } else {
            reconfig
                .fences
                .iter()
                .find(|f| f.refuses(epoch, txn.0, keys))
                .map(|f| f.journaled)
        };
        if refused.is_some() {
            self.metrics.record_vote_change_fenced();
        }
        refused
    }

    /// Whether `txn`, which this node does not hold, was planned under
    /// `layout`, an epoch since retired for one of `keys` (its route, when it
    /// has one): a key whose replica set changed after. Its coordinator planned
    /// before it installed the change, so it counts a quorum of replicas that
    /// no longer all hold the key — a commit it reached through them would
    /// miss the key's new replicas. Such a PreAccept, or read probe, gets no
    /// answer (counted); the coordinator retries under the new layout. A
    /// transaction this node already holds was proposed before the change,
    /// and is finished under its own layout as before.
    fn retired(&self, txn: TxnId, layout: &Layout, keys: &[Key]) -> bool {
        let current = self.topology.layout();
        if current.epoch() == layout.epoch()
            || !keys.iter().any(|k| !layout.same_replicas(&current, k))
        {
            return false;
        }
        if self
            .replica
            .lock()
            .expect("replica poisoned")
            .status(txn)
            .is_some()
        {
            return false;
        }
        self.metrics.record_vote_retired();
        tracing::debug!(
            node = self.id.0,
            ?txn,
            epoch = layout.epoch(),
            current = current.epoch(),
            "refusing a transaction planned under a layout retired since"
        );
        true
    }

    /// The distinct keys a set of events touches.
    fn keys_of(events: &[Event]) -> Vec<Key> {
        let mut seen: HashSet<std::borrow::Cow<'_, str>> = HashSet::new();
        let mut keys: Vec<Key> = Vec::new();
        for event in events {
            let key = Key::str_of(event);
            if !seen.contains(&key) {
                keys.push(Key(key.to_string()));
                seen.insert(key);
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
        self.bound_clock_past(stamp.micros).await?;
        Ok(stamp)
    }

    /// Persists a clock bound past `reading` (microseconds), unless one is
    /// already: restarted from its journal, the node starts its clock there,
    /// so nothing cut at that reading — a minted id, a redundancy point it
    /// reported, the vote fence a refusal relied on — is ever ahead of its
    /// clock again. Raised a skew window ahead, so the journal sees one write
    /// per window at most.
    async fn bound_clock_past(&self, reading: u64) -> anyhow::Result<()> {
        // A node whose journal its peers did not confirm (#339) mints, votes
        // and vouches for nothing, all of which pass here: an older copy of
        // its journal would have it mint ids it minted before.
        if self.rolled_back() {
            return Err(JournalRolledBack.into());
        }
        if !self.confirmed() {
            return Err(OpenUnconfirmed.into());
        }
        if reading < self.clock_bound.load(Ordering::Acquire) {
            return Ok(());
        }
        let _raising = self.clock_bound_lock.lock().await;
        if self.retired.load(Ordering::Acquire) {
            return Err(NodeClosed.into());
        }
        if reading < self.clock_bound.load(Ordering::Acquire) {
            return Ok(());
        }
        let ahead = self.clock.max_skew_micros().saturating_mul(2);
        let bound = reading
            .saturating_add(1)
            .max(self.clock.physical_micros().saturating_add(ahead));
        self.journal
            .save_clock_bound(bound)
            .await
            .map_err(|err| err.context("cannot persist the clock bound"))?;
        self.clock_bound.store(bound, Ordering::Release);
        Ok(())
    }

    /// [`bound_clock_past`](Self::bound_clock_past) this node's current
    /// reading, before a value cut at an earlier one leaves it. `false` when
    /// the bound cannot be persisted: the value must not be sent then.
    async fn bound_clock(&self) -> bool {
        self.bound_clock_past(self.clock.reading_micros())
            .await
            .is_ok()
    }

    /// Readies a report this node sends on its own — not with a handled
    /// batch, whose journal is flushed before its sends — for a peer to
    /// compact on: [`bound_clock`](Self::bound_clock), the vote fence its
    /// [`report`](Self::report) raised staged
    /// ([`stage_vote_fence`](Self::stage_vote_fence)), and the journal
    /// flushed, so the fence and everything the report counts as applied (an
    /// anti-entropy import, or a commit applied since the last batch) are
    /// held across a restart (#331, #337). A peer that compacted past it
    /// would otherwise ship it no more, and this node, restarted without it,
    /// could wait on it for good — or vote below it.
    async fn vouch_durably(&self) -> bool {
        if !self.bound_clock().await {
            return false;
        }
        let Some(fence) = self.stage_vote_fence().await else {
            return false;
        };
        match self.journal.flush().await {
            Ok(()) => {
                self.metrics.record_flush();
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .fence_durable(fence);
                true
            }
            Err(err) => {
                tracing::warn!(node = self.id.0, %err, "journal flush failed; withholding a report");
                false
            }
        }
    }

    /// Stages this node's vote fence as it stands for the next journal
    /// flush, unless it was staged already, and returns it: once that flush
    /// succeeds, the fence is durable up to it. Serialized, staging the
    /// fence read once the turn comes, so a stager that read an older fence
    /// can never be the last one staged. `None` when the journal refused it:
    /// nothing resting on the fence may leave the node then.
    async fn stage_vote_fence(&self) -> Option<Timestamp> {
        let _staging = self.vote_fence_lock.lock().await;
        let fence = self.replica.lock().expect("replica poisoned").vote_fence();
        if fence <= *self.staged_fence.lock().expect("fence poisoned") {
            return Some(fence);
        }
        match self.journal.save_vote_fence(fence).await {
            Ok(()) => {
                *self.staged_fence.lock().expect("fence poisoned") = fence;
                Some(fence)
            }
            Err(err) => {
                tracing::warn!(node = self.id.0, %err, "cannot stage the vote fence");
                None
            }
        }
    }

    /// Stops this node minting, for good: its host closed it, and the node
    /// reopening its journal starts from the clock bound persisted so far.
    /// Taken under the lock that raises the bound, so a bound being raised is
    /// persisted before this returns, and every later id or ballot it would
    /// mint fails with [`NodeClosed`] — a clone the application still holds can
    /// never mint an id the reopened node mints too. A host calls it before
    /// reopening the node over the same journal, then stops the node's inbox
    /// loop and recovery task before it discards the event store.
    pub async fn retire(&self) {
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
    async fn restore_clock(
        &self,
        commands: &[CommandState],
        bound: Option<u64>,
    ) -> anyhow::Result<()> {
        // The bound the journal holds, which this node's reports carry
        // (#339) and which no reading below it needs raised.
        if let Some(bound) = bound {
            self.clock_bound.fetch_max(bound, Ordering::AcqRel);
        }
        let bound = bound.map(|micros| Timestamp {
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

    /// Sets the [`AmnesiaFence`] of a node opened joining over a journal
    /// that holds nothing (#312): its wall clock plus three skews, above
    /// every `t0` it can have voted on or promised before the loss (none
    /// lies past its clock's reading plus two skews, see
    /// [`future_dated`](Self::future_dated), and the reading leads its wall
    /// clock by at most one) and every report it made (cut at its clock less
    /// the compaction margin). Journaled before anything else, then read
    /// back: a journal that cannot keep it would let a restart forget it.
    /// Assumes the wall clock did not step back across the loss.
    async fn set_amnesia_fence(&self) -> anyhow::Result<AmnesiaFence> {
        let skew = self.clock.max_skew_micros();
        let fence = AmnesiaFence {
            fence_micros: self
                .clock
                .physical_micros()
                .saturating_add(skew.saturating_mul(3))
                .saturating_add(1),
            joined: false,
            votes_restored: false,
        };
        self.journal
            .save_amnesia_fence(&fence)
            .await
            .map_err(|err| err.context("cannot persist the amnesia fence"))?;
        anyhow::ensure!(
            self.journal.load_amnesia_fence().await? == Some(fence),
            "this journal cannot keep an amnesia fence (Journal::save_amnesia_fence): \
             a node that lost its journal must come back under a new node id"
        );
        self.floor_clock_after_loss().await?;
        self.metrics.record_amnesia_fence();
        tracing::warn!(
            node = self.id.0,
            fence = fence.fence_micros,
            "opened joining over an empty journal: fenced until a contact vouches past it"
        );
        Ok(fence)
    }

    /// A plain open over a journal that holds nothing at all: a fresh
    /// replica, unless its data store holds events — then the journal that
    /// went with them was lost, and the node is refused ([`LostJournal`]),
    /// since it forgot the votes and reports it made. A cluster of this node
    /// alone holds nobody's votes but its own: it reopens over its store, its
    /// clock floored past the ids it minted. A fresh replica leaves its clock
    /// bound in the journal at once, so the journal is never empty again
    /// while the store holds what the node applies — even one it applied
    /// before its first flush.
    async fn open_over_an_empty_journal(&self) -> anyhow::Result<()> {
        let page = self.datastore.snapshot_page(None, 1, usize::MAX).await?;
        if page.events.is_empty() {
            return self
                .bound_clock_past(self.clock.reading_micros())
                .await
                .map_err(|err| err.context("cannot leave the clock bound in a fresh journal"));
        }
        if self.topology.nodes() != [self.id] {
            return Err(LostJournal {
                join_pending: false,
            }
            .into());
        }
        tracing::warn!(
            node = self.id.0,
            "the journal is empty but the store is not: a single-node cluster reopens over its store"
        );
        self.floor_clock_after_loss().await
    }

    /// Floors the clock past every id a node that lost its journal can have
    /// minted: its wall clock plus one skew (the clock is never pushed
    /// further by a peer, and a restart waits out any more), persisted as
    /// the clock bound. Not the fence itself: three skews ahead, its ids
    /// would run ahead of the wall clock past what peers accept.
    async fn floor_clock_after_loss(&self) -> anyhow::Result<()> {
        let floor = self
            .clock
            .physical_micros()
            .saturating_add(self.clock.max_skew_micros())
            .saturating_add(1);
        self.clock.restore(Timestamp {
            micros: floor,
            logical: 0,
            node: self.id,
        });
        self.bound_clock_past(floor).await
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
                        if let Some(shard) = Self::planned_shard(plans, from) {
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

    /// The shard `node` was planned in, if any: replies are counted for the
    /// shard the coordination planned their sender in, never by the layout
    /// installed when they land — an epoch installed meanwhile may have
    /// moved the sender, and the quorums are the plan's.
    fn planned_shard(plans: &[ShardPlan], node: NodeId) -> Option<ShardId> {
        plans
            .iter()
            .find(|p| p.replicas.contains(&node))
            .map(|p| p.shard)
    }

    /// Collects matching responses grouped by the responder's planned shard
    /// (see [`planned_shard`](Self::planned_shard)), stopping once every
    /// planned shard has `target(plan)` items (or at the deadline).
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
                        if let Some(shard) = Self::planned_shard(plans, from) {
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

    use crate::api::{
        ConfigEntry, DataStore, Drained, FenceRecord, Journal, ShardMap, ShardedTopology, Topology,
    };
    use crate::clock::{HybridLogicalClock, NodeId, Timestamp, TxnId};
    use crate::message::{CommandState, Key, Message, Status};
    use crate::store::{InMemoryDataStore, InMemoryJournal};
    use crate::transport::InMemoryNetwork;
    use crate::{api::MessageSink, Envelope, StaticTopology};
    use tokio::time::Duration;

    use super::{Node, NodeConfig, RecoverFields, RecoverResp};
    use crate::clock::Ballot;

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

    thread_local! {
        /// When this test built its first synthetic message. Transactions
        /// are stamped just after it, so they arrive within the compaction
        /// margin of the node's wall clock — not decades old, which the
        /// vote fence refuses.
        static BASE: u64 = crate::clock::system_micros();
    }

    /// `micros` after this test's [`BASE`]: a recent `t0`, the same on every
    /// call within the test.
    fn recent(micros: u64) -> u64 {
        BASE.with(|base| base + micros)
    }

    fn preaccept(i: u64) -> Message {
        let txn = TxnId(Timestamp {
            micros: recent(i + 1),
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
            epoch: 0,
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

        let (plans, union) = Node::plan(&node.topology.layout(), &keys, &events);

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
            let (plans, _) = Node::plan(&node.topology.layout(), std::slice::from_ref(&key), &[]);
            let known: Vec<(NodeId, RecoverFields)> = votes
                .iter()
                .map(|&(id, vote)| {
                    let report = RecoverFields {
                        epoch: 0,
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
                epoch: 0,
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
        let Message::PreAccept {
            txn, keys, events, ..
        } = preaccept(0)
        else {
            unreachable!()
        };
        let out = node
            .handle_staged(Envelope {
                from: coord,
                message: Message::Commit {
                    epoch: 0,
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

    /// A node over a `DynamicTopology` at epoch 0 — shards `{0, 1, 2}` and
    /// `{3, 4, 5}` — and events whose aggregates land in both shards.
    fn reshardable_node() -> (Node, Arc<crate::api::DynamicTopology>, Vec<Event>) {
        let id = NodeId(0);
        let topology = Arc::new(crate::api::DynamicTopology::new(
            id,
            0,
            vec![(0..3).map(NodeId).collect(), (3..6).map(NodeId).collect()],
        ));
        let net = InMemoryNetwork::new();
        let node = Node::new(
            id,
            Arc::clone(&topology) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );
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
        assert!(
            (0..2).all(|s| events.iter().any(|e| topology.shard_of(&Key::of(e)) == s)),
            "test aggregates must span both shards"
        );
        (node, topology, events)
    }

    /// The epoch-1 layout: nodes 2 and 3 swap shards.
    fn swapped() -> Vec<Vec<NodeId>> {
        vec![
            vec![NodeId(0), NodeId(1), NodeId(3)],
            vec![NodeId(2), NodeId(4), NodeId(5)],
        ]
    }

    /// Replies are counted for the shard the coordinator **planned** their
    /// sender in, even when a layout installed meanwhile moved the sender to
    /// another shard: the quorums are the plan's.
    #[tokio::test]
    async fn replies_are_counted_by_the_plan_not_the_live_layout() {
        use std::collections::HashSet;
        use tokio::time::{Duration, Instant};

        let (node, topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &events);
        let planned = |n: u64| {
            plans
                .iter()
                .find(|p| p.replicas.contains(&NodeId(n)))
                .expect("planned")
                .shard
        };
        let (shard_of_2, shard_of_3) = (planned(2), planned(3));
        topology.install(1, swapped());

        let watermark = |micros: u64| Message::Watermark {
            applied_through: Timestamp {
                micros,
                logical: 0,
                node: NodeId(micros),
            },
            applied: Timestamp::MIN,
            mark: crate::api::DurableMark::default(),
            fenced: None,
            copied: None,
            live_since: 0,
        };
        let micros_of = |m: Message| match m {
            Message::Watermark {
                applied_through, ..
            } => Some(applied_through.micros),
            _ => None,
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for n in [2, 3] {
            tx.send((NodeId(n), watermark(n))).unwrap();
        }
        drop(tx);
        let deadline = Instant::now() + Duration::from_millis(200);
        let tagged = node
            .collect_by_shard_tagged(
                &mut rx,
                &plans,
                |_, _| false,
                deadline,
                HashSet::new(),
                micros_of,
            )
            .await;
        let responders = |shard| {
            tagged[&shard]
                .iter()
                .map(|(from, _)| *from)
                .collect::<Vec<_>>()
        };
        assert_eq!(responders(shard_of_2), vec![NodeId(2)]);
        assert_eq!(responders(shard_of_3), vec![NodeId(3)]);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for n in [2, 3] {
            tx.send((NodeId(n), watermark(n))).unwrap();
        }
        drop(tx);
        let counted = node
            .collect_by_shard(&mut rx, &plans, |_| usize::MAX, deadline, micros_of)
            .await;
        assert_eq!(counted[&shard_of_2], vec![2]);
        assert_eq!(counted[&shard_of_3], vec![3]);
    }

    /// A routed recovery attributes each report to the shard it planned:
    /// two of shard B's planned replicas never witnessed the transaction,
    /// so it never committed — even though a layout installed meanwhile
    /// moved one of them to shard A.
    #[tokio::test]
    async fn routed_recovery_counts_reports_by_the_plan_not_the_live_layout() {
        use super::{RecoverFields, RecoverResp, Verdict};
        use crate::clock::Ballot;

        let (node, topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &[]);
        topology.install(1, swapped());

        let held = || {
            RecoverResp::Ok(RecoverFields {
                epoch: 0,
                known: true,
                status: Status::PreAccepted,
                accepted: Ballot(Timestamp::MIN),
                execute_at: Timestamp::MIN,
                deps: Vec::new(),
                superseding_rejects: false,
                wait: false,
                keys: Vec::new(),
                events: Vec::new(),
                route: None,
            })
        };
        let blind = || RecoverResp::NotWitnessed { redundant: false };
        let resp = vec![
            (NodeId(0), held()),
            (NodeId(1), held()),
            (NodeId(3), blind()),
            (NodeId(4), blind()),
        ];
        assert!(node.route_settled(&plans, &resp));
        assert!(
            matches!(node.route_verdict(&plans, &resp), Verdict::Invalidate),
            "a slow quorum of the planned shard never witnessed it"
        );
    }

    /// A report builder for the routed tallies below: a replica holding
    /// the transaction at `status`, with `events` as its shard's part.
    fn holder(status: Status, events: Vec<Event>) -> RecoverResp {
        RecoverResp::Ok(fields(status, events))
    }

    /// [`holder`]'s report.
    fn fields(status: Status, events: Vec<Event>) -> RecoverFields {
        RecoverFields {
            epoch: 0,
            known: true,
            status,
            accepted: Ballot(Timestamp::MIN),
            execute_at: Timestamp::MIN,
            deps: Vec::new(),
            superseding_rejects: false,
            wait: false,
            keys: events.iter().map(Key::of).collect(),
            events,
            route: None,
        }
    }

    /// A replica that holds nothing of the transaction and promised the
    /// ballot, waiting to be handed its part (#377).
    fn promised() -> RecoverResp {
        RecoverResp::Ok(RecoverFields {
            epoch: 0,
            known: false,
            status: Status::PreAccepted,
            accepted: Ballot(Timestamp::MIN),
            execute_at: Timestamp::MIN,
            deps: Vec::new(),
            superseding_rejects: false,
            wait: false,
            keys: Vec::new(),
            events: Vec::new(),
            route: None,
        })
    }

    /// Two replicas of a shard that promised the ballot and hold nothing
    /// are no proof the transaction never committed: each can still vote
    /// once handed its part (#377). Only a fenced or refused answer counts.
    #[tokio::test]
    async fn a_promised_replica_never_rules_a_commit_out() {
        use super::Verdict;

        let (node, _topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &[]);
        let resp = vec![
            (NodeId(0), holder(Status::PreAccepted, Vec::new())),
            (NodeId(1), promised()),
            (NodeId(2), promised()),
            (NodeId(3), holder(Status::PreAccepted, Vec::new())),
            (NodeId(4), holder(Status::PreAccepted, Vec::new())),
            (NodeId(5), holder(Status::PreAccepted, Vec::new())),
        ];
        assert!(node.route_settled(&plans, &resp), "every replica answered");
        assert!(
            matches!(node.route_verdict(&plans, &resp), Verdict::Undecided(_)),
            "a promise alone proves nothing"
        );
        assert!(!node.never_commits(&plans, &resp, true));
    }

    /// Two late replicas of a shard of three would, refusing for good,
    /// prove the transaction never committed there: the recovery asks them
    /// to (#377). Until they did, nothing is decided.
    #[tokio::test]
    async fn late_replicas_are_refused_when_that_proves_no_commit() {
        use super::Verdict;

        let (node, _topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &[]);
        let mut resp = vec![
            (NodeId(0), holder(Status::PreAccepted, Vec::new())),
            (NodeId(1), RecoverResp::Late),
            (NodeId(2), RecoverResp::Late),
            (NodeId(3), holder(Status::PreAccepted, Vec::new())),
            (NodeId(4), holder(Status::PreAccepted, Vec::new())),
            (NodeId(5), holder(Status::PreAccepted, Vec::new())),
        ];
        assert!(node.route_settled(&plans, &resp));
        assert!(matches!(
            node.route_verdict(&plans, &resp),
            Verdict::Undecided(_)
        ));
        assert!(!node.never_commits(&plans, &resp, false));
        assert!(
            node.never_commits(&plans, &resp, true),
            "refused, the late replicas meet every quorum of their shard"
        );
        assert!(!node.may_have_committed(&plans, &resp));
        for (_, report) in resp
            .iter_mut()
            .filter(|(_, r)| matches!(r, RecoverResp::Late))
        {
            *report = RecoverResp::NotWitnessed { redundant: false };
        }
        assert!(matches!(
            node.route_verdict(&plans, &resp),
            Verdict::Invalidate
        ));
    }

    /// In a shard of three, a holder that only pre-accepted the
    /// transaction, a replica that lost its journal and one late replica:
    /// were the late one to refuse for good, no quorum could have committed
    /// it — a fast one needs the late replica, a slow one an accept the
    /// holder never gave — nor ever will, so it is asked to refuse (#377).
    /// Refused, the verdict alone is still undecided, one refusal meeting no
    /// quorum by itself; the fence rule beside the amnesiac and the holder
    /// decides it (#386).
    #[tokio::test]
    async fn an_amnesiac_a_holder_and_one_refusal_rule_a_commit_out() {
        use super::Verdict;

        let (node, _topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &[]);
        let mut resp = vec![
            (NodeId(0), holder(Status::PreAccepted, Vec::new())),
            (NodeId(1), RecoverResp::Amnesiac),
            (NodeId(2), RecoverResp::Late),
            (NodeId(3), holder(Status::PreAccepted, Vec::new())),
            (NodeId(4), holder(Status::PreAccepted, Vec::new())),
            (NodeId(5), holder(Status::PreAccepted, Vec::new())),
        ];
        assert!(node.route_settled(&plans, &resp));
        assert!(matches!(
            node.route_verdict(&plans, &resp),
            Verdict::Undecided(_)
        ));
        assert!(
            !node.never_commits(&plans, &resp, false),
            "the late replica may still vote"
        );
        assert!(
            node.never_commits(&plans, &resp, true),
            "refused, it leaves no quorum that could have committed or could vote"
        );
        assert!(!node.may_have_committed(&plans, &resp));
        resp[2].1 = RecoverResp::NotWitnessed { redundant: false };
        assert!(
            matches!(node.route_verdict(&plans, &resp), Verdict::Undecided(_)),
            "one refusal meets no quorum by itself"
        );
        assert!(
            node.never_commits(&plans, &resp, false),
            "beside the amnesiac and a holder that never accepted, it rules a commit out"
        );
    }

    /// A replica that accepted the transaction and one that lost its
    /// journal make a slow quorum that may have committed it: the late
    /// replica of their shard is asked to vote, not to refuse (#377).
    #[tokio::test]
    async fn an_amnesiac_and_an_accept_let_a_late_replica_vote() {
        let (node, _topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &[]);
        let resp = vec![
            (NodeId(0), holder(Status::Accepted, Vec::new())),
            (NodeId(1), RecoverResp::Amnesiac),
            (NodeId(2), RecoverResp::Late),
            (NodeId(3), holder(Status::Accepted, Vec::new())),
            (NodeId(4), holder(Status::Accepted, Vec::new())),
            (NodeId(5), holder(Status::Accepted, Vec::new())),
        ];
        assert!(!node.never_commits(&plans, &resp, true));
        assert!(node.may_have_committed(&plans, &resp));
    }

    /// A planned shard whose part no holder reported is never committed on
    /// its voters alone: they were handed nothing to vote for (#377). One a
    /// report shows committed carries its decision instead.
    #[tokio::test]
    async fn no_part_is_committed_that_no_holder_reported() {
        let (node, topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let first: Vec<Event> = events
            .iter()
            .filter(|e| topology.shard_of(&Key::of(e)) == 0)
            .cloned()
            .collect();
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &first);
        let second = plans
            .iter()
            .find(|p| p.events.is_empty())
            .expect("a shard with no part");
        let known = vec![
            (NodeId(0), fields(Status::PreAccepted, first.clone())),
            (NodeId(1), fields(Status::PreAccepted, first.clone())),
            (NodeId(3), fields(Status::PreAccepted, Vec::new())),
            (NodeId(4), fields(Status::PreAccepted, Vec::new())),
        ];
        assert_eq!(Node::unreported_part(&plans, &known), Some(second.shard));
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &events);
        assert_eq!(Node::unreported_part(&plans, &known), None);
        let (plans, _) = Node::plan(&node.topology.layout(), &keys, &first);
        let decided = vec![
            (NodeId(0), fields(Status::PreAccepted, first.clone())),
            (NodeId(3), fields(Status::Applied, Vec::new())),
        ];
        assert_eq!(Node::unreported_part(&plans, &decided), None);
    }

    /// A shard of four commits through three of its replicas: two that never
    /// witnessed a transaction meet every such quorum, and rule its commit
    /// out as a slow quorum of them would. Two holding it and two blind is
    /// an invalidation; counting slow quorums only, it was undecided for
    /// good, and the transaction held back every write after it on its keys.
    #[tokio::test]
    async fn two_blind_replicas_of_four_rule_out_a_commit() {
        use super::{RecoverFields, RecoverResp, Verdict};
        use crate::clock::Ballot;

        let (node, _topology, events) = reshardable_node();
        let keys = Node::keys_of(&events);
        let layout = crate::api::Layout::new(
            0,
            vec![(0..4).map(NodeId).collect(), (4..7).map(NodeId).collect()],
        );
        let (plans, _) = Node::plan(&layout, &keys, &[]);
        assert!(plans.iter().any(|p| p.replicas.len() == 4));

        let held = || {
            RecoverResp::Ok(RecoverFields {
                epoch: 0,
                known: true,
                status: Status::PreAccepted,
                accepted: Ballot(Timestamp::MIN),
                execute_at: Timestamp::MIN,
                deps: Vec::new(),
                superseding_rejects: false,
                wait: false,
                keys: Vec::new(),
                events: Vec::new(),
                route: None,
            })
        };
        let blind = || RecoverResp::NotWitnessed { redundant: false };
        let mut resp = vec![
            (NodeId(0), held()),
            (NodeId(1), held()),
            (NodeId(2), blind()),
            (NodeId(4), held()),
            (NodeId(5), held()),
        ];
        assert!(
            !matches!(node.route_verdict(&plans, &resp), Verdict::Invalidate),
            "one blind replica of four rules nothing out"
        );
        resp.push((NodeId(3), blind()));
        assert!(node.route_settled(&plans, &resp));
        assert!(
            matches!(node.route_verdict(&plans, &resp), Verdict::Invalidate),
            "two blind replicas of four rule the commit out"
        );
    }

    /// A replica that holds nothing of a transaction and, under the layout
    /// the transaction was planned under, owns none of the keys a message
    /// carries is no replica of the shard it was sent as: it casts no vote,
    /// answers nothing and records nothing — a misrouted message must not be
    /// counted.
    #[tokio::test]
    async fn a_replica_that_owns_none_of_the_keys_casts_no_vote() {
        use crate::clock::Ballot;

        let (node, topology, events) = reshardable_node();
        let event = events
            .iter()
            .find(|e| topology.owns(NodeId(0), &Key::of(e)))
            .expect("an event node 0 owns")
            .clone();
        let key = Key::of(&event);
        // Epoch 1: node 0 moves to the other shard.
        topology.install(
            1,
            vec![
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(0), NodeId(4), NodeId(5)],
            ],
        );
        assert!(!topology.owns(NodeId(0), &key));

        let txn = TxnId(Timestamp {
            micros: recent(1_000),
            logical: 0,
            node: NodeId(9),
        });
        // Planned under epoch 1, where node 0 owns none of the key.
        let messages = [
            Message::PreAccept {
                epoch: 1,
                txn,
                keys: vec![key.clone()],
                events: vec![event.clone()],
            },
            Message::Accept {
                epoch: 1,
                txn,
                ballot: Ballot(txn.0),
                execute_at: txn.0,
                deps: Vec::new(),
                keys: vec![key.clone()],
            },
            Message::Commit {
                epoch: 1,
                txn,
                execute_at: txn.0,
                deps: Vec::new(),
                keys: vec![key.clone()],
                events: vec![event.clone()],
                reply_to: NodeId(9),
            },
        ];
        for message in messages {
            let out = node
                .handle_staged(Envelope {
                    from: NodeId(9),
                    message,
                })
                .await;
            assert!(out.is_empty(), "no vote and no answer: {out:?}");
            assert_eq!(node.command_status(txn), None, "nothing recorded");
        }
    }

    /// A replica a layout moved off a key still takes part in a transaction
    /// planned before the move, as one of the replicas it was planned for: a
    /// decision reached under that layout is applied, and an Accept a
    /// recovery of it sends is answered. Only a new vote — a PreAccept from
    /// a coordinator that planned before the change — is refused.
    #[tokio::test]
    async fn an_old_owner_finishes_a_transaction_planned_before_the_move() {
        use crate::clock::Ballot;

        let (node, topology, events) = reshardable_node();
        let event = events
            .iter()
            .find(|e| topology.owns(NodeId(0), &Key::of(e)))
            .expect("an event node 0 owns")
            .clone();
        let key = Key::of(&event);
        topology.install(
            1,
            vec![
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(0), NodeId(4), NodeId(5)],
            ],
        );

        let stale = TxnId(Timestamp {
            micros: recent(1_000),
            logical: 0,
            node: NodeId(9),
        });
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::PreAccept {
                    epoch: 0,
                    txn: stale,
                    keys: vec![key.clone()],
                    events: vec![event.clone()],
                },
            })
            .await;
        assert!(
            out.is_empty(),
            "no new vote under a retired layout: {out:?}"
        );

        let accepted = TxnId(Timestamp {
            micros: recent(2_000),
            logical: 0,
            node: NodeId(9),
        });
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::Accept {
                    epoch: 0,
                    txn: accepted,
                    ballot: Ballot(accepted.0),
                    execute_at: accepted.0,
                    deps: Vec::new(),
                    keys: vec![key.clone()],
                },
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::AcceptOk { .. })]),
            "{out:?}"
        );

        let committed = TxnId(Timestamp {
            micros: recent(3_000),
            logical: 0,
            node: NodeId(9),
        });
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::Commit {
                    epoch: 0,
                    txn: committed,
                    execute_at: committed.0,
                    deps: Vec::new(),
                    keys: vec![key.clone()],
                    events: vec![event.clone()],
                    reply_to: NodeId(9),
                },
            })
            .await;
        assert!(
            out.iter()
                .any(|(_, m)| matches!(m, Message::ReadOk { ok: true, .. })),
            "{out:?}"
        );
        assert!(node.command_status(committed) >= Some(Status::Committed));
    }

    /// A replica that witnessed a transaction while it owned its keys still
    /// finishes it after a layout moved them away: the guard against stale
    /// coordinators applies only to transactions it holds nothing of.
    #[tokio::test]
    async fn an_old_owner_still_finishes_a_transaction_it_holds() {
        use crate::clock::Ballot;

        let (node, topology, events) = reshardable_node();
        let event = events
            .iter()
            .find(|e| topology.owns(NodeId(0), &Key::of(e)))
            .expect("an event node 0 owns")
            .clone();
        let key = Key::of(&event);
        let txn = TxnId(Timestamp {
            micros: recent(1_000),
            logical: 0,
            node: NodeId(9),
        });
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::PreAccept {
                    epoch: 0,
                    txn,
                    keys: vec![key.clone()],
                    events: vec![event.clone()],
                },
            })
            .await;
        assert!(matches!(out.as_slice(), [(_, Message::PreAcceptOk { .. })]));

        topology.install(
            1,
            vec![
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(0), NodeId(4), NodeId(5)],
            ],
        );
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::Accept {
                    epoch: 0,
                    txn,
                    ballot: Ballot(txn.0),
                    execute_at: txn.0,
                    deps: Vec::new(),
                    keys: vec![key.clone()],
                },
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::AcceptOk { .. })]),
            "{out:?}"
        );
    }

    /// Node 0 of a single shard `{0, 1, 2}` over a dynamic topology at epoch
    /// 0, with its journal, not started: messages are injected.
    fn replaceable_node(journal: Arc<InMemoryJournal>) -> (Node, Arc<crate::api::DynamicTopology>) {
        let id = NodeId(0);
        let topology = Arc::new(crate::api::DynamicTopology::new(
            id,
            0,
            vec![(0..3).map(NodeId).collect()],
        ));
        let net = InMemoryNetwork::new();
        let node = Node::new(
            id,
            Arc::clone(&topology) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            journal as Arc<dyn Journal>,
        );
        (node, topology)
    }

    /// The transition that replaces node 2 with node 3, decided at epoch 1.
    fn replacing_two() -> ConfigEntry {
        ConfigEntry::Transition {
            active: ShardMap::even(vec![(0..3).map(NodeId).collect()]),
            pending: ShardMap::even(vec![vec![NodeId(0), NodeId(1), NodeId(3)]]),
            since: 1,
        }
    }

    fn preaccept_at(micros: u64, epoch: u64) -> (TxnId, Message) {
        let txn = TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        });
        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".into(),
            aggregate_id: "acc".into(),
            version: 1,
            name: "Opened".into(),
            ..Default::default()
        };
        let message = Message::PreAccept {
            txn,
            epoch,
            keys: vec![Key::of(&event)],
            events: vec![event],
        };
        (txn, message)
    }

    async fn commit_entry(node: &Node, epoch: u64, entry: ConfigEntry) {
        node.handle_staged(Envelope {
            from: NodeId(1),
            message: Message::ConfigCommit { epoch, entry },
        })
        .await;
    }

    /// The fence point a replica takes as it installs a transition is above
    /// every vote it cast, one for a `t0` its coordinator dated up to two skews
    /// past this replica's clock included. Above the point, it votes for no
    /// transaction it never witnessed on a moved key; below, it does.
    #[tokio::test]
    async fn a_transition_fence_covers_a_vote_dated_past_the_clock() {
        let (node, _topology) = replaceable_node(Arc::new(InMemoryJournal::new()));
        let skew = node.clock.max_skew_micros();
        let (ahead, message) = preaccept_at(node.clock.reading_micros() + skew * 3 / 2, 0);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message,
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::PreAcceptOk { .. })]),
            "{out:?}"
        );

        commit_entry(&node, 1, replacing_two()).await;
        let cap = {
            let reconfig = node.reconfig.lock().unwrap();
            assert!(reconfig.transition.is_some());
            let fence = &reconfig.fences[0];
            assert!(fence.journaled, "journaled before it is reported");
            fence.cap
        };
        assert!(cap > ahead.0, "{cap:?} <= {ahead:?}");

        // Once the clock moved on, a coordinator may date a vote past the
        // cap: refused, as is any never-witnessed vote above it.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (late, message) = preaccept_at(cap.micros + 1, 0);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message,
            })
            .await;
        assert!(out.is_empty(), "no vote above the fence point: {out:?}");
        assert_eq!(node.command_status(late), None);
        assert_eq!(node.metrics().votes_change_fenced, 1);
        // Below the point, a transaction never witnessed is voted for.
        let (early, message) = preaccept_at(ahead.0.micros + 1, 0);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message,
            })
            .await;
        assert!(
            matches!(out.as_slice(), [(_, Message::PreAcceptOk { .. })]),
            "{out:?}"
        );
        assert!(node.command_status(early).is_some());
    }

    /// A replica restarted mid-transition comes back with the fence point it
    /// journaled, not one recomputed from its restarted clock.
    #[tokio::test]
    async fn a_restarted_replica_keeps_its_fence() {
        let journal = Arc::new(InMemoryJournal::new());
        let (node, _topology) = replaceable_node(Arc::clone(&journal));
        commit_entry(&node, 1, replacing_two()).await;
        let cap = node.reconfig.lock().unwrap().fences[0].cap;
        assert_eq!(
            journal.load_fences().await.unwrap(),
            vec![FenceRecord { since: 1, cap }]
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        let (restarted, topology) = replaceable_node(Arc::clone(&journal));
        restarted.recover_state().await.unwrap();
        assert_eq!(topology.epoch(), 1);
        let reconfig = restarted.reconfig.lock().unwrap();
        assert_eq!(reconfig.transition.as_ref().map(|t| t.since), Some(1));
        assert_eq!(reconfig.fences.len(), 1);
        assert_eq!(reconfig.fences[0].cap, cap, "the journaled point");
        assert!(reconfig.fences[0].journaled);
    }

    /// An activation invalidates what a replica holds unapplied on a moved key
    /// above the drain point — no fenced replica votes for it, so it never
    /// commits — and keeps what lies below, which drained elsewhere.
    #[tokio::test]
    async fn activation_invalidates_what_can_never_commit() {
        let (node, topology) = replaceable_node(Arc::new(InMemoryJournal::new()));
        let now = node.clock.reading_micros();
        let (below, message) = preaccept_at(now, 0);
        node.handle_staged(Envelope {
            from: NodeId(9),
            message,
        })
        .await;
        let (above, message) = preaccept_at(now + 10, 0);
        node.handle_staged(Envelope {
            from: NodeId(9),
            message,
        })
        .await;
        commit_entry(&node, 1, replacing_two()).await;
        let point = Timestamp {
            micros: now + 5,
            logical: 0,
            node: NodeId(0),
        };
        commit_entry(
            &node,
            2,
            ConfigEntry::Active {
                layout: ShardMap::even(vec![vec![NodeId(0), NodeId(1), NodeId(3)]]),
                drained: Some(Drained {
                    point,
                    floor: Timestamp {
                        micros: now + 1_000,
                        logical: 0,
                        node: NodeId(0),
                    },
                }),
            },
        )
        .await;
        assert_eq!(topology.epoch(), 2);
        assert_eq!(node.command_status(below), Some(Status::PreAccepted));
        let invalidated = node.replica.lock().unwrap().snapshot(above).unwrap();
        assert!(invalidated.invalidated(), "{invalidated:?}");
        {
            let reconfig = node.reconfig.lock().unwrap();
            assert!(reconfig.transition.is_none());
            assert_eq!(reconfig.fences[0].end, Some(2));
            assert_eq!(reconfig.live_since, 2);
        }
        assert!(
            node.clock.reading_micros() >= now + 1_000,
            "the clock floor"
        );

        // Planned under the new layout below the floor: no vote.
        let (low, message) = preaccept_at(now + 500, 2);
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message,
            })
            .await;
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(node.command_status(low), None);
    }

    /// A transition replaced by another change keeps its fence for what was
    /// planned before it ended — a vote it refused stays refused — and fences
    /// nothing planned after: the replacing change moves other keys.
    #[tokio::test]
    async fn a_superseded_transition_keeps_refusing_what_it_fenced() {
        let (node, topology, events) = reshardable_node();
        let in_shard = |shard: usize| {
            events
                .iter()
                .find(|e| topology.shard_of(&Key::of(e)) == shard)
                .expect("an event per shard")
                .clone()
        };
        let (zero, one) = (in_shard(0), in_shard(1));
        let shards = |a: [u64; 3], b: [u64; 3]| {
            ShardMap::even(vec![a.map(NodeId).to_vec(), b.map(NodeId).to_vec()])
        };
        // Epoch 1 moves shard 1 (5 replaced by 6); epoch 2 replaces that
        // change by one moving shard 0 (2 replaced by 7) only.
        commit_entry(
            &node,
            1,
            ConfigEntry::Transition {
                active: shards([0, 1, 2], [3, 4, 5]),
                pending: shards([0, 1, 2], [3, 4, 6]),
                since: 1,
            },
        )
        .await;
        commit_entry(
            &node,
            2,
            ConfigEntry::Transition {
                active: shards([0, 1, 2], [3, 4, 5]),
                pending: shards([0, 1, 7], [3, 4, 5]),
                since: 2,
            },
        )
        .await;
        let (first, second) = {
            let reconfig = node.reconfig.lock().unwrap();
            assert_eq!(reconfig.transition.as_ref().map(|t| t.since), Some(2));
            assert_eq!(
                reconfig.fences[0].end,
                Some(2),
                "ended by the replacing change"
            );
            (reconfig.fences[0].cap, reconfig.fences[1].cap)
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        let past = |cap: Timestamp| {
            TxnId(Timestamp {
                micros: cap.micros + 1,
                logical: 0,
                node: NodeId(9),
            })
        };
        let refuses = |txn: TxnId, epoch: u64, event: &Event| {
            let replica = node.replica.lock().unwrap();
            node.change_refusal(&replica, txn, epoch, &[Key::of(event)])
                .is_some()
        };
        // Planned before the first change ended, on the key it moved: refused
        // for good.
        assert!(refuses(past(first), 1, &one));
        // Planned after it ended, on that key: the replacing change keeps it.
        assert!(!refuses(past(first.max(second)), 2, &one));
        // The replacing change fences the key it moves.
        assert!(refuses(past(second), 2, &zero));
    }

    /// A range page holds only the slots asked for, from the store's order.
    #[tokio::test]
    async fn a_range_page_holds_only_the_slots_asked_for() {
        let (node, _topology) = replaceable_node(Arc::new(InMemoryJournal::new()));
        let events: Vec<Event> = (0..64)
            .map(|i| Event {
                id: ulid::Ulid::generate(),
                aggregate_type: "test/Account".into(),
                aggregate_id: format!("acc{i}"),
                version: 1,
                name: "Opened".into(),
                ..Default::default()
            })
            .collect();
        node.datastore
            .apply(TxnId(Timestamp::MIN), Timestamp::MIN, events.clone(), true)
            .await
            .unwrap();
        let wanted: Vec<u16> = events
            .iter()
            .take(3)
            .map(|e| crate::api::slot_of(&Key::of(e)) as u16)
            .collect();
        let out = node
            .handle_staged(Envelope {
                from: NodeId(9),
                message: Message::RangePageRequest {
                    since: None,
                    id: 7,
                    slots: wanted.clone(),
                    after: None,
                },
            })
            .await;
        let [(
            to,
            Message::RangePage {
                id,
                events: page,
                next,
                stable,
            },
        )] = out.as_slice()
        else {
            panic!("{out:?}");
        };
        assert_eq!((*to, *id), (NodeId(9), 7));
        assert!(next.is_none(), "the store fits a page");
        assert!(stable.is_some(), "the page carries the stable point (#373)");
        let expected = events
            .iter()
            .filter(|e| wanted.contains(&(crate::api::slot_of(&Key::of(e)) as u16)))
            .count();
        assert!(expected >= 3);
        assert_eq!(page.len(), expected);
        assert!(page
            .iter()
            .all(|e| wanted.contains(&(crate::api::slot_of(&Key::of(e)) as u16))));
    }

    /// The state an acceptor that lost its journal adopts from the others
    /// (#338): per epoch, the highest ballot any of them promised or
    /// accepted, and the value accepted under the highest ballot.
    #[test]
    fn merged_acceptor_state_takes_the_highest_ballots() {
        use crate::api::AcceptorRecord;
        use crate::clock::Ballot;

        let ballot = |micros: u64, node: u64| {
            Ballot(Timestamp {
                micros,
                logical: 0,
                node: NodeId(node),
            })
        };
        let layout = |n: u64| ConfigEntry::active(vec![(0..n).map(NodeId).collect()]);
        let x: Vec<(u64, AcceptorRecord)> = vec![
            (
                1,
                AcceptorRecord {
                    promised: ballot(10, 0),
                    accepted: Some((ballot(10, 0), layout(3))),
                },
            ),
            (
                2,
                AcceptorRecord {
                    promised: ballot(50, 1),
                    accepted: None,
                },
            ),
        ];
        let y: Vec<(u64, AcceptorRecord)> = vec![
            (
                1,
                AcceptorRecord {
                    promised: ballot(12, 1),
                    accepted: Some((ballot(12, 1), layout(4))),
                },
            ),
            (
                2,
                AcceptorRecord {
                    promised: ballot(40, 2),
                    accepted: Some((ballot(40, 2), layout(5))),
                },
            ),
            (
                3,
                AcceptorRecord {
                    promised: ballot(1, 1),
                    accepted: None,
                },
            ),
        ];
        let merged = Node::merge_acceptors([x.as_slice(), y.as_slice()]);
        assert_eq!(merged.len(), 3);
        // Epoch 1: Y accepted under the higher ballot.
        assert_eq!(merged[&1].promised, ballot(12, 1));
        assert_eq!(merged[&1].accepted, Some((ballot(12, 1), layout(4))));
        // Epoch 2: X's promise is above Y's acceptance, which stands.
        assert_eq!(merged[&2].promised, ballot(50, 1));
        assert_eq!(merged[&2].accepted, Some((ballot(40, 2), layout(5))));
        // Epoch 3: a promise alone.
        assert_eq!(merged[&3].promised, ballot(1, 1));
        assert_eq!(merged[&3].accepted, None);
        // An acceptance above every promise raises the promised ballot too.
        let z: Vec<(u64, AcceptorRecord)> = vec![(
            1,
            AcceptorRecord {
                promised: ballot(5, 0),
                accepted: Some((ballot(20, 2), layout(3))),
            },
        )];
        let merged = Node::merge_acceptors([z.as_slice()]);
        assert_eq!(merged[&1].promised, ballot(20, 2));
        assert!(Node::merge_acceptors(std::iter::empty()).is_empty());
    }
}
