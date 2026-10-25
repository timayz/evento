//! Wire messages and per-transaction command state.
//!
//! The protocol vocabulary: the PreAccept → Accept → Commit → Read → Apply
//! phases (ballot-fenced), plus the recovery exchange (`Recover` / `RecoverOk` /
//! `Nack`) a live node uses to take over a transaction whose coordinator is
//! presumed dead. `Commit` carries the events so a replica that missed PreAccept
//! (it was slow or down) can still apply, and the per-shard dependency set for
//! that shard's execution barrier.

use std::borrow::Cow;

use evento_core::{
    cursor::{Args, PageInfo, Value},
    Event, EventFilter, RoutingKey, TenantFilter,
};
use serde::{Deserialize, Serialize};

use crate::clock::{Ballot, NodeId, Timestamp, TxnId};

/// Sharding / ownership unit. Derived from an event's routing key (falling back
/// to its aggregate id), in its tenant, this is what the
/// [`Topology`](crate::api::Topology) maps to a replica set. Two transactions
/// *conflict* when they share a key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(pub String);

/// Separates a tenant from the routing key or aggregate id in a [`Key`].
const TENANT_SEPARATOR: char = '\u{1f}';

impl Key {
    /// The key a write touches: its routing key, or the aggregate id when no
    /// routing key is set — in the event's tenant (see [`in_tenant`](Key::in_tenant)).
    pub fn of(event: &Event) -> Self {
        Key(Self::str_of(event).into_owned())
    }

    /// [`of`](Key::of), borrowed whenever it can be — for hashing/grouping hot
    /// paths that only need to compare or bucket by the key string.
    pub fn str_of(event: &Event) -> Cow<'_, str> {
        Self::str_in(
            &event.tenant,
            event.routing_key.as_deref().unwrap_or(&event.aggregate_id),
        )
    }

    /// The key of `base` (a routing key or an aggregate id) in `tenant`. The
    /// default tenant's keys are the bare `base`, as before tenants existed;
    /// another tenant's carry it in front, so the same aggregate id in two
    /// tenants is two keys — two transactions that do not conflict. (A
    /// tenant and an id that spell the same key only add a false conflict:
    /// over-ordering, never a missed one.)
    pub fn in_tenant(tenant: &str, base: &str) -> Self {
        Key(Self::str_in(tenant, base).into_owned())
    }

    /// [`in_tenant`](Key::in_tenant), borrowed in the default tenant.
    fn str_in<'a>(tenant: &str, base: &'a str) -> Cow<'a, str> {
        if tenant.is_empty() {
            Cow::Borrowed(base)
        } else {
            Cow::Owned(format!("{tenant}{TENANT_SEPARATOR}{base}"))
        }
    }
}

/// Serde default for [`Message::SyncData`]'s `coverage`: a peer predating the
/// field decodes as "no coverage proven", which is conservatively ignored.
fn min_timestamp() -> Timestamp {
    Timestamp::MIN
}

/// How far a transaction has progressed on a given replica. Ordered so that
/// `>= Committed` means the execution timestamp and dependencies are final.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Status {
    /// PreAccepted: replica has witnessed `t0` and reported its dependencies.
    PreAccepted,
    /// Accepted: a final execution timestamp and dependency set were agreed on
    /// the slow path (or by recovery), under some ballot.
    Accepted,
    /// Committed: execution timestamp and dependencies are final; the replica
    /// may not yet have executed.
    Committed,
    /// Reading: dependencies are satisfied and this replica has read its owned
    /// keys and reported the version condition, but is still holding the slot
    /// awaiting the coordinator's atomic commit/abort decision.
    Reading,
    /// Applied: the commit/abort decision has been durably enacted.
    Applied,
}

/// A replica's knowledge of one transaction — the unit the
/// [`Journal`](crate::api::Journal) persists and recovery replays, and that a
/// joining node imports during bootstrap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandState {
    /// Transaction identity (`t0`); its node component is the coordinator.
    pub txn: TxnId,
    /// The topology epoch the transaction was coordinated under: its keys are
    /// owned, its quorums counted and its recovery planned by that epoch's
    /// layout, whatever this replica installed since.
    pub epoch: u64,
    /// Phase reached on this replica.
    pub status: Status,
    /// Highest ballot promised (advanced by Accept and Recover); rejects any
    /// message carrying a lower ballot.
    pub promised: Ballot,
    /// Ballot at which `execute_at`/`deps` were last Accepted. Recovery keeps the
    /// values from the highest accepted ballot.
    pub accepted: Ballot,
    /// Agreed execution timestamp `t` (`== t0` on the fast path, `> t0` when a
    /// conflict raised it on the slow path).
    pub execute_at: Timestamp,
    /// Transactions this one must execute after (conflicts with smaller `t0`).
    pub deps: Vec<TxnId>,
    /// Keys this transaction touches (for conflict detection).
    pub keys: Vec<Key>,
    /// The events this transaction conditionally appends on apply.
    #[serde(with = "wire_events")]
    pub events: Vec<Event>,
    /// Node to report the execution result to — the original coordinator, or a
    /// recovery coordinator that took over.
    pub reply_to: NodeId,
    /// The coordinator's commit/abort decision once known (from Apply). Recorded
    /// even if this replica is not yet ready to apply, so a lagging replica
    /// applies it as soon as its dependencies clear.
    pub decision: Option<bool>,
    /// The apply outcome once executed (`Some(conflict)`), so a recoverer that
    /// commits an already-applied transaction still learns the result.
    pub applied_conflict: Option<bool>,
}

/// The execution timestamp an **invalidated** command carries — a transaction
/// a recovery proved never committed, recorded applied as an abort without a
/// place in the order ([`Replica::invalidate`](crate::replica::Replica::invalidate)).
/// No real transaction executes at it (every `execute_at` is at least its
/// `t0`, a clock reading), so the state needs no field of its own: the journal
/// and the wire carry it as is.
pub const INVALIDATED_AT: Timestamp = Timestamp::MIN;

impl CommandState {
    /// Whether this command was invalidated (see [`INVALIDATED_AT`]).
    pub fn invalidated(&self) -> bool {
        self.status == Status::Applied && self.execute_at == INVALIDATED_AT
    }
}

/// An anti-entropy requester's view of its own applied state: its redundancy
/// floor plus the ids of every applied command it holds at/above that floor.
/// The responder ships only applied commands with `t0 >= since` missing from
/// `txns`, so a healthy in-sync round transfers ids instead of full command
/// payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncKnown {
    /// The requester's redundancy floor (`Replica::redundant_before`).
    pub since: Timestamp,
    /// Applied txns the requester already holds at/above `since`.
    pub txns: Vec<TxnId>,
}

/// A protocol message between nodes. Every message names its transaction by
/// [`TxnId`]; the id's node component identifies the *original* coordinator, so
/// replicas route execution results back without any separate addressing.
// Field semantics are described in each variant's doc; per-field one-liners
// would only repeat them.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Coordinator → replicas: propose `txn` (= `t0`) for the given keys/events.
    /// `epoch` is the layout the coordinator planned the transaction under
    /// (every message about it carries the same one): replicas judge
    /// ownership, and recoveries plan, by that layout.
    PreAccept {
        txn: TxnId,
        epoch: u64,
        keys: Vec<Key>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
    },
    /// Replica → coordinator: the timestamp this replica would execute at and
    /// the dependencies it witnessed. The fast path succeeds when a fast quorum
    /// all report `execute_at == t0`.
    PreAcceptOk {
        txn: TxnId,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
    },
    /// Coordinator → replicas (slow path / recovery): adopt the execution
    /// timestamp and dependency set under `ballot`. Carries the keys so a
    /// replica that missed PreAccept still indexes the transaction in its
    /// conflict graph (otherwise later conflicting transactions would miss it
    /// as a dependency, and recovery's superseding check would be wrong).
    Accept {
        txn: TxnId,
        ballot: Ballot,
        epoch: u64,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        keys: Vec<Key>,
    },
    /// Replica → coordinator: accepted under `ballot`, echoing any additional
    /// dependencies. The ballot lets the coordinator drop stale responses from
    /// an earlier attempt.
    AcceptOk {
        txn: TxnId,
        ballot: Ballot,
        deps: Vec<TxnId>,
    },
    /// Coordinator → a shard's replicas: the execution timestamp and
    /// dependencies are final; execute when dependencies allow. Carries the
    /// shard's keys and events explicitly (a replica that missed PreAccept must
    /// still index the transaction in its conflict graph, and events/keys are
    /// per-shard subsets now, so they cannot be re-derived from each other),
    /// and `reply_to` so the execution result reaches whoever is driving the
    /// commit (original coordinator or recoverer).
    Commit {
        txn: TxnId,
        epoch: u64,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        keys: Vec<Key>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
        reply_to: NodeId,
    },
    /// Replica → coordinator: this replica read its owned keys at the execution
    /// timestamp; `ok` is whether their version condition holds. The coordinator
    /// commits only if every touched shard reports `ok`.
    ReadOk { txn: TxnId, ok: bool },
    /// Coordinator → replicas: the atomic decision. `commit` true appends the
    /// events; false aborts the conditional write as a no-op.
    Apply { txn: TxnId, commit: bool },
    /// Replica → coordinator: the decision has been durably enacted here.
    Applied { txn: TxnId },
    /// Recovery coordinator → replicas: take over `txn` under a higher `ballot`.
    Recover { txn: TxnId, ballot: Ballot },
    /// Replica → recovery coordinator: everything known about `txn`, including
    /// the recovery-specific `superseding_rejects` signal (a later conflicting
    /// transaction did not witness `txn`, so it cannot have taken the fast path).
    RecoverOk {
        txn: TxnId,
        /// The ballot this report answers, so the recovery coordinator can drop
        /// stale responses from an earlier attempt.
        ballot: Ballot,
        /// The epoch the replica holds the transaction under (see
        /// [`BeginRecoveryOk`](Message::BeginRecoveryOk)).
        epoch: u64,
        /// False when this replica never witnessed the transaction.
        known: bool,
        status: Status,
        accepted: Ballot,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        superseding_rejects: bool,
        /// The replica's owned keys for this transaction; the recovery
        /// coordinator unions them across shards to plan the touched shards.
        keys: Vec<Key>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
    },
    /// Replica → coordinator: a ballot was too low; `promised` is the ballot the
    /// replica has already promised, so the sender can retry above it.
    Nack { txn: TxnId, promised: Ballot },
    /// Anti-entropy, replica → a shard peer: send me the applied commands I
    /// am missing. `known` is the digest of what the requester holds; the
    /// responder ships only what it lacks, bounded like a snapshot page.
    /// `id` correlates the reply with the in-flight request, so a concurrent
    /// join and anti-entropy round cannot steal each other's data. A join asks
    /// with [`JoinHeaderRequest`](Message::JoinHeaderRequest) instead.
    SyncRequest { id: u64, known: SyncKnown },
    /// Periodic gossip between shard replicas: the sender has applied every
    /// transaction below `applied_through`. The cluster compacts (drops redundant
    /// consensus state and truncates the log) below the per-shard minimum of these.
    Watermark { applied_through: Timestamp },
    /// Existing replica → joining node (or anti-entropy requester): the
    /// committed state to import.
    SyncData {
        /// Correlates with the [`SyncRequest`](Message::SyncRequest) or
        /// [`JoinHeaderRequest`](Message::JoinHeaderRequest) `id`.
        id: u64,
        /// The contact's redundancy watermark; the joiner adopts it as its floor,
        /// so a dependency on a compacted-away transaction counts as satisfied.
        watermark: Timestamp,
        /// The responder's applied-through point (lagged by its compaction
        /// margin) when it answered: below this, everything the responder holds
        /// is applied — and, together with this reply's `commands`, now in the
        /// requester's store too. The requester records it as proven sync
        /// coverage, which clamps its own compaction-watermark reports.
        #[serde(default = "min_timestamp")]
        coverage: Timestamp,
        /// The un-truncated applied commands (`t0 >= watermark`) the requester
        /// is missing, for the conflict graph and dependency barriers. Empty
        /// in a join header: a joiner pulls them through
        /// [`CommandPageRequest`](Message::CommandPageRequest).
        commands: Vec<CommandState>,
    },
    /// Joining node → the contact: the page of its snapshot after `after`
    /// (`None` for the first). Idempotent, so a lost reply is simply asked
    /// for again; the contact bounds the page by
    /// [`NodeConfig::snapshot_page_events`](crate::NodeConfig::snapshot_page_events)
    /// and [`snapshot_page_bytes`](crate::NodeConfig::snapshot_page_bytes).
    SnapshotPageRequest { id: u64, after: Option<Value> },
    /// Contact → joining node: one page of its materialised events; `next` is
    /// `None` after the last page (which may be empty: an empty store is a
    /// legitimate snapshot).
    SnapshotPage {
        id: u64,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
        next: Option<Value>,
    },
    /// Config coordinator → acceptors: Paxos phase 1 for `epoch`'s layout.
    ConfigPrepare { epoch: u64, ballot: Ballot },
    /// Acceptor → coordinator: promised `ballot`, reporting any value it has
    /// already accepted (which the coordinator must adopt). `ballot` names the
    /// prepare this promise answers, so a coordinator can drop stale replies.
    ConfigPromise {
        epoch: u64,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_layout: Option<Vec<Vec<NodeId>>>,
    },
    /// Config coordinator → acceptors: Paxos phase 2 — accept this layout.
    ConfigAccept {
        epoch: u64,
        ballot: Ballot,
        layout: Vec<Vec<NodeId>>,
    },
    /// Acceptor → coordinator: accepted under `ballot` (named so a coordinator
    /// can drop stale replies from an earlier attempt or another proposer).
    ConfigAccepted { epoch: u64, ballot: Ballot },
    /// Config coordinator → all nodes: the layout is decided; install it.
    ConfigCommit {
        epoch: u64,
        layout: Vec<Vec<NodeId>>,
    },
    /// Acceptor → coordinator: a config ballot was too low for `epoch`.
    ConfigNack { epoch: u64, promised: Ballot },
    /// A behind node → any peer: send every decided metadata-log entry strictly
    /// after `after_epoch`, so this node can fill a gap in its topology history.
    MetadataFetch { after_epoch: u64 },
    /// A peer → the requester: contiguous decided `(epoch, layout)` entries in
    /// ascending order (all `> after_epoch`). Fire-and-forget; the requester ingests
    /// them, installing each now-contiguous epoch. May be empty.
    MetadataEntries {
        entries: Vec<(u64, Vec<Vec<NodeId>>)>,
    },
    /// A node → an owner of the queried key range: serve this read locally.
    /// `to_micros` forwards the exclusive stamp bound (see
    /// `evento_core::Executor::read`).
    ReadForward {
        id: u64,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    },
    /// Owner → requester: the read result (events paired with their cursors).
    ReadReply {
        id: u64,
        cursors: Vec<Value>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
        page_info: PageInfo,
    },
    /// Linearizable-read coordinator → a quorum of a key's replicas: report the
    /// execution timestamp and dependencies a read at `txn`'s timestamp would
    /// witness over `key`. Unlike `PreAccept`, the replica stores **nothing** —
    /// it is a pure query of the conflict graph (a read-index probe).
    /// `epoch` is the layout the read coordinator probed under: a replica
    /// for which `key`'s replica set changed since does not answer.
    ReadProbe { txn: TxnId, epoch: u64, key: Key },
    /// Replica → read coordinator: the probe's witnessed `(execute_at, deps)`.
    ReadProbeOk {
        txn: TxnId,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
    },
    /// Replica → a peer it stopped waiting for: the peer was silent past
    /// `NodeConfig::peer_absence_limit`, and this replica has since compacted
    /// transactions at or above the peer's last watermark report, which
    /// anti-entropy can no longer ship. The peer must rejoin the group from a
    /// snapshot before it takes part again (`Node::rejoin_required`); `above`
    /// is the highest compacted `t0` it may be missing.
    Rejoin { above: Timestamp },
    /// Recovery coordinator → replicas: take `txn` over under `ballot`, like
    /// [`Recover`](Message::Recover), carrying the keys and events the
    /// coordinator holds for it. A replica that never witnessed the transaction
    /// pre-accepts its owned part under the ballot, so its vote counts and the
    /// original coordinator can no longer win it there; one that owns none of
    /// it still remembers the promise. Answered by
    /// [`BeginRecoveryOk`](Message::BeginRecoveryOk). A multi-shard
    /// transaction's recovery is [`Routed`](Message::Routed) instead, and a
    /// replica that never witnessed it then pre-accepts nothing: it answers
    /// [`NotWitnessed`](Message::NotWitnessed).
    BeginRecovery {
        txn: TxnId,
        ballot: Ballot,
        epoch: u64,
        keys: Vec<Key>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
    },
    /// Replica → recovery coordinator: a [`RecoverOk`](Message::RecoverOk) plus
    /// `wait` — an earlier conflict was accepted to execute after `txn`'s `t0`
    /// without witnessing it, so until that conflict commits nobody can tell
    /// whether `txn` could have taken the fast path. `epoch` is the layout
    /// the replica holds the transaction under (meaningful when `known`), so
    /// a node that learns the decision from it records it under that layout.
    BeginRecoveryOk {
        txn: TxnId,
        ballot: Ballot,
        epoch: u64,
        known: bool,
        status: Status,
        accepted: Ballot,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        superseding_rejects: bool,
        wait: bool,
        keys: Vec<Key>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
    },
    /// Contact → joining node, right after the [`SyncData`](Message::SyncData)
    /// answering its [`JoinHeaderRequest`](Message::JoinHeaderRequest) (same `id`):
    /// the contact's own watermark report `vouched`, taken in the same cut as
    /// that header under topology `epoch`. The contact holds every committed
    /// transaction below it, and the joiner copies that state, so once the
    /// snapshot is installed the joiner may report it too — without it, its
    /// first report predates the coverage proof a shard of four or more
    /// replicas needs, and peers that gave it up keep telling it to rejoin. It
    /// follows the header on the same link, so it is in by the time the joiner
    /// installs the snapshot; an anti-entropy `SyncData` has no vouch to carry.
    JoinProof {
        id: u64,
        epoch: u64,
        vouched: Timestamp,
    },
    /// `inner` — a `PreAccept`, `Accept`, `Commit`, `BeginRecovery` or
    /// `BeginRecoveryOk` — for a transaction whose `route` (its **full** key
    /// set, every shard's part) spans more than one shard. Each replica only
    /// stores its own shard's keys, so without the route a recovery
    /// coordinator cannot tell which other shards the transaction touches —
    /// least of all one that never witnessed it. A replica keeps the route
    /// (journaled) with the command. Single-shard traffic is never wrapped, so
    /// it stays byte-identical; a node predating this variant sheds the frame.
    Routed {
        route: Vec<Key>,
        inner: Box<Message>,
    },
    /// Replica → recovery coordinator, answering a routed `BeginRecovery`
    /// for a transaction this replica holds nothing of. `redundant` false: it
    /// never witnessed it, and has durably promised `ballot` — so it never
    /// will at a lower one (a slow quorum of a shard answering this proves the
    /// transaction never committed: it needs a PreAccept quorum on every
    /// shard). `redundant` true: `txn` is below this replica's redundancy
    /// watermark, so it may also have applied it and compacted it away — no
    /// evidence either way.
    NotWitnessed {
        txn: TxnId,
        ballot: Ballot,
        redundant: bool,
    },
    /// Recovery coordinator → a transaction's replicas: it never committed
    /// and never will (a shard never witnessed it), so every replica holding
    /// it un-committed records it applied as an abort, without executing it
    /// — an abort changes nothing, so it needs no place in the order.
    Invalidate { txn: TxnId },
    /// Joining node → an existing replica: the bootstrap header, answered
    /// with a [`SyncData`](Message::SyncData) carrying **no** commands (then
    /// the [`JoinProof`](Message::JoinProof)): the applied commands follow
    /// page by page through [`CommandPageRequest`](Message::CommandPageRequest),
    /// and only then the materialised events through
    /// [`SnapshotPageRequest`](Message::SnapshotPageRequest), so a store and a
    /// backlog of any size cross in frames far under the transport's cap.
    JoinHeaderRequest { id: u64 },
    /// Joining node → the contact: the page of its applied commands after
    /// `t0` `after` (`None` for the first), bounded like a snapshot page.
    /// Idempotent, so a lost reply is simply asked for again.
    CommandPageRequest { id: u64, after: Option<TxnId> },
    /// Contact → joining node: one page of its applied commands in `t0`
    /// order; `next` (the last `t0` shipped) is `None` after the last page.
    /// `watermark` is the contact's redundancy floor when it cut the page:
    /// a command compacted since the header is no longer walked, but its
    /// events are in the snapshot pages that follow, and the joiner adopts
    /// the highest floor it was sent, dropping every command below it.
    CommandPage {
        id: u64,
        watermark: Timestamp,
        commands: Vec<CommandState>,
        next: Option<TxnId>,
    },
    /// Owner → requester: it could not serve the [`ReadForward`](Message::ReadForward)
    /// `id` — its store refused the query, or it is still joining and holds
    /// part of its store — so the requester fails the read rather than take
    /// an empty or partial page for "no such events". `reason` is the
    /// owner's error, cut short. A node predating this variant sheds it, and
    /// its read times out: an error too, never an empty page.
    ReadFailed { id: u64, reason: String },
    /// Joiner → contact, every `collect_timeout` while its join runs: keeps
    /// the contact's hold on its watermark report alive between page
    /// requests a slow install spaces out (a slow disk, a large page). It
    /// only refreshes a live hold, never re-creates a lapsed one. A contact
    /// predating this variant sheds it, and its hold lapses as before.
    JoinKeepalive,
    /// Replica → recovery coordinator, answering a `BeginRecovery` (routed
    /// or not) while it joins over a lost journal (#312), for a transaction
    /// below its amnesia fence: it holds nothing of it, may have voted for
    /// it before the loss — any vote, promise or accept — and never votes
    /// for it again, so it promises nothing. An unrouted recovery counts it
    /// as a replica that may have voted but will not: with the reports of
    /// replicas that never witnessed the transaction, it can prove one
    /// never committed and never will (`Node::never_commits`). A recovery
    /// predating this variant sheds the frame, as it would no answer.
    Amnesiac { txn: TxnId, ballot: Ballot },
}

impl Message {
    /// The transaction this message concerns, used to route responses to the
    /// in-flight coordination. `None` for cluster-management messages (sync).
    pub fn txn(&self) -> Option<TxnId> {
        match self {
            Message::PreAccept { txn, .. }
            | Message::PreAcceptOk { txn, .. }
            | Message::Accept { txn, .. }
            | Message::AcceptOk { txn, .. }
            | Message::Commit { txn, .. }
            | Message::ReadOk { txn, .. }
            | Message::Apply { txn, .. }
            | Message::Applied { txn, .. }
            | Message::Recover { txn, .. }
            | Message::RecoverOk { txn, .. }
            | Message::Nack { txn, .. }
            | Message::BeginRecovery { txn, .. }
            | Message::BeginRecoveryOk { txn, .. }
            | Message::NotWitnessed { txn, .. }
            | Message::Amnesiac { txn, .. }
            | Message::Invalidate { txn }
            | Message::ReadProbeOk { txn, .. } => Some(*txn),
            Message::Routed { inner, .. } => inner.txn(),
            // A request the replica handles inline (not routed to a coordinator).
            Message::ReadProbe { .. } => None,
            Message::SyncRequest { .. }
            | Message::SyncData { .. }
            | Message::SnapshotPageRequest { .. }
            | Message::SnapshotPage { .. }
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
            | Message::Rejoin { .. }
            | Message::JoinProof { .. }
            | Message::JoinHeaderRequest { .. }
            | Message::CommandPageRequest { .. }
            | Message::CommandPage { .. }
            | Message::JoinKeepalive => None,
        }
    }
}

// NOTE: keep in sync with evento-remote/src/wire.rs `mod wire_events`.
/// Serde bridge for `Vec<Event>`. evento's [`Event`] is not itself
/// (de)serializable, so it is mirrored field-for-field by [`WireEvent`], whose
/// only non-trivial field is the bitcode-encoded [`Metadata`]. This lets the
/// whole [`Message`] derive serde for the network transport without modifying
/// evento-core.
mod wire_events {
    use evento_core::{metadata::Metadata, Event};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use ulid::Ulid;

    #[derive(Serialize, Deserialize)]
    struct WireEvent {
        id: Ulid,
        aggregate_id: String,
        aggregate_type: String,
        version: u16,
        name: String,
        routing_key: Option<String>,
        data: Vec<u8>,
        /// bitcode-encoded [`Metadata`].
        metadata: Vec<u8>,
        timestamp: u64,
        timestamp_subsec: u32,
        tenant: String,
    }

    impl From<&Event> for WireEvent {
        fn from(e: &Event) -> Self {
            WireEvent {
                id: e.id,
                aggregate_id: e.aggregate_id.clone(),
                aggregate_type: e.aggregate_type.clone(),
                version: e.version,
                name: e.name.clone(),
                routing_key: e.routing_key.clone(),
                data: e.data.clone(),
                metadata: bitcode::encode(&e.metadata),
                timestamp: e.timestamp,
                timestamp_subsec: e.timestamp_subsec,
                tenant: e.tenant.clone(),
            }
        }
    }

    impl TryFrom<WireEvent> for Event {
        type Error = bitcode::Error;

        fn try_from(w: WireEvent) -> Result<Self, Self::Error> {
            Ok(Event {
                id: w.id,
                aggregate_id: w.aggregate_id,
                aggregate_type: w.aggregate_type,
                version: w.version,
                name: w.name,
                routing_key: w.routing_key,
                tenant: w.tenant,
                data: w.data,
                metadata: bitcode::decode::<Metadata>(&w.metadata)?,
                timestamp: w.timestamp,
                timestamp_subsec: w.timestamp_subsec,
            })
        }
    }

    pub fn serialize<S: Serializer>(events: &[Event], s: S) -> Result<S::Ok, S::Error> {
        let wire: Vec<WireEvent> = events.iter().map(WireEvent::from).collect();
        wire.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Event>, D::Error> {
        Vec::<WireEvent>::deserialize(d)?
            .into_iter()
            .map(|w| Event::try_from(w).map_err(serde::de::Error::custom))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{decode_tagged, encode_tagged, RecordKind};

    fn page_event() -> Event {
        Event {
            aggregate_type: "test/Account".into(),
            aggregate_id: "acc".into(),
            version: 1,
            name: "Opened".into(),
            data: vec![1, 2, 3],
            ..Default::default()
        }
    }

    /// A key lives in its tenant: the default tenant's keys are the bare
    /// routing key or aggregate id, as before tenants existed; the same id in
    /// two tenants is two keys, so their writes never conflict.
    #[test]
    fn keys_are_scoped_by_tenant() {
        let event = |tenant: &str, routing_key: Option<&str>| Event {
            aggregate_id: "acc".into(),
            routing_key: routing_key.map(str::to_owned),
            tenant: tenant.into(),
            ..Default::default()
        };
        assert_eq!(Key::of(&event("", None)), Key("acc".into()));
        assert_eq!(Key::of(&event("", Some("eu"))), Key("eu".into()));
        assert!(matches!(
            Key::str_of(&event("", None)),
            Cow::Borrowed("acc")
        ));

        let a = Key::of(&event("a", None));
        let b = Key::of(&event("b", None));
        assert_ne!(a, b);
        assert_ne!(a, Key("acc".into()));
        assert_eq!(a, Key::in_tenant("a", "acc"));
        assert_eq!(Key::of(&event("a", Some("eu"))), Key::in_tenant("a", "eu"));
    }

    /// The snapshot variants cross the wire with their cursor and events
    /// intact.
    #[test]
    fn snapshot_variants_round_trip_tagged() {
        let messages = [
            Message::JoinHeaderRequest { id: 7 },
            Message::SnapshotPageRequest {
                id: 8,
                after: Some(Value("cursor".into())),
            },
            Message::SnapshotPage {
                id: 8,
                events: vec![page_event()],
                next: None,
            },
        ];
        for message in messages {
            let bytes = encode_tagged(RecordKind::WireFrame, &message).unwrap();
            let back: Message = decode_tagged(RecordKind::WireFrame, &bytes).unwrap();
            match (&message, &back) {
                (Message::JoinHeaderRequest { id }, Message::JoinHeaderRequest { id: b }) => {
                    assert_eq!(id, b)
                }
                (
                    Message::SnapshotPageRequest { id, after },
                    Message::SnapshotPageRequest { id: b, after: c },
                ) => assert_eq!((id, after), (b, c)),
                (
                    Message::SnapshotPage { id, events, next },
                    Message::SnapshotPage {
                        id: b,
                        events: e,
                        next: n,
                    },
                ) => {
                    assert_eq!((id, next), (b, n));
                    assert_eq!(events.len(), e.len());
                    assert_eq!(events[0].data, e[0].data);
                    assert_eq!(events[0].aggregate_id, e[0].aggregate_id);
                }
                other => panic!("variant changed on the wire: {other:?}"),
            }
        }
    }

    /// A failed forwarded read crosses the wire with its reason and belongs to
    /// no transaction.
    #[test]
    fn read_failed_round_trips_tagged() {
        let message = Message::ReadFailed {
            id: 9,
            reason: "disk full".into(),
        };
        assert_eq!(message.txn(), None);
        let bytes = encode_tagged(RecordKind::WireFrame, &message).unwrap();
        match decode_tagged(RecordKind::WireFrame, &bytes).unwrap() {
            Message::ReadFailed { id, reason } => {
                assert_eq!((id, reason.as_str()), (9, "disk full"))
            }
            other => panic!("variant changed on the wire: {other:?}"),
        }
    }

    /// A join keepalive crosses the wire and belongs to no transaction.
    #[test]
    fn join_keepalive_round_trips_tagged() {
        let message = Message::JoinKeepalive;
        assert_eq!(message.txn(), None);
        let bytes = encode_tagged(RecordKind::WireFrame, &message).unwrap();
        match decode_tagged(RecordKind::WireFrame, &bytes).unwrap() {
            Message::JoinKeepalive => {}
            other => panic!("variant changed on the wire: {other:?}"),
        }
    }

    /// The paged join's header request and command pages cross the wire
    /// intact — a command's events included. (Appended after every earlier
    /// variant, like `JoinProof`.)
    #[test]
    fn command_pages_round_trip_tagged() {
        let txn = TxnId(Timestamp {
            micros: 42,
            logical: 1,
            node: NodeId(2),
        });
        let command = CommandState {
            epoch: 0,
            txn,
            status: Status::Applied,
            promised: Ballot(txn.0),
            accepted: Ballot(txn.0),
            execute_at: txn.0,
            deps: vec![txn],
            keys: vec![Key("acc".into())],
            events: vec![page_event()],
            reply_to: NodeId(2),
            decision: Some(true),
            applied_conflict: Some(false),
        };
        let messages = [
            Message::JoinHeaderRequest { id: 5 },
            Message::CommandPageRequest {
                id: 6,
                after: Some(txn),
            },
            Message::CommandPage {
                id: 7,
                watermark: txn.0,
                commands: vec![command],
                next: Some(txn),
            },
        ];
        for message in messages {
            let bytes = encode_tagged(RecordKind::WireFrame, &message).unwrap();
            let back: Message = decode_tagged(RecordKind::WireFrame, &bytes).unwrap();
            match (&message, &back) {
                (Message::JoinHeaderRequest { id }, Message::JoinHeaderRequest { id: b }) => {
                    assert_eq!(id, b)
                }
                (
                    Message::CommandPageRequest { id, after },
                    Message::CommandPageRequest { id: b, after: c },
                ) => assert_eq!((id, after), (b, c)),
                (
                    Message::CommandPage {
                        id,
                        watermark,
                        commands,
                        next,
                    },
                    Message::CommandPage {
                        id: b,
                        watermark: w,
                        commands: c,
                        next: n,
                    },
                ) => {
                    assert_eq!((id, watermark, next), (b, w, n));
                    assert_eq!(c.len(), 1);
                    assert_eq!(c[0].txn, commands[0].txn);
                    assert_eq!(c[0].deps, commands[0].deps);
                    assert_eq!(c[0].events[0].data, commands[0].events[0].data);
                }
                other => panic!("variant changed on the wire: {other:?}"),
            }
        }
    }

    /// The join proof crosses the wire intact. (It is appended after every
    /// earlier variant: bitcode encodes a variant by position, so every
    /// earlier frame keeps its bytes.)
    #[test]
    fn join_proof_round_trips_tagged() {
        let vouched = Timestamp {
            micros: 42,
            logical: 3,
            node: NodeId(2),
        };
        let proof = Message::JoinProof {
            id: 9,
            epoch: 4,
            vouched,
        };
        let bytes = encode_tagged(RecordKind::WireFrame, &proof).unwrap();
        match decode_tagged(RecordKind::WireFrame, &bytes).unwrap() {
            Message::JoinProof {
                id,
                epoch,
                vouched: back,
            } => assert_eq!((id, epoch, back), (9, 4, vouched)),
            other => panic!("variant changed on the wire: {other:?}"),
        }
    }

    /// The route wrapper and the recovery verdicts cross the wire intact, and
    /// a wrapped message names its inner transaction.
    #[test]
    fn route_variants_round_trip_tagged() {
        let txn = TxnId(Timestamp {
            micros: 42,
            logical: 1,
            node: NodeId(3),
        });
        let ballot = Ballot(Timestamp {
            micros: 50,
            logical: 0,
            node: NodeId(4),
        });
        let route = vec![Key("a".into()), Key("b".into())];
        let routed = Message::Routed {
            route: route.clone(),
            inner: Box::new(Message::PreAccept {
                epoch: 0,
                txn,
                keys: vec![Key("a".into())],
                events: vec![page_event()],
            }),
        };
        assert_eq!(routed.txn(), Some(txn));
        let bytes = encode_tagged(RecordKind::WireFrame, &routed).unwrap();
        match decode_tagged::<Message>(RecordKind::WireFrame, &bytes).unwrap() {
            Message::Routed { route: r, inner } => {
                assert_eq!(r, route);
                match *inner {
                    Message::PreAccept {
                        txn: t,
                        epoch,
                        keys,
                        events,
                    } => {
                        assert_eq!(t, txn);
                        assert_eq!(epoch, 0);
                        assert_eq!(keys, vec![Key("a".into())]);
                        assert_eq!(events[0].data, page_event().data);
                    }
                    other => panic!("inner changed on the wire: {other:?}"),
                }
            }
            other => panic!("variant changed on the wire: {other:?}"),
        }
        for message in [
            Message::NotWitnessed {
                txn,
                ballot,
                redundant: true,
            },
            Message::Invalidate { txn },
            Message::Amnesiac { txn, ballot },
        ] {
            assert_eq!(message.txn(), Some(txn));
            let bytes = encode_tagged(RecordKind::WireFrame, &message).unwrap();
            let back: Message = decode_tagged(RecordKind::WireFrame, &bytes).unwrap();
            assert_eq!(format!("{back:?}"), format!("{message:?}"));
        }
    }

    /// Appending enum variants is wire-compatible under bitcode's serde
    /// encoding: an existing variant encodes the same bytes (an older node
    /// still decodes every message it knows), and a new variant fails to
    /// decode on an older node — which sheds the frame — instead of being
    /// misread as another variant.
    #[test]
    fn an_appended_variant_is_shed_by_an_older_decoder() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        enum Before {
            First(u64),
            Second { key: Key },
        }
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        enum After {
            First(u64),
            Second { key: Key },
            Appended { route: Vec<Key>, inner: Box<After> },
        }
        let known = After::Second {
            key: Key("k".into()),
        };
        let bytes = bitcode::serialize(&known).unwrap();
        assert_eq!(
            bytes,
            bitcode::serialize(&Before::Second {
                key: Key("k".into())
            })
            .unwrap(),
            "an existing variant's encoding must not change"
        );
        let appended = After::Appended {
            route: vec![Key("k".into())],
            inner: Box::new(After::First(7)),
        };
        let bytes = bitcode::serialize(&appended).unwrap();
        assert!(
            bitcode::deserialize::<Before>(&bytes).is_err(),
            "an older node must reject the appended variant"
        );
        assert_eq!(bitcode::deserialize::<After>(&bytes).unwrap(), appended);
    }

    /// None of a join's requests or pages concerns a transaction.
    #[test]
    fn snapshot_variants_route_as_control() {
        let page_request = Message::SnapshotPageRequest { id: 2, after: None };
        let page = Message::SnapshotPage {
            id: 2,
            events: Vec::new(),
            next: None,
        };
        let proof = Message::JoinProof {
            id: 1,
            epoch: 0,
            vouched: Timestamp::MIN,
        };
        let header_request = Message::JoinHeaderRequest { id: 1 };
        let command_request = Message::CommandPageRequest { id: 1, after: None };
        let command_page = Message::CommandPage {
            id: 1,
            watermark: Timestamp::MIN,
            commands: Vec::new(),
            next: None,
        };
        for message in [
            page_request,
            page,
            proof,
            header_request,
            command_request,
            command_page,
        ] {
            assert_eq!(message.txn(), None);
        }
    }
}
