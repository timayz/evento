//! Wire messages and per-transaction command state.
//!
//! The protocol vocabulary: the PreAccept → Accept → Commit → Read → Apply
//! phases (ballot-fenced), plus the recovery exchange (`Recover` / `RecoverOk` /
//! `Nack`) a live node uses to take over a transaction whose coordinator is
//! presumed dead. `Commit` carries the events so a replica that missed PreAccept
//! (it was slow or down) can still apply, and the per-shard dependency set for
//! that shard's execution barrier.

use evento_core::{
    cursor::{Args, PageInfo, Value},
    Event, EventFilter, RoutingKey,
};
use serde::{Deserialize, Serialize};

use crate::clock::{Ballot, NodeId, Timestamp, TxnId};

/// Sharding / ownership unit. Derived from an event's routing key (falling back
/// to its aggregate id), this is what the [`Topology`](crate::api::Topology)
/// maps to a replica set. Two transactions *conflict* when they share a key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(pub String);

impl Key {
    /// The key a write touches: its routing key, or the aggregate id when no
    /// routing key is set.
    pub fn of(event: &Event) -> Self {
        Key(Self::str_of(event).to_string())
    }

    /// [`of`](Key::of) without the allocation — for hashing/grouping hot paths
    /// that only need to compare or bucket by the key string.
    pub fn str_of(event: &Event) -> &str {
        event.routing_key.as_deref().unwrap_or(&event.aggregate_id)
    }
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Coordinator → replicas: propose `txn` (= `t0`) for the given keys/events.
    PreAccept {
        txn: TxnId,
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
    /// Joining node → an existing replica: send me your committed state so I can
    /// bootstrap. `snapshot` requests the materialized data-store snapshot too
    /// (set by a bootstrapping join, whose state may be below the contact's
    /// truncation watermark); anti-entropy clears it (it only needs recent
    /// commands). `known` is the anti-entropy digest — when present, the
    /// responder ships only the applied commands the requester is missing;
    /// bootstrap sends `None` (ship everything). `id` correlates the reply
    /// with the in-flight request, so a concurrent join and anti-entropy round
    /// cannot steal each other's data.
    SyncRequest {
        id: u64,
        snapshot: bool,
        known: Option<SyncKnown>,
    },
    /// Periodic gossip between shard replicas: the sender has applied every
    /// transaction below `applied_through`. The cluster compacts (drops redundant
    /// consensus state and truncates the log) below the per-shard minimum of these.
    Watermark { applied_through: Timestamp },
    /// Existing replica → joining node: the committed state to import.
    SyncData {
        /// Correlates with the [`SyncRequest`](Message::SyncRequest) `id`.
        id: u64,
        /// The contact's redundancy watermark; the joiner adopts it as its floor,
        /// so a dependency on a compacted-away transaction counts as satisfied.
        watermark: Timestamp,
        /// Materialized applied events (the state below the watermark that command
        /// replay no longer covers). Empty unless a snapshot was requested.
        #[serde(with = "wire_events")]
        snapshot: Vec<Event>,
        /// The un-truncated applied commands (`t0 >= watermark`), for the conflict
        /// graph and dependency barriers.
        commands: Vec<CommandState>,
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
    ReadProbe { txn: TxnId, key: Key },
    /// Replica → read coordinator: the probe's witnessed `(execute_at, deps)`.
    ReadProbeOk {
        txn: TxnId,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
    },
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
            | Message::ReadProbeOk { txn, .. } => Some(*txn),
            // A request the replica handles inline (not routed to a coordinator).
            Message::ReadProbe { .. } => None,
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
            | Message::ReadReply { .. } => None,
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
