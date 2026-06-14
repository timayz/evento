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
    Event, ReadAggregator, RoutingKey,
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
        Key(event
            .routing_key
            .clone()
            .unwrap_or_else(|| event.aggregator_id.clone()))
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
    /// timestamp and dependency set under `ballot`.
    Accept {
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
    },
    /// Replica → coordinator: accepted, echoing any additional dependencies.
    AcceptOk { txn: TxnId, deps: Vec<TxnId> },
    /// Coordinator → replicas: the execution timestamp and dependencies are
    /// final; execute when dependencies allow. Carries events for replicas that
    /// missed PreAccept, and `reply_to` so the execution result reaches whoever
    /// is driving the commit (original coordinator or recoverer).
    Commit {
        txn: TxnId,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
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
    /// bootstrap.
    SyncRequest,
    /// Existing replica → joining node: the applied commands to import.
    SyncData { commands: Vec<CommandState> },
    /// Config coordinator → acceptors: Paxos phase 1 for `epoch`'s layout.
    ConfigPrepare { epoch: u64, ballot: Ballot },
    /// Acceptor → coordinator: promised `ballot`, reporting any value it has
    /// already accepted (which the coordinator must adopt).
    ConfigPromise {
        epoch: u64,
        accepted_ballot: Ballot,
        accepted_layout: Option<Vec<Vec<NodeId>>>,
    },
    /// Config coordinator → acceptors: Paxos phase 2 — accept this layout.
    ConfigAccept {
        epoch: u64,
        ballot: Ballot,
        layout: Vec<Vec<NodeId>>,
    },
    /// Acceptor → coordinator: accepted under the proposing ballot.
    ConfigAccepted { epoch: u64 },
    /// Config coordinator → all nodes: the layout is decided; install it.
    ConfigCommit {
        epoch: u64,
        layout: Vec<Vec<NodeId>>,
    },
    /// Acceptor → coordinator: a config ballot was too low for `epoch`.
    ConfigNack { epoch: u64, promised: Ballot },
    /// A node → an owner of the queried key range: serve this read locally.
    ReadForward {
        id: u64,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    },
    /// Owner → requester: the read result (events paired with their cursors).
    ReadReply {
        id: u64,
        cursors: Vec<Value>,
        #[serde(with = "wire_events")]
        events: Vec<Event>,
        page_info: PageInfo,
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
            | Message::Nack { txn, .. } => Some(*txn),
            Message::SyncRequest
            | Message::SyncData { .. }
            | Message::ConfigPrepare { .. }
            | Message::ConfigPromise { .. }
            | Message::ConfigAccept { .. }
            | Message::ConfigAccepted { .. }
            | Message::ConfigCommit { .. }
            | Message::ConfigNack { .. }
            | Message::ReadForward { .. }
            | Message::ReadReply { .. } => None,
        }
    }
}

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
        aggregator_id: String,
        aggregator_type: String,
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
                aggregator_id: e.aggregator_id.clone(),
                aggregator_type: e.aggregator_type.clone(),
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
                aggregator_id: w.aggregator_id,
                aggregator_type: w.aggregator_type,
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
