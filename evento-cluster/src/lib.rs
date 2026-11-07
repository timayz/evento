//! The clustered evento [`Executor`]: wrap a local store and get a **leaderless,
//! strictly-serializable, highly-available** replicated event store. Writes are
//! replicated across `N = 2f + 1` nodes with a single global serial order and no
//! elected leader, tolerating `f` failures; reads, subscriptions and snapshots
//! are served by the local backend the executor wraps. Replication runs on the
//! Accord consensus protocol (Apache Cassandra [CEP-15]).
//!
//! See `DESIGN.md` for the architecture and `OPERATIONS.md` for the deployment
//! runbook.
//!
//! [CEP-15]: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15:+General+Purpose+Transactions
//! [`Executor`]: evento_core::Executor
//!
//! # What a deployer touches
//!
//! The crate root holds everything one process needs to run one node of a
//! cluster, and nothing else:
//!
//! - [`ClusterExecutor`] — the drop-in [`Executor`] — over an
//!   [`ExecutorDataStore`] wrapping the local backend;
//! - a [`Node`] and its [`NodeConfig`], with the errors a caller can meet
//!   ([`Joining`], [`TopologyChanging`], [`RejoinRequired`], …);
//! - the membership: [`Topology`] and its [`StaticTopology`], [`ShardedTopology`]
//!   and [`DynamicTopology`] implementations, with [`Layout`] / [`ShardMap`];
//! - the transport: [`TcpTransport`] and [`serve`] (or [`serve_tls`] /
//!   [`serve_tls_verified`] with a [`TlsClient`] and [`PeerCerts`]), plus
//!   [`InMemoryNetwork`] for a single-node or in-process cluster;
//! - the [`Clock`] ([`HybridLogicalClock`]) and this node's [`NodeId`];
//! - [`Metrics`] / [`MetricsSnapshot`];
//! - the [`Journal`] and [`DataStore`] traits a backend implements, with the
//!   [`InMemoryJournal`] / [`InMemoryDataStore`] stand-ins. The journals the
//!   local backends ship live next to their executor (`FjallJournal`,
//!   `SqlJournal`, `TursoJournal`), and each backend implements
//!   [`ClusterBackend`] under its `cluster` feature: the journal colocated
//!   with the store, and the store emptied for a join.
//!
//! # The protocol
//!
//! The protocol's own vocabulary — transaction ids and ballots, command state,
//! wire messages, the records a journal persists — lives under [`protocol`].
//! A journal or data-store implementor imports from there; a deployer never
//! needs it. The fault-injection knobs of the in-memory transport
//! ([`MessageFaults`](transport::MessageFaults), [`TraceEntry`](transport::TraceEntry),
//! …) stay under [`transport`], and the record framing under [`format`](mod@format).
//!
//! # Layering
//!
//! ```text
//! ClusterExecutor (evento_core::Executor)
//!         │
//!    Node (coordinator + replica) ── Replica (consensus state machine)
//!         │
//!    api traits: MessageSink · Topology · Clock · Journal · DataStore
//! ```
//!
//! The protocol depends only on the [`api`] traits, each with a deterministic
//! in-process implementation for testing and a production implementation.

pub mod api;
pub mod backend;
pub mod clock;
pub mod executor;
pub mod failure_detector;
pub mod format;
pub mod message;
pub mod metrics;
pub mod node;
mod reconfig;
pub mod replica;
pub mod store;
pub mod tcp;
pub mod transport;

pub use api::{
    slot_of, validate_layout, DataStore, DynamicTopology, Journal, Layout, MessageSink, RegionId,
    ShardId, ShardMap, ShardedTopology, StaticTopology, Topology, SLOTS,
};
pub use backend::ClusterBackend;
pub use clock::{Clock, HybridLogicalClock, NodeId, MAX_SKEW_MICROS};
pub use executor::{ClusterExecutor, ExecutorDataStore};
pub use metrics::{Metrics, MetricsSnapshot};
pub use node::{
    BacklogAtCap, CommandTooLarge, CommitOutcome, FenceNotCovered, Invalidated, Joining,
    JournalRolledBack, LostJournal, Node, NodeClosed, NodeConfig, OpenUnconfirmed, PartialSnapshot,
    RejoinRequired, SweepPressure, TopologyChanging, MAX_COMMAND_BYTES,
};
pub use store::{InMemoryDataStore, InMemoryJournal};
pub use tcp::{serve, serve_tls, serve_tls_verified, PeerCerts, TcpTransport, TlsClient};
pub use transport::InMemoryNetwork;

/// The protocol-level types: what a [`Journal`] or [`DataStore`] implementor,
/// a test harness or a protocol-aware tool imports. A deployer wrapping a
/// local store in a [`ClusterExecutor`] touches none of these.
///
/// - Identity and ordering: [`Timestamp`](protocol::Timestamp),
///   [`TxnId`](protocol::TxnId), [`Ballot`](protocol::Ballot),
///   [`Key`](protocol::Key).
/// - A transaction's state: [`CommandState`](protocol::CommandState),
///   [`Status`](protocol::Status), [`RecoveryRound`](protocol::RecoveryRound).
/// - The wire: [`Message`](protocol::Message).
/// - What a journal persists: [`AcceptorRecord`](protocol::AcceptorRecord),
///   [`DecisionRecord`](protocol::DecisionRecord),
///   [`FenceRecord`](protocol::FenceRecord),
///   [`AmnesiaFence`](protocol::AmnesiaFence),
///   [`ConfigEntry`](protocol::ConfigEntry) / [`Drained`](protocol::Drained),
///   [`DurableMark`](protocol::DurableMark),
///   [`CopyState`](protocol::CopyState) / [`CopyPhase`](protocol::CopyPhase).
/// - What a data store hands over: [`SnapshotPage`](protocol::SnapshotPage),
///   [`snapshot_event_size`](protocol::snapshot_event_size),
///   [`StoreBehind`](protocol::StoreBehind),
///   [`AppliedEntry`](protocol::AppliedEntry).
///
/// The modules the types are defined in ([`clock`], [`message`], [`api`],
/// [`store`], [`failure_detector`]) stay public; this module is the curated
/// view of them.
pub mod protocol {
    pub use crate::api::{
        snapshot_event_size, AcceptorRecord, AmnesiaFence, ConfigEntry, CopyPhase, CopyState,
        DecisionRecord, Drained, DurableMark, FenceRecord, SnapshotPage, StoreBehind,
    };
    pub use crate::clock::{Ballot, Timestamp, TxnId};
    pub use crate::failure_detector::FailureDetector;
    pub use crate::message::{CommandState, Key, Message, RecoveryRound, Status};
    pub use crate::store::AppliedEntry;
}
