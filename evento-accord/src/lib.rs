//! Accord consensus protocol (Apache Cassandra [CEP-15]) for evento.
//!
//! This crate is building toward a new evento [`Executor`] that turns evento
//! into a **leaderless, strictly-serializable, highly-available replicated
//! event store**: events are replicated across `N = 2f + 1` nodes with a single
//! global serial order and no elected leader, tolerating `f` failures.
//!
//! See `DESIGN.md` for the full architecture and milestone plan. The crate is
//! currently at **M0 (skeleton)**: the [`api`] traits, core types, an in-memory
//! transport, and a tested Hybrid Logical Clock are in place; the protocol
//! phases (M1+) are not yet implemented.
//!
//! [CEP-15]: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15:+General+Purpose+Transactions
//! [`Executor`]: evento_core::Executor
//!
//! # Layering
//!
//! ```text
//! Executor impl (M5) → Coordinator → protocol core → api traits
//!                                                     (MessageSink, Topology,
//!                                                      Clock, Journal, DataStore)
//! ```
//!
//! The protocol depends only on the [`api`] traits, each with a deterministic
//! in-process implementation for testing and a production implementation.

pub mod api;
pub mod clock;
pub mod executor;
pub mod failure_detector;
pub mod fjall_journal;
pub mod format;
pub mod message;
pub mod metrics;
pub mod node;
pub mod replica;
pub mod store;
pub mod tcp;
pub mod transport;

pub use api::{
    AcceptorRecord, DataStore, DynamicTopology, Journal, MessageSink, RegionId, ShardId,
    ShardedTopology, StaticTopology, Topology,
};
pub use clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId, MAX_SKEW_MICROS};
pub use executor::{AccordExecutor, ExecutorDataStore};
pub use failure_detector::FailureDetector;
pub use fjall_journal::FjallJournal;
pub use message::{CommandState, Key, Message, Status};
pub use metrics::{Metrics, MetricsSnapshot};
pub use node::{CommitOutcome, Node, NodeConfig};
pub use store::{AppliedEntry, InMemoryDataStore, InMemoryJournal};
pub use tcp::{serve, serve_tls, serve_tls_verified, PeerCerts, TcpTransport, TlsClient};
pub use transport::{Envelope, InMemoryNetwork, InMemorySink};
