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
pub mod fjall_journal;
pub mod message;
pub mod node;
pub mod replica;
pub mod store;
pub mod tcp;
pub mod transport;

pub use api::{
    DataStore, DynamicTopology, Journal, MessageSink, ShardId, ShardedTopology, StaticTopology,
    Topology,
};
pub use clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
pub use executor::{AccordExecutor, ExecutorDataStore};
pub use fjall_journal::FjallJournal;
pub use message::{CommandState, Key, Message, Status};
pub use node::{CommitOutcome, Node};
pub use store::{AppliedEntry, InMemoryDataStore, InMemoryJournal};
pub use tcp::{serve, TcpTransport};
pub use transport::{Envelope, InMemoryNetwork, InMemorySink};
