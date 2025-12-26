//! ACCORD consensus protocol for evento.
//!
//! This crate provides distributed consensus for event writes using the ACCORD protocol.
//! ACCORD is a leaderless consensus protocol that provides:
//!
//! - **Strict Serializability**: All nodes observe the same order of conflicting transactions
//! - **Low Latency**: Single round-trip for non-conflicting transactions (fast path)
//! - **Fault Tolerance**: Survive f failures with 2f+1 nodes
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │              Application                 │
//! │                   │                      │
//! │                   ▼                      │
//! │          ┌───────────────┐              │
//! │          │AccordExecutor │              │
//! │          └───────┬───────┘              │
//! │                  │                       │
//! │    ┌─────────────┼─────────────┐        │
//! │    ▼             ▼             ▼        │
//! │ ┌──────┐   ┌──────────┐  ┌─────────┐   │
//! │ │State │   │ Protocol │  │Transport│   │
//! │ └──────┘   └──────────┘  └─────────┘   │
//! │    │                          │         │
//! │    ▼                          ▼         │
//! │ ┌──────┐                 ┌─────────┐   │
//! │ │Inner │                 │  Other  │   │
//! │ │Exec. │                 │  Nodes  │   │
//! │ └──────┘                 └─────────┘   │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Protocol Phases
//!
//! ACCORD operates in four phases:
//!
//! 1. **PreAccept**: Propose transaction with initial timestamp, collect dependencies
//! 2. **Accept**: (Slow path only) Coordinate final dependencies if replicas disagreed
//! 3. **Commit**: Persist the decision
//! 4. **Execute**: Apply transaction after dependencies are satisfied
//!
//! # Quick Start
//!
//! ## Single Node (Development)
//!
//! ```ignore
//! use evento_accord::AccordExecutor;
//! use evento_sql::SqlExecutor;
//!
//! let sql_executor = SqlExecutor::new(pool).await?;
//! let executor = AccordExecutor::single_node(sql_executor);
//!
//! // Use like any other executor
//! evento::create()
//!     .event(&MyEvent { ... })
//!     .commit(&executor)
//!     .await?;
//! ```
//!
//! ## Cluster Mode (Production)
//!
//! ```ignore
//! use evento_accord::{AccordExecutor, AccordConfig, NodeAddr};
//!
//! let config = AccordConfig::cluster(
//!     NodeAddr { id: 0, host: "node1".into(), port: 5000 },
//!     vec![
//!         NodeAddr { id: 0, host: "node1".into(), port: 5000 },
//!         NodeAddr { id: 1, host: "node2".into(), port: 5000 },
//!         NodeAddr { id: 2, host: "node3".into(), port: 5000 },
//!     ],
//! );
//!
//! let executor = AccordExecutor::cluster(sql_executor, config).await?;
//! ```

pub mod backpressure;
pub mod config;
pub mod error;
pub mod executor;
pub mod keys;
pub mod metrics;
pub mod protocol;
pub mod sim;
pub mod state;
pub mod timestamp;
pub mod tracing_ext;
pub mod transport;
pub mod txn;

// Re-export main types at crate root
pub use backpressure::{BackpressureConfig, ConcurrencyLimiter, LimiterStats, Permit};
pub use config::{AccordConfig, ConnectionConfig, ExecutionConfig, TimeoutConfig};
pub use error::{AccordError, Result};
pub use executor::{AccordExecutor, ShutdownHandle};
pub use keys::Key;
pub use protocol::{BackgroundExecutor, ExecuteConfig, Message, PreAcceptResult, ProtocolResult};
pub use state::{ConsensusState, ConsensusStateStats, DurableStore, MemoryDurableStore};

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use state::{load_executed_txn_ids, SqlDurableStore};
pub use timestamp::{HybridClock, Timestamp};
pub use transport::{ChannelTransport, NodeAddr, NodeId, TcpServer, TcpTransport, Transport};
pub use txn::{Ballot, SerializableEvent, Transaction, TxnId, TxnStatus};

// Re-export evento-core types for convenience
pub use evento_core::{Event, Executor, WriteError};

// Re-export tokio_util types for background task management
pub use tokio_util::sync::CancellationToken;
