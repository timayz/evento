# Phase 5: AccordExecutor Integration

## Objective

Create the `AccordExecutor` wrapper that implements the `Executor` trait, providing a drop-in replacement that adds ACCORD consensus to any underlying executor.

## Files to Create

```
evento-accord/src/
├── executor.rs         # AccordExecutor implementation
├── config.rs           # Configuration types
├── migration.rs        # SQL migration for accord_txn table
└── lib.rs              # Public API
```

## Tasks

### 5.1 Configuration

**File**: `src/config.rs`

```rust
use crate::transport::NodeAddr;
use std::time::Duration;

/// ACCORD cluster configuration
#[derive(Clone, Debug)]
pub struct AccordConfig {
    /// This node's configuration
    pub local: NodeAddr,

    /// All nodes in the cluster (including self)
    pub nodes: Vec<NodeAddr>,

    /// Operation timeouts
    pub timeouts: TimeoutConfig,

    /// Execution settings
    pub execution: ExecutionConfig,
}

#[derive(Clone, Debug)]
pub struct TimeoutConfig {
    /// Timeout for PreAccept phase
    pub preaccept: Duration,

    /// Timeout for Accept phase
    pub accept: Duration,

    /// Timeout for waiting on dependencies
    pub dependency_wait: Duration,

    /// Connection timeout
    pub connect: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            preaccept: Duration::from_secs(5),
            accept: Duration::from_secs(5),
            dependency_wait: Duration::from_secs(30),
            connect: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionConfig {
    /// Number of concurrent execution workers
    pub workers: usize,

    /// How long to keep executed transactions before cleanup
    pub retention: Duration,

    /// Batch size for execution
    pub batch_size: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            retention: Duration::from_secs(3600), // 1 hour
            batch_size: 100,
        }
    }
}

impl AccordConfig {
    /// Create single-node configuration (no consensus, for dev/test)
    pub fn single_node() -> Self {
        Self {
            local: NodeAddr { id: 0, host: "127.0.0.1".into(), port: 0 },
            nodes: vec![],
            timeouts: TimeoutConfig::default(),
            execution: ExecutionConfig::default(),
        }
    }

    /// Create cluster configuration
    pub fn cluster(local: NodeAddr, nodes: Vec<NodeAddr>) -> Self {
        Self {
            local,
            nodes,
            timeouts: TimeoutConfig::default(),
            execution: ExecutionConfig::default(),
        }
    }

    /// Check if running in single-node mode
    pub fn is_single_node(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// Calculate quorum size
    pub fn quorum_size(&self) -> usize {
        self.nodes.len() / 2 + 1
    }
}
```

**Acceptance Criteria**:
- [ ] Configuration types defined
- [ ] Single-node mode supported
- [ ] Sensible defaults
- [ ] Builder pattern for customization

---

### 5.2 SQL Migration

**File**: `src/migration.rs`

Add consensus state table to existing migrations.

```rust
use sea_query::{ColumnDef, Iden, Index, Table};

#[derive(Iden)]
pub enum AccordTxn {
    Table,
    TxnId,          // VARCHAR(64) PRIMARY KEY
    Status,         // TINYINT (0=preaccept, 1=accept, 2=commit, 3=executed)
    Keys,           // BLOB (bitcode encoded Vec<String>)
    Deps,           // BLOB (bitcode encoded Vec<TxnId>)
    ExecuteAt,      // VARCHAR(64) (timestamp as string)
    EventsData,     // BLOB (bitcode encoded events)
    Ballot,         // VARCHAR(32) (round:node_id)
    CreatedAt,      // BIGINT (unix millis)
    UpdatedAt,      // BIGINT (unix millis)
}

pub fn create_table_statement() -> String {
    Table::create()
        .table(AccordTxn::Table)
        .if_not_exists()
        .col(ColumnDef::new(AccordTxn::TxnId).string_len(64).primary_key())
        .col(ColumnDef::new(AccordTxn::Status).tiny_integer().not_null())
        .col(ColumnDef::new(AccordTxn::Keys).blob(sea_query::BlobSize::Medium).not_null())
        .col(ColumnDef::new(AccordTxn::Deps).blob(sea_query::BlobSize::Medium).not_null())
        .col(ColumnDef::new(AccordTxn::ExecuteAt).string_len(64).not_null())
        .col(ColumnDef::new(AccordTxn::EventsData).blob(sea_query::BlobSize::Long).not_null())
        .col(ColumnDef::new(AccordTxn::Ballot).string_len(32).not_null())
        .col(ColumnDef::new(AccordTxn::CreatedAt).big_integer().not_null())
        .col(ColumnDef::new(AccordTxn::UpdatedAt).big_integer().not_null())
        .to_string(sea_query::SqliteQueryBuilder) // Adapt for DB type
}

pub fn create_indexes() -> Vec<String> {
    vec![
        Index::create()
            .name("idx_accord_txn_status")
            .table(AccordTxn::Table)
            .col(AccordTxn::Status)
            .to_string(sea_query::SqliteQueryBuilder),
        Index::create()
            .name("idx_accord_txn_execute_at")
            .table(AccordTxn::Table)
            .col(AccordTxn::ExecuteAt)
            .to_string(sea_query::SqliteQueryBuilder),
    ]
}
```

**Acceptance Criteria**:
- [ ] Table schema defined
- [ ] Indexes for common queries
- [ ] Compatible with existing migrator pattern

---

### 5.3 AccordExecutor

**File**: `src/executor.rs`

```rust
use crate::{
    config::AccordConfig,
    protocol::Protocol,
    state::ConsensusState,
    transport::{ChannelTransport, TcpTransport, Transport},
    timestamp::HybridClock,
    txn::{Transaction, TxnId},
    keys::Key,
    Result,
};
use async_trait::async_trait;
use evento_core::{Executor, Event, WriteError, ReadResult, Value};
use std::sync::Arc;
use ulid::Ulid;

/// ACCORD consensus wrapper for any Executor
pub struct AccordExecutor<E: Executor> {
    inner: E,
    config: AccordConfig,
    state: Arc<ConsensusState>,
    clock: HybridClock,
    mode: ExecutorMode,
}

enum ExecutorMode {
    /// Single node: bypass consensus, write directly
    SingleNode,
    /// Cluster: run full ACCORD protocol
    Cluster {
        protocol: Protocol<E, TcpTransport>,
    },
}

impl<E: Executor> AccordExecutor<E> {
    /// Create single-node executor (no consensus overhead)
    pub fn single_node(executor: E) -> Self {
        Self {
            inner: executor,
            config: AccordConfig::single_node(),
            state: Arc::new(ConsensusState::new()),
            clock: HybridClock::new(0),
            mode: ExecutorMode::SingleNode,
        }
    }

    /// Create clustered executor with full ACCORD consensus
    pub async fn cluster(executor: E, config: AccordConfig) -> Result<Self> {
        let clock = HybridClock::new(config.local.id);
        let state = Arc::new(ConsensusState::new());

        // Initialize transport
        let peers: Vec<_> = config.nodes.iter()
            .filter(|n| n.id != config.local.id)
            .cloned()
            .collect();

        let (transport, server) = TcpTransport::new(
            config.local.clone(),
            peers,
        ).await?;

        let protocol = Protocol::new(
            state.clone(),
            executor.clone(), // Needs Clone bound
            transport,
            config.quorum_size(),
        );

        // Start server in background
        let protocol_clone = protocol.clone();
        tokio::spawn(async move {
            server.run(|msg| protocol_clone.handle_message(msg)).await
        });

        // Start execution workers
        Self::start_execution_workers(
            state.clone(),
            executor.clone(),
            config.execution.workers,
        );

        Ok(Self {
            inner: executor,
            config,
            state,
            clock,
            mode: ExecutorMode::Cluster { protocol },
        })
    }

    fn start_execution_workers(
        state: Arc<ConsensusState>,
        executor: E,
        count: usize,
    ) {
        for _ in 0..count {
            let state = state.clone();
            let executor = executor.clone();

            tokio::spawn(async move {
                loop {
                    if let Some(txn) = state.next_executable().await {
                        let execute = ExecutePhase::new(&state, &executor);
                        if let Err(e) = execute.run(txn.id).await {
                            tracing::error!("Execution failed: {}", e);
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            });
        }
    }
}

#[async_trait]
impl<E: Executor + Clone> Executor for AccordExecutor<E> {
    async fn write(&self, events: Vec<Event>) -> std::result::Result<(), WriteError> {
        match &self.mode {
            ExecutorMode::SingleNode => {
                // Bypass consensus, write directly
                self.inner.write(events).await
            }
            ExecutorMode::Cluster { protocol } => {
                // Extract keys for conflict detection
                let keys: Vec<String> = Key::from_events(&events)
                    .into_iter()
                    .map(|k| k.0)
                    .collect();

                // Create transaction
                let txn = Transaction {
                    id: TxnId {
                        timestamp: self.clock.now(),
                    },
                    events_data: bitcode::encode(&events),
                    keys,
                    deps: vec![],
                    status: TxnStatus::PreAccepted,
                    execute_at: Timestamp::ZERO,
                    ballot: Ballot { round: 0, node_id: self.config.local.id },
                };

                // Run ACCORD protocol
                protocol.submit(txn).await.map_err(|e| {
                    WriteError::Internal(e.into())
                })
            }
        }
    }

    async fn read(
        &self,
        aggregator_type: String,
        aggregator_id: Option<String>,
        cursor: Option<Value>,
        routing_key: Option<String>,
        limit: usize,
    ) -> anyhow::Result<ReadResult<Event>> {
        // Reads go directly to inner executor
        // ACCORD only coordinates writes
        self.inner.read(aggregator_type, aggregator_id, cursor, routing_key, limit).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.inner.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.inner.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.inner.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.inner.acknowledge(key, cursor, lag).await
    }
}
```

**Acceptance Criteria**:
- [ ] Implements full `Executor` trait
- [ ] Single-node mode works with zero overhead
- [ ] Cluster mode runs ACCORD protocol
- [ ] Background execution workers
- [ ] Graceful shutdown

---

### 5.4 Public API

**File**: `src/lib.rs`

```rust
//! ACCORD consensus protocol for evento
//!
//! This crate provides distributed consensus for event writes using the ACCORD protocol.
//!
//! # Quick Start
//!
//! ## Single Node (Development)
//! ```rust
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
//! ```rust
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

mod config;
mod error;
mod executor;
mod keys;
mod migration;
mod protocol;
mod state;
mod timestamp;
mod transport;
mod txn;

pub use config::{AccordConfig, ExecutionConfig, TimeoutConfig};
pub use error::{AccordError, Result};
pub use executor::AccordExecutor;
pub use transport::NodeAddr;

// Re-export for convenience
pub use evento_core::{Executor, Event, WriteError};
```

**Acceptance Criteria**:
- [ ] Clean public API
- [ ] Good documentation
- [ ] Example code in docs
- [ ] Re-exports for convenience

---

### 5.5 Integration Tests

**File**: `tests/integration.rs`

```rust
use evento_accord::{AccordExecutor, AccordConfig, NodeAddr};
use evento_core::Executor;

#[tokio::test]
async fn test_single_node_write() {
    let executor = setup_test_executor().await;
    let accord = AccordExecutor::single_node(executor);

    let events = vec![create_test_event("agg-1")];
    accord.write(events).await.unwrap();

    // Verify event was written
    let result = accord.read("test".into(), Some("agg-1".into()), None, None, 10).await.unwrap();
    assert_eq!(result.events.len(), 1);
}

#[tokio::test]
async fn test_three_node_cluster_no_conflict() {
    let (nodes, executors) = setup_3_node_cluster().await;

    // Write to node 0
    let events = vec![create_test_event("agg-1")];
    executors[0].write(events).await.unwrap();

    // Verify all nodes have the event
    for executor in &executors {
        let result = executor.read("test".into(), Some("agg-1".into()), None, None, 10).await.unwrap();
        assert_eq!(result.events.len(), 1);
    }
}

#[tokio::test]
async fn test_concurrent_writes_same_aggregate() {
    let (nodes, executors) = setup_3_node_cluster().await;

    // Concurrent writes to same aggregate from different nodes
    let (r1, r2) = tokio::join!(
        executors[0].write(vec![create_test_event("agg-1")]),
        executors[1].write(vec![create_test_event("agg-1")]),
    );

    // Both should succeed (ACCORD handles ordering)
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    // All nodes should see same order
    let events_0 = executors[0].read("test".into(), Some("agg-1".into()), None, None, 10).await.unwrap();
    let events_1 = executors[1].read("test".into(), Some("agg-1".into()), None, None, 10).await.unwrap();
    let events_2 = executors[2].read("test".into(), Some("agg-1".into()), None, None, 10).await.unwrap();

    assert_eq!(events_0.events, events_1.events);
    assert_eq!(events_1.events, events_2.events);
}

#[tokio::test]
async fn test_node_failure_during_write() {
    let (nodes, executors) = setup_3_node_cluster().await;

    // Kill one node
    drop(executors[2]);

    // Write should still succeed (quorum = 2)
    let events = vec![create_test_event("agg-1")];
    executors[0].write(events).await.unwrap();
}

#[tokio::test]
async fn test_recovery_after_restart() {
    // Test that consensus state is recovered from DB on restart
}
```

**Acceptance Criteria**:
- [ ] Single-node tests pass
- [ ] Multi-node tests pass
- [ ] Conflict resolution tested
- [ ] Failure scenarios tested
- [ ] Recovery tested

---

## Definition of Done

- [ ] AccordExecutor implements Executor trait
- [ ] Single-node mode works without network
- [ ] Cluster mode coordinates consensus
- [ ] Migration adds accord_txn table
- [ ] Integration tests pass
- [ ] Documentation complete
- [ ] Examples in crate docs
