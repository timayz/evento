# Phase 2: Consensus State Machine

## Objective

Implement the core state machine that tracks transaction states and dependencies. This runs locally on each node and is the foundation for the protocol.

## Files to Create

```
evento-accord/src/
├── state/
│   ├── mod.rs
│   ├── store.rs        # Transaction storage
│   ├── deps.rs         # Dependency graph
│   └── waiting.rs      # Execution queue
```

## Tasks

### 2.1 Transaction Store

**File**: `src/state/store.rs`

In-memory store for active transactions with persistence via Executor.

```rust
use crate::{Transaction, TxnId, TxnStatus, Key, Timestamp};
use std::collections::{HashMap, BTreeMap, BTreeSet};

/// Stores all known transactions
pub struct TxnStore {
    /// All transactions by ID
    txns: HashMap<TxnId, Transaction>,

    /// Index: key -> transactions touching that key
    by_key: HashMap<Key, BTreeSet<TxnId>>,

    /// Index: transactions pending execution, ordered by execute_at
    pending_execution: BTreeMap<Timestamp, TxnId>,

    /// Index: committed but not yet executed
    committed: BTreeSet<TxnId>,
}

impl TxnStore {
    pub fn new() -> Self;

    /// Insert or update a transaction
    pub fn upsert(&mut self, txn: Transaction);

    /// Get transaction by ID
    pub fn get(&self, id: &TxnId) -> Option<&Transaction>;

    /// Get mutable transaction by ID
    pub fn get_mut(&mut self, id: &TxnId) -> Option<&mut Transaction>;

    /// Find all transactions that conflict with given keys
    pub fn find_conflicts(&self, keys: &[Key]) -> Vec<&Transaction>;

    /// Find transactions with execute_at < given timestamp that are committed
    pub fn find_ready_to_execute(&self, up_to: Timestamp) -> Vec<TxnId>;

    /// Remove fully executed transaction (after retention period)
    pub fn remove(&mut self, id: &TxnId);

    /// Update transaction status
    pub fn set_status(&mut self, id: &TxnId, status: TxnStatus) -> Result<()>;
}
```

**Acceptance Criteria**:
- [ ] CRUD operations work correctly
- [ ] Indexes stay consistent after mutations
- [ ] Conflict detection uses index efficiently
- [ ] Unit tests for all operations

---

### 2.2 Dependency Graph

**File**: `src/state/deps.rs`

Track and resolve dependencies between transactions.

```rust
use crate::{TxnId, Timestamp};
use std::collections::{HashMap, HashSet};

/// Dependency graph for transaction ordering
pub struct DependencyGraph {
    /// txn_id -> set of dependencies (transactions that must execute first)
    deps: HashMap<TxnId, HashSet<TxnId>>,

    /// txn_id -> set of dependents (transactions waiting on this one)
    reverse_deps: HashMap<TxnId, HashSet<TxnId>>,

    /// Transactions with all dependencies satisfied
    ready: HashSet<TxnId>,
}

impl DependencyGraph {
    pub fn new() -> Self;

    /// Register a transaction with its dependencies
    pub fn register(&mut self, txn_id: TxnId, deps: HashSet<TxnId>);

    /// Mark a transaction as executed, updating dependents
    pub fn mark_executed(&mut self, txn_id: TxnId) -> Vec<TxnId>;

    /// Check if transaction is ready to execute (all deps satisfied)
    pub fn is_ready(&self, txn_id: &TxnId) -> bool;

    /// Get all ready transactions
    pub fn get_ready(&self) -> impl Iterator<Item = &TxnId>;

    /// Get missing dependencies for a transaction
    pub fn get_pending_deps(&self, txn_id: &TxnId) -> Vec<TxnId>;
}
```

**Acceptance Criteria**:
- [ ] Dependencies correctly tracked
- [ ] Ready set updated when deps satisfied
- [ ] No memory leaks (executed txns cleaned up)
- [ ] Unit tests for dependency chains

---

### 2.3 Execution Queue

**File**: `src/state/waiting.rs`

Orders committed transactions for execution.

```rust
use crate::{TxnId, Timestamp};
use std::collections::BTreeMap;

/// Priority queue for transaction execution
/// Ordered by execute_at timestamp
pub struct ExecutionQueue {
    /// Transactions ready to execute, ordered by execute_at
    queue: BTreeMap<Timestamp, Vec<TxnId>>,

    /// Transactions waiting for dependencies
    waiting: HashMap<TxnId, HashSet<TxnId>>,
}

impl ExecutionQueue {
    pub fn new() -> Self;

    /// Add transaction to queue (will wait for deps if needed)
    pub fn enqueue(&mut self, txn_id: TxnId, execute_at: Timestamp, deps: HashSet<TxnId>);

    /// Get next transaction to execute (if deps satisfied)
    pub fn pop_ready(&mut self) -> Option<TxnId>;

    /// Notify that a transaction was executed
    pub fn notify_executed(&mut self, txn_id: TxnId);

    /// Peek at next execute_at timestamp
    pub fn peek_next_time(&self) -> Option<Timestamp>;
}
```

**Acceptance Criteria**:
- [ ] Transactions execute in timestamp order
- [ ] Dependencies block execution
- [ ] Notify correctly unblocks dependents
- [ ] Unit tests for ordering

---

### 2.4 Combined State Machine

**File**: `src/state/mod.rs`

Unified interface for consensus state.

```rust
mod store;
mod deps;
mod waiting;

pub use store::TxnStore;
pub use deps::DependencyGraph;
pub use waiting::ExecutionQueue;

use crate::{Transaction, TxnId, TxnStatus, Key, Timestamp, Result};
use tokio::sync::RwLock;

/// Thread-safe consensus state
pub struct ConsensusState {
    store: RwLock<TxnStore>,
    deps: RwLock<DependencyGraph>,
    queue: RwLock<ExecutionQueue>,
}

impl ConsensusState {
    pub fn new() -> Self;

    // === PreAccept Phase ===

    /// Register new transaction, compute initial dependencies
    pub async fn preaccept(&self, txn: Transaction) -> Result<Vec<TxnId>> {
        let mut store = self.store.write().await;

        // Find conflicting transactions with earlier timestamps
        let conflicts = store.find_conflicts(&txn.keys);
        let deps: Vec<TxnId> = conflicts
            .iter()
            .filter(|c| c.id < txn.id)
            .map(|c| c.id)
            .collect();

        let mut txn = txn;
        txn.deps = deps.clone();
        txn.status = TxnStatus::PreAccepted;
        store.upsert(txn);

        Ok(deps)
    }

    // === Accept Phase ===

    /// Update transaction with final deps and execute_at
    pub async fn accept(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
    ) -> Result<()>;

    // === Commit Phase ===

    /// Mark transaction as committed
    pub async fn commit(&self, txn_id: TxnId) -> Result<()>;

    // === Execute Phase ===

    /// Get next transaction ready for execution
    pub async fn next_executable(&self) -> Option<Transaction>;

    /// Mark transaction as executed
    pub async fn mark_executed(&self, txn_id: TxnId) -> Result<()>;

    // === Queries ===

    pub async fn get(&self, txn_id: &TxnId) -> Option<Transaction>;
    pub async fn get_status(&self, txn_id: &TxnId) -> Option<TxnStatus>;
}
```

**Acceptance Criteria**:
- [ ] Thread-safe access with RwLock
- [ ] State transitions follow ACCORD protocol
- [ ] All phases integrated correctly
- [ ] Integration tests for full lifecycle

---

### 2.5 Persistence Layer

**File**: `src/state/persist.rs`

Persist consensus state to Executor for recovery.

```rust
use evento_core::Executor;
use crate::{Transaction, TxnId};

/// Schema for consensus state (stored in same DB as events)
/// Table: accord_txn
pub struct Persistence<E: Executor> {
    executor: E,
}

impl<E: Executor> Persistence<E> {
    /// Save transaction state
    pub async fn save(&self, txn: &Transaction) -> Result<()>;

    /// Load transaction by ID
    pub async fn load(&self, id: &TxnId) -> Result<Option<Transaction>>;

    /// Load all non-executed transactions (for recovery)
    pub async fn load_pending(&self) -> Result<Vec<Transaction>>;

    /// Delete executed transaction (cleanup)
    pub async fn delete(&self, id: &TxnId) -> Result<()>;
}
```

**Note**: Actual SQL implementation deferred to Phase 4 integration. This phase focuses on in-memory state machine.

---

## Definition of Done

- [ ] All state machine components implemented
- [ ] Thread-safety with proper locking
- [ ] Unit tests for each component
- [ ] Integration test: full txn lifecycle (preaccept → commit → execute)
- [ ] No deadlocks under concurrent access
