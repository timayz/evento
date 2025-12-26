# Phase 1: Core Types and Data Structures

## Objective

Define the foundational types for ACCORD consensus that will be used throughout the implementation.

## Files to Create

```
evento-accord/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── timestamp.rs    # Hybrid Logical Clock
│   ├── txn.rs          # Transaction types
│   ├── keys.rs         # Key extraction and conflict detection
│   └── error.rs        # Error types
```

## Tasks

### 1.1 Create Crate Structure

**File**: `Cargo.toml`

```toml
[package]
name = "evento-accord"
version = "0.1.0"
edition = "2021"
license = "MIT OR Apache-2.0"
description = "ACCORD consensus protocol for evento"

[dependencies]
evento-core = { path = "../evento-core" }
bitcode = { version = "0.6", features = ["serde"] }
tokio = { version = "1", features = ["full"] }
thiserror = "2"
tracing = "0.1"

[dev-dependencies]
tokio-test = "0.4"
```

**Acceptance Criteria**:
- [ ] Crate compiles with `cargo check -p evento-accord`
- [ ] Added to workspace in root `Cargo.toml`

---

### 1.2 Implement Hybrid Logical Clock

**File**: `src/timestamp.rs`

The timestamp is the foundation of ACCORD ordering.

```rust
/// Hybrid Logical Clock timestamp
/// Total ordering: (time, seq, node_id)
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct Timestamp {
    /// Physical time in milliseconds since epoch
    pub time: u64,
    /// Logical sequence for same millisecond
    pub seq: u32,
    /// Node ID for tie-breaking
    pub node_id: u16,
}

impl Timestamp {
    pub const ZERO: Self = Self { time: 0, seq: 0, node_id: 0 };

    pub fn new(time: u64, seq: u32, node_id: u16) -> Self;
    pub fn increment(&self) -> Self;
    pub fn max_for_time(time: u64, node_id: u16) -> Self;
}

/// Hybrid Logical Clock for generating timestamps
pub struct HybridClock {
    node_id: u16,
    last: AtomicU64,  // Packed (time << 32 | seq)
}

impl HybridClock {
    pub fn new(node_id: u16) -> Self;
    pub fn now(&self) -> Timestamp;
    pub fn update(&self, received: Timestamp) -> Timestamp;
}
```

**Acceptance Criteria**:
- [ ] Timestamps are totally ordered
- [ ] `now()` is monotonically increasing
- [ ] `update()` handles clock skew correctly
- [ ] Unit tests for ordering, increment, clock skew

---

### 1.3 Define Transaction Types

**File**: `src/txn.rs`

```rust
/// Unique transaction identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct TxnId {
    pub timestamp: Timestamp,
}

/// Transaction status in the consensus protocol
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(bitcode::Encode, bitcode::Decode)]
#[repr(u8)]
pub enum TxnStatus {
    PreAccepted = 0,
    Accepted = 1,
    Committed = 2,
    Executed = 3,
}

/// A transaction in the ACCORD protocol
#[derive(Clone, Debug)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct Transaction {
    /// Unique identifier (also initial timestamp)
    pub id: TxnId,

    /// Events to write (bitcode encoded)
    pub events_data: Vec<u8>,

    /// Keys this transaction touches (for conflict detection)
    pub keys: Vec<String>,

    /// Dependencies: transactions that must execute before this one
    pub deps: Vec<TxnId>,

    /// Current status
    pub status: TxnStatus,

    /// Final execution timestamp (may differ from id.timestamp)
    pub execute_at: Timestamp,
}

/// Ballot number for Paxos-style leader election within a transaction
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct Ballot {
    pub round: u32,
    pub node_id: u16,
}
```

**Acceptance Criteria**:
- [ ] All types implement `bitcode::Encode` + `bitcode::Decode`
- [ ] `TxnId` is totally ordered
- [ ] `Transaction` can be serialized/deserialized round-trip
- [ ] Unit tests for serialization

---

### 1.4 Implement Key Extraction

**File**: `src/keys.rs`

Keys determine which transactions conflict.

```rust
use evento_core::Event;

/// A key representing an aggregate instance
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub String);

impl Key {
    /// Create key from aggregator type and id
    pub fn new(aggregator_type: &str, aggregator_id: &str) -> Self {
        Self(format!("{}:{}", aggregator_type, aggregator_id))
    }

    /// Extract keys from events
    pub fn from_events(events: &[Event]) -> Vec<Key> {
        events
            .iter()
            .map(|e| Key::new(&e.aggregator_type, &e.aggregator_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

/// Check if two transactions conflict (touch same keys)
pub fn conflicts(a: &[Key], b: &[Key]) -> bool {
    // O(n+m) merge check since keys are sorted
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            Ordering::Equal => return true,
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
        }
    }
    false
}
```

**Acceptance Criteria**:
- [ ] Keys extracted correctly from events
- [ ] Conflict detection is correct
- [ ] Keys are sorted and deduplicated
- [ ] Unit tests for key extraction and conflict detection

---

### 1.5 Define Error Types

**File**: `src/error.rs`

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AccordError {
    #[error("transaction {0:?} not found")]
    TxnNotFound(TxnId),

    #[error("transaction {0:?} already exists")]
    TxnExists(TxnId),

    #[error("invalid transaction status: expected {expected:?}, got {actual:?}")]
    InvalidStatus {
        expected: TxnStatus,
        actual: TxnStatus,
    },

    #[error("quorum not reached: got {got}, need {need}")]
    QuorumNotReached { got: usize, need: usize },

    #[error("node {0} not reachable")]
    NodeUnreachable(u16),

    #[error("timeout waiting for {0}")]
    Timeout(String),

    #[error("storage error: {0}")]
    Storage(#[from] evento_core::WriteError),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, AccordError>;
```

**Acceptance Criteria**:
- [ ] All error variants defined
- [ ] Errors implement `std::error::Error`
- [ ] Errors are `Send + Sync`

---

## Definition of Done

- [ ] All files created and compile
- [ ] Unit tests pass: `cargo test -p evento-accord`
- [ ] No clippy warnings: `cargo clippy -p evento-accord`
- [ ] Documentation comments on public types
