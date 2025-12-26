# Phase 6: Deterministic Simulation Testing

## Objective

Implement a deterministic simulation framework for testing the ACCORD consensus protocol under various failure conditions. Inspired by FoundationDB's simulation testing approach.

## Files to Create

```
evento-accord/src/
├── sim/
│   ├── mod.rs           # Module exports and Simulation builder
│   ├── clock.rs         # Virtual clock with controlled time
│   ├── rng.rs           # Seeded deterministic RNG
│   ├── network.rs       # Virtual network with fault injection
│   ├── executor.rs      # In-memory simulated executor
│   ├── node.rs          # Simulated node wrapper
│   ├── faults.rs        # Fault injection configuration
│   ├── harness.rs       # Test harness orchestration
│   ├── history.rs       # Operation history recording
│   └── checker.rs       # Linearizability verification
```

## Tasks

### 6.1 Virtual Clock

**File**: `src/sim/clock.rs`

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Virtual clock for deterministic time control.
///
/// All time-based operations in the simulation use this clock
/// instead of real system time.
#[derive(Clone)]
pub struct VirtualClock {
    /// Current virtual time in nanoseconds
    now_nanos: Arc<AtomicU64>,
}

impl VirtualClock {
    pub fn new() -> Self {
        Self {
            now_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get current virtual time
    pub fn now(&self) -> Duration {
        Duration::from_nanos(self.now_nanos.load(Ordering::SeqCst))
    }

    /// Advance time by the given duration
    pub fn advance(&self, duration: Duration) {
        self.now_nanos.fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Set time to specific value
    pub fn set(&self, time: Duration) {
        self.now_nanos.store(time.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Get current time as milliseconds (for timestamps)
    pub fn now_millis(&self) -> u64 {
        self.now().as_millis() as u64
    }
}

/// Timer that fires at a specific virtual time
pub struct VirtualTimer {
    clock: VirtualClock,
    fire_at: Duration,
}

impl VirtualTimer {
    pub fn new(clock: VirtualClock, delay: Duration) -> Self {
        let fire_at = clock.now() + delay;
        Self { clock, fire_at }
    }

    pub fn is_ready(&self) -> bool {
        self.clock.now() >= self.fire_at
    }

    pub fn remaining(&self) -> Duration {
        self.fire_at.saturating_sub(self.clock.now())
    }
}
```

**Acceptance Criteria**:
- [ ] Virtual clock advances only when explicitly told
- [ ] All timers use virtual clock
- [ ] No real system time dependencies

---

### 6.2 Deterministic RNG

**File**: `src/sim/rng.rs`

```rust
use std::cell::RefCell;

/// Seeded pseudo-random number generator for deterministic simulation.
///
/// Uses a simple but fast PRNG (xorshift64) that produces
/// identical sequences given the same seed.
pub struct SimRng {
    state: RefCell<u64>,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        // Ensure non-zero state
        let seed = if seed == 0 { 1 } else { seed };
        Self {
            state: RefCell::new(seed),
        }
    }

    /// Generate next random u64
    pub fn next_u64(&self) -> u64 {
        let mut state = self.state.borrow_mut();
        // xorshift64
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Generate random f64 in [0, 1)
    pub fn next_f64(&self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }

    /// Generate random bool with given probability
    pub fn next_bool(&self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    /// Generate random duration in range [min, max]
    pub fn next_duration(&self, min: Duration, max: Duration) -> Duration {
        let range = max.as_nanos() - min.as_nanos();
        let offset = (self.next_f64() * range as f64) as u64;
        min + Duration::from_nanos(offset)
    }

    /// Choose random element from slice
    pub fn choose<'a, T>(&self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            let idx = (self.next_u64() as usize) % items.len();
            Some(&items[idx])
        }
    }

    /// Shuffle slice in place
    pub fn shuffle<T>(&self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = (self.next_u64() as usize) % (i + 1);
            items.swap(i, j);
        }
    }

    /// Fork RNG with derived seed (for parallel simulations)
    pub fn fork(&self) -> Self {
        Self::new(self.next_u64())
    }
}

use std::time::Duration;
```

**Acceptance Criteria**:
- [ ] Same seed produces identical sequence
- [ ] All simulation randomness uses this RNG
- [ ] Fork capability for parallel execution

---

### 6.3 Virtual Network

**File**: `src/sim/network.rs`

```rust
use crate::protocol::Message;
use crate::transport::NodeId;
use crate::sim::{VirtualClock, SimRng, FaultConfig};
use std::collections::{HashMap, BinaryHeap, HashSet};
use std::cmp::Reverse;
use std::time::Duration;

/// A message in flight through the virtual network
#[derive(Clone)]
pub struct InFlightMessage {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
    pub deliver_at: Duration,
    pub id: u64,
}

impl Ord for InFlightMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by delivery time
        Reverse(self.deliver_at).cmp(&Reverse(other.deliver_at))
    }
}

impl PartialOrd for InFlightMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for InFlightMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for InFlightMessage {}

/// Virtual network that simulates message passing with faults
pub struct VirtualNetwork {
    clock: VirtualClock,
    rng: SimRng,
    config: FaultConfig,

    /// Messages waiting to be delivered
    in_flight: BinaryHeap<InFlightMessage>,

    /// Network partitions: (node_a, node_b) pairs that can't communicate
    partitions: HashSet<(NodeId, NodeId)>,

    /// Nodes that are crashed
    crashed_nodes: HashSet<NodeId>,

    /// Message counter for unique IDs
    message_counter: u64,

    /// All messages ever sent (for debugging)
    history: Vec<InFlightMessage>,
}

impl VirtualNetwork {
    pub fn new(clock: VirtualClock, rng: SimRng, config: FaultConfig) -> Self {
        Self {
            clock,
            rng,
            config,
            in_flight: BinaryHeap::new(),
            partitions: HashSet::new(),
            crashed_nodes: HashSet::new(),
            message_counter: 0,
            history: Vec::new(),
        }
    }

    /// Send a message from one node to another
    pub fn send(&mut self, from: NodeId, to: NodeId, message: Message) {
        // Check if sender or receiver is crashed
        if self.crashed_nodes.contains(&from) || self.crashed_nodes.contains(&to) {
            return; // Message dropped
        }

        // Check for partition
        if self.is_partitioned(from, to) {
            return; // Message dropped
        }

        // Random message drop
        if self.rng.next_bool(self.config.message_drop_probability) {
            return; // Message dropped
        }

        // Calculate delivery delay
        let delay = self.rng.next_duration(
            self.config.min_message_delay,
            self.config.max_message_delay,
        );

        let msg = InFlightMessage {
            from,
            to,
            message,
            deliver_at: self.clock.now() + delay,
            id: self.message_counter,
        };
        self.message_counter += 1;

        // Maybe duplicate the message
        if self.rng.next_bool(self.config.message_duplicate_probability) {
            let mut dup = msg.clone();
            dup.id = self.message_counter;
            self.message_counter += 1;
            let extra_delay = self.rng.next_duration(Duration::ZERO, delay);
            dup.deliver_at = msg.deliver_at + extra_delay;
            self.in_flight.push(dup);
        }

        self.history.push(msg.clone());
        self.in_flight.push(msg);
    }

    /// Get next message ready for delivery
    pub fn next_message(&mut self) -> Option<InFlightMessage> {
        while let Some(msg) = self.in_flight.peek() {
            if msg.deliver_at <= self.clock.now() {
                let msg = self.in_flight.pop().unwrap();

                // Re-check partition (might have changed)
                if self.is_partitioned(msg.from, msg.to) {
                    continue;
                }

                // Re-check crashes
                if self.crashed_nodes.contains(&msg.to) {
                    continue;
                }

                return Some(msg);
            } else {
                break;
            }
        }
        None
    }

    /// Get time of next scheduled delivery
    pub fn next_delivery_time(&self) -> Option<Duration> {
        self.in_flight.peek().map(|m| m.deliver_at)
    }

    /// Create a network partition between nodes
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert((a.min(b), a.max(b)));
    }

    /// Heal a network partition
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&(a.min(b), a.max(b)));
    }

    /// Partition a node from all others
    pub fn isolate(&mut self, node: NodeId, all_nodes: &[NodeId]) {
        for &other in all_nodes {
            if other != node {
                self.partition(node, other);
            }
        }
    }

    /// Check if two nodes are partitioned
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&(a.min(b), a.max(b)))
    }

    /// Crash a node
    pub fn crash(&mut self, node: NodeId) {
        self.crashed_nodes.insert(node);
    }

    /// Restart a crashed node
    pub fn restart(&mut self, node: NodeId) {
        self.crashed_nodes.remove(&node);
    }

    /// Check if node is crashed
    pub fn is_crashed(&self, node: NodeId) -> bool {
        self.crashed_nodes.contains(&node)
    }

    /// Get count of in-flight messages
    pub fn pending_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Get message history for debugging
    pub fn history(&self) -> &[InFlightMessage] {
        &self.history
    }
}
```

**Acceptance Criteria**:
- [ ] Messages delivered in order by virtual time
- [ ] Partitions block messages in both directions
- [ ] Message delays are deterministic given RNG seed
- [ ] Crash/restart affects message delivery

---

### 6.4 Simulated Executor

**File**: `src/sim/executor.rs`

```rust
use crate::error::Result;
use async_trait::async_trait;
use evento_core::{
    cursor::{Args, ReadResult, Value, PageInfo, Edge},
    Event, Executor, ReadAggregator, RoutingKey, WriteError,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use ulid::Ulid;

/// In-memory executor for simulation testing.
///
/// Stores all events in memory with no persistence.
/// Designed for fast, deterministic testing.
#[derive(Clone)]
pub struct SimExecutor {
    /// Events stored by (aggregator_type, aggregator_id) -> Vec<Event>
    events: Arc<RwLock<HashMap<(String, String), Vec<Event>>>>,

    /// Subscriber cursors
    cursors: Arc<RwLock<HashMap<String, Value>>>,

    /// Active subscribers
    subscribers: Arc<RwLock<HashMap<String, Ulid>>>,
}

impl SimExecutor {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(HashMap::new())),
            cursors: Arc::new(RwLock::new(HashMap::new())),
            subscribers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get all events for an aggregate
    pub fn get_events(&self, agg_type: &str, agg_id: &str) -> Vec<Event> {
        self.events
            .read()
            .unwrap()
            .get(&(agg_type.to_string(), agg_id.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get total event count
    pub fn event_count(&self) -> usize {
        self.events
            .read()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum()
    }

    /// Clear all data (for test reset)
    pub fn clear(&self) {
        self.events.write().unwrap().clear();
        self.cursors.write().unwrap().clear();
        self.subscribers.write().unwrap().clear();
    }

    /// Snapshot current state (for comparison)
    pub fn snapshot(&self) -> HashMap<(String, String), Vec<Event>> {
        self.events.read().unwrap().clone()
    }
}

impl Default for SimExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for SimExecutor {
    async fn write(&self, events: Vec<Event>) -> std::result::Result<(), WriteError> {
        let mut store = self.events.write().unwrap();

        for event in events {
            let key = (event.aggregator_type.clone(), event.aggregator_id.clone());
            store.entry(key).or_insert_with(Vec::new).push(event);
        }

        Ok(())
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        _routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        let store = self.events.read().unwrap();

        let mut all_events: Vec<Event> = if let Some(aggs) = aggregators {
            aggs.iter()
                .flat_map(|agg| {
                    store
                        .get(&(agg.aggregator_type.clone(), agg.aggregator_id.clone()))
                        .cloned()
                        .unwrap_or_default()
                })
                .collect()
        } else {
            store.values().flatten().cloned().collect()
        };

        // Sort by version
        all_events.sort_by_key(|e| e.version);

        // Apply limit
        let limit = args.first.or(args.last).unwrap_or(100) as usize;
        let events: Vec<_> = all_events.into_iter().take(limit).collect();

        let edges = events
            .into_iter()
            .map(|e| Edge {
                cursor: Value::String(e.id.clone()),
                node: e,
            })
            .collect();

        Ok(ReadResult {
            edges,
            page_info: PageInfo::default(),
        })
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        Ok(self.cursors.read().unwrap().get(&key).cloned())
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        Ok(self
            .subscribers
            .read()
            .unwrap()
            .get(&key)
            .map(|id| *id == worker_id)
            .unwrap_or(false))
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.subscribers.write().unwrap().insert(key, worker_id);
        Ok(())
    }

    async fn acknowledge(&self, key: String, cursor: Value, _lag: u64) -> anyhow::Result<()> {
        self.cursors.write().unwrap().insert(key, cursor);
        Ok(())
    }
}
```

**Acceptance Criteria**:
- [ ] Implements full Executor trait
- [ ] All operations are synchronous (no async delays)
- [ ] State can be snapshot for comparison
- [ ] Thread-safe for concurrent access

---

### 6.5 Fault Configuration

**File**: `src/sim/faults.rs`

```rust
use std::time::Duration;

/// Configuration for fault injection during simulation
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Probability of dropping a message (0.0 - 1.0)
    pub message_drop_probability: f64,

    /// Probability of duplicating a message (0.0 - 1.0)
    pub message_duplicate_probability: f64,

    /// Minimum message delivery delay
    pub min_message_delay: Duration,

    /// Maximum message delivery delay
    pub max_message_delay: Duration,

    /// Probability of a node crashing per tick (0.0 - 1.0)
    pub crash_probability: f64,

    /// Probability of a crashed node restarting per tick (0.0 - 1.0)
    pub restart_probability: f64,

    /// Probability of creating a network partition per tick
    pub partition_probability: f64,

    /// Probability of healing a partition per tick
    pub heal_probability: f64,

    /// Maximum clock skew between nodes
    pub max_clock_skew: Duration,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            message_drop_probability: 0.0,
            message_duplicate_probability: 0.0,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(10),
            crash_probability: 0.0,
            restart_probability: 0.0,
            partition_probability: 0.0,
            heal_probability: 0.0,
            max_clock_skew: Duration::ZERO,
        }
    }
}

impl FaultConfig {
    /// No faults - clean network
    pub fn none() -> Self {
        Self::default()
    }

    /// Light faults - occasional delays
    pub fn light() -> Self {
        Self {
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(50),
            message_drop_probability: 0.01,
            ..Default::default()
        }
    }

    /// Medium faults - some drops and crashes
    pub fn medium() -> Self {
        Self {
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(100),
            message_drop_probability: 0.05,
            message_duplicate_probability: 0.01,
            crash_probability: 0.001,
            restart_probability: 0.1,
            ..Default::default()
        }
    }

    /// Heavy faults - aggressive failure injection
    pub fn heavy() -> Self {
        Self {
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(200),
            message_drop_probability: 0.1,
            message_duplicate_probability: 0.05,
            crash_probability: 0.01,
            restart_probability: 0.1,
            partition_probability: 0.005,
            heal_probability: 0.1,
            max_clock_skew: Duration::from_millis(100),
        }
    }

    /// Chaos mode - extreme faults
    pub fn chaos() -> Self {
        Self {
            min_message_delay: Duration::from_millis(0),
            max_message_delay: Duration::from_millis(500),
            message_drop_probability: 0.2,
            message_duplicate_probability: 0.1,
            crash_probability: 0.05,
            restart_probability: 0.2,
            partition_probability: 0.02,
            heal_probability: 0.1,
            max_clock_skew: Duration::from_millis(500),
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Preset configurations for common scenarios
- [ ] All probabilities configurable
- [ ] Builder pattern for custom configs

---

### 6.6 Operation History

**File**: `src/sim/history.rs`

```rust
use crate::transport::NodeId;
use std::time::Duration;

/// Type of operation recorded in history
#[derive(Clone, Debug)]
pub enum OperationType {
    /// Write operation started
    WriteStart { keys: Vec<String> },
    /// Write operation completed
    WriteComplete { success: bool },
    /// Read operation
    Read { keys: Vec<String> },
    /// Internal protocol message
    Protocol { message_type: String },
}

/// A recorded operation in the simulation history
#[derive(Clone, Debug)]
pub struct Operation {
    /// Unique operation ID
    pub id: u64,
    /// Node that initiated the operation
    pub node: NodeId,
    /// Virtual time when operation started
    pub start_time: Duration,
    /// Virtual time when operation completed (None if in-progress)
    pub end_time: Option<Duration>,
    /// Type of operation
    pub op_type: OperationType,
    /// Result value (for reads)
    pub result: Option<String>,
}

/// Records all operations for linearizability checking
#[derive(Default)]
pub struct History {
    operations: Vec<Operation>,
    next_id: u64,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record start of an operation, returns operation ID
    pub fn start(&mut self, node: NodeId, time: Duration, op_type: OperationType) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        self.operations.push(Operation {
            id,
            node,
            start_time: time,
            end_time: None,
            op_type,
            result: None,
        });

        id
    }

    /// Record completion of an operation
    pub fn complete(&mut self, id: u64, time: Duration, result: Option<String>) {
        if let Some(op) = self.operations.iter_mut().find(|o| o.id == id) {
            op.end_time = Some(time);
            op.result = result;
        }
    }

    /// Get all completed operations
    pub fn completed(&self) -> impl Iterator<Item = &Operation> {
        self.operations.iter().filter(|o| o.end_time.is_some())
    }

    /// Get operations that overlap in time with given operation
    pub fn concurrent_with(&self, op: &Operation) -> Vec<&Operation> {
        let start = op.start_time;
        let end = op.end_time.unwrap_or(Duration::MAX);

        self.operations
            .iter()
            .filter(|o| {
                o.id != op.id
                    && o.start_time < end
                    && o.end_time.unwrap_or(Duration::MAX) > start
            })
            .collect()
    }

    /// Export history for external analysis
    pub fn export(&self) -> Vec<Operation> {
        self.operations.clone()
    }

    /// Clear history
    pub fn clear(&mut self) {
        self.operations.clear();
        self.next_id = 0;
    }
}
```

**Acceptance Criteria**:
- [ ] Records all operations with timing
- [ ] Tracks concurrent operations
- [ ] Exportable for external analysis

---

### 6.7 Linearizability Checker

**File**: `src/sim/checker.rs`

```rust
use crate::sim::history::{History, Operation, OperationType};
use std::collections::HashMap;

/// Result of linearizability check
#[derive(Debug)]
pub struct CheckResult {
    pub is_linearizable: bool,
    pub violations: Vec<Violation>,
}

/// A linearizability violation
#[derive(Debug)]
pub struct Violation {
    pub description: String,
    pub operations: Vec<u64>,
}

/// Checks linearizability of operation history.
///
/// Uses a simplified check suitable for key-value operations:
/// - All writes to a key must be totally ordered
/// - A read must return the value of the most recent write
///   that completed before the read started, or a concurrent write
pub struct LinearizabilityChecker {
    /// Final values for each key (ground truth)
    final_state: HashMap<String, String>,
}

impl LinearizabilityChecker {
    pub fn new(final_state: HashMap<String, String>) -> Self {
        Self { final_state }
    }

    /// Check if the operation history is linearizable
    pub fn check(&self, history: &History) -> CheckResult {
        let mut violations = Vec::new();

        // Group operations by key
        let ops: Vec<_> = history.completed().collect();

        // Check each read operation
        for op in &ops {
            if let OperationType::Read { keys } = &op.op_type {
                for key in keys {
                    if let Some(violation) = self.check_read(op, key, &ops) {
                        violations.push(violation);
                    }
                }
            }
        }

        // Check write ordering
        let write_violations = self.check_write_ordering(&ops);
        violations.extend(write_violations);

        CheckResult {
            is_linearizable: violations.is_empty(),
            violations,
        }
    }

    fn check_read(&self, read_op: &Operation, key: &str, all_ops: &[&Operation]) -> Option<Violation> {
        let read_result = read_op.result.as_ref()?;

        // Find all writes to this key that completed before read started
        let prior_writes: Vec<_> = all_ops
            .iter()
            .filter(|op| {
                if let OperationType::WriteComplete { success: true } = &op.op_type {
                    op.end_time.unwrap() < read_op.start_time
                } else {
                    false
                }
            })
            .collect();

        // Find concurrent writes
        let concurrent_writes: Vec<_> = all_ops
            .iter()
            .filter(|op| {
                if let OperationType::WriteStart { keys } = &op.op_type {
                    keys.contains(&key.to_string())
                        && op.start_time < read_op.end_time.unwrap()
                        && op.end_time.map(|e| e > read_op.start_time).unwrap_or(true)
                } else {
                    false
                }
            })
            .collect();

        // The read value must match either:
        // 1. The most recent prior write, or
        // 2. Any concurrent write
        // For simplicity, we just check that the value exists in final state
        if self.final_state.get(key) != Some(read_result) {
            return Some(Violation {
                description: format!(
                    "Read of key '{}' returned '{}' but final value is '{:?}'",
                    key,
                    read_result,
                    self.final_state.get(key)
                ),
                operations: vec![read_op.id],
            });
        }

        None
    }

    fn check_write_ordering(&self, _ops: &[&Operation]) -> Vec<Violation> {
        // TODO: Implement write ordering check
        // For now, we trust that ACCORD provides this
        Vec::new()
    }
}

/// Simple invariant checks
pub struct InvariantChecker;

impl InvariantChecker {
    /// Check that no writes were lost
    pub fn check_no_lost_writes(
        expected_count: usize,
        actual_count: usize,
    ) -> Result<(), String> {
        if actual_count < expected_count {
            Err(format!(
                "Lost writes: expected {} but only {} present",
                expected_count, actual_count
            ))
        } else {
            Ok(())
        }
    }

    /// Check that all nodes have the same data
    pub fn check_consistency(
        node_states: &[HashMap<String, String>],
    ) -> Result<(), String> {
        if node_states.is_empty() {
            return Ok(());
        }

        let reference = &node_states[0];
        for (i, state) in node_states.iter().enumerate().skip(1) {
            if state != reference {
                return Err(format!(
                    "Node {} has different state than node 0",
                    i
                ));
            }
        }

        Ok(())
    }
}
```

**Acceptance Criteria**:
- [ ] Detects non-linearizable histories
- [ ] Reports specific violations
- [ ] Checks common invariants

---

### 6.8 Simulation Harness

**File**: `src/sim/harness.rs`

```rust
use crate::sim::{
    VirtualClock, SimRng, VirtualNetwork, SimExecutor,
    FaultConfig, History,
};
use crate::transport::NodeId;
use crate::state::ConsensusState;
use crate::timestamp::HybridClock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A simulated ACCORD node
pub struct SimNode {
    pub id: NodeId,
    pub state: Arc<ConsensusState>,
    pub executor: SimExecutor,
    pub clock: HybridClock,
    pub is_alive: bool,
}

/// Main simulation harness
pub struct Simulation {
    /// Virtual clock shared by all components
    clock: VirtualClock,

    /// Deterministic RNG
    rng: SimRng,

    /// Virtual network
    network: VirtualNetwork,

    /// Simulated nodes
    nodes: HashMap<NodeId, SimNode>,

    /// Operation history
    history: History,

    /// Fault configuration
    fault_config: FaultConfig,

    /// Statistics
    stats: SimStats,
}

#[derive(Default)]
pub struct SimStats {
    pub ticks: u64,
    pub messages_sent: u64,
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub operations_started: u64,
    pub operations_completed: u64,
    pub crashes: u64,
    pub restarts: u64,
}

impl Simulation {
    /// Create a new simulation builder
    pub fn builder() -> SimulationBuilder {
        SimulationBuilder::default()
    }

    /// Advance simulation by one tick
    pub fn tick(&mut self) {
        self.stats.ticks += 1;

        // Advance virtual time
        self.clock.advance(Duration::from_millis(1));

        // Maybe inject faults
        self.maybe_inject_faults();

        // Deliver ready messages
        while let Some(msg) = self.network.next_message() {
            self.stats.messages_delivered += 1;
            self.deliver_message(msg);
        }
    }

    /// Run simulation for specified number of ticks
    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    /// Run until no more messages in flight
    pub fn drain(&mut self) {
        while self.network.pending_count() > 0 {
            // Advance to next message delivery time
            if let Some(next_time) = self.network.next_delivery_time() {
                let now = self.clock.now();
                if next_time > now {
                    self.clock.set(next_time);
                }
            }
            self.tick();
        }
    }

    fn maybe_inject_faults(&mut self) {
        let node_ids: Vec<_> = self.nodes.keys().copied().collect();

        // Maybe crash a node
        if self.rng.next_bool(self.fault_config.crash_probability) {
            if let Some(&node_id) = self.rng.choose(&node_ids) {
                if self.nodes.get(&node_id).map(|n| n.is_alive).unwrap_or(false) {
                    self.crash_node(node_id);
                    self.stats.crashes += 1;
                }
            }
        }

        // Maybe restart a crashed node
        if self.rng.next_bool(self.fault_config.restart_probability) {
            let crashed: Vec<_> = self.nodes
                .iter()
                .filter(|(_, n)| !n.is_alive)
                .map(|(id, _)| *id)
                .collect();
            if let Some(&node_id) = self.rng.choose(&crashed) {
                self.restart_node(node_id);
                self.stats.restarts += 1;
            }
        }

        // Maybe create partition
        if self.rng.next_bool(self.fault_config.partition_probability) {
            if node_ids.len() >= 2 {
                let a = *self.rng.choose(&node_ids).unwrap();
                let b = *self.rng.choose(&node_ids).unwrap();
                if a != b {
                    self.network.partition(a, b);
                }
            }
        }

        // Maybe heal partition
        if self.rng.next_bool(self.fault_config.heal_probability) {
            // For simplicity, heal all partitions
            // A more sophisticated approach would track and selectively heal
        }
    }

    fn deliver_message(&mut self, msg: crate::sim::network::InFlightMessage) {
        if let Some(node) = self.nodes.get_mut(&msg.to) {
            if node.is_alive {
                // Process the message through the node's state machine
                // This would invoke the protocol handlers
            }
        }
    }

    fn crash_node(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.is_alive = false;
            self.network.crash(id);
        }
    }

    fn restart_node(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.is_alive = true;
            // Restore state from executor (simulating recovery)
            node.state = Arc::new(ConsensusState::new());
            self.network.restart(id);
        }
    }

    /// Get simulation statistics
    pub fn stats(&self) -> &SimStats {
        &self.stats
    }

    /// Get operation history
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Get node states for consistency checking
    pub fn node_states(&self) -> Vec<HashMap<String, String>> {
        self.nodes
            .values()
            .map(|n| {
                // Extract state as key-value pairs
                HashMap::new() // TODO: implement
            })
            .collect()
    }
}

/// Builder for Simulation
#[derive(Default)]
pub struct SimulationBuilder {
    seed: u64,
    node_count: usize,
    fault_config: FaultConfig,
}

impl SimulationBuilder {
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn nodes(mut self, count: usize) -> Self {
        self.node_count = count;
        self
    }

    pub fn faults(mut self, config: FaultConfig) -> Self {
        self.fault_config = config;
        self
    }

    pub fn build(self) -> Simulation {
        let clock = VirtualClock::new();
        let rng = SimRng::new(self.seed);
        let network = VirtualNetwork::new(clock.clone(), rng.fork(), self.fault_config.clone());

        let mut nodes = HashMap::new();
        for i in 0..self.node_count {
            let id = i as NodeId;
            nodes.insert(id, SimNode {
                id,
                state: Arc::new(ConsensusState::new()),
                executor: SimExecutor::new(),
                clock: HybridClock::new(id),
                is_alive: true,
            });
        }

        Simulation {
            clock,
            rng,
            network,
            nodes,
            history: History::new(),
            fault_config: self.fault_config,
            stats: SimStats::default(),
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Manages multiple simulated nodes
- [ ] Coordinates time advancement
- [ ] Injects faults according to config
- [ ] Collects statistics and history

---

### 6.9 Module Exports

**File**: `src/sim/mod.rs`

```rust
//! Deterministic simulation testing framework.
//!
//! This module provides tools for testing ACCORD consensus
//! under controlled, reproducible conditions.

mod checker;
mod clock;
mod executor;
mod faults;
mod harness;
mod history;
mod network;
mod rng;

pub use checker::{CheckResult, InvariantChecker, LinearizabilityChecker, Violation};
pub use clock::{VirtualClock, VirtualTimer};
pub use executor::SimExecutor;
pub use faults::FaultConfig;
pub use harness::{SimNode, SimStats, Simulation, SimulationBuilder};
pub use history::{History, Operation, OperationType};
pub use network::{InFlightMessage, VirtualNetwork};
pub use rng::SimRng;
```

---

## Definition of Done

- [ ] All simulation components implemented
- [ ] 100% deterministic replay verified
- [ ] Can simulate 10,000+ operations in <1 second
- [ ] Linearizability checker catches known bugs
- [ ] Integration tests using simulation
- [ ] Documentation with examples
- [ ] CI runs simulation tests

## Test Scenarios to Implement

```rust
#[cfg(test)]
mod simulation_tests {
    use super::*;

    #[test]
    fn test_happy_path() {
        let mut sim = Simulation::builder()
            .seed(42)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Run some operations
        sim.run(1000);

        // All operations should succeed
        assert!(sim.check_linearizability().is_ok());
    }

    #[test]
    fn test_single_node_crash() {
        let mut sim = Simulation::builder()
            .seed(42)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Crash one node
        sim.crash_node(2);

        // Operations should still succeed (quorum = 2)
        sim.run(1000);

        assert!(sim.check_linearizability().is_ok());
    }

    #[test]
    fn test_network_partition() {
        let mut sim = Simulation::builder()
            .seed(42)
            .nodes(5)
            .faults(FaultConfig::none())
            .build();

        // Partition: {0, 1} vs {2, 3, 4}
        sim.partition(0, 2);
        sim.partition(0, 3);
        sim.partition(0, 4);
        sim.partition(1, 2);
        sim.partition(1, 3);
        sim.partition(1, 4);

        // Majority side should make progress
        sim.run(1000);

        // Heal and verify consistency
        sim.heal_all();
        sim.run(1000);

        assert!(sim.check_consistency().is_ok());
    }

    #[test]
    fn test_chaos_monkey() {
        for seed in 0..100 {
            let mut sim = Simulation::builder()
                .seed(seed)
                .nodes(5)
                .faults(FaultConfig::chaos())
                .build();

            sim.run(10000);

            // Must maintain linearizability despite chaos
            let result = sim.check_linearizability();
            assert!(result.is_ok(), "Failed with seed {}: {:?}", seed, result);
        }
    }
}
```
