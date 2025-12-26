# ACCORD Consensus for Evento - Product Requirements Document

## Overview

Implement the ACCORD consensus protocol as a new crate `evento-accord` that wraps the existing `Executor` trait to provide distributed consensus for event writes across a cluster of nodes.

## Goals

1. **Strict Serializability**: All nodes observe the same order of conflicting transactions
2. **Low Latency**: Single round-trip for non-conflicting transactions (fast path)
3. **Fault Tolerance**: Survive f failures with 2f+1 nodes
4. **Seamless Integration**: Drop-in replacement via `Executor` trait wrapper
5. **Incremental Adoption**: Support single-node mode for development/testing

## Non-Goals (v1)

- Dynamic membership (nodes joining/leaving)
- Sharding/partitioning across disjoint replica sets
- Cross-datacenter optimizations
- Read replicas / follower reads

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                             │
│                          │                                   │
│                          ▼                                   │
│                 ┌─────────────────┐                         │
│                 │ AccordExecutor  │                         │
│                 │   <E: Executor> │                         │
│                 └────────┬────────┘                         │
│                          │                                   │
│         ┌────────────────┼────────────────┐                 │
│         ▼                ▼                ▼                 │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│   │  State   │    │ Protocol │    │Transport │             │
│   │ Machine  │    │  Engine  │    │  Layer   │             │
│   └──────────┘    └──────────┘    └──────────┘             │
│         │                                │                   │
│         ▼                                ▼                   │
│   ┌──────────┐                    ┌──────────┐             │
│   │  Inner   │                    │  Other   │             │
│   │ Executor │                    │  Nodes   │             │
│   └──────────┘                    └──────────┘             │
└─────────────────────────────────────────────────────────────┘
```

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Transport | Custom TCP + Bitcode | Reuse existing serialization, minimal deps |
| State Storage | Executor trait (same DB) | Atomic consistency, simpler ops |
| Topology | Single-node → Fixed replica set | Incremental complexity |
| Clock | Hybrid Logical Clock (HLC) | Causality + real-time ordering |

## Success Metrics

- Single-node: No performance regression vs raw Executor
- 3-node cluster: <5ms p99 latency for fast path
- 5-node cluster: <10ms p99 latency for fast path
- Correctness: Pass Jepsen-style linearizability tests

## Phases

1. **Phase 1**: Core types (Timestamp, TxnId, Transaction, Ballot) ✅
2. **Phase 2**: State machine (ConsensusState, TxnStore, DependencyGraph) ✅
3. **Phase 3**: Protocol engine (PreAccept, Accept, Commit, Execute) ✅
4. **Phase 4**: TCP transport layer ✅
5. **Phase 5**: AccordExecutor integration ✅
6. **Phase 6**: Deterministic simulation testing (framework) ✅
7. **Phase 7**: Simulation with real ACCORD protocol ✅
8. **Phase 8**: Event replication ✅
9. **Phase 9**: Durability & recovery ✅
10. **Phase 10**: Production hardening ✅

## Phase 6: Deterministic Simulation Testing

### Overview

Implement a deterministic simulation framework inspired by FoundationDB's approach. This enables:
- **Reproducible failures**: Same seed = same execution, every time
- **Exhaustive testing**: Test years of operation in seconds
- **Fault injection**: Network partitions, node crashes, message delays
- **Property verification**: Prove linearizability and consistency

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Simulation Harness                        │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Deterministic│  │   Virtual    │  │  Fault Injector   │  │
│  │   Runtime   │  │   Network    │  │  (partitions,     │  │
│  │  (seeded    │  │  (delays,    │  │   crashes, slow)  │  │
│  │   RNG)      │  │   ordering)  │  │                   │  │
│  └─────────────┘  └──────────────┘  └───────────────────┘  │
│                           │                                  │
│  ┌────────────────────────┴────────────────────────────┐   │
│  │              Simulated Cluster                       │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐             │   │
│  │  │ Node 0  │  │ Node 1  │  │ Node 2  │  ...        │   │
│  │  │(Accord) │  │(Accord) │  │(Accord) │             │   │
│  │  └─────────┘  └─────────┘  └─────────┘             │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌────────────────────────┴────────────────────────────┐   │
│  │           Property Checkers / Invariants             │   │
│  │  • Linearizability   • No lost writes                │   │
│  │  • Consistency       • Progress (liveness)           │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Components

| Component | File | Purpose |
|-----------|------|---------|
| Virtual Clock | `sim/clock.rs` | Controlled time progression |
| Deterministic RNG | `sim/rng.rs` | Seeded random for reproducibility |
| Virtual Network | `sim/network.rs` | Message delivery with faults |
| Simulated Executor | `sim/executor.rs` | In-memory event storage |
| Simulated Node | `sim/node.rs` | Node lifecycle management |
| Fault Injector | `sim/faults.rs` | Partition, crash, delay injection |
| Test Harness | `sim/harness.rs` | Orchestrates simulation runs |
| Linearizability Checker | `sim/checker.rs` | Verifies correctness properties |
| History Recorder | `sim/history.rs` | Records operations for analysis |

### Key Features

#### 1. Deterministic Execution
- All randomness seeded from single seed
- Virtual time (no real sleeps)
- Deterministic task scheduling
- Same seed = identical execution path

#### 2. Network Simulation
- Configurable latency distributions
- Message reordering within bounds
- Packet loss / duplication
- Network partitions (symmetric/asymmetric)

#### 3. Fault Injection
- Node crashes (clean/unclean)
- Node restarts with state recovery
- Slow nodes (simulated GC pauses)
- Clock skew between nodes

#### 4. Property Verification
- **Linearizability**: Every operation appears atomic
- **Durability**: Committed writes survive failures
- **Progress**: System eventually makes progress
- **Consistency**: All nodes agree on order

### Test Scenarios

| Scenario | Description |
|----------|-------------|
| Happy path | No faults, verify correctness |
| Leader crash | Coordinator fails mid-protocol |
| Network partition | Minority/majority splits |
| Message delay | Slow messages cause retries |
| Cascading failure | Multiple nodes fail sequentially |
| Split brain | Asymmetric partition |
| Recovery | Node rejoins after crash |

### Usage Example

```rust
use evento_accord::sim::{Simulation, FaultConfig, Scenario};

#[test]
fn test_partition_tolerance() {
    let sim = Simulation::new()
        .seed(12345)
        .nodes(5)
        .faults(FaultConfig {
            partition_probability: 0.1,
            crash_probability: 0.05,
            max_message_delay: Duration::from_millis(100),
        })
        .build();

    // Run random workload
    sim.run_scenario(Scenario::RandomWrites {
        operations: 1000,
        concurrency: 10,
    });

    // Verify properties
    assert!(sim.check_linearizability());
    assert!(sim.check_no_lost_writes());
}
```

### Success Criteria

- [x] 100% deterministic replay with same seed
- [x] Can simulate 10,000+ operations in <1 second
- [ ] Catches known distributed system bugs (stale reads, lost writes)
- [ ] Linearizability checker validates all operations
- [ ] CI runs simulation tests on every commit

### Current Status

Phase 6 framework is complete:
- VirtualClock, SimRng, VirtualNetwork implemented
- FaultConfig with presets (none, light, medium, heavy, chaos)
- SimExecutor (in-memory mock executor)
- Simulation harness with bank simulation tests
- Continuous runner (`examples/sim_runner.rs`) and debug tool (`examples/sim_debug.rs`)

**Limitation**: SimExecutor is a mock that doesn't run the real ACCORD protocol. Consistency failures on crash are expected because nodes lose in-memory state.

## Phase 7: Simulation with Real ACCORD Protocol

### Overview

Wire the real ACCORD protocol into the simulation framework so we can test correctness under faults.

### Components

| Component | Purpose |
|-----------|---------|
| `SimTransport` | Implements `Transport` trait using `VirtualNetwork` |
| `SimAccordNode` | Wraps real `ConsensusState` + protocol handlers |
| `SimCluster` | Manages multiple `SimAccordNode` instances |

### Implementation

1. Create `SimTransport` that routes messages through `VirtualNetwork`
2. Create `SimAccordNode` with real `ConsensusState` and protocol handlers
3. Replace `SimExecutor` with nodes running actual ACCORD consensus
4. Messages flow through `VirtualNetwork` (subject to delays, drops, partitions)
5. Coordinator broadcasts PreAccept/Accept/Commit through virtual network
6. Verify consistency checks pass with real protocol

### Success Criteria

- [x] Simulation uses real ACCORD protocol code
- [x] Protocol messages flow through VirtualNetwork
- [x] Fast path and slow path both work in simulation
- [x] No consistency failures without crashes

### Current Status

Phase 7 is complete:
- `SimCoordinator` coordinates ACCORD protocol in simulation
- Protocol messages (PreAccept/Accept/Commit) flow through VirtualNetwork
- Transactions commit successfully through consensus
- Tests verify basic protocol and multiple transactions
- Events stored at coordinator (replication in Phase 8)
- **Refactored**: `SimCoordinator` now delegates to real protocol code (`protocol::preaccept::analyze_responses`, `protocol::accept::create_request`, `protocol::commit::create_request`) ensuring changes to the ACCORD protocol automatically apply to simulation

## Phase 8: Event Replication

### Overview

Currently only transaction metadata is replicated. Events must also be replicated for all nodes to have identical event logs.

### Implementation

1. Serialize events into `Transaction.events_data` at coordinator
2. Include events in `Commit` messages broadcast to replicas
3. Each replica stores events and executes against inner Executor
4. Execution happens in `execute_at` timestamp order
5. All replicas converge to identical event logs

### Current Status

Phase 8 is complete:
- `SerializableEvent` wrapper provides bitcode serialization for `Event` (converts `Ulid` to `[u8; 16]`)
- Events encoded into `Transaction.events_data` via `SerializableEvent::encode_events()`
- Commit messages include serialized events for replication
- Replicas decode and store events on commit
- `execute_ready()` writes replicated events to SimExecutor
- Test verifies all nodes have identical events after simulation

### Success Criteria

- [x] Events serialized via `SerializableEvent` wrapper
- [x] Events included in transactions and Commit messages
- [x] Events replicated to all nodes on commit
- [x] All replicas have identical events after simulation
- [x] Event ordering is deterministic across nodes

## Phase 9: Durability & Recovery

### Overview

Add crash recovery so nodes can rejoin the cluster after restart without losing committed data.

### Components

| Component | File | Purpose |
|-----------|------|---------|
| `DurableStore` trait | `state/durable.rs` | Abstract persistent storage interface |
| `MemoryDurableStore` | `state/durable.rs` | In-memory implementation for testing |
| `SimDurableStore` | `sim/durable.rs` | Simulated persistence (survives restart) |
| `SharedDurableStorage` | `sim/durable.rs` | Shared storage across simulated crashes |
| `SyncRequest/Response` | `protocol/messages.rs` | State sync protocol messages |
| `sync` module | `protocol/sync.rs` | Sync protocol handlers |

### Implementation

1. **DurableStore Trait**
   - `persist(&Transaction)` - Persist committed transaction
   - `load_committed()` - Load all committed transactions
   - `mark_executed(TxnId)` - Mark transaction as executed
   - `get_last_executed()` - Get last executed transaction ID

2. **SimDurableStore**
   - Uses `std::sync::RwLock` for synchronous access (avoids tokio deadlocks)
   - `SharedDurableStorage` survives simulated node crashes
   - Each node has isolated storage identified by `NodeId`

3. **Sync Protocol**
   - `SyncRequest` - Request missing transactions since timestamp
   - `SyncResponse` - Return list of committed transactions
   - `anti_entropy_sync()` - Sync all nodes for consistency

4. **Crash Recovery**
   - On restart, load persisted state from `SimDurableStore`
   - Sync with alive peers via `sync_with_peers()`
   - Execute ready transactions after sync
   - Rejoin cluster once caught up

5. **Deadlock Fix**
   - Fixed `create_from_commit()` to release locks before calling `commit()`
   - Simulation uses own tokio runtime to avoid conflicts with test runtime
   - Consolidated async operations in `anti_entropy_sync()` to minimize `block_on` calls

### Current Status

Phase 9 is complete:
- `DurableStore` trait with `MemoryDurableStore` and `SimDurableStore` implementations
- Sync protocol with `SyncRequest`/`SyncResponse` messages
- Nodes sync on restart and via periodic anti-entropy
- Fixed deadlock in `create_from_commit()` (was holding locks while calling `commit()`)
- All 205 tests pass including crash recovery scenarios

### Success Criteria

- [x] Committed transactions survive simulated crashes
- [x] Crashed nodes recover and catch up from peers
- [x] Consistency checks pass even with crashes
- [x] No data loss for committed transactions
- [x] No deadlocks during sync operations

## Phase 10: Production Hardening

### Overview

Production-ready features for operating ACCORD clusters.

### Components

1. **Graceful Shutdown** ✅
   - `ShutdownHandle` with `CancellationToken`
   - Execution workers check cancellation via `tokio::select!`
   - Configurable shutdown timeout
   - Clean termination of background tasks

2. **Connection Management** ✅
   - `ConnectionConfig` for retry/backoff/health settings
   - Automatic reconnection with exponential backoff
   - Connection health tracking (`is_usable()`, `is_idle()`)
   - Idle connection cleanup via `start_health_checks()`
   - Activity tracking for each connection

3. **Configuration Validation** ✅
   - `validate()` methods on all config types
   - Duplicate node ID detection
   - Local node presence check in cluster
   - Timeout, worker, and backoff validation
   - Config errors returned as `AccordError::Config`

4. **Observability** ✅
   - `metrics` module with counters, histograms, and gauges
   - Prometheus metrics via optional `prometheus` feature
   - `tracing_ext` module with span helpers
   - OpenTelemetry support via optional `opentelemetry` feature
   - Metrics recorded for transactions, protocol phases, errors

5. **Backpressure** ✅
   - `ConcurrencyLimiter` with semaphore-based limiting
   - `BackpressureConfig` with max_concurrent and max_queue_depth
   - `Permit` RAII guard for automatic release
   - Queue depth limiting with rejection
   - Acquire timeout for fairness

### Current Status

Phase 10 is complete:
- All production hardening features implemented
- 237 tests pass

### Success Criteria

- [x] Metrics exported to Prometheus (via `prometheus` feature)
- [x] Graceful shutdown completes in-flight work
- [x] Invalid configurations rejected at startup
- [x] Backpressure prevents overload

## Future Improvements

### Short-term (v1.x)

1. **Enhanced Bug Detection**
   - Add tests that specifically catch known distributed bugs (stale reads, lost writes)
   - Implement property-based testing with more edge cases
   - Add chaos testing scenarios (Byzantine faults, clock skew)

2. **Linearizability Validation**
   - Strengthen linearizability checker to validate all operations
   - Add Jepsen-style history analysis
   - Implement Wing & Gong linearizability algorithm

3. **CI Integration**
   - Run simulation tests on every commit
   - Add long-running soak tests in nightly builds
   - Automated regression testing with seed replay

### Long-term (v2.0)

These items are explicitly deferred from v1 (see Non-Goals):

1. **Dynamic Membership**
   - Nodes joining/leaving the cluster
   - Reconfiguration protocol
   - State transfer for new nodes

2. **Sharding/Partitioning**
   - Partition data across disjoint replica sets
   - Cross-shard transactions
   - Automatic rebalancing

3. **Cross-Datacenter Optimizations**
   - Witness replicas for latency reduction
   - Geographic-aware coordinator selection
   - Asynchronous replication modes

4. **Read Replicas / Follower Reads**
   - Read-only replicas for scaling reads
   - Consistent reads from followers
   - Stale read options with bounded staleness

## References

- [ACCORD Paper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf)
- [Apache Cassandra Accord](https://github.com/apache/cassandra-accord)
- [CEP-15: General Purpose Transactions](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15%3A+General+Purpose+Transactions)
