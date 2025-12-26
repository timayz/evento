# Phase 6: Testing and Hardening

## Objective

Comprehensive testing to ensure correctness, performance, and reliability of the ACCORD implementation.

## Files to Create

```
evento-accord/
├── tests/
│   ├── unit/
│   │   ├── timestamp_test.rs
│   │   ├── txn_test.rs
│   │   ├── state_test.rs
│   │   └── protocol_test.rs
│   ├── integration/
│   │   ├── single_node_test.rs
│   │   ├── cluster_test.rs
│   │   └── failure_test.rs
│   └── stress/
│       ├── concurrent_writes.rs
│       └── network_partition.rs
├── benches/
│   ├── single_node.rs
│   └── cluster.rs
```

## Tasks

### 6.1 Unit Tests

**Timestamp Tests**
```rust
#[test]
fn test_timestamp_ordering() {
    let t1 = Timestamp::new(100, 0, 1);
    let t2 = Timestamp::new(100, 1, 1);
    let t3 = Timestamp::new(101, 0, 1);

    assert!(t1 < t2);
    assert!(t2 < t3);
}

#[test]
fn test_hlc_monotonic() {
    let clock = HybridClock::new(1);
    let t1 = clock.now();
    let t2 = clock.now();
    assert!(t1 < t2);
}

#[test]
fn test_hlc_update_with_future_time() {
    let clock = HybridClock::new(1);
    let future = Timestamp::new(u64::MAX / 2, 0, 2);
    let updated = clock.update(future);
    assert!(updated > future);
}
```

**State Machine Tests**
```rust
#[tokio::test]
async fn test_preaccept_computes_deps() {
    let state = ConsensusState::new();

    // First transaction
    let txn1 = create_txn("key1", Timestamp::new(100, 0, 1));
    state.preaccept(txn1).await.unwrap();

    // Second transaction with same key -> should have dep on first
    let txn2 = create_txn("key1", Timestamp::new(101, 0, 1));
    let deps = state.preaccept(txn2).await.unwrap();

    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0], txn1.id);
}

#[tokio::test]
async fn test_no_deps_for_different_keys() {
    let state = ConsensusState::new();

    let txn1 = create_txn("key1", Timestamp::new(100, 0, 1));
    state.preaccept(txn1).await.unwrap();

    let txn2 = create_txn("key2", Timestamp::new(101, 0, 1));
    let deps = state.preaccept(txn2).await.unwrap();

    assert!(deps.is_empty());
}
```

**Acceptance Criteria**:
- [ ] All core types have unit tests
- [ ] Edge cases covered
- [ ] 90%+ code coverage on core modules

---

### 6.2 Protocol Tests

**Fast Path Tests**
```rust
#[tokio::test]
async fn test_fast_path_when_no_conflicts() {
    let cluster = TestCluster::new(3);

    let txn = create_txn("key1", cluster.node(0).clock.now());
    let result = cluster.node(0).preaccept(txn).await.unwrap();

    assert!(matches!(result, PreAcceptResult::FastPath { .. }));
}

#[tokio::test]
async fn test_slow_path_when_deps_differ() {
    let cluster = TestCluster::new(3);

    // Pre-populate different deps on different nodes
    cluster.node(1).inject_txn(create_txn("key1", ...));

    let txn = create_txn("key1", cluster.node(0).clock.now());
    let result = cluster.node(0).preaccept(txn).await.unwrap();

    assert!(matches!(result, PreAcceptResult::SlowPath { .. }));
}
```

**Linearizability Tests**
```rust
#[tokio::test]
async fn test_concurrent_writes_linearizable() {
    let cluster = TestCluster::new(3);
    let key = "account-1";

    // Run 100 concurrent increments from different nodes
    let mut handles = vec![];
    for i in 0..100 {
        let node = cluster.node(i % 3);
        handles.push(tokio::spawn(async move {
            node.write(vec![increment_event(key)]).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    // All nodes should see exactly 100 events in consistent order
    for node in cluster.nodes() {
        let events = node.read(key).await.unwrap();
        assert_eq!(events.len(), 100);
    }

    // Order should be identical across all nodes
    let order_0 = cluster.node(0).read(key).await.unwrap();
    let order_1 = cluster.node(1).read(key).await.unwrap();
    let order_2 = cluster.node(2).read(key).await.unwrap();

    assert_eq!(order_0, order_1);
    assert_eq!(order_1, order_2);
}
```

**Acceptance Criteria**:
- [ ] Fast path works correctly
- [ ] Slow path merges deps correctly
- [ ] Commit is durable
- [ ] Execution respects dependencies

---

### 6.3 Failure Tests

**Node Failure**
```rust
#[tokio::test]
async fn test_write_succeeds_with_one_node_down() {
    let cluster = TestCluster::new(3);

    // Kill node 2
    cluster.kill_node(2);

    // Write should succeed (quorum = 2)
    let result = cluster.node(0).write(vec![test_event()]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_write_fails_with_two_nodes_down() {
    let cluster = TestCluster::new(3);

    cluster.kill_node(1);
    cluster.kill_node(2);

    // Write should fail (can't reach quorum)
    let result = cluster.node(0).write(vec![test_event()]).await;
    assert!(matches!(result, Err(AccordError::QuorumNotReached { .. })));
}
```

**Network Partition**
```rust
#[tokio::test]
async fn test_network_partition_and_heal() {
    let cluster = TestCluster::new(5);

    // Partition: {0, 1} vs {2, 3, 4}
    cluster.partition(&[0, 1], &[2, 3, 4]);

    // Writes to majority partition succeed
    let result = cluster.node(2).write(vec![test_event()]).await;
    assert!(result.is_ok());

    // Writes to minority partition fail
    let result = cluster.node(0).write(vec![test_event()]).await;
    assert!(result.is_err());

    // Heal partition
    cluster.heal_partition();

    // Minority catches up
    tokio::time::sleep(Duration::from_secs(1)).await;
    let events_0 = cluster.node(0).read("test").await.unwrap();
    let events_2 = cluster.node(2).read("test").await.unwrap();
    assert_eq!(events_0, events_2);
}
```

**Acceptance Criteria**:
- [ ] Survives f failures with 2f+1 nodes
- [ ] Network partition handled correctly
- [ ] Recovery after partition heals

---

### 6.4 Stress Tests

**High Concurrency**
```rust
#[tokio::test]
async fn stress_1000_concurrent_writes() {
    let cluster = TestCluster::new(3);

    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..1000 {
        let node = cluster.node(i % 3);
        handles.push(tokio::spawn(async move {
            node.write(vec![test_event_with_key(&format!("key-{}", i % 100))]).await
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    let success_count = results.iter().filter(|r| r.is_ok()).count();
    println!("Completed {} writes in {:?}", success_count, elapsed);

    assert!(success_count >= 950); // Allow some failures under stress
}
```

**Acceptance Criteria**:
- [ ] Handles 1000+ concurrent operations
- [ ] No deadlocks under load
- [ ] Memory usage bounded

---

### 6.5 Benchmarks

**File**: `benches/single_node.rs`

```rust
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

fn bench_single_node_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let executor = rt.block_on(setup_single_node());

    let mut group = c.benchmark_group("single_node");
    group.throughput(Throughput::Elements(1));

    group.bench_function("write_1_event", |b| {
        b.iter(|| {
            rt.block_on(async {
                executor.write(vec![test_event()]).await.unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_single_node_write);
criterion_main!(benches);
```

**Performance Targets**:
- Single-node: <1ms p99 write latency
- 3-node fast path: <5ms p99
- 3-node slow path: <10ms p99

---

### 6.6 Documentation

- [ ] README.md with quick start
- [ ] Architecture diagram
- [ ] API documentation (rustdoc)
- [ ] Troubleshooting guide
- [ ] Performance tuning guide

---

## Definition of Done

- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Stress tests pass without crashes
- [ ] Benchmarks meet targets
- [ ] No clippy warnings
- [ ] Documentation complete
- [ ] Ready for alpha release
