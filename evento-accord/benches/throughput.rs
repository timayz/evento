//! Write-path throughput/latency benchmark for the Accord consensus engine over
//! the in-memory transport. Measures the per-write latency of a coordinator
//! driving a conflict-free write to quorum across clusters of 1, 3, and 5 nodes
//! — the baseline for any pipelining/batching work. Run with `cargo bench`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    MessageSink, Node, NodeId, StaticTopology,
};
use evento_core::Event;
use tokio::runtime::Runtime;

/// Builds and starts an `n`-node in-memory cluster, returning the nodes (their
/// inbox loops run on `rt`). No recovery sweep — this isolates the write path.
fn cluster(rt: &Runtime, n: u64) -> Vec<Node> {
    rt.block_on(async {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        let mut nodes = Vec::new();
        for &id in &ids {
            let inbox = net.register(id);
            let node = Node::new(
                id,
                Arc::new(StaticTopology::new(id, ids.clone())),
                Arc::new(HybridLogicalClock::new(id)),
                Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
                Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
                Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
            );
            node.start(inbox);
            nodes.push(node);
        }
        nodes
    })
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::new(),
        aggregate_type: "bench/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

fn bench_write_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_latency");

    for &n in &[1u64, 3, 5] {
        let nodes = cluster(&rt, n);
        let coordinator = nodes[0].clone();
        // A monotonic counter spreads writes over 1000 aggregates with strictly
        // increasing versions, so every write is conflict-free.
        let mut seq: u64 = 0;

        group.bench_function(format!("{n}_nodes"), |b| {
            b.iter(|| {
                let agg = format!("a{}", seq % 1000);
                let version = (seq / 1000) as u16 + 1;
                seq += 1;
                rt.block_on(async {
                    let _ = black_box(coordinator.write(vec![event(&agg, version)]).await);
                });
            });
        });
    }

    group.finish();
}

/// Pipelined throughput: many writers in flight at once (vs the one-at-a-time
/// latency benchmark above), showing how the concurrent-task model overlaps
/// independent transactions. Reports writes/second.
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");
    const CONCURRENCY: usize = 64;

    for &n in &[3u64, 5] {
        let nodes = cluster(&rt, n);
        let seq = Arc::new(AtomicU64::new(0));
        group.throughput(Throughput::Elements(CONCURRENCY as u64));
        group.bench_function(format!("{n}_nodes_x{CONCURRENCY}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(CONCURRENCY);
                    for w in 0..CONCURRENCY {
                        let coordinator = nodes[w % nodes.len()].clone();
                        let seq = Arc::clone(&seq);
                        handles.push(tokio::spawn(async move {
                            // A unique (aggregate, version) per write ⇒ conflict-free.
                            let i = seq.fetch_add(1, Ordering::Relaxed);
                            let agg = format!("a{}", i % 4096);
                            let version = (i / 4096) as u16 + 1;
                            let _ = coordinator.write(vec![event(&agg, version)]).await;
                        }));
                    }
                    for handle in handles {
                        let _ = black_box(handle.await);
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_write_latency, bench_throughput);
criterion_main!(benches);
