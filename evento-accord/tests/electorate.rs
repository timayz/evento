//! Phase D — **fast-path electorate** acceptance tests.
//!
//! A per-shard, cluster-agreed *electorate* (a subset of the replicas) decides
//! the one-round-trip fast path. Placed in one region, it lets a co-located
//! coordinator commit in a single **local** round-trip — without waiting for
//! remote replicas — while preserving strict serializability and `f`-fault
//! tolerance. These tests use the in-memory transport's **latency model** (a
//! per-link delay; off by default) to demonstrate and validate that.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AppliedEntry, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeId, StaticTopology,
};
use evento_core::Event;

/// One-way latency between two nodes in the same region (a local hop).
const INTRA: Duration = Duration::from_millis(1);
/// One-way latency between regions (a cross-region hop) — two orders of
/// magnitude above [`INTRA`] and well above the default `fast_timeout` (50 ms).
const CROSS: Duration = Duration::from_millis(200);

/// A running in-process cluster for the electorate tests.
struct Cluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    net: Arc<InMemoryNetwork>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    /// Builds and starts an `n`-node single-shard cluster. `electorate` (if set)
    /// is the shared fast-path electorate; `None` uses the default (all nodes).
    fn start(n: u64, electorate: Option<Vec<NodeId>>) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();

        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut loops = Vec::new();

        for &id in &ids {
            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let store = Arc::new(InMemoryDataStore::new());
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let mut topology = StaticTopology::new(id, ids.clone());
            if let Some(e) = &electorate {
                topology = topology.with_fast_electorate(e.clone());
            }
            let node = Node::new(
                id,
                Arc::new(topology),
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                journal,
            );
            loops.push(node.start(inbox));
            loops.push(node.start_recovery());
            nodes.push(node);
            stores.push(store);
        }

        Cluster {
            nodes,
            stores,
            net,
            _loops: loops,
        }
    }

    /// Splits the nodes into two regions and installs the latency matrix: links
    /// within a region cost [`INTRA`], links across cost [`CROSS`].
    fn set_two_region_latency(&self, region_a: &[u64], region_b: &[u64]) {
        for &a in region_a {
            for &b in region_b {
                self.net.set_link_latency(NodeId(a), NodeId(b), CROSS);
            }
        }
        for region in [region_a, region_b] {
            for (i, &x) in region.iter().enumerate() {
                for &y in &region[i + 1..] {
                    self.net.set_link_latency(NodeId(x), NodeId(y), INTRA);
                }
            }
        }
    }

    /// Waits until each listed replica has applied at least `expected_len`
    /// transactions.
    async fn await_applied(&self, indices: &[usize], expected_len: usize) {
        for _ in 0..2000 {
            if indices
                .iter()
                .all(|&i| self.stores[i].applied_log().len() >= expected_len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("cluster did not converge to {expected_len} applied transactions");
    }
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn electorate_commits_in_one_local_round_trip() {
    // N=5 (f=2). Electorate {0,1,2} all in region A; {3,4} in region B, a CROSS
    // hop away. A coordinator in region A reaches its electorate's fast quorum
    // (3) over local links only, so it commits on the fast path far sooner than a
    // region-B reply could even arrive.
    let cluster = Cluster::start(5, Some(vec![NodeId(0), NodeId(1), NodeId(2)]));
    cluster.set_two_region_latency(&[0, 1, 2], &[3, 4]);

    let start = tokio::time::Instant::now();
    let outcome = cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("write commits");
    let elapsed = start.elapsed();

    assert!(!outcome.conflict, "the lone write takes effect");
    // It took the one-round-trip fast path, decided by the local electorate.
    assert_eq!(cluster.nodes[0].metrics().fast_path, 1);
    assert_eq!(cluster.nodes[0].metrics().slow_path, 0);
    // And it finished in a handful of local round-trips — well before a CROSS
    // (200 ms) reply could land, and below the 50 ms fast-path timeout.
    assert!(
        elapsed < Duration::from_millis(50),
        "commit took {elapsed:?}; expected a few local round-trips, not a cross-region wait"
    );

    // The region-B replicas still converge (the write replicates to all).
    cluster.await_applied(&[0, 1, 2, 3, 4], 1).await;
}

#[tokio::test(start_paused = true)]
async fn without_an_electorate_a_geo_write_takes_the_slow_path() {
    // Same geography, but the default electorate (all 5 nodes) needs a fast quorum
    // of 4 — unreachable without a region-B vote. The coordinator waits out the
    // fast-path timeout, then falls back to the slow (Accept) path.
    let cluster = Cluster::start(5, None);
    cluster.set_two_region_latency(&[0, 1, 2], &[3, 4]);

    let start = tokio::time::Instant::now();
    let outcome = cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("write commits");
    let elapsed = start.elapsed();

    assert!(!outcome.conflict);
    assert_eq!(
        cluster.nodes[0].metrics().fast_path,
        0,
        "no local fast quorum"
    );
    assert_eq!(
        cluster.nodes[0].metrics().slow_path,
        1,
        "fell back to slow path"
    );
    assert!(
        elapsed >= Duration::from_millis(50),
        "the coordinator waited out the fast-path timeout (got {elapsed:?})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_requires_a_quorum_under_a_shrunk_electorate() {
    // N=5, electorate {0,1,2}. The recovery decision must rest on a slow quorum
    // (f+1=3): a recoverer that hears fewer must REFUSE to decide, rather than
    // (mis)decide on a sub-quorum sample — e.g. raise t0 while the txn already
    // fast-committed at t0 on the electorate. This is the new recovery-quorum gate.
    let cluster = Cluster::start(5, Some(vec![NodeId(0), NodeId(1), NodeId(2)]));

    let txn = cluster.nodes[0]
        .coordinate_preaccept(vec![event("acc", 1, "Opened")])
        .await
        .expect("preaccept reached a quorum");

    // The coordinator is gone, and recoverer node 3 is stranded with node 4 only:
    // it can hear itself + node 4 = 2 RecoverOk < slow quorum 3.
    cluster.net.crash(NodeId(0));
    for &peer in &[1u64, 2] {
        cluster.net.partition(NodeId(3), NodeId(peer));
        cluster.net.partition(NodeId(4), NodeId(peer));
    }

    let stranded = cluster.nodes[3].recover(txn).await;
    assert!(
        stranded.is_err(),
        "recovery must refuse to decide without a quorum"
    );

    // Heal: node 3 can now reach {1,2,3,4} — a quorum — and recovers safely.
    cluster.net.heal_all_partitions();
    let outcome = tokio::time::timeout(Duration::from_secs(5), cluster.nodes[3].recover(txn))
        .await
        .expect("recovery timed out")
        .expect("recovery completes once a quorum is reachable");
    assert_eq!(outcome.txn, txn);

    // The survivors applied exactly the recovered transaction, in agreement.
    cluster.await_applied(&[1, 2, 3, 4], 1).await;
    let logs: Vec<Vec<AppliedEntry>> = [1, 2, 3, 4]
        .iter()
        .map(|&i| cluster.stores[i].applied_log())
        .collect();
    for log in &logs {
        assert_eq!(log[0].txn, txn);
        assert_eq!(log, &logs[0], "survivors agree on the global order");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_under_an_electorate_is_idempotent() {
    // Two independent recoveries of the same preaccepted txn under a shrunk
    // electorate must agree on one outcome and apply it exactly once.
    let cluster = Cluster::start(5, Some(vec![NodeId(0), NodeId(1), NodeId(2)]));

    let txn = cluster.nodes[0]
        .coordinate_preaccept(vec![event("acc", 7, "Opened")])
        .await
        .expect("preaccept reached a quorum");

    let first = cluster.nodes[1].recover(txn).await.expect("first recovery");
    let second = cluster.nodes[2]
        .recover(txn)
        .await
        .expect("second recovery");
    assert_eq!(first.txn, second.txn);
    assert_eq!(
        first.conflict, second.conflict,
        "both recoveries report the same outcome"
    );

    cluster.await_applied(&[0, 1, 2, 3, 4], 1).await;
    let logs: Vec<Vec<AppliedEntry>> = (0..5).map(|i| cluster.stores[i].applied_log()).collect();
    for log in &logs {
        assert_eq!(log.len(), 1, "applied exactly once");
        assert_eq!(log, &logs[0], "every replica agrees on the order");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn electorate_cluster_stays_safe_under_minority_churn() {
    // A mini safety oracle: with a shrunk electorate {0,1,2}, concurrent writes
    // keep committing and every replica converges to ONE global order while a
    // minority {3,4} churns. (The full deterministic suite covers the rest.)
    let cluster = Cluster::start(5, Some(vec![NodeId(0), NodeId(1), NodeId(2)]));

    // Churn the minority while writes are in flight.
    let net = Arc::clone(&cluster.net);
    let chaos = tokio::spawn(async move {
        for round in 0..6u64 {
            tokio::time::sleep(Duration::from_millis(8)).await;
            let node = NodeId(3 + round % 2);
            net.crash(node);
            tokio::time::sleep(Duration::from_millis(8)).await;
            net.heal(node);
        }
    });

    // 30 sequential writes across 3 aggregates from a region-A coordinator.
    let coord = cluster.nodes[0].clone();
    for i in 0..30u64 {
        let agg = format!("a{}", i % 3);
        let version = (i / 3 + 1) as u16;
        let _ = coord.write(vec![event(&agg, version, "Ev")]).await;
    }
    let _ = chaos.await;
    cluster.net.heal_all_partitions();
    for n in 3..5 {
        cluster.net.heal(NodeId(n));
    }

    // Every replica converges to the same applied prefix, in identical order.
    let target = cluster.stores[0].applied_log().len();
    cluster.await_applied(&[0, 1, 2, 3, 4], target).await;
    let logs: Vec<Vec<AppliedEntry>> = (0..5).map(|i| cluster.stores[i].applied_log()).collect();
    for log in &logs {
        assert_eq!(
            log[..target],
            logs[0][..target],
            "all replicas agree on the global serial order"
        );
    }
}
