//! M1 acceptance tests: a multi-node Accord cluster over the in-memory
//! transport must replicate writes into one global serial order, and must
//! resolve concurrent same-version writes to exactly one winner — evento's
//! optimistic-concurrency semantics, now strictly serializable across replicas.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AppliedEntry, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, StaticTopology, Status, TxnId,
};
use evento_core::Event;

/// A running in-process cluster: the nodes, a handle to each node's applied
/// store for assertions, the shared network (for fault injection), and the
/// inbox-loop join handles.
struct Cluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    net: Arc<InMemoryNetwork>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    /// Builds and starts an `n`-node single-shard cluster.
    fn start(n: u64) -> Self {
        Self::start_with(n, NodeConfig::default())
    }

    /// Like [`start`](Self::start), every node with `config`.
    fn start_with(n: u64, config: NodeConfig) -> Self {
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
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                journal,
            )
            .with_config(config);

            loops.push(node.start(inbox));
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

    /// The applied log of each replica.
    fn logs(&self) -> Vec<Vec<AppliedEntry>> {
        self.stores.iter().map(|s| s.applied_log()).collect()
    }

    /// Waits until each listed replica has applied at least `expected_len`
    /// transactions. Quorum-based commit means a write returns before every
    /// replica has caught up; convergence is eventual, so tests wait for it.
    async fn await_applied(&self, indices: &[usize], expected_len: usize) {
        for _ in 0..1000 {
            if indices
                .iter()
                .all(|&i| self.stores[i].applied_log().len() >= expected_len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("replicas {indices:?} did not converge to {expected_len} applied entries");
    }

    /// Asserts the given replicas applied the same transactions in the same
    /// order with the same outcomes, returning that shared order. Used when some
    /// nodes are crashed and lag the survivors. Waits for convergence first.
    async fn assert_order_among(
        &self,
        indices: &[usize],
        expected_len: usize,
    ) -> Vec<(TxnId, bool)> {
        self.await_applied(indices, expected_len).await;
        let order: Vec<(TxnId, bool)> = self.stores[indices[0]]
            .applied_log()
            .iter()
            .map(|e| (e.txn, e.conflict))
            .collect();
        assert_eq!(order.len(), expected_len, "unexpected applied count");
        for &i in indices {
            let this: Vec<(TxnId, bool)> = self.stores[i]
                .applied_log()
                .iter()
                .map(|e| (e.txn, e.conflict))
                .collect();
            assert_eq!(this, order, "replica {i} diverged from the global order");
        }
        order
    }

    /// Asserts every replica applied the same transactions, in the same order,
    /// with the same conflict outcomes — strict serializability — and returns
    /// that shared order. Waits for convergence first.
    async fn assert_identical_order(&self, expected_len: usize) -> Vec<(TxnId, bool)> {
        let all: Vec<usize> = (0..self.stores.len()).collect();
        self.await_applied(&all, expected_len).await;
        let logs = self.logs();
        let order: Vec<(TxnId, bool)> = logs[0].iter().map(|e| (e.txn, e.conflict)).collect();
        assert_eq!(order.len(), expected_len, "unexpected applied count");
        for (i, log) in logs.iter().enumerate() {
            let this: Vec<(TxnId, bool)> = log.iter().map(|e| (e.txn, e.conflict)).collect();
            assert_eq!(this, order, "replica {i} diverged from the global order");
            // Execution timestamps must be strictly increasing within a replica.
            for w in log.windows(2) {
                assert!(
                    w[0].execute_at < w[1].execute_at,
                    "replica {i} applied out of timestamp order"
                );
            }
        }
        order
    }
}

/// A test event appending `version` to aggregate `aggregate_id`. The routing
/// key is unset, so the aggregate id is the Accord key.
fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_sequential_writes_in_one_global_order() {
    let cluster = Cluster::start(3);

    // Five non-conflicting writes, each issued from a rotating coordinator.
    for i in 0..5u32 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.nodes[(i % 3) as usize].write(vec![event(&format!("acc-{i}"), 1, "Opened")]),
        )
        .await
        .expect("write timed out")
        .expect("write failed");
        assert!(!outcome.conflict, "non-conflicting write must succeed");
    }

    let order = cluster.assert_identical_order(5).await;
    assert!(
        order.iter().all(|(_, conflict)| !conflict),
        "no write should conflict"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_version_writes_resolve_to_one_winner() {
    let cluster = Cluster::start(3);

    // Two writes race to append version 1 to the same aggregate from different
    // coordinators. Exactly one must win; the other gets a version conflict.
    let n0 = cluster.nodes[0].clone();
    let n1 = cluster.nodes[1].clone();
    let w0 = tokio::spawn(async move { n0.write(vec![event("acc", 1, "A")]).await.unwrap() });
    let w1 = tokio::spawn(async move { n1.write(vec![event("acc", 1, "B")]).await.unwrap() });

    let o0 = tokio::time::timeout(Duration::from_secs(5), w0)
        .await
        .expect("w0 timed out")
        .unwrap();
    let o1 = tokio::time::timeout(Duration::from_secs(5), w1)
        .await
        .expect("w1 timed out")
        .unwrap();

    assert!(
        o0.conflict ^ o1.conflict,
        "exactly one write must conflict, got {o0:?} and {o1:?}"
    );

    let order = cluster.assert_identical_order(2).await;
    let successes = order.iter().filter(|(_, conflict)| !conflict).count();
    assert_eq!(successes, 1, "exactly one append must take effect");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_cluster_orders_a_conflicting_burst() {
    let cluster = Cluster::start(5);

    // A burst of concurrent writes to the SAME aggregate at the SAME version:
    // exactly one can win, the rest must conflict, and all replicas must agree.
    let mut handles = Vec::new();
    for i in 0..5u32 {
        let node = cluster.nodes[i as usize].clone();
        handles.push(tokio::spawn(async move {
            node.write(vec![event("shared", 1, "Try")]).await.unwrap()
        }));
    }

    let mut conflicts = 0;
    for h in handles {
        let outcome = tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("write timed out")
            .unwrap();
        if outcome.conflict {
            conflicts += 1;
        }
    }
    assert_eq!(conflicts, 4, "four of five racing writes must conflict");

    let order = cluster.assert_identical_order(5).await;
    assert_eq!(
        order.iter().filter(|(_, c)| !c).count(),
        1,
        "exactly one append takes effect"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tolerates_a_down_node_via_quorum() {
    // 3 nodes, f = 1. Crash node 2; writes coordinated by the survivors must
    // still commit on a slow quorum, and the survivors stay consistent.
    let cluster = Cluster::start(3);
    cluster.net.crash(NodeId(2));

    for i in 0..4u32 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.nodes[(i % 2) as usize].write(vec![event(&format!("acc-{i}"), 1, "Opened")]),
        )
        .await
        .expect("write timed out")
        .expect("write should succeed on a quorum despite the down node");
        assert!(!outcome.conflict);
    }

    // The two live replicas agree; the crashed node is allowed to lag.
    let order = cluster.assert_order_among(&[0, 1], 4).await;
    assert!(order.iter().all(|(_, conflict)| !conflict));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_a_transaction_whose_coordinator_crashed() {
    // Node 0 preaccepts a write, then "crashes" before committing. Node 1
    // recovers the transaction and drives it to completion; the survivors apply
    // it consistently.
    let cluster = Cluster::start(3);

    let txn = cluster.nodes[0]
        .coordinate_preaccept(vec![event("acc", 1, "Opened")])
        .await
        .expect("preaccept reached a quorum");

    // The coordinator is now gone.
    cluster.net.crash(NodeId(0));

    let outcome = tokio::time::timeout(Duration::from_secs(5), cluster.nodes[1].recover(txn))
        .await
        .expect("recovery timed out")
        .expect("recovery should complete the transaction");
    assert_eq!(outcome.txn, txn);
    assert!(!outcome.conflict, "the lone write must take effect");

    // Both survivors applied exactly the recovered transaction, in agreement.
    let order = cluster.assert_order_among(&[1, 2], 1).await;
    assert_eq!(order[0].0, txn);
    assert!(!order[0].1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_a_transaction_whose_coordinator_crashed_after_commit() {
    // Node 0 commits a write, then "crashes" before it collects a single read
    // or sends the decision. Every replica reads, reports to a coordinator that
    // is no longer listening, and holds its slot. A recovery must still get
    // those reads: the replicas never read twice.
    let config = NodeConfig {
        collect_timeout: Duration::from_millis(1000),
        ..NodeConfig::default()
    };
    let cluster = Cluster::start_with(3, config);

    let txn = cluster.nodes[0]
        .coordinate_through_commit(vec![event("acc", 1, "Opened")])
        .await
        .expect("committed on a quorum");
    // Wait until the survivors have read it (and so will not read it again).
    for _ in 0..1000 {
        if [1, 2]
            .iter()
            .all(|&i| cluster.nodes[i].command_status(txn) == Some(Status::Reading))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for i in [1, 2] {
        assert_eq!(cluster.nodes[i].command_status(txn), Some(Status::Reading));
    }
    cluster.net.crash(NodeId(0));

    let outcome = tokio::time::timeout(Duration::from_secs(5), cluster.nodes[1].recover(txn))
        .await
        .expect("recovery timed out")
        .expect("recovery must decide a transaction every replica already read");
    assert_eq!(outcome.txn, txn);
    assert!(!outcome.conflict, "the lone write must take effect");

    let order = cluster.assert_order_among(&[1, 2], 1).await;
    assert_eq!(order[0], (txn, false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_is_idempotent_with_a_late_original_commit() {
    // A transaction is recovered by node 1; then node 2 also recovers it. The
    // second recovery must agree on the same outcome, not double-apply.
    let cluster = Cluster::start(3);

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
        "both recoveries must report the same outcome"
    );

    // The transaction was applied exactly once on every replica.
    let order = cluster.assert_identical_order(1).await;
    assert_eq!(order[0].0, txn);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_another_node_recovered_reports_its_outcome_not_a_failure() {
    // Node 0's peers answer it only after its collect timeout, so its own
    // coordination fails. Meanwhile they take the transaction for stalled and
    // recover it, committing the write. Node 0 must report that commit, not
    // the quorum it missed: the caller would otherwise retry a write that
    // already took effect.
    let config = NodeConfig {
        collect_timeout: Duration::from_millis(1000),
        recovery_timeout: Duration::from_millis(100),
        ..NodeConfig::default()
    };
    let cluster = Cluster::start_with(3, config);
    cluster
        .net
        .set_latency(NodeId(1), NodeId(0), Duration::from_millis(1500));
    cluster
        .net
        .set_latency(NodeId(2), NodeId(0), Duration::from_millis(1500));
    let _sweeps = [
        cluster.nodes[1].start_recovery(),
        cluster.nodes[2].start_recovery(),
    ];

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        cluster.nodes[0].write(vec![event("acc", 1, "Opened")]),
    )
    .await
    .expect("the write settles")
    .expect("a write another node committed is reported as committed");
    assert!(!outcome.conflict, "the lone write must take effect");

    // Applied once, everywhere, as the transaction node 0 reported.
    let order = cluster.assert_identical_order(1).await;
    assert_eq!(order[0].0, outcome.txn);
    assert!(!order[0].1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_no_quorum_ever_saw_still_fails() {
    // Settling a failed write must not turn a real failure into a success:
    // with both peers down nobody can decide the transaction, and the write
    // reports the quorum it missed, in bounded time.
    let config = NodeConfig {
        collect_timeout: Duration::from_millis(300),
        ..NodeConfig::default()
    };
    let cluster = Cluster::start_with(3, config);
    cluster.net.crash(NodeId(1));
    cluster.net.crash(NodeId(2));

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.nodes[0].write(vec![event("acc", 1, "Opened")]),
    )
    .await
    .expect("the write gives up")
    .expect_err("no quorum, no write");
    assert!(err.to_string().contains("quorum not reached"), "{err}");
    assert!(cluster.stores[0].applied_log().is_empty());
}
