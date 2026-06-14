//! M1 acceptance tests: a multi-node Accord cluster over the in-memory
//! transport must replicate writes into one global serial order, and must
//! resolve concurrent same-version writes to exactly one winner — evento's
//! optimistic-concurrency semantics, now strictly serializable across replicas.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AppliedEntry, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeId, StaticTopology, TxnId,
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
            );

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

/// A test event appending `version` to aggregate `aggregator_id`. The routing
/// key is unset, so the aggregate id is the Accord key.
fn event(aggregator_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregator_type: "test/Account".into(),
        aggregator_id: aggregator_id.into(),
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
