//! M3 acceptance tests: a multi-shard cluster (disjoint shards) must route
//! single-aggregate writes to the owning shard, replicate a transaction that
//! spans shards into both, and — crucially — commit such a transaction
//! **atomically**: if any one aggregate's version condition fails, the whole
//! write aborts and no shard appends.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, Journal, Key, MessageSink,
    Node, NodeId, ShardedTopology, Topology,
};
use evento_core::Event;

/// A cluster partitioned into disjoint shards.
struct ShardCluster {
    nodes: HashMap<NodeId, Node>,
    stores: HashMap<NodeId, Arc<InMemoryDataStore>>,
    shards: Vec<Vec<NodeId>>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl ShardCluster {
    fn start(shards: Vec<Vec<u64>>) -> Self {
        let shard_ids: Vec<Vec<NodeId>> = shards
            .iter()
            .map(|s| s.iter().copied().map(NodeId).collect())
            .collect();
        let net = evento_accord::InMemoryNetwork::new();

        let mut nodes = HashMap::new();
        let mut stores = HashMap::new();
        let mut loops = Vec::new();

        for id in shard_ids.iter().flatten().copied() {
            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let store = Arc::new(InMemoryDataStore::new());
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(ShardedTopology::new(id, shard_ids.clone()));
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                journal,
            );
            loops.push(node.start(inbox));
            nodes.insert(id, node);
            stores.insert(id, store);
        }

        ShardCluster {
            nodes,
            stores,
            shards: shard_ids,
            _loops: loops,
        }
    }

    /// A topology instance for computing which shard a key falls in.
    fn probe(&self) -> ShardedTopology {
        ShardedTopology::new(self.shards[0][0], self.shards.clone())
    }

    /// An aggregate id that hashes into `shard`.
    fn id_in_shard(&self, shard: usize, prefix: &str) -> String {
        let probe = self.probe();
        for i in 0..10_000 {
            let id = format!("{prefix}-{i}");
            if probe.shard_of(&Key(id.clone())) == shard {
                return id;
            }
        }
        panic!("no id found for shard {shard}");
    }

    /// Waits until every node in `nodes` has applied at least `len` transactions.
    async fn await_len(&self, nodes: &[NodeId], len: usize) {
        for _ in 0..1000 {
            if nodes
                .iter()
                .all(|n| self.stores[n].applied_log().len() >= len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("nodes {nodes:?} did not reach {len} applied entries");
    }

    fn shard_nodes(&self, shard: usize) -> Vec<NodeId> {
        self.shards[shard].clone()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn routes_single_aggregate_writes_to_their_shard() {
    // Two shards of three nodes each.
    let cluster = ShardCluster::start(vec![vec![0, 1, 2], vec![3, 4, 5]]);
    let a = cluster.id_in_shard(0, "a");
    let b = cluster.id_in_shard(1, "b");

    cluster.nodes[&NodeId(0)]
        .write(vec![event(&a, 1, "Opened")])
        .await
        .unwrap();
    cluster.nodes[&NodeId(3)]
        .write(vec![event(&b, 1, "Opened")])
        .await
        .unwrap();

    cluster.await_len(&cluster.shard_nodes(0), 1).await;
    cluster.await_len(&cluster.shard_nodes(1), 1).await;

    // Each write landed only in its own shard.
    for n in cluster.shard_nodes(0) {
        assert_eq!(cluster.stores[&n].applied_log().len(), 1);
        assert!(!cluster.stores[&n].applied_log()[0].conflict);
    }
    for n in cluster.shard_nodes(1) {
        assert_eq!(cluster.stores[&n].applied_log().len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn commits_a_cross_shard_transaction_atomically() {
    let cluster = ShardCluster::start(vec![vec![0, 1, 2], vec![3, 4, 5]]);
    let a = cluster.id_in_shard(0, "a");
    let b = cluster.id_in_shard(1, "b");

    // Open both aggregates (one per shard).
    cluster.nodes[&NodeId(0)]
        .write(vec![event(&a, 1, "Opened")])
        .await
        .unwrap();
    cluster.nodes[&NodeId(3)]
        .write(vec![event(&b, 1, "Opened")])
        .await
        .unwrap();

    // A single transaction spanning both shards.
    let outcome = cluster.nodes[&NodeId(0)]
        .write(vec![event(&a, 2, "Debited"), event(&b, 2, "Credited")])
        .await
        .unwrap();
    assert!(!outcome.conflict, "both conditions hold, so it must commit");

    cluster.await_len(&cluster.shard_nodes(0), 2).await;
    cluster.await_len(&cluster.shard_nodes(1), 2).await;

    // The spanning transaction is present in both shards, at the same id.
    for shard in 0..2 {
        for n in cluster.shard_nodes(shard) {
            let log = cluster.stores[&n].applied_log();
            assert_eq!(log.len(), 2);
            assert_eq!(log[1].txn, outcome.txn);
            assert!(!log[1].conflict);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn aborts_a_cross_shard_transaction_atomically() {
    let cluster = ShardCluster::start(vec![vec![0, 1, 2], vec![3, 4, 5]]);
    let a = cluster.id_in_shard(0, "a");
    let b = cluster.id_in_shard(1, "b");

    cluster.nodes[&NodeId(0)]
        .write(vec![event(&a, 1, "Opened")])
        .await
        .unwrap();
    cluster.nodes[&NodeId(3)]
        .write(vec![event(&b, 1, "Opened")])
        .await
        .unwrap();

    // Advance `a` to version 2 on its own.
    cluster.nodes[&NodeId(0)]
        .write(vec![event(&a, 2, "Debited")])
        .await
        .unwrap();

    // Cross-shard write reusing a's stale version 2 but a fresh b version 2:
    // a's condition fails, so the WHOLE transaction must abort.
    let outcome = cluster.nodes[&NodeId(1)]
        .write(vec![event(&a, 2, "Debited"), event(&b, 2, "Credited")])
        .await
        .unwrap();
    assert!(
        outcome.conflict,
        "a's stale version must abort the whole write"
    );

    cluster.await_len(&cluster.shard_nodes(1), 2).await;

    // Atomicity: b was NOT advanced by the aborted write, so a fresh write of
    // b version 2 now succeeds.
    let b_again = cluster.nodes[&NodeId(3)]
        .write(vec![event(&b, 2, "Credited")])
        .await
        .unwrap();
    assert!(
        !b_again.conflict,
        "b must be untouched by the aborted cross-shard write"
    );
}
