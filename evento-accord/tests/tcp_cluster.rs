//! End-to-end test of the production framed-TCP transport: a 3-node Accord
//! cluster wired over real localhost TCP sockets must replicate writes into one
//! global serial order and resolve a concurrent same-version race to a single
//! winner — exercising the same consensus logic as the in-memory tests, but with
//! messages crossing actual connections and bitcode wire encoding.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    serve, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, Journal, MessageSink,
    Node, NodeConfig, NodeId, StaticTopology, TcpTransport, TxnId,
};
use evento_core::Event;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A cluster whose nodes communicate over real TCP.
struct TcpCluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TcpCluster {
    async fn start(n: u64) -> Self {
        Self::start_with(n, NodeConfig::default(), None).await
    }

    /// [`start`](Self::start) with `config` on every node, and node `joining`
    /// (if any) left joining ([`Node::begin_join`]) until the test joins it.
    async fn start_with(n: u64, config: NodeConfig, joining: Option<usize>) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();

        // Bind every listener first so the address map is known and the kernel
        // accepts connections (into the backlog) before any node sends.
        let mut listeners = Vec::new();
        let mut peers: HashMap<NodeId, std::net::SocketAddr> = HashMap::new();
        for &id in &ids {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap());
            listeners.push((id, listener));
        }

        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut tasks = Vec::new();

        for (id, listener) in listeners {
            let (inbox_tx, inbox_rx) = mpsc::channel(1024);
            tasks.push(serve(listener, inbox_tx));

            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(TcpTransport::new(id, peers.clone()));
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
            if joining == Some(id.0 as usize) {
                node.begin_join();
            }

            tasks.push(node.start(inbox_rx));
            nodes.push(node);
            stores.push(store);
        }

        TcpCluster {
            nodes,
            stores,
            _tasks: tasks,
        }
    }

    /// Waits until every replica has applied at least `expected_len` transactions.
    async fn await_applied(&self, expected_len: usize) {
        for _ in 0..400 {
            if self
                .stores
                .iter()
                .all(|s| s.applied_log().len() >= expected_len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("replicas did not converge to {expected_len} applied entries");
    }

    /// Asserts every replica has the same applied order, returning it.
    fn assert_identical_order(&self, expected_len: usize) -> Vec<(TxnId, bool)> {
        let order: Vec<(TxnId, bool)> = self.stores[0]
            .applied_log()
            .iter()
            .map(|e| (e.txn, e.conflict))
            .collect();
        assert_eq!(order.len(), expected_len);
        for (i, store) in self.stores.iter().enumerate() {
            let this: Vec<(TxnId, bool)> = store
                .applied_log()
                .iter()
                .map(|e| (e.txn, e.conflict))
                .collect();
            assert_eq!(this, order, "replica {i} diverged");
        }
        order
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_over_real_tcp() {
    let cluster = TcpCluster::start(3).await;

    // Non-conflicting writes from rotating coordinators.
    for i in 0..4u32 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            cluster.nodes[(i % 3) as usize].write(vec![event(&format!("acc-{i}"), 1, "Opened")]),
        )
        .await
        .expect("write timed out")
        .expect("write failed over tcp");
        assert!(!outcome.conflict);
    }

    cluster.await_applied(4).await;
    let order = cluster.assert_identical_order(4);
    assert!(order.iter().all(|(_, conflict)| !conflict));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolves_a_conflict_over_real_tcp() {
    let cluster = TcpCluster::start(3).await;

    // Two coordinators race to append version 1 to the same aggregate.
    let n0 = cluster.nodes[0].clone();
    let n1 = cluster.nodes[1].clone();
    let w0 = tokio::spawn(async move { n0.write(vec![event("acc", 1, "A")]).await.unwrap() });
    let w1 = tokio::spawn(async move { n1.write(vec![event("acc", 1, "B")]).await.unwrap() });

    let o0 = tokio::time::timeout(Duration::from_secs(10), w0)
        .await
        .expect("w0 timed out")
        .unwrap();
    let o1 = tokio::time::timeout(Duration::from_secs(10), w1)
        .await
        .expect("w1 timed out")
        .unwrap();

    assert!(
        o0.conflict ^ o1.conflict,
        "exactly one write must conflict over tcp, got {o0:?} {o1:?}"
    );

    cluster.await_applied(2).await;
    let order = cluster.assert_identical_order(2);
    assert_eq!(order.iter().filter(|(_, c)| !c).count(), 1);
}

/// A join from a contact holding more commands than one page carries, over
/// real sockets: the header crosses without them, then the command pages,
/// then the snapshot pages, each through the bitcode wire encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joins_a_command_backlog_page_by_page_over_real_tcp() {
    const WRITES: u16 = 24;
    let config = NodeConfig {
        snapshot_page_bytes: 4 * 1024,
        ..NodeConfig::default()
    };
    // Node 2 joins later: until then it answers nothing, so nothing compacts
    // and node 0 holds every command.
    let cluster = TcpCluster::start_with(3, config, Some(2)).await;
    for version in 1..=WRITES {
        let mut write = event("acc", version, "Bumped");
        write.data = vec![version as u8; 1024];
        tokio::time::timeout(Duration::from_secs(10), cluster.nodes[0].write(vec![write]))
            .await
            .expect("write timed out")
            .expect("write failed over tcp");
    }

    let imported = tokio::time::timeout(Duration::from_secs(30), cluster.nodes[2].join(NodeId(0)))
        .await
        .expect("join timed out")
        .expect("join over tcp");
    assert_eq!(imported, usize::from(WRITES), "every command imported");
    assert!(
        cluster.nodes[0].metrics().command_pages >= 3,
        "the commands crossed in pages: {}",
        cluster.nodes[0].metrics().command_pages
    );
    let committed =
        |i: usize| -> BTreeSet<_> { cluster.stores[i].committed_events().into_iter().collect() };
    assert_eq!(
        committed(2),
        committed(0),
        "the joined store matches the contact's"
    );

    // It takes part: its own write commits everywhere.
    tokio::time::timeout(
        Duration::from_secs(10),
        cluster.nodes[2].write(vec![event("acc", WRITES + 1, "Bumped")]),
    )
    .await
    .expect("write timed out")
    .expect("a write coordinated by the joined node");
    for _ in 0..400 {
        if cluster
            .stores
            .iter()
            .all(|s| s.committed_events().len() == usize::from(WRITES) + 1)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the write after the join did not reach every replica");
}
