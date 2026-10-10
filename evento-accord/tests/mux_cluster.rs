//! End-to-end test of the **multiplexed** framed-TCP transport: three hosts, each
//! one `MuxTransport` + one `SweepScheduler` + one `GroupHost`, carry several
//! independent consensus groups over a single connection set. Proves per-group
//! isolation (a group's writes never land in another group's store), per-group
//! conflict resolution, the pending-register buffer (frames for a group that
//! registers a moment later are replayed, preserving the fast path), lazy re-open
//! with anti-entropy catch-up, and interop with a legacy single-group node.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    serve, DataStore, GroupHost, GroupId, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    Journal, Message, MessageSink, MuxTransport, Node, NodeConfig, NodeId, StaticTopology,
    SweepConfig, SweepScheduler, TcpTransport, Timestamp, Topology, TxnId,
};
use evento_core::Event;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// `n` multiplexed hosts over real localhost TCP, with no groups open yet.
struct MuxCluster {
    ids: Vec<NodeId>,
    peers: HashMap<NodeId, SocketAddr>,
    hosts: Vec<Arc<GroupHost>>,
    /// Groups each host's unrouted handler was told about.
    unrouted: Vec<Arc<Mutex<Vec<GroupId>>>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// One group's per-host stores and journals (kept across close/re-open).
struct Group {
    id: GroupId,
    stores: Vec<Arc<InMemoryDataStore>>,
    journals: Vec<Arc<InMemoryJournal>>,
}

impl Group {
    fn new(id: GroupId, n: usize) -> Self {
        Self {
            id,
            stores: (0..n).map(|_| Arc::new(InMemoryDataStore::new())).collect(),
            journals: (0..n).map(|_| Arc::new(InMemoryJournal::new())).collect(),
        }
    }

    /// Waits until the given hosts' replicas have applied `expected_len` entries.
    async fn await_applied_on(&self, hosts: &[usize], expected_len: usize) {
        for _ in 0..600 {
            if hosts
                .iter()
                .all(|&h| self.stores[h].applied_log().len() >= expected_len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "group {:?} did not converge to {expected_len} on hosts {hosts:?}: {:?}",
            self.id,
            self.stores
                .iter()
                .map(|s| s.applied_log().len())
                .collect::<Vec<_>>()
        );
    }

    async fn await_applied(&self, expected_len: usize) {
        let all: Vec<usize> = (0..self.stores.len()).collect();
        self.await_applied_on(&all, expected_len).await;
    }

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
            assert_eq!(this, order, "replica {i} of {:?} diverged", self.id);
        }
        order
    }
}

impl MuxCluster {
    async fn start(n: u64, pending_ttl: Duration) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();

        let mut listeners = Vec::new();
        let mut peers: HashMap<NodeId, SocketAddr> = HashMap::new();
        for &id in &ids {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap());
            listeners.push((id, listener));
        }

        let mut hosts = Vec::new();
        let mut unrouted = Vec::new();
        let mut tasks = Vec::new();
        for (id, listener) in listeners {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let hook = Arc::clone(&seen);
            let mux = Arc::new(
                MuxTransport::new(id, peers.clone())
                    .with_pending_buffer(64, 64, pending_ttl)
                    .with_unrouted_handler(move |g| hook.lock().unwrap().push(g)),
            );
            tasks.push(mux.serve(listener));
            let scheduler = SweepScheduler::new(SweepConfig {
                active_hold: Duration::from_secs(1),
                idle_interval: Duration::from_secs(1),
                ..SweepConfig::default()
            });
            tasks.push(scheduler.start());
            hosts.push(Arc::new(GroupHost::new(mux, scheduler)));
            unrouted.push(seen);
        }

        MuxCluster {
            ids,
            peers,
            hosts,
            unrouted,
            _tasks: tasks,
        }
    }

    fn topology(&self, host: usize) -> Arc<dyn Topology> {
        Arc::new(StaticTopology::new(self.ids[host], self.ids.clone()))
    }

    /// Opens `group` on `host` with the given node config.
    async fn open_with(&self, group: &Group, host: usize, config: NodeConfig) -> Node {
        self.hosts[host]
            .open(
                group.id,
                self.topology(host),
                Arc::clone(&group.stores[host]) as Arc<dyn DataStore>,
                Arc::clone(&group.journals[host]) as Arc<dyn Journal>,
                config,
            )
            .await
            .expect("open group")
    }

    async fn open(&self, group: &Group, host: usize) -> Node {
        self.open_with(group, host, NodeConfig::default()).await
    }

    /// Opens `group` on every host, returning the nodes by host.
    async fn open_everywhere(&self, group: &Group) -> Vec<Node> {
        let mut nodes = Vec::new();
        for h in 0..self.hosts.len() {
            nodes.push(self.open(group, h).await);
        }
        nodes
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

async fn write(node: &Node, events: Vec<Event>) -> evento_accord::CommitOutcome {
    tokio::time::timeout(Duration::from_secs(10), node.write(events))
        .await
        .expect("write timed out")
        .expect("write failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_replicate_independently_over_one_connection_set() {
    let cluster = MuxCluster::start(3, Duration::from_secs(5)).await;
    let a = Group::new(GroupId(1), 3);
    let b = Group::new(GroupId(2), 3);
    let a_nodes = cluster.open_everywhere(&a).await;
    let b_nodes = cluster.open_everywhere(&b).await;

    // Writes in A from rotating coordinators; B stays untouched.
    for i in 0..4u32 {
        let outcome = write(
            &a_nodes[(i % 3) as usize],
            vec![event(&format!("acc-{i}"), 1, "Opened")],
        )
        .await;
        assert!(!outcome.conflict);
    }
    a.await_applied(4).await;
    a.assert_identical_order(4);
    assert!(b.stores.iter().all(|s| s.applied_log().is_empty()));

    // The same aggregate id in B is a different aggregate: version 1 is free.
    let outcome = write(&b_nodes[1], vec![event("acc-0", 1, "Opened")]).await;
    assert!(!outcome.conflict);
    b.await_applied(1).await;
    b.assert_identical_order(1);
    a.assert_identical_order(4);

    // Nothing was dropped for want of a group.
    for host in &cluster.hosts {
        assert_eq!(host.transport().metrics().messages_unrouted, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicts_resolve_per_group() {
    let cluster = MuxCluster::start(3, Duration::from_secs(5)).await;
    let a = Group::new(GroupId(10), 3);
    let b = Group::new(GroupId(20), 3);
    let a_nodes = cluster.open_everywhere(&a).await;
    let b_nodes = cluster.open_everywhere(&b).await;

    // Two coordinators race on version 1 of the same aggregate — in both groups
    // at once. Each group must produce exactly one winner.
    let (a0, a1) = (a_nodes[0].clone(), a_nodes[1].clone());
    let (b1, b2) = (b_nodes[1].clone(), b_nodes[2].clone());
    let wa0 = tokio::spawn(async move { a0.write(vec![event("acc", 1, "A")]).await.unwrap() });
    let wa1 = tokio::spawn(async move { a1.write(vec![event("acc", 1, "B")]).await.unwrap() });
    let wb1 = tokio::spawn(async move { b1.write(vec![event("acc", 1, "C")]).await.unwrap() });
    let wb2 = tokio::spawn(async move { b2.write(vec![event("acc", 1, "D")]).await.unwrap() });
    let outcomes = tokio::time::timeout(Duration::from_secs(10), async {
        (
            wa0.await.unwrap(),
            wa1.await.unwrap(),
            wb1.await.unwrap(),
            wb2.await.unwrap(),
        )
    })
    .await
    .expect("writes timed out");
    assert!(
        outcomes.0.conflict ^ outcomes.1.conflict,
        "group A: {outcomes:?}"
    );
    assert!(
        outcomes.2.conflict ^ outcomes.3.conflict,
        "group B: {outcomes:?}"
    );

    a.await_applied(2).await;
    b.await_applied(2).await;
    assert_eq!(
        a.assert_identical_order(2)
            .iter()
            .filter(|(_, c)| !c)
            .count(),
        1
    );
    assert_eq!(
        b.assert_identical_order(2)
            .iter()
            .filter(|(_, c)| !c)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_parked_before_register_are_replayed_and_keep_the_fast_path() {
    let cluster = MuxCluster::start(3, Duration::from_secs(5)).await;
    let g = Group::new(GroupId(30), 3);
    // A generous fast-path timeout so the test is about the buffer, not timing —
    // and a recovery timeout above it, or the peers would "recover" the healthy
    // in-flight write while it waits for the third vote.
    let config = NodeConfig {
        fast_timeout: Duration::from_secs(3),
        recovery_timeout: Duration::from_secs(10),
        ..NodeConfig::default()
    };
    let n0 = cluster.open_with(&g, 0, config).await;
    let _n1 = cluster.open_with(&g, 1, config).await;
    // Host 2 has not opened the group yet (a tenant created a moment ago).

    let writer = {
        let n0 = n0.clone();
        tokio::spawn(async move { n0.write(vec![event("acc", 1, "Opened")]).await.unwrap() })
    };
    // The PreAccept reaches host 2 while unregistered → parked, handler told.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cluster.unrouted[2].lock().unwrap().as_slice(), &[g.id]);
    assert!(writer.is_finished() == false, "fast quorum needs host 2");

    // Opening the group replays the parked frames; the fast path completes.
    let _n2 = cluster.open_with(&g, 2, config).await;
    let outcome = tokio::time::timeout(Duration::from_secs(10), writer)
        .await
        .expect("write timed out")
        .unwrap();
    assert!(!outcome.conflict);
    let m = n0.metrics();
    assert_eq!((m.fast_path, m.slow_path), (1, 0), "fast path preserved");

    g.await_applied(1).await;
    g.assert_identical_order(1);
    assert_eq!(cluster.hosts[2].transport().metrics().messages_unrouted, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_group_keeps_committing_elsewhere_and_catches_up_on_reopen() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(40), 3);
    let nodes = cluster.open_everywhere(&g).await;
    assert!(
        !write(&nodes[0], vec![event("acc-0", 1, "Opened")])
            .await
            .conflict
    );
    g.await_applied(1).await;

    // Host 2 evicts the group. Writes still commit on the remaining quorum.
    assert!(cluster.hosts[2].close(g.id).is_some());
    assert!(!cluster.hosts[2].is_open(g.id));
    assert!(
        !write(&nodes[1], vec![event("acc-1", 1, "Opened")])
            .await
            .conflict
    );
    g.await_applied_on(&[0, 1], 2).await;
    assert_eq!(
        g.stores[2].applied_log().len(),
        1,
        "closed host saw nothing"
    );
    assert!(cluster.unrouted[2].lock().unwrap().contains(&g.id));

    // Re-open on host 2 with its kept journal: `recover_state` rebuilds the store
    // from the journal (the in-memory store models a non-durable backend, so — as
    // in the restart tests — it is a fresh instance), then anti-entropy, driven by
    // the shared scheduler, pulls the write it missed while closed.
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    cluster.open(&g, 2).await;
    g.await_applied(2).await;
    g.assert_identical_order(2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_for_an_unknown_group_are_parked_then_counted_when_they_expire() {
    let cluster = MuxCluster::start(2, Duration::from_millis(100)).await;
    let stray = GroupId(99);
    let sink = cluster.hosts[0].transport().sink(stray);
    let msg = Message::Applied {
        txn: TxnId(Timestamp {
            micros: 1,
            logical: 0,
            node: NodeId(0),
        }),
    };
    sink.send(NodeId(1), msg.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Parked (not yet counted) and the handler was told once.
    assert_eq!(cluster.unrouted[1].lock().unwrap().as_slice(), &[stray]);
    assert_eq!(cluster.hosts[1].transport().metrics().messages_unrouted, 0);
    // Registering after the TTL finds only an expired frame: dropped and counted.
    let mut inbox = cluster.hosts[1].transport().register(stray);
    assert!(inbox.try_recv().is_err());
    assert_eq!(cluster.hosts[1].transport().metrics().messages_unrouted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_legacy_single_group_node_interoperates_in_the_default_group() {
    // Hosts 0 and 1 are multiplexed (their `GroupId::DEFAULT` group speaks grouped
    // frames); node 2 is a classic `TcpTransport` + `serve` node speaking un-grouped
    // frames. A mux listener delivers un-grouped frames to DEFAULT, and a legacy
    // listener accepts grouped frames addressed to DEFAULT, so the three form one
    // cluster.
    let n = 3u64;
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let mut listeners = Vec::new();
    let mut peers: HashMap<NodeId, SocketAddr> = HashMap::new();
    for &id in &ids {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(id, listener.local_addr().unwrap());
        listeners.push((id, listener));
    }
    let mut tasks = Vec::new();
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    for (id, listener) in listeners {
        let store = Arc::new(InMemoryDataStore::new());
        let topology: Arc<dyn Topology> = Arc::new(StaticTopology::new(id, ids.clone()));
        let clock = Arc::new(HybridLogicalClock::new(id));
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let (sink, inbox): (Arc<dyn MessageSink>, mpsc::Receiver<_>) = if id.0 < 2 {
            let mux = Arc::new(MuxTransport::new(id, peers.clone()));
            tasks.push(mux.serve(listener));
            let inbox = mux.register(GroupId::DEFAULT);
            (Arc::new(mux.sink(GroupId::DEFAULT)), inbox)
        } else {
            let (tx, rx) = mpsc::channel(1024);
            tasks.push(serve(listener, tx));
            (Arc::new(TcpTransport::new(id, peers.clone())), rx)
        };
        let node = Node::new(
            id,
            topology,
            clock,
            sink,
            Arc::clone(&store) as Arc<dyn DataStore>,
            journal,
        );
        tasks.push(node.start(inbox));
        nodes.push(node);
        stores.push(store);
    }

    for i in 0..3u32 {
        let outcome = write(
            &nodes[i as usize],
            vec![event(&format!("acc-{i}"), 1, "Opened")],
        )
        .await;
        assert!(!outcome.conflict);
    }
    for _ in 0..400 {
        if stores.iter().all(|s| s.applied_log().len() >= 3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let order: Vec<TxnId> = stores[0].applied_log().iter().map(|e| e.txn).collect();
    assert_eq!(order.len(), 3);
    for store in &stores {
        let this: Vec<TxnId> = store.applied_log().iter().map(|e| e.txn).collect();
        assert_eq!(this, order);
    }
    drop(tasks);
}
