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
    hosts: Vec<Arc<GroupHost>>,
    /// Groups each host's unrouted handler was told about.
    unrouted: Vec<Arc<Mutex<Vec<GroupId>>>>,
    /// Groups each host's rejoin handler was told about.
    rejoined: Vec<Arc<Mutex<Vec<GroupId>>>>,
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
        let mut rejoined = Vec::new();
        let mut tasks = Vec::new();
        for (id, listener) in listeners {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let hook = Arc::clone(&seen);
            let told = Arc::new(Mutex::new(Vec::new()));
            let rejoin_hook = Arc::clone(&told);
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
            hosts.push(Arc::new(
                GroupHost::new(mux, scheduler)
                    .with_rejoin_handler(move |g| rejoin_hook.lock().unwrap().push(g)),
            ));
            unrouted.push(seen);
            rejoined.push(told);
        }

        MuxCluster {
            ids,
            hosts,
            unrouted,
            rejoined,
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
    assert!(!writer.is_finished(), "fast quorum needs host 2");

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
    assert!(cluster.hosts[2].close(g.id).await.is_some());
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

/// Every committed `(type, id, version)` on a host, comparable across hosts.
fn committed(store: &InMemoryDataStore) -> std::collections::BTreeSet<(String, String, u16)> {
    store
        .committed_events()
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

async fn await_committed(store: &InMemoryDataStore, expected_len: usize) {
    for _ in 0..600 {
        if store.committed_events().len() >= expected_len {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "store holds {} committed events, expected {expected_len}",
        store.committed_events().len()
    );
}

const COMPACT_ALL: Timestamp = Timestamp {
    micros: u64::MAX,
    logical: 0,
    node: NodeId(0),
};

impl MuxCluster {
    async fn open_joining(
        &self,
        group: &Group,
        host: usize,
        config: NodeConfig,
        contacts: &[NodeId],
    ) -> anyhow::Result<Node> {
        self.hosts[host]
            .open_joining(
                group.id,
                self.topology(host),
                Arc::clone(&group.stores[host]) as Arc<dyn DataStore>,
                Arc::clone(&group.journals[host]) as Arc<dyn Journal>,
                config,
                contacts,
            )
            .await
    }
}

/// A host that lost a group's store (a new disk, a deleted file) after its
/// peers compacted cannot catch up through anti-entropy — the prefix is no
/// longer held as commands anywhere. Opening it by joining installs a peer's
/// snapshot and floor, and the floor is persisted for the next restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_with_a_lost_store_rejoins_after_compaction() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(50), 3);
    let nodes = cluster.open_everywhere(&g).await;
    for i in 0..3 {
        write(&nodes[0], vec![event(&format!("acc-{i}"), 1, "Opened")]).await;
    }
    for store in &g.stores {
        await_committed(store, 3).await;
    }
    for node in &nodes {
        node.compact(COMPACT_ALL).await;
        assert_eq!(node.command_count(), 0, "every command compacted away");
    }

    // Host 2 comes back with an empty store; its journal survived.
    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());

    // A plain open recovers only the journal's floor, and anti-entropy ships
    // nothing: every command it would replay is compacted away everywhere.
    cluster.open(&g, 2).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(g.stores[2].committed_events().is_empty());
    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());

    let joined = cluster
        .open_joining(&g, 2, NodeConfig::default(), &[NodeId(0), NodeId(1)])
        .await
        .expect("join");
    assert!(cluster.hosts[2].is_open(g.id));
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
    let floor = g.journals[2]
        .load_watermark()
        .await
        .unwrap()
        .expect("the adopted floor is persisted");
    assert!(floor > Timestamp::MIN);

    // A write that depends on a compacted transaction lands on the joiner,
    // and the joiner can coordinate one too.
    write(&nodes[0], vec![event("acc-0", 2, "Deposited")]).await;
    write(&joined, vec![event("acc-1", 2, "Deposited")]).await;
    for store in &g.stores {
        await_committed(store, 5).await;
    }
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
}

/// The snapshot crosses in pages budgeted by bytes: with a budget under one
/// event, a six-event group takes six pages over real TCP, and the joiner
/// ends up identical, with the floor persisted, and coordinating writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_group_joins_page_by_page_over_tcp() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(53), 3);
    let config = NodeConfig {
        snapshot_page_bytes: 1,
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    };
    let mut nodes = Vec::new();
    for h in 0..3 {
        nodes.push(cluster.open_with(&g, h, config).await);
    }
    for i in 0..6 {
        write(&nodes[0], vec![event(&format!("acc-{i}"), 1, "Opened")]).await;
    }
    for store in &g.stores {
        await_committed(store, 6).await;
    }
    for node in &nodes {
        node.compact(COMPACT_ALL).await;
    }

    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    let joined = cluster
        .open_joining(&g, 2, config, &[NodeId(0), NodeId(1)])
        .await
        .expect("join");
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
    assert!(
        nodes[0].metrics().snapshot_pages >= 6,
        "the contact served a page per event: {}",
        nodes[0].metrics().snapshot_pages
    );
    assert_eq!(joined.metrics().joins_partial, 0);
    let floor = g.journals[2]
        .load_watermark()
        .await
        .unwrap()
        .expect("the adopted floor is persisted");
    assert!(floor > Timestamp::MIN);

    write(&nodes[0], vec![event("acc-0", 2, "Deposited")]).await;
    write(&joined, vec![event("acc-1", 2, "Deposited")]).await;
    for store in &g.stores {
        await_committed(store, 8).await;
    }
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
}

/// A `DataStore` whose snapshot pages stop after the first: what a contact
/// that cannot read its backend halfway looks like to a joiner.
struct BreaksAfterFirstPage {
    inner: Arc<InMemoryDataStore>,
    pages: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl DataStore for BreaksAfterFirstPage {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        self.inner.apply(txn, execute_at, events, commit).await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        self.inner.snapshot().await
    }

    async fn snapshot_page(
        &self,
        after: Option<evento_core::cursor::Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<evento_accord::SnapshotPage> {
        use std::sync::atomic::Ordering;
        if self.pages.fetch_add(1, Ordering::SeqCst) >= 1 {
            anyhow::bail!("the backend is gone");
        }
        self.inner.snapshot_page(after, max_events, max_bytes).await
    }
}

/// A join that breaks once a page has landed cannot move on to another
/// contact (its pages would overlap the installed prefix and be skipped,
/// leaving a gap): it stops, says so with `PartialSnapshot`, and leaves
/// nothing registered. A fresh store joins fine afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_that_breaks_after_a_page_stops_and_names_the_partial_install() {
    use evento_accord::PartialSnapshot;

    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(54), 3);
    let config = NodeConfig {
        snapshot_page_events: 1,
        collect_timeout: Duration::from_millis(300),
        ..NodeConfig::default()
    };
    let breaking = Arc::new(BreaksAfterFirstPage {
        inner: Arc::clone(&g.stores[0]),
        pages: std::sync::atomic::AtomicUsize::new(0),
    });
    let node0 = cluster.hosts[0]
        .open(
            g.id,
            cluster.topology(0),
            breaking as Arc<dyn DataStore>,
            Arc::clone(&g.journals[0]) as Arc<dyn Journal>,
            config,
        )
        .await
        .expect("open host 0");
    let node1 = cluster.open_with(&g, 1, config).await;
    let node2 = cluster.open_with(&g, 2, config).await;
    for i in 0..3 {
        write(&node0, vec![event(&format!("acc-{i}"), 1, "Opened")]).await;
    }
    for store in &g.stores {
        await_committed(store, 3).await;
    }
    for node in [&node0, &node1, &node2] {
        node.compact(COMPACT_ALL).await;
    }

    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    let err = match cluster
        .open_joining(&g, 2, config, &[NodeId(0), NodeId(1)])
        .await
    {
        Ok(_) => panic!("the join must break after the first page"),
        Err(err) => err,
    };
    assert_eq!(
        err.downcast_ref::<PartialSnapshot>(),
        Some(&PartialSnapshot {
            pages: 1,
            events: 1
        }),
        "unexpected error: {err:#}"
    );
    assert!(format!("{err:#}").contains("empty the store"));
    assert_eq!(
        node1.metrics().snapshot_pages,
        0,
        "the second contact was never asked"
    );
    assert!(!cluster.hosts[2].is_open(g.id));
    assert_eq!(g.stores[2].committed_events().len(), 1, "the prefix stayed");

    // Emptied (the journal kept), the store joins through the healthy contact.
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    let joined = cluster
        .open_joining(&g, 2, config, &[NodeId(1)])
        .await
        .expect("join through host 1");
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[1]));
    assert_eq!(joined.metrics().joins_partial, 0);
}

/// A store larger than one transport frame (64 MiB) joins: the default page
/// budget splits it into frames the codec accepts. Seeds the contacts'
/// stores directly (consensus would take far too long for this much data)
/// and moves about 70 MiB over loopback, so it is left out of the default
/// run: `cargo test -p evento-accord --test mux_cluster -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "moves 70 MiB over loopback; run on demand"]
async fn a_store_larger_than_one_frame_joins() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let g = Group::new(GroupId(55), 3);
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(30),
        ..NodeConfig::default()
    };
    let events: Vec<Event> = (0..70)
        .map(|i| Event {
            data: vec![i as u8; 1024 * 1024],
            ..event(&format!("blob-{i}"), 1, "Stored")
        })
        .collect();
    for store in &g.stores[..2] {
        store
            .apply(TxnId(Timestamp::MIN), Timestamp::MIN, events.clone(), true)
            .await
            .unwrap();
    }
    let node0 = cluster.open_with(&g, 0, config).await;
    cluster.open_with(&g, 1, config).await;

    let joined = cluster
        .open_joining(&g, 2, config, &[NodeId(0)])
        .await
        .expect("join");
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
    assert!(
        node0.metrics().snapshot_pages >= 17,
        "about 4 MiB per page: {}",
        node0.metrics().snapshot_pages
    );
    write(&joined, vec![event("blob-0", 2, "Touched")]).await;
    for store in &g.stores {
        await_committed(store, 71).await;
    }
}

/// A contact that cannot answer (here: the group is closed on it and nothing
/// reopens it) costs one timeout; the join moves on to the next contact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_moves_on_when_a_contact_does_not_answer() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(51), 3);
    let nodes = cluster.open_everywhere(&g).await;
    write(&nodes[0], vec![event("acc", 1, "Opened")]).await;
    for store in &g.stores {
        await_committed(store, 1).await;
    }
    for node in &nodes {
        node.compact(COMPACT_ALL).await;
    }

    cluster.hosts[1].close(g.id).await.expect("open on host 1");
    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    let config = NodeConfig {
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    };
    cluster
        .open_joining(&g, 2, config, &[NodeId(1), NodeId(0)])
        .await
        .expect("join through the second contact");
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
    // The join request asked the closed host to open the group.
    assert!(cluster.unrouted[1].lock().unwrap().contains(&g.id));

    // With no contact answering, nothing is left registered.
    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    cluster.hosts[0].close(g.id).await.expect("open on host 0");
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    assert!(cluster
        .open_joining(&g, 2, config, &[NodeId(1), NodeId(0)])
        .await
        .is_err());
    assert!(!cluster.hosts[2].is_open(g.id));
}

/// An open dropped mid-join (its caller hung up) leaves nothing running: the
/// group is unregistered, its inbox loop stopped, and a later open joins anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_dropped_mid_join_leaves_nothing_running() {
    let cluster = MuxCluster::start(3, Duration::from_millis(200)).await;
    let mut g = Group::new(GroupId(52), 3);
    let nodes = cluster.open_everywhere(&g).await;
    write(&nodes[0], vec![event("acc", 1, "Opened")]).await;
    for store in &g.stores {
        await_committed(store, 1).await;
    }

    // Host 1 is closed and nothing reopens it: a join through it hangs.
    cluster.hosts[1].close(g.id).await.expect("open on host 1");
    cluster.hosts[2].close(g.id).await.expect("open on host 2");
    g.stores[2] = Arc::new(InMemoryDataStore::new());
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(30),
        ..NodeConfig::default()
    };
    let dropped = tokio::time::timeout(
        Duration::from_millis(300),
        cluster.open_joining(&g, 2, config, &[NodeId(1)]),
    )
    .await;
    assert!(dropped.is_err(), "the join through a closed host finished");
    assert!(!cluster.hosts[2].is_open(g.id));
    assert!(
        !cluster.hosts[2].transport().is_registered(g.id),
        "the dropped open left its inbox registered"
    );

    cluster
        .open_joining(&g, 2, NodeConfig::default(), &[NodeId(0)])
        .await
        .expect("join through host 0");
    assert_eq!(committed(&g.stores[2]), committed(&g.stores[0]));
    assert!(cluster.hosts[2].close(g.id).await.is_some());
}

/// An idle group's peers keep gossiping watermarks; a host that closed the group
/// must not be asked to reopen it for them (that would undo every close).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_traffic_for_an_unknown_group_never_wakes_the_handler() {
    let cluster = MuxCluster::start(2, Duration::from_millis(100)).await;
    let stray = GroupId(98);
    let sink = cluster.hosts[0].transport().sink(stray);
    let watermark = Message::Watermark {
        applied_through: Timestamp {
            micros: 1,
            logical: 0,
            node: NodeId(0),
        },
    };
    sink.send(NodeId(1), watermark).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        cluster.unrouted[1].lock().unwrap().is_empty(),
        "a watermark is not a reason to open a group"
    );

    // A consensus message for the same group still is.
    let applied = Message::Applied {
        txn: TxnId(Timestamp {
            micros: 1,
            logical: 0,
            node: NodeId(0),
        }),
    };
    sink.send(NodeId(1), applied).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cluster.unrouted[1].lock().unwrap().as_slice(), &[stray]);

    // So is a joining replica's snapshot request, unlike anti-entropy's.
    let other = GroupId(97);
    let sink = cluster.hosts[0].transport().sink(other);
    let anti_entropy = Message::SyncRequest {
        id: 1,
        snapshot: false,
        known: None,
    };
    sink.send(NodeId(1), anti_entropy).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!cluster.unrouted[1].lock().unwrap().contains(&other));
    let join = Message::SyncRequest {
        id: 2,
        snapshot: true,
        known: None,
    };
    sink.send(NodeId(1), join).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(cluster.unrouted[1].lock().unwrap().contains(&other));
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

/// A host that had a group closed (a tenant evicted, a node down) past
/// `peer_absence_limit` no longer holds its peers' compaction back. Reopened
/// over its old journal after writes were compacted without it, its node is
/// told to rejoin and the host's handler hears of it; the host then does what
/// a lost store needs: close, discard, open by joining.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_rejoins_a_group_its_peers_gave_up_on() {
    a_host_rejoins_a_group_of(3).await;
}

/// The same with four hosts, where a report needs two peers' coverage: the
/// joined node is counted again from its first report, and its host is not
/// told to rejoin a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_rejoins_a_group_of_four_its_peers_gave_up_on() {
    a_host_rejoins_a_group_of(4).await;
}

/// Host `n - 1` closes the group past the absence limit, is told to rejoin
/// over its old journal, and rejoins through the others.
async fn a_host_rejoins_a_group_of(n: u64) {
    let cluster = MuxCluster::start(n, Duration::from_millis(200)).await;
    let size = n as usize;
    let gone = size - 1;
    let mut g = Group::new(GroupId(52), size);
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(300),
        peer_absence_limit: Some(Duration::from_secs(1)),
        ..NodeConfig::default()
    };
    let mut nodes = Vec::new();
    for h in 0..size {
        nodes.push(cluster.open_with(&g, h, config).await);
    }
    write(&nodes[0], vec![event("acc", 1, "Opened")]).await;
    for store in &g.stores {
        await_committed(store, 1).await;
    }

    // The last host closes the group; the others keep writing and, once it
    // has been silent past the limit, compact without it.
    cluster.hosts[gone]
        .close(g.id)
        .await
        .expect("open on the last host");
    write(&nodes[0], vec![event("acc", 2, "Deposited")]).await;
    write(&nodes[1], vec![event("acc", 3, "Deposited")]).await;
    for _ in 0..600 {
        if (0..gone).all(|h| nodes[h].command_count() == 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        nodes[0].command_count(),
        0,
        "host 0 compacted without the last host"
    );
    assert_eq!(nodes[0].absent_peers(), vec![NodeId(gone as u64)]);

    // Back over its old journal (the in-memory store is not durable, so it is
    // rebuilt from the journal as in the restart tests), the last host's node
    // is behind and told so.
    g.stores[gone] = Arc::new(InMemoryDataStore::new());
    let stale = cluster.open_with(&g, gone, config).await;
    for _ in 0..600 {
        if cluster.rejoined[gone].lock().unwrap().contains(&g.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(cluster.rejoined[gone].lock().unwrap().as_slice(), &[g.id]);
    assert!(stale.rejoin_required());
    assert!(stale
        .write(vec![event("acc", 4, "Deposited")])
        .await
        .is_err());

    // The handler's job: close, discard the event store (keeping the
    // journal), open by joining.
    cluster.hosts[gone]
        .close(g.id)
        .await
        .expect("open on the last host");
    g.stores[gone] = Arc::new(InMemoryDataStore::new());
    let contacts: Vec<NodeId> = (0..gone as u64).map(NodeId).collect();
    let joined = cluster
        .open_joining(&g, gone, config, &contacts)
        .await
        .expect("join");
    assert_eq!(committed(&g.stores[gone]), committed(&g.stores[0]));
    for _ in 0..200 {
        if (0..gone).all(|h| nodes[h].absent_peers().is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (h, node) in nodes.iter().enumerate().take(gone) {
        assert!(
            node.absent_peers().is_empty(),
            "host {h} counts the last host again"
        );
    }
    assert!(!joined.rejoin_required(), "not told to rejoin again");

    // Writes land on the joiner and it can coordinate one.
    write(&nodes[0], vec![event("acc", 4, "Deposited")]).await;
    write(&joined, vec![event("acc", 5, "Deposited")]).await;
    for store in &g.stores {
        await_committed(store, 5).await;
    }
    assert_eq!(committed(&g.stores[gone]), committed(&g.stores[0]));
    assert_eq!(
        cluster.rejoined[gone].lock().unwrap().as_slice(),
        &[g.id],
        "the host was told to rejoin once"
    );
}

/// A journal whose restart load pauses, so two opens of one group overlap.
struct SlowJournal(Arc<InMemoryJournal>, Duration);

#[async_trait::async_trait]
impl Journal for SlowJournal {
    async fn record(&self, state: &evento_accord::CommandState) -> anyhow::Result<()> {
        self.0.record(state).await
    }
    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<evento_accord::CommandState>> {
        self.0.load(txn).await
    }
    async fn load_all(&self) -> anyhow::Result<Vec<evento_accord::CommandState>> {
        self.0.load_all().await
    }
    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        tokio::time::sleep(self.1).await;
        self.0.load_watermark().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opens_of_one_group_leave_one_working_node() {
    let cluster = Arc::new(MuxCluster::start(3, Duration::from_secs(5)).await);
    let group = Arc::new(Group::new(GroupId(77), 3));
    let nodes = [cluster.open(&group, 0).await, cluster.open(&group, 1).await];

    // A request and the unrouted handler race to open the group on host 2: the
    // first to start is also the first to finish recovering its state.
    let open = |pause: Duration| {
        let (cluster, group) = (Arc::clone(&cluster), Arc::clone(&group));
        tokio::spawn(async move {
            cluster.hosts[2]
                .open(
                    group.id,
                    cluster.topology(2),
                    Arc::clone(&group.stores[2]) as Arc<dyn DataStore>,
                    Arc::new(SlowJournal(Arc::clone(&group.journals[2]), pause))
                        as Arc<dyn Journal>,
                    NodeConfig::default(),
                )
                .await
                .expect("open")
        })
    };
    let first = open(Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = open(Duration::from_millis(100));
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!(cluster.hosts[2].len(), 1);
    let kept = cluster.hosts[2].get(group.id).expect("open");
    for node in [&first, &second] {
        assert!(
            Arc::ptr_eq(&kept.metrics_handle(), &node.metrics_handle()),
            "both opens return the one node the host keeps"
        );
    }

    // The surviving node still hears its peers: it applies a peer's write…
    write(&nodes[0], vec![event("acc", 1, "Opened")]).await;
    group.await_applied(1).await;
    // …and coordinates one of its own.
    write(&kept, vec![event("acc", 2, "Bumped")]).await;
    group.await_applied(2).await;
    group.assert_identical_order(2);
}
