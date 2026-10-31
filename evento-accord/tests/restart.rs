//! Phase B — durability: a node rebuilds its consensus state and data from the
//! journal after a restart.
//!
//! Here the journal is a kept `InMemoryJournal` handle (it survives the node
//! being torn down and rebuilt) — this validates the *recovery logic*. A genuinely
//! disk-backed journal (over fjall/sql) is the mechanical production step; the
//! same `recover_state` path drives it.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Key, Node, NodeConfig, NodeId, ShardedTopology, StaticTopology, Status, Timestamp, Topology,
    MAX_SKEW_MICROS,
};
use evento_core::Event;

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

async fn await_version(store: &InMemoryDataStore, agg: &str, want: u16) {
    for _ in 0..1000 {
        if store.version("", "test/Account", agg).await.unwrap_or(0) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("store did not reach version {want} for {agg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restarts_from_its_journal() {
    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();

    // Journals are kept across the restart (durable); stores/nodes are not.
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let mut stores: Vec<Arc<InMemoryDataStore>> = Vec::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut tasks: Vec<Vec<tokio::task::JoinHandle<()>>> = Vec::new();

    for &id in &ids {
        let inbox = net.register(id);
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        );
        tasks.push(vec![node.start(inbox), node.start_recovery()]);
        nodes.push(node);
        stores.push(store);
    }

    // Build up some state.
    for v in 1..=3 {
        nodes[0].write(vec![event("acc", v)]).await.unwrap();
    }
    for s in &stores {
        await_version(s, "acc", 3).await;
    }

    // Restart node 1: tear it down (losing its in-memory state) and bring it back
    // with a FRESH store but the SAME (durable) journal.
    for t in &tasks[1] {
        t.abort();
    }
    let id = NodeId(1);
    let inbox = net.register(id);
    let fresh_store = Arc::new(InMemoryDataStore::new());
    let restarted = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)),
        Arc::clone(&fresh_store) as Arc<dyn DataStore>,
        Arc::clone(&journals[1]) as Arc<dyn Journal>,
    );

    // Rebuild from the journal before serving.
    restarted.recover_state().await.unwrap();
    assert_eq!(
        fresh_store
            .version("", "test/Account", "acc")
            .await
            .unwrap(),
        3,
        "the restarted node rebuilt its data from the journal"
    );

    tasks[1] = vec![restarted.start(inbox), restarted.start_recovery()];
    nodes[1] = restarted;
    stores[1] = fresh_store;

    // The restarted node participates in new writes and converges.
    nodes[0].write(vec![event("acc", 4)]).await.unwrap();
    for s in &stores {
        await_version(s, "acc", 4).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restarts_while_writes_are_in_flight() {
    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let mut stores: Vec<Arc<InMemoryDataStore>> = Vec::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut tasks: Vec<Vec<tokio::task::JoinHandle<()>>> = Vec::new();

    for &id in &ids {
        let inbox = net.register(id);
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        );
        tasks.push(vec![node.start(inbox), node.start_recovery()]);
        nodes.push(node);
        stores.push(store);
    }

    // A writer keeps bumping the aggregate (read current, write next) via node 0.
    let writer_node = nodes[0].clone();
    let writer_store = Arc::clone(&stores[0]);
    let writer = tokio::spawn(async move {
        for _ in 0..30 {
            let v = writer_store
                .version("", "test/Account", "acc")
                .await
                .unwrap_or(0)
                + 1;
            let _ = writer_node.write(vec![event("acc", v)]).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    // Restart node 1 mid-flight: it loses in-memory state and misses whatever
    // commits during the window, then rebuilds and catches up.
    tokio::time::sleep(Duration::from_millis(40)).await;
    for t in &tasks[1] {
        t.abort();
    }
    let id = NodeId(1);
    let inbox = net.register(id);
    let fresh_store = Arc::new(InMemoryDataStore::new());
    let restarted = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)),
        Arc::clone(&fresh_store) as Arc<dyn DataStore>,
        Arc::clone(&journals[1]) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.unwrap();
    tasks[1] = vec![restarted.start(inbox), restarted.start_recovery()];
    nodes[1] = restarted;
    stores[1] = fresh_store;

    writer.await.unwrap();

    // After the dust settles, all three nodes converge on the same final version
    // (recovery + anti-entropy bring the restarted node fully up to date).
    for _ in 0..1000 {
        let v0 = stores[0].version("", "test/Account", "acc").await.unwrap();
        let v1 = stores[1].version("", "test/Account", "acc").await.unwrap();
        let v2 = stores[2].version("", "test/Account", "acc").await.unwrap();
        if v0 == v1 && v1 == v2 && v0 > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let versions: Vec<u16> = futures_versions(&stores).await;
    panic!("nodes did not converge after restart-under-load: {versions:?}");
}

async fn futures_versions(stores: &[Arc<InMemoryDataStore>]) -> Vec<u16> {
    let mut out = Vec::new();
    for s in stores {
        out.push(s.version("", "test/Account", "acc").await.unwrap_or(0));
    }
    out
}

/// After compaction has truncated a node's journal, a restart rebuilds correctly
/// from the durable data store plus the persisted watermark and the (now empty)
/// journal tail — it restores the redundancy floor, holds no stale consensus
/// state, and rejoins the cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restarts_after_compaction() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();

    // Both the journal and the data store are durable (kept across the restart);
    // only the in-memory node state is lost. This models a real disk-backed store.
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let stores: Vec<Arc<InMemoryDataStore>> =
        (0..3).map(|_| Arc::new(InMemoryDataStore::new())).collect();
    let mut nodes: Vec<Node> = Vec::new();
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for &id in &ids {
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&stores[id.0 as usize]) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        );
        tasks.push(node.start(inbox));
        nodes.push(node);
    }

    // Commit three events; let all three replicas apply them.
    let mut txns = Vec::new();
    for v in 1..=3 {
        txns.push(nodes[0].write(vec![event("acc", v)]).await.unwrap().txn);
    }
    for s in &stores {
        await_version(s, "acc", 3).await;
    }
    // A store shows an apply a moment before its replica records it: wait
    // until node 1 recorded all three, or compaction clamps below the last.
    for _ in 0..500 {
        if txns
            .iter()
            .all(|&txn| nodes[1].command_status(txn) == Some(Status::Applied))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Compact node 1 above every committed transaction: its journal is truncated
    // to empty and its in-memory commands are dropped, but its durable store keeps
    // the materialised state.
    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let watermark = Timestamp {
        micros: now_micros + 10_000,
        logical: 0,
        node: NodeId(0),
    };
    nodes[1].compact(watermark).await;
    assert_eq!(
        nodes[1].command_count(),
        0,
        "compaction dropped node 1's consensus state"
    );

    // Restart node 1: drop its in-memory state, rebuild with the SAME (durable)
    // store and the SAME (truncated) journal.
    tasks[1].abort();
    let id = NodeId(1);
    let inbox = net.register(id);
    let restarted = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)),
        Arc::clone(&stores[1]) as Arc<dyn DataStore>,
        Arc::clone(&journals[1]) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.unwrap();

    // Rebuilt from the durable store + persisted watermark: state intact, no stale
    // consensus commands replayed (the journal was truncated).
    assert_eq!(
        stores[1].version("", "test/Account", "acc").await.unwrap(),
        3,
        "the durable store retained the compacted state"
    );
    assert_eq!(
        restarted.command_count(),
        0,
        "nothing to replay from the truncated journal"
    );

    tasks[1] = restarted.start(inbox);
    nodes[1] = restarted;

    // A fresh write (above the watermark) lands on the restarted node too.
    tokio::time::sleep(Duration::from_millis(20)).await;
    nodes[0].write(vec![event("acc", 4)]).await.unwrap();
    for s in &stores {
        await_version(s, "acc", 4).await;
    }
}

/// A node restarted with its wall clock back where it minted a transaction id
/// must not mint that id again: replicas would take the new write for the old
/// transaction and answer with its outcome, so the write would be acked while
/// its events were never applied. Node 0 coordinates a write to a key only the
/// other shard owns, so its own journal never holds the transaction — only the
/// clock bound it persisted before minting can tell it how far it got.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_coordinator_never_reuses_a_transaction_id() {
    let net = InMemoryNetwork::new();
    let shards = vec![
        vec![NodeId(0), NodeId(1), NodeId(2)],
        vec![NodeId(3), NodeId(4), NodeId(5)],
    ];
    let ids: Vec<NodeId> = shards.iter().flatten().copied().collect();
    let probe = ShardedTopology::new(NodeId(0), shards.clone());
    let agg = (0..10_000)
        .map(|i| format!("far-{i}"))
        .find(|id| probe.shard_of(&Key(id.clone())) == 1)
        .expect("an id in shard 1");

    let journals: Vec<Arc<InMemoryJournal>> = ids
        .iter()
        .map(|_| Arc::new(InMemoryJournal::new()))
        .collect();
    let stores: Vec<Arc<InMemoryDataStore>> = ids
        .iter()
        .map(|_| Arc::new(InMemoryDataStore::new()))
        .collect();
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::new(ShardedTopology::new(id, shards.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&stores[i]) as Arc<dyn DataStore>,
            Arc::clone(&journals[i]) as Arc<dyn Journal>,
        );
        tasks.push(node.start(inbox));
        nodes.push(node);
    }

    let old = nodes[0].write(vec![event(&agg, 1)]).await.unwrap().txn;
    for store in &stores[3..] {
        await_version(store, &agg, 1).await;
    }
    assert_eq!(old.0.node, NodeId(0));
    assert_eq!(
        old.0.logical, 0,
        "precondition: first stamp of its microsecond"
    );
    assert!(
        journals[0].load(old).await.unwrap().is_none(),
        "precondition: node 0 owns none of the key"
    );

    // Restart node 0 with its wall clock back at the microsecond it minted `old`.
    tasks[0].abort();
    let inbox = net.register(NodeId(0));
    let micros = old.0.micros;
    let restarted = Node::new(
        NodeId(0),
        Arc::new(ShardedTopology::new(NodeId(0), shards.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::with_physical(NodeId(0), move || micros)),
        Arc::new(net.sink(NodeId(0))),
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::clone(&journals[0]) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.unwrap();
    tasks[0] = restarted.start(inbox);
    await_confirmed(&restarted).await;

    let new = tokio::time::timeout(
        Duration::from_secs(10),
        restarted.write(vec![event(&agg, 2)]),
    )
    .await
    .expect("the write timed out")
    .expect("the write commits")
    .txn;
    assert!(new > old, "re-minted {new:?} after a restart (was {old:?})");
    for store in &stores[3..] {
        await_version(store, &agg, 2).await;
    }
}

/// The clock bound sits a few skews past the wall clock, and a restart starts
/// the clock there: restarting again and again, each time minting, must not
/// push the clock ever further ahead of the wall clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_restarts_keep_the_clock_within_the_skew_bound() {
    use evento_accord::clock::system_micros;
    use evento_accord::Clock;

    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        );
        tasks.push(node.start(inbox));
        nodes.push(node);
    }

    // Scheduling noise on top of the bound.
    let slack = 50_000;
    for version in 1..=4 {
        nodes[0].write(vec![event("acc", version)]).await.unwrap();
        tasks[0].abort();
        let inbox = net.register(NodeId(0));
        let clock = Arc::new(HybridLogicalClock::new(NodeId(0)));
        let restarted = Node::new(
            NodeId(0),
            Arc::new(StaticTopology::new(NodeId(0), ids.clone())) as Arc<dyn Topology>,
            Arc::clone(&clock),
            Arc::new(net.sink(NodeId(0))),
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journals[0]) as Arc<dyn Journal>,
        );
        restarted.recover_state().await.unwrap();
        let lead = clock.now().micros.saturating_sub(system_micros());
        assert!(
            lead <= MAX_SKEW_MICROS + slack,
            "restart {version}: the clock leads the wall clock by {lead}µs"
        );
        tasks[0] = restarted.start(inbox);
        await_confirmed(&restarted).await;
        nodes[0] = restarted;
    }
}

/// A retired node mints nothing more: a write through it — a clone the
/// application still holds — fails with `NodeClosed`, so it can never mint an
/// id the node reopened over the same journal mints too. That node writes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_node_refuses_to_write_and_the_reopened_one_writes_on() {
    use evento_accord::NodeClosed;

    let id = NodeId(0);
    let net = InMemoryNetwork::new();
    let journal = Arc::new(InMemoryJournal::new());
    let store = Arc::new(InMemoryDataStore::new());
    let open = |inbox| {
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, vec![id])) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        );
        let task = node.start(inbox);
        (node, task)
    };

    let (node, task) = open(net.register(id));
    node.write(vec![event("acc", 1)]).await.unwrap();

    node.retire().await;
    task.abort();
    let err = node.write(vec![event("acc", 2)]).await.unwrap_err();
    assert!(err.is::<NodeClosed>(), "unexpected error: {err:#}");

    let (reopened, _task) = open(net.register(id));
    reopened.recover_state().await.unwrap();
    reopened.write(vec![event("acc", 2)]).await.unwrap();
    assert_eq!(store.version("", "test/Account", "acc").await.unwrap(), 2);
}

/// Every node stopped, then reopened over its journal and store — a
/// whole-group restart, or a cold copy of the whole group restored (#452):
/// no node takes a contact for "truncated above its proven coverage". Its
/// sync coverage starts over at the restart, but what it vouched for is in
/// its journal, and a peer compacts no further than the reports it heard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_whole_group_restart_raises_no_truncation_alarm() {
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .without_time()
            .with_writer(std::io::stderr)
            .try_init();
    }
    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let stores: Vec<Arc<InMemoryDataStore>> =
        (0..3).map(|_| Arc::new(InMemoryDataStore::new())).collect();
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(50),
        anti_entropy_interval: Duration::from_millis(10),
        // Every compaction reaches the journal: the floor a node restarts
        // from is the one it reached.
        truncate_interval: Duration::ZERO,
        ..NodeConfig::default()
    };
    let open = |id: NodeId| {
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            Arc::clone(&stores[id.0 as usize]) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        )
        .with_config(config);
        let task = node.start(net.register(id));
        (node, task)
    };
    let (mut nodes, mut tasks): (Vec<Node>, Vec<tokio::task::JoinHandle<()>>) =
        ids.iter().map(|&id| open(id)).unzip();

    for v in 1..=3 {
        nodes[0].write(vec![event("acc", v)]).await.unwrap();
    }
    for store in &stores {
        await_version(store, "acc", 3).await;
    }
    // Past the margin, every report covers the three writes. Sweeping by
    // hand: a few rounds prove every node's coverage and compact. Then,
    // the reports having moved on by the margin, nodes 1 and 2 sweep
    // first and compact below node 0's older report; node 0, sweeping
    // last, compacts below their fresh ones. Each journal keeps the floor
    // its node reached — node 0's above node 1's, and no higher than what
    // node 1 reported.
    tokio::time::sleep(Duration::from_millis(150)).await;
    for _ in 0..4 {
        for node in &nodes {
            node.sweep().await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    nodes[1].sweep().await;
    nodes[2].sweep().await;
    nodes[0].sweep().await;
    assert!(
        nodes.iter().all(|node| node.metrics().compactions > 0),
        "every node compacted before the restart"
    );

    // The whole group stops, then every node reopens over its files.
    for (node, task) in nodes.drain(..).zip(tasks.drain(..)) {
        node.retire().await;
        task.abort();
    }
    for &id in &ids {
        let (node, task) = open(id);
        node.recover_state().await.unwrap();
        nodes.push(node);
        tasks.push(task);
    }
    // Each asked the others to confirm its journal as it started, before
    // they all listened: the sweep asks again (#339).
    for node in &nodes {
        node.sweep().await;
    }
    for node in &nodes {
        await_confirmed(node).await;
    }

    // Node 1's first anti-entropy round asks node 0, whose floor is above
    // node 1's own: above the coverage node 1 has proven since it reopened
    // (nothing yet), not above what it vouched for before.
    nodes[1].sweep().await;
    let metrics = nodes[1].metrics();
    assert_eq!(
        metrics.sync_contact_truncated, 0,
        "a healthy whole-group restart was taken for a divergence"
    );
    assert!(!nodes[1].rejoin_required());
}

/// A journal that is not the one this node id ran under — a fresh file
/// under a known id, as a node started over a directory emptied while
/// another process of that id still served (#453) — is found out at its
/// first report instead of being taken for a fresh replica: its mark names
/// another journal of the same life, and the peers saw more of the one
/// before. The node stops, as over a rolled-back journal, and comes back
/// joining over an empty journal and store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_journal_under_a_known_node_id_is_found_rolled_back() {
    use evento_accord::JournalRolledBack;

    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let open = |id: NodeId, store: Arc<InMemoryDataStore>, journal: Arc<InMemoryJournal>| {
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)),
            store as Arc<dyn DataStore>,
            journal as Arc<dyn Journal>,
        );
        let tasks = vec![node.start(net.register(id)), node.start_recovery()];
        (node, tasks)
    };
    let stores: Vec<Arc<InMemoryDataStore>> =
        (0..3).map(|_| Arc::new(InMemoryDataStore::new())).collect();
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for &id in &ids {
        let (node, node_tasks) = open(
            id,
            Arc::clone(&stores[id.0 as usize]),
            Arc::new(InMemoryJournal::new()),
        );
        // As a host opens a node: over its journal, which gets its identity.
        node.recover_state().await.unwrap();
        nodes.push(node);
        tasks.push(node_tasks);
    }
    for v in 1..=3 {
        nodes[0].write(vec![event("acc", v)]).await.unwrap();
    }
    for store in &stores {
        await_version(store, "acc", 3).await;
    }
    // The peers heard node 1's reports, and the mark of its journal with them.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Node 1 stops; its files are gone; it is started again plainly, over
    // an empty journal and store — a fresh replica, as far as it knows.
    nodes[1].retire().await;
    for task in &tasks[1] {
        task.abort();
    }
    let (fresh, fresh_tasks) = open(
        NodeId(1),
        Arc::new(InMemoryDataStore::new()),
        Arc::new(InMemoryJournal::new()),
    );
    fresh.recover_state().await.unwrap();
    tasks[1] = fresh_tasks;
    nodes[1] = fresh;

    // Its first report tells its peers of another journal under its id.
    for _ in 0..400 {
        if nodes[1].rolled_back() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        nodes[1].rolled_back(),
        "a fresh journal under a known node id was taken for a fresh replica"
    );
    let err = nodes[1].write(vec![event("acc", 4)]).await.unwrap_err();
    assert!(err.is::<JournalRolledBack>(), "unexpected error: {err:#}");
}

/// Waits until `node`, restarted over a journal that holds something, is
/// confirmed by its peers (#339): until then it takes no write.
async fn await_confirmed(node: &Node) {
    for _ in 0..1_000 {
        if node.confirmed() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the restarted node is confirmed");
}
