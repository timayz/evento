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
    Node, NodeId, StaticTopology, Topology,
};
use evento_core::Event;

fn event(aggregator_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::new(),
        aggregator_type: "test/Account".into(),
        aggregator_id: aggregator_id.into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

async fn await_version(store: &InMemoryDataStore, agg: &str, want: u16) {
    for _ in 0..1000 {
        if store.version("test/Account", agg).await.unwrap_or(0) >= want {
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
    let journals: Vec<Arc<InMemoryJournal>> = (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
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
        fresh_store.version("test/Account", "acc").await.unwrap(),
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
                .version("test/Account", "acc")
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
        let v0 = stores[0].version("test/Account", "acc").await.unwrap();
        let v1 = stores[1].version("test/Account", "acc").await.unwrap();
        let v2 = stores[2].version("test/Account", "acc").await.unwrap();
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
        out.push(s.version("test/Account", "acc").await.unwrap_or(0));
    }
    out
}
