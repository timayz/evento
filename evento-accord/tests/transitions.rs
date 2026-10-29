//! Two-phase layout changes (#288, #359): a change that gives a key replicas
//! a quorum of which could miss a quorum of the old ones drains the writes in
//! flight on it under the old layout, refusing new ones, before the new
//! layout is installed. These nodes run the recovery sweep, whose watermark
//! gossip and anti-entropy the drain waits on.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, LinkFilter, Message, MessageSink, Node, NodeConfig, NodeId, Status,
    Topology, TopologyChanging, TxnId,
};
use evento_core::Event;

fn ids(nodes: &[u64]) -> Vec<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

struct TestNode {
    node: Node,
    store: Arc<InMemoryDataStore>,
    topology: Arc<DynamicTopology>,
    _loop: tokio::task::JoinHandle<()>,
}

impl TestNode {
    fn sweep(&self) -> tokio::task::JoinHandle<()> {
        self.node.start_recovery()
    }
}

fn spawn(net: &Arc<InMemoryNetwork>, id: u64, shards: Vec<Vec<NodeId>>) -> TestNode {
    spawn_with(net, id, shards, NodeConfig::default())
}

/// [`spawn`], with `config`.
fn spawn_with(
    net: &Arc<InMemoryNetwork>,
    id: u64,
    shards: Vec<Vec<NodeId>>,
    config: NodeConfig,
) -> TestNode {
    let id = NodeId(id);
    let inbox = net.register(id);
    let store = Arc::new(InMemoryDataStore::new());
    let topology = Arc::new(DynamicTopology::new(id, 0, shards));
    let node = Node::new(
        id,
        Arc::clone(&topology) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(config);
    let _loop = node.start(inbox);
    TestNode {
        node,
        store,
        topology,
        _loop,
    }
}

async fn await_epoch(topology: &DynamicTopology, epoch: u64) {
    for _ in 0..2_000 {
        if topology.epoch() >= epoch {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("topology did not reach epoch {epoch}");
}

/// Waits until `node` applied `txn` as a commit.
async fn await_committed(node: &Node, store: &InMemoryDataStore, txn: TxnId) {
    for _ in 0..2_000 {
        if node.command_status(txn) == Some(Status::Applied)
            || store.applied_log().iter().any(|entry| entry.txn == txn)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let applied = store
        .applied_log()
        .into_iter()
        .find(|entry| entry.txn == txn)
        .unwrap_or_else(|| panic!("{txn:?} never applied"));
    assert!(!applied.conflict, "{txn:?} applied as an abort");
}

/// #359 end to end. T was accepted and committed by replicas 0 and 2 only:
/// replica 1 never heard of it, and nobody applied it. Replica 2 dies, and
/// the layout replaces it with node 3. Recovered under the layout it was
/// coordinated under, T is committed through 0 and 1 as decided — a recovery
/// under the new layout could have counted 1 and 3 as fenced and invalidated
/// it. The change installs the new layout only once T drained, so node 3,
/// joining through a replica the shard kept, holds it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replacing_a_dead_replica_drains_a_write_in_flight() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    let new = ids(&[0, 1, 3]);
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn(&net, id, vec![old.clone()]))
        .collect();
    let joiner = spawn(&net, 3, vec![old.clone()]);
    joiner.node.begin_join();

    // Replica 1 hears nothing of what node 0 coordinates.
    let to_one: LinkFilter = Arc::new(|from, to, message| {
        from == NodeId(0)
            && to == NodeId(1)
            && matches!(
                message,
                Message::PreAccept { .. } | Message::Accept { .. } | Message::Commit { .. }
            )
    });
    net.set_link_filter(Some(to_one));
    let txn = nodes[0]
        .node
        .coordinate_through_commit(vec![event("acc", 1)])
        .await
        .expect("committed through replicas 0 and 2");
    net.set_link_filter(None);
    assert_eq!(
        nodes[1].node.command_status(txn),
        None,
        "replica 1 never saw T"
    );
    net.crash(NodeId(2));

    let _sweeps: Vec<_> = nodes.iter().chain([&joiner]).map(TestNode::sweep).collect();
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, vec![new.clone()]),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, vec![new.clone()]);
    // Epoch 1 drained the moved keys, epoch 2 installed the new layout.
    assert_eq!(nodes[0].topology.epoch(), 2);
    assert_eq!(nodes[0].topology.nodes(), new);

    await_epoch(&joiner.topology, 2).await;
    joiner.node.join(NodeId(0)).await.expect("joins through 0");
    await_committed(&nodes[0].node, &nodes[0].store, txn).await;
    await_committed(&nodes[1].node, &nodes[1].store, txn).await;
    for _ in 0..2_000 {
        if !joiner.store.committed_events().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        joiner.store.committed_events().len(),
        1,
        "the new replica holds T"
    );

    // The new layout serves writes, with node 3 among its replicas.
    nodes[0]
        .node
        .write(vec![event("acc", 2)])
        .await
        .expect("a write after the change");
}

/// A new node a transition gives slots that no commit reaches — still
/// joining, it handles nothing else that would tell it — learns of the
/// transition from the metadata its sweep fetches, copies its slots, and the
/// change installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_new_replica_no_commit_reaches_learns_of_its_transition() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    let new = ids(&[0, 1, 3]);
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn(&net, id, vec![old.clone()]))
        .collect();
    let joiner = spawn(&net, 3, vec![old.clone()]);
    joiner.node.begin_join();
    let no_commit: LinkFilter = Arc::new(|_, to, message| {
        to == NodeId(3) && matches!(message, Message::ConfigCommit { .. })
    });
    net.set_link_filter(Some(no_commit));
    let _sweeps: Vec<_> = nodes.iter().chain([&joiner]).map(TestNode::sweep).collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1)])
        .await
        .expect("a write before the change");

    let installed = tokio::time::timeout(
        Duration::from_secs(20),
        nodes[0].node.change_topology(1, vec![new.clone()]),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, vec![new]);
    await_epoch(&joiner.topology, 2).await;
    assert_eq!(
        joiner.store.committed_events().len(),
        1,
        "node 3 copied the write"
    );
}

/// A key whose replicas change takes no new write while the change drains:
/// the write fails as `TopologyChanging` (`Unavailable` through the
/// executor), and succeeds once the new layout is installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_write_to_a_moving_key_is_unavailable_until_the_change_installs() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    let new = ids(&[0, 1, 3]);
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn(&net, id, vec![old.clone()]))
        .collect();
    let joiner = spawn(&net, 3, vec![old.clone()]);
    joiner.node.begin_join();
    let _sweeps: Vec<_> = nodes.iter().chain([&joiner]).map(TestNode::sweep).collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1)])
        .await
        .expect("a write before the change");

    let driver = nodes[0].node.clone();
    let change =
        tokio::spawn(async move { driver.change_topology(1, vec![ids(&[0, 1, 3])]).await });
    await_epoch(&nodes[1].topology, 1).await;
    let err = nodes[1]
        .node
        .write(vec![event("acc", 2)])
        .await
        .expect_err("the key is moving");
    let refused = err
        .downcast_ref::<TopologyChanging>()
        .unwrap_or_else(|| panic!("not refused as moving: {err:#}"));
    assert_eq!(refused.since, 1);
    assert!(nodes[1].node.metrics().writes_topology_changing >= 1);

    let installed = tokio::time::timeout(Duration::from_secs(30), change)
        .await
        .expect("the change ends in bounded time")
        .unwrap()
        .expect("the change installs");
    assert_eq!(installed, vec![new]);
    await_epoch(&nodes[1].topology, 2).await;
    nodes[1]
        .node
        .write(vec![event("acc", 2)])
        .await
        .expect("a write once the layout is installed");
}

/// A change back to the live layout while a transition is in progress
/// cancels it: nothing changes replicas, so it installs at once, and the keys
/// the transition fenced take writes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_change_back_to_the_live_layout_cancels_a_transition() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    // No sweep runs: no fence or watermark is reported, so the transition
    // never drains on its own.
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn(&net, id, vec![old.clone()]))
        .collect();
    let decided = nodes[0]
        .node
        .propose_topology(1, vec![ids(&[0, 1, 3])])
        .await
        .expect("a transition is decided");
    assert!(
        matches!(
            decided,
            evento_accord::ConfigEntry::Transition { since: 1, .. }
        ),
        "{decided:?}"
    );
    nodes[0].node.recover_topology(1).await.expect("committed");
    await_epoch(&nodes[0].topology, 1).await;
    let err = nodes[0]
        .node
        .write(vec![event("acc", 1)])
        .await
        .expect_err("the key is moving");
    assert!(err.is::<TopologyChanging>(), "{err:#}");

    let installed = nodes[0]
        .node
        .change_topology(2, vec![old.clone()])
        .await
        .expect("the change back installs at once");
    assert_eq!(installed, vec![old]);
    assert_eq!(nodes[0].topology.epoch(), 2);
    nodes[0]
        .node
        .write(vec![event("acc", 1)])
        .await
        .expect("the key takes writes again");
}

/// A change whose new quorums all meet the old ones — one replica added to
/// a shard of three — needs no drain: it installs at once, and writes go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn adding_one_replica_installs_at_once() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    let new = ids(&[0, 1, 2, 3]);
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn(&net, id, vec![old.clone()]))
        .collect();
    let joiner = spawn(&net, 3, vec![old.clone()]);
    joiner.node.begin_join();
    let installed = nodes[0]
        .node
        .change_topology(1, vec![new.clone()])
        .await
        .unwrap();
    assert_eq!(installed, vec![new]);
    assert_eq!(nodes[0].topology.epoch(), 1, "one epoch, no transition");
    nodes[0]
        .node
        .write(vec![event("acc", 1)])
        .await
        .expect("writes go on");
    // The new replica was not pre-seeded (#373): it joins, over an empty
    // store.
    assert_eq!(joiner.node.metrics().preseed_passes, 0);
    joiner
        .node
        .join(NodeId(0))
        .await
        .expect("joins over an empty store");
}

/// Whether `store` holds `event`, at its version.
fn holds(store: &InMemoryDataStore, event: &Event) -> bool {
    store
        .committed_events()
        .iter()
        .any(|((_, _, id, version), ulid)| {
            *id == event.aggregate_id && *version == event.version && *ulid == event.id
        })
}

/// #443, the nightly re-sharding swarm. T was committed through replicas 1
/// and 2 of `{0, 1, 2}`: node 0 never heard of it. Node 3 is added at once
/// and joins through node 0, so it holds nothing of T either, and W, a
/// later write on T's key, commits with T as a dependency. Cut off from 1
/// and 2, nodes 0 and 3 learn T's decision from each other alone. Counted
/// under the layout they installed, their two refusals leave no quorum of
/// three of `{0, 1, 2, 3}`; but T was planned under `{0, 1, 2}`, where 1
/// and 2 are a quorum — they committed it. Neither may invalidate it, and
/// healed, every replica holds T and W.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_dependency_planned_before_a_replica_was_added_is_not_invalidated() {
    let net = InMemoryNetwork::new();
    let old = ids(&[0, 1, 2]);
    let new = ids(&[0, 1, 2, 3]);
    let config = NodeConfig {
        fast_timeout: Duration::from_millis(20),
        collect_timeout: Duration::from_millis(500),
        recovery_interval: Duration::from_millis(20),
        anti_entropy_interval: Duration::from_millis(25),
        recovery_timeout: Duration::from_millis(100),
        compaction_margin: Duration::from_millis(200),
        ..NodeConfig::default()
    };
    let nodes: Vec<TestNode> = (0..3)
        .map(|id| spawn_with(&net, id, vec![old.clone()], config))
        .collect();
    let joiner = spawn_with(&net, 3, vec![old.clone()], config);
    joiner.node.begin_join();
    let _sweeps: Vec<_> = nodes.iter().chain([&joiner]).map(TestNode::sweep).collect();

    // Nodes 0 and 3 hear nothing of T from 1 and 2, nor anything they
    // could import it from.
    let t: Arc<Mutex<Option<TxnId>>> = Arc::default();
    let blind: LinkFilter = {
        let t = Arc::clone(&t);
        let outside = |node: NodeId| node != NodeId(0) && node != NodeId(3);
        Arc::new(move |from, to, message| {
            if outside(to) || !outside(from) {
                return false;
            }
            if matches!(message, Message::SyncData { .. }) {
                return true;
            }
            match *t.lock().unwrap() {
                // T in flight: no transaction at all.
                None => message.txn().is_some(),
                Some(t) => message.txn() == Some(t),
            }
        })
    };
    net.set_link_filter(Some(blind));
    let first = event("acc", 1);
    let committed = nodes[1]
        .node
        .write(vec![first.clone()])
        .await
        .expect("T commits through replicas 1 and 2");
    assert!(!committed.conflict);
    let txn = committed.txn;
    *t.lock().unwrap() = Some(txn);
    assert_eq!(
        nodes[0].node.command_status(txn),
        None,
        "node 0 never saw T"
    );

    let installed = nodes[1]
        .node
        .change_topology(1, vec![new.clone()])
        .await
        .expect("node 3 is added");
    assert_eq!(installed, vec![new]);
    await_epoch(&nodes[0].topology, 1).await;
    joiner.node.join(NodeId(0)).await.expect("joins through 0");
    // Committed only: nodes 0 and 3 cannot read it before T is decided
    // there, and a read quorum needs one of them.
    let second = event("acc", 2);
    nodes[1]
        .node
        .coordinate_through_commit(vec![second.clone()])
        .await
        .expect("W commits, after T");

    net.partition(NodeId(0), NodeId(1));
    net.partition(NodeId(0), NodeId(2));
    for _ in 0..200 {
        for (i, node) in [(0, &nodes[0].node), (3, &joiner.node)] {
            assert_eq!(
                node.command_status(txn),
                None,
                "node {i} decided T, which it never witnessed, alone with a replica added since"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !holds(&nodes[0].store, &second),
        "node 0 applied W without T"
    );

    net.set_link_filter(None);
    net.heal_all_partitions();
    let stores: Vec<&InMemoryDataStore> = nodes
        .iter()
        .chain([&joiner])
        .map(|n| n.store.as_ref())
        .collect();
    for _ in 0..2_000 {
        if stores
            .iter()
            .all(|store| holds(store, &first) && holds(store, &second))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for (i, store) in stores.iter().enumerate() {
        assert!(holds(store, &first), "node {i} lacks T");
        assert!(holds(store, &second), "node {i} lacks W");
    }
}
