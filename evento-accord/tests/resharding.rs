//! M4 — moving keys between shards (#288, #360). A change that adds a shard,
//! or moves a replica to another shard, is a layout transition: the keys it
//! moves drain under the old layout, every node gaining slots copies their
//! events from a replica of the shard that held them, and only then does the
//! new layout go live. The slot table moves only the slots the change needs.
//! Garbage-collecting a moved range from its old owners is future work; the old
//! replicas simply stop being contacted for it.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, Key, MessageSink, Node, NodeId, ShardMap, Topology,
};
use evento_core::Event;

struct TestNode {
    node: Node,
    store: Arc<InMemoryDataStore>,
    topology: Arc<DynamicTopology>,
    _loop: tokio::task::JoinHandle<()>,
    _sweep: tokio::task::JoinHandle<()>,
}

/// A node over `shards` at epoch 0, running the recovery sweep the drain and
/// the copies are driven by.
fn spawn_node(id: NodeId, shards: Vec<Vec<NodeId>>, net: &Arc<InMemoryNetwork>) -> TestNode {
    let inbox = net.register(id);
    let clock = Arc::new(HybridLogicalClock::new(id));
    let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
    let store = Arc::new(InMemoryDataStore::new());
    let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
    let topology = Arc::new(DynamicTopology::new(id, 0, shards));
    let node = Node::new(
        id,
        topology.clone() as Arc<dyn Topology>,
        clock,
        sink,
        Arc::clone(&store) as Arc<dyn DataStore>,
        journal,
    );
    let _loop = node.start(inbox);
    let _sweep = node.start_recovery();
    TestNode {
        node,
        store,
        topology,
        _loop,
        _sweep,
    }
}

fn ids(nodes: &[u64]) -> Vec<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

/// The versions `store` holds for `aggregate_id`, ascending.
fn versions_of(store: &InMemoryDataStore, aggregate_id: &str) -> Vec<u16> {
    let mut versions: Vec<u16> = store
        .committed_events()
        .into_iter()
        .filter(|((_, _, id, _), _)| id == aggregate_id)
        .map(|((_, _, _, version), _)| version)
        .collect();
    versions.sort_unstable();
    versions
}

async fn await_versions(store: &InMemoryDataStore, aggregate_id: &str, versions: &[u16]) {
    for _ in 0..2_000 {
        if versions_of(store, aggregate_id) == versions {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!(
        "{aggregate_id}: {:?}, not {versions:?}",
        versions_of(store, aggregate_id)
    );
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

/// An aggregate id whose key is in `shard` under `map`.
fn id_in_shard(map: &ShardMap, shard: usize, prefix: &str) -> String {
    let probe = evento_accord::Layout::from_map(0, map.clone());
    for i in 0..100_000 {
        let id = format!("{prefix}-{i}");
        if probe.shard_of(&Key(id.clone())) == shard {
            return id;
        }
    }
    panic!("no id found for shard {shard}");
}

/// A single shard splits in two. The new shard's nodes copy only the slots
/// they gain during the transition — a key that stays is not copied — and a
/// moved key's writes then go to its new replicas, with its versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_key_range_moves_to_a_new_shard() {
    let net = InMemoryNetwork::new();
    let abc = ids(&[0, 1, 2]);
    let def = ids(&[3, 4, 5]);
    let split = ShardMap::even(vec![abc.clone()]).rebalanced(vec![abc.clone(), def.clone()]);
    let stay = id_in_shard(&split, 0, "stay");
    let moved = id_in_shard(&split, 1, "move");

    let founders: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, vec![abc.clone()], &net))
        .collect();
    founders[0]
        .node
        .write(vec![event(&stay, 1, "Opened")])
        .await
        .unwrap();
    founders[0]
        .node
        .write(vec![event(&moved, 1, "Opened")])
        .await
        .unwrap();

    let newcomers: Vec<TestNode> = def
        .iter()
        .map(|&id| spawn_node(id, vec![abc.clone()], &net))
        .collect();
    for n in &newcomers {
        n.node.begin_join();
    }

    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        founders[0]
            .node
            .change_topology(1, vec![abc.clone(), def.clone()]),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the split installs");
    assert_eq!(installed, vec![abc.clone(), def.clone()]);
    assert_eq!(
        founders[0].topology.epoch(),
        2,
        "a transition, then the split"
    );
    for n in founders.iter().chain(&newcomers) {
        await_epoch(&n.topology, 2).await;
    }

    // Each new node copied the moved key, not the one that stayed; a join
    // has nothing left to do.
    for n in &newcomers {
        assert_eq!(versions_of(&n.store, &moved), vec![1]);
        assert!(
            versions_of(&n.store, &stay).is_empty(),
            "the stayed key moved"
        );
        assert_eq!(n.node.join(NodeId(3)).await.unwrap(), 0);
    }

    // A write to the moved key now goes to the new shard.
    let outcome = newcomers[0]
        .node
        .write(vec![event(&moved, 2, "Deposited")])
        .await
        .unwrap();
    assert!(!outcome.conflict);
    for n in &newcomers {
        await_versions(&n.store, &moved, &[1, 2]).await;
    }
    // The stayed key still works on the original shard.
    let stayed = founders[0]
        .node
        .write(vec![event(&stay, 2, "Deposited")])
        .await
        .unwrap();
    assert!(!stayed.conflict);
    // The moved key's version state came with it.
    let stale = newcomers[1]
        .node
        .write(vec![event(&moved, 2, "Again")])
        .await
        .unwrap();
    assert!(stale.conflict, "the moved key's version state followed it");

    // A write on the moved key orders after every moved one: its events
    // stamp past them (the activation's clock floor).
    await_versions(&newcomers[2].store, &moved, &[1, 2]).await;
    let events = DataStore::snapshot(newcomers[2].store.as_ref())
        .await
        .unwrap();
    let stamp = |version: u16| {
        events
            .iter()
            .find(|e| e.aggregate_id == moved && e.version == version)
            .map(|e| (e.timestamp, e.timestamp_subsec))
            .unwrap()
    };
    assert!(stamp(2) > stamp(1));
}

/// Two shards split into three: the new shard takes a third of the slots,
/// from both old shards, and copies them from each; nothing else moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_shards_split_into_three_move_only_their_slots() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let after = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5]), ids(&[6, 7, 8])];
    let old = ShardMap::even(before.clone());
    let new = old.rebalanced(after.clone());
    let probe = |map: &ShardMap, id: &str| {
        evento_accord::Layout::from_map(0, map.clone()).shard_of(&Key(id.to_owned()))
    };
    // A key staying in each old shard, and one moving from each.
    let mut keys: Vec<(String, usize, usize)> = Vec::new();
    for i in 0..10_000 {
        let id = format!("acc-{i}");
        let (from, to) = (probe(&old, &id), probe(&new, &id));
        if !keys.iter().any(|(_, f, t)| (*f, *t) == (from, to)) {
            keys.push((id, from, to));
        }
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(keys.len(), 4, "{keys:?}");

    let nodes: Vec<TestNode> = (0..9)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    for n in &nodes[6..] {
        n.node.begin_join();
    }
    for (id, _, _) in &keys {
        nodes[0]
            .node
            .write(vec![event(id, 1, "Opened")])
            .await
            .unwrap();
    }
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, after.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the split installs");
    assert_eq!(installed, after);
    for n in &nodes {
        await_epoch(&n.topology, 2).await;
    }
    for (id, from, to) in &keys {
        for n in &nodes[6..] {
            let held = versions_of(&n.store, id);
            if *to == 2 {
                assert_eq!(
                    held,
                    vec![1],
                    "{id} moved from shard {from} to the new shard"
                );
            } else {
                assert!(held.is_empty(), "{id} stayed in shard {from}");
            }
        }
        let outcome = nodes[0]
            .node
            .write(vec![event(id, 2, "Bumped")])
            .await
            .unwrap();
        assert!(!outcome.conflict, "{id}: its versions came with it");
    }
}

/// An existing member moves to the other shard: it copies that shard's keys
/// during the transition, while still serving its own until the change goes
/// live, and enforces the versions it copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_existing_member_gains_a_range_with_its_versions() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let right = id_in_shard(&map, 1, "right");
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    for (id, version) in [(&left, 1), (&right, 1), (&right, 2)] {
        nodes[0]
            .node
            .write(vec![event(id, version, "Bumped")])
            .await
            .unwrap();
    }

    // Node 2 moves to the second shard, replacing 5; node 6 takes its place.
    let after = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, after.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, after);
    await_epoch(&nodes[2].topology, 2).await;
    assert!(nodes[2].node.owns_key(&Key(right.clone())));
    assert!(!nodes[2].node.owns_key(&Key(left.clone())));
    await_versions(&nodes[2].store, &right, &[1, 2]).await;
    await_versions(&nodes[6].store, &left, &[1]).await;

    let stale = nodes[2]
        .node
        .write(vec![event(&right, 2, "Again")])
        .await
        .unwrap();
    assert!(stale.conflict, "the copied versions are enforced");
    let fresh = nodes[2]
        .node
        .write(vec![event(&right, 3, "Bumped")])
        .await
        .unwrap();
    assert!(!fresh.conflict);
    for n in [3usize, 4, 2] {
        await_versions(&nodes[n].store, &right, &[1, 2, 3]).await;
    }
}

/// A node that would get back a slot it replicated before is refused: its
/// store still holds that slot's events from then, and nothing drops them.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_change_that_gives_a_node_back_its_slots_is_refused() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("node 2 moves");
    let err = nodes[0]
        .node
        .change_topology(3, vec![ids(&[0, 1, 2]), ids(&[3, 4, 6])])
        .await
        .expect_err("node 2 would regain shard 0's slots");
    assert!(err.to_string().contains("regain"), "{err:#}");
}

/// A join goes through a replica of the joiner's own shard only (#360): a
/// contact in another shard holds none of what it must copy, and its floor
/// is its own shard's.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_join_through_another_shards_replica_is_refused() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    // One replica added to the first shard installs at once, and node 6
    // joins after it.
    nodes[0]
        .node
        .change_topology(1, vec![ids(&[0, 1, 2, 6]), ids(&[3, 4, 5])])
        .await
        .unwrap();
    await_epoch(&nodes[6].topology, 1).await;
    let err = nodes[6]
        .node
        .join(NodeId(3))
        .await
        .expect_err("node 3 is in the other shard");
    assert!(err.to_string().contains("no replica"), "{err:#}");
    nodes[6]
        .node
        .join(NodeId(0))
        .await
        .expect("joins through its shard");
}
