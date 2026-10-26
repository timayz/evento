//! M4 — moving keys between shards (#288, #360). A change that adds a shard,
//! or moves a replica to another shard, is a layout transition: the keys it
//! moves drain under the old layout, every node gaining slots copies their
//! events from a replica of the shard that held them, and only then does the
//! new layout go live. The slot table moves only the slots the change needs.
//! A node drops the slots a change takes from it once the change installs,
//! and drops what it still holds for a slot a change gives it before copying
//! it — so a change may give a node back slots it replicated before (#372).
//! The nodes a change gives slots copy them before the change is decided,
//! while writes still flow, so the copy under the fence brings only the tail
//! (#373).

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, Key, LinkFilter, Message, MessageSink, Node, NodeConfig, NodeId,
    ShardMap, Topology,
};
use evento_core::cursor::Args;
use evento_core::{Event, EventFilter, TenantFilter};

struct TestNode {
    node: Node,
    store: Arc<InMemoryDataStore>,
    journal: Arc<InMemoryJournal>,
    topology: Arc<DynamicTopology>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TestNode {
    /// Stops the node's tasks, as a crash would, keeping its store and
    /// journal for a restart.
    fn stop(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// A node over `shards` at epoch 0, running the recovery sweep the drain and
/// the copies are driven by.
fn spawn_node(id: NodeId, shards: Vec<Vec<NodeId>>, net: &Arc<InMemoryNetwork>) -> TestNode {
    spawn_node_cfg(id, shards, net, NodeConfig::default())
}

/// [`spawn_node`] with `config`.
fn spawn_node_cfg(
    id: NodeId,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
    config: NodeConfig,
) -> TestNode {
    spawn_node_over_cfg(
        id,
        shards,
        net,
        Arc::new(InMemoryDataStore::new()),
        Arc::new(InMemoryJournal::new()),
        config,
    )
}

/// The settings the pre-seed tests run with: short rounds, so a change whose
/// Paxos round is cut off gives up quickly.
fn quick() -> NodeConfig {
    NodeConfig {
        collect_timeout: Duration::from_millis(500),
        preseed_timeout: Duration::from_secs(5),
        ..NodeConfig::default()
    }
}

/// A node over `shards` at epoch 0, over `store` and `journal` — a restart,
/// when they held a node before. Not recovered: call `recover_state` before
/// `start_tasks` when the journal holds something.
fn spawn_node_over(
    id: NodeId,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
    store: Arc<InMemoryDataStore>,
    journal: Arc<InMemoryJournal>,
) -> TestNode {
    spawn_node_over_cfg(id, shards, net, store, journal, NodeConfig::default())
}

/// [`spawn_node_over`] with `config`.
fn spawn_node_over_cfg(
    id: NodeId,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
    store: Arc<InMemoryDataStore>,
    journal: Arc<InMemoryJournal>,
    config: NodeConfig,
) -> TestNode {
    let clock = Arc::new(HybridLogicalClock::new(id));
    let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
    let topology = Arc::new(DynamicTopology::new(id, 0, shards));
    let node = Node::new(
        id,
        topology.clone() as Arc<dyn Topology>,
        clock,
        sink,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::clone(&journal) as Arc<dyn Journal>,
    )
    .with_config(config);
    let inbox = net.register(id);
    let tasks = vec![node.start(inbox), node.start_recovery()];
    TestNode {
        node,
        store,
        journal,
        topology,
        tasks,
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
    // The slot node 2 left goes from its store once the change installed.
    await_versions(&nodes[2].store, &left, &[]).await;

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

/// A node gets back a slot it replicated before (#372): what it still held
/// for the slot was dropped when the change that took it installed, and it
/// drops whatever is left before copying, so the copy brings the slot's
/// whole history — the writes from after it left included — and the
/// versions it enforces are the slot's.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_node_regains_its_slots() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let right = id_in_shard(&map, 1, "right");
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    for (id, version) in [(&left, 1), (&right, 1)] {
        nodes[0]
            .node
            .write(vec![event(id, version, "Bumped")])
            .await
            .unwrap();
    }

    // Node 2 moves to the second shard, replacing 5; node 6 takes its place.
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("node 2 moves");
    await_epoch(&nodes[2].topology, 2).await;
    await_versions(&nodes[2].store, &right, &[1]).await;
    await_versions(&nodes[2].store, &left, &[]).await;
    // The slot moves on without node 2.
    nodes[0]
        .node
        .write(vec![event(&left, 2, "Bumped")])
        .await
        .unwrap();
    await_versions(&nodes[6].store, &left, &[1, 2]).await;

    // Node 2 comes back to the first shard, and node 6 goes to the second.
    let back = vec![ids(&[0, 1, 2]), ids(&[3, 4, 6])];
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(3, back.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("node 2 regains shard 0's slots");
    assert_eq!(installed, back);
    await_epoch(&nodes[2].topology, 4).await;
    assert!(nodes[2].node.owns_key(&Key(left.clone())));
    await_versions(&nodes[2].store, &left, &[1, 2]).await;
    await_versions(&nodes[2].store, &right, &[]).await;
    await_versions(&nodes[6].store, &right, &[1]).await;
    await_versions(&nodes[6].store, &left, &[]).await;

    let stale = nodes[2]
        .node
        .write(vec![event(&left, 2, "Again")])
        .await
        .unwrap();
    assert!(stale.conflict, "the regained versions are enforced");
    let fresh = nodes[2]
        .node
        .write(vec![event(&left, 3, "Bumped")])
        .await
        .unwrap();
    assert!(!fresh.conflict);
    for n in [0usize, 1, 2] {
        await_versions(&nodes[n].store, &left, &[1, 2, 3]).await;
    }
}

/// A read forwarded to a node for a key a change took from it fails, rather
/// than answering from a store that dropped the key (an empty page would
/// read as "no such events").
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_forwarded_read_of_a_key_this_node_lost_fails() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    nodes[0]
        .node
        .write(vec![event(&left, 1, "Bumped")])
        .await
        .unwrap();
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("node 2 moves");
    await_epoch(&nodes[2].topology, 2).await;
    await_versions(&nodes[2].store, &left, &[]).await;

    let mut reader = net.register(NodeId(9));
    let forward = |id| Message::ReadForward {
        id,
        aggregators: Some(vec![EventFilter::by_id_raw("test/Account", left.clone())]),
        routing_key: None,
        tenant: TenantFilter::Only(String::new()),
        args: Args::forward(10, None),
        to_micros: None,
    };
    for (to, id) in [(2u64, 1u64), (0, 2)] {
        net.sink(NodeId(9))
            .send(NodeId(to), forward(id))
            .await
            .unwrap();
    }
    let mut replies = Vec::new();
    for _ in 0..2 {
        let env = tokio::time::timeout(Duration::from_secs(5), reader.recv())
            .await
            .expect("a reply comes")
            .expect("the network is up");
        replies.push((env.from, env.message));
    }
    replies.sort_by_key(|(from, _)| from.0);
    assert!(
        matches!(&replies[0], (NodeId(0), Message::ReadReply { id: 2, .. })),
        "an owner answers the read: {:?}",
        replies[0]
    );
    match &replies[1] {
        (NodeId(2), Message::ReadFailed { id: 1, reason }) => {
            assert!(reason.contains("not an owner"), "{reason}");
        }
        other => panic!("node 2 should refuse the read: {other:?}"),
    }
}

/// A node restarted over its journal after a change took a slot from it does
/// not replay the slot's writes into its store: the journal still holds them
/// applied, the store dropped them, and only the keys the node owns under
/// the layout it restored land in the store again.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restarted_loser_does_not_replay_dropped_slots() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let right = id_in_shard(&map, 1, "right");
    let mut nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node(NodeId(id), before.clone(), &net))
        .collect();
    nodes[6].node.begin_join();
    for (id, version) in [(&left, 1), (&right, 1)] {
        nodes[0]
            .node
            .write(vec![event(id, version, "Bumped")])
            .await
            .unwrap();
    }
    await_versions(&nodes[2].store, &left, &[1]).await;
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("node 2 moves");
    await_epoch(&nodes[2].topology, 2).await;
    await_versions(&nodes[2].store, &right, &[1]).await;
    await_versions(&nodes[2].store, &left, &[]).await;
    for _ in 0..2_000 {
        if nodes[2].journal.load_slot_gc().await.unwrap() == Some(2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        nodes[2].journal.load_slot_gc().await.unwrap(),
        Some(2),
        "the drop is journaled with the epoch it ran for"
    );

    // Node 2 restarts over its store and journal.
    nodes[2].stop();
    let store = Arc::clone(&nodes[2].store);
    let journal = Arc::clone(&nodes[2].journal);
    let restarted = spawn_node_over(NodeId(2), before.clone(), &net, store, journal);
    for task in &restarted.tasks {
        task.abort();
    }
    restarted.node.recover_state().await.expect("recovers");
    assert_eq!(restarted.topology.epoch(), 2, "the layout comes back");
    assert_eq!(
        versions_of(&restarted.store, &left),
        Vec::<u16>::new(),
        "the dropped slot stays dropped after the replay"
    );
    assert_eq!(versions_of(&restarted.store, &right), vec![1]);
    nodes[2] = restarted;
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

/// A node a change gives slots copies them before the change is decided
/// (#373): while the driver's Paxos round is cut off, the gainers already
/// hold the moved keys' events and no epoch has moved; once the change goes
/// through, the copy under the fence has nothing left to bring.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_gainer_pre_seeds_before_the_change_is_decided() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let right = id_in_shard(&map, 1, "right");
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node_cfg(NodeId(id), before.clone(), &net, quick()))
        .collect();
    nodes[6].node.begin_join();
    for (id, version) in [(&left, 1), (&right, 1), (&right, 2)] {
        nodes[0]
            .node
            .write(vec![event(id, version, "Bumped")])
            .await
            .unwrap();
    }

    // Nothing the driver proposes gets through: the change is not decided.
    let no_paxos: LinkFilter = Arc::new(|from, _, message| {
        from == NodeId(0) && matches!(message, Message::ConfigPrepare { .. })
    });
    net.set_link_filter(Some(no_paxos));
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    let driver = nodes[0].node.clone();
    let to = moved.clone();
    let change = tokio::spawn(async move { driver.change_topology(1, to).await });
    // Node 2 (moving to the second shard) and node 6 (replacing it) hold the
    // slots they would gain, with no epoch decided.
    await_versions(&nodes[2].store, &right, &[1, 2]).await;
    await_versions(&nodes[6].store, &left, &[1]).await;
    assert_eq!(nodes[0].topology.epoch(), 0, "nothing decided yet");
    assert_eq!(nodes[2].topology.epoch(), 0);
    assert!(nodes[2].node.metrics().preseed_passes >= 1);
    assert_eq!(nodes[2].node.metrics().preseed_events, 2);
    assert_eq!(nodes[6].node.metrics().preseed_events, 1);

    net.set_link_filter(None);
    let installed = match tokio::time::timeout(Duration::from_secs(30), change).await {
        Ok(Ok(Ok(installed))) => installed,
        // The cut-off round gave up: proposed again, the pre-seed is done.
        _ => tokio::time::timeout(
            Duration::from_secs(30),
            nodes[0].node.change_topology(1, moved.clone()),
        )
        .await
        .expect("the change ends in bounded time")
        .expect("the change installs"),
    };
    assert_eq!(installed, moved);
    await_epoch(&nodes[2].topology, 2).await;
    await_versions(&nodes[2].store, &right, &[1, 2]).await;
    assert_eq!(
        nodes[2].node.metrics().range_copy_events,
        0,
        "the copy under the fence had nothing left to bring"
    );
    assert_eq!(nodes[6].node.metrics().range_copy_events, 0);
    let fresh = nodes[2]
        .node
        .write(vec![event(&right, 3, "Bumped")])
        .await
        .unwrap();
    assert!(!fresh.conflict, "the pre-seeded versions are enforced");
}

/// A pre-seed for a change that never goes through leaves events for slots
/// the node does not own; the next activation drops them (#373).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_pre_seed_that_never_becomes_owner_is_dropped_at_the_next_activation() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let right = id_in_shard(&map, 1, "right");
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node_cfg(NodeId(id), before.clone(), &net, quick()))
        .collect();
    nodes[6].node.begin_join();
    for (id, version) in [(&left, 1), (&right, 1)] {
        nodes[0]
            .node
            .write(vec![event(id, version, "Bumped")])
            .await
            .unwrap();
    }
    let no_paxos: LinkFilter = Arc::new(|from, _, message| {
        from == NodeId(0) && matches!(message, Message::ConfigPrepare { .. })
    });
    net.set_link_filter(Some(no_paxos));
    // Node 6 would replace node 2 in the first shard: it pre-seeds `left`.
    let driver = nodes[0].node.clone();
    let change = tokio::spawn(async move {
        driver
            .change_topology(1, vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])])
            .await
    });
    await_versions(&nodes[6].store, &left, &[1]).await;
    // The change never goes through: the driver gives up.
    change.abort();
    let _ = change.await;
    net.set_link_filter(None);

    // Another change puts node 6 in the second shard instead.
    let other = vec![ids(&[0, 1, 2]), ids(&[3, 4, 6])];
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, other.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, other);
    await_epoch(&nodes[6].topology, 2).await;
    await_versions(&nodes[6].store, &right, &[1]).await;
    await_versions(&nodes[6].store, &left, &[]).await;
}

/// A gainer that does not answer the pre-seed in time does not hold the
/// change back: it is proposed anyway, and the copy under the fence brings
/// the slots (#373).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_pre_seed_timeout_does_not_block_the_change() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let right = id_in_shard(&map, 1, "right");
    let config = NodeConfig {
        preseed_timeout: Duration::from_millis(500),
        ..quick()
    };
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node_cfg(NodeId(id), before.clone(), &net, config))
        .collect();
    nodes[6].node.begin_join();
    for version in [1, 2] {
        nodes[0]
            .node
            .write(vec![event(&right, version, "Bumped")])
            .await
            .unwrap();
    }
    // No pre-seed reaches the gainers.
    let no_preseed: LinkFilter =
        Arc::new(|_, _, message| matches!(message, Message::Preseed { .. }));
    net.set_link_filter(Some(no_preseed));
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, moved);
    assert_eq!(nodes[0].node.metrics().preseeds_timed_out, 2);
    await_epoch(&nodes[2].topology, 2).await;
    await_versions(&nodes[2].store, &right, &[1, 2]).await;
    assert_eq!(nodes[2].node.metrics().preseed_passes, 0);
    assert_eq!(
        nodes[2].node.metrics().range_copy_events,
        2,
        "the copy under the fence brought the slots"
    );
}

/// A pre-seed for a change that is not decided in time expires (#373): the
/// slots it brought in, which the node does not own, go like any other, and
/// the record stands at "dropped", so the change — should it come after all
/// — copies under the fence without dropping again.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_pre_seed_for_a_change_that_never_came_expires() {
    let net = InMemoryNetwork::new();
    let before = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let map = ShardMap::even(before.clone());
    let left = id_in_shard(&map, 0, "left");
    let config = NodeConfig {
        preseed_timeout: Duration::from_secs(1),
        ..quick()
    };
    let nodes: Vec<TestNode> = (0..7)
        .map(|id| spawn_node_cfg(NodeId(id), before.clone(), &net, config))
        .collect();
    nodes[6].node.begin_join();
    nodes[0]
        .node
        .write(vec![event(&left, 1, "Bumped")])
        .await
        .unwrap();
    let no_paxos: LinkFilter = Arc::new(|from, _, message| {
        from == NodeId(0) && matches!(message, Message::ConfigPrepare { .. })
    });
    net.set_link_filter(Some(no_paxos));
    let driver = nodes[0].node.clone();
    let change = tokio::spawn(async move {
        driver
            .change_topology(1, vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])])
            .await
    });
    await_versions(&nodes[6].store, &left, &[1]).await;
    change.abort();
    let _ = change.await;
    // The grace is the pre-seed wait plus two rounds: 2 s here.
    await_versions(&nodes[6].store, &left, &[]).await;
    assert_eq!(nodes[6].topology.epoch(), 0, "no change came");

    // The change comes after all: copied under the fence, whole.
    net.set_link_filter(None);
    let moved = vec![ids(&[0, 1, 6]), ids(&[3, 4, 2])];
    let installed = tokio::time::timeout(
        Duration::from_secs(30),
        nodes[0].node.change_topology(1, moved.clone()),
    )
    .await
    .expect("the change ends in bounded time")
    .expect("the change installs");
    assert_eq!(installed, moved);
    await_epoch(&nodes[6].topology, 2).await;
    await_versions(&nodes[6].store, &left, &[1]).await;
}
