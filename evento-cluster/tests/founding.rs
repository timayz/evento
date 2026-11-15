//! A group comes back from one node's store (#541).
//!
//! The store of one node, copied while its group served, holds the events
//! as they stood at one moment — and a journal that is that node's alone.
//! Put under every node of a group, each over an empty journal and told so
//! (`Node::found_over_store`), it is what a new group begins with: the nodes
//! journal a digest of the store as their group, start their clocks above
//! the newest event, and take part once a majority of them opened over the
//! same events. A node founded over another copy, one left out of the
//! restore and one that still runs for the old group are of another group:
//! they are told so and stop, rather than diverge.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_cluster::protocol::{Timestamp, TxnId};
use evento_cluster::{
    Clock, DataStore, FoundedApart, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, StaticTopology, Topology,
};
use evento_core::Event;
use tokio::task::JoinHandle;

const SKEW_MICROS: u64 = 20_000;

fn config() -> NodeConfig {
    NodeConfig {
        fast_timeout: Duration::from_millis(20),
        collect_timeout: Duration::from_millis(500),
        recovery_interval: Duration::from_millis(20),
        anti_entropy_interval: Duration::from_millis(25),
        recovery_timeout: Duration::from_millis(100),
        compaction_margin: Duration::from_millis(200),
        amnesia_join_timeout: Duration::from_secs(20),
        ..NodeConfig::default()
    }
}

fn clock(id: NodeId) -> Arc<HybridLogicalClock> {
    Arc::new(HybridLogicalClock::new(id).with_max_skew(SKEW_MICROS))
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

async fn await_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting until {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Every event `store` holds, sorted.
fn events_of(store: &InMemoryDataStore) -> Vec<((String, String, String, u16), ulid::Ulid)> {
    let mut events = store.committed_events();
    events.sort();
    events
}

/// A copy of `store` as a backup taken now holds it: the events, and
/// nothing of the journal.
async fn copy_of(store: &InMemoryDataStore) -> Arc<InMemoryDataStore> {
    let copy = Arc::new(InMemoryDataStore::new());
    let events = store.snapshot().await.unwrap();
    copy.apply(TxnId(Timestamp::MIN), Timestamp::MIN, events, true)
        .await
        .unwrap();
    copy
}

/// The nodes `{0..n}` of one shard over one in-memory network, each slot
/// holding the node that runs under its id now.
struct Group {
    net: Arc<InMemoryNetwork>,
    n: u64,
    nodes: Vec<Option<Node>>,
    stores: Vec<Arc<InMemoryDataStore>>,
    journals: Vec<Arc<InMemoryJournal>>,
    tasks: Vec<Arc<Mutex<Vec<JoinHandle<()>>>>>,
}

impl Group {
    fn new(n: u64) -> Self {
        Self {
            net: InMemoryNetwork::new(),
            n,
            nodes: (0..n).map(|_| None).collect(),
            stores: (0..n).map(|_| Arc::new(InMemoryDataStore::new())).collect(),
            journals: (0..n).map(|_| Arc::new(InMemoryJournal::new())).collect(),
            tasks: (0..n).map(|_| Arc::default()).collect(),
        }
    }

    /// A group of `n` nodes that began empty, running.
    async fn serving(n: u64) -> Self {
        let mut group = Self::new(n);
        for i in 0..n {
            group.open(i).await.expect("a plain open");
        }
        group
    }

    fn build(&self, i: u64) -> Node {
        let id = NodeId(i);
        Node::new(
            id,
            Arc::new(StaticTopology::new(id, (0..self.n).map(NodeId).collect()))
                as Arc<dyn Topology>,
            clock(id),
            Arc::new(self.net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&self.stores[i as usize]) as Arc<dyn DataStore>,
            Arc::clone(&self.journals[i as usize]) as Arc<dyn Journal>,
        )
        .with_config(config())
    }

    fn run(&mut self, i: u64, node: Node) {
        let inbox = self.net.register(NodeId(i));
        let mut tasks = self.tasks[i as usize].lock().unwrap();
        tasks.push(node.start(inbox));
        tasks.push(node.start_recovery());
        drop(tasks);
        self.nodes[i as usize] = Some(node);
    }

    /// Opens node `i` plainly over its journal and store, and runs it.
    async fn open(&mut self, i: u64) -> anyhow::Result<()> {
        let node = self.build(i);
        node.recover_state().await?;
        self.run(i, node);
        Ok(())
    }

    /// What a host does to found a group on node `i`: stops it, puts
    /// `store` under it with an empty journal, founds, and runs it.
    async fn found(&mut self, i: u64, store: Arc<InMemoryDataStore>) -> anyhow::Result<u64> {
        self.stop(i);
        self.stores[i as usize] = store;
        self.journals[i as usize] = Arc::new(InMemoryJournal::new());
        let node = self.build(i);
        let foundation = node.found_over_store().await?;
        self.run(i, node);
        Ok(foundation.group)
    }

    /// Aborts node `i`'s tasks: a crash.
    fn stop(&self, i: u64) {
        for task in self.tasks[i as usize].lock().unwrap().drain(..) {
            task.abort();
        }
    }

    fn node(&self, i: u64) -> &Node {
        self.nodes[i as usize]
            .as_ref()
            .expect("the node was opened")
    }

    async fn await_confirmed(&self, i: u64) {
        let node = self.node(i).clone();
        await_until("the node is confirmed", Duration::from_secs(5), || {
            node.confirmed()
        })
        .await;
    }

    async fn await_agreement(&self, nodes: &[u64]) {
        let stores: Vec<_> = nodes
            .iter()
            .map(|&i| Arc::clone(&self.stores[i as usize]))
            .collect();
        await_until("the stores agree", Duration::from_secs(5), || {
            stores
                .iter()
                .all(|store| events_of(store) == events_of(&stores[0]))
        })
        .await;
    }
}

/// A group that served some writes, and a copy of one of its nodes' stores.
async fn a_served_group() -> (Group, Arc<InMemoryDataStore>) {
    let group = Group::serving(3).await;
    for version in 1..=3 {
        group
            .node(0)
            .write(vec![event("acc", version)])
            .await
            .expect("a write to the first group");
    }
    group.await_agreement(&[0, 1, 2]).await;
    let copy = copy_of(&group.stores[1]).await;
    (group, copy)
}

/// One node's store under every node of a new group, each over an empty
/// journal: the group serves, its writes follow the stored ones — in
/// version and in date — and a founded node restarts plainly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_founded_over_one_nodes_store_serves() {
    let (_old, copy) = a_served_group().await;
    let newest = copy
        .snapshot()
        .await
        .unwrap()
        .iter()
        .map(Event::timestamp_micros)
        .max()
        .unwrap();

    let mut group = Group::new(3);
    let mut groups = Vec::new();
    for i in 0..3 {
        let founded = group.found(i, copy_of(&copy).await).await.expect("founded");
        groups.push(founded);
    }
    assert_ne!(groups[0], 0, "a founded group is not one that began empty");
    assert!(
        groups.iter().all(|&g| g == groups[0]),
        "one store, one group: {groups:?}"
    );
    for i in 0..3 {
        group.await_confirmed(i).await;
        assert_eq!(group.node(i).group(), Some(groups[0]));
    }

    let next = event("acc", 4);
    let outcome = group
        .node(2)
        .write(vec![next.clone()])
        .await
        .expect("the founded group writes");
    assert!(!outcome.conflict, "version 4 follows the stored 3");
    let stale = group
        .node(0)
        .write(vec![event("acc", 2)])
        .await
        .expect("a stale write is decided");
    assert!(stale.conflict, "the stored versions still hold");
    group.await_agreement(&[0, 1, 2]).await;
    let written = group.stores[0]
        .snapshot()
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.id == next.id)
        .expect("the write is stored");
    assert!(
        written.timestamp_micros() > newest,
        "a new write is dated above every stored one"
    );

    // The group is in the journal: the next open is a restart like any other.
    group.stop(1);
    group
        .open(1)
        .await
        .expect("a founded node restarts plainly");
    group.await_confirmed(1).await;
    assert_eq!(group.node(1).group(), Some(groups[0]));
    group
        .node(1)
        .write(vec![event("acc", 5)])
        .await
        .expect("the restarted node writes");
    group.await_agreement(&[0, 1, 2]).await;
}

/// A node founded over another copy than the two others: they confirm one
/// another and serve; it is told it is of another group, takes no write,
/// and nothing of the group's reaches its store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_founded_over_another_store_is_refused() {
    let (old, copy) = a_served_group().await;
    old.node(0)
        .write(vec![event("later", 1)])
        .await
        .expect("a write after the copy");
    old.await_agreement(&[0, 1, 2]).await;
    let other = copy_of(&old.stores[1]).await;

    let mut group = Group::new(3);
    let alike = group.found(0, copy_of(&copy).await).await.unwrap();
    let apart = group.found(2, other).await.unwrap();
    assert_ne!(alike, apart, "two stores, two groups");
    assert_eq!(group.found(1, copy_of(&copy).await).await.unwrap(), alike);

    let odd = group.node(2).clone();
    await_until(
        "the odd node hears it was founded apart",
        Duration::from_secs(5),
        || odd.founded_apart(),
    )
    .await;
    assert!(!odd.confirmed());
    assert_eq!(odd.metrics().founded_apart, 1);
    let err = odd
        .write(vec![event("acc", 4)])
        .await
        .expect_err("a node founded apart takes no write");
    assert!(err.is::<FoundedApart>(), "{err:#}");

    group.await_confirmed(0).await;
    group.await_confirmed(1).await;
    assert!(!group.node(0).founded_apart() && !group.node(1).founded_apart());
    let held = events_of(&group.stores[2]);
    group
        .node(0)
        .write(vec![event("acc", 4)])
        .await
        .expect("the two founded alike serve");
    group.await_agreement(&[0, 1]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        events_of(&group.stores[2]),
        held,
        "the odd node applied a write of a group it is not of"
    );
}

/// Two nodes are founded over a copy while the third still runs for the old
/// group, over its own journal and store, with the marks it saw from the
/// old nodes under their ids. It does not stop them — their journals are no
/// older copies of anything: the group is another — and is itself told so.
/// It comes back as a node that lost its journal does, by copying the
/// group's store, and is of the group from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_left_out_of_the_founding_is_refused_and_copies_the_group() {
    let (mut group, copy) = a_served_group().await;
    // The old group goes on past the copy: the node left out holds more.
    group
        .node(0)
        .write(vec![event("lost", 1)])
        .await
        .expect("a write after the copy");
    group.await_agreement(&[0, 1, 2]).await;

    // Both restored nodes are stopped before either is founded: one founded
    // beside two nodes of the old group would be the odd one out.
    group.stop(0);
    group.stop(1);
    let founded = group.found(0, copy_of(&copy).await).await.unwrap();
    assert_eq!(group.found(1, copy_of(&copy).await).await.unwrap(), founded);
    let straggler = group.node(2).clone();
    await_until(
        "the node left out hears it is of another group",
        Duration::from_secs(5),
        || straggler.founded_apart(),
    )
    .await;
    group.await_confirmed(0).await;
    group.await_confirmed(1).await;
    for i in 0..2 {
        assert!(
            !group.node(i).rolled_back() && !group.node(i).founded_apart(),
            "node {i} was stopped by a node of the old group"
        );
    }
    group
        .node(0)
        .write(vec![event("acc", 4)])
        .await
        .expect("the founded group serves without it");

    // The straggler's host discards its journal and its store, and opens it
    // joining.
    group.stop(2);
    group.stores[2] = Arc::new(InMemoryDataStore::new());
    group.journals[2] = Arc::new(InMemoryJournal::new());
    let node = group.build(2);
    node.recover_state_for_join().await.expect("opened joining");
    assert_eq!(node.group(), None, "it takes its contact's group");
    let inbox = group.net.register(NodeId(2));
    group.tasks[2].lock().unwrap().push(node.start(inbox));
    tokio::time::timeout(Duration::from_secs(20), node.join(NodeId(0)))
        .await
        .expect("the join completes")
        .expect("joined");
    group.tasks[2].lock().unwrap().push(node.start_recovery());
    assert_eq!(node.group(), Some(founded));
    assert_eq!(
        group.journals[2].load_foundation().await.unwrap(),
        Some(founded),
        "the group it took is journaled"
    );
    group.nodes[2] = Some(node);
    group.await_agreement(&[0, 1, 2]).await;
    assert!(
        !events_of(&group.stores[2])
            .iter()
            .any(|((_, _, id, _), _)| id == "lost"),
        "what the old group wrote after the copy came back"
    );
    group
        .node(2)
        .write(vec![event("acc", 5)])
        .await
        .expect("the node that copied the group writes");
    group.await_agreement(&[0, 1, 2]).await;
    for i in 0..3 {
        assert!(!group.node(i).founded_apart(), "node {i}");
    }
}

/// A group is founded over an empty journal only: one that holds a node's
/// votes is the copy's own, or this node's from another life.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn founding_is_refused_over_a_journal_that_holds_something() {
    let (group, _copy) = a_served_group().await;
    group.stop(0);
    let node = group.build(0);
    let err = node
        .found_over_store()
        .await
        .expect_err("a journal that holds votes founds nothing");
    assert!(format!("{err:#}").contains("empty journal"), "{err:#}");
}

/// A store whose newest event is dated past the node's clock and its skew
/// bound founds nothing: the group's writes would be dated below it, or its
/// clock run ahead of every peer's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn founding_is_refused_over_events_dated_past_the_clock() {
    let store = Arc::new(InMemoryDataStore::new());
    let mut ahead = event("acc", 1);
    ahead.timestamp = clock(NodeId(0)).now().micros / 1_000_000 + 3_600;
    store
        .apply(TxnId(Timestamp::MIN), Timestamp::MIN, vec![ahead], true)
        .await
        .unwrap();
    let mut group = Group::new(3);
    let err = group
        .found(0, store)
        .await
        .expect_err("events an hour ahead found nothing");
    assert!(format!("{err:#}").contains("check the clock"), "{err:#}");
    assert_eq!(
        group.journals[0].load_foundation().await.unwrap(),
        None,
        "nothing was journaled"
    );
}

/// A node alone founds its group and serves at once: it has nobody to ask.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_alone_founds_its_group_over_a_store() {
    let (_old, copy) = a_served_group().await;
    let mut group = Group::new(1);
    let founded = group.found(0, copy).await.expect("founded");
    assert_ne!(founded, 0);
    assert!(group.node(0).confirmed());
    let outcome = group
        .node(0)
        .write(vec![event("acc", 4)])
        .await
        .expect("it writes");
    assert!(!outcome.conflict);
}
