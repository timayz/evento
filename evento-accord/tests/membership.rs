//! M4: elastic membership. A node coordinates an epoch change across the cluster
//! (`change_topology`); a node joins by bootstrapping from a peer and then
//! participates in writes — including a write that commits *during* the join; and
//! a node leaves, after which the smaller cluster keeps serving.
//!
//! A joining node `begin_join`s (buffering consensus messages), the new epoch is
//! installed cluster-wide, it syncs the committed snapshot, then replays anything
//! buffered meanwhile — so a transaction that commits across the join still lands
//! (proven via a partition that hides it from the bootstrap source). The epoch
//! change is decided by single-decree Paxos over the **current members**, so it
//! survives the coordinator failing mid-change (another node recovers the accepted
//! layout) and keeps working after the original founders have left. Range movement
//! is covered in `resharding.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AppliedEntry, DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore,
    InMemoryJournal, InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, RegionId,
    Topology,
};
use evento_core::Event;

/// A node and the handles needed to drive and inspect it.
struct TestNode {
    node: Node,
    store: Arc<InMemoryDataStore>,
    journal: Arc<InMemoryJournal>,
    topology: Arc<DynamicTopology>,
    _loop: tokio::task::JoinHandle<()>,
}

fn spawn_node(
    id: NodeId,
    epoch: u64,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
) -> TestNode {
    spawn_node_with(id, epoch, shards, net, NodeConfig::default())
}

fn spawn_node_with(
    id: NodeId,
    epoch: u64,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
    config: NodeConfig,
) -> TestNode {
    let clock = Arc::new(HybridLogicalClock::new(id));
    spawn_node_on(id, epoch, shards, net, config, clock)
}

/// [`spawn_node_with`] over a given clock.
fn spawn_node_on(
    id: NodeId,
    epoch: u64,
    shards: Vec<Vec<NodeId>>,
    net: &Arc<InMemoryNetwork>,
    config: NodeConfig,
    clock: Arc<HybridLogicalClock>,
) -> TestNode {
    let inbox = net.register(id);
    let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
    let store = Arc::new(InMemoryDataStore::new());
    let journal = Arc::new(InMemoryJournal::new());
    let topology = Arc::new(DynamicTopology::new(id, epoch, shards));
    let node = Node::new(
        id,
        topology.clone() as Arc<dyn evento_accord::Topology>,
        clock,
        sink,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::clone(&journal) as Arc<dyn Journal>,
    )
    .with_config(config);
    let _loop = node.start(inbox);
    TestNode {
        node,
        store,
        journal,
        topology,
        _loop,
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

async fn await_len(store: &InMemoryDataStore, len: usize) -> Vec<AppliedEntry> {
    for _ in 0..1000 {
        let log = store.applied_log();
        if log.len() >= len {
            return log;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("store did not reach {len} applied entries");
}

/// Waits until `store` holds at least `n` committed events (works for a node that
/// bootstrapped via snapshot, whose applied log does not replay each transaction).
async fn await_committed(store: &InMemoryDataStore, n: usize) {
    for _ in 0..1000 {
        if store.committed_events().len() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("store did not reach {n} committed events");
}

/// Waits until `topology` reaches at least `epoch` (config commits propagate
/// asynchronously after the Paxos decision).
async fn await_epoch(topology: &DynamicTopology, epoch: u64) {
    for _ in 0..1000 {
        if topology.epoch() >= epoch {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("topology did not reach epoch {epoch}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_bootstraps_and_then_participates() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    // Epoch 0: a 3-node single-shard cluster.
    let mut nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();

    // Write two events before the join.
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    nodes[0]
        .node
        .write(vec![event("acc", 2, "Deposited")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 2).await;
    }

    // A fourth node comes up (not yet a member — epoch 0 layout).
    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);
    d.node.begin_join();

    // A coordinates installing epoch 1 (which includes D) across the cluster,
    // then D bootstraps from A.
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await; // the joiner installs the new epoch

    let imported = d.node.join(NodeId(0)).await.unwrap();
    assert_eq!(imported, 2, "D must import both committed transactions");

    // D now holds the pre-join state, materialised from the bootstrap snapshot.
    assert_eq!(
        d.store.version("", "test/Account", "acc").await.unwrap(),
        2,
        "D must hold the pre-join aggregate version"
    );
    assert_eq!(
        d.store.committed_events().len(),
        2,
        "both committed events are present"
    );

    // A subsequent write coordinates over the 4-node epoch; D participates.
    nodes.push(d);
    let outcome = nodes[0]
        .node
        .write(vec![event("acc", 3, "Withdrawn")])
        .await
        .unwrap();
    assert!(!outcome.conflict);

    // Every node — including the freshly-joined D — converges on all 3 committed
    // events. (D materialised the pre-join pair from the bootstrap snapshot, so we
    // compare committed state rather than per-entry applied-log replay.)
    for n in &nodes {
        await_committed(&n.store, 3).await;
    }

    // The joined node's committed state matches the founders'.
    use std::collections::BTreeSet;
    let committed0: BTreeSet<_> = nodes[0].store.committed_events().into_iter().collect();
    let committed_d: BTreeSet<_> = nodes[3].store.committed_events().into_iter().collect();
    assert_eq!(
        committed0, committed_d,
        "joined node agrees on the committed state"
    );
}

/// A node joins a cluster whose contact has already **compacted and truncated**
/// the pre-join commands: the joiner must still converge, reconstructing the lost
/// state from the contact's data-store snapshot rather than command replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_bootstraps_from_a_compacted_contact() {
    use evento_accord::Timestamp;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    let mut nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();

    // Build up three committed events, then let everyone converge.
    for (v, name) in [(1, "Opened"), (2, "Deposited"), (3, "Withdrawn")] {
        nodes[0]
            .node
            .write(vec![event("acc", v, name)])
            .await
            .unwrap();
    }
    for n in &nodes {
        await_committed(&n.store, 3).await;
    }

    // Compact the contact (A) above every committed transaction: its in-memory
    // commands and journal records for the pre-join writes are now gone — only its
    // data store retains the materialised state. At the wall clock, not beyond:
    // a node mints above its floor, and peers refuse a `t0` past their clock's
    // skew bound.
    let high = Timestamp {
        micros: evento_accord::clock::system_micros(),
        logical: 0,
        node: NodeId(0),
    };
    nodes[0].node.compact(high).await;

    // A fourth node joins, bootstrapping from the compacted contact A.
    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);
    d.node.begin_join();
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await;
    let imported = d.node.join(NodeId(0)).await.unwrap();
    assert_eq!(
        imported, 0,
        "the contact had compacted away every command; state came via the snapshot"
    );

    // D reconstructed the pre-join state from the snapshot alone.
    await_committed(&d.store, 3).await;
    use std::collections::BTreeSet;
    let committed0: BTreeSet<_> = nodes[0].store.committed_events().into_iter().collect();
    let committed_d: BTreeSet<_> = d.store.committed_events().into_iter().collect();
    assert_eq!(
        committed0, committed_d,
        "joiner matches the compacted contact"
    );

    // And D participates in a new write over the 4-node epoch.
    nodes.push(d);
    let outcome = nodes[0]
        .node
        .write(vec![event("acc", 4, "Closed")])
        .await
        .unwrap();
    assert!(!outcome.conflict);
    for n in &nodes {
        await_committed(&n.store, 4).await;
    }
}

/// The snapshot crosses page by page: with one event per page the joiner
/// pulls as many pages as the contact holds events, installs each as it
/// lands, and ends up identical to the contact — then takes part in writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_bootstraps_page_by_page() {
    use evento_accord::Timestamp;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let config = NodeConfig {
        snapshot_page_events: 1,
        ..NodeConfig::default()
    };

    let mut nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node_with(id, 0, vec![abc.clone()], &net, config))
        .collect();
    for (v, name) in [(1, "Opened"), (2, "Deposited"), (3, "Withdrawn")] {
        nodes[0]
            .node
            .write(vec![event("acc", v, name)])
            .await
            .unwrap();
    }
    nodes[0]
        .node
        .write(vec![event("other", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_committed(&n.store, 4).await;
    }
    // At the wall clock: above every committed transaction, and a floor the
    // node's next id may sit on without its peers refusing it as future-dated.
    let high = Timestamp {
        micros: evento_accord::clock::system_micros(),
        logical: 0,
        node: NodeId(0),
    };
    nodes[0].node.compact(high).await;

    let d = spawn_node_with(NodeId(3), 0, vec![abc.clone()], &net, config);
    d.node.begin_join();
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await;
    let imported = d.node.join(NodeId(0)).await.unwrap();
    assert_eq!(imported, 0, "state came through the snapshot pages");
    assert!(
        nodes[0].node.metrics().snapshot_pages >= 4,
        "the contact served a page per event: {}",
        nodes[0].node.metrics().snapshot_pages
    );

    use std::collections::BTreeSet;
    let committed0: BTreeSet<_> = nodes[0].store.committed_events().into_iter().collect();
    let committed_d: BTreeSet<_> = d.store.committed_events().into_iter().collect();
    assert_eq!(committed0, committed_d, "joiner matches the contact");

    nodes.push(d);
    let outcome = nodes[0]
        .node
        .write(vec![event("acc", 4, "Closed")])
        .await
        .unwrap();
    assert!(!outcome.conflict);
    for n in &nodes {
        await_committed(&n.store, 5).await;
    }
}

/// A page that overlaps events already in the store would be skipped whole
/// as an idempotent re-apply, leaving a gap: a join refuses a store that is
/// not empty before it fetches anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_refuses_a_non_empty_store() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_committed(&n.store, 1).await;
    }

    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);
    let stale = evento_accord::TxnId(evento_accord::Timestamp::MIN);
    d.store
        .apply(
            stale,
            evento_accord::Timestamp::MIN,
            vec![event("old", 1, "Opened")],
            true,
        )
        .await
        .unwrap();
    d.node.begin_join();
    let err = d.node.join(NodeId(0)).await.expect_err("refused");
    assert!(
        err.to_string().contains("empty store"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        d.store.committed_events().len(),
        1,
        "nothing was fetched or installed"
    );
    assert_eq!(nodes[0].node.metrics().snapshot_pages, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_node_enforces_optimistic_concurrency() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    let mut nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);
    d.node.begin_join();
    for n in &nodes {
        n.topology.install(1, vec![abcd.clone()]);
    }
    d.topology.install(1, vec![abcd.clone()]);
    d.node.join(NodeId(0)).await.unwrap();
    nodes.push(d);

    // A stale write coordinated *by the joined node* must be rejected, proving it
    // bootstrapped the version state, not just the events.
    let err = nodes[3]
        .node
        .write(vec![event("acc", 1, "OpenedAgain")])
        .await;
    assert!(matches!(
        err,
        Ok(o) if o.conflict
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_replays_a_write_committed_during_bootstrap() {
    let net = InMemoryNetwork::new();
    // Four founding nodes so that after D joins (five nodes, majority 3) a
    // write still reaches a sound quorum with A partitioned and D buffering.
    // (The old three-node version relied on the unsound even-N "quorum" of 2
    // out of 4 that the majority fix removed.)
    let abce = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(4)];
    let abcde = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(4), NodeId(3)];

    let nodes: Vec<TestNode> = abce
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abce.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    // D begins joining (buffering) and the epoch that includes it is installed.
    let d = spawn_node(NodeId(3), 0, vec![abce.clone()], &net);
    d.node.begin_join();
    for n in &nodes {
        n.topology.install(1, vec![abcde.clone()]);
    }
    d.topology.install(1, vec![abcde.clone()]);

    // Partition node A — D's future bootstrap source — from every peer, so A
    // will miss the next write. Y then commits on the {B, C, E} majority while
    // D buffers its messages; A stays at version 1.
    net.partition(NodeId(0), NodeId(1));
    net.partition(NodeId(0), NodeId(2));
    net.partition(NodeId(0), NodeId(4));
    nodes[1]
        .node
        .write(vec![event("acc", 2, "Deposited")])
        .await
        .unwrap();
    await_len(&nodes[1].store, 2).await;
    await_len(&nodes[2].store, 2).await;
    assert_eq!(
        nodes[0].store.applied_log().len(),
        1,
        "A is partitioned from the second write"
    );

    // D bootstraps from the stale A (snapshot has only the first write), then
    // replays the buffered Y — so Y reaches D *only* through the bootstrap buffer.
    let imported = d.node.join(NodeId(0)).await.unwrap();
    assert_eq!(
        imported, 1,
        "bootstrap snapshot from A carried only the first write"
    );

    let d_log = await_len(&d.store, 2).await;
    assert_eq!(
        d_log.len(),
        2,
        "D replayed the write that committed during its bootstrap"
    );
    assert!(d_log.iter().all(|e| !e.conflict));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_leaves_and_the_cluster_continues() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    // A 4-node cluster at epoch 0.
    let nodes: Vec<TestNode> = abcd
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abcd.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    // Node D leaves: A coordinates epoch 1 with the smaller layout.
    nodes[0]
        .node
        .change_topology(1, vec![abc.clone()])
        .await
        .unwrap();
    await_epoch(&nodes[3].topology, 1).await; // the departing node learns it is out

    // The remaining three continue serving writes over the smaller quorum.
    nodes[0]
        .node
        .write(vec![event("acc", 2, "Deposited")])
        .await
        .unwrap();
    for n in &nodes[0..3] {
        await_len(&n.store, 2).await;
    }

    // The departed node is excluded — it never sees the post-leave write.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        nodes[3].store.applied_log().len(),
        1,
        "the departed node receives no further writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_change_survives_a_coordinator_crash() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();

    // A drives the config Paxos to a durable decision (a quorum accepts the new
    // layout) but crashes before committing it.
    nodes[0]
        .node
        .propose_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    net.crash(NodeId(0));

    // B recovers the interrupted change: it learns the already-accepted layout
    // and commits it. The decision is not lost with the coordinator.
    let decided = nodes[1].node.recover_topology(1).await.unwrap();
    assert_eq!(
        decided,
        vec![abcd.clone()],
        "the accepted layout is recovered"
    );

    // The surviving acceptors install epoch 1 with the recovered layout.
    await_epoch(&nodes[1].topology, 1).await;
    await_epoch(&nodes[2].topology, 1).await;
    assert_eq!(nodes[1].topology.nodes(), abcd);
    assert_eq!(nodes[2].topology.nodes(), abcd);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_recovery_of_one_epoch_converges() {
    // Liveness under contention: every surviving node drives recovery of the SAME
    // accepted-but-uncommitted epoch at once (what the per-node sweep does). Without
    // the distinguished-proposer deference they duel ballots and all fail; with it,
    // the preferred proposer commits and the rest adopt that decision.
    let net = InMemoryNetwork::new();
    let abcde = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    let nodes: Vec<TestNode> = abcde
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abcde.clone()], &net))
        .collect();

    // Node 0 makes epoch 1 durable (a quorum accepts) but crashes before committing.
    nodes[0]
        .node
        .propose_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    net.crash(NodeId(0));

    // All four survivors recover epoch 1 concurrently.
    let mut handles = Vec::new();
    for n in &nodes[1..] {
        let node = n.node.clone();
        handles.push(tokio::spawn(async move { node.recover_topology(1).await }));
    }
    for h in handles {
        let decided = tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("recovery timed out")
            .unwrap()
            .expect("recovery converges, not duels to failure");
        assert_eq!(decided, vec![abcd.clone()], "all recover the same layout");
    }

    for n in &nodes[1..] {
        await_epoch(&n.topology, 1).await;
        assert_eq!(n.topology.nodes(), abcd);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn racing_operator_changes_converge() {
    // Two operators reconfigure the same epoch to DIFFERENT layouts concurrently.
    // Single-decree Paxos chooses one; yield-on-conflict makes the loser adopt and
    // report that same committed layout rather than erroring out under a ballot duel.
    let net = InMemoryNetwork::new();
    let abcde = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let drop_e = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let drop_d = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(4)];

    let nodes: Vec<TestNode> = abcde
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abcde.clone()], &net))
        .collect();

    let n1 = nodes[1].node.clone();
    let n2 = nodes[2].node.clone();
    let x = drop_e.clone();
    let y = drop_d.clone();
    let w1 = tokio::spawn(async move { n1.change_topology(1, vec![x]).await });
    let w2 = tokio::spawn(async move { n2.change_topology(1, vec![y]).await });

    let d1 = tokio::time::timeout(Duration::from_secs(10), w1)
        .await
        .expect("w1 timed out")
        .unwrap()
        .expect("operator change 1 converges");
    let d2 = tokio::time::timeout(Duration::from_secs(10), w2)
        .await
        .expect("w2 timed out")
        .unwrap()
        .expect("operator change 2 converges");
    assert_eq!(d1, d2, "both operators converge on the one chosen layout");
    assert!(
        d1 == vec![drop_e.clone()] || d1 == vec![drop_d.clone()],
        "the chosen layout is one of the two proposed: {d1:?}"
    );

    for n in &nodes {
        await_epoch(&n.topology, 1).await;
        assert_eq!(
            n.topology.nodes(),
            d1[0],
            "every node installs the chosen layout"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_changes_on_one_node_converge() {
    // Two changes for the same epoch on ONE node: each proposal hears its
    // own replies, so neither starves the other into a quorum timeout.
    let net = InMemoryNetwork::new();
    let abcde = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let drop_e = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let drop_d = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(4)];

    let nodes: Vec<TestNode> = abcde
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abcde.clone()], &net))
        .collect();

    let a = nodes[1].node.clone();
    let b = nodes[1].node.clone();
    let x = drop_e.clone();
    let y = drop_d.clone();
    let w1 = tokio::spawn(async move { a.change_topology(1, vec![x]).await });
    let w2 = tokio::spawn(async move { b.change_topology(1, vec![y]).await });

    let d1 = tokio::time::timeout(Duration::from_secs(10), w1)
        .await
        .expect("w1 timed out")
        .unwrap()
        .expect("change 1 converges");
    let d2 = tokio::time::timeout(Duration::from_secs(10), w2)
        .await
        .expect("w2 timed out")
        .unwrap()
        .expect("change 2 converges");
    assert_eq!(d1, d2, "both proposals report the one chosen layout");
    assert!(
        d1 == vec![drop_e.clone()] || d1 == vec![drop_d.clone()],
        "the chosen layout is one of the two proposed: {d1:?}"
    );
    for n in &nodes {
        await_epoch(&n.topology, 1).await;
        assert_eq!(n.topology.nodes(), d1[0], "every node installs it");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn config_changes_outlive_the_founders() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcde = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let cde = vec![NodeId(2), NodeId(3), NodeId(4)];
    let cdef = vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)];

    // Founders A, B, C. D, E, F are spawned but not yet members.
    let all: Vec<TestNode> = (0..6)
        .map(|id| spawn_node(NodeId(id), 0, vec![abc.clone()], &net))
        .collect();

    // Grow to {A,B,C,D,E} (acceptors = the founders {A,B,C}) ...
    all[0]
        .node
        .change_topology(1, vec![abcde.clone()])
        .await
        .unwrap();
    for n in &all[0..5] {
        await_epoch(&n.topology, 1).await;
    }

    // ... then to {C,D,E}, removing founders A and B. This change is decided by
    // the epoch-1 members {A,B,C,D,E} (a majority of 3), not just the founders.
    all[0]
        .node
        .change_topology(2, vec![cde.clone()])
        .await
        .unwrap();
    for n in &all[2..5] {
        await_epoch(&n.topology, 2).await;
    }

    // A majority of the *founders* is now gone.
    net.crash(NodeId(0));
    net.crash(NodeId(1));

    // C can still reconfigure (add F): the acceptor set has evolved to the
    // current members {C,D,E}, so the dead founders no longer matter. With a
    // fixed founder acceptor set this would deadlock.
    all[2]
        .node
        .change_topology(3, vec![cdef.clone()])
        .await
        .expect("reconfiguration succeeds without the founders");
    for n in &all[2..6] {
        await_epoch(&n.topology, 3).await;
    }
    assert_eq!(
        all[5].topology.nodes(),
        cdef,
        "F joined under the new epoch"
    );
}

/// A node that comes up at a stale epoch and receives a *later* `ConfigCommit`
/// **replays the missed epochs in order** via the metadata-log catch-up, rather
/// than skipping straight to the latest layout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_log_replays_missed_epochs() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];

    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();
    // Node D starts at epoch 0 and is NOT in epoch 1, so it never sees commit 1.
    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);

    // Epoch 1 stays within {A,B,C} — D misses it entirely.
    nodes[0]
        .node
        .change_topology(1, vec![abc.clone()])
        .await
        .unwrap();
    for n in &nodes {
        await_epoch(&n.topology, 1).await;
    }
    assert_eq!(d.topology.epoch(), 0, "D missed epoch 1");

    // Epoch 2 adds D. The commit for epoch 2 reveals the gap (D is at 0), so D pulls
    // the missed epoch 1 and installs 1 then 2 — contiguously, never skipping.
    nodes[0]
        .node
        .change_topology(2, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 2).await;
    assert_eq!(
        d.topology.nodes(),
        abcd,
        "D caught up to the epoch-2 layout via metadata-log replay"
    );
}

/// The metadata log is durable: after a restart from its journal, a node restores
/// the committed topology sequence and re-installs the contiguous prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_log_survives_restart() {
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abc = ids.clone();
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let abc_only = vec![NodeId(0), NodeId(1)];

    // Build nodes keeping the journal handles, so node 0 can be rebuilt over its
    // durable journal (mirrors tests/restart.rs).
    let journals: Vec<Arc<InMemoryJournal>> =
        (0..3).map(|_| Arc::new(InMemoryJournal::new())).collect();
    let mut topos: Vec<Arc<DynamicTopology>> = Vec::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let topology = Arc::new(DynamicTopology::new(id, 0, vec![abc.clone()]));
        let node = Node::new(
            id,
            topology.clone() as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journals[id.0 as usize]) as Arc<dyn Journal>,
        );
        tasks.push(node.start(inbox));
        topos.push(topology);
        nodes.push(node);
    }

    // Drive three epoch changes; node 0 records each in its durable journal.
    nodes[0]
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    nodes[0]
        .change_topology(2, vec![abc.clone()])
        .await
        .unwrap();
    nodes[0]
        .change_topology(3, vec![abc_only.clone()])
        .await
        .unwrap();
    await_epoch(&topos[0], 3).await;

    // Restart node 0: tear it down (losing in-memory state) and rebuild over the
    // SAME durable journal with a fresh topology at epoch 0.
    tasks[0].abort();
    let fresh_topo = Arc::new(DynamicTopology::new(NodeId(0), 0, vec![abc.clone()]));
    let restarted = Node::new(
        NodeId(0),
        fresh_topo.clone() as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::clone(&journals[0]) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.unwrap();

    assert_eq!(
        fresh_topo.epoch(),
        3,
        "the restarted node re-installs the committed metadata log up to epoch 3"
    );
    assert_eq!(
        fresh_topo.nodes(),
        abc_only,
        "and ends at the epoch-3 layout"
    );
}

/// One-way latency between two same-region nodes (a local hop).
const INTRA: Duration = Duration::from_millis(1);
/// One-way latency between regions — well above the 50 ms fast-path timeout.
const CROSS: Duration = Duration::from_millis(200);

/// `DynamicTopology` derives its fast-path electorate from the agreed layout +
/// static region tags, so a region-local coordinator commits in one local
/// round-trip — and the property **survives an epoch change** (the electorate is
/// re-derived from the new layout, nothing extra crosses consensus).
#[tokio::test(start_paused = true)]
async fn dynamic_region_electorate_gives_a_local_fast_path_across_epochs() {
    let ids: Vec<NodeId> = (0..5).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    // Region A = {0,1,2}, Region B = {3,4}. The same map on every node.
    let region_map: HashMap<NodeId, RegionId> = HashMap::from([
        (NodeId(0), 0),
        (NodeId(1), 0),
        (NodeId(2), 0),
        (NodeId(3), 1),
        (NodeId(4), 1),
    ]);
    let shard = vec![ids.clone()];

    let mut nodes = Vec::new();
    let mut topos = Vec::new();
    let mut loops = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let clock = Arc::new(HybridLogicalClock::new(id));
        let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
        let store = Arc::new(InMemoryDataStore::new());
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let topology =
            Arc::new(DynamicTopology::new(id, 0, shard.clone()).with_regions(region_map.clone()));
        let node = Node::new(
            id,
            topology.clone() as Arc<dyn Topology>,
            clock,
            sink,
            Arc::clone(&store) as Arc<dyn DataStore>,
            journal,
        );
        loops.push(node.start(inbox));
        loops.push(node.start_recovery());
        topos.push(topology);
        nodes.push(node);
    }

    // Latency matrix: cheap within a region, expensive across.
    for &a in &[0u64, 1, 2] {
        for &b in &[3u64, 4] {
            net.set_link_latency(NodeId(a), NodeId(b), CROSS);
        }
    }
    net.set_link_latency(NodeId(0), NodeId(1), INTRA);
    net.set_link_latency(NodeId(0), NodeId(2), INTRA);
    net.set_link_latency(NodeId(1), NodeId(2), INTRA);
    net.set_link_latency(NodeId(3), NodeId(4), INTRA);

    // Region-A coordinator: the derived electorate {0,1,2} forms a fast quorum
    // over local links, so it commits well before a region-B reply could arrive.
    let start = tokio::time::Instant::now();
    nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("write commits");
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "epoch 0: expected a local fast path, took {:?}",
        start.elapsed()
    );
    assert_eq!(nodes[0].metrics().fast_path, 1, "epoch 0 fast path");

    // Bump the epoch (config-Paxos commits a new layout). The electorate is a pure
    // function of the installed layout, so it re-derives to {0,1,2} again.
    nodes[0]
        .change_topology(1, vec![ids.clone()])
        .await
        .expect("epoch change commits");
    for t in &topos {
        await_epoch(t, 1).await;
    }

    // A region-A write still takes the local fast path after the epoch change.
    let start = tokio::time::Instant::now();
    nodes[0]
        .write(vec![event("acc", 2, "Bumped")])
        .await
        .expect("post-epoch write commits");
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "epoch 1: expected a local fast path, took {:?}",
        start.elapsed()
    );
    assert_eq!(
        nodes[0].metrics().fast_path,
        2,
        "epoch 1 fast path persists"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicated_config_promise_never_completes_a_quorum() {
    // Five acceptors, node 0 real. Acceptor 1's promise (and accept) is
    // delivered twice, the other three stay silent: with node 0's own, that is
    // two distinct acceptors — not the majority of five an epoch's decision
    // must rest on.
    use evento_accord::Message;
    let net = InMemoryNetwork::new();
    let members: Vec<NodeId> = (0..5).map(NodeId).collect();
    let inbox = net.register(NodeId(0));
    let topology = Arc::new(DynamicTopology::new(NodeId(0), 1, vec![members.clone()]));
    let node = Node::new(
        NodeId(0),
        Arc::clone(&topology) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(NodeConfig {
        collect_timeout: Duration::from_millis(300),
        ..NodeConfig::default()
    });
    let _loop = node.start(inbox);

    let mut peers = Vec::new();
    for id in 1..5u64 {
        let mut inbox = net.register(NodeId(id));
        let sink = net.sink(NodeId(id));
        peers.push(tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                if id != 1 {
                    continue;
                }
                let reply = match env.message {
                    Message::ConfigPrepare { epoch, ballot } => Message::ConfigPromise {
                        epoch,
                        ballot,
                        accepted_ballot: evento_accord::Ballot(evento_accord::Timestamp::MIN),
                        accepted_layout: None,
                    },
                    Message::ConfigAccept { epoch, ballot, .. } => {
                        Message::ConfigAccepted { epoch, ballot }
                    }
                    _ => continue,
                };
                let _ = sink.send(env.from, reply.clone()).await;
                let _ = sink.send(env.from, reply).await;
            }
        }));
    }

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        node.change_topology(2, vec![members[..3].to_vec()]),
    )
    .await
    .expect("the change gives up in bounded time");
    assert!(
        result.is_err(),
        "two distinct acceptors must not decide an epoch: {result:?}"
    );
    assert_eq!(topology.epoch(), 1, "nothing was installed");
}

/// A layout that would break the cluster is refused before any vote: no shards
/// (every key's shard index divides by zero), an empty shard (its keys have no
/// replicas, so they never commit), a node in two shards (ownership is
/// ambiguous) or twice in one (it would count twice towards a quorum). Nothing
/// is installed and the cluster keeps serving, then takes a valid change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_invalid_layout_is_refused_and_the_cluster_keeps_serving() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();

    for invalid in [
        vec![],
        vec![abc.clone(), vec![]],
        vec![vec![NodeId(0), NodeId(1)], vec![NodeId(1), NodeId(2)]],
        vec![vec![NodeId(0), NodeId(0), NodeId(1)]],
    ] {
        let result = nodes[0].node.change_topology(1, invalid.clone()).await;
        assert!(result.is_err(), "{invalid:?} must be refused: {result:?}");
        let result = nodes[0].node.propose_topology(1, invalid.clone()).await;
        assert!(result.is_err(), "{invalid:?} must be refused: {result:?}");
    }
    for n in &nodes {
        assert_eq!(n.topology.epoch(), 0, "nothing was installed");
    }

    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }
    nodes[0]
        .node
        .change_topology(1, vec![abc.clone()])
        .await
        .unwrap();
    for n in &nodes {
        await_epoch(&n.topology, 1).await;
    }
}

/// A commit of an invalid layout arriving off the wire (a forged or corrupt
/// frame, or an older proposer) is neither installed nor persisted. Persisting
/// it would be worse than ignoring it: every journal keeps the first entry per
/// epoch, so it would shut out the epoch's real layout for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_of_an_invalid_layout_is_neither_installed_nor_persisted() {
    use evento_accord::Message;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();

    let forger = net.sink(NodeId(9));
    forger
        .send(
            NodeId(0),
            Message::ConfigCommit {
                epoch: 1,
                layout: vec![],
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        nodes[0].topology.epoch(),
        0,
        "the invalid layout was installed"
    );
    assert!(
        nodes[0].journal.load_metadata().await.unwrap().is_empty(),
        "the invalid layout was persisted"
    );

    // The node still serves, and the epoch's real layout still lands.
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }
    forger
        .send(
            NodeId(0),
            Message::ConfigCommit {
                epoch: 1,
                layout: vec![abc.clone()],
            },
        )
        .await
        .unwrap();
    await_epoch(&nodes[0].topology, 1).await;
}

/// A node still joining holds only part of its store, so it refuses a
/// forwarded read rather than answer from it; once joined, it serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_refuses_a_forwarded_read_until_it_has_joined() {
    use evento_core::cursor::Args;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    let d = spawn_node(NodeId(3), 0, vec![abc.clone()], &net);
    d.node.begin_join();
    let err = nodes[0]
        .node
        .forward_read(
            NodeId(3),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await
        .expect_err("a joining node served a read from a partial store");
    assert!(format!("{err:#}").contains("bootstrapping"), "{err:#}");

    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await;
    d.node.join(NodeId(0)).await.unwrap();
    nodes[0]
        .node
        .forward_read(
            NodeId(3),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await
        .expect("a joined node serves reads");
}

/// A node told to rejoin takes no part until its host rejoins it, but it
/// refuses a forwarded read out loud: its requester fails at once instead of
/// waiting out `collect_timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_told_to_rejoin_refuses_a_forwarded_read_at_once() {
    use evento_accord::{Message, Timestamp};
    use evento_core::cursor::Args;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(30),
        ..NodeConfig::default()
    };
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node_with(id, 0, vec![abc.clone()], &net, config))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    // A bound above anything it holds: it cannot answer with a report.
    net.sink(NodeId(1))
        .send(
            NodeId(2),
            Message::Rejoin {
                above: Timestamp {
                    micros: u64::MAX,
                    logical: u32::MAX,
                    node: NodeId(u64::MAX),
                },
            },
        )
        .await
        .unwrap();
    for _ in 0..1000 {
        if nodes[2].node.rejoin_required() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(nodes[2].node.rejoin_required());

    let started = std::time::Instant::now();
    let err = nodes[0]
        .node
        .forward_read(
            NodeId(2),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await
        .expect_err("a node told to rejoin served a read");
    assert!(format!("{err:#}").contains("rejoin"), "{err:#}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the read waited out collect_timeout: {:?}",
        started.elapsed()
    );
}

/// A node still joining does not answer a read barrier's probe: its conflict
/// graph lacks the commands the join is importing. A barrier that needs it
/// for a quorum fails; once it has joined, it counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_does_not_count_toward_a_read_barrier() {
    use evento_accord::Key;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let config = NodeConfig {
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    };
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node_with(id, 0, vec![abc.clone()], &net, config))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }

    let d = spawn_node_with(NodeId(3), 0, vec![abc.clone()], &net, config);
    d.node.begin_join();
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    for n in &nodes {
        await_epoch(&n.topology, 1).await;
    }
    await_epoch(&d.topology, 1).await;

    // Four replicas, a slow quorum of three: without node 1, node 0 needs
    // the joiner's answer.
    net.partition(NodeId(0), NodeId(1));
    let err = nodes[0]
        .node
        .read_barrier(Key("acc".into()))
        .await
        .expect_err("a joining node's probe counted toward the barrier");
    assert!(format!("{err:#}").contains("quorum not reached"), "{err:#}");

    d.node.join(NodeId(0)).await.unwrap();
    nodes[0]
        .node
        .read_barrier(Key("acc".into()))
        .await
        .expect("a joined node counts toward the barrier");
}

/// Asks `node` for anti-entropy as `probe` would, and returns the coverage it
/// answers with: what a requester records as proof `node` holds everything
/// it applied below.
async fn coverage_of(
    net: &Arc<InMemoryNetwork>,
    probe: &mut tokio::sync::mpsc::Receiver<evento_accord::Envelope>,
    node: NodeId,
) -> evento_accord::Timestamp {
    use evento_accord::Message;

    net.sink(NodeId(9))
        .send(
            node,
            Message::SyncRequest {
                id: 1,
                snapshot: false,
                known: None,
            },
        )
        .await
        .unwrap();
    loop {
        let env = tokio::time::timeout(Duration::from_secs(5), probe.recv())
            .await
            .expect("no anti-entropy reply")
            .expect("probe inbox closed");
        if let Message::SyncData { coverage, .. } = env.message {
            return coverage;
        }
    }
}

/// A freshly joined node holds provably only what its contact vouched for:
/// a transaction committed before its inbox started, not yet applied by the
/// contact when it vouched, is in neither the snapshot nor the replayed
/// inbox. The coverage it hands an anti-entropy requester stays at the vouch
/// until its own anti-entropy has covered the join, then advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_node_vouches_for_no_more_than_its_join_proved() {
    use evento_accord::clock::system_micros;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(1),
        ..NodeConfig::default()
    };
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node_with(id, 0, vec![abc.clone()], &net, config))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }
    let mut probe = net.register(NodeId(9));

    // No anti-entropy has run: the contact can vouch for nothing yet.
    let before_join = system_micros();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let d = spawn_node_with(NodeId(3), 0, vec![abc.clone()], &net, config);
    d.node.begin_join();
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await;
    d.node.join(NodeId(0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let coverage = coverage_of(&net, &mut probe, NodeId(3)).await;
    assert!(
        coverage.micros < before_join,
        "a joined node vouched past its join proof: {coverage:?}"
    );

    // Its own anti-entropy covers the join: the cap lifts.
    let joined = system_micros();
    for _ in 0..200 {
        d.node.sweep().await;
        if coverage_of(&net, &mut probe, NodeId(3)).await.micros > joined {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("a joined node's coverage never advanced past its join");
}

/// A node whose wall clock lags its group by more than the compaction margin
/// still mints above the group's compaction floor: a transaction below it
/// would count as applied at a replica that never saw it. A joiner adopts its
/// contact's floor, and its first write is minted above it and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_with_a_lagging_clock_mints_above_the_floor() {
    use evento_accord::clock::system_micros;
    use evento_accord::Timestamp;

    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let abcd = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
    let nodes: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, 0, vec![abc.clone()], &net))
        .collect();
    nodes[0]
        .node
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    for n in &nodes {
        await_len(&n.store, 1).await;
    }
    let floor = Timestamp {
        micros: system_micros() + 10_000,
        logical: 0,
        node: NodeId(0),
    };
    for n in &nodes {
        n.node.compact(floor).await;
    }

    let lag = 60_000_000;
    let d = spawn_node_on(
        NodeId(3),
        0,
        vec![abc.clone()],
        &net,
        NodeConfig::default(),
        Arc::new(HybridLogicalClock::with_physical(NodeId(3), move || {
            system_micros() - lag
        })),
    );
    d.node.begin_join();
    nodes[0]
        .node
        .change_topology(1, vec![abcd.clone()])
        .await
        .unwrap();
    await_epoch(&d.topology, 1).await;
    d.node.join(NodeId(0)).await.unwrap();

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        d.node.write(vec![event("acc", 2, "Deposited")]),
    )
    .await
    .expect("the write timed out")
    .expect("the write commits");
    assert!(
        outcome.txn.0 > floor,
        "minted {:?} below the floor {floor:?}",
        outcome.txn
    );
    for n in &nodes {
        await_len(&n.store, 2).await;
    }
}
