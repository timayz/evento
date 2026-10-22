//! Storage-fault coverage for the durability path: a node whose journal cannot
//! `flush` (fsync) must **withhold its ack**, so a write is reported committed only
//! if a *durable* quorum persisted it. Modelled with `FaultJournal` — a staged record
//! becomes durable only after a successful flush, so an un-flushed record is exactly
//! the data a crash would lose.
//!
//! This is the invariant behind the flush-failure gate: **acked ⇒ durable on f+1
//! nodes**. Without the gate, replicas would ack despite a failed fsync and a write
//! could be "committed" while persisted on fewer than a quorum — lost if those nodes
//! restart. The test asserts the invariant directly (no restart needed): if a write
//! returns committed, at least a slow quorum holds it durably.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::journal::FaultJournal;
use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryNetwork, Journal,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology, Timestamp, Topology, TxnId,
};
use evento_core::Event;

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

struct Cluster {
    ids: Vec<NodeId>,
    net: Arc<InMemoryNetwork>,
    nodes: Vec<Node>,
    journals: Vec<Arc<FaultJournal>>,
    stores: Vec<Arc<InMemoryDataStore>>,
    topologies: Vec<Arc<dyn Topology>>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

fn start(n: u64) -> Cluster {
    start_with(n, true, NodeConfig::default())
}

/// `sweep: false` leaves the recovery sweep (which compacts on every pass)
/// unstarted, so a test owns every [`Node::compact`] call.
fn start_with(n: u64, sweep: bool, config: NodeConfig) -> Cluster {
    build(n, sweep, config, |id, ids| {
        Arc::new(StaticTopology::new(id, ids.to_vec())) as Arc<dyn Topology>
    })
}

/// A single-shard cluster on a [`DynamicTopology`], sweeps started, so it
/// can change epoch and catch up on the metadata log by itself.
fn start_dynamic(n: u64) -> Cluster {
    build(n, true, NodeConfig::default(), |id, ids| {
        Arc::new(DynamicTopology::new(id, 0, vec![ids.to_vec()])) as Arc<dyn Topology>
    })
}

fn build(
    n: u64,
    sweep: bool,
    config: NodeConfig,
    topology: impl Fn(NodeId, &[NodeId]) -> Arc<dyn Topology>,
) -> Cluster {
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let (mut nodes, mut journals, mut stores, mut loops) = (vec![], vec![], vec![], vec![]);
    let mut topologies = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let journal = Arc::new(FaultJournal::new());
        let store = Arc::new(InMemoryDataStore::new());
        let topology = topology(id, &ids);
        topologies.push(Arc::clone(&topology));
        let node = Node::new(
            id,
            topology,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        )
        // A write that cannot reach a durable quorum now fails only at
        // `collect_timeout` (the recovery sweep no longer hijacks the node's
        // own in-flight write, which used to abort it early by accident) —
        // keep the timeout below this test's 3s write budget.
        .with_config(NodeConfig {
            collect_timeout: Duration::from_secs(1),
            ..config
        });
        loops.push(node.start(inbox));
        if sweep {
            loops.push(node.start_recovery());
        }
        nodes.push(node);
        journals.push(journal);
        stores.push(store);
    }
    Cluster {
        ids,
        net,
        nodes,
        journals,
        stores,
        topologies,
        _loops: loops,
    }
}

async fn await_version(store: &InMemoryDataStore, agg: &str, want: u16) {
    for _ in 0..500 {
        if store.version("test/Account", agg).await.unwrap_or(0) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("store did not reach version {want}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_write_is_always_durable_on_a_quorum() {
    // N=3 (f=1), so a slow quorum is 2.
    let cluster = start(3);
    let slow_quorum = 2;

    // Sanity (positive control): with healthy journals a write commits and is durable
    // on all three nodes.
    let outcome = cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("healthy write commits");
    assert!(!outcome.conflict);
    for n in &cluster.stores {
        await_version(n, "acc", 1).await;
    }
    let durable = count_durable(&cluster.journals, outcome.txn).await;
    assert_eq!(durable, 3, "a healthy commit is durable everywhere");

    // Now fail fsync on a quorum (nodes 0 and 1 — node 0 is also the coordinator).
    cluster.journals[0].set_failing(true);
    cluster.journals[1].set_failing(true);

    // Attempt a write. With the durability gate, nodes 0 and 1 withhold their acks
    // (they could not persist), so the coordinator cannot reach a durable quorum and
    // the write must NOT report a durable commit.
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        cluster.nodes[0].write(vec![event("acc", 2, "Deposited")]),
    )
    .await
    .expect("write did not hang");

    if let Ok(outcome) = result {
        if !outcome.conflict {
            // If the cluster *did* report this write committed, the invariant is that
            // a slow quorum must hold it durably — otherwise a restart of the faulted
            // nodes would lose an acked write.
            let durable = count_durable(&cluster.journals, outcome.txn).await;
            assert!(
                durable >= slow_quorum,
                "acked write durable on only {durable} node(s) (< quorum {slow_quorum}) \
                 — a flush-failing node acked a write it did not persist"
            );
        }
    }
    // (A write that fails to commit under a flush-failing quorum is the correct,
    // safe outcome — the node refused to ack what it could not persist.)
}

/// A compaction watermark above every transaction committed so far (`offset`
/// orders several such watermarks taken in a row).
fn above_now(offset: u64) -> Timestamp {
    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    Timestamp {
        micros: now_micros + 10_000 + offset,
        logical: 0,
        node: NodeId(0),
    }
}

/// Journal truncation unpaced, so a test isolates the watermark check.
fn unpaced() -> NodeConfig {
    NodeConfig {
        truncate_interval: Duration::ZERO,
        ..NodeConfig::default()
    }
}

/// Writes one event and waits for every store to hold it.
async fn write_one(cluster: &Cluster, version: u16) {
    cluster.nodes[0]
        .write(vec![event("acc", version, "Opened")])
        .await
        .expect("healthy write commits");
    for s in &cluster.stores {
        await_version(s, "acc", version).await;
    }
}

/// The sweep compacts on every pass, and a journal's `truncate` is a write
/// transaction even when it has nothing to delete — so a node asks for one only
/// when the watermark moved past what the journal already persisted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unchanged_watermark_does_not_touch_the_journal() {
    let cluster = start_with(3, false, unpaced());
    write_one(&cluster, 1).await;

    let w = above_now(0);
    cluster.nodes[1].compact(w).await;
    assert_eq!(cluster.journals[1].truncates(), 1);
    assert_eq!(
        cluster.journals[1].load_watermark().await.unwrap(),
        Some(w),
        "the first compaction persists the watermark"
    );

    for _ in 0..5 {
        cluster.nodes[1].compact(w).await;
    }
    assert_eq!(
        cluster.journals[1].truncates(),
        1,
        "compacting at an unchanged watermark must not truncate the journal again"
    );

    // A restart recovers the persisted watermark, so it does not re-persist it
    // either.
    let id = cluster.ids[1];
    let restarted = Node::new(
        id,
        Arc::new(StaticTopology::new(id, cluster.ids.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(cluster.net.sink(id)) as Arc<dyn MessageSink>,
        Arc::clone(&cluster.stores[1]) as Arc<dyn DataStore>,
        Arc::clone(&cluster.journals[1]) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.expect("recovers");
    restarted.compact(w).await;
    assert_eq!(
        cluster.journals[1].truncates(),
        1,
        "a restarted node does not re-persist the watermark it recovered"
    );
}

/// A truncate that failed is retried on the next compaction even if the
/// watermark did not move — tracked against what the journal persisted, not
/// against the replica's previous watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_truncate_is_retried_at_the_same_watermark() {
    let cluster = start_with(3, false, unpaced());
    write_one(&cluster, 1).await;

    let w = above_now(0);
    cluster.journals[1].set_truncate_failing(true);
    cluster.nodes[1].compact(w).await;
    assert_eq!(cluster.journals[1].truncates(), 1);
    assert_eq!(
        cluster.journals[1].load_watermark().await.unwrap(),
        None,
        "the failed truncate persisted nothing"
    );

    cluster.journals[1].set_truncate_failing(false);
    cluster.nodes[1].compact(w).await;
    assert_eq!(
        cluster.journals[1].truncates(),
        2,
        "the same watermark is retried once the journal is back"
    );
    assert_eq!(cluster.journals[1].load_watermark().await.unwrap(), Some(w));
}

/// Under steady writes the watermark moves on every sweep, so "moved at all"
/// alone would still truncate every pass: the journal is asked at most once per
/// `truncate_interval`, and the watermark that was skipped lands on the next
/// due truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncation_is_paced_by_the_interval() {
    let interval = Duration::from_millis(300);
    let cluster = start_with(
        3,
        false,
        NodeConfig {
            truncate_interval: interval,
            ..NodeConfig::default()
        },
    );
    write_one(&cluster, 1).await;

    let w1 = above_now(0);
    cluster.nodes[1].compact(w1).await;
    assert_eq!(cluster.journals[1].truncates(), 1);

    // The watermark moved, but the interval has not elapsed.
    let w2 = above_now(1);
    cluster.nodes[1].compact(w2).await;
    assert_eq!(
        cluster.journals[1].truncates(),
        1,
        "a watermark that moved within the interval is not truncated yet"
    );
    assert_eq!(
        cluster.journals[1].load_watermark().await.unwrap(),
        Some(w1)
    );

    tokio::time::sleep(interval).await;
    let w3 = above_now(2);
    cluster.nodes[1].compact(w3).await;
    assert_eq!(
        cluster.journals[1].truncates(),
        2,
        "due again after the interval"
    );
    assert_eq!(
        cluster.journals[1].load_watermark().await.unwrap(),
        Some(w3),
        "the due truncate carries the current watermark"
    );
}

async fn count_durable(journals: &[Arc<FaultJournal>], txn: TxnId) -> usize {
    let mut n = 0;
    for j in journals {
        if j.durable_has(txn).await {
            n += 1;
        }
    }
    n
}

/// A config acceptor whose journal cannot persist its state must not vote: a
/// promise or an accept is a durable commitment that a restart restores, so
/// one sent without it could be forgotten, and two proposers could then
/// decide different layouts for one epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_vote_the_journal_did_not_persist_is_not_sent() {
    let cluster = start_with(3, false, NodeConfig::default());
    let layout = vec![cluster.ids.clone()];

    // Two of three acceptors cannot persist a promise: no prepare quorum.
    cluster.journals[1].set_acceptor_failing(true);
    cluster.journals[2].set_acceptor_failing(true);
    let result = cluster.nodes[0].propose_topology(1, layout.clone()).await;
    assert!(
        result.is_err(),
        "decided on promises that were never persisted: {result:?}"
    );
    for journal in &cluster.journals[1..] {
        assert!(journal.load_acceptors().await.unwrap().is_empty());
    }
    cluster.journals[1].set_acceptor_failing(false);
    cluster.journals[2].set_acceptor_failing(false);

    // Promises persist but accepts do not: no accept quorum.
    cluster.journals[1].set_accept_failing(true);
    cluster.journals[2].set_accept_failing(true);
    let result = cluster.nodes[0].propose_topology(2, layout.clone()).await;
    assert!(
        result.is_err(),
        "decided on accepts that were never persisted: {result:?}"
    );
    cluster.journals[1].set_accept_failing(false);
    cluster.journals[2].set_accept_failing(false);

    // Healthy journals: the change is decided.
    cluster.nodes[0]
        .propose_topology(3, layout)
        .await
        .expect("acceptors that persist their votes decide");
}

/// Waits until `topology` is at `epoch` or later.
async fn await_epoch(topology: &dyn Topology, epoch: u64) {
    for _ in 0..500 {
        if topology.epoch() >= epoch {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("topology did not reach epoch {epoch}");
}

/// A node whose journal cannot persist a decided epoch does not install it.
/// Installed, it would act on the epoch, yet fall back to the one before after
/// a restart; and if the epoch retired every member of the one before, the
/// peers it would fetch the entry from are gone. Once the journal recovers,
/// the sweep's metadata catch-up installs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_the_journal_did_not_persist_is_not_installed() {
    let cluster = start_dynamic(3);
    cluster.journals[2].set_metadata_failing(true);

    cluster.nodes[0]
        .change_topology(1, vec![cluster.ids.clone()])
        .await
        .unwrap();
    await_epoch(cluster.topologies[1].as_ref(), 1).await;
    // Several sweeps' worth of catch-up attempts, all failing to persist.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cluster.topologies[2].epoch(),
        0,
        "installed an epoch its journal did not take"
    );
    assert!(cluster.journals[2]
        .load_metadata()
        .await
        .unwrap()
        .is_empty());

    cluster.journals[2].set_metadata_failing(false);
    await_epoch(cluster.topologies[2].as_ref(), 1).await;
    assert_eq!(cluster.journals[2].load_metadata().await.unwrap().len(), 1);
}

/// A node that cannot persist its clock bound must not use a timestamp past
/// it: after a restart it could mint that id again. A write whose id it cannot
/// cover fails before any replica hears of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_whose_id_cannot_be_covered_fails_before_it_is_sent() {
    let cluster = start_with(3, false, NodeConfig::default());
    cluster.journals[0].set_clock_bound_failing(true);

    let result = cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await;
    assert!(result.is_err(), "used an id it could not cover: {result:?}");
    for node in &cluster.nodes {
        assert_eq!(node.command_count(), 0, "a replica heard of the write");
    }

    cluster.journals[0].set_clock_bound_failing(false);
    cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("a covered id is used");
    assert!(cluster.journals[0]
        .load_clock_bound()
        .await
        .unwrap()
        .is_some());
}
