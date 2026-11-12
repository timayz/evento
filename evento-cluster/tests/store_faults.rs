//! Data-store faults on the execution path: a replica whose store fails an
//! apply or a commit-condition read must not pretend it succeeded.
//!
//! `Applied` is the promise that a transaction's effect is in the store — the
//! read barrier, the execution gate and anti-entropy all trust it — so an apply
//! the store refused must stay un-applied and be retried, not be marked and
//! acknowledged (the events would be lost on that replica for good). Likewise a
//! version read that failed says nothing about the condition: answering
//! "holds" would let a stale write commit.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::store::FaultStore;
use evento_cluster::protocol::{Message, Status};
use evento_cluster::{
    DataStore, HybridLogicalClock, InMemoryJournal, InMemoryNetwork, Journal, MessageSink, Node,
    NodeConfig, NodeId, StaticTopology,
};
use evento_core::cursor::Args;
use evento_core::Event;

struct Cluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<FaultStore>>,
    net: Arc<InMemoryNetwork>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

fn start(n: u64, config: NodeConfig) -> Cluster {
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    let mut loops = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let store = Arc::new(FaultStore::default());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())),
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        )
        .with_config(config);
        loops.push(node.start(inbox));
        nodes.push(node);
        stores.push(store);
    }
    Cluster {
        nodes,
        stores,
        net,
        _loops: loops,
    }
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

/// Polls `check` every 5 ms for up to 5 s.
async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..1000 {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_the_store_failed_is_retried_not_marked_applied() {
    let cluster = start(3, NodeConfig::default());
    cluster.stores[2]
        .apply_failing
        .store(true, Ordering::SeqCst);

    // A quorum (nodes 0 and 1) applies, so the write commits.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.nodes[0].write(vec![event("acc", 1)]),
    )
    .await
    .expect("write timed out")
    .expect("a quorum applied the write");
    assert!(!outcome.conflict);
    let txn = outcome.txn;

    // Node 2 has the decision but its store refused the events: the command
    // must not claim to be applied.
    assert!(
        eventually(|| cluster.nodes[2].metrics().execution_failures > 0).await,
        "node 2 never attempted the apply"
    );
    assert_ne!(cluster.nodes[2].command_status(txn), Some(Status::Applied));
    assert!(cluster.stores[2].applied().is_empty());

    // Once the store takes writes again, the next sweep applies it. The
    // sweep wakes the applier and runs an anti-entropy round, and both may
    // write it: an import does not wait for a local apply in flight, the
    // second write being idempotent in a store. So the store may log it
    // twice, and nothing else.
    cluster.stores[2]
        .apply_failing
        .store(false, Ordering::SeqCst);
    cluster.nodes[2].sweep().await;
    assert!(
        eventually(|| cluster.stores[2].applied().contains(&txn)).await,
        "the decided transaction was never re-applied"
    );
    assert!(cluster.stores[2].applied().iter().all(|&t| t == txn));
    assert_eq!(cluster.nodes[2].command_status(txn), Some(Status::Applied));
}

/// A store that lost an apply it acknowledged — or was reopened over an
/// older copy of itself — is behind: the next apply on that aggregate brings
/// a version more than one past what it holds (#453). The store refuses it
/// (`StoreBehind`), so nothing is served over the hole, and the node asks to
/// be rejoined: the missing versions can no longer come from its journal,
/// and a peer may have compacted them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_behind_by_a_version_is_not_served_over_the_hole() {
    let cluster = start(3, NodeConfig::default());
    cluster.stores[2]
        .lose_next_apply
        .store(true, Ordering::SeqCst);

    let first = cluster.nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("a quorum applied the write")
        .txn;
    // Node 2's store acknowledged the apply and kept nothing.
    assert!(
        eventually(|| cluster.nodes[2].command_status(first) == Some(Status::Applied)).await,
        "node 2 never applied the first write"
    );
    assert!(cluster.stores[2].applied().is_empty());

    let second = cluster.nodes[0]
        .write(vec![event("acc", 2)])
        .await
        .expect("a quorum applied the write")
        .txn;
    assert!(
        eventually(|| cluster.nodes[2].rejoin_required()).await,
        "node 2 never asked for a rejoin"
    );
    assert_ne!(
        cluster.nodes[2].command_status(second),
        Some(Status::Applied),
        "version 2 was applied over the hole"
    );
    assert!(cluster.stores[2].applied().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_read_the_store_failed_is_not_a_vote() {
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(3),
        ..NodeConfig::default()
    };
    let cluster = start(3, config);
    let _sweeps: Vec<_> = cluster.nodes.iter().map(Node::start_recovery).collect();

    let first = cluster.nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("first write");
    assert!(!first.conflict);
    assert!(
        eventually(|| cluster.stores.iter().all(|s| s.applied().len() == 1)).await,
        "the first write never reached every replica"
    );

    // A stale write (version 1 again) while no replica can read versions: an
    // error must not count as "the condition holds".
    for store in &cluster.stores {
        store.version_failing.store(true, Ordering::SeqCst);
    }
    let node = cluster.nodes[0].clone();
    let stale = tokio::spawn(async move { node.write(vec![event("acc", 1)]).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    for store in &cluster.stores {
        store.version_failing.store(false, Ordering::SeqCst);
    }

    let outcome = tokio::time::timeout(Duration::from_secs(10), stale)
        .await
        .expect("stale write timed out")
        .expect("task")
        .expect("the stale write settles once reads work again");
    assert!(
        outcome.conflict,
        "a stale write must lose the version condition"
    );
    assert!(cluster.nodes[0].metrics().execution_failures > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anti_entropy_stops_at_an_import_the_store_failed() {
    // Node 2 misses two writes to one aggregate while partitioned, then
    // repairs by anti-entropy — and its store fails the first import. The
    // second must not land ahead of the first: the store would hold version 2
    // without version 1, and a subscription could pass the first event's
    // cursor before its retry arrives.
    let config = NodeConfig {
        anti_entropy_interval: Duration::from_millis(200),
        ..NodeConfig::default()
    };
    let cluster = start(3, config);
    for peer in [NodeId(0), NodeId(1)] {
        cluster.net.partition(NodeId(2), peer);
    }
    let first = cluster.nodes[0].write(vec![event("acc", 1)]).await.unwrap();
    let second = cluster.nodes[0].write(vec![event("acc", 2)]).await.unwrap();
    assert!(eventually(|| cluster.stores[1].applied() == vec![first.txn, second.txn]).await);

    cluster.stores[2].applies_to_fail.store(1, Ordering::SeqCst);
    cluster.net.heal_all_partitions();
    for _ in 0..6 {
        cluster.nodes[2].sweep().await;
        tokio::time::sleep(Duration::from_millis(210)).await;
        if cluster.stores[2].applied().len() == 2 {
            break;
        }
    }
    assert_eq!(
        cluster.stores[2].applied(),
        vec![first.txn, second.txn],
        "imports land in execution order, the failed one first on retry"
    );
}

/// A forwarded read the owner's store failed is an error for the reader, not
/// an empty page: an empty page reads as "no such events", so an aggregate
/// would load as never created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_read_the_store_failed_is_an_error() {
    let cluster = start(3, NodeConfig::default());
    cluster.stores[1].read_failing.store(true, Ordering::SeqCst);

    let result = cluster.nodes[0]
        .forward_read(
            NodeId(1),
            evento_cluster::protocol::Key("acc".into()),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await;
    let err = result.expect_err("a failed read came back as a page");
    // The owner's own failure, not a timeout.
    assert!(
        format!("{err:#}").contains("simulated query failure"),
        "unexpected error: {err:#}"
    );

    cluster.stores[1]
        .read_failing
        .store(false, Ordering::SeqCst);
    cluster.nodes[0]
        .forward_read(
            NodeId(1),
            evento_cluster::protocol::Key("acc".into()),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await
        .expect("a healthy owner serves the read");
}

/// A requester on a build predating `ReadFailed` sheds it, and its read times
/// out: still an error, never an empty page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_requester_that_cannot_decode_the_failure_still_fails_the_read() {
    let cluster = start(
        3,
        NodeConfig {
            collect_timeout: Duration::from_millis(300),
            ..NodeConfig::default()
        },
    );
    cluster
        .net
        .set_unreadable(Some(Arc::new(|message: &Message| {
            matches!(message, Message::ReadFailed { .. })
        })));
    cluster.stores[1].read_failing.store(true, Ordering::SeqCst);

    let result = cluster.nodes[0]
        .forward_read(
            NodeId(1),
            evento_cluster::protocol::Key("acc".into()),
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "a failed read came back as a page: {result:?}"
    );
}

/// A node alone in its group, over a store whose applies stall, commits a
/// burst in the store's time and recovers nothing (#475). The applies run on
/// the applier, so no reply queues behind them; and a coordinator short of
/// its own acknowledgement while its store is still enacting the queue waits
/// for it rather than time the phase out at `collect_timeout` and recover a
/// write nobody refused.
#[tokio::test(start_paused = true)]
async fn a_node_alone_commits_a_stalled_burst_without_recovering() {
    const WRITES: u32 = 20;
    let delay = Duration::from_millis(400);
    let config = NodeConfig::default();
    let cluster = start(1, config);
    cluster.stores[0].set_apply_delay(delay);
    let node = cluster.nodes[0].clone();

    let started = tokio::time::Instant::now();
    let mut writes = tokio::task::JoinSet::new();
    for i in 0..WRITES {
        let node = node.clone();
        writes.spawn(async move { node.write(vec![event(&format!("acc-{i}"), 1)]).await });
    }
    while let Some(outcome) = writes.join_next().await {
        let outcome = outcome
            .expect("writer panicked")
            .expect("the write committed");
        assert!(!outcome.conflict);
    }
    let elapsed = started.elapsed();
    // The applies are serial, so the burst takes the store's time — and
    // nothing waited a `collect_timeout` on top of it.
    assert!(elapsed >= delay * WRITES, "{elapsed:?}");
    assert!(
        elapsed < delay * WRITES + config.collect_timeout,
        "the burst waited past the store's time: {elapsed:?}"
    );
    let metrics = node.metrics();
    assert_eq!(metrics.writes_settled, 0, "a write fell to recovery");
    assert_eq!(metrics.recoveries, 0);
    assert_eq!(metrics.writes_committed, u64::from(WRITES));
    assert!(
        metrics.own_store_waits > 0,
        "the tail of the burst outlasted collect_timeout and waited on the store"
    );
    assert_eq!(cluster.stores[0].applied().len(), WRITES as usize);
}

/// A read a node's own store is still working is waited for, past
/// `collect_timeout`, rather than timed out and recovered: a recovery would
/// ask the same replica, which reads no transaction twice (#475).
#[tokio::test(start_paused = true)]
async fn a_node_alone_waits_for_a_read_its_store_is_still_working() {
    let config = NodeConfig::default();
    let read = config.collect_timeout + Duration::from_secs(2);
    let cluster = start(1, config);
    cluster.stores[0].set_version_delay(read);

    let started = tokio::time::Instant::now();
    let outcome = cluster.nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("the write committed");
    assert!(!outcome.conflict);
    let elapsed = started.elapsed();
    assert!(elapsed >= read, "{elapsed:?}");
    assert!(
        elapsed < read + config.collect_timeout,
        "the write waited past the read: {elapsed:?}"
    );
    let metrics = cluster.nodes[0].metrics();
    assert_eq!(metrics.writes_settled, 0, "the write fell to recovery");
    assert!(metrics.own_store_waits > 0);
    assert_eq!(cluster.stores[0].applied().len(), 1);
}
