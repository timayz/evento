//! Data-store faults on the execution path: a replica whose store fails an
//! apply or a commit-condition read must not pretend it succeeded.
//!
//! `Applied` is the promise that a transaction's effect is in the store — the
//! read barrier, the execution gate and anti-entropy all trust it — so an apply
//! the store refused must stay un-applied and be retried, not be marked and
//! acknowledged (the events would be lost on that replica for good). Likewise a
//! version read that failed says nothing about the condition: answering
//! "holds" would let a stale write commit.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Message, MessageSink, Node, NodeConfig, NodeId, StaticTopology, Status, Timestamp, TxnId,
};
use evento_core::cursor::{Args, ReadResult};
use evento_core::{Event, EventFilter, RoutingKey};

/// An in-memory store whose applies and version reads can be made to fail.
#[derive(Default)]
struct FaultStore {
    inner: InMemoryDataStore,
    apply_failing: AtomicBool,
    /// Fails this many applies, then works again.
    applies_to_fail: AtomicUsize,
    version_failing: AtomicBool,
    /// Fails every query (`DataStore::read`), as a forwarded read runs one.
    read_failing: AtomicBool,
}

impl FaultStore {
    fn applied(&self) -> Vec<TxnId> {
        self.inner.applied_log().iter().map(|e| e.txn).collect()
    }
}

#[async_trait]
impl DataStore for FaultStore {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        if self.version_failing.load(Ordering::SeqCst) {
            anyhow::bail!("simulated read failure");
        }
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if self.apply_failing.load(Ordering::SeqCst)
            || self
                .applies_to_fail
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            anyhow::bail!("simulated write failure");
        }
        self.inner.apply(txn, execute_at, events, commit).await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        if self.read_failing.load(Ordering::SeqCst) {
            anyhow::bail!("simulated query failure");
        }
        self.inner
            .read(aggregators, routing_key, args, to_micros)
            .await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        self.inner.snapshot().await
    }
}

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

    // Once the store takes writes again, the next sweep applies it.
    cluster.stores[2]
        .apply_failing
        .store(false, Ordering::SeqCst);
    cluster.nodes[2].sweep().await;
    assert!(
        eventually(|| cluster.stores[2].applied() == vec![txn]).await,
        "the decided transaction was never re-applied"
    );
    assert_eq!(cluster.nodes[2].command_status(txn), Some(Status::Applied));
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
        .forward_read(NodeId(1), None, None, Args::forward(10, None), None)
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
        .forward_read(NodeId(1), None, None, Args::forward(10, None), None)
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
        .forward_read(NodeId(1), None, None, Args::forward(10, None), None)
        .await;
    assert!(
        result.is_err(),
        "a failed read came back as a page: {result:?}"
    );
}
