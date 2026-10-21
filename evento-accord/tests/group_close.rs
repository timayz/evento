//! `GroupHost::close` returns only once the group's node no longer touches its
//! store or journal: the caller closes or discards them right after (a tenant
//! evicted from an LRU cache, the rejoin flow), and a write landing in them
//! then is lost — or, on SQLite, lands in the files of the next database
//! opened at that path. And the closed node mints nothing more, so a clone
//! the application still holds cannot mint an id the group's next node, on
//! the same journal, mints too.
//!
//! The tests run on the current-thread runtime, so what `close` left running
//! is still running when it returns: nothing else gets to run in between.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use evento_accord::{
    DataStore, GroupHost, GroupId, InMemoryDataStore, InMemoryJournal, Journal, MuxTransport, Node,
    NodeClosed, NodeConfig, NodeId, StaticTopology, SweepConfig, SweepScheduler, Timestamp,
    Topology, TxnId,
};
use evento_core::Event;
use tokio::net::TcpListener;
use tokio::sync::Notify;

const GROUP: GroupId = GroupId(1);

/// A store whose applies fail `fail_first` times, then block until dropped,
/// counting the ones in flight.
#[derive(Default)]
struct BlockingStore {
    inner: InMemoryDataStore,
    fail_first: usize,
    calls: AtomicUsize,
    in_flight: Arc<AtomicUsize>,
    entered: Notify,
}

/// Counts an apply in flight until it is dropped.
struct InFlight(Arc<AtomicUsize>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DataStore for BlockingStore {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        _txn: TxnId,
        _execute_at: Timestamp,
        _events: Vec<Event>,
        _commit: bool,
    ) -> anyhow::Result<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_first {
            anyhow::bail!("simulated write failure");
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let _in_flight = InFlight(Arc::clone(&self.in_flight));
        self.entered.notify_one();
        std::future::pending::<()>().await;
        unreachable!("an apply that never completes")
    }
}

fn event(version: u16) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: "acc".into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

/// A one-node host over real localhost TCP, its scheduler driven by hand
/// ([`SweepScheduler::tick`]), so no sweep runs unless a test asks for one.
async fn host(scheduler: &Arc<SweepScheduler>) -> GroupHost {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peers = HashMap::from([(NodeId(0), listener.local_addr().unwrap())]);
    let mux = Arc::new(MuxTransport::new(NodeId(0), peers));
    mux.serve(listener);
    GroupHost::new(mux, Arc::clone(scheduler))
}

async fn open(host: &GroupHost, store: Arc<dyn DataStore>, journal: Arc<InMemoryJournal>) -> Node {
    host.open(
        GROUP,
        Arc::new(StaticTopology::new(NodeId(0), vec![NodeId(0)])) as Arc<dyn Topology>,
        store,
        journal as Arc<dyn Journal>,
        NodeConfig::default(),
    )
    .await
    .unwrap()
}

/// A message being handled — the apply of a write — is stopped before
/// `close` returns.
#[tokio::test]
async fn close_returns_once_the_inbox_loop_has_stopped() {
    let scheduler = SweepScheduler::new(SweepConfig::default());
    let host = host(&scheduler).await;
    let store = Arc::new(BlockingStore::default());
    let node = open(&host, store.clone(), Arc::new(InMemoryJournal::new())).await;

    let writer = tokio::spawn(async move { node.write(vec![event(1)]).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .expect("the apply never started");
    assert_eq!(store.in_flight.load(Ordering::SeqCst), 1);

    host.close(GROUP).await.expect("the group was open");
    assert_eq!(
        store.in_flight.load(Ordering::SeqCst),
        0,
        "close returned with an apply still in flight"
    );
    writer.abort();
}

/// A sweep in flight — re-driving an apply the store refused the first time —
/// is stopped before `close` returns.
#[tokio::test]
async fn close_returns_once_the_sweep_has_stopped() {
    let scheduler = SweepScheduler::new(SweepConfig::default());
    let host = host(&scheduler).await;
    let store = Arc::new(BlockingStore {
        fail_first: 1,
        ..BlockingStore::default()
    });
    let node = open(&host, store.clone(), Arc::new(InMemoryJournal::new())).await;

    // The inbox loop's apply fails; only the sweep drives it again.
    let writer = tokio::spawn(async move { node.write(vec![event(1)]).await });
    for _ in 0..500 {
        if store.calls.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store.calls.load(Ordering::SeqCst),
        1,
        "the first apply failed"
    );
    scheduler.tick();
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .expect("the sweep never re-drove the apply");
    assert_eq!(store.in_flight.load(Ordering::SeqCst), 1);

    host.close(GROUP).await.expect("the group was open");
    assert_eq!(
        store.in_flight.load(Ordering::SeqCst),
        0,
        "close returned with a sweep still in flight"
    );
    writer.abort();
}

/// The node `close` returns mints nothing more: a write through it fails with
/// `NodeClosed`. The group's next node, over the same journal, writes on.
#[tokio::test]
async fn a_closed_node_refuses_to_write_and_the_reopened_one_writes_on() {
    let scheduler = SweepScheduler::new(SweepConfig::default());
    let host = host(&scheduler).await;
    let journal = Arc::new(InMemoryJournal::new());
    let store = Arc::new(InMemoryDataStore::new());
    let node = open(&host, store.clone(), Arc::clone(&journal)).await;
    node.write(vec![event(1)]).await.unwrap();

    let closed = host.close(GROUP).await.expect("the group was open");
    let err = closed.write(vec![event(2)]).await.unwrap_err();
    assert!(err.is::<NodeClosed>(), "unexpected error: {err:#}");

    let reopened = open(&host, store.clone(), journal).await;
    reopened.write(vec![event(2)]).await.unwrap();
    assert_eq!(store.version("test/Account", "acc").await.unwrap(), 2);
}
