//! M5 acceptance tests: [`AccordExecutor`] is a real `evento_core::Executor`.
//! Writes coordinated through a single-shard Accord cluster replicate to every
//! node's local Fjall backend (served back through the standard `read` path) and
//! enforce evento's optimistic-concurrency contract across the cluster.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor, WriteError};
use evento_fjall::Fjall;
use tempfile::TempDir;
use ulid::Ulid;

/// A single-shard cluster of fjall-backed `AccordExecutor`s.
struct ExecCluster {
    execs: Vec<AccordExecutor<Fjall>>,
    _temps: Vec<TempDir>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl ExecCluster {
    fn start(n: u64) -> Self {
        Self::start_with(n, false)
    }

    /// A cluster whose nodes serve **linearizable** reads (read barriers on).
    fn start_linearizable(n: u64) -> Self {
        Self::start_with(n, true)
    }

    fn start_with(n: u64, linearizable_reads: bool) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();

        let mut execs = Vec::new();
        let mut temps = Vec::new();
        let mut loops = Vec::new();

        for &id in &ids {
            let temp = tempfile::Builder::new()
                .prefix("evento_accord_m5")
                .tempdir()
                .unwrap();
            let fjall = Fjall::open(temp.path()).unwrap();

            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let datastore: Arc<dyn DataStore> = Arc::new(ExecutorDataStore::new(fjall.clone()));
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node =
                Node::new(id, topology, clock, sink, datastore, journal).with_config(NodeConfig {
                    linearizable_reads,
                    ..Default::default()
                });

            loops.push(node.start(inbox));
            execs.push(AccordExecutor::new(node, fjall));
            temps.push(temp);
        }

        ExecCluster {
            execs,
            _temps: temps,
            _loops: loops,
        }
    }

    /// Reads all events for an aggregate from one node's executor.
    async fn read_all(&self, node: usize, id: &str) -> Vec<Event> {
        let result = self.execs[node]
            .read(
                Some(vec![EventFilter::by_id("test/Account", id)]),
                None,
                Args::forward(50, None),
            )
            .await
            .unwrap();
        result.edges.into_iter().map(|e| e.node).collect()
    }

    /// Waits until `node`'s executor shows at least `len` events for `id`.
    async fn await_read(&self, node: usize, id: &str, len: usize) -> Vec<Event> {
        for _ in 0..1000 {
            let events = self.read_all(node, id).await;
            if events.len() >= len {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("node {node} never saw {len} events for {id}");
    }
}

/// A test event for an aggregate at a version. Each event gets a strictly
/// increasing `timestamp_subsec` so the backend orders them by creation, exactly
/// as evento's commit builder produces real timestamps (events with all-zero
/// timestamps would break cursor ordering).
fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(1);
    Event {
        id: Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        timestamp: 1,
        timestamp_subsec: SEQ.fetch_add(1, Ordering::SeqCst),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_through_accord_replicates_to_every_node() {
    let cluster = ExecCluster::start(3);

    // Write through node 0's executor.
    cluster.execs[0]
        .write(vec![event("acc-1", 1, "Opened")])
        .await
        .unwrap();

    // Every node's local backend serves it through the standard read path.
    for node in 0..3 {
        let events = cluster.await_read(node, "acc-1", 1).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version, 1);
        assert_eq!(events[0].name, "Opened");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_your_writes_on_the_coordinator() {
    let cluster = ExecCluster::start(3);

    cluster.execs[0]
        .write(vec![event("z", 1, "Opened")])
        .await
        .unwrap();

    // No polling: the coordinator waited for its own apply, so the event is
    // immediately visible on the same executor.
    let events = cluster.read_all(0, "z").await;
    assert_eq!(
        events.len(),
        1,
        "the write must be readable on its coordinator"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_observes_a_write_from_another_node() {
    let cluster = ExecCluster::start_linearizable(3);

    // Write through node 0 and wait for its own apply.
    cluster.execs[0]
        .write(vec![event("k", 1, "Opened")])
        .await
        .unwrap();

    // A linearizable read on node 1 — with NO polling — must observe the write:
    // the read barrier coordinates a read-only transaction whose timestamp orders
    // after the write, so node 1 applies the write before serving its backend.
    // (Without the barrier this immediate cross-node read could still be empty.)
    let events = cluster.read_all(1, "k").await;
    assert_eq!(
        events.len(),
        1,
        "a linearizable read must observe a write that completed before it began"
    );
    assert_eq!(events[0].version, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_reads_store_nothing() {
    let cluster = ExecCluster::start_linearizable(3);

    // No writes: the conflict graph is empty everywhere.
    for e in &cluster.execs {
        assert_eq!(e.node().command_count(), 0);
    }

    // Many linearizable reads of various keys. Each is a read-index probe that
    // stores nothing — the conflict graph must stay empty (unlike the previous
    // read-only-transaction approach, which left a command per read).
    for i in 0..20 {
        let _ = cluster.read_all(i % 3, &format!("acc-{i}")).await;
    }
    for e in &cluster.execs {
        assert_eq!(
            e.node().command_count(),
            0,
            "linearizable reads must not enter the conflict graph"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforces_optimistic_concurrency_across_the_cluster() {
    let cluster = ExecCluster::start(3);

    // Open the aggregate at version 1.
    cluster.execs[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    cluster.await_read(1, "acc", 1).await;

    // Two coordinators race to write version 2 from different nodes.
    let a = cluster.execs[0].clone();
    let b = cluster.execs[1].clone();
    let w0 = tokio::spawn(async move { a.write(vec![event("acc", 2, "DebitedA")]).await });
    let w1 = tokio::spawn(async move { b.write(vec![event("acc", 2, "DebitedB")]).await });

    let r0 = w0.await.unwrap();
    let r1 = w1.await.unwrap();

    // Exactly one succeeds; the other gets InvalidOriginalVersion.
    let ok = r0.is_ok() as u8 + r1.is_ok() as u8;
    assert_eq!(ok, 1, "exactly one write must win: {r0:?} {r1:?}");
    assert!(
        matches!(r0, Err(WriteError::InvalidOriginalVersion))
            || matches!(r1, Err(WriteError::InvalidOriginalVersion)),
        "the loser must be a version conflict"
    );

    // The surviving version-2 event replicated everywhere alongside the open.
    for node in 0..3 {
        let events = cluster.await_read(node, "acc", 2).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].version, 2);
    }
}

/// `ExecutorDataStore::version`/`snapshot` paginate the backend, so an aggregate with
/// more than one page of events (> `SNAPSHOT_PAGE_SIZE`, 4096) is handled fully — not
/// capped at a single page. Seeds the Fjall backend directly (bypassing consensus,
/// which would be far too slow for thousands of events) and reads through the bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_store_paginates_beyond_one_page() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_page")
        .tempdir()
        .unwrap();
    let fjall = Fjall::open(temp.path()).unwrap();

    // More than one 4096-event page for a single aggregate.
    let count: u16 = 5000;
    let events: Vec<Event> = (1..=count).map(|v| event("big", v, "Tick")).collect();
    fjall.write(events).await.unwrap();

    let store = ExecutorDataStore::new(fjall);

    // `version` must return the true max, which lives past the first page boundary.
    let version = store.version("test/Account", "big").await.unwrap();
    assert_eq!(version, count);

    // `snapshot` must return every event across all pages.
    let snap = store.snapshot().await.unwrap();
    assert_eq!(snap.len() as u16, count);
    assert_eq!(snap.iter().map(|e| e.version).max().unwrap(), count);
}
