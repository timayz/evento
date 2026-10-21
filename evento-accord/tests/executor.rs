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
use ulid::Ulid;

/// A single-shard cluster of fjall-backed `AccordExecutor`s.
struct ExecCluster {
    execs: Vec<AccordExecutor<Fjall>>,
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
        Self::start_configured(
            n,
            NodeConfig {
                linearizable_reads,
                ..Default::default()
            },
        )
    }

    fn start_configured(n: u64, config: NodeConfig) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();

        let mut execs = Vec::new();
        let mut loops = Vec::new();

        for &id in &ids {
            let fjall = Fjall::temporary().unwrap();

            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let datastore: Arc<dyn DataStore> = Arc::new(ExecutorDataStore::new(fjall.clone()));
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node = Node::new(id, topology, clock, sink, datastore, journal).with_config(config);

            loops.push(node.start(inbox));
            execs.push(AccordExecutor::new(node, fjall));
        }

        ExecCluster {
            execs,
            _loops: loops,
        }
    }

    /// Reads all events for an aggregate from one node's executor.
    async fn read_all(&self, node: usize, id: &str) -> Vec<Event> {
        let result = self.execs[node]
            .read(
                Some([EventFilter::by_id_raw("test/Account", id)].into()),
                None,
                Args::forward(50, None),
                None,
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

/// Two hosts' skewed clocks cannot reverse an aggregate's versions: the
/// stamps an event carries in are the writing host's provisional ones, and
/// every replica re-stamps it from its transaction's agreed `execute_at`. So
/// v1, stamped 10 s ahead by one host, and v2, stamped at the present by
/// another, read back as `[1, 2]` everywhere — with identical stamps on every
/// replica, neither of them the client's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skewed_client_stamps_cannot_reverse_an_aggregate() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let cluster = ExecCluster::start(3);
    let stamped = |version: u16, at: Duration| Event {
        timestamp: at.as_secs(),
        timestamp_subsec: at.subsec_millis(),
        ..event("skewed", version, "Opened")
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let ahead = now + Duration::from_secs(10);

    cluster.execs[0]
        .write(vec![stamped(1, ahead)])
        .await
        .unwrap();
    cluster.execs[1].write(vec![stamped(2, now)]).await.unwrap();

    let stamp = |e: &Event| (e.timestamp, e.timestamp_subsec);
    let first = cluster.await_read(0, "skewed", 2).await;
    for node in 0..3 {
        let events = cluster.await_read(node, "skewed", 2).await;
        let versions: Vec<u16> = events.iter().map(|e| e.version).collect();
        assert_eq!(versions, [1, 2], "node {node} reads the versions reversed");
        assert_eq!(
            events.iter().map(stamp).collect::<Vec<_>>(),
            first.iter().map(stamp).collect::<Vec<_>>(),
            "node {node} stamps the events differently"
        );
        assert!(
            events[0].timestamp < ahead.as_secs(),
            "node {node} kept the client's stamp"
        );
        assert!(
            events.iter().all(|e| e.timestamp_subsec < 1000),
            "node {node} stamps finer than milliseconds"
        );
    }
}

/// A write the node refuses because its un-compacted backlog is at
/// `max_commands` is a transient refusal, not a failure: the executor answers
/// `WriteError::Unavailable`, and the typed cause is still there to downcast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_at_its_cap_is_a_transient_refusal() {
    use evento_accord::BacklogAtCap;

    // One node never compacts (no peer to agree a watermark with below the
    // margin), so two writes fill a cap of two.
    let cluster = ExecCluster::start_configured(
        1,
        NodeConfig {
            max_commands: 2,
            compaction_margin: Duration::from_secs(3600),
            ..Default::default()
        },
    );
    cluster.execs[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .unwrap();
    cluster.execs[0]
        .write(vec![event("acc", 2, "Deposited")])
        .await
        .unwrap();
    let err = cluster.execs[0]
        .write(vec![event("acc", 3, "Deposited")])
        .await
        .expect_err("the third write is refused");
    match &err {
        WriteError::Unavailable(cause) => {
            let cap = cause
                .downcast_ref::<BacklogAtCap>()
                .expect("the cause is the typed backlog error");
            assert_eq!(cap.cap, 2);
            assert!(cap.backlog >= 2);
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
    assert!(err.to_string().contains("not taking writes right now"));
}

/// `ExecutorDataStore::version`/`snapshot` paginate the backend, so an aggregate with
/// more than one page of events (> `SNAPSHOT_PAGE_SIZE`, 4096) is handled fully — not
/// capped at a single page. Seeds the Fjall backend directly (bypassing consensus,
/// which would be far too slow for thousands of events) and reads through the bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_store_paginates_beyond_one_page() {
    let fjall = Fjall::temporary().unwrap();

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

/// `ExecutorDataStore::snapshot_page` walks the backend in the pieces a joiner
/// pulls: capped by events, capped by bytes (never below one event), and
/// chained through `next` until the whole store has crossed exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_page_walks_the_backend_by_events_and_bytes() {
    use evento_accord::snapshot_event_size;

    let fjall = Fjall::temporary().unwrap();
    let count: u16 = 5000;
    let events: Vec<Event> = (1..=count).map(|v| event("big", v, "Tick")).collect();
    fjall.write(events).await.unwrap();
    let store = ExecutorDataStore::new(fjall);

    // By events: 100 per page, chained to the end, every version once.
    let mut after = None;
    let mut pages = 0;
    let mut seen = Vec::new();
    loop {
        let page = store.snapshot_page(after, 100, usize::MAX).await.unwrap();
        assert!(page.events.len() <= 100);
        pages += 1;
        seen.extend(page.events.iter().map(|e| e.version));
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    assert_eq!(pages, 50);
    assert_eq!(seen.len() as u16, count);
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "versions ascend");

    // By bytes: a budget under one event still ships one event per page.
    let first = store.snapshot_page(None, 100, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.next.is_some());
    let second = store
        .snapshot_page(first.next.clone(), 100, 1)
        .await
        .unwrap();
    assert_eq!(
        second.events[0].version, 2,
        "the next page starts after the trimmed one"
    );

    // By bytes: a budget for about ten events gives pages of about ten.
    let one = snapshot_event_size(&first.events[0]);
    let page = store
        .snapshot_page(None, u16::MAX, one * 10 + one / 2)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 10);

    // The last page is the end: no `next`, and nothing after it.
    let tail = store
        .snapshot_page(None, u16::MAX, usize::MAX)
        .await
        .unwrap();
    assert_eq!(tail.events.len(), 4096, "one backend read at most");
    let rest = store
        .snapshot_page(tail.next, u16::MAX, usize::MAX)
        .await
        .unwrap();
    assert_eq!(rest.events.len(), 5000 - 4096);
    assert!(rest.next.is_none());
}
