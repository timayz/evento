//! Subscription ordering on a multi-node `AccordExecutor`, and the stability
//! watermark that makes it safe.
//!
//! A running subscription reads its backend with a forward cursor ordered by the
//! event's `(timestamp, timestamp_subsec, version, id)` — fields stamped on the
//! *originating* node before consensus. Accord, however, only orders *conflicting*
//! (same-key) transactions and applies committed transactions to each node's local
//! store in `(execute_at, txn)` order. So an event with a *smaller* cursor can be
//! applied to a node *after* its subscription has advanced past a *larger* cursor
//! from a different aggregate written elsewhere — and a naive forward read would
//! **skip** it.
//!
//! [`AccordExecutor`] guards against this via [`Executor::stable_timestamp`]
//! ([`Node::stable_micros`]): the subscription only processes events whose
//! timestamp is below the node's stability watermark — the `applied_through` point
//! that trails `now` by `compaction_margin` and is pinned below any known
//! un-applied transaction. Below it, everything is applied and nothing lower can
//! still arrive, **provided propagation + clock skew stays within
//! `compaction_margin`** — the same bound the node already assumes for compaction.
//!
//! - `subscription_waits_for_a_late_lower_cursor_event` proves the gate: with the
//!   margin above the link delay, the late event is processed in order, not skipped.
//! - `skip_returns_when_propagation_exceeds_the_margin` documents the boundary:
//!   violate the assumption (delay > margin) and the skip reappears.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::cursor::Args;
use evento_core::subscription::{Context, Handler, SubscriptionBuilder};
use evento_core::{Event, EventFilter, Executor};
use evento_fjall::Fjall;
use tempfile::TempDir;
use ulid::Ulid;

/// Records the `aggregate_id` of every event the subscription handles, in order.
struct Recorder(Arc<Mutex<Vec<String>>>);

impl<E: Executor> Handler<E> for Recorder {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        let recorded = self.0.clone();
        let id = event.aggregate_id.clone();
        Box::pin(async move {
            recorded.lock().unwrap().push(id);
            Ok(())
        })
    }

    fn aggregate_type(&self) -> &'static str {
        "test/Account"
    }

    fn event_name(&self) -> &'static str {
        "Opened"
    }
}

/// An "Opened" event stamped with the real wall clock, exactly as evento's commit
/// builder does — so its cursor is comparable to the node's stability watermark.
fn opened(aggregate_id: &str) -> Event {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    Event {
        id: Ulid::new(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version: 1,
        name: "Opened".into(),
        timestamp: now.as_secs(),
        timestamp_subsec: now.subsec_millis(),
        ..Default::default()
    }
}

/// A single-shard cluster of fjall-backed `AccordExecutor`s plus the network
/// handle, so a test can inject per-link latency.
struct Cluster {
    execs: Vec<AccordExecutor<Fjall>>,
    net: Arc<InMemoryNetwork>,
    _temps: Vec<TempDir>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    fn start(n: u64, compaction_margin: Duration) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        let mut execs = Vec::new();
        let mut temps = Vec::new();
        let mut loops = Vec::new();

        for &id in &ids {
            let temp = tempfile::Builder::new()
                .prefix("evento_accord_skip")
                .tempdir()
                .unwrap();
            let fjall = Fjall::open(temp.path()).unwrap();

            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            // Share one fjall instance between the apply path and the read/subscribe
            // path so a node's applied writes are visible to (and wake) its reads.
            let datastore: Arc<dyn DataStore> = Arc::new(ExecutorDataStore::new(fjall.clone()));
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node =
                Node::new(id, topology, clock, sink, datastore, journal).with_config(NodeConfig {
                    compaction_margin,
                    ..Default::default()
                });

            loops.push(node.start(inbox));
            execs.push(AccordExecutor::new(node, fjall));
            temps.push(temp);
        }

        Cluster {
            execs,
            net,
            _temps: temps,
            _loops: loops,
        }
    }

    async fn count(&self, node: usize, aggregate_id: &str) -> usize {
        self.execs[node]
            .read(
                Some(vec![EventFilter::by_id("test/Account", aggregate_id)]),
                None,
                Args::forward(50, None),
            )
            .await
            .unwrap()
            .edges
            .len()
    }
}

/// Writes X (aggregate "delayed", lower cursor) via node A and Y (aggregate
/// "local", higher cursor) via node B, with the `A → B` link delayed so B applies
/// its own Y first and X arrives late. Returns the cluster, the subscription
/// handle, and the shared record of handled aggregate ids.
async fn run_late_event_scenario(
    margin: Duration,
    link_delay: Duration,
) -> (
    Cluster,
    evento_core::subscription::Subscription,
    Arc<Mutex<Vec<String>>>,
) {
    // Nodes: A = 0 (writes X), B = 1 (writes Y, runs the subscription), C = 2.
    let cluster = Cluster::start(3, margin);

    // Delay ONLY A -> B. A still commits X via the {A, C} quorum, but B does not
    // apply X until ~link_delay later — after B has handled its own Y.
    cluster.net.set_latency(NodeId(0), NodeId(1), link_delay);

    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let subscription = SubscriptionBuilder::new("skip-demo")
        .handler(Recorder(handled.clone()))
        .start(&cluster.execs[1])
        .await
        .unwrap();

    // Let the subscription reach its idle wait.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // X first => smaller cursor; the sleep guarantees a distinct millisecond so
    // the cursor order (X before Y) is deterministic. Different aggregates =>
    // non-conflicting => Accord won't order them.
    let event_x = opened("delayed");
    cluster.execs[0].write(vec![event_x]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let event_y = opened("local");
    cluster.execs[1].write(vec![event_y]).await.unwrap();

    (cluster, subscription, handled)
}

/// THE FIX: with the compaction margin above the link delay, the stability
/// watermark holds B's own event Y back until A's earlier-cursor X has arrived, so
/// both are processed in cursor order — X is not skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_waits_for_a_late_lower_cursor_event() {
    let margin = Duration::from_secs(1);
    let link_delay = Duration::from_millis(300); // < margin
    let (cluster, subscription, handled) = run_late_event_scenario(margin, link_delay).await;

    // Shortly after the writes, before X has crossed the delayed link: Y is in B's
    // store but the watermark is still below it, so nothing is handled yet.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        cluster.count(1, "delayed").await,
        0,
        "X must not have reached B yet (link still delayed)"
    );
    assert_eq!(
        *handled.lock().unwrap(),
        Vec::<String>::new(),
        "the watermark must hold B's own Y back until the prefix is stable"
    );

    // Wait for the watermark to advance past Y (it trails `now` by the margin).
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if handled.lock().unwrap().len() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "subscription never caught up; handled = {:?}",
            *handled.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Both events present, and processed in cursor order — X was NOT skipped.
    assert_eq!(cluster.count(1, "delayed").await, 1);
    assert_eq!(cluster.count(1, "local").await, 1);
    assert_eq!(
        *handled.lock().unwrap(),
        vec!["delayed".to_string(), "local".to_string()],
        "the late lower-cursor event X must be handled, in order before Y"
    );

    subscription.shutdown().await.ok();
}

/// THE BOUNDARY: the watermark is only safe while propagation stays within
/// `compaction_margin`. Violate it (link delay > margin) and the skip returns —
/// B handles Y, advances past it, and X (which arrives even later) is skipped.
/// This documents the assumption rather than a supported configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_returns_when_propagation_exceeds_the_margin() {
    let margin = Duration::from_millis(100);
    let link_delay = Duration::from_millis(800); // > margin: assumption violated
    let (cluster, subscription, handled) = run_late_event_scenario(margin, link_delay).await;

    // Wait until X has finally arrived and been applied to B.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if cluster.count(1, "delayed").await == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "X never reached B");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Give the subscription a chance to (not) process the late X.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(cluster.count(1, "local").await, 1);
    assert_eq!(
        *handled.lock().unwrap(),
        vec!["local".to_string()],
        "with the margin under the propagation delay, the watermark released Y \
         early and the late lower-cursor X was skipped"
    );

    subscription.shutdown().await.ok();
}
