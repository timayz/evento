//! Subscription ordering on a multi-node `AccordExecutor`, and the stability
//! watermark that makes it safe.
//!
//! A running subscription reads its backend with a forward cursor ordered by the
//! event's `(timestamp, timestamp_subsec, version, id)` — stamped at apply from
//! the transaction's agreed `execute_at`. Accord, however, only orders
//! *conflicting* (same-key) transactions, and a node learns of non-conflicting
//! ones in no particular order. So an event with a *smaller* cursor can be
//! applied to a node *after* its subscription has advanced past a *larger* cursor
//! from a different aggregate written elsewhere — and a naive forward read would
//! **skip** it.
//!
//! [`AccordExecutor`] guards against this via [`Executor::stable_timestamp`]
//! ([`Node::stable_micros`]): the subscription only processes events whose
//! timestamp is below the node's stability watermark — the `applied_through` point
//! that trails `now` by `compaction_margin`, is pinned below any known
//! un-applied transaction, and is clamped to the window anti-entropy has proven
//! with enough peers (a replica cannot vouch for transactions it never
//! witnessed). Below it, everything is applied and nothing lower can still
//! arrive.
//!
//! - `subscription_waits_for_a_late_lower_cursor_event` proves the gate: the late
//!   event is processed in order, not skipped.
//! - `a_late_event_is_not_skipped_even_past_the_margin`: propagation slower than
//!   the margin (a lagging or cut-off replica) no longer releases the earlier
//!   cursor early — the coverage clamp waits for anti-entropy to prove the
//!   window.
//! - `skewed_hosts_cannot_reverse_an_aggregate_s_versions`: the cursor is the
//!   *applied* stamp, re-derived from the transaction's agreed `execute_at` on
//!   every replica, so a writer whose clock lags cannot slip v2 in ahead of v1.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
    InMemoryNetwork, Journal, Message, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::cursor::Args;
use evento_core::subscription::{Context, Handler, SubscriptionBuilder};
use evento_core::{Event, EventFilter, Executor};
use evento_fjall::Fjall;
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

/// Records the `version` of every event the subscription handles, in order.
struct VersionRecorder(Arc<Mutex<Vec<u16>>>);

impl<E: Executor> Handler<E> for VersionRecorder {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        let recorded = self.0.clone();
        let version = event.version;
        Box::pin(async move {
            recorded.lock().unwrap().push(version);
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
        id: Ulid::generate(),
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
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    /// Every node runs its sweep: the stability watermark only advances over
    /// windows anti-entropy has proven. `anti_entropy` paces those rounds.
    fn start(n: u64, compaction_margin: Duration, anti_entropy: Duration) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        let mut execs = Vec::new();
        let mut loops = Vec::new();

        for &id in &ids {
            let fjall = Fjall::temporary().unwrap();

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
                    anti_entropy_interval: anti_entropy,
                    ..Default::default()
                });

            loops.push(node.start(inbox));
            loops.push(node.start_recovery());
            execs.push(AccordExecutor::new(node, fjall));
        }

        Cluster {
            execs,
            net,
            _loops: loops,
        }
    }

    async fn count(&self, node: usize, aggregate_id: &str) -> usize {
        self.execs[node]
            .read(
                Some([EventFilter::by_id_raw("test/Account", aggregate_id)].into()),
                None,
                Args::forward(50, None),
                None,
            )
            .await
            .unwrap()
            .edges
            .len()
    }
}

/// Writes X (aggregate "delayed", lower cursor) via node A and Y (aggregate
/// "local", higher cursor) via node B, with the `A → B` link delayed so B applies
/// its own Y first and X arrives late. `hold_syncs` also drops every
/// anti-entropy reply from before X is written until the caller lifts it with
/// `net.set_unreadable(None)`: otherwise B's sync round with C, which applied
/// X at once, can fetch X before the delayed link delivers it. Returns the
/// cluster, the subscription handle, and the shared record of handled
/// aggregate ids.
async fn run_late_event_scenario(
    margin: Duration,
    link_delay: Duration,
    anti_entropy: Duration,
    hold_syncs: bool,
) -> (
    Cluster,
    evento_core::subscription::Subscription,
    Arc<Mutex<Vec<String>>>,
) {
    // Nodes: A = 0 (writes X), B = 1 (writes Y, runs the subscription), C = 2.
    let cluster = Cluster::start(3, margin, anti_entropy);

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

    if hold_syncs {
        cluster
            .net
            .set_unreadable(Some(Arc::new(|message: &Message| {
                matches!(message, Message::SyncData { .. })
            })));
    }

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

/// The gate: the stability watermark holds B's own event Y back until A's
/// earlier-cursor X has arrived, so both are processed in cursor order — X is
/// not skipped.
///
/// Timings are deliberately generous: a loaded CI runner can oversleep a fixed
/// sleep by hundreds of milliseconds, so the "X has not arrived yet" check is
/// event-driven (poll for Y, which `write` has already applied locally) and the
/// link delay is large enough that scheduler jitter cannot cross it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_waits_for_a_late_lower_cursor_event() {
    let margin = Duration::from_secs(3);
    let link_delay = Duration::from_millis(1500); // < margin, >> scheduler jitter
    let (cluster, subscription, handled) =
        run_late_event_scenario(margin, link_delay, Duration::from_millis(250), true).await;

    // B's own Y is applied locally by the time its `write` returned (the
    // coordinator waits for its own apply); confirm event-driven rather than
    // with a fixed sleep, then check the delayed X is still in flight and the
    // watermark is holding Y back.
    let deadline = Instant::now() + Duration::from_secs(2);
    while cluster.count(1, "local").await == 0 {
        assert!(Instant::now() < deadline, "B never applied its own write Y");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
    // Sync rounds prove the windows the watermark advances over.
    cluster.net.set_unreadable(None);

    // Wait for the watermark to advance past Y (it trails `now` by the margin).
    let deadline = Instant::now() + Duration::from_secs(10);
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

/// Past the margin: the link delay (2 s) dwarfs the margin (200 ms), as for a
/// lagging or partitioned replica. Without the coverage clamp, B releases Y a
/// margin after it applied it, and X — arriving later still — falls behind the
/// subscription's cursor and is skipped for good. With it, B releases nothing
/// past what a sync round has proven, and that round brings X (or X arrives
/// first): both are handled, in order. Anti-entropy is paced (4 s) so that it
/// cannot happen to fetch X before a premature release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_event_is_not_skipped_even_past_the_margin() {
    let margin = Duration::from_millis(200);
    let link_delay = Duration::from_millis(2000); // >> margin
    let (cluster, subscription, handled) =
        run_late_event_scenario(margin, link_delay, Duration::from_secs(4), false).await;

    let deadline = Instant::now() + Duration::from_secs(20);
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
    assert_eq!(cluster.count(1, "delayed").await, 1);
    assert_eq!(cluster.count(1, "local").await, 1);
    assert_eq!(
        *handled.lock().unwrap(),
        vec!["delayed".to_string(), "local".to_string()],
        "the late lower-cursor X must be handled, before Y"
    );

    subscription.shutdown().await.ok();
}

/// Skewed writers: v1 is written via A at the present, v2 via B with a clock
/// 10 s behind. Consensus orders v2 after v1, but a cursor built from the
/// writers' stamps puts v2 first — below the watermark at once, so the
/// subscription handled v2 and then v1. Re-stamped from `execute_at` at apply,
/// the cursor follows the agreed order: v1, then v2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skewed_hosts_cannot_reverse_an_aggregate_s_versions() {
    let cluster = Cluster::start(3, Duration::from_secs(1), Duration::from_millis(250));

    let handled = Arc::new(Mutex::new(Vec::<u16>::new()));
    let subscription = SubscriptionBuilder::new("skew-demo")
        .handler(VersionRecorder(handled.clone()))
        .start(&cluster.execs[1])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let v1 = opened("skewed");
    let mut v2 = opened("skewed");
    v2.version = 2;
    v2.timestamp -= 10;
    cluster.execs[0].write(vec![v1]).await.unwrap();
    cluster.execs[1].write(vec![v2]).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
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
    assert_eq!(
        *handled.lock().unwrap(),
        vec![1, 2],
        "an aggregate's versions must be handled in order"
    );

    subscription.shutdown().await.ok();
}
