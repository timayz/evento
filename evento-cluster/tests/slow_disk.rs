//! A disk whose syncs stall slows every write of the group, and a write that
//! is only slow is not one whose coordinator is gone (#560): while a node's
//! own journal flushes are slow, its sweep leaves the transactions past
//! `recovery_timeout` to their coordinator, up to `recovery_patience` — a
//! recovery of them decides nothing the coordinator does not, and adds its
//! own flushes to the disk that is the bottleneck. A coordinator that is
//! gone is recovered all the same: when the failure detector suspects it, or
//! at the patience's end; and one that is alive and gave its write up
//! recovers it itself. Under paused time, the journals' fsyncs taking
//! virtual time.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::journal::FaultJournal;
use evento_cluster::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryNetwork, Journal, MessageSink,
    MetricsSnapshot, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::Event;

/// A tracing subscriber over `RUST_LOG`, to debug a test; nothing without it.
fn init_tracing() {
    if std::env::var_os("RUST_LOG").is_none() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .with_writer(std::io::stderr)
        .try_init();
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Order".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Placed".into(),
        ..Default::default()
    }
}

/// A sync of the disk of #558 in one of its episodes: 100 to 250 ms, against
/// 4 to 6 ms otherwise.
const STALLED_SYNC: Duration = Duration::from_millis(165);
const SYNC: Duration = Duration::from_millis(5);

struct Cluster {
    net: Arc<InMemoryNetwork>,
    nodes: Vec<Node>,
    journals: Vec<Arc<FaultJournal>>,
    stores: Vec<Arc<InMemoryDataStore>>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    /// Three nodes over virtual time, their sweeps running, every journal
    /// sync taking `sync`.
    fn start(config: NodeConfig, sync: Duration) -> Self {
        let net = InMemoryNetwork::new();
        let base = tokio::time::Instant::now();
        let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
        let mut cluster = Cluster {
            net: Arc::clone(&net),
            nodes: Vec::new(),
            journals: Vec::new(),
            stores: Vec::new(),
            _loops: Vec::new(),
        };
        for &id in &ids {
            let store = Arc::new(InMemoryDataStore::new());
            let journal = Arc::new(FaultJournal::new());
            journal.set_fsync_delay(sync);
            let node = Node::new(
                id,
                Arc::new(StaticTopology::new(id, ids.clone())),
                Arc::new(HybridLogicalClock::with_physical(id, move || {
                    (tokio::time::Instant::now() - base).as_micros() as u64
                })),
                Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
                Arc::clone(&store) as Arc<dyn DataStore>,
                Arc::clone(&journal) as Arc<dyn Journal>,
            )
            .with_config(config);
            cluster._loops.push(node.start(net.register(id)));
            cluster._loops.push(node.start_recovery());
            cluster.nodes.push(node);
            cluster.journals.push(journal);
            cluster.stores.push(store);
        }
        cluster
    }

    fn set_sync(&self, sync: Duration) {
        for journal in &self.journals {
            journal.set_fsync_delay(sync);
        }
    }

    /// The nodes' counters, added up.
    fn sum(&self, of: impl Fn(&MetricsSnapshot) -> u64) -> u64 {
        self.nodes.iter().map(|node| of(&node.metrics())).sum()
    }

    /// Waits until every store holds `events` events.
    async fn await_applied(&self, events: usize, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if self
                .stores
                .iter()
                .all(|store| store.applied_log().len() >= events)
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stores hold {:?} of {events} events after {within:?}",
                self.stores
                    .iter()
                    .map(|store| store.applied_log().len())
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// What a workload over a stalling disk cost.
#[derive(Debug)]
struct Cost {
    posts: usize,
    recoveries: u64,
    held: u64,
    /// The posts' latencies, sorted.
    latencies: Vec<Duration>,
}

impl Cost {
    fn percentile(&self, p: usize) -> Duration {
        self.latencies[(self.latencies.len() * p / 100).min(self.latencies.len() - 1)]
    }
    fn over(&self, bound: Duration) -> usize {
        self.latencies.iter().filter(|l| **l > bound).count()
    }
}

/// The workload of #558: four writers, spread over the three nodes, each
/// posting `per_second` times a second for `run` — a post is three writes on
/// streams of its own, one behind the other — while the disk stalls for
/// `stall` every `every`: its syncs take [`STALLED_SYNC`] instead of
/// [`SYNC`].
async fn posts_over_a_stalling_disk(
    config: NodeConfig,
    run: Duration,
    stall: Duration,
    every: Duration,
) -> Cost {
    const WRITERS: usize = 4;
    const PERIOD: Duration = Duration::from_millis(200);
    let cluster = Arc::new(Cluster::start(config, SYNC));
    // Let the failure detectors learn the sweeps' rhythm.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let disk = tokio::spawn({
        let cluster = Arc::clone(&cluster);
        async move {
            loop {
                tokio::time::sleep(every - stall).await;
                cluster.set_sync(STALLED_SYNC);
                tokio::time::sleep(stall).await;
                cluster.set_sync(SYNC);
            }
        }
    });
    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let cluster = Arc::clone(&cluster);
        writers.push(tokio::spawn(async move {
            let node = cluster.nodes[writer % cluster.nodes.len()].clone();
            let end = tokio::time::Instant::now() + run;
            let mut latencies = Vec::new();
            let mut post = 0;
            while tokio::time::Instant::now() < end {
                let started = tokio::time::Instant::now();
                for step in 0..3 {
                    let outcome = node
                        .write(vec![event(&format!("order-{writer}-{post}-{step}"), 1)])
                        .await
                        .expect("the write is answered");
                    assert!(!outcome.conflict, "a stream of its own conflicted");
                }
                latencies.push(started.elapsed());
                post += 1;
                tokio::time::sleep_until(started + PERIOD).await;
            }
            latencies
        }));
    }
    let mut latencies = Vec::new();
    for writer in writers {
        latencies.extend(writer.await.expect("a writer panicked"));
    }
    disk.abort();
    cluster.set_sync(SYNC);
    cluster
        .await_applied(latencies.len() * 3, Duration::from_secs(30))
        .await;
    latencies.sort();
    Cost {
        posts: latencies.len(),
        recoveries: cluster.sum(|m| m.recoveries),
        held: cluster.sum(|m| m.recoveries_held),
        latencies,
    }
}

fn without_patience() -> NodeConfig {
    NodeConfig {
        recovery_patience: Duration::ZERO,
        ..NodeConfig::default()
    }
}

/// On a disk that stalls for two seconds, the writes in flight outlast
/// `recovery_timeout` with their coordinators alive. Their peers' sweeps
/// leave them alone: no recovery runs, every write commits, and none takes
/// longer than without the patience.
#[tokio::test(start_paused = true)]
async fn writes_slowed_by_the_disk_are_left_to_their_coordinators() {
    init_tracing();
    let run = Duration::from_secs(20);
    let (stall, every) = (Duration::from_secs(2), Duration::from_secs(5));
    let patient = posts_over_a_stalling_disk(NodeConfig::default(), run, stall, every).await;
    let hasty = posts_over_a_stalling_disk(without_patience(), run, stall, every).await;

    assert!(
        hasty.recoveries > 0 && hasty.held == 0,
        "without the patience the sweeps take the slow writes over: {hasty:?}"
    );
    assert!(patient.held > 0, "nothing was held back: {patient:?}");
    assert_eq!(
        patient.recoveries, 0,
        "a write whose coordinator was alive was recovered"
    );
    assert!(
        patient.posts >= hasty.posts,
        "{} posts with the patience, {} without",
        patient.posts,
        hasty.posts
    );
    assert!(
        patient.percentile(100) <= hasty.percentile(100),
        "the longest post took {:?} with the patience, {:?} without",
        patient.percentile(100),
        hasty.percentile(100)
    );
}

/// On a disk that never stalls nothing is held back, whatever the patience:
/// a write past `recovery_timeout` is recovered as before.
#[tokio::test(start_paused = true)]
async fn on_a_fast_disk_the_sweep_goes_by_the_timeout_alone() {
    init_tracing();
    let cluster = Cluster::start(NodeConfig::default(), SYNC);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (coordinator, others) = (NodeId(0), [NodeId(1), NodeId(2)]);

    // The coordinator's replies never reach it: its write stays undecided,
    // the coordinator alive and beating.
    for other in others {
        cluster.net.cut(other, coordinator);
    }
    let write = tokio::spawn({
        let node = cluster.nodes[0].clone();
        async move { node.write(vec![event("order", 1)]).await }
    });
    tokio::time::sleep(NodeConfig::default().recovery_timeout * 3).await;
    assert!(
        cluster.sum(|m| m.recoveries) > 0,
        "a write past recovery_timeout was not recovered"
    );
    assert_eq!(cluster.sum(|m| m.recoveries_held), 0);
    for other in others {
        cluster.net.restore(other, coordinator);
    }
    let _ = tokio::time::timeout(Duration::from_secs(10), write).await;
    cluster.await_applied(1, Duration::from_secs(10)).await;
}

/// A coordinator that crashes with its write undecided, on a disk that is
/// stalled throughout, is recovered: at once when the failure detector
/// suspects it, and at the patience's end where it cannot tell (too few
/// beats heard to judge a silence).
#[tokio::test(start_paused = true)]
async fn a_coordinator_gone_on_a_slow_disk_is_still_recovered() {
    init_tracing();
    let config = NodeConfig::default();
    for learned in [true, false] {
        let cluster = Cluster::start(config, STALLED_SYNC);
        if learned {
            // The sweeps' gossip teaches each detector its peers' rhythm.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        let write = tokio::spawn({
            let node = cluster.nodes[0].clone();
            async move { node.write(vec![event("order", 1)]).await }
        });
        // Its PreAccept is out, and voted for behind a slow sync.
        tokio::time::sleep(Duration::from_millis(20)).await;
        cluster.net.crash(NodeId(0));
        let crashed = tokio::time::Instant::now();

        // Recovered by a survivor, and in both survivors' stores.
        let recovered = || {
            cluster.nodes[1].metrics().recoveries + cluster.nodes[2].metrics().recoveries > 0
                && cluster.stores[1..]
                    .iter()
                    .all(|store| store.applied_log().len() == 1)
        };
        while !recovered() {
            assert!(
                crashed.elapsed() < config.recovery_patience + Duration::from_secs(5),
                "the crashed coordinator's write was never recovered (learned: {learned})"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let took = crashed.elapsed();
        if learned {
            assert!(
                took < config.recovery_patience,
                "a suspected coordinator waited out the patience: {took:?}"
            );
        }
        write.abort();
    }
}

/// A coordinator that is alive and gave its write up — its caller went away
/// — recovers it itself at `recovery_timeout`: a node has no patience with
/// its own transactions, however slow its disk.
#[tokio::test(start_paused = true)]
async fn a_write_its_caller_gave_up_is_recovered_by_its_own_node() {
    init_tracing();
    let config = NodeConfig::default();
    let cluster = Cluster::start(config, STALLED_SYNC);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let write = tokio::spawn({
        let node = cluster.nodes[0].clone();
        async move { node.write(vec![event("order", 1)]).await }
    });
    // Its PreAccept is out; the peers' votes wait on their slow syncs.
    tokio::time::sleep(Duration::from_millis(20)).await;
    write.abort();
    let given_up = tokio::time::Instant::now();

    cluster.await_applied(1, config.recovery_patience).await;
    assert!(
        given_up.elapsed() < config.recovery_patience / 2,
        "took {:?}",
        given_up.elapsed()
    );
    assert_eq!(
        cluster.nodes[1].metrics().recoveries + cluster.nodes[2].metrics().recoveries,
        0,
        "a peer took over the write of a coordinator it heard from"
    );
    assert!(cluster.nodes[0].metrics().recoveries > 0);
}

/// Not a test: prints what the workload of #558 costs with and without the
/// patience, over a minute of a disk that stalls 2 s in every 8.
///
/// ```text
/// cargo test -p evento-cluster --test slow_disk -- --ignored --nocapture
/// ```
#[tokio::test(start_paused = true)]
#[ignore = "a measurement, not a test"]
async fn sizing() {
    init_tracing();
    let run = Duration::from_secs(60);
    for (stall, every) in [(2, 8), (2, 5), (1, 4)] {
        for (label, config) in [
            ("patient", NodeConfig::default()),
            ("hasty", without_patience()),
        ] {
            let cost = posts_over_a_stalling_disk(
                config,
                run,
                Duration::from_secs(stall),
                Duration::from_secs(every),
            )
            .await;
            println!(
                "stall {stall} s in {every}, {label}: {} posts, p50 {:?}, p99 {:?}, longest {:?}, \
                 {} over 1 s, {} recoveries, {} held",
                cost.posts,
                cost.percentile(50),
                cost.percentile(99),
                cost.percentile(100),
                cost.over(Duration::from_secs(1)),
                cost.recoveries,
                cost.held,
            );
        }
    }
}
