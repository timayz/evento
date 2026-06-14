//! Phase A — simulation harness (first cut).
//!
//! A seeded fault-injection harness that runs a chaotic workload against a
//! cluster and then checks correctness oracles. The scenario (workload + fault
//! schedule) is derived from a seed; faults are crash/heal of a minority of
//! replicas while a fixed live quorum coordinates. This is a scenario the
//! protocol *should* survive safely, so a pass validates the harness and the
//! oracles, and any failure is a real bug.
//!
//! Not yet bit-deterministic (tokio still interleaves tasks); true deterministic
//! simulation (virtual-time runtime) is the next step. The oracles below are the
//! reusable, load-bearing part.
//!
//! ## Oracles
//!
//! After healing and converging the always-live quorum {0,1,2}:
//! - **Agreement:** the quorum nodes agree on every aggregate's final version.
//! - **No double-commit:** no two writes committed the same `(aggregate, version)`.
//! - **No lost / phantom commits:** the committed versions of each aggregate are
//!   exactly `1..=max`, and the stored version equals that max (a gap = a lost
//!   commit; an excess = a phantom).
//! - **No split-brain on churned nodes:** a churned replica never stores a
//!   version beyond the quorum's (it may only lag).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    MessageSink, Node, NodeId, StaticTopology,
};
use evento_core::Event;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const AGG_TYPE: &str = "sim/Account";

struct Sim {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    net: Arc<InMemoryNetwork>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Sim {
    fn build(n: u64) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut loops = Vec::new();
        for &id in &ids {
            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let store = Arc::new(InMemoryDataStore::new());
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                journal,
            );
            loops.push(node.start(inbox));
            loops.push(node.start_recovery());
            nodes.push(node);
            stores.push(store);
        }
        Sim {
            nodes,
            stores,
            net,
            _loops: loops,
        }
    }
}

fn event(aggregator_id: &str, version: u16) -> Event {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(1);
    Event {
        id: ulid::Ulid::new(),
        aggregator_type: AGG_TYPE.into(),
        aggregator_id: aggregator_id.into(),
        version,
        name: "Bumped".into(),
        timestamp: 1,
        timestamp_subsec: SEQ.fetch_add(1, Ordering::SeqCst),
        ..Default::default()
    }
}

/// One client write's outcome, recorded for the oracles.
#[derive(Clone)]
struct WriteRecord {
    agg: String,
    version: u16,
    committed: bool,
}

type History = Arc<Mutex<Vec<WriteRecord>>>;

/// Runs one seeded scenario; returns an error string describing any oracle
/// violation.
async fn run_scenario(seed: u64) -> Result<(), String> {
    const N: u64 = 5; // f = 2, slow quorum = 3
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;

    let sim = Sim::build(N);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    // Pre-generate the seeded plans so the scenario is seed-derived.
    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<usize>> = (0..WRITERS)
        .map(|_| (0..WRITES_EACH).map(|_| rng.random_range(0..AGGS)).collect())
        .collect();
    // Fault schedule: toggle the two churning replicas (nodes 3, 4).
    let fault_plan: Vec<(u64, NodeId, bool)> = (0..16)
        .map(|_| {
            (
                rng.random_range(2..25),               // delay ms
                NodeId(3 + rng.random_range(0..2)),    // node 3 or 4
                rng.random_bool(0.5),                  // crash (true) or heal
            )
        })
        .collect();

    // Fault task: churns nodes 3 and 4. The quorum {0,1,2} stays up.
    let net = Arc::clone(&sim.net);
    let fault_task = tokio::spawn(async move {
        for (delay, node, crash) in fault_plan {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if crash {
                net.crash(node);
            } else {
                net.heal(node);
            }
        }
    });

    // Writer tasks: all coordinate through node 0 (always live), so every write
    // reaches the {0,1,2} quorum and returns a definite outcome.
    let coordinator = sim.nodes[0].clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        writers.push(tokio::spawn(async move {
            for agg_idx in plan {
                let agg = format!("a{agg_idx}");
                let current = store0.version(AGG_TYPE, &agg).await.unwrap_or(0);
                let version = current + 1;
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    coordinator.write(vec![event(&agg, version)]),
                )
                .await
                {
                    history.lock().unwrap().push(WriteRecord {
                        agg,
                        version,
                        committed: !outcome.conflict,
                    });
                }
            }
        }));
    }

    for w in writers {
        let _ = w.await;
    }
    let _ = fault_task.await;

    // Heal everything and let the always-live quorum converge.
    sim.net.heal(NodeId(3));
    sim.net.heal(NodeId(4));
    converge(&sim.stores[0..3]).await;

    let records = history.lock().unwrap().clone();
    check_oracles(&sim, &records)
        .await
        .map_err(|e| format!("seed {seed}: {e}"))
}

/// Waits until the given stores stop changing (a quiescent point to check).
async fn converge(stores: &[Arc<InMemoryDataStore>]) {
    let mut last = vec![0usize; stores.len()];
    for _ in 0..400 {
        let lens: Vec<usize> = stores.iter().map(|s| s.applied_log().len()).collect();
        if lens == last && lens.iter().all(|&l| l > 0) {
            return;
        }
        last = lens;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn version_of(store: &InMemoryDataStore, agg: &str) -> u16 {
    store.version(AGG_TYPE, agg).await.unwrap_or(0)
}

async fn check_oracles(sim: &Sim, history: &[WriteRecord]) -> Result<(), String> {
    let aggs: BTreeSet<String> = history.iter().map(|r| r.agg.clone()).collect();

    for agg in &aggs {
        // Committed versions for this aggregate, from the client's perspective.
        let committed: Vec<u16> = history
            .iter()
            .filter(|r| &r.agg == agg && r.committed)
            .map(|r| r.version)
            .collect();
        let distinct: BTreeSet<u16> = committed.iter().copied().collect();
        if distinct.len() != committed.len() {
            return Err(format!("double-commit on {agg}: versions {committed:?}"));
        }

        // Quorum {0,1,2} must agree on the final version.
        let v0 = version_of(&sim.stores[0], agg).await;
        let v1 = version_of(&sim.stores[1], agg).await;
        let v2 = version_of(&sim.stores[2], agg).await;
        if !(v0 == v1 && v1 == v2) {
            return Err(format!("quorum disagreement on {agg}: {v0}/{v1}/{v2}"));
        }

        // Committed versions must be exactly 1..=max, and the store at max.
        if distinct.is_empty() {
            if v0 != 0 {
                return Err(format!("phantom on {agg}: stored {v0} with no commit"));
            }
        } else {
            let max = *distinct.iter().max().unwrap();
            let expected: BTreeSet<u16> = (1..=max).collect();
            if distinct != expected {
                return Err(format!(
                    "non-contiguous commits on {agg}: {distinct:?} (a lost commit)"
                ));
            }
            if v0 != max {
                return Err(format!("{agg}: stored {v0} but max commit {max}"));
            }
        }

        // Churned nodes must never be ahead of the quorum (no split-brain).
        for n in [3usize, 4] {
            let vn = version_of(&sim.stores[n], agg).await;
            if vn > v0 {
                return Err(format!("phantom on churned node {n} for {agg}: {vn} > {v0}"));
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_replica_churn() {
    for seed in 0..24 {
        if let Err(e) = run_scenario(seed).await {
            panic!("simulation oracle violated: {e}");
        }
    }
}

/// Outcome of one adversarial run.
struct Adversarial {
    /// A safety violation (split-brain / double-commit), if any. Must be `None`.
    safety: Option<String>,
    /// Whether all nodes converged to the same committed set after healing.
    converged: bool,
}

/// Full chaos: writers pick random coordinators while a minority of *any* nodes
/// crash and heal — so coordinators themselves fail mid-write. Safety must hold,
/// and the cluster must converge after healing (via automatic recovery +
/// anti-entropy repair).
async fn run_adversarial(seed: u64) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;
    const MAX_DOWN: usize = 2; // keep a quorum (3 of 5) alive

    let sim = Sim::build(N);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| (rng.random_range(0..AGGS), rng.random_range(0..N as usize)))
                .collect()
        })
        .collect();
    let fault_plan: Vec<(u64, usize)> = (0..40)
        .map(|_| (rng.random_range(1..15), rng.random_range(0..N as usize)))
        .collect();

    // Fault task: toggle random nodes, never exceeding MAX_DOWN crashed.
    let net = Arc::clone(&sim.net);
    let fault_task = tokio::spawn(async move {
        let mut down: BTreeSet<usize> = BTreeSet::new();
        for (delay, node) in fault_plan {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if down.contains(&node) {
                net.heal(NodeId(node as u64));
                down.remove(&node);
            } else if down.len() < MAX_DOWN {
                net.crash(NodeId(node as u64));
                down.insert(node);
            }
        }
    });

    let nodes = sim.nodes.clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version)]),
                )
                .await
                {
                    history.lock().unwrap().push(WriteRecord {
                        agg,
                        version,
                        committed: !outcome.conflict,
                    });
                }
                // Timeouts (a crashed coordinator) are intentionally not recorded.
            }
        }));
    }
    for w in writers {
        let _ = w.await;
    }
    let _ = fault_task.await;

    for id in 0..N {
        sim.net.heal(NodeId(id));
    }
    converge(&sim.stores).await;

    let records = history.lock().unwrap().clone();
    Adversarial {
        safety: safety_violation(&sim, &records),
        converged: all_converged(&sim),
    }
}

/// The core safety oracle: no `(aggregate, version)` is committed by two distinct
/// events anywhere (split-brain), and no client saw two commits for one version.
fn safety_violation(sim: &Sim, history: &[WriteRecord]) -> Option<String> {
    use std::collections::HashMap;
    let mut by_version: HashMap<(String, String, u16), BTreeSet<ulid::Ulid>> = HashMap::new();
    for store in &sim.stores {
        for (key, event_id) in store.committed_events() {
            by_version.entry(key).or_default().insert(event_id);
        }
    }
    for (key, events) in &by_version {
        if events.len() > 1 {
            return Some(format!("split-brain at {key:?}: {} distinct events", events.len()));
        }
    }

    let mut seen: BTreeSet<(String, u16)> = BTreeSet::new();
    for r in history.iter().filter(|r| r.committed) {
        if !seen.insert((r.agg.clone(), r.version)) {
            return Some(format!("client double-commit at ({}, {})", r.agg, r.version));
        }
    }
    None
}

fn all_converged(sim: &Sim) -> bool {
    let first: BTreeSet<_> = sim.stores[0].committed_events().into_iter().collect();
    sim.stores
        .iter()
        .all(|s| s.committed_events().into_iter().collect::<BTreeSet<_>>() == first)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adversarial_safety_holds_and_measures_convergence() {
    const TOTAL: u64 = 20;
    for seed in 0..TOTAL {
        let result = run_adversarial(seed).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION at seed {seed}: {v}");
        }
        // With automatic recovery + anti-entropy repair, the cluster converges
        // even after coordinators crash mid-write and replicas miss transactions.
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after healing"
        );
    }
}
