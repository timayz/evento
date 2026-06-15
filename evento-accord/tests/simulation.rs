//! Phase A — deterministic simulation harness.
//!
//! A seeded fault-injection harness that runs a chaotic workload against a
//! cluster and then checks correctness oracles. The scenario (workload + fault
//! schedule) is derived from a seed; faults are crash/heal, network partitions,
//! and node restarts. A pass validates the harness and the oracles, and any
//! failure is a real bug.
//!
//! **Deterministic runtime.** The tests run under `#[tokio::test(start_paused =
//! true)]`: a single-threaded runtime with *virtual* time that advances only
//! between scheduling steps. Combined with a virtual physical-time source for the
//! HLC ([`virtual_micros`]) and deterministic event ids ([`event`]), a given seed
//! reproduces *exactly* — proven by [`simulation_is_bit_reproducible`], which
//! runs a seed twice and asserts byte-identical fingerprints. So a failure found
//! at a seed can be replayed and debugged. (The protocol core is already
//! order-independent: every map iteration that feeds a decision is sorted or
//! keyed.) The oracles below are the reusable, load-bearing part.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::Event;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const AGG_TYPE: &str = "sim/Account";

/// A virtual physical-time source for the HLC: microseconds of *virtual* time
/// elapsed since `base`. The tests run under `#[tokio::test(start_paused = true)]`,
/// where tokio's clock advances only deterministically (auto-advancing to the
/// next timer when idle), so every timestamp the cluster issues — and thus the
/// whole run — is bit-reproducible for a given seed.
fn virtual_micros(base: tokio::time::Instant) -> impl Fn() -> u64 + Send + Sync + 'static {
    move || {
        tokio::time::Instant::now()
            .saturating_duration_since(base)
            .as_micros() as u64
    }
}

/// Like [`virtual_micros`] but `skew` microseconds ahead — a node with a fast
/// wall clock, to exercise bounded clock-skew handling.
fn skewed_virtual_micros(
    base: tokio::time::Instant,
    skew: u64,
) -> impl Fn() -> u64 + Send + Sync + 'static {
    move || {
        (tokio::time::Instant::now()
            .saturating_duration_since(base)
            .as_micros() as u64)
            + skew
    }
}

struct Sim {
    ids: Vec<NodeId>,
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    /// Kept across a restart (durable) so a rebuilt node recovers from it.
    journals: Vec<Arc<InMemoryJournal>>,
    net: Arc<InMemoryNetwork>,
    /// Per-node loops (`[inbox, recovery]`), so one node can be torn down alone.
    tasks: Vec<Vec<tokio::task::JoinHandle<()>>>,
    /// The virtual-time origin all node clocks measure from (see [`virtual_micros`]).
    base: tokio::time::Instant,
}

impl Sim {
    fn build(n: u64) -> Self {
        Self::build_inner(n, None, NodeConfig::default())
    }

    /// Builds a cluster where node `skewed`'s clock runs `skew` microseconds ahead
    /// of the others — to exercise bounded clock-skew handling.
    fn build_with_skew(n: u64, skewed: usize, skew: u64) -> Self {
        Self::build_inner(n, Some((skewed, skew)), NodeConfig::default())
    }

    /// Builds a cluster with a custom [`NodeConfig`] on every node.
    fn build_with_config(n: u64, config: NodeConfig) -> Self {
        Self::build_inner(n, None, config)
    }

    fn build_inner(n: u64, skew: Option<(usize, u64)>, config: NodeConfig) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        let base = tokio::time::Instant::now();
        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut journals = Vec::new();
        let mut tasks = Vec::new();
        for &id in &ids {
            let inbox = net.register(id);
            let clock = match skew {
                Some((s, micros)) if id.0 as usize == s => Arc::new(
                    HybridLogicalClock::with_physical(id, skewed_virtual_micros(base, micros)),
                ),
                _ => Arc::new(HybridLogicalClock::with_physical(id, virtual_micros(base))),
            };
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let store = Arc::new(InMemoryDataStore::new());
            let journal = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                Arc::clone(&journal) as Arc<dyn Journal>,
            )
            .with_config(config);
            tasks.push(vec![node.start(inbox), node.start_recovery()]);
            nodes.push(node);
            stores.push(store);
            journals.push(journal);
        }
        Sim {
            ids,
            nodes,
            stores,
            journals,
            net,
            tasks,
            base,
        }
    }

    /// Restarts node `i`: aborts its loops (dropping in-memory state), rebuilds a
    /// fresh [`Node`] + fresh store keeping the durable journal, recovers from the
    /// journal, restarts the loops, and swaps everything into the `Sim`. Mirrors
    /// `tests/restart.rs`.
    async fn restart(&mut self, i: usize) {
        for t in &self.tasks[i] {
            t.abort();
        }
        let id = self.ids[i];
        let inbox = self.net.register(id); // replaces the inbox sender
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, self.ids.clone())),
            Arc::new(HybridLogicalClock::with_physical(
                id,
                virtual_micros(self.base),
            )),
            Arc::new(self.net.sink(id)),
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&self.journals[i]) as Arc<dyn Journal>,
        );
        node.recover_state().await.unwrap();
        self.tasks[i] = vec![node.start(inbox), node.start_recovery()];
        self.nodes[i] = node;
        self.stores[i] = store;
    }
}

/// Builds an event with a *deterministic* id derived from `seq` (a per-scenario
/// counter), so a given seed produces byte-identical events run to run — the
/// random `Ulid::new()` would otherwise defeat reproducibility. Distinct events
/// still get distinct ids because the counter is monotonic and, under the
/// deterministic runtime, advances in a reproducible order.
fn event(aggregator_id: &str, version: u16, seq: u64) -> Event {
    Event {
        id: ulid::Ulid::from(seq as u128),
        aggregator_type: AGG_TYPE.into(),
        aggregator_id: aggregator_id.into(),
        version,
        name: "Bumped".into(),
        timestamp: 1,
        timestamp_subsec: seq as u32,
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
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| rng.random_range(0..AGGS))
                .collect()
        })
        .collect();
    // Fault schedule: toggle the two churning replicas (nodes 3, 4).
    let fault_plan: Vec<(u64, NodeId, bool)> = (0..16)
        .map(|_| {
            (
                rng.random_range(2..25),            // delay ms
                NodeId(3 + rng.random_range(0..2)), // node 3 or 4
                rng.random_bool(0.5),               // crash (true) or heal
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
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for agg_idx in plan {
                let agg = format!("a{agg_idx}");
                let current = store0.version(AGG_TYPE, &agg).await.unwrap_or(0);
                let version = current + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    coordinator.write(vec![event(&agg, version, seq)]),
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
    if !converge(&sim.stores[0..3]).await {
        return Err(format!("seed {seed}: quorum {{0,1,2}} did not converge"));
    }

    let records = history.lock().unwrap().clone();
    check_oracles(&sim, &records)
        .await
        .map_err(|e| format!("seed {seed}: {e}"))
}

/// The set of `(type, id, version) -> event` commits at a store. Two replicas
/// have converged when these sets are identical (the applied *log* length differs
/// legitimately — it also records aborts, which vary per replica — so it is not a
/// convergence signal; the committed set is).
fn committed_set(store: &InMemoryDataStore) -> BTreeSet<((String, String, u16), ulid::Ulid)> {
    store.committed_events().into_iter().collect()
}

/// Waits until the given stores converge to an identical, non-empty committed
/// set — the exact predicate the oracle asserts, so it returns the instant
/// convergence is reached and never on a false quiescence. Returns `false` if the
/// budget (a generous liveness ceiling, not a tuned value) elapses first.
async fn converge(stores: &[Arc<InMemoryDataStore>]) -> bool {
    for _ in 0..1500 {
        let first = committed_set(&stores[0]);
        if !first.is_empty() && stores.iter().all(|s| committed_set(s) == first) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
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
                return Err(format!(
                    "phantom on churned node {n} for {agg}: {vn} > {v0}"
                ));
            }
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
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
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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
    let converged = converge(&sim.stores).await;

    let records = history.lock().unwrap().clone();
    Adversarial {
        safety: safety_violation(&sim, &records),
        converged,
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
            return Some(format!(
                "split-brain at {key:?}: {} distinct events",
                events.len()
            ));
        }
    }

    let mut seen: BTreeSet<(String, u16)> = BTreeSet::new();
    for r in history.iter().filter(|r| r.committed) {
        if !seen.insert((r.agg.clone(), r.version)) {
            return Some(format!(
                "client double-commit at ({}, {})",
                r.agg, r.version
            ));
        }
    }
    None
}

#[tokio::test(start_paused = true)]
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

/// The two cross-cutting groups used by the partition scenario.
const MAJORITY: [usize; 3] = [0, 1, 2];
const MINORITY: [usize; 2] = [3, 4];

/// Full chaos under *network partitions*: the cluster is repeatedly split into a
/// live majority {0,1,2} (always a quorum) and a stranded minority {3,4}, then
/// healed. Writers coordinate only through the majority so the scenario tests
/// whether a stranded minority can cause split-brain or phantom commits — it must
/// not. Safety must hold throughout, and the cluster must converge after healing.
async fn run_partition(seed: u64) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let sim = Sim::build(N);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    // Writers coordinate through majority nodes only (never stranded out of quorum).
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| {
                    (
                        rng.random_range(0..AGGS),
                        MAJORITY[rng.random_range(0..MAJORITY.len())],
                    )
                })
                .collect()
        })
        .collect();
    // Partition schedule: cut (true) or heal (false) the majority|minority divide.
    let partition_plan: Vec<(u64, bool)> = (0..16)
        .map(|_| (rng.random_range(2..25), rng.random_bool(0.5)))
        .collect();

    // Fault task: toggle the partition between {0,1,2} and {3,4}.
    let net = Arc::clone(&sim.net);
    let fault_task = tokio::spawn(async move {
        for (delay, cut) in partition_plan {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            for &a in &MAJORITY {
                for &b in &MINORITY {
                    if cut {
                        net.partition(NodeId(a as u64), NodeId(b as u64));
                    } else {
                        net.heal_partition(NodeId(a as u64), NodeId(b as u64));
                    }
                }
            }
        }
    });

    let nodes = sim.nodes.clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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

    sim.net.heal_all_partitions();
    let converged = converge(&sim.stores).await;

    let records = history.lock().unwrap().clone();
    let safety = match safety_violation(&sim, &records) {
        Some(v) => Some(v),
        None => minority_never_ahead(&sim, &records).await,
    };
    Adversarial { safety, converged }
}

/// Post-heal oracle for the partition scenario: a stranded minority node must
/// never have invented a commit beyond what the majority reached. After healing
/// and convergence, no minority store's version may exceed the majority's max for
/// any aggregate (a strictly-greater minority version is a phantom from a split).
async fn minority_never_ahead(sim: &Sim, history: &[WriteRecord]) -> Option<String> {
    let aggs: BTreeSet<String> = history.iter().map(|r| r.agg.clone()).collect();
    for agg in &aggs {
        let mut maj_max = 0u16;
        for &m in &MAJORITY {
            maj_max = maj_max.max(version_of(&sim.stores[m], agg).await);
        }
        for &m in &MINORITY {
            let vn = version_of(&sim.stores[m], agg).await;
            if vn > maj_max {
                return Some(format!(
                    "phantom on stranded minority node {m} for {agg}: {vn} > majority max {maj_max}"
                ));
            }
        }
    }
    None
}

#[tokio::test(start_paused = true)]
async fn safety_holds_under_partitions() {
    const TOTAL: u64 = 20;
    for seed in 0..TOTAL {
        let result = run_partition(seed).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (partition) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after partition heal"
        );
    }
}

/// Full chaos under *node restarts*: minority nodes {3,4} are repeatedly torn
/// down and rebuilt from their durable journal mid-workload (the `restart.rs`
/// path, now under concurrent multi-writer load across seeds). A restarted node
/// must never resurrect a stale/phantom commit (safety), and recovery +
/// anti-entropy must bring it fully up to date (convergence).
///
/// Restart targets are restricted to the minority {3,4} and writers coordinate
/// only through the never-restarted majority {0,1,2}: a writer holds a `Node`
/// clone for the run, and restarting a node replaces its inbox — so coordinating
/// through a just-restarted node would route replies to the new inbox and hang
/// the old clone's write. Keeping coordinators stable sidesteps that hazard.
async fn run_with_restarts(seed: u64) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let mut sim = Sim::build(N);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| {
                    (
                        rng.random_range(0..AGGS),
                        MAJORITY[rng.random_range(0..MAJORITY.len())],
                    )
                })
                .collect()
        })
        .collect();
    // Restart schedule: which minority node to bounce, after what delay.
    let restart_plan: Vec<(u64, usize)> = (0..8)
        .map(|_| {
            (
                rng.random_range(10..40),
                MINORITY[rng.random_range(0..MINORITY.len())],
            )
        })
        .collect();

    // node 0 is never restarted, so its store handle stays valid for version reads.
    let nodes = sim.nodes.clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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

    // Drive restarts on the main task (Sim::restart needs &mut self), interleaved
    // with the writers running on their own tasks.
    for (delay, node) in restart_plan {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        sim.restart(node).await;
    }
    for w in writers {
        let _ = w.await;
    }

    let converged = converge(&sim.stores).await;

    let records = history.lock().unwrap().clone();
    Adversarial {
        safety: safety_violation(&sim, &records),
        converged,
    }
}

#[tokio::test(start_paused = true)]
async fn safety_holds_under_restarts() {
    const TOTAL: u64 = 20;
    for seed in 0..TOTAL {
        let result = run_with_restarts(seed).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (restart) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after restarts"
        );
    }
}

/// One committed event in a fingerprint: its `(type, id, version)` key and the
/// (deterministic) event id that committed it.
type Commit = ((String, String, u16), u128);

/// A reproducibility fingerprint of a finished run: each node's committed set
/// (with the deterministic event ids) plus the client history, all canonically
/// sorted. Under the deterministic runtime two runs of one seed must produce
/// equal fingerprints.
#[derive(PartialEq, Eq, Debug)]
struct Probe {
    per_node: Vec<Vec<Commit>>,
    history: Vec<(String, u16, bool)>,
}

/// Runs a seeded chaos scenario (random coordinators, a churning minority of any
/// nodes) and returns its [`Probe`] fingerprint after healing and convergence.
async fn run_probe(seed: u64) -> Probe {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 10;
    const MAX_DOWN: usize = 2;

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
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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
    for id in 0..N {
        sim.net.heal(NodeId(id));
    }
    converge(&sim.stores).await;

    let per_node = sim
        .stores
        .iter()
        .map(|s| {
            let mut v: Vec<Commit> = s
                .committed_events()
                .into_iter()
                .map(|(k, id)| (k, u128::from(id)))
                .collect();
            v.sort();
            v
        })
        .collect();
    let mut history: Vec<(String, u16, bool)> = history
        .lock()
        .unwrap()
        .iter()
        .map(|r| (r.agg.clone(), r.version, r.committed))
        .collect();
    history.sort();
    Probe { per_node, history }
}

/// The payoff of the deterministic runtime: a given seed reproduces *exactly* —
/// same commits, same per-node state, same client outcomes — so a failure found
/// at a seed can be replayed and debugged. Running the same chaotic scenario
/// twice must yield byte-identical fingerprints.
#[tokio::test(start_paused = true)]
async fn simulation_is_bit_reproducible() {
    for seed in 0..8 {
        let first = run_probe(seed).await;
        let second = run_probe(seed).await;
        assert_eq!(
            first, second,
            "seed {seed}: two runs diverged — the simulation is not deterministic"
        );
    }
}

/// The compaction payoff: in-memory consensus state is **bounded**, not
/// proportional to lifetime write volume. A workload runs, the cluster heals and
/// goes idle, and once the redundancy watermark advances past every committed
/// transaction (all replicas applied it, plus the propagation margin), every
/// command is compacted away — `command_count` returns to zero — while the
/// committed state (in the data store) is untouched.
async fn run_bounded(seed: u64) -> Result<(), String> {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;

    let sim = Sim::build(N);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<usize>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| rng.random_range(0..AGGS))
                .collect()
        })
        .collect();

    // Writers coordinate through the always-live node 0, so every write commits
    // and applies cluster-wide (no faults — this isolates the compaction claim).
    let coordinator = sim.nodes[0].clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for agg_idx in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    coordinator.write(vec![event(&agg, version, seq)]),
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
    let total_writes = history.lock().unwrap().len();
    assert!(total_writes > 0, "the workload committed nothing");

    converge(&sim.stores).await;

    // Idle: virtual time advances via the recovery sweep until the watermark
    // passes every write (+ margin) and all commands compact away.
    let mut bounded = false;
    for _ in 0..4000 {
        let max = sim
            .nodes
            .iter()
            .map(|n| n.command_count())
            .max()
            .unwrap_or(0);
        if max == 0 {
            bounded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !bounded {
        let counts: Vec<usize> = sim.nodes.iter().map(|n| n.command_count()).collect();
        return Err(format!(
            "seed {seed}: consensus state not bounded after {total_writes} writes: {counts:?}"
        ));
    }

    // Compaction dropped consensus state but not the committed data: the cluster
    // still holds every committed event, and safety was never violated.
    let records = history.lock().unwrap().clone();
    if let Some(v) = safety_violation(&sim, &records) {
        return Err(format!("seed {seed}: {v}"));
    }
    if !converge(&sim.stores).await {
        return Err(format!(
            "seed {seed}: committed state lost after compaction"
        ));
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn consensus_state_stays_bounded() {
    for seed in 0..8 {
        if let Err(e) = run_bounded(seed).await {
            panic!("bound oracle violated: {e}");
        }
    }
}

/// Bounded clock-skew handling: one node's wall clock runs ahead (within the
/// clock's `MAX_SKEW`), and writers coordinate through *every* node — so the
/// skewed node issues future-dated timestamps that the others must witness. The
/// cluster must stay **safe**, **converge**, and keep its consensus state
/// **bounded** (recovery and compaction cutoffs remain valid because witnessing
/// keeps every clock within the bound). Skew is kept under `MAX_SKEW` (200ms) so
/// the bound oracle is clean; the cap on a *beyond*-bound timestamp is covered by
/// the clock unit tests.
async fn run_skew(seed: u64) -> Result<(), String> {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;
    const SKEW_MICROS: u64 = 150_000; // 150ms, within MAX_SKEW (200ms)
    const SKEWED: usize = 2;

    let sim = Sim::build_with_skew(N, SKEWED, SKEW_MICROS);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    // Writers coordinate through random nodes, including the skewed one, so its
    // future-dated timestamps flow through the cluster.
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| (rng.random_range(0..AGGS), rng.random_range(0..N as usize)))
                .collect()
        })
        .collect();

    let nodes = sim.nodes.clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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

    if !converge(&sim.stores).await {
        return Err(format!("seed {seed}: cluster did not converge under skew"));
    }

    // Despite the skew, recovery/compaction stay healthy: state compacts away.
    let mut bounded = false;
    for _ in 0..4000 {
        let max = sim
            .nodes
            .iter()
            .map(|n| n.command_count())
            .max()
            .unwrap_or(0);
        if max == 0 {
            bounded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !bounded {
        let counts: Vec<usize> = sim.nodes.iter().map(|n| n.command_count()).collect();
        return Err(format!(
            "seed {seed}: state not bounded under skew (skew stalled compaction): {counts:?}"
        ));
    }

    let records = history.lock().unwrap().clone();
    if let Some(v) = safety_violation(&sim, &records) {
        return Err(format!("seed {seed}: {v}"));
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn safety_and_bound_hold_under_clock_skew() {
    for seed in 0..8 {
        if let Err(e) = run_skew(seed).await {
            panic!("clock-skew oracle violated: {e}");
        }
    }
}

/// A healthy chaos workload with the fixed recovery timeout effectively disabled
/// (`recovery_timeout = 1h`), so the *only* thing that could trigger recovery is
/// phi-accrual suspicion. Returns the total recoveries performed — which for a
/// healthy cluster (regular heartbeats, no crashes) must be zero. Also checks
/// safety + convergence.
async fn run_healthy_high_timeout(seed: u64) -> Result<u64, String> {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let config = NodeConfig {
        recovery_timeout: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let sim = Sim::build_with_config(N, config);
    let history: History = Arc::new(Mutex::new(Vec::new()));

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| (rng.random_range(0..AGGS), rng.random_range(0..N as usize)))
                .collect()
        })
        .collect();

    let nodes = sim.nodes.clone();
    let store0 = Arc::clone(&sim.stores[0]);
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                if let Ok(Ok(outcome)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    nodes[coord].write(vec![event(&agg, version, seq)]),
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

    if !converge(&sim.stores).await {
        return Err(format!("seed {seed}: cluster did not converge"));
    }
    let records = history.lock().unwrap().clone();
    if let Some(v) = safety_violation(&sim, &records) {
        return Err(format!("seed {seed}: {v}"));
    }
    Ok(sim.nodes.iter().map(|n| n.recovery_count()).sum())
}

/// The detector does not false-positive: a healthy cluster (regular heartbeats,
/// no crashes), even with the fixed timeout disabled, suspects no one — so
/// nothing is recovered.
#[tokio::test(start_paused = true)]
async fn no_spurious_recovery_when_healthy() {
    for seed in 0..8 {
        match run_healthy_high_timeout(seed).await {
            Ok(recoveries) => assert_eq!(
                recoveries, 0,
                "seed {seed}: a healthy cluster must not recover anything"
            ),
            Err(e) => panic!("{e}"),
        }
    }
}

/// A coordinator crashes mid-transaction (pre-accepted but not committed), leaving
/// it stalled at the replicas. With the fixed timeout disabled, **only** the
/// phi-accrual detector can notice the coordinator has gone silent and drive a
/// peer to take the transaction over — which the automatic recovery sweep must do.
#[tokio::test(start_paused = true)]
async fn failure_detector_recovers_a_crashed_coordinator() {
    let config = NodeConfig {
        recovery_timeout: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let sim = Sim::build_with_config(5, config);

    // Warm up: several gossip rounds give the peers a heartbeat baseline for node 0.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node 0 pre-accepts a write across the cluster, then is isolated before it can
    // commit — the transaction is now stalled at the replicas.
    let _txn = sim.nodes[0]
        .coordinate_preaccept(vec![event("a0", 1, 1)])
        .await
        .expect("preaccept");
    sim.net.crash(NodeId(0));

    // The automatic sweep on a peer must suspect node 0 (its heartbeats stopped)
    // and recover the stalled transaction — driven by φ, since the fixed timeout
    // cannot fire for an hour.
    let mut recovered = false;
    for _ in 0..3000 {
        if sim.nodes.iter().map(|n| n.recovery_count()).sum::<u64>() > 0 {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        recovered,
        "the phi-accrual detector did not take over the crashed coordinator's transaction"
    );

    // The recovered decision is enacted consistently across the cluster.
    sim.net.heal(NodeId(0));
    assert!(
        converge(&sim.stores).await,
        "cluster did not converge after phi-driven recovery"
    );
}

/// Observability counters move with real activity: clean writes commit, a raced
/// write conflicts, and the transport/journal counters advance.
#[tokio::test(start_paused = true)]
async fn metrics_track_cluster_activity() {
    let sim = Sim::build(5);

    // Three clean, non-conflicting writes through node 0.
    for v in 1..=3u16 {
        let _ = sim.nodes[0].write(vec![event("acc", v, v as u64)]).await;
    }
    // A race: two coordinators append the same (aggregate, version) — one wins, one
    // conflicts.
    let n0 = sim.nodes[0].clone();
    let n1 = sim.nodes[1].clone();
    let w0 = tokio::spawn(async move { n0.write(vec![event("race", 1, 100)]).await });
    let w1 = tokio::spawn(async move { n1.write(vec![event("race", 1, 101)]).await });
    let _ = w0.await;
    let _ = w1.await;

    converge(&sim.stores).await;

    let sum = |f: fn(&Node) -> u64| -> u64 { sim.nodes.iter().map(f).sum() };
    let committed = sum(|n| n.metrics().writes_committed);
    let conflicted = sum(|n| n.metrics().writes_conflicted);
    let paths = sum(|n| n.metrics().fast_path + n.metrics().slow_path);
    let messages = sum(|n| n.metrics().messages_handled);
    let flushes = sum(|n| n.metrics().journal_flushes);

    assert!(
        committed >= 4,
        "3 clean writes + the race winner should be committed, got {committed}"
    );
    assert!(conflicted >= 1, "the race should produce a conflict");
    assert_eq!(
        paths,
        committed + conflicted,
        "every completed write records exactly one fast/slow path"
    );
    assert!(messages > 0, "replicas handled inbound messages");
    assert!(flushes > 0, "replicas performed journal flushes");
}
