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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
    Topology,
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
    /// Shared fast-path electorate (a subset of the nodes); `None` ⇒ all nodes.
    /// Kept so a restarted node rebuilds with the same topology.
    electorate: Option<Vec<NodeId>>,
}

impl Sim {
    fn build(n: u64) -> Self {
        Self::build_inner(n, None, NodeConfig::default(), None)
    }

    /// Builds a cluster where node `skewed`'s clock runs `skew` microseconds ahead
    /// of the others — to exercise bounded clock-skew handling.
    fn build_with_skew(n: u64, skewed: usize, skew: u64) -> Self {
        Self::build_inner(n, Some((skewed, skew)), NodeConfig::default(), None)
    }

    /// Builds a cluster with a custom [`NodeConfig`] on every node.
    fn build_with_config(n: u64, config: NodeConfig) -> Self {
        Self::build_inner(n, None, config, None)
    }

    /// Builds a cluster with a shared fast-path electorate, to exercise the
    /// shrunk-electorate consensus & recovery paths under the oracles.
    fn build_with_electorate(n: u64, electorate: Vec<NodeId>) -> Self {
        Self::build_inner(n, None, NodeConfig::default(), Some(electorate))
    }

    /// Builds a [`StaticTopology`] for `id`, applying the shared electorate if set.
    fn topology(id: NodeId, ids: &[NodeId], electorate: &Option<Vec<NodeId>>) -> StaticTopology {
        let topology = StaticTopology::new(id, ids.to_vec());
        match electorate {
            Some(e) => topology.with_fast_electorate(e.clone()),
            None => topology,
        }
    }

    fn build_inner(
        n: u64,
        skew: Option<(usize, u64)>,
        config: NodeConfig,
        electorate: Option<Vec<NodeId>>,
    ) -> Self {
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
            let topology = Arc::new(Self::topology(id, &ids, &electorate));
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
            electorate,
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
            Arc::new(Self::topology(id, &self.ids, &self.electorate)),
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
/// random `Ulid::generate()` would otherwise defeat reproducibility. Distinct events
/// still get distinct ids because the counter is monotonic and, under the
/// deterministic runtime, advances in a reproducible order.
fn event(aggregate_id: &str, version: u16, seq: u64) -> Event {
    Event {
        id: ulid::Ulid::from(seq as u128),
        aggregate_type: AGG_TYPE.into(),
        aggregate_id: aggregate_id.into(),
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
async fn run_partition(seed: u64, electorate: Option<Vec<NodeId>>) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let sim = match electorate {
        Some(e) => Sim::build_with_electorate(N, e),
        None => Sim::build(N),
    };
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
        let result = run_partition(seed, None).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (partition) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after partition heal"
        );
    }
}

/// The partition scenario under a **shrunk fast-path electorate** {0,1,2} (= the
/// majority side). Writers coordinate through the majority, so they take the
/// region-local fast path; the stranded minority {3,4} is recovered across the
/// heal. This exercises the shrunk-electorate consensus and the recovery-quorum
/// gate against the same safety + convergence oracles.
#[tokio::test(start_paused = true)]
async fn safety_holds_under_partitions_with_a_shrunk_electorate() {
    const TOTAL: u64 = 20;
    let electorate = vec![NodeId(0), NodeId(1), NodeId(2)];
    for seed in 0..TOTAL {
        let result = run_partition(seed, Some(electorate.clone())).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (partition+electorate) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after partition heal (electorate)"
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

/// Issue #228 regression: a minority stranded during consensus never witnesses
/// the partition-era transactions, so after healing its (unclamped) redundancy
/// report would vouch for a window it wasn't connected for — unblocking
/// cluster-wide compaction before an anti-entropy round can repair it. Once the
/// witnesses truncate, the missing commands can never be shipped and the
/// minority's store is *permanently* divergent (jepsen's
/// `G-single-item-realtime`: a barriered read then serves the stale prefix).
///
/// The scenario forces the race the wrong way on purpose: a short
/// `compaction_margin` with a much slower `anti_entropy_interval` guarantees
/// many watermark rounds fit between the heal and the first repair round.
/// Correct behaviour — enforced by clamping watermark reports to proven sync
/// coverage — is that compaction *waits* for the minority to catch up, so the
/// cluster still converges, a barriered read on a minority node sees every
/// acked write, and compaction then resumes (bounded state).
#[tokio::test(start_paused = true)]
async fn partitioned_minority_cannot_unblock_compaction_and_diverge() {
    const HOT: &str = "hot";
    const PARTITION_WRITES: u16 = 5;

    let sim = Sim::build_with_config(
        5,
        NodeConfig {
            compaction_margin: Duration::from_millis(300),
            anti_entropy_interval: Duration::from_secs(2),
            ..NodeConfig::default()
        },
    );

    // Baseline: healthy-cluster writes still compact away with the coverage
    // clamp in place (liveness of the fix itself).
    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await, "baseline did not converge");
    let mut compacted = false;
    for _ in 0..2000 {
        if sim.nodes.iter().all(|n| n.command_count() == 0) {
            compacted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        compacted,
        "healthy cluster failed to compact (clamp too strict)"
    );

    // Strand the minority, then commit writes it can never witness.
    for &a in &MAJORITY {
        for &b in &MINORITY {
            sim.net.partition(NodeId(a as u64), NodeId(b as u64));
        }
    }
    let mut acked = 3u16;
    for _ in 0..PARTITION_WRITES {
        let version = acked + 1;
        let outcome = sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("partitioned-majority write");
        assert!(!outcome.conflict, "partition-era write conflicted");
        acked = version;
    }

    // Outlast the compaction margin while stranded, then heal and give the
    // watermark sweep (100ms) a long head start on anti-entropy (2s): the
    // unfixed race truncates the partition-era commands right here.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for &a in &MAJORITY {
        for &b in &MINORITY {
            sim.net.heal_partition(NodeId(a as u64), NodeId(b as u64));
        }
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Repair must still be possible: every store converges on the acked writes.
    assert!(
        converge(&sim.stores).await,
        "cluster failed to converge after heal — partition-era writes were compacted away"
    );
    for &m in &MINORITY {
        sim.nodes[m]
            .read_barrier(Key(HOT.into()))
            .await
            .expect("read barrier on healed minority node");
        let version = version_of(&sim.stores[m], HOT).await;
        assert_eq!(
            version, acked,
            "stale barriered read on minority node {m}: {version} < acked {acked}"
        );
    }

    // And the clamp releases once coverage catches up: state stays bounded.
    let mut bounded = false;
    for _ in 0..4000 {
        if sim.nodes.iter().all(|n| n.command_count() == 0) {
            bounded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        bounded,
        "compaction never resumed after the minority caught up"
    );
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

/// Builds a three-node cluster with the fixed recovery timeout disabled (only
/// suspicion can recover anything), warms the failure detectors up with a few
/// writes and heartbeat rounds, then leaves node 0 holding a transaction the
/// other two have pre-accepted.
async fn suspicion_rig() -> Sim {
    let config = NodeConfig {
        recovery_timeout: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let sim = Sim::build_with_config(3, config);
    for seq in 1..=9u64 {
        let coord = (seq % 3) as usize;
        sim.nodes[coord]
            .write(vec![event(&format!("w{seq}"), 1, seq)])
            .await
            .expect("warm-up write");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    sim.nodes[0]
        .coordinate_preaccept(vec![event("a0", 1, 100)])
        .await
        .expect("preaccept");
    sim
}

/// A coordinator that goes quiet for a few hundred milliseconds (a loaded
/// machine, a slow disk) is alive: its peers must not take its transaction over.
#[tokio::test(start_paused = true)]
async fn a_briefly_quiet_coordinator_is_not_suspected() {
    let sim = suspicion_rig().await;
    for peer in [NodeId(1), NodeId(2)] {
        sim.net
            .set_latency(NodeId(0), peer, Duration::from_millis(700));
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        sim.nodes.iter().map(|n| n.recovery_count()).sum::<u64>(),
        0,
        "a coordinator quiet for 700 ms was taken for dead"
    );
}

/// A node that hears from no one for a while (its own inbox or machine stalled)
/// has no evidence against any one peer: it must not suspect the coordinator.
#[tokio::test(start_paused = true)]
async fn a_node_that_hears_from_no_one_suspects_no_one() {
    let sim = suspicion_rig().await;
    for peer in [NodeId(0), NodeId(2)] {
        sim.net.set_latency(peer, NodeId(1), Duration::from_secs(2));
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        sim.nodes[1].recovery_count(),
        0,
        "a node cut off from everyone blamed the coordinator"
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

// ----- membership churn (the metadata log under faults) -------------------------
//
// The cluster reconfigures (a sequence of epoch changes) **while** writers run and
// a minority churns. Every epoch is decided by config-Paxos, appended to the
// durable metadata log, and disseminated; a node that misses epochs (crashed across
// a `ConfigCommit`) must replay them **contiguously** via the metadata-log catch-up
// to converge on the same epoch — no epoch skipped. Membership stays the full five
// nodes in one shard across every epoch (so quorums/ownership are invariant and no
// data bootstrap is needed); what churns is the *epoch sequence*, which is exactly
// what the metadata log must converge. Data-plane safety must hold throughout.

/// A churn run's reproducibility fingerprint: each node's committed set, each
/// node's installed epoch, and the client history — all canonically sorted.
#[derive(PartialEq, Eq, Debug)]
struct ChurnProbe {
    per_node_committed: Vec<Vec<Commit>>,
    per_node_epoch: Vec<u64>,
    history: Vec<(String, u16, bool)>,
}

async fn run_churn(seed: u64) -> ChurnProbe {
    const N: u64 = 5;
    const AGGS: usize = 3;
    const WRITERS: usize = 3;
    const WRITES_EACH: usize = 10;

    let ids: Vec<NodeId> = (0..N).map(NodeId).collect();
    // One shard with every node, identical across epochs — only the epoch advances.
    let layout = vec![ids.clone()];
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();

    let mut nodes: Vec<Node> = Vec::new();
    let mut stores: Vec<Arc<InMemoryDataStore>> = Vec::new();
    let mut topos: Vec<Arc<DynamicTopology>> = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let clock = Arc::new(HybridLogicalClock::with_physical(id, virtual_micros(base)));
        let store = Arc::new(InMemoryDataStore::new());
        let journal = Arc::new(InMemoryJournal::new());
        let topology = Arc::new(DynamicTopology::new(id, 0, layout.clone()));
        let node = Node::new(
            id,
            Arc::clone(&topology) as Arc<dyn Topology>,
            clock,
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        );
        // The recovery loop drives the metadata-log catch-up sweep.
        let _ = (node.start(inbox), node.start_recovery());
        nodes.push(node);
        stores.push(store);
        topos.push(topology);
    }

    // Seed-derived plans.
    let mut rng = StdRng::seed_from_u64(seed);
    let reconfigs: u64 = rng.random_range(3..7);
    let writer_plans: Vec<Vec<usize>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| rng.random_range(0..AGGS))
                .collect()
        })
        .collect();
    // Reconfig schedule: when to drive each epoch change (coordinated by node 0).
    let reconfig_delays: Vec<u64> = (0..reconfigs).map(|_| rng.random_range(2..20)).collect();
    // Fault schedule: churn the minority {3,4}; the quorum {0,1,2} stays live for
    // both the data and the config Paxos.
    let fault_plan: Vec<(u64, NodeId, bool)> = (0..16)
        .map(|_| {
            (
                rng.random_range(2..25),
                NodeId(3 + rng.random_range(0..2)),
                rng.random_bool(0.5),
            )
        })
        .collect();

    let net_f = Arc::clone(&net);
    let fault_task = tokio::spawn(async move {
        for (delay, node, crash) in fault_plan {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if crash {
                net_f.crash(node);
            } else {
                net_f.heal(node);
            }
        }
    });

    // Reconfig task: node 0 drives epochs 1..=reconfigs sequentially.
    let coordinator = nodes[0].clone();
    let reconfig_layout = layout.clone();
    let reconfig_task = tokio::spawn(async move {
        for (i, delay) in reconfig_delays.into_iter().enumerate() {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let epoch = i as u64 + 1;
            let _ = tokio::time::timeout(
                Duration::from_secs(3),
                coordinator.change_topology(epoch, reconfig_layout.clone()),
            )
            .await;
        }
    });

    // Writers coordinate through node 0 (always live).
    let history: History = Arc::new(Mutex::new(Vec::new()));
    let coordinator = nodes[0].clone();
    let store0 = Arc::clone(&stores[0]);
    let id_seq = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = Arc::clone(&store0);
        let history = Arc::clone(&history);
        let id_seq = Arc::clone(&id_seq);
        writers.push(tokio::spawn(async move {
            for agg_idx in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = id_seq.fetch_add(1, Ordering::Relaxed);
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
    let _ = reconfig_task.await;
    let _ = fault_task.await;

    // Heal the minority and let the cluster converge: data (committed sets) and the
    // control plane (every node installs the full epoch sequence via catch-up).
    net.heal(NodeId(3));
    net.heal(NodeId(4));
    converge(&stores).await;
    for topo in &topos {
        for _ in 0..1500 {
            if topo.epoch() >= reconfigs {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let per_node_committed = stores
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
    let per_node_epoch = topos.iter().map(|t| t.epoch()).collect();
    let mut history: Vec<(String, u16, bool)> = history
        .lock()
        .unwrap()
        .iter()
        .map(|r| (r.agg.clone(), r.version, r.committed))
        .collect();
    history.sort();

    ChurnProbe {
        per_node_committed,
        per_node_epoch,
        history,
    }
}

/// Under concurrent reconfiguration, writes, and minority churn: every node
/// converges on the **same final epoch** (the metadata log replayed contiguously,
/// including for nodes that missed epochs), the data converges, and no
/// `(aggregate, version)` was double-committed.
#[tokio::test(start_paused = true)]
async fn membership_churn_converges() {
    for seed in 0..8 {
        let probe = run_churn(seed).await;

        // Control plane: all nodes reach the same, highest epoch.
        let max_epoch = *probe.per_node_epoch.iter().max().unwrap();
        assert!(
            max_epoch >= 3,
            "seed {seed}: reconfigs should have advanced epochs"
        );
        for (node, &e) in probe.per_node_epoch.iter().enumerate() {
            assert_eq!(
                e, max_epoch,
                "seed {seed}: node {node} stuck at epoch {e}, cluster at {max_epoch}"
            );
        }

        // Data plane: every node converged to the same non-empty committed set.
        let first = &probe.per_node_committed[0];
        assert!(!first.is_empty(), "seed {seed}: no commits");
        for (node, set) in probe.per_node_committed.iter().enumerate() {
            assert_eq!(
                set, first,
                "seed {seed}: node {node} diverged on committed set"
            );
        }

        // No double-commit of any (aggregate, version) from the client's view.
        let mut seen = BTreeSet::new();
        for (agg, version, committed) in &probe.history {
            if *committed {
                assert!(
                    seen.insert((agg.clone(), *version)),
                    "seed {seed}: double-commit on {agg} v{version}"
                );
            }
        }
    }
}

/// The churn scenario is bit-reproducible: two runs of a seed produce identical
/// committed state, epochs, and client history (the metadata-log paths — Paxos,
/// replay, the catch-up sweep — are all deterministic under virtual time).
#[tokio::test(start_paused = true)]
async fn membership_churn_is_bit_reproducible() {
    for seed in 0..6 {
        let first = run_churn(seed).await;
        let second = run_churn(seed).await;
        assert_eq!(
            first, second,
            "seed {seed}: two churn runs diverged — not deterministic"
        );
    }
}

/// Aggressive bug-hunt: run **every** scenario across a wide seed range and fail on
/// the first oracle violation (printing the seed, so it replays exactly). Ignored by
/// default — it's a soak, not a CI gate:
///   cargo test -p evento-accord --test simulation -- --ignored --nocapture sweep_for_bugs
#[tokio::test(start_paused = true)]
#[ignore = "soak: run explicitly with --ignored"]
async fn sweep_for_bugs() {
    const COUNT: u64 = 250;
    let electorate = vec![NodeId(0), NodeId(1), NodeId(2)];

    for seed in 0..COUNT {
        // Chaos (crash/heal minority, coordinators crash mid-write) — strong oracle.
        if let Err(v) = run_scenario(seed).await {
            panic!("chaos seed {seed}: {v}");
        }
        // Full chaos + convergence measurement.
        let a = run_adversarial(seed).await;
        assert!(
            a.safety.is_none(),
            "adversarial seed {seed}: {:?}",
            a.safety
        );
        assert!(a.converged, "adversarial seed {seed}: did not converge");
        // Partitions — default electorate and the shrunk {0,1,2} electorate.
        for (label, e) in [
            ("partition", None),
            ("partition+electorate", Some(electorate.clone())),
        ] {
            let p = run_partition(seed, e).await;
            assert!(p.safety.is_none(), "{label} seed {seed}: {:?}", p.safety);
            assert!(p.converged, "{label} seed {seed}: did not converge");
        }
        // Restarts from the durable journal mid-workload.
        let r = run_with_restarts(seed).await;
        assert!(r.safety.is_none(), "restarts seed {seed}: {:?}", r.safety);
        assert!(r.converged, "restarts seed {seed}: did not converge");
        // Compaction boundedness + clock skew.
        if let Err(v) = run_bounded(seed).await {
            panic!("bounded seed {seed}: {v}");
        }
        if let Err(v) = run_skew(seed).await {
            panic!("skew seed {seed}: {v}");
        }
        // Membership churn (concurrent reconfiguration + writes + minority churn).
        let probe = run_churn(seed).await;
        let max_epoch = *probe.per_node_epoch.iter().max().unwrap();
        for (node, &e) in probe.per_node_epoch.iter().enumerate() {
            assert_eq!(
                e, max_epoch,
                "churn seed {seed}: node {node} stuck at epoch {e}/{max_epoch}"
            );
        }
        let first = &probe.per_node_committed[0];
        for (node, set) in probe.per_node_committed.iter().enumerate() {
            assert_eq!(
                set, first,
                "churn seed {seed}: node {node} committed-set diverged"
            );
        }
        let mut seen = std::collections::HashSet::new();
        for (agg, ver, committed) in &probe.history {
            if *committed && !seen.insert((agg.clone(), *ver)) {
                panic!("churn seed {seed}: double-commit {agg} v{ver}");
            }
        }

        if seed % 25 == 0 {
            eprintln!("swept {seed}/{COUNT}");
        }
    }
    eprintln!("swept all {COUNT} seeds with no violation");
}

// ---------------------------------------------------------------------------
// Absent peers: a silent peer stops holding compaction back
// (`NodeConfig::peer_absence_limit`), is counted again if nothing it lacks
// was compacted, and is told to rejoin otherwise.
// ---------------------------------------------------------------------------

impl Sim {
    /// What a host does when node `i` is told to rejoin: tears it down, builds
    /// a fresh node over an **empty** store and journal, and joins through
    /// `contact` before its sweeps start (as `GroupHost::open_joining` does).
    async fn rejoin(&mut self, i: usize, contact: NodeId, config: NodeConfig) {
        let store = Arc::new(InMemoryDataStore::new());
        self.rejoin_over(
            i,
            contact,
            config,
            Arc::clone(&store) as Arc<dyn DataStore>,
            store,
        )
        .await;
    }

    /// [`rejoin`](Self::rejoin) over `backend`, a store wrapping `store`.
    async fn rejoin_over(
        &mut self,
        i: usize,
        contact: NodeId,
        config: NodeConfig,
        backend: Arc<dyn DataStore>,
        store: Arc<InMemoryDataStore>,
    ) {
        for t in &self.tasks[i] {
            t.abort();
        }
        let id = self.ids[i];
        let inbox = self.net.register(id);
        let journal = Arc::new(InMemoryJournal::new());
        let node = Node::new(
            id,
            Arc::new(Self::topology(id, &self.ids, &self.electorate)),
            Arc::new(HybridLogicalClock::with_physical(
                id,
                virtual_micros(self.base),
            )),
            Arc::new(self.net.sink(id)),
            backend,
            Arc::clone(&journal) as Arc<dyn Journal>,
        )
        .with_config(config);
        node.begin_join();
        let inbox_task = node.start(inbox);
        node.join(contact).await.expect("rejoin from a snapshot");
        self.tasks[i] = vec![inbox_task, node.start_recovery()];
        self.nodes[i] = node;
        self.stores[i] = store;
        self.journals[i] = journal;
    }
}

/// A short absence limit, a short margin, and anti-entropy fast enough that
/// coverage re-proves itself within the test budget. One event per snapshot
/// page, so every rejoin in these tests crosses as several pages.
fn absence_config(limit: Duration) -> NodeConfig {
    NodeConfig {
        compaction_margin: Duration::from_millis(300),
        peer_absence_limit: Some(limit),
        snapshot_page_events: 1,
        ..NodeConfig::default()
    }
}

/// Waits until every node in `which` holds no command, or fails.
async fn await_compacted(sim: &Sim, which: &[usize], what: &str) {
    for _ in 0..2000 {
        if which.iter().all(|&i| sim.nodes[i].command_count() == 0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "{what}: commands still held: {:?}",
        which
            .iter()
            .map(|&i| (i, sim.nodes[i].command_count()))
            .collect::<Vec<_>>()
    );
}

async fn await_rejoin_required(node: &Node) {
    for _ in 0..1000 {
        if node.rejoin_required() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("node {:?} was never told to rejoin", node.id());
}

/// A node down past the absence limit no longer holds compaction back: its
/// peers compact the writes it missed. Back, it is told to rejoin (anti-entropy
/// can no longer ship what was compacted), refuses writes meanwhile, and once
/// rejoined from a snapshot it is counted again and the cluster compacts as
/// before.
#[tokio::test(start_paused = true)]
async fn an_absent_peer_stops_holding_compaction_back() {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(3, config);

    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "baseline").await;

    // Node 2 goes down; writes continue on the quorum.
    sim.net.crash(NodeId(2));
    for version in 4..=8u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1], "with node 2 down").await;
    assert_eq!(sim.nodes[0].absent_peers(), vec![NodeId(2)]);
    assert_eq!(sim.nodes[1].absent_peers(), vec![NodeId(2)]);
    assert!(sim.nodes[0].metrics().peers_absent >= 1);

    // Back with its old state, node 2 is behind what its peers compacted.
    sim.net.heal(NodeId(2));
    await_rejoin_required(&sim.nodes[2]).await;
    let err = sim.nodes[2]
        .write(vec![event(HOT, 9, 999)])
        .await
        .expect_err("a node told to rejoin refuses writes");
    assert!(
        err.downcast_ref::<evento_accord::RejoinRequired>()
            .is_some(),
        "unexpected error: {err:#}"
    );
    assert!(sim.nodes[2].metrics().rejoins_required >= 1);
    // Its peers keep refusing to count it: nothing it sends gets it back in.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(sim.nodes[0].absent_peers(), vec![NodeId(2)]);

    // Its host rejoins it from a snapshot — page by page, one event each —
    // and it is counted again; the next writes compact everywhere.
    sim.rejoin(2, NodeId(0), config).await;
    assert!(converge(&sim.stores).await, "the rejoined node converges");
    assert_eq!(version_of(&sim.stores[2], HOT).await, 8);
    assert!(
        sim.nodes[0].metrics().snapshot_pages >= 8,
        "the contact served a page per event: {}",
        sim.nodes[0].metrics().snapshot_pages
    );
    assert_eq!(sim.nodes[2].metrics().joins_partial, 0);
    for version in 9..=10u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 200 + version as u64)])
            .await
            .expect("write after the rejoin");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "after the rejoin").await;
    assert!(sim.nodes[0].absent_peers().is_empty());
    assert!(sim.nodes[1].absent_peers().is_empty());
}

/// [`an_absent_peer_stops_holding_compaction_back`] in a group of `n ≥ 4`,
/// where a report needs coverage from two peers or more: the rejoined node's
/// first report must still count it back in — the contact vouches for the
/// state it copied — rather than have it told to rejoin again.
async fn a_rejoined_peer_is_counted_again(n: u64) {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(n, config);
    let all: Vec<usize> = (0..n as usize).collect();
    let gone = n as usize - 1;
    let survivors: Vec<usize> = (0..gone).collect();

    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "baseline").await;

    sim.net.crash(sim.ids[gone]);
    for version in 4..=8u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &survivors, "with a node down").await;
    for &i in &survivors {
        assert_eq!(sim.nodes[i].absent_peers(), vec![sim.ids[gone]]);
    }
    sim.net.heal(sim.ids[gone]);
    await_rejoin_required(&sim.nodes[gone]).await;

    sim.rejoin(gone, NodeId(0), config).await;
    assert!(converge(&sim.stores).await, "the rejoined node converges");
    assert_eq!(version_of(&sim.stores[gone], HOT).await, 8);
    for _ in 0..500 {
        if survivors
            .iter()
            .all(|&i| sim.nodes[i].absent_peers().is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for &i in &all {
        assert!(
            sim.nodes[i].absent_peers().is_empty(),
            "node {i} counts the rejoined node again: {:?}",
            sim.nodes[i].absent_peers()
        );
    }
    assert!(
        !sim.nodes[gone].rejoin_required(),
        "the rejoined node is not told to rejoin again"
    );
    assert_eq!(sim.nodes[gone].metrics().rejoins_required, 0);

    // It takes part again: the next writes, one coordinated by it, compact
    // everywhere.
    for version in 9..=10u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 200 + version as u64)])
            .await
            .expect("write after the rejoin");
    }
    sim.nodes[gone]
        .write(vec![event(HOT, 11, 300)])
        .await
        .expect("a write coordinated by the rejoined node");
    assert!(converge(&sim.stores).await);
    assert_eq!(version_of(&sim.stores[gone], HOT).await, 11);
    await_compacted(&sim, &all, "after the rejoin").await;
    assert!(!sim.nodes[gone].rejoin_required());
    assert_eq!(sim.nodes[gone].metrics().rejoins_required, 0);
    for &i in &all {
        assert!(sim.nodes[i].absent_peers().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn a_rejoined_peer_is_counted_again_in_a_group_of_four() {
    a_rejoined_peer_is_counted_again(4).await;
}

#[tokio::test(start_paused = true)]
async fn a_rejoined_peer_is_counted_again_in_a_group_of_five() {
    a_rejoined_peer_is_counted_again(5).await;
}

/// A rejoin that takes a while (a slow link to the contact, many pages)
/// while the group keeps writing: the survivors must not compact past what
/// the contact vouched for the joiner in the meantime, or its first report
/// lands below their bound and it is told to rejoin again — every time, for
/// a store large enough.
#[tokio::test(start_paused = true)]
async fn a_peer_rejoining_under_writes_is_counted_again() {
    const HOT: &str = "hot";
    const COLD: &str = "cold";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(4, config);
    let all = [0, 1, 2, 3];

    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "baseline").await;

    sim.net.crash(NodeId(3));
    for version in 4..=12u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1, 2], "with node 3 down").await;
    sim.net.heal(NodeId(3));
    await_rejoin_required(&sim.nodes[3]).await;

    // A page round trip to the contact now takes 200 ms: the dozen pages
    // span several compaction margins, during which node 1 keeps writing.
    sim.net
        .set_link_latency(NodeId(0), NodeId(3), Duration::from_millis(100));
    let writer = sim.nodes[1].clone();
    let writes = tokio::spawn(async move {
        for version in 1..=15u16 {
            writer
                .write(vec![event(COLD, version, 400 + version as u64)])
                .await
                .expect("write during the rejoin");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    sim.rejoin(3, NodeId(0), config).await;
    writes.await.unwrap();

    assert!(converge(&sim.stores).await, "the rejoined node converges");
    assert_eq!(version_of(&sim.stores[3], COLD).await, 15);
    for _ in 0..500 {
        if (0..3).all(|i| sim.nodes[i].absent_peers().is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !sim.nodes[3].rejoin_required(),
        "the rejoined node is not told to rejoin again"
    );
    for i in all {
        assert!(
            sim.nodes[i].absent_peers().is_empty(),
            "node {i} counts the rejoined node again"
        );
    }
    sim.nodes[3]
        .write(vec![event(HOT, 13, 900)])
        .await
        .expect("a write coordinated by the rejoined node");
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "after the rejoin").await;
}

/// A store whose applies take `delay` while `slow` is set: a joiner's slow
/// disk, installing each snapshot page long after it landed.
struct SlowStore {
    inner: Arc<InMemoryDataStore>,
    delay: Duration,
    slow: AtomicBool,
}

#[async_trait::async_trait]
impl DataStore for SlowStore {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        txn: evento_accord::TxnId,
        execute_at: evento_accord::Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if self.slow.load(Ordering::SeqCst) {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.apply(txn, execute_at, events, commit).await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[evento_core::EventFilter]>>,
        routing_key: Option<evento_core::RoutingKey>,
        args: evento_core::cursor::Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<evento_core::cursor::ReadResult<Event>> {
        self.inner
            .read(aggregators, routing_key, args, to_micros)
            .await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        self.inner.snapshot().await
    }

    async fn snapshot_page(
        &self,
        after: Option<evento_core::cursor::Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<evento_accord::SnapshotPage> {
        self.inner.snapshot_page(after, max_events, max_bytes).await
    }
}

/// A joiner whose disk installs each snapshot page slower than its
/// contact's hold lapses without a page request (`collect_timeout × 4`),
/// while the group keeps writing: its keepalives hold the contact's report
/// at the vouch, so no peer compacts past it and the joiner is counted
/// again rather than told to rejoin once more.
#[tokio::test(start_paused = true)]
async fn a_joiner_with_a_slow_disk_keeps_its_contact_holding() {
    const HOT: &str = "hot";
    const COLD: &str = "cold";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(4, config);
    let all = [0, 1, 2, 3];

    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "baseline").await;

    sim.net.crash(NodeId(3));
    for version in 4..=6u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1, 2], "with node 3 down").await;
    sim.net.heal(NodeId(3));
    await_rejoin_required(&sim.nodes[3]).await;

    let lapse = config.collect_timeout * 4;
    let store = Arc::new(InMemoryDataStore::new());
    let slow = Arc::new(SlowStore {
        inner: Arc::clone(&store),
        delay: lapse + Duration::from_secs(5),
        slow: AtomicBool::new(true),
    });
    let writer = sim.nodes[1].clone();
    let writes = tokio::spawn(async move {
        for version in 1..=40u16 {
            writer
                .write(vec![event(COLD, version, 400 + version as u64)])
                .await
                .expect("write during the rejoin");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    sim.rejoin_over(
        3,
        NodeId(0),
        config,
        Arc::clone(&slow) as Arc<dyn DataStore>,
        Arc::clone(&store),
    )
    .await;
    slow.slow.store(false, Ordering::SeqCst);
    writes.await.unwrap();

    assert!(converge(&sim.stores).await, "the rejoined node converges");
    for _ in 0..500 {
        if (0..3).all(|i| sim.nodes[i].absent_peers().is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !sim.nodes[3].rejoin_required(),
        "the slow joiner is not told to rejoin again"
    );
    for i in all {
        assert!(
            sim.nodes[i].absent_peers().is_empty(),
            "node {i} counts the rejoined node again"
        );
    }
}

/// The other side of the vouch: a replica back with its **old** state after
/// its peers compacted past it (never rejoined) must not repair its way back
/// in — whatever its anti-entropy rounds can still fetch, the compacted
/// window is gone. It ends told to rejoin, with nothing imported.
#[tokio::test(start_paused = true)]
async fn a_stale_replica_cannot_sync_its_way_back_in() {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let sim = Sim::build_with_config(4, config);
    let all = [0, 1, 2, 3];

    for version in 1..=2u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "baseline").await;

    sim.net.crash(NodeId(3));
    for version in 3..=6u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1, 2], "with node 3 down").await;
    let before = committed_set(&sim.stores[3]);

    sim.net.heal(NodeId(3));
    await_rejoin_required(&sim.nodes[3]).await;
    // Several anti-entropy intervals later it is still out, still stale.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(sim.nodes[3].rejoin_required());
    assert_eq!(committed_set(&sim.stores[3]), before, "nothing imported");
    assert_eq!(version_of(&sim.stores[3], HOT).await, 2);
    for i in 0..3 {
        assert_eq!(sim.nodes[i].absent_peers(), vec![NodeId(3)]);
    }
}

/// A node down past the limit while nothing was written lacks nothing: back,
/// its first report counts it again, and no rejoin is asked of it.
#[tokio::test(start_paused = true)]
async fn a_peer_back_before_anything_compacted_past_it_is_reinstated() {
    const HOT: &str = "hot";
    let sim = Sim::build_with_config(3, absence_config(Duration::from_secs(2)));

    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "baseline").await;

    sim.net.crash(NodeId(2));
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(sim.nodes[0].absent_peers(), vec![NodeId(2)]);

    sim.net.heal(NodeId(2));
    for _ in 0..500 {
        if sim.nodes[0].absent_peers().is_empty() && sim.nodes[1].absent_peers().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        sim.nodes[0].absent_peers().is_empty(),
        "node 0 counts node 2 again"
    );
    assert!(
        sim.nodes[1].absent_peers().is_empty(),
        "node 1 counts node 2 again"
    );
    assert!(!sim.nodes[2].rejoin_required(), "nothing to rejoin for");
    assert_eq!(sim.nodes[2].metrics().rejoins_required, 0);

    for version in 4..=5u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write after the return");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "after the return").await;
}

/// A partitioned minority never stops waiting for enough peers to compact on
/// its own: with three nodes, the stranded one may give up on one peer, not
/// two. The majority gives it up and compacts; back, it is told to rejoin.
#[tokio::test(start_paused = true)]
async fn a_minority_never_drops_its_way_to_compaction() {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(3, config);

    for version in 1..=2u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "baseline").await;

    sim.net.partition(NodeId(0), NodeId(2));
    sim.net.partition(NodeId(1), NodeId(2));
    for version in 3..=5u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("majority write");
    }
    await_compacted(&sim, &[0, 1], "majority").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        sim.nodes[2].absent_peers().len(),
        1,
        "the stranded node keeps waiting for a quorum's worth of peers"
    );
    assert_eq!(sim.nodes[2].metrics().peers_absent, 1);

    sim.net.heal_all_partitions();
    await_rejoin_required(&sim.nodes[2]).await;
    sim.rejoin(2, NodeId(1), config).await;
    assert!(converge(&sim.stores).await);
    assert_eq!(version_of(&sim.stores[2], HOT).await, 5);
}

/// The bound an absent peer must report above survives the restart of the
/// node that gave it up: a peer back after that restart is still told to
/// rejoin rather than silently counted in with a gap.
#[tokio::test(start_paused = true)]
async fn rejoin_bounds_survive_a_restart() {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(3, config);

    for version in 1..=2u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "baseline").await;

    sim.net.crash(NodeId(2));
    for version in 3..=4u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1], "with node 2 down").await;
    assert_eq!(
        sim.journals[0].load_rejoin_bounds().await.unwrap().len(),
        1,
        "node 0 persisted the bound"
    );

    // Both survivors restart (with the default config: no absence limit), so
    // only the persisted bound can keep node 2 out.
    sim.restart(0).await;
    sim.restart(1).await;
    assert_eq!(sim.nodes[0].absent_peers(), vec![NodeId(2)]);
    assert_eq!(sim.nodes[1].absent_peers(), vec![NodeId(2)]);

    sim.net.heal(NodeId(2));
    await_rejoin_required(&sim.nodes[2]).await;
}

// ---------------------------------------------------------------------------
// The transport's frame cap: a backlog too big for one frame still crosses,
// a page at a time — through anti-entropy and through a join.
// ---------------------------------------------------------------------------

/// A frame cap far below the backlog these tests build, standing in for the
/// TCP transport's 64 MiB (`InMemoryNetwork::set_frame_cap`).
const FRAME_CAP: usize = 64 * 1024;

/// Pages a quarter of the cap: a backlog of [`BACKLOG`] writes needs a dozen.
fn paged_config() -> NodeConfig {
    NodeConfig {
        compaction_margin: Duration::from_millis(300),
        snapshot_page_bytes: 16 * 1024,
        ..NodeConfig::default()
    }
}

/// Writes in the backlog, each of [`heavy_event`]'s 2 KiB: about 200 KiB,
/// three times the frame cap.
const BACKLOG: u64 = 100;

/// An [`event`] carrying 2 KiB of data.
fn heavy_event(aggregate_id: &str, version: u16, seq: u64) -> Event {
    Event {
        data: vec![seq as u8; 2048],
        ..event(aggregate_id, version, seq)
    }
}

/// Writes [`BACKLOG`] heavy events through node 0, round-robin over five
/// aggregates (so each has a version chain), while node `down` is crashed.
async fn write_backlog(sim: &Sim, down: usize) {
    sim.net.crash(sim.ids[down]);
    for seq in 0..BACKLOG {
        let agg = format!("acc-{}", seq % 5);
        let version = (seq / 5 + 1) as u16;
        sim.nodes[0]
            .write(vec![heavy_event(&agg, version, 1000 + seq)])
            .await
            .expect("write with a node down");
    }
}

/// A peer back from a long partition repairs a backlog three times the
/// frame cap through anti-entropy: each reply is a budgeted prefix of what
/// it misses, the next round's digest asks for the rest. Unbounded, the one
/// reply holding all of it would be shed every round, forever — compaction
/// stalled for good.
#[tokio::test(start_paused = true)]
async fn a_healed_peer_repairs_a_backlog_over_the_frame_cap() {
    let sim = Sim::build_with_config(3, paged_config());
    sim.net.set_frame_cap(Some(FRAME_CAP));
    sim.nodes[0]
        .write(vec![event("baseline", 1, 1)])
        .await
        .expect("baseline write");
    assert!(converge(&sim.stores).await);

    write_backlog(&sim, 2).await;
    assert!(
        sim.nodes[0].command_count() as u64 > BACKLOG,
        "nothing compacts while node 2 is down"
    );
    sim.net.heal(NodeId(2));

    assert!(
        converge(&sim.stores).await,
        "the healed node repairs the backlog: {} of {} commands",
        sim.nodes[2].command_count(),
        sim.nodes[0].command_count()
    );
    for agg in 0..5 {
        assert_eq!(
            version_of(&sim.stores[2], &format!("acc-{agg}")).await,
            (BACKLOG / 5) as u16
        );
    }
    // Coverage re-proved over the repair: the cluster compacts it all.
    await_compacted(&sim, &[0, 1, 2], "after the repair").await;
}

/// A join from a contact holding a command backlog three times the frame
/// cap: the header carries none of it, the commands follow page by page.
/// Unbounded, the header would be shed and the join time out against every
/// contact.
#[tokio::test(start_paused = true)]
async fn a_join_pages_a_command_backlog_over_the_frame_cap() {
    let config = paged_config();
    let mut sim = Sim::build_with_config(3, config);
    sim.net.set_frame_cap(Some(FRAME_CAP));
    sim.nodes[0]
        .write(vec![event("baseline", 1, 1)])
        .await
        .expect("baseline write");
    assert!(converge(&sim.stores).await);

    write_backlog(&sim, 2).await;
    sim.net.heal(NodeId(2));
    sim.rejoin(2, NodeId(0), config).await;

    assert!(
        sim.nodes[0].metrics().command_pages >= 3,
        "the backlog crossed in pages: {}",
        sim.nodes[0].metrics().command_pages
    );
    assert!(converge(&sim.stores).await, "the joined node converges");
    for agg in 0..5 {
        assert_eq!(
            version_of(&sim.stores[2], &format!("acc-{agg}")).await,
            (BACKLOG / 5) as u16
        );
    }
    // It takes part: a write it coordinates commits, and the whole backlog
    // compacts everywhere.
    sim.nodes[2]
        .write(vec![event("acc-0", (BACKLOG / 5 + 1) as u16, 5000)])
        .await
        .expect("a write coordinated by the joined node");
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "after the join").await;
    assert_eq!(sim.nodes[2].metrics().joins_partial, 0);
}

/// Rolling upgrade, both ways: a contact predating command pages sheds the
/// paged header request, so the joiner asks again for the inline header —
/// which is also all a joiner predating pages ever asks a current contact
/// for. The join completes, the contact's vouch still counts it back in at
/// a group size needing it, and it takes part again.
#[tokio::test(start_paused = true)]
async fn a_joiner_falls_back_to_the_inline_header_of_a_contact_predating_pages() {
    const HOT: &str = "hot";
    let config = absence_config(Duration::from_secs(2));
    let mut sim = Sim::build_with_config(4, config);
    let all = [0, 1, 2, 3];
    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "baseline").await;

    sim.net.crash(NodeId(3));
    for version in 4..=8u16 {
        sim.nodes[0]
            .write(vec![event(HOT, version, 100 + version as u64)])
            .await
            .expect("write with a node down");
    }
    await_compacted(&sim, &[0, 1, 2], "with node 3 down").await;
    sim.net.heal(NodeId(3));
    await_rejoin_required(&sim.nodes[3]).await;

    sim.net.set_unreadable(Some(Arc::new(|message: &Message| {
        matches!(
            message,
            Message::JoinHeaderRequest { .. }
                | Message::CommandPageRequest { .. }
                | Message::CommandPage { .. }
        )
    })));
    sim.rejoin(3, NodeId(0), config).await;
    assert_eq!(
        sim.nodes[0].metrics().command_pages,
        0,
        "nothing paged: the inline header"
    );
    assert!(converge(&sim.stores).await, "the rejoined node converges");
    assert_eq!(version_of(&sim.stores[3], HOT).await, 8);
    for _ in 0..500 {
        if (0..3).all(|i| sim.nodes[i].absent_peers().is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for i in all {
        assert!(
            sim.nodes[i].absent_peers().is_empty(),
            "node {i} counts the rejoined node again"
        );
    }
    assert!(!sim.nodes[3].rejoin_required());
    sim.nodes[3]
        .write(vec![event(HOT, 9, 900)])
        .await
        .expect("a write coordinated by the rejoined node");
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &all, "after the rejoin").await;
}
