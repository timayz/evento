//! Phase A — deterministic simulation harness.
//!
//! A seeded fault-injection harness that runs a chaotic workload against a
//! cluster and then checks correctness oracles. The scenario (workload + fault
//! schedule) is derived from a seed; faults are crash/heal, network partitions,
//! and node restarts — and, per message, drops, duplicates and delays that
//! reorder a link, with one-way cuts in the partition plan; and failing
//! storage: a restart loses its journal's unflushed tail, fsyncs, truncates
//! and applies fail at seeded rates, and a restarted node may keep its data
//! store — the production `ExecutorDataStore`, over [`SimExecutor`], may come
//! back behind its journal ([`Faults`]). A pass validates the harness and the
//! oracles, and any failure is a real bug.
//!
//! **Deterministic runtime.** The tests run under `#[tokio::test(start_paused =
//! true)]`: a single-threaded runtime with *virtual* time that advances only
//! between scheduling steps. Combined with a virtual physical-time source for the
//! HLC ([`virtual_micros`]) and deterministic events ([`event`]), a given seed
//! reproduces *exactly*. [`every_scenario_kind_replays_its_trace`] proves it on
//! the execution itself: every sim traces its network ([`InMemoryNetwork::enable_trace`]
//! — each message, its ends, its fate, at its virtual time), and two runs of a
//! seed must trace identically, not merely end in the same state. So a failure
//! found at a seed can be replayed and debugged. (The protocol core is
//! order-independent: every map iteration that feeds a decision or a send is
//! sorted or keyed, and its one `select!` is biased.) The oracles below are the
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
//! - **Linearizable reads** (#318): readers on any node — a crashed, cut-off,
//!   restarted or rejoined one too — fence each read with a read barrier,
//!   then read the node's store, as `AccordExecutor` does with
//!   `linearizable_reads` on. Per key, no read sees fewer events than one
//!   that ended before it started, misses a write acknowledged before it
//!   started, or shows a write that returned a conflict or was invoked after
//!   it ended; and every read is a prefix of the converged history
//!   ([`read_violation`]).
//!
//! ## Seeds
//!
//! Every seeded test runs a fixed range by default, the one CI runs. The
//! environment overrides it ([`sim_seeds`], #319):
//! - `SIM_SEED=<n>` replays seed `n`;
//! - `SIM_SEEDS=<count>` runs that many fresh seeds, and
//!   `SIM_DURATION=<secs>` fresh seeds for that long.
//!
//! A failing seed prints the one line that replays it. The `swarm` test
//! (ignored) draws each seed's whole shape — cluster size, which faults are
//! on and how hard, the store, the workload and the `NodeConfig`
//! ([`SwarmConfig`]); the nightly workflow runs it on fresh seeds, and
//! `SIM_PLANT_FAILURE=<seed>` fails that seed on purpose to exercise the
//! reporting. For example:
//!
//! ```text
//! SIM_DURATION=600 cargo test -p evento-accord --release --test simulation swarm -- --exact --ignored --nocapture
//! ```

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod common;

use common::executor::SimExecutor;
use common::journal::{FaultJournal, JournalFaults};
use common::store::{FaultStore, Outcomes};
use evento_accord::{
    DataStore, DynamicTopology, ExecutorDataStore, HybridLogicalClock, InMemoryDataStore,
    InMemoryNetwork, Journal, Key, LinkFilter, Message, MessageFaults, MessageSink, Node,
    NodeConfig, NodeId, PartialSnapshot, StaticTopology, Status, Timestamp, Topology, TraceEntry,
    TxnId, MAX_SKEW_MICROS,
};
use evento_core::metadata::Metadata;
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

thread_local! {
    /// Whether sims built on this thread keep every step of their trace, not
    /// just its hash (see [`assert_replays`]). Thread-local: each test runs
    /// its own single-threaded runtime on its own thread.
    static RECORD_TRACE: Cell<bool> = const { Cell::new(false) };
}

/// A finished run's execution trace (see [`InMemoryNetwork::enable_trace`]):
/// its hash, and every step when recording.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RunTrace {
    hash: u64,
    steps: Vec<TraceEntry>,
}

/// The faults a seeded scenario injects on top of its crash, partition or
/// restart plan (#315). Off by default, so the existing seeds replay as they
/// always have.
#[derive(Clone, Copy, Default)]
struct Faults {
    /// Per-message drop, duplication and delay.
    messages: Option<MessageFaults>,
    /// Whether the partition plan also severs single directions.
    one_way: bool,
    /// Failing storage, and restarts that keep the data store or lose part
    /// of it (#316).
    storage: Option<StorageFaults>,
}

/// Storage faults (#316), drawn per node from the scenario seed.
#[derive(Clone, Copy, Debug)]
struct StorageFaults {
    /// Journal flushes, truncates and direct writes that fail.
    journal: JournalFaults,
    /// The probability a data-store apply fails.
    apply: f64,
    /// How long a journal fsync takes, so concurrent flushes overlap.
    fsync: Duration,
    /// The probability a write to a [`SimExecutor`] store persists what was
    /// written before it: the lower, the longer the tail a crash loses.
    persist: f64,
}

/// A few percent of fsyncs, direct writes and applies fail, one truncate in
/// ten, an fsync takes 2 ms, and a third of the executor store's writes
/// persist the ones before them.
const STORAGE_FAULTS: StorageFaults = StorageFaults {
    journal: JournalFaults {
        flush: 0.02,
        truncate: 0.1,
        metadata: 0.02,
    },
    apply: 0.02,
    fsync: Duration::from_millis(2),
    persist: 0.3,
};

/// A few percent of messages lost or doubled, and every message held back up
/// to 30 ms: below `fast_timeout` (50 ms), so some fast paths miss and the
/// slow path and recovery run, without starving the workload.
const MESSAGE_FAULTS: MessageFaults = MessageFaults {
    drop: 0.03,
    duplicate: 0.03,
    max_delay: Duration::from_millis(30),
};

/// Salts that derive each fault generator from the scenario seed, so turning
/// one on never shifts the draws of the plan, or of another.
const MESSAGE_SALT: u64 = 0x6d65_7373_6167_6573;
const ONE_WAY_SALT: u64 = 0x6f6e_652d_7761_7973;
const JOURNAL_SALT: u64 = 0x6a6f_7572_6e61_6c73;
const APPLY_SALT: u64 = 0x6170_706c_6965_7321;
const PERSIST_SALT: u64 = 0x7065_7273_6973_7473;
const RESTART_SALT: u64 = 0x7265_7374_6172_7473;

impl Faults {
    /// Message faults at [`MESSAGE_FAULTS`], and one-way cuts.
    const ALL: Faults = Faults {
        messages: Some(MESSAGE_FAULTS),
        one_way: true,
        storage: None,
    };

    /// Storage faults at [`STORAGE_FAULTS`] alone.
    const STORAGE: Faults = Faults {
        messages: None,
        one_way: false,
        storage: Some(STORAGE_FAULTS),
    };

    /// Turns the faults on in `sim`, drawn from `seed`.
    fn arm(&self, sim: &mut Sim, seed: u64) {
        if let Some(faults) = self.messages {
            sim.net.set_message_faults(seed ^ MESSAGE_SALT, faults);
        }
        if let Some(faults) = self.storage {
            sim.set_storage_faults(seed, faults);
        }
    }
}

/// Which data store the nodes run on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StoreKind {
    /// `InMemoryDataStore`.
    #[default]
    Memory,
    /// The production `ExecutorDataStore`, over a [`SimExecutor`] (#316).
    Executor,
}

/// A node's data store: what the node writes through — the backend behind
/// seeded apply failures — and the backend itself, which the oracles read.
#[derive(Clone)]
struct SimStore {
    node: Arc<FaultStore>,
    backend: Backend,
}

#[derive(Clone)]
enum Backend {
    Memory(Arc<InMemoryDataStore>),
    Executor(SimExecutor),
}

impl SimStore {
    /// A store of `kind` recording its outcomes in `outcomes`.
    fn new(kind: StoreKind, outcomes: &Arc<Outcomes>) -> Self {
        let (node, backend): (Arc<dyn DataStore>, _) = match kind {
            StoreKind::Memory => {
                let memory = Arc::new(InMemoryDataStore::new());
                (memory.clone(), Backend::Memory(memory))
            }
            StoreKind::Executor => {
                let executor = SimExecutor::default();
                (
                    Arc::new(ExecutorDataStore::new(executor.clone())),
                    Backend::Executor(executor),
                )
            }
        };
        SimStore {
            node: Arc::new(FaultStore::over(node).recording(Arc::clone(outcomes))),
            backend,
        }
    }

    /// What the node is built over.
    fn data_store(&self) -> Arc<dyn DataStore> {
        Arc::clone(&self.node) as Arc<dyn DataStore>
    }

    /// Every `(tenant, type, id, version)` committed here and the id of its
    /// event.
    fn committed_events(&self) -> Vec<((String, String, String, u16), ulid::Ulid)> {
        match &self.backend {
            Backend::Memory(memory) => memory.committed_events(),
            Backend::Executor(executor) => executor.committed_events(),
        }
    }

    /// The ids of `agg`'s committed events here, in version order.
    fn events_of(&self, agg: &str) -> Vec<u64> {
        let mut events: Vec<(u16, u64)> = self
            .committed_events()
            .into_iter()
            .filter(|((_, _, id, _), _)| id == agg)
            .map(|((_, _, _, version), event)| (version, u128::from(event) as u64))
            .collect();
        events.sort_unstable();
        events.into_iter().map(|(_, id)| id).collect()
    }

    /// The aggregate's version as the backend holds it, faults aside.
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        match &self.backend {
            Backend::Memory(memory) => memory.version("", aggregate_type, aggregate_id).await,
            Backend::Executor(executor) => Ok(executor.version_of(aggregate_type, aggregate_id)),
        }
    }
}

/// How a restarted node's data store comes back. Its journal always loses
/// what was staged and not flushed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Restart {
    /// Empty: the node replays its journal into a fresh store — a store that
    /// is not durable (in memory); a durable one lost is a rejoin.
    EmptyStore,
    /// As it was: a SQLite or fjall store survives a restart — possibly
    /// ahead of the journal, holding an apply whose record was not flushed.
    KeepStore,
    /// As it was, less what it had not persisted (a [`SimExecutor`] store
    /// only): a store **behind** its journal, the case #309 suspects.
    KeepStoreLoseTail,
}

/// How [`Sim::build_with`] lays a cluster out.
#[derive(Clone)]
struct SimOptions {
    n: u64,
    /// A node whose clock runs this many microseconds ahead.
    skew: Option<(usize, u64)>,
    config: NodeConfig,
    /// A shared fast-path electorate; `None` ⇒ all nodes.
    electorate: Option<Vec<NodeId>>,
    /// Over [`DynamicTopology`] (not restartable).
    dynamic: bool,
    /// The dynamic topology's starting layout; `None` ⇒ one shard of every
    /// node. A node outside it starts as a spare, to be added.
    layout: Option<Vec<Vec<NodeId>>>,
    store: StoreKind,
}

impl SimOptions {
    fn new(n: u64) -> Self {
        SimOptions {
            n,
            skew: None,
            config: NodeConfig::default(),
            electorate: None,
            dynamic: false,
            layout: None,
            store: StoreKind::Memory,
        }
    }
}

/// Every node and its store as they are now. A restart or a rejoin swaps a
/// node's in, so a task that looks one up per operation reaches the live
/// node: a clone held across a restart would wait for replies on an inbox
/// no loop drains any more.
#[derive(Clone, Default)]
struct Live(Arc<Mutex<Vec<(Node, SimStore)>>>);

impl Live {
    /// Node `i` and its store.
    fn get(&self, i: usize) -> (Node, SimStore) {
        self.0.lock().unwrap()[i].clone()
    }

    fn node(&self, i: usize) -> Node {
        self.get(i).0
    }

    fn set(&self, i: usize, node: Node, store: SimStore) {
        let mut live = self.0.lock().unwrap();
        if i == live.len() {
            live.push((node, store));
        } else {
            live[i] = (node, store);
        }
    }
}

struct Sim {
    ids: Vec<NodeId>,
    nodes: Vec<Node>,
    stores: Vec<SimStore>,
    /// Kept across a restart (durable) so a rebuilt node recovers from it.
    journals: Vec<Arc<FaultJournal>>,
    net: Arc<InMemoryNetwork>,
    /// Per-node loops (`[inbox, recovery]`), so one node can be torn down alone.
    tasks: Vec<Vec<tokio::task::JoinHandle<()>>>,
    /// The virtual-time origin all node clocks measure from (see [`virtual_micros`]).
    base: tokio::time::Instant,
    /// Shared fast-path electorate (a subset of the nodes); `None` ⇒ all nodes.
    /// Kept so a restarted node rebuilds with the same topology.
    electorate: Option<Vec<NodeId>>,
    /// Every node's configuration, kept so a restarted node rebuilds with it.
    config: NodeConfig,
    /// Each node's epoch-versioned topology, for a cluster built by
    /// [`build_dynamic`](Sim::build_dynamic); empty otherwise.
    dynamic: Vec<Arc<DynamicTopology>>,
    /// The data store every node runs on, kept for the ones rebuilt later.
    store_kind: StoreKind,
    /// Seeded storage faults and their seed, kept so a rebuilt node's fresh
    /// store gets them too.
    storage: Option<(u64, StorageFaults)>,
    /// Every transaction's outcome as each node's store enacted it.
    outcomes: Arc<Outcomes>,
    /// The nodes and stores as they are now, for tasks that outlive a restart.
    live: Live,
}

/// A dropped sim stops its nodes: left running, every finished scenario's
/// cluster would keep sweeping alongside the next one.
impl Drop for Sim {
    fn drop(&mut self) {
        for task in self.tasks.iter().flatten() {
            task.abort();
        }
    }
}

impl Sim {
    fn build(n: u64) -> Self {
        Self::build_with(SimOptions::new(n))
    }

    /// Builds a cluster where node `skewed`'s clock runs `skew` microseconds ahead
    /// of the others — to exercise bounded clock-skew handling.
    fn build_with_skew(n: u64, skewed: usize, skew: u64) -> Self {
        Self::build_with(SimOptions {
            skew: Some((skewed, skew)),
            ..SimOptions::new(n)
        })
    }

    /// Builds a cluster with a custom [`NodeConfig`] on every node.
    fn build_with_config(n: u64, config: NodeConfig) -> Self {
        Self::build_with(SimOptions {
            config,
            ..SimOptions::new(n)
        })
    }

    /// Builds a cluster with a shared fast-path electorate, to exercise the
    /// shrunk-electorate consensus & recovery paths under the oracles.
    fn build_with_electorate(n: u64, electorate: Vec<NodeId>) -> Self {
        Self::build_with(SimOptions {
            electorate: Some(electorate),
            ..SimOptions::new(n)
        })
    }

    /// Builds a cluster over [`DynamicTopology`] — one shard of every node,
    /// at epoch 0 — whose epochs the scenario changes. Not restartable.
    fn build_dynamic(n: u64) -> Self {
        Self::build_with(SimOptions {
            dynamic: true,
            ..SimOptions::new(n)
        })
    }

    /// The execution trace so far (see [`RunTrace`]).
    fn trace(&self) -> RunTrace {
        RunTrace {
            hash: self.net.trace_hash().expect("every sim traces"),
            steps: self.net.trace(),
        }
    }

    /// Builds a [`StaticTopology`] for `id`, applying the shared electorate if set.
    fn topology(id: NodeId, ids: &[NodeId], electorate: &Option<Vec<NodeId>>) -> StaticTopology {
        let topology = StaticTopology::new(id, ids.to_vec());
        match electorate {
            Some(e) => topology.with_fast_electorate(e.clone()),
            None => topology,
        }
    }

    fn build_with(options: SimOptions) -> Self {
        let SimOptions {
            n,
            skew,
            config,
            electorate,
            dynamic,
            layout,
            store: store_kind,
        } = options;
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let net = InMemoryNetwork::new();
        net.enable_trace(RECORD_TRACE.with(Cell::get));
        let outcomes = Arc::new(Outcomes::default());
        let base = tokio::time::Instant::now();
        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut journals = Vec::new();
        let mut tasks = Vec::new();
        let mut topologies = Vec::new();
        for &id in &ids {
            let inbox = net.register(id);
            let clock = match skew {
                Some((s, micros)) if id.0 as usize == s => Arc::new(
                    HybridLogicalClock::with_physical(id, skewed_virtual_micros(base, micros)),
                ),
                _ => Arc::new(HybridLogicalClock::with_physical(id, virtual_micros(base))),
            };
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let store = SimStore::new(store_kind, &outcomes);
            let journal = Arc::new(FaultJournal::new());
            let topology: Arc<dyn Topology> = if dynamic {
                let layout = layout.clone().unwrap_or_else(|| vec![ids.clone()]);
                let topology = Arc::new(DynamicTopology::new(id, 0, layout));
                topologies.push(Arc::clone(&topology));
                topology
            } else {
                Arc::new(Self::topology(id, &ids, &electorate))
            };
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                store.data_store(),
                Arc::clone(&journal) as Arc<dyn Journal>,
            )
            .with_config(config);
            tasks.push(vec![node.start(inbox), node.start_recovery()]);
            nodes.push(node);
            stores.push(store);
            journals.push(journal);
        }
        let live = Live::default();
        for (i, (node, store)) in nodes.iter().zip(&stores).enumerate() {
            live.set(i, node.clone(), store.clone());
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
            config,
            dynamic: topologies,
            store_kind,
            storage: None,
            outcomes,
            live,
        }
    }

    /// Turns `faults` on for every node's journal and store, drawn from
    /// `seed`.
    fn set_storage_faults(&mut self, seed: u64, faults: StorageFaults) {
        self.storage = Some((seed, faults));
        for i in 0..self.ids.len() {
            self.arm_journal(i);
            self.arm_store(i, &self.stores[i]);
        }
    }

    /// Stops the storage failures — part of a heal. Fsyncs keep their time
    /// and executor stores their persistence.
    fn clear_storage_faults(&mut self) {
        self.storage = None;
        for journal in &self.journals {
            journal.clear_storage_faults();
        }
        for store in &self.stores {
            store.node.clear_faults();
        }
    }

    /// Each node draws from its own generators, derived from the seed.
    fn node_seed(seed: u64, i: usize) -> u64 {
        seed ^ (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }

    fn arm_journal(&self, i: usize) {
        let Some((seed, faults)) = self.storage else {
            return;
        };
        let seed = Self::node_seed(seed, i);
        self.journals[i].set_storage_faults(seed ^ JOURNAL_SALT, faults.journal);
        self.journals[i].set_fsync_delay(faults.fsync);
    }

    fn arm_store(&self, i: usize, store: &SimStore) {
        let Some((seed, faults)) = self.storage else {
            return;
        };
        let seed = Self::node_seed(seed, i);
        store.node.set_apply_faults(seed ^ APPLY_SALT, faults.apply);
        if let Backend::Executor(executor) = &store.backend {
            executor.set_persistence(seed ^ PERSIST_SALT, faults.persist);
        }
    }

    /// Restarts node `i` with `config`, over a fresh store (see
    /// [`restart_as`](Self::restart_as)).
    async fn restart_with(&mut self, i: usize, config: NodeConfig) {
        self.restart_as(i, config, Restart::EmptyStore).await;
    }

    /// Restarts node `i`: aborts its loops (dropping in-memory state, and
    /// whatever its journal had staged but not flushed), rebuilds a fresh
    /// [`Node`] with `config` over the durable journal and a data store as
    /// `mode` says, recovers from the journal, restarts the loops, and swaps
    /// everything into the `Sim`. Mirrors `tests/restart.rs`.
    async fn restart_as(&mut self, i: usize, config: NodeConfig, mode: Restart) {
        assert!(
            self.dynamic.is_empty(),
            "a restart over a dynamic topology is not modelled"
        );
        for t in &self.tasks[i] {
            t.abort();
        }
        self.journals[i].crash();
        // Nothing durable survived: the node never voted, reported or minted
        // (each makes its journal durable first), so it starts afresh — a
        // store it kept would read as a lost journal's (#312).
        let mode = if self.journals[i].durable_is_pristine().await {
            Restart::EmptyStore
        } else {
            mode
        };
        let store = match mode {
            Restart::EmptyStore => {
                let store = SimStore::new(self.store_kind, &self.outcomes);
                self.arm_store(i, &store);
                store
            }
            Restart::KeepStore => self.stores[i].clone(),
            Restart::KeepStoreLoseTail => {
                let store = self.stores[i].clone();
                match &store.backend {
                    Backend::Executor(executor) => executor.crash(),
                    Backend::Memory(_) => panic!("only an executor store loses a tail"),
                }
                store
            }
        };
        let id = self.ids[i];
        let inbox = self.net.register(id); // replaces the inbox sender
                                           // A recovery its storage fails fails the start, and its host starts
                                           // the node again: the faults are seeded rates, so one soon passes.
        let mut attempts = 0;
        let node = loop {
            let node = Node::new(
                id,
                Arc::new(Self::topology(id, &self.ids, &self.electorate)),
                Arc::new(HybridLogicalClock::with_physical(
                    id,
                    virtual_micros(self.base),
                )),
                Arc::new(self.net.sink(id)),
                store.data_store(),
                Arc::clone(&self.journals[i]) as Arc<dyn Journal>,
            )
            .with_config(config);
            match node.recover_state().await {
                Ok(()) => break node,
                Err(_) if attempts < 100 => attempts += 1,
                Err(err) => panic!("node {i} could not restart: {err:#}"),
            }
        };
        self.tasks[i] = vec![node.start(inbox), node.start_recovery()];
        self.nodes[i] = node;
        self.stores[i] = store;
        self.live
            .set(i, self.nodes[i].clone(), self.stores[i].clone());
    }
}

/// Runs `run` twice for `seed` and requires both runs to trace identical
/// executions: the same messages, between the same nodes, with the same fates,
/// at the same virtual times. Two runs that interleave differently but end in
/// the same state fail here. On a mismatch it runs `run` twice more, recording
/// every step, and reports where those two parted.
async fn assert_replays<F, Fut>(label: &str, seed: u64, run: F)
where
    F: Fn(u64) -> Fut,
    Fut: Future<Output = RunTrace>,
{
    let first = run(seed).await;
    let second = run(seed).await;
    if first.hash == second.hash {
        return;
    }
    RECORD_TRACE.with(|record| record.set(true));
    let a = run(seed).await;
    let b = run(seed).await;
    RECORD_TRACE.with(|record| record.set(false));
    panic!(
        "{label} seed {seed}: two runs traced different executions \
         ({:#x} vs {:#x}) — the simulation is not deterministic\n{}",
        first.hash,
        second.hash,
        first_difference(&a.steps, &b.steps)
    );
}

/// Where two recorded traces part: the first differing step, with the few
/// steps before it for context.
fn first_difference(a: &[TraceEntry], b: &[TraceEntry]) -> String {
    let Some(at) = (0..a.len().max(b.len())).find(|&i| a.get(i) != b.get(i)) else {
        return "the recorded reruns traced identically (an intermittent leak)".into();
    };
    let step = |entry: Option<&TraceEntry>| match entry {
        Some(e) => format!(
            "{}µs {}→{} {:?} {} ({:#x})",
            e.micros, e.from.0, e.to.0, e.fate, e.kind, e.digest
        ),
        None => "(trace ended)".into(),
    };
    let mut out = format!("first difference at step {at}:\n");
    for i in at.saturating_sub(3)..at {
        out.push_str(&format!("    same  {}\n", step(a.get(i))));
    }
    out.push_str(&format!("  run A  {}\n", step(a.get(at))));
    out.push_str(&format!("  run B  {}\n", step(b.get(at))));
    out
}

/// Builds an event with a *deterministic* id derived from `seq` (a per-scenario
/// counter), so a given seed produces byte-identical events run to run — the
/// random `Ulid::generate()` would otherwise defeat reproducibility. Distinct events
/// still get distinct ids because the counter is monotonic and, under the
/// deterministic runtime, advances in a reproducible order. The metadata id is
/// derived from `seq` too: `Metadata::default()` mints a random one, and the
/// event's wire encoding — which the execution trace hashes — carries it.
fn event(aggregate_id: &str, version: u16, seq: u64) -> Event {
    let mut metadata = Metadata::default();
    metadata.id = format!("sim-{seq}");
    Event {
        id: ulid::Ulid::from(seq as u128),
        aggregate_type: AGG_TYPE.into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Bumped".into(),
        timestamp: 1,
        timestamp_subsec: seq as u32,
        metadata,
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

/// A read that returned: on which node, when (virtual time since the sim's
/// base), and the ids of the key's committed events it saw, in version order.
#[derive(Clone, Debug)]
struct ReadObs {
    agg: String,
    node: usize,
    start: Duration,
    end: Duration,
    ids: Vec<u64>,
}

/// A client write: when it was invoked, and — if it returned — when, and
/// whether it committed.
#[derive(Clone, Debug)]
struct WriteObs {
    agg: String,
    start: Duration,
    end: Option<(Duration, bool)>,
}

/// Every read a workload made and every write it invoked, by event id.
#[derive(Default)]
struct Ops {
    reads: Vec<ReadObs>,
    writes: BTreeMap<u64, WriteObs>,
}

/// Readers per workload, and the reads each makes.
const READERS: usize = 3;
const READS_EACH: usize = 12;

/// How long a read's barrier may take before the read counts as unavailable.
const READ_TIMEOUT: Duration = Duration::from_secs(2);

const READ_SALT: u64 = 0x7265_6164_6572_7321;

thread_local! {
    /// Whether reads fence with a read barrier: off, they read the node's
    /// store as it stands, which the read oracle must catch.
    static READ_BARRIER: Cell<bool> = const { Cell::new(true) };
}

/// What a workload's clients record: each write's outcome, for the write
/// oracles, and every operation with its timing, for the read oracle.
#[derive(Clone)]
struct Clients {
    history: History,
    ops: Arc<Mutex<Ops>>,
    base: tokio::time::Instant,
}

impl Clients {
    fn new(sim: &Sim) -> Self {
        Clients {
            history: Arc::new(Mutex::new(Vec::new())),
            ops: Arc::default(),
            base: sim.base,
        }
    }

    fn now(&self) -> Duration {
        tokio::time::Instant::now() - self.base
    }

    /// Writes `agg` at `version` through `node`. The invocation is logged —
    /// an unanswered write may still commit — and, if the write returns
    /// within `timeout`, its outcome.
    async fn write(&self, node: &Node, agg: &str, version: u16, seq: u64, timeout: Duration) {
        let start = self.now();
        self.ops.lock().unwrap().writes.insert(
            seq,
            WriteObs {
                agg: agg.to_string(),
                start,
                end: None,
            },
        );
        let write = node.write(vec![event(agg, version, seq)]);
        if let Ok(Ok(outcome)) = tokio::time::timeout(timeout, write).await {
            let committed = !outcome.conflict;
            let end = self.now();
            if let Some(w) = self.ops.lock().unwrap().writes.get_mut(&seq) {
                w.end = Some((end, committed));
            }
            self.history.lock().unwrap().push(WriteRecord {
                agg: agg.to_string(),
                version,
                committed,
            });
        }
    }

    /// Reads `agg` on node `i` as `AccordExecutor` does with
    /// `linearizable_reads` on: a read barrier, then the node's store. A
    /// barrier that fails or times out observes nothing, like Jepsen's `:info`.
    async fn read(&self, live: &Live, i: usize, agg: &str) {
        let start = self.now();
        let (node, store) = live.get(i);
        if READ_BARRIER.with(Cell::get) {
            let barrier = node.read_barrier(Key(agg.to_string()));
            if !matches!(
                tokio::time::timeout(READ_TIMEOUT, barrier).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
        let ids = store.events_of(agg);
        let end = self.now();
        self.ops.lock().unwrap().reads.push(ReadObs {
            agg: agg.to_string(),
            node: i,
            start,
            end,
            ids,
        });
    }
}

/// Spawns the workload's readers. Each reads a seeded aggregate (of `aggs`)
/// on a seeded node — any node, a crashed or cut-off one too — after a seeded
/// pause, all drawn from its own generator so the scenario's plan is
/// unchanged.
fn spawn_readers(
    sim: &Sim,
    clients: &Clients,
    seed: u64,
    aggs: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut rng = StdRng::seed_from_u64(seed ^ READ_SALT);
    let n = sim.ids.len();
    (0..READERS)
        .map(|_| {
            let plan: Vec<(usize, usize, u64)> = (0..READS_EACH)
                .map(|_| {
                    (
                        rng.random_range(0..aggs),
                        rng.random_range(0..n),
                        rng.random_range(5..80),
                    )
                })
                .collect();
            let live = sim.live.clone();
            let clients = clients.clone();
            tokio::spawn(async move {
                for (agg_idx, node, pause) in plan {
                    tokio::time::sleep(Duration::from_millis(pause)).await;
                    clients.read(&live, node, &format!("a{agg_idx}")).await;
                }
            })
        })
        .collect()
}

/// The read oracle (#318), a seeded form of `linearizable_stress.rs`'s, over
/// every read that returned. A key's events only grow, so per key:
/// - **stale:** no read sees fewer events than one that ended before it
///   started;
/// - **lost:** no read misses a write acknowledged as committed before it
///   started;
/// - **aborted:** no read shows a write that returned as a conflict;
/// - **premature:** no read shows a write invoked after it ended;
/// - **not a prefix:** every read is the start of the key's history in
///   `converged`, a store the cluster converged on.
fn read_violation(clients: &Clients, converged: Option<&SimStore>) -> Option<String> {
    let ops = clients.ops.lock().unwrap();
    let show = |r: &ReadObs| {
        format!(
            "the read of {} on node {} from {:?} to {:?} saw {} events",
            r.agg,
            r.node,
            r.start,
            r.end,
            r.ids.len()
        )
    };
    for b in &ops.reads {
        let earlier = ops
            .reads
            .iter()
            .find(|a| a.agg == b.agg && a.end < b.start && a.ids.len() > b.ids.len());
        if let Some(a) = earlier {
            return Some(format!("stale read: {}; {}", show(b), show(a)));
        }
        for (id, w) in ops.writes.iter().filter(|(_, w)| w.agg == b.agg) {
            if let Some((end, true)) = w.end {
                if end < b.start && !b.ids.contains(id) {
                    return Some(format!(
                        "lost write: {} misses event {id}, acknowledged as committed at {end:?}",
                        show(b)
                    ));
                }
            }
        }
        for id in &b.ids {
            let Some(w) = ops.writes.get(id) else {
                continue;
            };
            if let Some((end, false)) = w.end {
                return Some(format!(
                    "aborted write read: {} shows event {id}, which returned as a conflict at {end:?}",
                    show(b)
                ));
            }
            if w.start > b.end {
                return Some(format!(
                    "premature read: {} shows event {id}, written from {:?}",
                    show(b),
                    w.start
                ));
            }
        }
        if let Some(store) = converged {
            let history = store.events_of(&b.agg);
            if !history.starts_with(&b.ids) {
                return Some(format!(
                    "not a prefix: {} ({:?}), the converged history is {history:?}",
                    show(b),
                    b.ids
                ));
            }
        }
    }
    None
}

/// Runs one seeded scenario; returns its trace, or an error string describing
/// any oracle violation.
async fn run_scenario(seed: u64) -> Result<RunTrace, String> {
    const N: u64 = 5; // f = 2, slow quorum = 3
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;

    let mut sim = Sim::build(N);
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
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = store0.clone();
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
        .map_err(|e| format!("seed {seed}: {e}"))?;
    assert_live(&mut sim, &acked_versions(&records))
        .await
        .map_err(|e| format!("seed {seed}: {e}"))?;
    Ok(sim.trace())
}

/// The set of `(tenant, type, id, version) -> event` commits at a store. Two replicas
/// have converged when these sets are identical (the applied *log* length differs
/// legitimately — it also records aborts, which vary per replica — so it is not a
/// convergence signal; the committed set is).
fn committed_set(store: &SimStore) -> BTreeSet<((String, String, String, u16), ulid::Ulid)> {
    store.committed_events().into_iter().collect()
}

/// Waits until the given stores converge to an identical, non-empty committed
/// set — the exact predicate the oracle asserts, so it returns the instant
/// convergence is reached and never on a false quiescence. Returns `false` if the
/// budget (a generous liveness ceiling, not a tuned value) elapses first.
async fn converge(stores: &[SimStore]) -> bool {
    for _ in 0..1500 {
        let first = committed_set(&stores[0]);
        if !first.is_empty() && stores.iter().all(|s| committed_set(s) == first) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn version_of(store: &SimStore, agg: &str) -> u16 {
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
    for (seed, _replay) in sim_seeds("safety_under_replica_churn", 0..24) {
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
    /// Why the healed cluster did not serve again, if it did not
    /// ([`assert_live`]).
    liveness: Option<String>,
    /// How many reads returned, all checked by [`read_violation`].
    reads: usize,
    /// The run's execution trace.
    trace: RunTrace,
}

impl Adversarial {
    /// Panics on any oracle the run broke, `what` naming the scenario and
    /// seed; returns how many reads it checked.
    fn assert_ok(&self, what: &str) -> usize {
        if let Some(v) = &self.safety {
            panic!("SAFETY VIOLATION ({what}): {v}");
        }
        if let Some(stall) = &self.liveness {
            panic!("{what}: {stall}");
        }
        assert!(
            self.converged,
            "{what}: cluster failed to converge after healing"
        );
        self.reads
    }
}

/// Full chaos: writers pick random coordinators while a minority of *any* nodes
/// crash and heal — so coordinators themselves fail mid-write. Safety must hold,
/// and the cluster must converge after healing (via automatic recovery +
/// anti-entropy repair).
async fn run_adversarial(seed: u64, faults: Faults) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;
    const MAX_DOWN: usize = 2; // keep a quorum (3 of 5) alive

    let mut sim = Sim::build(N);
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);

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

    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let live = sim.live.clone();
        let store0 = store0.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                let node = live.node(coord);
                clients
                    .write(&node, &agg, version, seq, Duration::from_secs(2))
                    .await;
                // A timeout (a crashed coordinator) records no outcome.
            }
        }));
    }
    writers.extend(spawn_readers(&sim, &clients, seed, AGGS));
    for w in writers {
        let _ = w.await;
    }
    let _ = fault_task.await;

    for id in 0..N {
        sim.net.heal(NodeId(id));
    }
    sim.net.clear_message_faults();
    let converged = converge(&sim.stores).await;

    let records = clients.history.lock().unwrap().clone();
    let safety = safety_violation(&sim, &records);
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();
    let converged_store = liveness.is_none().then(|| &sim.stores[0]);
    let safety = safety.or_else(|| read_violation(&clients, converged_store));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

/// The core safety oracle: no transaction is enacted as a commit on one replica
/// and an abort on another (a split decision), no `(aggregate, version)` is
/// committed by two distinct events anywhere (split-brain), and no client saw
/// two commits for one version.
fn safety_violation(sim: &Sim, history: &[WriteRecord]) -> Option<String> {
    let split = sim.outcomes.split();
    if let Some(txn) = split.first() {
        return Some(format!(
            "split decision: {txn:?} enacted as a commit on one replica and an abort \
             on another ({} such)",
            split.len()
        ));
    }
    use std::collections::HashMap;
    let mut by_version: HashMap<(String, String, String, u16), BTreeSet<ulid::Ulid>> =
        HashMap::new();
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
    for (seed, _replay) in sim_seeds(
        "adversarial_safety_holds_and_measures_convergence",
        0..TOTAL,
    ) {
        let result = run_adversarial(seed, Faults::default()).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION at seed {seed}: {v}");
        }
        // With automatic recovery + anti-entropy repair, the cluster converges
        // even after coordinators crash mid-write and replicas miss transactions.
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after healing"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
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
async fn run_partition(seed: u64, electorate: Option<Vec<NodeId>>, faults: Faults) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let mut sim = match electorate {
        Some(e) => Sim::build_with_electorate(N, e),
        None => Sim::build(N),
    };
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);

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
    // With one-way faults, each cut severs both directions, only majority →
    // minority, or only minority → majority — drawn from its own generator,
    // so the plan above is unchanged.
    let mut directions = StdRng::seed_from_u64(seed ^ ONE_WAY_SALT);
    let directions: Vec<u8> = partition_plan
        .iter()
        .map(|_| {
            if faults.one_way {
                directions.random_range(0..3)
            } else {
                0
            }
        })
        .collect();

    // Fault task: toggle the partition between {0,1,2} and {3,4}.
    let net = Arc::clone(&sim.net);
    let fault_task = tokio::spawn(async move {
        for ((delay, cut), direction) in partition_plan.into_iter().zip(directions) {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            for &a in &MAJORITY {
                for &b in &MINORITY {
                    let (a, b) = (NodeId(a as u64), NodeId(b as u64));
                    match (cut, direction) {
                        (false, _) => net.heal_partition(a, b),
                        (true, 1) => net.cut(a, b),
                        (true, 2) => net.cut(b, a),
                        (true, _) => net.partition(a, b),
                    }
                }
            }
        }
    });

    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let live = sim.live.clone();
        let store0 = store0.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                let node = live.node(coord);
                clients
                    .write(&node, &agg, version, seq, Duration::from_secs(2))
                    .await;
            }
        }));
    }
    writers.extend(spawn_readers(&sim, &clients, seed, AGGS));
    for w in writers {
        let _ = w.await;
    }
    let _ = fault_task.await;

    sim.net.heal_all_partitions();
    sim.net.clear_message_faults();
    let converged = converge(&sim.stores).await;

    let records = clients.history.lock().unwrap().clone();
    let safety = match safety_violation(&sim, &records) {
        Some(v) => Some(v),
        None => minority_never_ahead(&sim, &records).await,
    };
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();
    let converged_store = liveness.is_none().then(|| &sim.stores[0]);
    let safety = safety.or_else(|| read_violation(&clients, converged_store));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

/// The highest committed version of each aggregate, as the clients saw them.
fn acked_versions(history: &[WriteRecord]) -> BTreeMap<String, u16> {
    let mut acked = BTreeMap::new();
    for r in history {
        let version = acked.entry(r.agg.clone()).or_insert(0);
        if r.committed {
            *version = (*version).max(r.version);
        }
    }
    acked
}

/// Event ids of [`assert_live`]'s writes, clear of every workload's.
const LIVE_SEQ: u64 = 1_000_000;

/// How many `collect_timeout` rounds a healed cluster may take before it
/// must serve again (see [`assert_live`]).
const SETTLE_ROUNDS: u32 = 20;

impl Sim {
    /// Heals everything a scenario may have broken: crashed nodes, partitions
    /// and cuts, link latency, and the message and storage faults.
    fn heal_all(&mut self) {
        for &id in &self.ids {
            self.net.heal(id);
        }
        self.net.heal_all_partitions();
        self.net.clear_latency();
        self.net.clear_message_faults();
        self.clear_storage_faults();
    }

    /// Rejoins every node told to rejoin, as its host would: through the
    /// peer that told it to ([`Node::rejoin_contact`]), else through any not
    /// told to. A join that fails — its contact told to rejoin meanwhile
    /// answers nothing more (#348) — is made again through the next contact;
    /// one that fails through every contact leaves the node told to rejoin,
    /// for the next call.
    async fn rejoin_told(&mut self, since: tokio::time::Instant) -> Result<(), String> {
        if !self.dynamic.is_empty() {
            return Ok(());
        }
        for i in 0..self.ids.len() {
            if !self.nodes[i].rejoin_required() {
                continue;
            }
            let told_by = self.nodes[i]
                .rejoin_contact()
                .and_then(|peer| self.ids.iter().position(|&id| id == peer));
            let mut contacts: Vec<usize> = told_by.into_iter().collect();
            contacts.extend((0..self.ids.len()).filter(|&j| Some(j) != told_by));
            let mut tried = false;
            for j in contacts {
                if self.nodes[j].rejoin_required() {
                    continue;
                }
                tried = true;
                if self.try_rejoin(i, self.ids[j], self.config).await.is_ok() {
                    break;
                }
            }
            if !tried {
                return Err(self.stalled(i, "rejoin", "every node was told to rejoin", since));
            }
        }
        Ok(())
    }

    /// A liveness failure: which node stalled at which step, and every node's
    /// state.
    fn stalled(&self, node: usize, step: &str, what: &str, since: tokio::time::Instant) -> String {
        let mut out = format!(
            "not live: node {node} stalled at the {step} step, {:?} after the heal: {what}",
            tokio::time::Instant::now() - since
        );
        for (i, n) in self.nodes.iter().enumerate() {
            let m = n.metrics();
            out.push_str(&format!(
                "\n  node {i}: commands={} absent={:?} rejoin_required={} recoveries={} \
                 execution_failures={} committed={}",
                n.command_count(),
                n.absent_peers(),
                n.rejoin_required(),
                m.recoveries,
                m.execution_failures,
                self.stores[i].committed_events().len()
            ));
        }
        out
    }
}

/// What a step of [`assert_live`] failed with: its last attempt's failure,
/// and the one before's, if any. The last attempt only had what was left of
/// the bound; when each one is refused at once — a node told to rejoin on
/// every rejoin (#349) — the one before says why.
fn tried(last: String, attempt: u64, before: Option<String>) -> String {
    match before {
        Some(before) => format!("{last} (attempt {}; the one before: {before})", attempt + 1),
        None => last,
    }
}

/// Liveness once every fault is healed (#317): the cluster serves again. A
/// node dark after a heal (#291), or one whose store stopped growing (#309),
/// can pass every safety oracle; it fails here.
///
/// After [`heal_all`](Sim::heal_all), and rejoining any node told to (as its
/// host would), within bounds derived from the cluster's [`NodeConfig`]:
/// 1. a write each node coordinates commits;
/// 2. a read barrier on every key returns on every node, whose store then
///    holds at least the highest acknowledged version;
/// 3. every node compacts its consensus state away, and the stores agree.
///
/// The bounds are ceilings in virtual time: a live cluster passes at once.
async fn assert_live(sim: &mut Sim, acked: &BTreeMap<String, u16>) -> Result<(), String> {
    sim.heal_all();
    let config = sim.config;
    let n = sim.ids.len() as u32;
    // After a heal, the transactions left stalled are recovered within a few
    // `collect_timeout` rounds, what each one's read waits on first (#326),
    // and a replica that missed applied transactions catches up through
    // anti-entropy, up to 16 pages a round (#348). Over 60,000 swarm seeds,
    // the slowest serves again within 11 rounds. The bounds allow for more,
    // and catch a cluster that never serves again.
    let serve = config.collect_timeout * SETTLE_ROUNDS
        + config.recovery_timeout * 4
        + config.anti_entropy_interval * n;
    let compact = serve
        + (config.compaction_margin
            + config.anti_entropy_interval * (2 * n)
            + config.recovery_interval * 10)
            * 3;
    let start = tokio::time::Instant::now();

    sim.rejoin_told(start).await?;

    // 1. Each node coordinates a write that commits. An attempt that fails
    //    (a quorum not reached while the healed cluster settles) is retried,
    //    each on an aggregate of its own, so a retry never conflicts with an
    //    attempt that committed after all.
    let mut keys: Vec<(String, u16)> = acked
        .iter()
        .map(|(agg, version)| (agg.clone(), *version))
        .collect();
    for i in 0..sim.ids.len() {
        let deadline = tokio::time::Instant::now() + serve;
        let mut attempt = 0;
        let mut before = None;
        loop {
            let agg = format!("live-{i}-{attempt}");
            let seq = LIVE_SEQ + 1000 * i as u64 + attempt;
            let write = sim.nodes[i].write(vec![event(&agg, 1, seq)]);
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let what = match tokio::time::timeout(left, write).await {
                Ok(Ok(outcome)) if !outcome.conflict => {
                    keys.push((agg, 1));
                    break;
                }
                Ok(Ok(_)) => "a fresh aggregate conflicted".to_string(),
                Ok(Err(err)) => format!("{err:#}"),
                Err(_) => format!("no outcome within {left:?}"),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(sim.stalled(i, "write", &tried(what, attempt, before), start));
            }
            before = Some(what);
            // A refusal can come at once (told to rejoin meanwhile): let the
            // cluster move before the next attempt.
            tokio::time::sleep(config.recovery_interval).await;
            sim.rejoin_told(start).await?;
            attempt += 1;
        }
    }

    // 2. A read barrier on every key, on every node — retried while its own
    //    wait (`collect_timeout`) is shorter than the bound.
    for i in 0..sim.ids.len() {
        for (agg, version) in &keys {
            let deadline = tokio::time::Instant::now() + serve;
            let (mut attempt, mut before) = (0, None);
            loop {
                let barrier = sim.nodes[i].read_barrier(Key(agg.clone()));
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                let failed = match tokio::time::timeout(left, barrier).await {
                    Ok(Ok(())) => break,
                    Ok(Err(err)) => format!("on {agg}: {err:#}"),
                    Err(_) => format!("on {agg}: no answer within {left:?}"),
                };
                if tokio::time::Instant::now() >= deadline {
                    let failed = tried(failed, attempt, before);
                    return Err(sim.stalled(i, "read barrier", &failed, start));
                }
                before = Some(failed);
                attempt += 1;
                tokio::time::sleep(config.recovery_interval).await;
                sim.rejoin_told(start).await?;
            }
            let stored = version_of(&sim.stores[i], agg).await;
            if stored < *version {
                let what = format!("{agg} at version {stored} after a barrier, acked {version}");
                return Err(sim.stalled(i, "read", &what, start));
            }
        }
    }

    // 3. Consensus state compacts away everywhere, and the stores agree.
    let deadline = tokio::time::Instant::now() + compact;
    loop {
        let first = committed_set(&sim.stores[0]);
        if sim.nodes.iter().all(|n| n.command_count() == 0)
            && sim.stores.iter().all(|s| committed_set(s) == first)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let laggard = (0..sim.ids.len())
                .find(|&i| sim.nodes[i].command_count() > 0)
                .unwrap_or(0);
            let what = format!("not compacted, or the stores apart, within {compact:?}");
            return Err(sim.stalled(laggard, "compaction", &what, start));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        // A node can be told to rejoin while the others compact: a peer
        // given up meanwhile must be rejoined before it counts again.
        sim.rejoin_told(start).await?;
    }
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
    for (seed, _replay) in sim_seeds("safety_holds_under_partitions", 0..TOTAL) {
        let result = run_partition(seed, None, Faults::default()).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (partition) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after partition heal"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
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
    for (seed, _replay) in sim_seeds(
        "safety_holds_under_partitions_with_a_shrunk_electorate",
        0..TOTAL,
    ) {
        let result = run_partition(seed, Some(electorate.clone()), Faults::default()).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (partition+electorate) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after partition heal (electorate)"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
    }
}

/// Full chaos under *node restarts*: minority nodes {3,4} are repeatedly torn
/// down and rebuilt from their durable journal mid-workload (the `restart.rs`
/// path, now under concurrent multi-writer load across seeds). A restarted node
/// must never resurrect a stale/phantom commit (safety), and recovery +
/// anti-entropy must bring it fully up to date (convergence).
///
/// Restart targets are the minority {3,4}. Writers and readers go through any
/// node, looked up per operation in [`Live`]: a restart replaces the node's
/// inbox, so a clone held across it would wait on replies no loop delivers.
/// An operation in flight on the old node times out, its outcome unknown.
async fn run_with_restarts(seed: u64, faults: Faults, store: StoreKind) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;

    let mut sim = Sim::build_with(SimOptions {
        store,
        ..SimOptions::new(N)
    });
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| (rng.random_range(0..AGGS), rng.random_range(0..N as usize)))
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
    // With storage faults, each restart keeps the store or (in-memory)
    // rebuilds it from the journal — and an executor store may lose its
    // unpersisted tail — drawn from its own generator, so the plan above is
    // unchanged. A durable store never comes back empty from a restart: a
    // lost store is a rejoin.
    let mut modes = StdRng::seed_from_u64(seed ^ RESTART_SALT);
    let restart_modes: Vec<Restart> = restart_plan
        .iter()
        .map(|_| match (faults.storage.is_some(), store) {
            (false, StoreKind::Memory) => Restart::EmptyStore,
            (false, StoreKind::Executor) => Restart::KeepStore,
            (true, StoreKind::Memory) => {
                [Restart::EmptyStore, Restart::KeepStore][modes.random_range(0..2)]
            }
            (true, StoreKind::Executor) => {
                [Restart::KeepStore, Restart::KeepStoreLoseTail][modes.random_range(0..2)]
            }
        })
        .collect();

    // Node 0 is never restarted, so its store handle stays valid for the
    // writers' version guesses.
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let live = sim.live.clone();
        let store0 = store0.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                let node = live.node(coord);
                clients
                    .write(&node, &agg, version, seq, Duration::from_secs(2))
                    .await;
            }
        }));
    }
    writers.extend(spawn_readers(&sim, &clients, seed, AGGS));

    // Drive restarts on the main task (Sim::restart needs &mut self), interleaved
    // with the writers running on their own tasks.
    for ((delay, node), mode) in restart_plan.into_iter().zip(restart_modes) {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        sim.restart_as(node, sim.config, mode).await;
    }
    for w in writers {
        let _ = w.await;
    }

    sim.net.clear_message_faults();
    sim.clear_storage_faults();
    let converged = converge(&sim.stores).await;

    let records = clients.history.lock().unwrap().clone();
    let safety = safety_violation(&sim, &records);
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();
    let converged_store = liveness.is_none().then(|| &sim.stores[0]);
    let safety = safety.or_else(|| read_violation(&clients, converged_store));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

#[tokio::test(start_paused = true)]
async fn safety_holds_under_restarts() {
    const TOTAL: u64 = 20;
    for (seed, _replay) in sim_seeds("safety_holds_under_restarts", 0..TOTAL) {
        let result = run_with_restarts(seed, Faults::default(), StoreKind::Memory).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (restart) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after restarts"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
    }
}

/// A replica that lost its journal comes back under its own node id (#312):
/// while writers and readers run, a node loses its journal and its store — a
/// dead disk — and is reopened joining, one node at a time. Fenced, it takes
/// no part until a contact vouches past everything it may have voted on or
/// reported before; it answers a recovery of a transaction below its fence
/// that it holds nothing of. Safety, the read oracle and liveness after the
/// heal must hold.
///
/// Under message faults the node is wiped only when nothing is in flight:
/// writers pause, and every node holds every transaction it knows applied.
/// A transaction whose recovery is slow (#326) can outlive the compaction
/// margin, past which the replicas that never witnessed it refuse it a vote
/// (#310), leaving a slow quorum that must all take part; wiping one of
/// those leaves it undecided for good — OPERATIONS §7, which no contact can
/// then vouch past. Writes resume while the node rejoins.
async fn run_amnesia(seed: u64, faults: Faults) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 5;
    const WRITES_EACH: usize = 12;
    /// The longest a rejoin may take before the run counts it stalled.
    const REJOIN_LIMIT: Duration = Duration::from_secs(120);
    /// The longest a wipe waits for nothing to be in flight.
    const QUIESCE_LIMIT: Duration = Duration::from_secs(60);

    let mut sim = Sim::build(N);
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);
    let quiesce = faults.messages.is_some();

    let mut rng = StdRng::seed_from_u64(seed);
    let writer_plans: Vec<Vec<(usize, usize)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| (rng.random_range(0..AGGS), rng.random_range(0..N as usize)))
                .collect()
        })
        .collect();
    // Which node loses its journal, after what delay. Never node 0: its
    // store serves the writers' version guesses.
    let amnesia_plan: Vec<(u64, usize)> = (0..3)
        .map(|_| (rng.random_range(20..80), rng.random_range(1..N as usize)))
        .collect();

    // Writers hold this open while they write; a wipe waiting for quiet
    // closes it.
    let gate = Arc::new(tokio::sync::RwLock::new(()));
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let live = sim.live.clone();
        let store0 = store0.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        let gate = Arc::clone(&gate);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord) in plan {
                let _open = gate.read().await;
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                let node = live.node(coord);
                clients
                    .write(&node, &agg, version, seq, Duration::from_secs(2))
                    .await;
            }
        }));
    }
    writers.extend(spawn_readers(&sim, &clients, seed, AGGS));

    let mut stalled = None;
    for (delay, node) in amnesia_plan {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        let paused = if quiesce {
            let paused = gate.write().await;
            let deadline = tokio::time::Instant::now() + QUIESCE_LIMIT;
            while sim.nodes.iter().any(|n| n.sweep_pressure().busy)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(sim.config.recovery_interval).await;
            }
            if sim.nodes.iter().any(|n| n.sweep_pressure().busy) {
                continue;
            }
            Some(paused)
        } else {
            None
        };
        if let Err(err) = sim.lose_journal(node).await {
            stalled = Some(err);
            break;
        }
        drop(paused);
        if let Err(err) = sim.join_amnesiac(node, REJOIN_LIMIT).await {
            stalled = Some(err);
            break;
        }
    }
    for w in writers {
        let _ = w.await;
    }

    sim.net.clear_message_faults();
    sim.clear_storage_faults();
    let converged = converge(&sim.stores).await;

    let records = clients.history.lock().unwrap().clone();
    let safety = safety_violation(&sim, &records);
    let liveness = match stalled {
        Some(stall) => Some(stall),
        None => assert_live(&mut sim, &acked_versions(&records)).await.err(),
    };
    let converged_store = liveness.is_none().then(|| &sim.stores[0]);
    let safety = safety.or_else(|| read_violation(&clients, converged_store));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

#[tokio::test(start_paused = true)]
async fn safety_and_liveness_hold_under_amnesia() {
    for (seed, _replay) in sim_seeds("safety_and_liveness_hold_under_amnesia", 0..20) {
        run_amnesia(seed, Faults::default())
            .await
            .assert_ok(&format!("amnesia at seed {seed}"));
    }
}

#[tokio::test(start_paused = true)]
async fn safety_and_liveness_hold_under_amnesia_and_message_faults() {
    for (seed, _replay) in sim_seeds(
        "safety_and_liveness_hold_under_amnesia_and_message_faults",
        0..20,
    ) {
        run_amnesia(seed, Faults::ALL)
            .await
            .assert_ok(&format!("amnesia +faults at seed {seed}"));
    }
}

/// The adversarial, partition and restart scenarios once more, over links that
/// lose, duplicate and reorder messages, with one-way cuts in the partition
/// plan (#315): the paths whole-node faults barely reach — a late or doubled
/// PreAccept, Accept, Commit or Apply, a reply arriving after its ballot moved
/// on, a recovery racing a slow original coordinator. The same oracles hold.
#[tokio::test(start_paused = true)]
async fn adversarial_safety_holds_under_message_faults() {
    for (seed, _replay) in sim_seeds("adversarial_safety_holds_under_message_faults", 0..20) {
        let result = run_adversarial(seed, Faults::ALL).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (adversarial +faults) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after healing (message faults)"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
    }
}

/// [`adversarial_safety_holds_under_message_faults`] for the partition
/// scenario, under both electorates.
#[tokio::test(start_paused = true)]
async fn safety_holds_under_partitions_and_message_faults() {
    let electorate = vec![NodeId(0), NodeId(1), NodeId(2)];
    for (seed, _replay) in sim_seeds("safety_holds_under_partitions_and_message_faults", 0..20) {
        for (label, e) in [("", None), ("+electorate", Some(electorate.clone()))] {
            let result = run_partition(seed, e, Faults::ALL).await;
            if let Some(v) = result.safety {
                panic!("SAFETY VIOLATION (partition{label} +faults) at seed {seed}: {v}");
            }
            assert!(
                result.converged,
                "seed {seed}: cluster failed to converge after partition heal ({label} faults)"
            );
            if let Some(stall) = result.liveness {
                panic!("seed {seed}: {stall}");
            }
        }
    }
}

/// [`adversarial_safety_holds_under_message_faults`] for the restart scenario.
#[tokio::test(start_paused = true)]
async fn safety_holds_under_restarts_and_message_faults() {
    for (seed, _replay) in sim_seeds("safety_holds_under_restarts_and_message_faults", 0..20) {
        let result = run_with_restarts(seed, Faults::ALL, StoreKind::Memory).await;
        if let Some(v) = result.safety {
            panic!("SAFETY VIOLATION (restart +faults) at seed {seed}: {v}");
        }
        assert!(
            result.converged,
            "seed {seed}: cluster failed to converge after restarts (message faults)"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
    }
}

/// The restart scenario over failing storage (#316): every restart loses what
/// the node's journal staged and had not flushed; fsyncs, truncates, direct
/// journal writes and applies fail at seeded rates; and about half the
/// restarted nodes keep their data store, possibly ahead of their journal. On
/// the production `ExecutorDataStore` a kept store may also lose its
/// unpersisted tail and come back behind its journal.
#[tokio::test(start_paused = true)]
async fn safety_holds_under_restarts_and_storage_faults() {
    for store in [StoreKind::Memory, StoreKind::Executor] {
        for (seed, _replay) in sim_seeds("safety_holds_under_restarts_and_storage_faults", 0..20) {
            let result = run_with_restarts(seed, Faults::STORAGE, store).await;
            if let Some(v) = result.safety {
                panic!("SAFETY VIOLATION (restart +storage, {store:?}) at seed {seed}: {v}");
            }
            assert!(
                result.converged,
                "seed {seed}: cluster failed to converge after restarts (storage faults, {store:?})"
            );
            if let Some(stall) = result.liveness {
                panic!("seed {seed}: {stall}");
            }
        }
    }
}

/// The read oracle bites, and a violation replays from its seed (#318). With
/// the read barrier off, a read takes the node's store as it stands: a node
/// that lags behind lossy links, or a partition, serves a stale read.
#[tokio::test(start_paused = true)]
async fn the_read_oracle_catches_reads_without_a_barrier() {
    READ_BARRIER.with(|b| b.set(false));
    let caught = 'seeds: {
        for seed in 0..20 {
            let run = run_partition(seed, None, Faults::ALL).await;
            assert!(run.reads > 0, "seed {seed}: no read returned");
            if let Some(violation) = run.safety {
                break 'seeds Some((seed, violation));
            }
        }
        None
    };
    let (seed, violation) = caught.expect("reads without a barrier went unnoticed");
    let again = run_partition(seed, None, Faults::ALL).await.safety;
    READ_BARRIER.with(|b| b.set(true));
    assert_eq!(
        again.as_deref(),
        Some(violation.as_str()),
        "seed {seed} did not replay its violation"
    );
}

/// The seeds of the restart scenario, over failing storage on the production
/// store, that freeze a replica's store (#309). A restart that keeps the
/// store but loses its unpersisted tail can leave it behind what the
/// replica records as applied, with a hole; an apply across the hole fails
/// the store's version check, `ExecutorDataStore` takes that for "already
/// present", the replica marks the transaction applied — and every later
/// apply on the aggregate fails the same way. Compaction then removes any way
/// back: the node stays behind for good.
const FROZEN_STORE_SEEDS: [u64; 4] = [76, 189, 222, 224];

#[tokio::test(start_paused = true)]
async fn a_store_that_lost_its_tail_catches_up() {
    for seed in FROZEN_STORE_SEEDS {
        let result = run_with_restarts(seed, Faults::STORAGE, StoreKind::Executor).await;
        assert!(result.safety.is_none(), "seed {seed}: {:?}", result.safety);
        assert!(
            result.converged,
            "seed {seed}: a replica's store froze behind the others (#309)"
        );
        if let Some(stall) = result.liveness {
            panic!("seed {seed}: {stall}");
        }
    }
}

/// One committed event in a fingerprint: its `(tenant, type, id, version)` key
/// and the (deterministic) event id that committed it.
type Commit = ((String, String, String, u16), u128);

/// A reproducibility fingerprint of a finished run: each node's committed set
/// (with the deterministic event ids) plus the client history, all canonically
/// sorted. Under the deterministic runtime two runs of one seed must produce
/// equal fingerprints.
#[derive(PartialEq, Eq, Debug)]
struct Probe {
    per_node: Vec<Vec<Commit>>,
    history: Vec<(String, u16, bool)>,
    /// The run's execution trace.
    trace: RunTrace,
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
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = store0.clone();
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
    Probe {
        per_node,
        history,
        trace: sim.trace(),
    }
}

/// The payoff of the deterministic runtime: a given seed reproduces *exactly* —
/// same commits, same per-node state, same client outcomes — so a failure found
/// at a seed can be replayed and debugged. Running the same chaotic scenario
/// twice must yield byte-identical fingerprints.
#[tokio::test(start_paused = true)]
async fn simulation_is_bit_reproducible() {
    for (seed, _replay) in sim_seeds("simulation_is_bit_reproducible", 0..8) {
        let first = run_probe(seed).await;
        let second = run_probe(seed).await;
        assert_eq!(
            first, second,
            "seed {seed}: two runs diverged — the simulation is not deterministic"
        );
    }
}

/// Each scenario kind replays its *execution*, not only its outcome: two runs
/// of a seed must trace the same messages, between the same nodes, with the
/// same fates, at the same virtual times ([`assert_replays`]). The fingerprint
/// checks above compare end states, which two differently interleaved runs
/// can share; as faults and configurations open more paths, a leak would
/// otherwise surface as a CI failure whose seed does not replay locally.
#[tokio::test(start_paused = true)]
async fn every_scenario_kind_replays_its_trace() {
    let electorate = vec![NodeId(0), NodeId(1), NodeId(2)];
    for (seed, _replay) in sim_seeds("every_scenario_kind_replays_its_trace", 0..4) {
        assert_replays("replica churn", seed, |s| async move {
            run_scenario(s).await.expect("replica churn oracles")
        })
        .await;
        for (label, faults) in [("", Faults::default()), (" +faults", Faults::ALL)] {
            assert_replays(&format!("adversarial{label}"), seed, |s| async move {
                run_adversarial(s, faults).await.trace
            })
            .await;
            assert_replays(&format!("partition{label}"), seed, |s| async move {
                run_partition(s, None, faults).await.trace
            })
            .await;
            assert_replays(&format!("partition+electorate{label}"), seed, |s| {
                let electorate = electorate.clone();
                async move { run_partition(s, Some(electorate), faults).await.trace }
            })
            .await;
            assert_replays(&format!("restarts{label}"), seed, |s| async move {
                run_with_restarts(s, faults, StoreKind::Memory).await.trace
            })
            .await;
            assert_replays(&format!("amnesia{label}"), seed, |s| async move {
                run_amnesia(s, faults).await.trace
            })
            .await;
        }
        for store in [StoreKind::Memory, StoreKind::Executor] {
            assert_replays(
                &format!("restarts +storage {store:?}"),
                seed,
                |s| async move { run_with_restarts(s, Faults::STORAGE, store).await.trace },
            )
            .await;
        }
        assert_replays("probe", seed, |s| async move { run_probe(s).await.trace }).await;
        assert_replays("bounded", seed, |s| async move {
            run_bounded(s).await.expect("bound oracle")
        })
        .await;
        assert_replays("skew", seed, |s| async move {
            run_skew(s).await.expect("clock-skew oracle")
        })
        .await;
        assert_replays("membership churn", seed, |s| async move {
            run_churn(s).await.trace
        })
        .await;
        assert_replays("absence +faults", seed, |s| async move {
            run_absence(s, Faults::ALL).await.trace
        })
        .await;
    }
    assert_replays("absent peer rejoins", 0, |_| absent_peer_rejoins()).await;
}

/// The compaction payoff: in-memory consensus state is **bounded**, not
/// proportional to lifetime write volume. A workload runs, the cluster heals and
/// goes idle, and once the redundancy watermark advances past every committed
/// transaction (all replicas applied it, plus the propagation margin), every
/// command is compacted away — `command_count` returns to zero — while the
/// committed state (in the data store) is untouched.
async fn run_bounded(seed: u64) -> Result<RunTrace, String> {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;

    let mut sim = Sim::build(N);
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
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = store0.clone();
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
    // passes every write (+ margin) and all commands compact away — the third
    // step of the liveness check, which also requires the cluster to keep
    // serving and the stores to agree.
    let records = history.lock().unwrap().clone();
    assert_live(&mut sim, &acked_versions(&records))
        .await
        .map_err(|e| format!("seed {seed}: after {total_writes} writes, {e}"))?;

    // Compaction dropped consensus state but not the committed data: the cluster
    // still holds every committed event, and safety was never violated.
    if let Some(v) = safety_violation(&sim, &records) {
        return Err(format!("seed {seed}: {v}"));
    }
    Ok(sim.trace())
}

#[tokio::test(start_paused = true)]
async fn consensus_state_stays_bounded() {
    for (seed, _replay) in sim_seeds("consensus_state_stays_bounded", 0..8) {
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
async fn run_skew(seed: u64) -> Result<RunTrace, String> {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;
    const SKEW_MICROS: u64 = 150_000; // 150ms, within MAX_SKEW (200ms)
    const SKEWED: usize = 2;

    let mut sim = Sim::build_with_skew(N, SKEWED, SKEW_MICROS);
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
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = store0.clone();
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

    // Despite the skew, recovery/compaction stay healthy: the cluster serves,
    // and its state compacts away.
    let records = history.lock().unwrap().clone();
    assert_live(&mut sim, &acked_versions(&records))
        .await
        .map_err(|e| format!("seed {seed}: under skew, {e}"))?;
    if let Some(v) = safety_violation(&sim, &records) {
        return Err(format!("seed {seed}: {v}"));
    }
    Ok(sim.trace())
}

#[tokio::test(start_paused = true)]
async fn safety_and_bound_hold_under_clock_skew() {
    for (seed, _replay) in sim_seeds("safety_and_bound_hold_under_clock_skew", 0..8) {
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
    let mut sim = Sim::build_with_config(N, config);
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
    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let nodes = nodes.clone();
        let store0 = store0.clone();
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
    let recoveries = sim.nodes.iter().map(|n| n.recovery_count()).sum();
    assert_live(&mut sim, &acked_versions(&records))
        .await
        .map_err(|e| format!("seed {seed}: {e}"))?;
    Ok(recoveries)
}

/// The detector does not false-positive: a healthy cluster (regular heartbeats,
/// no crashes), even with the fixed timeout disabled, suspects no one — so
/// nothing is recovered.
#[tokio::test(start_paused = true)]
async fn no_spurious_recovery_when_healthy() {
    for (seed, _replay) in sim_seeds("no_spurious_recovery_when_healthy", 0..8) {
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
    /// Why the healed cluster did not serve again, if it did not.
    liveness: Option<String>,
    /// The run's execution trace.
    trace: RunTrace,
}

async fn run_churn(seed: u64) -> ChurnProbe {
    const N: u64 = 5;
    const AGGS: usize = 3;
    const WRITERS: usize = 3;
    const WRITES_EACH: usize = 10;

    // One shard with every node, identical across epochs — only the epoch
    // advances. The recovery loop drives the metadata-log catch-up sweep.
    let mut sim = Sim::build_dynamic(N);
    let layout = vec![sim.ids.clone()];
    let net = Arc::clone(&sim.net);
    let nodes = &sim.nodes;
    let stores = &sim.stores;
    let topos = &sim.dynamic;

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
    let store0 = stores[0].clone();
    let id_seq = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let coordinator = coordinator.clone();
        let store0 = store0.clone();
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
    converge(stores).await;
    for topo in topos {
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
    let records = history.lock().unwrap().clone();
    let mut history: Vec<(String, u16, bool)> = records
        .iter()
        .map(|r| (r.agg.clone(), r.version, r.committed))
        .collect();
    history.sort();
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();

    ChurnProbe {
        per_node_committed,
        per_node_epoch,
        history,
        liveness,
        trace: sim.trace(),
    }
}

/// Under concurrent reconfiguration, writes, and minority churn: every node
/// converges on the **same final epoch** (the metadata log replayed contiguously,
/// including for nodes that missed epochs), the data converges, and no
/// `(aggregate, version)` was double-committed.
#[tokio::test(start_paused = true)]
async fn membership_churn_converges() {
    for (seed, _replay) in sim_seeds("membership_churn_converges", 0..8) {
        let probe = run_churn(seed).await;
        if let Some(stall) = &probe.liveness {
            panic!("seed {seed}: {stall}");
        }

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
    for (seed, _replay) in sim_seeds("membership_churn_is_bit_reproducible", 0..6) {
        let first = run_churn(seed).await;
        let second = run_churn(seed).await;
        assert_eq!(
            first, second,
            "seed {seed}: two churn runs diverged — not deterministic"
        );
    }
}

// ---------------------------------------------------------------------------
// Seeds from the environment (#319).
// ---------------------------------------------------------------------------

/// The seeds a seeded test runs (#319):
/// - `SIM_SEED=<n>` replays seed `n` alone;
/// - `SIM_SEEDS=<count>` runs that many fresh seeds, and `SIM_DURATION=<secs>`
///   fresh seeds for that long (wall-clock), from a base drawn from OS entropy
///   and printed;
/// - otherwise `default`, the fixed range CI runs.
///
/// Each seed comes with a [`Replay`] guard: should the test fail while the
/// seed runs, it prints the one line that replays it.
fn sim_seeds(test: &'static str, default: Range<u64>) -> impl Iterator<Item = (u64, Replay)> {
    let seeds: Box<dyn Iterator<Item = u64>> = match env_u64("SIM_SEED") {
        Some(seed) => Box::new(std::iter::once(seed)),
        None if env_u64("SIM_SEEDS").is_some() || env_u64("SIM_DURATION").is_some() => {
            let base: u64 = rand::random();
            eprintln!("{test}: fresh seeds from {base}");
            let count = env_u64("SIM_SEEDS").unwrap_or(u64::MAX);
            let deadline = env_u64("SIM_DURATION")
                .map(|secs| std::time::Instant::now() + Duration::from_secs(secs));
            Box::new(
                (0..count)
                    .map(move |i| base.wrapping_add(i))
                    .take_while(move |_| deadline.is_none_or(|d| std::time::Instant::now() < d)),
            )
        }
        None => Box::new(default),
    };
    seeds.map(move |seed| (seed, Replay { test, seed }))
}

/// A numeric environment variable, if set and not empty.
fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok().filter(|value| !value.is_empty())?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name}={value}: not a number")),
    )
}

/// Prints how to replay its seed if the test panics while the seed runs.
struct Replay {
    test: &'static str,
    seed: u64,
}

impl Drop for Replay {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let planted = env_u64("SIM_PLANT_FAILURE")
                .filter(|&planted| planted == self.seed)
                .map(|planted| format!("SIM_PLANT_FAILURE={planted} "))
                .unwrap_or_default();
            eprintln!(
                "replay: {planted}SIM_SEED={} cargo test -p evento-accord --release \
                 --test simulation {} -- --exact --include-ignored --nocapture",
                self.seed, self.test
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Swarm testing (#319): each seed draws its own cluster, faults, store,
// workload and configuration.
// ---------------------------------------------------------------------------

const SWARM_SALT: u64 = 0x7377_6172_6d73_6565;

/// A swarm run's shape, drawn from its seed. Turning some faults off lets a
/// run go deeper into the others, which a fixed mix of every fault hides.
#[derive(Clone, Debug)]
struct SwarmConfig {
    /// 3, 5 or 7 nodes.
    n: u64,
    /// Nodes crash and heal, at most a minority down at once.
    crash: bool,
    /// A minority is cut off and healed, in turn; `Some(true)` cuts one way
    /// too.
    partition: Option<bool>,
    /// Nodes restart, each losing its journal's unflushed tail; until a node
    /// compacts, possibly its store's unpersisted tail, or an in-memory store
    /// whole.
    restarts: bool,
    /// An outside coordinator pre-accepts a write at one node and vanishes.
    orphans: bool,
    /// A node goes silent past `peer_absence_limit`, crashed or cut one way.
    absence: bool,
    messages: Option<MessageFaults>,
    storage: Option<StorageFaults>,
    /// The last node's clock runs this many microseconds ahead.
    skew: Option<u64>,
    store: StoreKind,
    aggs: usize,
    clients: usize,
    ops_each: usize,
    /// The share of operations that read.
    read_share: f64,
    /// The longest pause, in milliseconds, a client takes between operations.
    max_pause: u64,
    /// Within OPERATIONS.md's constraints.
    config: NodeConfig,
}

impl SwarmConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed ^ SWARM_SALT);
        let n = [3, 5, 7][rng.random_range(0..3)];
        let messages = rng.random_bool(0.4).then(|| MessageFaults {
            drop: rng.random_range(0.0..0.06),
            duplicate: rng.random_range(0.0..0.06),
            max_delay: Duration::from_millis(rng.random_range(0..40)),
        });
        let storage = rng.random_bool(0.4).then(|| StorageFaults {
            journal: JournalFaults {
                flush: rng.random_range(0.0..0.04),
                truncate: rng.random_range(0.0..0.2),
                metadata: rng.random_range(0.0..0.04),
            },
            apply: rng.random_range(0.0..0.04),
            fsync: Duration::from_millis(rng.random_range(0..5)),
            persist: rng.random_range(0.1..1.0),
        });
        let skew = rng
            .random_bool(0.3)
            .then(|| rng.random_range(0..MAX_SKEW_MICROS));
        let recovery_timeout = Duration::from_millis(rng.random_range(200..800));
        // The margin is at least the recovery timeout, twice the clock-skew
        // bound and the worst message delay.
        let compaction_margin = recovery_timeout
            + Duration::from_micros(2 * MAX_SKEW_MICROS)
            + messages.map_or(Duration::ZERO, |m| m.max_delay)
            + Duration::from_millis(rng.random_range(0..1000));
        let absence = rng.random_bool(0.25);
        let peer_absence_limit = (absence || rng.random_bool(0.2))
            .then(|| Duration::from_millis(rng.random_range(1500..4000)));
        // Well below the absence limit: a recovery holds up the sweep that
        // sends the node's heartbeats for up to this long.
        let collect_timeout = match peer_absence_limit {
            Some(limit) => (limit / 4).min(Duration::from_secs(1)),
            None => Duration::from_millis([500, 1000, 5000][rng.random_range(0..3)]),
        };
        let config = NodeConfig {
            fast_timeout: Duration::from_millis(rng.random_range(30..120)),
            collect_timeout,
            recovery_interval: Duration::from_millis(rng.random_range(50..200)),
            anti_entropy_interval: Duration::from_millis(rng.random_range(100..400))
                .min(compaction_margin / 2),
            recovery_timeout,
            compaction_margin,
            max_journal_batch: [1, 8, 128][rng.random_range(0..3)],
            snapshot_page_events: [1, 16, 4096][rng.random_range(0..3)],
            peer_absence_limit,
            ..NodeConfig::default()
        };
        SwarmConfig {
            n,
            crash: rng.random_bool(0.5),
            partition: rng.random_bool(0.5).then(|| rng.random_bool(0.5)),
            restarts: rng.random_bool(0.5),
            orphans: rng.random_bool(0.3),
            absence,
            messages,
            storage,
            skew,
            store: if rng.random_bool(0.5) {
                StoreKind::Executor
            } else {
                StoreKind::Memory
            },
            aggs: rng.random_range(1..=8),
            clients: rng.random_range(1..=8),
            ops_each: rng.random_range(6..=16),
            read_share: rng.random_range(0.0..0.5),
            max_pause: if absence {
                400
            } else {
                [20, 100, 300][rng.random_range(0..3)]
            },
            config,
        }
    }
}

/// One swarm run of `seed` in `shape`: the workload under the faults the
/// shape turns on, then the write and read oracles and, healed,
/// [`assert_live`].
async fn run_swarm(seed: u64, shape: &SwarmConfig) -> Adversarial {
    let n = shape.n as usize;
    let minority = (n - 1) / 2;
    let mut sim = Sim::build_with(SimOptions {
        skew: shape.skew.map(|micros| (n - 1, micros)),
        config: shape.config,
        store: shape.store,
        ..SimOptions::new(shape.n)
    });
    let faults = Faults {
        messages: shape.messages,
        one_way: false,
        storage: shape.storage,
    };
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);
    let mut rng = StdRng::seed_from_u64(seed);
    let ids = Arc::new(AtomicU64::new(1));
    let done = Arc::new(AtomicBool::new(false));
    let _orphan_inbox = sim.net.register(ORPHAN_COORDINATOR);

    // Clients: each operation reads, or writes, a seeded aggregate on a
    // seeded node, after a seeded pause.
    let mut workload = Vec::new();
    for _ in 0..shape.clients {
        let plan: Vec<(bool, usize, usize, u64)> = (0..shape.ops_each)
            .map(|_| {
                (
                    rng.random_bool(shape.read_share),
                    rng.random_range(0..shape.aggs),
                    rng.random_range(0..n),
                    rng.random_range(0..=shape.max_pause),
                )
            })
            .collect();
        let live = sim.live.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        workload.push(tokio::spawn(async move {
            for (read, agg_idx, node, pause) in plan {
                tokio::time::sleep(Duration::from_millis(pause)).await;
                let agg = format!("a{agg_idx}");
                if read {
                    clients.read(&live, node, &agg).await;
                    continue;
                }
                let (coordinator, store) = live.get(node);
                let version = store.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                clients
                    .write(&coordinator, &agg, version, seq, Duration::from_secs(2))
                    .await;
            }
        }));
    }
    let finished = {
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            for task in workload {
                let _ = task.await;
            }
            done.store(true, Ordering::Relaxed);
        })
    };

    let mut faulting = Vec::new();
    if shape.crash {
        // At most a minority down at once, until the clients are done.
        let plan: Vec<(u64, usize)> = (0..64)
            .map(|_| (rng.random_range(5..120), rng.random_range(0..n)))
            .collect();
        let net = Arc::clone(&sim.net);
        let done = Arc::clone(&done);
        faulting.push(tokio::spawn(async move {
            let mut down = BTreeSet::new();
            for (delay, node) in plan.into_iter().cycle() {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                if done.load(Ordering::Relaxed) {
                    break;
                }
                if down.remove(&node) {
                    net.heal(NodeId(node as u64));
                } else if down.len() < minority {
                    net.crash(NodeId(node as u64));
                    down.insert(node);
                }
            }
        }));
    }
    if let Some(one_way) = shape.partition {
        // A seeded minority cut off for a while, then healed for a while.
        let plan: Vec<(u64, u64, Vec<usize>, u8)> = (0..32)
            .map(|_| {
                let size = rng.random_range(1..=minority);
                let mut nodes: Vec<usize> = (0..n).collect();
                for i in 0..size {
                    let j = rng.random_range(i..n);
                    nodes.swap(i, j);
                }
                nodes.truncate(size);
                let direction = if one_way { rng.random_range(0..3) } else { 0 };
                (
                    rng.random_range(20..300),
                    rng.random_range(20..300),
                    nodes,
                    direction,
                )
            })
            .collect();
        let net = Arc::clone(&sim.net);
        let done = Arc::clone(&done);
        faulting.push(tokio::spawn(async move {
            for (cut, healed, cut_off, direction) in plan.into_iter().cycle() {
                if done.load(Ordering::Relaxed) {
                    break;
                }
                let links: Vec<(NodeId, NodeId)> = (0..n)
                    .filter(|i| !cut_off.contains(i))
                    .flat_map(|a| cut_off.iter().map(move |&b| (a, b)))
                    .map(|(a, b)| (NodeId(a as u64), NodeId(b as u64)))
                    .collect();
                for &(a, b) in &links {
                    match direction {
                        1 => net.cut(a, b),
                        2 => net.cut(b, a),
                        _ => net.partition(a, b),
                    }
                }
                tokio::time::sleep(Duration::from_millis(cut)).await;
                for &(a, b) in &links {
                    net.heal_partition(a, b);
                }
                tokio::time::sleep(Duration::from_millis(healed)).await;
            }
        }));
    }
    if shape.absence {
        let limit = shape
            .config
            .peer_absence_limit
            .expect("an absence has a limit");
        let node = NodeId(rng.random_range(0..n) as u64);
        let start = rng.random_range(0..1500);
        let length = limit + Duration::from_millis(rng.random_range(500..2500));
        let crash = rng.random_bool(0.5);
        let net = Arc::clone(&sim.net);
        let ids = sim.ids.clone();
        faulting.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(start)).await;
            let peers = ids.iter().filter(|&&id| id != node);
            if crash {
                net.crash(node);
            } else {
                peers.clone().for_each(|&peer| net.cut(node, peer));
            }
            tokio::time::sleep(length).await;
            if crash {
                net.heal(node);
            } else {
                peers.for_each(|&peer| net.restore(node, peer));
            }
        }));
    }
    if shape.orphans {
        for k in 0..rng.random_range(1..=3u32) {
            let at = rng.random_range(0..2000);
            let node = rng.random_range(0..n);
            let agg = format!("a{}", rng.random_range(0..shape.aggs));
            let net = Arc::clone(&sim.net);
            let live = sim.live.clone();
            let base = sim.base;
            faulting.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(at)).await;
                let version = live.get(node).1.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let orphan = event(&agg, version, ORPHAN_SEQ + u64::from(k));
                orphan_vote(&net, base, node, orphan, k).await;
            }));
        }
    }

    // Restarts, driven here since they need the sim, until the clients are
    // done. A restart loses store state only while no node has compacted: a
    // node rebuilds a lost write from its journal, or re-imports it from a
    // peer, only while both still hold it. Past that, a lost store is a
    // rejoin (OPERATIONS §7). A node's compaction count starts over when it
    // restarts, so every node is checked before each restart.
    let restarts: Vec<(u64, usize, bool)> = if shape.restarts {
        (0..24)
            .map(|_| {
                (
                    rng.random_range(50..400),
                    rng.random_range(0..n),
                    rng.random_bool(0.5),
                )
            })
            .collect()
    } else {
        Vec::new()
    };
    let mut compacted = false;
    for (delay, node, lossy) in restarts.into_iter().cycle() {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        if done.load(Ordering::Relaxed) {
            break;
        }
        compacted |= sim.nodes.iter().any(|n| n.metrics().compactions > 0);
        let mode = match (lossy && !compacted, shape.store) {
            (false, _) => Restart::KeepStore,
            (true, StoreKind::Memory) => Restart::EmptyStore,
            (true, StoreKind::Executor) => Restart::KeepStoreLoseTail,
        };
        sim.restart_as(node, shape.config, mode).await;
    }
    let _ = finished.await;
    for task in faulting {
        let _ = task.await;
    }

    let records = clients.history.lock().unwrap().clone();
    let safety = safety_violation(&sim, &records);
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();
    let converged = liveness.is_none();
    let safety = safety.or_else(|| read_violation(&clients, converged.then(|| &sim.stores[0])));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

/// Swarm testing (#319): each seed draws its own shape ([`SwarmConfig`]) and
/// must keep every oracle. Run with `SIM_SEEDS` or `SIM_DURATION` for fresh
/// seeds, as the nightly workflow does; a failure prints its shape and the
/// line that replays it. `SIM_PLANT_FAILURE=<seed>` runs that seed first and
/// fails it on purpose, to exercise the reporting.
#[tokio::test(start_paused = true)]
#[ignore = "swarm: run explicitly, with SIM_SEEDS or SIM_DURATION for fresh seeds"]
async fn swarm() {
    let planted = env_u64("SIM_PLANT_FAILURE");
    let first = planted.map(|seed| {
        (
            seed,
            Replay {
                test: "swarm",
                seed,
            },
        )
    });
    let seeds = first
        .into_iter()
        .chain(sim_seeds("swarm", 0..64).filter(move |(seed, _)| Some(*seed) != planted));
    let mut runs = 0;
    for (seed, _replay) in seeds {
        let shape = SwarmConfig::from_seed(seed);
        let run = run_swarm(seed, &shape).await;
        let failed = run.safety.is_some() || run.liveness.is_some();
        if failed || planted == Some(seed) {
            eprintln!("swarm config: {shape:?}");
        }
        assert!(
            planted != Some(seed),
            "swarm seed {seed}: a planted failure (SIM_PLANT_FAILURE)"
        );
        run.assert_ok(&format!("swarm seed {seed}"));
        runs += 1;
    }
    eprintln!("swarm: {runs} seeds with no violation");
}

/// Swarm seeds whose healed cluster served again too late (#326), one from
/// the nightly run and two from earlier swarm runs: the sweeps' recoveries
/// kept waiting out `collect_timeout` for a read quorum, so serving again
/// took more than [`SETTLE_ROUNDS`] rounds.
const SLOW_HEAL_SEEDS: [u64; 3] = [
    12498532817691066228,
    11045260470284367390,
    2014812038337545728,
];

#[tokio::test(start_paused = true)]
async fn a_healed_cluster_serves_again_in_time() {
    swarm_seeds_pass(&SLOW_HEAL_SEEDS).await;
}

/// Swarm seeds (#349) where a replica given up while absent rejoined
/// through a contact that had restarted and vouched for it below the
/// contact's own bound, then was told to rejoin on every rejoin: the hold
/// the contact kept on its report for the first join capped the vouch for
/// every next one (see `a_rejoin_through_a_contact_that_vouched_too_low_is_counted_next_time`).
const REJOIN_LOOP_SEEDS: [u64; 2] = [142649, 135462];

#[tokio::test(start_paused = true)]
async fn a_replica_rejoining_through_a_restarted_contact_is_counted_in() {
    swarm_seeds_pass(&REJOIN_LOOP_SEEDS).await;
}

/// A swarm seed (#348) whose rejoin went through a contact that was told
/// to rejoin itself mid-join, and answered nothing from then on: the
/// harness makes the join again through another contact, as a host would.
const CONTACT_TOLD_TO_REJOIN_SEEDS: [u64; 1] = [109900];

#[tokio::test(start_paused = true)]
async fn a_rejoin_whose_contact_is_told_to_rejoin_goes_through_another() {
    swarm_seeds_pass(&CONTACT_TOLD_TO_REJOIN_SEEDS).await;
}

/// Runs each swarm seed of `seeds`, failing on the first that breaks an
/// oracle.
async fn swarm_seeds_pass(seeds: &[u64]) {
    for &seed in seeds {
        let _replay = Replay {
            test: "swarm",
            seed,
        };
        run_swarm(seed, &SwarmConfig::from_seed(seed))
            .await
            .assert_ok(&format!("swarm seed {seed}"));
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
    let (mut reads, mut swept) = (0, 0);

    for (seed, _replay) in sim_seeds("sweep_for_bugs", 0..COUNT) {
        // Chaos (crash/heal minority, coordinators crash mid-write) — strong oracle.
        if let Err(v) = run_scenario(seed).await {
            panic!("chaos seed {seed}: {v}");
        }
        // Full chaos, partitions (default and shrunk {0,1,2} electorate) and
        // restarts — each also over lossy, duplicating, reordering links with
        // one-way cuts.
        for (label, faults) in [("", Faults::default()), (" +faults", Faults::ALL)] {
            reads += run_adversarial(seed, faults)
                .await
                .assert_ok(&format!("adversarial{label} seed {seed}"));
            for (kind, e) in [
                ("partition", None),
                ("partition+electorate", Some(electorate.clone())),
            ] {
                reads += run_partition(seed, e, faults)
                    .await
                    .assert_ok(&format!("{kind}{label} seed {seed}"));
            }
            reads += run_with_restarts(seed, faults, StoreKind::Memory)
                .await
                .assert_ok(&format!("restarts{label} seed {seed}"));
        }
        // Restarts over failing storage, on both stores, alone and with every
        // network fault on top.
        for store in [StoreKind::Memory, StoreKind::Executor] {
            for (label, faults) in [
                ("+storage", Faults::STORAGE),
                (
                    "+storage +faults",
                    Faults {
                        storage: Some(STORAGE_FAULTS),
                        ..Faults::ALL
                    },
                ),
            ] {
                reads += run_with_restarts(seed, faults, store)
                    .await
                    .assert_ok(&format!("restarts {label} {store:?} seed {seed}"));
            }
        }
        // Absences: silent peers given up, compacted past, and back (#310).
        for (label, faults) in [("", Faults::default()), (" +faults", Faults::ALL)] {
            reads += run_absence(seed, faults)
                .await
                .assert_ok(&format!("absence{label} seed {seed}"));
        }
        // Compaction boundedness + clock skew.
        if let Err(v) = run_bounded(seed).await {
            panic!("bounded seed {seed}: {v}");
        }
        if let Err(v) = run_skew(seed).await {
            panic!("skew seed {seed}: {v}");
        }
        // Membership churn (concurrent reconfiguration + writes + minority churn).
        let probe = run_churn(seed).await;
        if let Some(stall) = &probe.liveness {
            panic!("churn seed {seed}: {stall}");
        }
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

        swept += 1;
        if swept % 25 == 0 {
            eprintln!("swept {swept} seeds");
        }
    }
    eprintln!("swept {swept} seeds with no violation ({reads} reads checked)");
}

// ---------------------------------------------------------------------------
// Absent peers: a silent peer stops holding compaction back
// (`NodeConfig::peer_absence_limit`), is counted again if nothing it lacks
// was compacted, and is told to rejoin otherwise.
// ---------------------------------------------------------------------------

impl Sim {
    /// What a host does when node `i` is told to rejoin: tears it down, builds
    /// a fresh node over an **empty** store and its **kept** journal, and
    /// joins through `contact` before its sweeps start.
    async fn rejoin(&mut self, i: usize, contact: NodeId, config: NodeConfig) {
        self.try_rejoin(i, contact, config)
            .await
            .expect("rejoin from a snapshot");
    }

    /// [`rejoin`](Self::rejoin), handing a failed join back: node `i` then
    /// stays the stopped node told to rejoin, for its host to rejoin through
    /// another contact (see [`rejoin_told`](Self::rejoin_told)).
    async fn try_rejoin(
        &mut self,
        i: usize,
        contact: NodeId,
        config: NodeConfig,
    ) -> anyhow::Result<()> {
        let store = SimStore::new(self.store_kind, &self.outcomes);
        self.arm_store(i, &store);
        self.try_rejoin_over(i, contact, config, store.data_store(), store)
            .await
    }

    /// What a host does when node `i` lost its journal along with its store
    /// — a dead disk (#312): retires the node (a writer still holding it
    /// mints nothing more under its id), gives it an empty store and journal,
    /// and opens it **joining** under its old id. The node fences itself: until a
    /// contact vouches past the fence ([`join_amnesiac`](Self::join_amnesiac))
    /// it votes on nothing and refuses reads, though it serves writers and
    /// readers ([`Live`]) at once.
    async fn lose_journal(&mut self, i: usize) -> Result<(), String> {
        assert!(
            self.dynamic.is_empty(),
            "a restart over a dynamic topology is not modelled"
        );
        self.nodes[i].retire().await;
        self.journals[i] = Arc::new(FaultJournal::new());
        self.arm_journal(i);
        self.reopen_joining(i).await
    }

    /// Joins node `i`, reopened joining after it lost its journal, as its
    /// host would: through each other node in turn, again and again, until
    /// one vouches past its fence or `limit` passes. A join that broke after
    /// installing a page is retried over an emptied store.
    async fn join_amnesiac(&mut self, i: usize, limit: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + limit;
        let n = self.ids.len();
        let mut tried = 0;
        loop {
            let node = self.nodes[i].clone();
            let contact = self.ids[(i + 1 + tried % (n - 1)) % n];
            tried += 1;
            match node.join(contact).await {
                Ok(_) => {
                    self.tasks[i].push(node.start_recovery());
                    return Ok(());
                }
                Err(err) if tokio::time::Instant::now() >= deadline => {
                    return Err(format!(
                        "node {i} found no contact to vouch past its fence: {err:#}"
                    ));
                }
                // The store holds a prefix no contact can complete: emptied,
                // the node joins again over its fenced journal.
                Err(err) if err.downcast_ref::<PartialSnapshot>().is_some() => {
                    self.reopen_joining(i).await?;
                }
                Err(_) => tokio::time::sleep(self.config.recovery_interval).await,
            }
        }
    }

    /// Tears node `i` down and reopens it joining over its journal and an
    /// empty store — its inbox running, its join and sweeps to come.
    async fn reopen_joining(&mut self, i: usize) -> Result<(), String> {
        for t in &self.tasks[i] {
            t.abort();
        }
        let id = self.ids[i];
        let inbox = self.net.register(id);
        let store = SimStore::new(self.store_kind, &self.outcomes);
        self.arm_store(i, &store);
        // A recovery its storage fails fails the open, and its host opens
        // the node again: the faults are seeded rates, so one soon passes.
        let mut attempts = 0;
        let node = loop {
            let node = Node::new(
                id,
                Arc::new(Self::topology(id, &self.ids, &self.electorate)),
                Arc::new(HybridLogicalClock::with_physical(
                    id,
                    virtual_micros(self.base),
                )),
                Arc::new(self.net.sink(id)),
                store.data_store(),
                Arc::clone(&self.journals[i]) as Arc<dyn Journal>,
            )
            .with_config(self.config);
            match node.recover_state_for_join().await {
                Ok(()) => break node,
                Err(_) if attempts < 100 => attempts += 1,
                Err(err) => return Err(format!("node {i} could not reopen joining: {err:#}")),
            }
        };
        self.tasks[i] = vec![node.start(inbox)];
        self.nodes[i] = node.clone();
        self.stores[i] = store.clone();
        self.live.set(i, node, store);
        Ok(())
    }

    /// [`rejoin`](Self::rejoin) over `backend`, a store wrapping `store`.
    async fn rejoin_over(
        &mut self,
        i: usize,
        contact: NodeId,
        config: NodeConfig,
        backend: Arc<dyn DataStore>,
        store: SimStore,
    ) {
        self.try_rejoin_over(i, contact, config, backend, store)
            .await
            .expect("rejoin from a snapshot");
    }

    /// [`try_rejoin`](Self::try_rejoin) over `backend`, a store wrapping
    /// `store`.
    async fn try_rejoin_over(
        &mut self,
        i: usize,
        contact: NodeId,
        config: NodeConfig,
        backend: Arc<dyn DataStore>,
        store: SimStore,
    ) -> anyhow::Result<()> {
        for t in &self.tasks[i] {
            t.abort();
        }
        let id = self.ids[i];
        let inbox = self.net.register(id);
        let journal = Arc::clone(&self.journals[i]);
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
        node.recover_state_for_join()
            .await
            .expect("recover the kept journal");
        node.begin_join();
        let inbox_task = node.start(inbox);
        if let Err(err) = node.join(contact).await {
            inbox_task.abort();
            return Err(err);
        }
        self.tasks[i] = vec![inbox_task, node.start_recovery()];
        self.nodes[i] = node;
        self.stores[i] = store;
        self.journals[i] = journal;
        self.live
            .set(i, self.nodes[i].clone(), self.stores[i].clone());
        Ok(())
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
    absent_peer_rejoins().await;
}

/// [`an_absent_peer_stops_holding_compaction_back`]'s flow — an absence, a
/// compaction past it, a refused write, a paged rejoin — returning its trace,
/// so the join paths are replay-checked too.
async fn absent_peer_rejoins() -> RunTrace {
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
    sim.trace()
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
    inner: Arc<dyn DataStore>,
    delay: Duration,
    slow: AtomicBool,
}

#[async_trait::async_trait]
impl DataStore for SlowStore {
    async fn version(
        &self,
        tenant: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<u16> {
        self.inner
            .version(tenant, aggregate_type, aggregate_id)
            .await
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

    async fn holds(&self, events: &[Event]) -> anyhow::Result<bool> {
        self.inner.holds(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[evento_core::EventFilter]>>,
        routing_key: Option<evento_core::RoutingKey>,
        tenant: evento_core::TenantFilter,
        args: evento_core::cursor::Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<evento_core::cursor::ReadResult<Event>> {
        self.inner
            .read(aggregators, routing_key, tenant, args, to_micros)
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
    let store = SimStore::new(StoreKind::Memory, &sim.outcomes);
    let slow = Arc::new(SlowStore {
        inner: store.data_store(),
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
        store,
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
    sim.restart_with(0, NodeConfig::default()).await;
    sim.restart_with(1, NodeConfig::default()).await;
    assert_eq!(sim.nodes[0].absent_peers(), vec![NodeId(2)]);
    assert_eq!(sim.nodes[1].absent_peers(), vec![NodeId(2)]);

    sim.net.heal(NodeId(2));
    await_rejoin_required(&sim.nodes[2]).await;
}

/// A contact can report below what it compacted past an absent peer (#349):
/// restarted over a journal that kept the writes since its last truncate,
/// with a store that fails their replay, it holds them decided but
/// un-applied. Its vouch for that peer's rejoin is that report, so the
/// joiner's first report is below the bound and it is told to rejoin once
/// more. Once the contact has applied them again, the next rejoin through it
/// must count the peer in. The hold the contact kept on its report for the
/// first join (see `JoinHold`) must not cap the vouch for the next, or the
/// peer is told to rejoin again on every rejoin, for good.
#[tokio::test(start_paused = true)]
async fn a_rejoin_through_a_contact_that_vouched_too_low_is_counted_next_time() {
    const HOT: &str = "hot";
    // The journal truncates once, after the baseline: node 0's restart
    // replays every write since. Its sweep never takes them over: they are
    // decided, only their apply waits (#351).
    let config = NodeConfig {
        truncate_interval: Duration::from_secs(600),
        ..absence_config(Duration::from_secs(2))
    };
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
            .expect("write with node 3 down");
    }
    await_compacted(&sim, &[0, 1, 2], "with node 3 down").await;

    // Node 0 comes back holding versions 4 to 8 decided but un-applied: its
    // report stays below them, below its bound for node 3, while its sync
    // coverage, re-proven with nodes 1 and 2, is past them.
    sim.stores[0]
        .node
        .apply_failing
        .store(true, Ordering::SeqCst);
    sim.restart_as(0, config, Restart::KeepStore).await;
    tokio::time::sleep(config.anti_entropy_interval * 4).await;

    sim.net.heal(NodeId(3));
    await_rejoin_required(&sim.nodes[3]).await;
    sim.rejoin(3, NodeId(0), config).await;
    await_rejoin_required(&sim.nodes[3]).await;

    // Node 0 applies them again, and its own report passes the bound.
    sim.stores[0]
        .node
        .apply_failing
        .store(false, Ordering::SeqCst);
    for _ in 0..500 {
        if !sim.nodes[0].sweep_pressure().busy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!sim.nodes[0].sweep_pressure().busy, "node 0 applied again");
    tokio::time::sleep(config.compaction_margin).await;

    // Rejoined through node 0 again, as its host would (node 0 told it to),
    // node 3 is counted in.
    assert_eq!(sim.nodes[3].rejoin_contact(), Some(NodeId(0)));
    sim.rejoin(3, NodeId(0), config).await;
    for _ in 0..500 {
        if (0..3).all(|i| sim.nodes[i].absent_peers().is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !sim.nodes[3].rejoin_required(),
        "the second rejoin through node 0 counts node 3 in"
    );
    for i in all {
        assert!(
            sim.nodes[i].absent_peers().is_empty(),
            "node {i} counts node 3 again: {:?}",
            sim.nodes[i].absent_peers()
        );
    }
    sim.nodes[3]
        .write(vec![event(HOT, 9, 900)])
        .await
        .expect("a write coordinated by the rejoined node");
    assert!(converge(&sim.stores).await);
    assert_eq!(version_of(&sim.stores[3], HOT).await, 9);
    await_compacted(&sim, &all, "after the rejoin").await;
}

/// The aggregate the #351 tests write.
const REPLAYED: &str = "replayed";

/// Restarts node 0 of three over a journal that kept every write since its
/// last truncate and a store that fails their replay (#351): it holds them
/// decided but un-applied, and its peers compacted past them. Returns those
/// writes, oldest first.
async fn restart_over_a_failing_store(sim: &mut Sim, config: NodeConfig) -> Vec<TxnId> {
    let all = [0, 1, 2];
    // The journal truncates once, after the baseline (`truncate_interval`):
    // the restart replays every write since.
    for version in 1..=3u16 {
        sim.nodes[0]
            .write(vec![event(REPLAYED, version, version as u64)])
            .await
            .expect("baseline write");
    }
    assert!(converge(&sim.stores).await);
    await_compacted(sim, &all, "baseline").await;
    let mut writes = Vec::new();
    for version in 4..=8u16 {
        let outcome = sim.nodes[0]
            .write(vec![event(REPLAYED, version, 100 + version as u64)])
            .await
            .expect("write before the restart");
        writes.push(outcome.txn);
    }
    assert!(converge(&sim.stores).await);
    await_compacted(sim, &all, "before the restart").await;

    sim.stores[0]
        .node
        .apply_failing
        .store(true, Ordering::SeqCst);
    sim.restart_as(0, config, Restart::KeepStore).await;
    for &txn in &writes {
        assert_eq!(
            sim.nodes[0].command_status(txn),
            Some(Status::Committed),
            "{txn:?} handed back decided but un-applied"
        );
    }
    writes
}

/// Lets node 0's store take its applies again, and waits for it to apply
/// what it held: one more write then reaches every replica, and every
/// replica compacts it.
async fn store_works_again(sim: &Sim) {
    sim.stores[0]
        .node
        .apply_failing
        .store(false, Ordering::SeqCst);
    for _ in 0..500 {
        if !sim.nodes[0].sweep_pressure().busy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!sim.nodes[0].sweep_pressure().busy, "node 0 applied them");
    sim.nodes[0]
        .write(vec![event(REPLAYED, 9, 900)])
        .await
        .expect("a write once the store works again");
    assert!(converge(&sim.stores).await);
    assert_eq!(version_of(&sim.stores[0], REPLAYED).await, 9);
    await_compacted(sim, &[0, 1, 2], "once the store works again").await;
}

/// A replica's sweep never takes over a write it holds decided (#351). Its
/// peers compacted past the replayed writes, so a recovery would find too
/// few replicas left that could vote and send the node to rejoin, when all
/// the node lacks is their apply. The redrive retries it every sweep, and
/// the store takes it once it works again.
#[tokio::test(start_paused = true)]
async fn a_replayed_write_held_decided_waits_for_its_store() {
    let config = NodeConfig {
        truncate_interval: Duration::from_secs(600),
        ..NodeConfig::default()
    };
    let mut sim = Sim::build_with_config(3, config);
    restart_over_a_failing_store(&mut sim, config).await;

    // Every write is past `recovery_timeout`, many sweeps over.
    tokio::time::sleep(config.recovery_timeout * 10).await;
    assert!(
        sim.nodes[0].sweep_pressure().busy,
        "the store still fails them"
    );
    let metrics = sim.nodes[0].metrics();
    assert!(metrics.execution_failures > 1, "the redrive retries them");
    assert_eq!(metrics.recoveries, 0, "the sweep took a decided write over");
    assert!(
        !sim.nodes[0].rejoin_required(),
        "node 0 holds every decision it lacks an apply of: nothing to rejoin for"
    );
    assert_eq!(metrics.rejoins_required, 0);
    store_works_again(&sim).await;
}

/// A recovery of a write the node holds decided, not the sweep's (#351),
/// finds its peers compacted past the write, and too few replicas left that
/// could vote. It fails, but it must not send the node to rejoin: holding
/// the decision, the node is not stale, and lacks only the apply.
#[tokio::test(start_paused = true)]
async fn a_recovery_of_a_write_held_decided_never_sends_its_node_to_rejoin() {
    // The sweep never takes the writes over within the test either way.
    let config = NodeConfig {
        truncate_interval: Duration::from_secs(600),
        recovery_timeout: Duration::from_secs(600),
        ..NodeConfig::default()
    };
    let mut sim = Sim::build_with_config(3, config);
    let writes = restart_over_a_failing_store(&mut sim, config).await;

    let err = sim.nodes[0]
        .recover(writes[0])
        .await
        .expect_err("too few replicas left that could vote for it");
    assert!(
        !sim.nodes[0].rejoin_required(),
        "a recovery of a decided write sent node 0 to rejoin: {err:#}"
    );
    assert_eq!(sim.nodes[0].metrics().rejoins_required, 0);
    store_works_again(&sim).await;
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

/// A peer back from a partition catches up within a few anti-entropy
/// rounds, however small the pages (#348): a round goes on pulling pages
/// from its peer while each brings something in. At one page a round, a
/// backlog of one write a page took a round a write — [`BACKLOG`] rounds of
/// `anti_entropy_interval` here, 25 s.
#[tokio::test(start_paused = true)]
async fn a_healed_peer_catches_up_a_backlog_in_a_few_rounds() {
    let config = NodeConfig {
        snapshot_page_events: 1,
        ..paged_config()
    };
    let sim = Sim::build_with_config(3, config);
    sim.nodes[0]
        .write(vec![event("baseline", 1, 1)])
        .await
        .expect("baseline write");
    assert!(converge(&sim.stores).await);

    write_backlog(&sim, 2).await;
    sim.net.heal(NodeId(2));
    // Every write of the backlog, in node 2's store.
    let held = || async {
        let mut held = 0;
        for agg in 0..5 {
            held += version_of(&sim.stores[2], &format!("acc-{agg}")).await as u64;
        }
        held
    };
    let deadline = tokio::time::Instant::now() + config.anti_entropy_interval * 20;
    while held().await < BACKLOG && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        held().await,
        BACKLOG,
        "the healed node holds the whole backlog within 20 anti-entropy rounds"
    );
    assert!(converge(&sim.stores).await);
    await_compacted(&sim, &[0, 1, 2], "after the catch-up").await;
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

// ---------------------------------------------------------------------------
// Absences under load (#310): peers give up silent ones and compact past them.
// ---------------------------------------------------------------------------

/// How long a peer must be silent before it is given up, in
/// [`run_absence`].
const ABSENCE_LIMIT: Duration = Duration::from_secs(2);

/// The outside coordinator of [`run_absence`]'s orphan votes.
const ORPHAN_COORDINATOR: NodeId = NodeId(9);

/// Event ids of [`run_absence`]'s orphan writes, clear of every workload's.
const ORPHAN_SEQ: u64 = 2_000_000;

/// Pre-accepts `event` at node `to` alone, as [`ORPHAN_COORDINATOR`], which
/// then vanishes — as a coordinator that crashed before its own vote was
/// durable would — and returns the transaction. The caller registers the
/// coordinator's inbox first.
async fn orphan_vote(
    net: &Arc<InMemoryNetwork>,
    base: tokio::time::Instant,
    to: usize,
    event: Event,
    logical: u32,
) -> TxnId {
    let txn = TxnId(Timestamp {
        micros: (tokio::time::Instant::now() - base).as_micros() as u64,
        logical,
        node: ORPHAN_COORDINATOR,
    });
    let _ = net
        .sink(ORPHAN_COORDINATOR)
        .send(
            NodeId(to as u64),
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: vec![Key(event.aggregate_id.clone())],
                events: vec![event],
            },
        )
        .await;
    txn
}

/// The minority nodes go silent past [`ABSENCE_LIMIT`] while the majority
/// keeps writing — crashed, or cut one way so they still hear the others —
/// and come back. Their peers give them up and compact past them, so a
/// returning node lone-holding a transaction may recover it: the vote fence
/// must keep it from committing what a peer compacted past (#310). A node
/// left behind the others' compaction is told to rejoin, which
/// [`assert_live`] does as its host would.
///
/// The lone holders come from orphan votes: an outside coordinator
/// pre-accepts a write at one node and vanishes, as one that crashed before
/// its own vote was durable would. A minority node may get one shortly
/// before its absence, so its recovery of it fails until it is back. The
/// orphan writes the next version of a workload aggregate: committed by some
/// replicas and not others, it splits a later write's decision, which the
/// safety oracle reports.
async fn run_absence(seed: u64, faults: Faults) -> Adversarial {
    const N: u64 = 5;
    const AGGS: usize = 4;
    const WRITERS: usize = 4;
    const WRITES_EACH: usize = 12;

    let config = NodeConfig {
        peer_absence_limit: Some(ABSENCE_LIMIT),
        // Above the recovery timeout plus twice the skew bound, as
        // OPERATIONS.md asks of a margin.
        compaction_margin: Duration::from_millis(700),
        // Well below the absence limit, as OPERATIONS.md asks: a recovery
        // blocks the sweep that sends a node's heartbeats for up to this
        // long, and a live node silent past the limit is given up.
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    };
    let mut sim = Sim::build_with_config(N, config);
    faults.arm(&mut sim, seed);
    let clients = Clients::new(&sim);

    let mut rng = StdRng::seed_from_u64(seed);
    // Writes through the majority, spaced so the run outlasts the absences.
    let writer_plans: Vec<Vec<(usize, usize, u64)>> = (0..WRITERS)
        .map(|_| {
            (0..WRITES_EACH)
                .map(|_| {
                    (
                        rng.random_range(0..AGGS),
                        MAJORITY[rng.random_range(0..MAJORITY.len())],
                        rng.random_range(50..400),
                    )
                })
                .collect()
        })
        .collect();
    // One absence per minority node: when it starts, how long it lasts (past
    // the limit), and whether it crashes or only stops being heard.
    let absences: Vec<(usize, u64, u64, bool)> = MINORITY
        .iter()
        .map(|&node| {
            (
                node,
                rng.random_range(0..1_500),
                rng.random_range(2_500..4_500),
                rng.random_bool(0.5),
            )
        })
        .collect();
    // Orphan votes: which node, when — inside the recovery timeout before
    // the node's absence — and on which aggregate.
    let mut orphans: Vec<(usize, u64, usize)> = Vec::new();
    for &(node, start, _, _) in &absences {
        if rng.random_bool(0.5) {
            let lead = rng.random_range(0..config.recovery_timeout.as_millis() as u64);
            orphans.push((node, start.saturating_sub(lead), rng.random_range(0..AGGS)));
        }
    }

    let _orphan_inbox = sim.net.register(ORPHAN_COORDINATOR);
    let mut orphaned = Vec::new();
    for (k, (node, at, agg_idx)) in orphans.into_iter().enumerate() {
        let net = Arc::clone(&sim.net);
        let store0 = sim.stores[0].clone();
        let base = sim.base;
        orphaned.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(at)).await;
            let agg = format!("a{agg_idx}");
            let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
            let orphan = event(&agg, version, ORPHAN_SEQ + k as u64);
            orphan_vote(&net, base, node, orphan, k as u32).await;
        }));
    }

    let mut absent = Vec::new();
    for (node, start, length, crash) in absences {
        let net = Arc::clone(&sim.net);
        let ids = sim.ids.clone();
        absent.push(tokio::spawn(async move {
            let me = NodeId(node as u64);
            tokio::time::sleep(Duration::from_millis(start)).await;
            if crash {
                net.crash(me);
            } else {
                for &peer in ids.iter().filter(|&&id| id != me) {
                    net.cut(me, peer);
                }
            }
            tokio::time::sleep(Duration::from_millis(length)).await;
            if crash {
                net.heal(me);
            } else {
                for &peer in ids.iter().filter(|&&id| id != me) {
                    net.restore(me, peer);
                }
            }
        }));
    }

    let store0 = sim.stores[0].clone();
    let ids = Arc::new(AtomicU64::new(1));
    let mut writers = Vec::new();
    for plan in writer_plans {
        let live = sim.live.clone();
        let store0 = store0.clone();
        let clients = clients.clone();
        let ids = Arc::clone(&ids);
        writers.push(tokio::spawn(async move {
            for (agg_idx, coord, pause) in plan {
                tokio::time::sleep(Duration::from_millis(pause)).await;
                let agg = format!("a{agg_idx}");
                let version = store0.version(AGG_TYPE, &agg).await.unwrap_or(0) + 1;
                let seq = ids.fetch_add(1, Ordering::Relaxed);
                let node = live.node(coord);
                clients
                    .write(&node, &agg, version, seq, Duration::from_secs(2))
                    .await;
            }
        }));
    }
    writers.extend(spawn_readers(&sim, &clients, seed, AGGS));
    for w in writers {
        let _ = w.await;
    }
    for a in absent.into_iter().chain(orphaned) {
        let _ = a.await;
    }

    sim.net.heal_all_partitions();
    sim.net.clear_message_faults();
    let records = clients.history.lock().unwrap().clone();
    let safety = safety_violation(&sim, &records);
    // The returning nodes may need a rejoin before they converge: the
    // liveness check does it, then requires the stores to agree.
    let liveness = assert_live(&mut sim, &acked_versions(&records)).await.err();
    let converged = liveness.is_none();
    let safety = safety
        .or_else(|| safety_violation(&sim, &records))
        .or_else(|| read_violation(&clients, converged.then(|| &sim.stores[0])));
    let reads = clients.ops.lock().unwrap().reads.len();
    Adversarial {
        safety,
        converged,
        liveness,
        reads,
        trace: sim.trace(),
    }
}

/// Safety and liveness hold while silent peers are given up, compacted past,
/// and come back (#310).
#[tokio::test(start_paused = true)]
async fn safety_and_liveness_hold_under_absences() {
    for (seed, _replay) in sim_seeds("safety_and_liveness_hold_under_absences", 0..10) {
        run_absence(seed, Faults::default())
            .await
            .assert_ok(&format!("absence seed {seed}"));
    }
}

/// [`safety_and_liveness_hold_under_absences`] over lossy links.
#[tokio::test(start_paused = true)]
async fn safety_and_liveness_hold_under_absences_and_message_faults() {
    for (seed, _replay) in sim_seeds(
        "safety_and_liveness_hold_under_absences_and_message_faults",
        0..10,
    ) {
        run_absence(seed, Faults::ALL)
            .await
            .assert_ok(&format!("absence +faults seed {seed}"));
    }
}

// ---------------------------------------------------------------------------
// The vote fence (#310): a replica never votes for a transaction it does not
// hold below its clock less the compaction margin, so a peer that compacted
// past it, trusting its reports, is never contradicted.
// ---------------------------------------------------------------------------

/// The issue's flow. Only R (node 2) pre-accepts T, its coordinator gone. P
/// (node 0) compacts past T, as it may once it gave R up; Q (node 1)'s floor
/// lags. R recovers T through Q: without the fence Q pre-accepts it, R and Q
/// commit it, and P — which ignores a Commit below its floor — never applies
/// it. With the fence Q refuses it ("never witnessed"), and P's "compacted"
/// plus Q's refusal plus R's uncommitted report prove T never committed: the
/// recovery invalidates it, on every replica alike.
#[tokio::test(start_paused = true)]
async fn a_lone_holder_cannot_commit_what_a_peer_compacted_past() {
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(300),
        // Only the explicit recovery below recovers T.
        recovery_timeout: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let sim = Sim::build_with_config(3, config);
    let _coordinator = sim.net.register(ORPHAN_COORDINATOR);
    let txn = orphan_vote(&sim.net, sim.base, 2, event("acc", 1, 1), 0).await;
    // Past the compaction margin, P compacts past T, which it never held.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(sim.nodes[2].command_status(txn), Some(Status::PreAccepted));
    sim.nodes[0]
        .compact(Timestamp {
            micros: txn.0.micros + 1,
            logical: 0,
            node: NodeId(0),
        })
        .await;

    let outcome = sim.nodes[2].recover(txn).await.expect("a decided recovery");
    assert!(outcome.conflict, "T must not commit past P's compaction");
    assert_eq!(
        sim.nodes[1].command_status(txn),
        None,
        "Q never voted for T"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    for (i, store) in sim.stores.iter().enumerate() {
        assert!(store.committed_events().is_empty(), "replica {i} applied T");
    }
    assert!(sim.nodes[1].metrics().votes_fenced >= 1);
}

/// Waits until node `i` has given up exactly `peers`.
async fn await_absent(sim: &Sim, i: usize, peers: &[NodeId]) {
    for _ in 0..500 {
        if sim.nodes[i].absent_peers() == peers {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "node {i} gave up {:?}, not {peers:?}",
        sim.nodes[i].absent_peers()
    );
}

/// The partition flow. R (node 2) lone-holds T. P (node 0), cut off from R,
/// gives it up and compacts past T; Q (node 1) still hears R, whose report
/// keeps Q's floor below T. R recovers T through Q alone: without the fence Q
/// pre-accepts it, R and Q commit it, and P — compacted past it — never
/// applies it, so the stores diverge. With the fence Q refuses it, and R
/// cannot decide it until it hears P: P's "compacted" with Q's refusal and
/// R's uncommitted report prove it never committed.
#[tokio::test(start_paused = true)]
async fn a_lone_holder_cut_off_from_a_compacted_peer_cannot_commit() {
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(300),
        peer_absence_limit: Some(Duration::from_millis(500)),
        // Only the explicit recoveries below recover T.
        recovery_timeout: Duration::from_secs(3600),
        collect_timeout: Duration::from_millis(200),
        ..NodeConfig::default()
    };
    let sim = Sim::build_with_config(3, config);
    let _coordinator = sim.net.register(ORPHAN_COORDINATOR);
    let txn = orphan_vote(&sim.net, sim.base, 2, event("acc", 1, 1), 0).await;
    sim.net.cut(NodeId(0), NodeId(2));
    sim.net.cut(NodeId(2), NodeId(0));
    await_absent(&sim, 0, &[NodeId(2)]).await;
    // Past the margin, P's sweep compacts past T.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(sim.nodes[1].absent_peers().is_empty());

    let through_q = sim.nodes[2].recover(txn).await;
    assert!(through_q.is_err(), "T decided without P: {through_q:?}");
    assert_eq!(sim.nodes[1].command_status(txn), None, "Q voted for T");

    sim.net.restore(NodeId(0), NodeId(2));
    sim.net.restore(NodeId(2), NodeId(0));
    let outcome = sim.nodes[2].recover(txn).await.expect("a decided recovery");
    assert!(outcome.conflict, "T must not commit past P's compaction");
    // P answered "compacted", not "fenced": it had compacted past T.
    assert_eq!(sim.nodes[0].metrics().votes_fenced, 0);
    await_compacted(&sim, &[0, 1, 2], "after T's invalidation").await;
    for (i, store) in sim.stores.iter().enumerate() {
        assert!(store.committed_events().is_empty(), "replica {i} applied T");
    }
}

/// A lone holder stranded below its peers' compaction. R (node 2) lone-holds
/// T and goes silent; P and Q give it up and compact past T. They never held
/// T, so they dropped nothing R lacks and do not tell it to rejoin. Back, R
/// cannot decide T: both answer "compacted", which proves nothing — they
/// might have applied it and dropped it — and neither votes for it. Held for
/// ever, T would pin every replica's compaction. R rejoins through a peer
/// that compacted past T instead: the rejoin forgets T, and the snapshot
/// carries T's effect if it had one.
#[tokio::test(start_paused = true)]
async fn a_lone_holder_stranded_below_its_peers_compaction_rejoins() {
    let config = NodeConfig {
        compaction_margin: Duration::from_millis(300),
        peer_absence_limit: Some(Duration::from_millis(500)),
        collect_timeout: Duration::from_millis(200),
        ..NodeConfig::default()
    };
    let mut sim = Sim::build_with_config(3, config);
    let _coordinator = sim.net.register(ORPHAN_COORDINATOR);
    let txn = orphan_vote(&sim.net, sim.base, 2, event("acc", 1, 1), 0).await;
    sim.net.crash(NodeId(2));
    await_absent(&sim, 0, &[NodeId(2)]).await;
    await_absent(&sim, 1, &[NodeId(2)]).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sim.net.heal(NodeId(2));

    await_rejoin_required(&sim.nodes[2]).await;
    let contact = sim.nodes[2]
        .rejoin_contact()
        .expect("the peer it rejoins through");
    assert!([NodeId(0), NodeId(1)].contains(&contact), "{contact:?}");
    sim.rejoin(2, contact, config).await;
    assert_eq!(sim.nodes[2].command_status(txn), None);
    await_compacted(&sim, &[0, 1, 2], "after the rejoin").await;
    for (i, store) in sim.stores.iter().enumerate() {
        assert!(store.committed_events().is_empty(), "replica {i} applied T");
    }
}

// ---------------------------------------------------------------------------
// A dependency never witnessed (#330): a replica waiting on a transaction it
// never saw learns its decision instead of waiting for it to be applied
// somewhere.
// ---------------------------------------------------------------------------

/// `U` and `T` write one key. `T` commits with `U` among its dependencies but
/// executes first, and `U` waits on it. Nodes 0 and 1 hold `U` committed;
/// node 2 never witnessed it. With node 1 down, `T`'s read needs node 2,
/// which waits on `U` until it learns that `U` executes after `T`. Node 0
/// holds `U` blocked on `T`, so it never recovers it, and `U`'s Commit never
/// reaches node 2: nothing applies unless node 2 recovers `U` itself. Node 2
/// is cut off past the compaction margin, so on its return its own vote for
/// `U` is fenced, and node 0's committed report alone carries `U`'s
/// decision.
#[tokio::test(start_paused = true)]
async fn a_replica_recovers_a_dependency_it_never_witnessed() {
    let sim = Sim::build(3);
    let _coordinator = sim.net.register(ORPHAN_COORDINATOR);
    let now = (tokio::time::Instant::now() - sim.base).as_micros() as u64;
    let stamp = |micros| Timestamp {
        micros,
        logical: 0,
        node: ORPHAN_COORDINATOR,
    };
    let (u, t) = (TxnId(stamp(now)), TxnId(stamp(now + 1_000)));
    let commit = |txn: TxnId, execute_at, dep, version, seq| Message::Commit {
        epoch: 0,
        txn,
        execute_at,
        deps: vec![dep],
        keys: vec![Key("acc".into())],
        events: vec![event("acc", version, seq)],
        reply_to: ORPHAN_COORDINATOR,
    };
    let sink = sim.net.sink(ORPHAN_COORDINATOR);
    for to in [0, 1] {
        sink.send(NodeId(to), commit(u, stamp(now + 2_000), t, 2, 2))
            .await
            .unwrap();
    }
    for to in [0, 1, 2] {
        sink.send(NodeId(to), commit(t, t.0, u, 1, 1))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    sim.net.crash(NodeId(1));
    sim.net.partition(NodeId(0), NodeId(2));
    tokio::time::sleep(Duration::from_secs(2)).await;
    sim.net.heal_partition(NodeId(0), NodeId(2));

    for _ in 0..1500 {
        let mut versions = Vec::new();
        for i in [0, 2] {
            versions.push(version_of(&sim.stores[i], "acc").await);
        }
        if versions == [2, 2] {
            assert!(sim.nodes[2].metrics().dependencies_learned >= 1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "T and U never applied on nodes 0 and 2: node 2 holds T {:?}, U {:?}",
        sim.nodes[2].command_status(t),
        sim.nodes[2].command_status(u)
    );
}

// ---------------------------------------------------------------------------
// Durable imports (#331): a node's reports vouch for what it imported, so a
// restart must not forget it.
// ---------------------------------------------------------------------------

/// `T` commits everywhere with `D` among its dependencies; `D`, an orphan
/// node 0 alone pre-accepted, is invalidated. Node 2, cut off meanwhile,
/// later imports both from anti-entropy — `D` invalidated, `T` applied — and
/// its reports let nodes 0 and 1 compact them away. It restarts, keeping its
/// store or over an empty one. Its journal must still hold what it imported:
/// otherwise it restores `T` only as committed, from the Commit it
/// journaled, and waits for good on `D`, which no one holds any more — and
/// over an empty store, `T`'s write is lost, and no peer can ship it back.
/// Its journal is not truncated meanwhile (a crash within
/// `truncate_interval`), and nothing recovers on its own
/// (`recovery_timeout`), so the import is the only way in.
#[tokio::test(start_paused = true)]
async fn a_restart_keeps_what_anti_entropy_imported() {
    for mode in [Restart::KeepStore, Restart::EmptyStore] {
        restart_after_an_import(mode).await;
    }
}

async fn restart_after_an_import(mode: Restart) {
    let config = NodeConfig {
        recovery_timeout: Duration::from_secs(3600),
        truncate_interval: Duration::from_secs(3600),
        ..NodeConfig::default()
    };
    let mut sim = Sim::build_with_config(3, config);
    let _coordinator = sim.net.register(ORPHAN_COORDINATOR);
    let d = orphan_vote(&sim.net, sim.base, 0, event("acc", 1, 1), 0).await;
    let t = TxnId(Timestamp {
        micros: d.0.micros + 1_000,
        logical: 0,
        node: ORPHAN_COORDINATOR,
    });
    let sink = sim.net.sink(ORPHAN_COORDINATOR);
    for to in [0, 1, 2] {
        let commit = Message::Commit {
            epoch: 0,
            txn: t,
            execute_at: t.0,
            deps: vec![d],
            keys: vec![Key("acc".into())],
            events: vec![event("acc", 1, 2)],
            reply_to: ORPHAN_COORDINATOR,
        };
        sink.send(NodeId(to), commit).await.unwrap();
    }
    // Past the margin, nodes 1 and 2 are fenced for D: it is invalidated.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let outcome = sim.nodes[0].recover(d).await.expect("D decided");
    assert!(outcome.conflict, "D invalidated");

    // T applies on nodes 0 and 1 while node 2 is cut off.
    sim.net.partition(NodeId(0), NodeId(2));
    sim.net.partition(NodeId(1), NodeId(2));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let outcome = sim.nodes[0].recover(t).await.expect("T decided");
    assert!(!outcome.conflict, "T committed");
    sim.net.heal_all_partitions();

    // Node 2 imports both; its reports let nodes 0 and 1 compact them away.
    await_compacted(&sim, &[0, 1], "behind node 2's import").await;
    assert_eq!(
        version_of(&sim.stores[2], "acc").await,
        1,
        "node 2 imported T"
    );

    sim.restart_as(2, config, mode).await;
    await_compacted(
        &sim,
        &[0, 1, 2],
        &format!("after node 2's restart ({mode:?})"),
    )
    .await;
    sim.nodes[2]
        .read_barrier(Key("acc".into()))
        .await
        .expect("node 2 serves reads");
    assert_eq!(
        version_of(&sim.stores[2], "acc").await,
        1,
        "node 2 lost T's write ({mode:?})"
    );
}

/// A change that takes a replica off a key drains first, as does every
/// change but an addition. Two such changes installed at once, one after
/// the other, could leave a write in flight from before them without a read
/// quorum among the replicas that stay. T1, coordinated by node 1 and
/// pre-accepted by nodes 0 and 1 only, is in flight while node 4 replaces
/// node 1 and node 0 leaves; recovered after T2, a later write on its key,
/// it would depend on T2 — which nodes 0 and 1, replicas of T1 but not of
/// T2, never get — and hold back every write on its key.
#[tokio::test(start_paused = true)]
async fn a_write_in_flight_outlives_two_removals() {
    let sim = Sim::build_with(SimOptions {
        dynamic: true,
        layout: Some(vec![vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)]]),
        ..SimOptions::new(5)
    });
    sim.nodes[4].begin_join();
    let to_two_three: LinkFilter = Arc::new(|from, to, message| {
        from == NodeId(1)
            && (to == NodeId(2) || to == NodeId(3))
            && matches!(message, Message::PreAccept { .. })
    });
    sim.net.set_link_filter(Some(to_two_three));
    let coordinator = sim.nodes[1].clone();
    let t1 = tokio::spawn(async move { coordinator.write(vec![event("acc", 1, 1)]).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    sim.net.set_link_filter(None);

    let replaced = vec![vec![NodeId(0), NodeId(4), NodeId(2), NodeId(3)]];
    let epoch = sim.dynamic[0].epoch() + 1;
    sim.nodes[0]
        .change_topology(epoch, replaced)
        .await
        .expect("node 4 replaces node 1");
    sim.nodes[4].join(NodeId(0)).await.expect("node 4 joins");
    let shrunk = vec![vec![NodeId(4), NodeId(2), NodeId(3)]];
    let epoch = sim.dynamic[0].epoch() + 1;
    for _ in 0..1_000 {
        if sim.dynamic[2].epoch() + 1 == epoch {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sim.nodes[2]
        .change_topology(epoch, shrunk.clone())
        .await
        .expect("node 0 leaves");

    // T2, then one more once T1 is recovered: the key takes writes.
    for seq in [2, 3] {
        if seq == 3 {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let mut wrote = false;
        for _ in 0..50 {
            let version = version_of(&sim.stores[2], "acc").await + 1;
            let write = sim.nodes[2].write(vec![event("acc", version, seq)]);
            if let Ok(Ok(outcome)) = tokio::time::timeout(Duration::from_secs(3), write).await {
                if !outcome.conflict {
                    wrote = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(wrote, "write {seq} to the key never went through");
    }
    let _ = t1.await;
}

/// Sync coverage holds only between two nodes under the same live layout.
/// Node 2 copies the slots a merge gives it, then hears nothing of the
/// activation that ends it: under the layout before, the moved keys are not
/// its own, and it skips W, a write on one that its peers ship it from under
/// the new layout. Were it to take their coverage, it would report — and
/// compact — past W, then own the key and never get W.
#[tokio::test(start_paused = true)]
async fn coverage_counts_only_under_one_live_layout() {
    let sim = Sim::build_with(SimOptions {
        dynamic: true,
        layout: Some(vec![
            vec![NodeId(0), NodeId(1), NodeId(2)],
            vec![NodeId(3), NodeId(4), NodeId(5)],
        ]),
        ..SimOptions::new(6)
    });
    let moved = (0..)
        .map(|i| format!("m{i}"))
        .find(|agg| sim.dynamic[0].layout().shard_of(&Key(agg.clone())) == 1)
        .expect("a key of shard 1");
    // Node 2 hears of no epoch past the transition, nor decides one.
    let late = |message: &Message| match message {
        Message::ConfigPrepare { epoch, .. }
        | Message::ConfigPromise { epoch, .. }
        | Message::ConfigAccept { epoch, .. }
        | Message::ConfigAccepted { epoch, .. }
        | Message::ConfigNack { epoch, .. }
        | Message::ConfigCommit { epoch, .. } => *epoch >= 2,
        Message::MetadataEntries { entries } => entries.iter().any(|(epoch, _)| *epoch >= 2),
        _ => false,
    };
    sim.net
        .set_link_filter(Some(Arc::new(move |_, to, message| {
            to == NodeId(2) && late(message)
        })));
    let merged = vec![vec![NodeId(0), NodeId(1), NodeId(2)]];
    let installed = sim.nodes[0]
        .change_topology(1, merged.clone())
        .await
        .expect("the merge installs");
    assert_eq!(installed, merged);
    assert_eq!(
        sim.dynamic[2].epoch(),
        1,
        "node 2 is still in the transition"
    );
    sim.nodes[0]
        .write(vec![event(&moved, 1, 1)])
        .await
        .expect("W commits through 0 and 1");
    tokio::time::sleep(Duration::from_secs(3)).await;
    sim.net.set_link_filter(None);

    let mut held = Vec::new();
    for _ in 0..1_000 {
        held = sim.stores[2].events_of(&moved);
        if !held.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(held, vec![1], "node 2 holds W");
}

/// A replica that keeps a moved slot gets no copy of it: a change waits for
/// its report past the drain point too. Activated without it, a replica
/// that lagged would have to catch up under the new layout — where the
/// replica that copied the slot holds its writes as events, with no command
/// to ship, and vouches for them all the same. Node 2, cut off while T
/// commits through 0 and 1 and while node 3 replaces node 1, comes back
/// unable to sync with node 0 for a while.
#[tokio::test(start_paused = true)]
async fn a_change_waits_for_every_replica_that_keeps_a_moved_slot() {
    let sim = Sim::build_with(SimOptions {
        dynamic: true,
        layout: Some(vec![vec![NodeId(0), NodeId(1), NodeId(2)]]),
        ..SimOptions::new(4)
    });
    sim.nodes[3].begin_join();
    for other in [0, 1, 3] {
        sim.net.partition(NodeId(2), NodeId(other));
    }
    sim.nodes[0]
        .write(vec![event("acc", 1, 1)])
        .await
        .expect("T commits through 0 and 1");
    let replaced = vec![vec![NodeId(0), NodeId(2), NodeId(3)]];
    let driver = sim.nodes[0].clone();
    let change = {
        let replaced = replaced.clone();
        tokio::spawn(async move { driver.change_topology(1, replaced).await })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Node 2 comes back, syncing with anyone but node 0 for a while.
    let not_with_zero: LinkFilter = Arc::new(|from, to, message| {
        let link = [from, to];
        link.contains(&NodeId(2))
            && link.contains(&NodeId(0))
            && matches!(
                message,
                Message::SyncRequest { .. } | Message::SyncData { .. }
            )
    });
    sim.net.set_link_filter(Some(not_with_zero));
    sim.net.heal_all_partitions();
    tokio::time::sleep(Duration::from_secs(3)).await;
    sim.net.set_link_filter(None);
    let installed = change.await.unwrap().expect("node 3 replaces node 1");
    assert_eq!(installed, replaced);
    assert_eq!(sim.stores[3].events_of("acc"), vec![1], "node 3 copied T");

    let mut held = Vec::new();
    for _ in 0..1_000 {
        held = sim.stores[2].events_of("acc");
        if !held.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(held, vec![1], "node 2 holds T");
}
