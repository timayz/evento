//! A shared, idle-aware driver for many nodes' recovery sweeps.
//!
//! [`Node::start_recovery`](crate::node::Node::start_recovery) gives one node one
//! task that sweeps every `recovery_interval` (100 ms by default). Each sweep
//! broadcasts a `Watermark` to every peer and, every `anti_entropy_interval`, sends
//! a `SyncRequest` — fine for one node, but a process hosting a thousand consensus
//! groups (one per tenant, say) would emit tens of thousands of frames per second
//! while completely idle.
//!
//! [`SweepScheduler`] drives any number of [`Sweepable`] nodes from **one** task and
//! sweeps each according to its [`SweepPressure`]:
//!
//! - **busy** (un-applied or in-flight work) — every tick, so the stalled-transaction
//!   recovery bound (`recovery_timeout` + one tick) is exactly what it is today;
//! - **recently active** (applied state advanced within `active_hold`) — every tick,
//!   so a burst's tail is gossiped and compacted away before the group goes quiet;
//! - **idle** — once per `idle_interval`, staggered across groups, which still bounds
//!   anti-entropy convergence and metadata catch-up for a group nothing is touching.
//!
//! Only *applied-state* changes count as activity, never the sweep's own gossip,
//! so idle peers cannot keep each other awake. Nothing here changes a node's
//! behaviour; a single-node deployment keeps using `start_recovery`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::node::Node;
use crate::transport::GroupId;

/// What a [`Sweepable`] reports to the scheduler each tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepPressure {
    /// Un-applied or in-flight transactions exist: sweep at full cadence so the
    /// recovery bound holds.
    pub busy: bool,
    /// A monotone counter that moves only when applied state advances. A change
    /// since the last tick marks the node *active* for `active_hold`.
    pub activity: u64,
}

/// Something the scheduler can sweep — a [`Node`], or a test double.
#[async_trait]
pub trait Sweepable: Send + Sync + 'static {
    /// Current liveness signal; must be cheap (called every tick for every node).
    fn pressure(&self) -> SweepPressure;
    /// One sweep iteration. Never invoked concurrently for the same node.
    async fn sweep(&self);
}

#[async_trait]
impl Sweepable for Node {
    fn pressure(&self) -> SweepPressure {
        self.sweep_pressure()
    }

    async fn sweep(&self) {
        Node::sweep(self).await
    }
}

/// Timing of a [`SweepScheduler`]. `Default` reproduces a lone node's cadence for
/// busy/active groups and sweeps idle ones every 5 s.
#[derive(Debug, Clone, Copy)]
pub struct SweepConfig {
    /// Scheduler tick; busy and recently-active nodes sweep every tick. Match the
    /// nodes' `NodeConfig::recovery_interval` (default 100 ms).
    pub tick: Duration,
    /// How long after its applied state last advanced a node stays at full cadence.
    /// Must cover `compaction_margin` plus a few `anti_entropy_interval` rounds, so a
    /// burst's tail is compacted (which needs watermark gossip and proven sync
    /// coverage) before the group backs off. Default 3 s.
    pub active_hold: Duration,
    /// Cadence of an idle node's sweeps (stuck check, anti-entropy round, watermark
    /// heartbeat, metadata catch-up). Bounds how long an idle group that missed
    /// something takes to converge. Default 5 s.
    pub idle_interval: Duration,
    /// Most sweeps in flight at once (each may await an anti-entropy reply or a
    /// recovery). A due node that finds no slot is retried next tick. Size it above
    /// the number of simultaneously active groups you expect. Default 64.
    pub max_concurrent: usize,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(100),
            active_hold: Duration::from_secs(3),
            idle_interval: Duration::from_secs(5),
            max_concurrent: 64,
        }
    }
}

struct Entry {
    node: Arc<dyn Sweepable>,
    last_activity: u64,
    /// Full cadence until here (a fresh node starts held, so a group just opened
    /// gossips and converges promptly).
    active_until: Instant,
    /// Idle slot (`tick_index % slots`) this node sweeps on; staggers idle sweeps.
    phase: u64,
    /// A sweep is running; skip until it finishes (sweeps are not re-entrant).
    in_flight: Arc<AtomicBool>,
}

/// Point-in-time counters of a [`SweepScheduler`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Sweeps started.
    pub sweeps_run: u64,
    /// Due sweeps deferred to the next tick because `max_concurrent` sweeps were
    /// already in flight.
    pub sweeps_deferred: u64,
    /// Nodes currently scheduled.
    pub nodes: usize,
}

/// Drives many nodes' sweeps from one task; see the module docs.
pub struct SweepScheduler {
    config: SweepConfig,
    entries: Mutex<HashMap<GroupId, Entry>>,
    semaphore: Arc<Semaphore>,
    tick_index: AtomicU64,
    sweeps_run: AtomicU64,
    sweeps_deferred: AtomicU64,
}

impl SweepScheduler {
    /// Creates a scheduler; call [`start`](Self::start) to run it.
    pub fn new(config: SweepConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            entries: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(config.max_concurrent.max(1))),
            tick_index: AtomicU64::new(0),
            sweeps_run: AtomicU64::new(0),
            sweeps_deferred: AtomicU64::new(0),
        })
    }

    /// The configuration in force.
    pub fn config(&self) -> SweepConfig {
        self.config
    }

    /// Number of idle slots: an idle node sweeps once every `slots` ticks.
    fn slots(&self) -> u64 {
        let tick = self.config.tick.as_nanos().max(1);
        (self.config.idle_interval.as_nanos() / tick).max(1) as u64
    }

    /// Schedules `node` under `group` (replacing any previous entry). The node
    /// starts at full cadence for `active_hold`.
    pub fn add(&self, group: GroupId, node: Arc<dyn Sweepable>) {
        let pressure = node.pressure();
        let entry = Entry {
            node,
            last_activity: pressure.activity,
            active_until: Instant::now() + self.config.active_hold,
            phase: group.0 % self.slots(),
            in_flight: Arc::new(AtomicBool::new(false)),
        };
        self.entries
            .lock()
            .expect("entries poisoned")
            .insert(group, entry);
    }

    /// Stops scheduling `group` (a sweep already in flight completes).
    pub fn remove(&self, group: GroupId) {
        self.entries
            .lock()
            .expect("entries poisoned")
            .remove(&group);
    }

    /// Whether `group` is scheduled.
    pub fn contains(&self, group: GroupId) -> bool {
        self.entries
            .lock()
            .expect("entries poisoned")
            .contains_key(&group)
    }

    /// Current counters.
    pub fn stats(&self) -> SweepStats {
        SweepStats {
            sweeps_run: self.sweeps_run.load(Ordering::Relaxed),
            sweeps_deferred: self.sweeps_deferred.load(Ordering::Relaxed),
            nodes: self.entries.lock().expect("entries poisoned").len(),
        }
    }

    /// Spawns the scheduler loop: one [`tick`](Self::tick) every `config.tick`.
    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.config.tick);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // The first tick of an interval completes immediately; skip it so the
            // cadence starts one tick after `start`, like `start_recovery`.
            interval.tick().await;
            loop {
                interval.tick().await;
                this.tick();
            }
        })
    }

    /// One scheduling pass: decides which nodes are due and spawns their sweeps
    /// (bounded by `max_concurrent`). Public so a test — or a caller with its own
    /// timer — can drive the scheduler without [`start`](Self::start).
    pub fn tick(&self) {
        let index = self.tick_index.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let slots = self.slots();
        let hold = self.config.active_hold;

        let due: Vec<(Arc<dyn Sweepable>, Arc<AtomicBool>)> = {
            let mut entries = self.entries.lock().expect("entries poisoned");
            entries
                .values_mut()
                .filter_map(|entry| {
                    let pressure = entry.node.pressure();
                    if pressure.activity != entry.last_activity {
                        entry.last_activity = pressure.activity;
                        entry.active_until = now + hold;
                    }
                    let due =
                        pressure.busy || now < entry.active_until || index % slots == entry.phase;
                    if due && !entry.in_flight.load(Ordering::Acquire) {
                        Some((Arc::clone(&entry.node), Arc::clone(&entry.in_flight)))
                    } else {
                        None
                    }
                })
                .collect()
        };

        for (node, in_flight) in due {
            let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
                self.sweeps_deferred.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            in_flight.store(true, Ordering::Release);
            self.sweeps_run.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                node.sweep().await;
                drop(permit);
                in_flight.store(false, Ordering::Release);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Fake {
        busy: AtomicBool,
        activity: AtomicU64,
        sweeps: AtomicU64,
        /// Hold each sweep until released (to test the concurrency cap).
        hold: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait]
    impl Sweepable for Fake {
        fn pressure(&self) -> SweepPressure {
            SweepPressure {
                busy: self.busy.load(Ordering::Relaxed),
                activity: self.activity.load(Ordering::Relaxed),
            }
        }
        async fn sweep(&self) {
            self.sweeps.fetch_add(1, Ordering::Relaxed);
            if let Some(hold) = &self.hold {
                hold.notified().await;
            }
        }
    }

    fn config() -> SweepConfig {
        SweepConfig {
            tick: Duration::from_millis(100),
            active_hold: Duration::from_secs(1),
            idle_interval: Duration::from_secs(5), // 50 slots
            max_concurrent: 64,
        }
    }

    /// Runs `n` ticks, advancing virtual time one tick each and letting spawned
    /// sweeps run.
    async fn run_ticks(s: &SweepScheduler, n: usize) {
        for _ in 0..n {
            s.tick();
            tokio::task::yield_now().await;
            tokio::time::advance(s.config.tick).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_node_is_swept_every_tick() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake::default());
        node.busy.store(true, Ordering::Relaxed);
        s.add(GroupId(1), node.clone());
        tokio::time::advance(Duration::from_secs(2)).await; // past the add hold
        run_ticks(&s, 10).await;
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 10);
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_node_is_swept_once_per_idle_interval() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake::default());
        s.add(GroupId(1), node.clone());
        tokio::time::advance(Duration::from_secs(2)).await; // past the add hold
        run_ticks(&s, 100).await; // 2 idle intervals
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_node_is_held_at_full_cadence_then_backs_off() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake::default());
        s.add(GroupId(7), node.clone());
        run_ticks(&s, 10).await; // within the 1 s hold: every tick
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 10);
        run_ticks(&s, 40).await; // hold over: only the idle slot (one in 50)
        let after = node.sweeps.load(Ordering::Relaxed);
        assert!(after <= 11, "backed off to idle cadence, got {after}");
    }

    #[tokio::test(start_paused = true)]
    async fn activity_reactivates_full_cadence_for_the_hold() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake::default());
        s.add(GroupId(3), node.clone());
        tokio::time::advance(Duration::from_secs(2)).await;
        run_ticks(&s, 10).await;
        let idle = node.sweeps.load(Ordering::Relaxed);
        assert!(idle <= 1);

        node.activity.fetch_add(1, Ordering::Relaxed);
        run_ticks(&s, 10).await; // 1 s hold = 10 ticks at full cadence
        assert_eq!(node.sweeps.load(Ordering::Relaxed), idle + 10);
        run_ticks(&s, 10).await;
        assert!(node.sweeps.load(Ordering::Relaxed) <= idle + 11);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_nodes_are_staggered_across_slots() {
        let s = SweepScheduler::new(config());
        let a = Arc::new(Fake::default());
        let b = Arc::new(Fake::default());
        s.add(GroupId(1), a.clone());
        s.add(GroupId(2), b.clone());
        tokio::time::advance(Duration::from_secs(2)).await;
        // Find the ticks each sweeps on: they must differ.
        let mut a_ticks = Vec::new();
        let mut b_ticks = Vec::new();
        for i in 0..50 {
            let (a0, b0) = (
                a.sweeps.load(Ordering::Relaxed),
                b.sweeps.load(Ordering::Relaxed),
            );
            run_ticks(&s, 1).await;
            if a.sweeps.load(Ordering::Relaxed) > a0 {
                a_ticks.push(i);
            }
            if b.sweeps.load(Ordering::Relaxed) > b0 {
                b_ticks.push(i);
            }
        }
        assert_eq!(a_ticks.len(), 1);
        assert_eq!(b_ticks.len(), 1);
        assert_ne!(a_ticks, b_ticks);
    }

    #[tokio::test(start_paused = true)]
    async fn max_concurrent_caps_in_flight_sweeps_and_defers_the_rest() {
        let s = SweepScheduler::new(SweepConfig {
            max_concurrent: 2,
            ..config()
        });
        let hold = Arc::new(tokio::sync::Notify::new());
        let nodes: Vec<Arc<Fake>> = (0..4)
            .map(|_| {
                Arc::new(Fake {
                    busy: AtomicBool::new(true),
                    hold: Some(hold.clone()),
                    ..Default::default()
                })
            })
            .collect();
        for (i, n) in nodes.iter().enumerate() {
            s.add(GroupId(i as u64), n.clone());
        }
        s.tick();
        tokio::task::yield_now().await;
        let total: u64 = nodes.iter().map(|n| n.sweeps.load(Ordering::Relaxed)).sum();
        assert_eq!(total, 2, "only two sweeps may be in flight");
        assert_eq!(s.stats().sweeps_deferred, 2);
        assert_eq!(s.stats().sweeps_run, 2);

        // A node whose sweep is still running is not swept again.
        s.tick();
        tokio::task::yield_now().await;
        let total: u64 = nodes.iter().map(|n| n.sweeps.load(Ordering::Relaxed)).sum();
        assert_eq!(total, 2);

        // Release: the slots free up and the deferred nodes get swept.
        hold.notify_waiters();
        tokio::task::yield_now().await;
        s.tick();
        tokio::task::yield_now().await;
        let total: u64 = nodes.iter().map(|n| n.sweeps.load(Ordering::Relaxed)).sum();
        assert_eq!(total, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn removed_nodes_are_no_longer_swept() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake::default());
        node.busy.store(true, Ordering::Relaxed);
        s.add(GroupId(1), node.clone());
        run_ticks(&s, 2).await;
        s.remove(GroupId(1));
        assert!(!s.contains(GroupId(1)));
        run_ticks(&s, 5).await;
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 2);
        assert_eq!(s.stats().nodes, 0);
    }
}
