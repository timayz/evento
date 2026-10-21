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
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// [`SweepScheduler::cancel`] drops it at an await point, for a node
    /// being discarded.
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
    /// recovery). A due node that finds no slot stays due, and when more nodes are
    /// due than slots are free, those swept longest ago go first — so every due
    /// node is swept within about `due / max_concurrent` ticks, none starved by
    /// the rest. Size it above the number of simultaneously active groups you
    /// expect. Default 64.
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
    /// The node's last sweep; while it runs, the node is skipped (sweeps are
    /// not re-entrant). A sweep that panicked counts as finished.
    sweep: Option<JoinHandle<()>>,
    /// Tick index at which its last sweep started, `None` before the first:
    /// under saturation, the nodes swept longest ago get the free slots first,
    /// and a node never swept goes before them all (`None` sorts first — a
    /// `0` would tie with a sweep at tick 0).
    last_swept: Option<u64>,
    /// A due sweep was deferred for want of a slot: the node stays due until it
    /// runs, so an idle node that missed its slot is not put off a whole
    /// `idle_interval`.
    owed: bool,
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
            sweep: None,
            last_swept: None,
            owed: false,
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

    /// Stops scheduling `group` and cancels a sweep of it in flight,
    /// returning once that sweep has stopped: from then on nothing here
    /// touches the node's store or journal.
    ///
    /// For a node being discarded ([`GroupHost::close`](crate::GroupHost::close)).
    /// A sweep dropped midway leaves the node's in-memory bookkeeping as a
    /// crash at that await point would — a recovery still registered, a
    /// transaction still marked as applying — so a node that keeps running
    /// is [`remove`](Self::remove)d instead.
    pub async fn cancel(&self, group: GroupId) {
        let entry = self
            .entries
            .lock()
            .expect("entries poisoned")
            .remove(&group);
        if let Some(sweep) = entry.and_then(|entry| entry.sweep) {
            sweep.abort();
            let _ = sweep.await;
        }
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
    /// (bounded by `max_concurrent`, oldest-swept first when more are due than
    /// slots are free). Public so a test — or a caller with its own
    /// timer — can drive the scheduler without [`start`](Self::start).
    pub fn tick(&self) {
        let index = self.tick_index.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let slots = self.slots();
        let hold = self.config.active_hold;

        // Spawned under the lock, so `cancel` never misses a sweep that is
        // about to start.
        let mut entries = self.entries.lock().expect("entries poisoned");
        let mut due: Vec<&mut Entry> = entries
            .values_mut()
            .filter_map(|entry| {
                let pressure = entry.node.pressure();
                if pressure.activity != entry.last_activity {
                    entry.last_activity = pressure.activity;
                    entry.active_until = now + hold;
                }
                let due = entry.owed
                    || pressure.busy
                    || now < entry.active_until
                    || index % slots == entry.phase;
                let running = entry
                    .sweep
                    .as_ref()
                    .is_some_and(|sweep| !sweep.is_finished());
                (due && !running).then_some(entry)
            })
            .collect();
        // Map order is fixed, so first-come would hand the slots to the same
        // nodes every tick; when they are short, the longest-unswept go first.
        if due.len() > self.semaphore.available_permits() {
            due.sort_by_key(|entry| entry.last_swept);
        }
        for entry in due {
            let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
                entry.owed = true;
                self.sweeps_deferred.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            entry.owed = false;
            entry.last_swept = Some(index);
            self.sweeps_run.fetch_add(1, Ordering::Relaxed);
            let node = Arc::clone(&entry.node);
            entry.sweep = Some(tokio::spawn(async move {
                node.sweep().await;
                drop(permit);
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicBool;

    #[derive(Default)]
    struct Fake {
        busy: AtomicBool,
        activity: AtomicU64,
        sweeps: AtomicU64,
        /// Hold each sweep until released (to test the concurrency cap).
        hold: Option<Arc<tokio::sync::Notify>>,
        /// Sweeps currently running (dropped ones included, through a guard).
        running: Arc<AtomicU64>,
        /// Panic in every sweep.
        panics: bool,
    }

    /// Counts a running sweep until it ends, finished or dropped.
    struct Running(Arc<AtomicU64>);

    impl Drop for Running {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
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
            self.running.fetch_add(1, Ordering::SeqCst);
            let _running = Running(Arc::clone(&self.running));
            if self.panics {
                panic!("a sweep panicked");
            }
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
    async fn a_saturated_scheduler_sweeps_every_busy_node_in_turn() {
        let s = SweepScheduler::new(SweepConfig {
            max_concurrent: 2,
            ..config()
        });
        let nodes: Vec<Arc<Fake>> = (0..6)
            .map(|_| {
                Arc::new(Fake {
                    busy: AtomicBool::new(true),
                    ..Default::default()
                })
            })
            .collect();
        for (i, n) in nodes.iter().enumerate() {
            s.add(GroupId(i as u64), n.clone());
        }
        tokio::time::advance(Duration::from_secs(2)).await; // past the add hold
        run_ticks(&s, 9).await; // 18 slots for 6 always-due nodes
        let sweeps: Vec<u64> = nodes
            .iter()
            .map(|n| n.sweeps.load(Ordering::Relaxed))
            .collect();
        assert!(
            sweeps.iter().all(|&n| n >= 2),
            "every due node gets its turn: {sweeps:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_deferred_idle_node_is_swept_on_the_next_free_tick() {
        let s = SweepScheduler::new(SweepConfig {
            max_concurrent: 1,
            ..config()
        });
        let hold = Arc::new(tokio::sync::Notify::new());
        // Busy, and its sweep holds the only slot until released.
        let busy = Arc::new(Fake {
            busy: AtomicBool::new(true),
            hold: Some(hold.clone()),
            ..Default::default()
        });
        let idle = Arc::new(Fake::default());
        s.add(GroupId(1), busy.clone());
        s.add(GroupId(5), idle.clone()); // idle slot: tick 5 of 50
        tokio::time::advance(Duration::from_secs(2)).await; // past the add hold
        run_ticks(&s, 10).await; // the idle slot passes while the slot is taken
        assert_eq!(idle.sweeps.load(Ordering::Relaxed), 0);

        hold.notify_waiters();
        // Let the released sweep finish and hand its slot back.
        while s.semaphore.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
        // Both are due for the one slot: the busy node, swept at tick 0, and
        // the idle one, never swept — which goes first.
        run_ticks(&s, 1).await;
        assert_eq!(
            idle.sweeps.load(Ordering::Relaxed),
            1,
            "the deferred idle sweep runs once a slot frees, not an idle interval later"
        );
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

    /// `cancel` stops a sweep in flight and returns once it has: a node being
    /// discarded must not have its store or journal touched afterwards.
    #[tokio::test(start_paused = true)]
    async fn cancel_stops_a_sweep_in_flight_before_returning() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake {
            busy: AtomicBool::new(true),
            hold: Some(Arc::new(tokio::sync::Notify::new())),
            ..Default::default()
        });
        s.add(GroupId(1), node.clone());
        s.tick();
        tokio::task::yield_now().await;
        assert_eq!(node.running.load(Ordering::SeqCst), 1, "the sweep is held");

        s.cancel(GroupId(1)).await;
        assert_eq!(
            node.running.load(Ordering::SeqCst),
            0,
            "the sweep was dropped"
        );
        assert!(!s.contains(GroupId(1)));
        run_ticks(&s, 3).await;
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 1);
    }

    /// `remove` leaves a sweep in flight to finish on its own, for a node that
    /// keeps running: dropped midway, it could leave its bookkeeping behind.
    #[tokio::test(start_paused = true)]
    async fn remove_lets_a_sweep_in_flight_finish() {
        let s = SweepScheduler::new(config());
        let hold = Arc::new(tokio::sync::Notify::new());
        let node = Arc::new(Fake {
            busy: AtomicBool::new(true),
            hold: Some(hold.clone()),
            ..Default::default()
        });
        s.add(GroupId(1), node.clone());
        s.tick();
        tokio::task::yield_now().await;
        s.remove(GroupId(1));
        assert_eq!(node.running.load(Ordering::SeqCst), 1, "still running");
        hold.notify_waiters();
        tokio::task::yield_now().await;
        assert_eq!(
            node.running.load(Ordering::SeqCst),
            0,
            "finished on its own"
        );
    }

    /// A sweep that panicked does not keep its node from being swept again.
    #[tokio::test(start_paused = true)]
    async fn a_panicked_sweep_does_not_wedge_its_node() {
        let s = SweepScheduler::new(config());
        let node = Arc::new(Fake {
            busy: AtomicBool::new(true),
            panics: true,
            ..Default::default()
        });
        s.add(GroupId(1), node.clone());
        run_ticks(&s, 3).await;
        assert_eq!(node.sweeps.load(Ordering::Relaxed), 3);
    }
}
