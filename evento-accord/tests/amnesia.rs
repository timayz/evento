//! A replica that lost its journal comes back under its own node id (#312).
//!
//! It forgot the votes, promises and reports it made, and the ids it minted.
//! Reopened by joining over an empty journal, it sets an amnesia fence above
//! all of them, and joins only through a contact that vouches past the fence:
//! every write that committed below it is then in the snapshot, and none
//! that did not can commit any more. A plain open over an empty journal but
//! a store holding events is refused. Whatever the journal, a replica never
//! votes for a transaction dated past its clock's skew bound, and one that
//! lost a journal under its node id abstains from config Paxos for good.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::clock::system_micros;
use evento_accord::{
    AmnesiaFence, Ballot, Clock, DataStore, Envelope, FenceNotCovered, HybridLogicalClock,
    InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal, Key, LostJournal, Message,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology, Timestamp, TxnId,
};
use evento_core::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// The clock-skew bound every node's clock runs with: small, so a fence
/// (three skews past a reopening) is soon passed.
const SKEW_MICROS: u64 = 20_000;

/// Fast timings, so a contact's report passes a fence within a fraction of a
/// second. The margin covers the recovery timeout plus two skews, as
/// OPERATIONS §3 asks.
fn config() -> NodeConfig {
    NodeConfig {
        fast_timeout: Duration::from_millis(20),
        collect_timeout: Duration::from_millis(500),
        recovery_interval: Duration::from_millis(20),
        anti_entropy_interval: Duration::from_millis(25),
        recovery_timeout: Duration::from_millis(100),
        compaction_margin: Duration::from_millis(200),
        amnesia_join_timeout: Duration::from_secs(20),
        ..NodeConfig::default()
    }
}

fn clock(id: NodeId) -> Arc<HybridLogicalClock> {
    Arc::new(HybridLogicalClock::new(id).with_max_skew(SKEW_MICROS))
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

/// The key a write of aggregate `aggregate_id` conflicts on.
fn key(aggregate_id: &str) -> Key {
    Key::of(&event(aggregate_id, 1))
}

/// Whether `store` holds `event`, at its version.
fn holds(store: &InMemoryDataStore, event: &Event) -> bool {
    store
        .committed_events()
        .iter()
        .any(|((_, _, id, version), ulid)| {
            *id == event.aggregate_id && *version == event.version && *ulid == event.id
        })
}

/// Whether every store holds exactly the same events.
fn stores_agree(stores: &[Arc<InMemoryDataStore>]) -> bool {
    let mut all = stores.iter().map(|store| {
        let mut events = store.committed_events();
        events.sort();
        events
    });
    let first = all.next().unwrap_or_default();
    all.all(|events| events == first)
}

async fn await_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting until {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The next message an outside node's inbox receives, if one arrives within
/// `wait`.
async fn next_within(inbox: &mut mpsc::Receiver<Envelope>, wait: Duration) -> Option<Message> {
    tokio::time::timeout(wait, inbox.recv())
        .await
        .ok()
        .flatten()
        .map(|envelope| envelope.message)
}

/// A node's running tasks, shared with a join in flight so it can add the
/// recovery sweep it starts once joined.
type Tasks = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// `n` real replicas `{0..n}` of one shard, each running its inbox loop and
/// recovery sweep over an in-memory store and journal, plus the inbox of an
/// outside coordinator (node 9) the tests play.
struct Cluster {
    net: Arc<InMemoryNetwork>,
    n: u64,
    config: NodeConfig,
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    journals: Vec<Arc<InMemoryJournal>>,
    clocks: Vec<Arc<HybridLogicalClock>>,
    tasks: Vec<Tasks>,
    coordinator: mpsc::Receiver<Envelope>,
}

impl Cluster {
    fn start(n: u64) -> Self {
        Self::start_with(n, config(), clock)
    }

    fn start_with(
        n: u64,
        config: NodeConfig,
        clocks: impl Fn(NodeId) -> Arc<HybridLogicalClock>,
    ) -> Self {
        let net = InMemoryNetwork::new();
        let mut cluster = Self {
            coordinator: net.register(NodeId(9)),
            net,
            n,
            config,
            nodes: Vec::new(),
            stores: Vec::new(),
            journals: Vec::new(),
            clocks: Vec::new(),
            tasks: Vec::new(),
        };
        for i in 0..n {
            let id = NodeId(i);
            let store = Arc::new(InMemoryDataStore::new());
            let journal = Arc::new(InMemoryJournal::new());
            let clock = clocks(id);
            let inbox = cluster.net.register(id);
            let node = cluster.build(i, &store, &journal, &clock);
            let tasks: Tasks = Arc::new(Mutex::new(vec![node.start(inbox), node.start_recovery()]));
            cluster.nodes.push(node);
            cluster.stores.push(store);
            cluster.journals.push(journal);
            cluster.clocks.push(clock);
            cluster.tasks.push(tasks);
        }
        cluster
    }

    fn build(
        &self,
        i: u64,
        store: &Arc<InMemoryDataStore>,
        journal: &Arc<impl Journal>,
        clock: &Arc<HybridLogicalClock>,
    ) -> Node {
        let id = NodeId(i);
        Node::new(
            id,
            Arc::new(StaticTopology::new(id, (0..self.n).map(NodeId).collect())),
            Arc::clone(clock),
            Arc::new(self.net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(store) as Arc<dyn DataStore>,
            Arc::clone(journal) as Arc<dyn Journal>,
        )
        .with_config(self.config)
    }

    /// Aborts node `i`'s tasks: a crash.
    fn stop(&self, i: u64) {
        for task in self.tasks[i as usize].lock().unwrap().drain(..) {
            task.abort();
        }
    }

    /// Severs every link between node `i` and the other replicas.
    fn cut_off(&self, i: u64) {
        for peer in (0..self.n).filter(|&p| p != i) {
            self.net.partition(NodeId(i), NodeId(peer));
        }
    }

    fn heal(&self) {
        self.net.heal_all_partitions();
    }

    /// What a host does once node `i` lost its journal and its store: stops
    /// it, gives it an empty store and journal, and opens it **joining**
    /// through `contact` (`recover_state_for_join`, `begin_join`, `join`). Returns the
    /// join in flight; once joined, the node runs its recovery sweep.
    async fn reopen_amnesiac(&mut self, i: u64, contact: u64) -> JoinHandle<anyhow::Result<usize>> {
        let journal = Arc::new(InMemoryJournal::new());
        let clock = clock(NodeId(i));
        self.reopen_joining(i, journal, clock, contact).await
    }

    /// [`reopen_amnesiac`](Self::reopen_amnesiac) over `journal`, on `clock`.
    async fn reopen_joining(
        &mut self,
        i: u64,
        journal: Arc<InMemoryJournal>,
        clock: Arc<HybridLogicalClock>,
        contact: u64,
    ) -> JoinHandle<anyhow::Result<usize>> {
        self.stop(i);
        let store = Arc::new(InMemoryDataStore::new());
        let inbox = self.net.register(NodeId(i));
        let node = self.build(i, &store, &journal, &clock);
        node.recover_state_for_join()
            .await
            .expect("recover the journal for a join");
        node.begin_join();
        self.tasks[i as usize]
            .lock()
            .unwrap()
            .push(node.start(inbox));
        self.nodes[i as usize] = node.clone();
        self.stores[i as usize] = store;
        self.journals[i as usize] = journal;
        self.clocks[i as usize] = clock;
        self.join(i, contact)
    }

    /// Joins node `i`, already reopened joining, through `contact`.
    fn join(&self, i: u64, contact: u64) -> JoinHandle<anyhow::Result<usize>> {
        let node = self.nodes[i as usize].clone();
        let tasks = Arc::clone(&self.tasks[i as usize]);
        tokio::spawn(async move {
            let joined = node.join(NodeId(contact)).await;
            if joined.is_ok() {
                tasks.lock().unwrap().push(node.start_recovery());
            }
            joined
        })
    }

    /// Restarts node `i` over its kept journal and store, with a plain open.
    async fn restart(&mut self, i: u64) {
        self.stop(i);
        let inbox = self.net.register(NodeId(i));
        let node = self.build(
            i,
            &self.stores[i as usize],
            &self.journals[i as usize],
            &self.clocks[i as usize],
        );
        node.recover_state().await.expect("a plain restart");
        let mut tasks = self.tasks[i as usize].lock().unwrap();
        tasks.push(node.start(inbox));
        tasks.push(node.start_recovery());
        drop(tasks);
        self.nodes[i as usize] = node;
    }

    async fn send(&self, from: u64, to: u64, message: Message) {
        self.net
            .sink(NodeId(from))
            .send(NodeId(to), message)
            .await
            .unwrap();
    }

    /// A write every replica applies.
    async fn baseline(&self) {
        let base = event("base", 1);
        self.nodes[0]
            .write(vec![base.clone()])
            .await
            .expect("baseline write");
        for store in &self.stores {
            await_until(
                "every store holds the baseline",
                Duration::from_secs(5),
                || holds(store, &base),
            )
            .await;
        }
    }
}

/// In a three-replica shard: the replica that loses its journal, and the
/// two others.
const R: u64 = 2;
const A: u64 = 0;
const B: u64 = 1;

/// A store holding events over an empty journal is a lost journal, not a
/// fresh replica: the node forgot what it voted, so a plain open refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_open_refuses_a_lost_journal_over_a_kept_store() {
    let c = Cluster::start(3);
    c.baseline().await;
    c.stop(R);
    let node = c.build(
        R,
        &c.stores[R as usize],
        &Arc::new(InMemoryJournal::new()),
        &clock(NodeId(R)),
    );
    let err = node
        .recover_state()
        .await
        .expect_err("a lost journal under a kept store must be refused");
    assert_eq!(
        err.downcast_ref::<LostJournal>(),
        Some(&LostJournal {
            join_pending: false
        }),
        "{err:#}"
    );
}

/// An empty journal over an empty store is a fresh replica. Its first open
/// leaves a mark in the journal, so that the journal is never empty again
/// while the store holds what it applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_open_over_an_empty_journal_and_store_starts_fresh() {
    let c = Cluster::start(3);
    c.stop(R);
    let journal = Arc::new(InMemoryJournal::new());
    let node = c.build(
        R,
        &Arc::new(InMemoryDataStore::new()),
        &journal,
        &clock(NodeId(R)),
    );
    node.recover_state().await.expect("a fresh replica opens");
    assert_eq!(node.amnesia_fence(), None, "a fresh replica is not fenced");
    assert!(
        journal.load_clock_bound().await.unwrap().is_some(),
        "the first open leaves a clock bound in the journal"
    );
}

/// A cluster of this one node holds nobody's votes but its own: it may come
/// back over a lost journal and keep its store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_node_reopens_over_a_lost_journal() {
    let mut c = Cluster::start(1);
    let first = event("acc", 1);
    c.nodes[0].write(vec![first.clone()]).await.expect("write");
    c.stop(0);
    c.journals[0] = Arc::new(InMemoryJournal::new());
    c.restart(0).await;
    let outcome = c.nodes[0]
        .write(vec![event("acc", 2)])
        .await
        .expect("the reopened node writes");
    assert!(!outcome.conflict, "it kept its store: version 2 follows 1");
}

/// R mints ids on a wall clock stuck at one microsecond, then loses its
/// journal and comes back on the same clock. Reopened, its clock already
/// reads above every id it minted before — whatever its join brings back
/// later — and so does every id it mints once joined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_node_never_reuses_an_id() {
    let frozen = system_micros();
    let stuck = move |id: NodeId| {
        Arc::new(HybridLogicalClock::with_physical(id, move || frozen).with_max_skew(SKEW_MICROS))
    };
    let mut c = Cluster::start_with(3, config(), |id| {
        if id == NodeId(R) {
            stuck(id)
        } else {
            clock(id)
        }
    });
    let before = c.nodes[R as usize]
        .write(vec![event("acc", 1)])
        .await
        .expect("R coordinates a write")
        .txn;

    let journal = Arc::new(InMemoryJournal::new());
    let reopened = stuck(NodeId(R));
    let join = c.reopen_joining(R, journal, Arc::clone(&reopened), A).await;
    let next = reopened.now();
    assert!(
        next > before.0,
        "the reopened clock reads {next:?}, not above {before:?}"
    );

    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes")
        .unwrap()
        .expect("joined");
    let after = c.nodes[R as usize]
        .write(vec![event("acc", 2)])
        .await
        .expect("R coordinates again")
        .txn;
    assert!(after > before, "minted {after:?}, not above {before:?}");
}

/// T' commits through A and R; B never hears of it. R loses its journal and
/// store and comes back through B while A is away. B and R alone must not
/// commit a conflicting write: R would vote without T' among its
/// dependencies, and A applies T' first while B applies T first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_replica_does_not_split_what_it_forgot() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    c.cut_off(B);
    let forgotten = event("acc", 1);
    let outcome = c.nodes[A as usize]
        .write(vec![forgotten.clone()])
        .await
        .expect("A and R commit T'");
    assert!(!outcome.conflict);
    await_until("R applies T'", Duration::from_secs(5), || {
        holds(&c.stores[R as usize], &forgotten)
    })
    .await;

    c.heal();
    c.cut_off(A);
    let join = c.reopen_amnesiac(R, B).await;
    let rival = event("acc", 1);
    let b = c.nodes[B as usize].clone();
    let write = tokio::spawn({
        let rival = rival.clone();
        async move { b.write(vec![rival]).await }
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !holds(&c.stores[B as usize], &rival),
        "B committed over T', which it never saw, through a replica that forgot it"
    );

    c.heal();
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes once A is back")
        .unwrap()
        .expect("joined");
    await_until("the stores agree on T'", Duration::from_secs(20), || {
        stores_agree(&c.stores) && c.stores.iter().all(|store| holds(store, &forgotten))
    })
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(10), write).await;
}

/// The same loss, read through a barrier at B: the barrier must not pass
/// without T', which committed before it began.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_linearizable_read_never_misses_what_an_amnesiac_forgot() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    c.cut_off(B);
    let forgotten = event("acc", 1);
    c.nodes[A as usize]
        .write(vec![forgotten.clone()])
        .await
        .expect("A and R commit T'");
    await_until("R applies T'", Duration::from_secs(5), || {
        holds(&c.stores[R as usize], &forgotten)
    })
    .await;

    c.heal();
    c.cut_off(A);
    let mut join = c.reopen_amnesiac(R, B).await;
    let _ = tokio::time::timeout(Duration::from_millis(500), &mut join).await;
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        c.nodes[B as usize].read_barrier(key("acc")),
    )
    .await;
    if let Ok(Ok(())) = read {
        assert!(
            holds(&c.stores[B as usize], &forgotten),
            "a read barrier at B passed without T'"
        );
    }
}

/// Five replicas. T' commits and applies at R, A and B; C and D lag, but
/// hear R's reports, which pass T'. R loses its journal and store and
/// rejoins through C, which never saw T'; R's first report reaches C only.
/// C and D then catch up, and A, B and D — still counting R's stale report —
/// compact T' away. R's coverage from them passes T' (they ship nothing
/// below their floor), so C compacts it too before R syncs with C. R must
/// still end up holding T'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_join_holds_what_its_stale_report_vouched_for() {
    const A5: u64 = 0;
    const B5: u64 = 1;
    const C5: u64 = 2;
    const D5: u64 = 3;
    const R5: u64 = 4;
    let mut c = Cluster::start(5);
    c.baseline().await;

    // Phase 1: C and D hear nothing but watermark reports. Phase 2: R joins
    // through C; D still hears only reports, C cannot sync with A or B, R's
    // reports reach only C, and R runs no anti-entropy. Phase 3: everyone
    // else syncs; R still reports to C only, and runs no anti-entropy.
    // Phase 4: R reports to everyone and syncs with everyone but C. Phase 5:
    // nothing is lost. (A join's requests are not anti-entropy: R's join
    // through C always goes through.)
    let phase = Arc::new(AtomicU8::new(1));
    {
        let phase = Arc::clone(&phase);
        c.net
            .set_link_filter(Some(Arc::new(move |from, to, message| {
                let report = matches!(message, Message::Watermark { .. });
                let anti_entropy = matches!(message, Message::SyncRequest { .. });
                let ends = |a: u64, b: u64| {
                    (from == NodeId(a) && to == NodeId(b)) || (from == NodeId(b) && to == NodeId(a))
                };
                let touches = |x: u64| from == NodeId(x) || to == NodeId(x);
                let r_report_to_others =
                    report && from == NodeId(R5) && [A5, B5, D5].contains(&to.0);
                match phase.load(Ordering::SeqCst) {
                    1 => !report && (touches(C5) || touches(D5)),
                    2 => {
                        (!report && touches(D5))
                            || (!report && (ends(C5, A5) || ends(C5, B5)))
                            || r_report_to_others
                            || (anti_entropy && touches(R5))
                    }
                    3 => r_report_to_others || (anti_entropy && touches(R5)),
                    4 => anti_entropy && ends(R5, C5),
                    _ => false,
                }
            })));
    }

    let forgotten = event("acc", 1);
    let outcome = c.nodes[A5 as usize]
        .write(vec![forgotten.clone()])
        .await
        .expect("A, B and R commit T'");
    for i in [A5, B5, R5] {
        await_until("A, B and R apply T'", Duration::from_secs(5), || {
            holds(&c.stores[i as usize], &forgotten)
        })
        .await;
    }
    // R's reports pass T' (its own clamp, as its subscriptions see it), and
    // then reach every peer.
    let r = c.nodes[R5 as usize].clone();
    await_until("R's reports pass T'", Duration::from_secs(10), || {
        r.stable_micros() > outcome.txn.0.micros + 1_000
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    phase.store(2, Ordering::SeqCst);
    let mut join = c.reopen_amnesiac(R5, C5).await;
    let joined_early = tokio::time::timeout(Duration::from_secs(2), &mut join)
        .await
        .is_ok();

    phase.store(3, Ordering::SeqCst);
    let a = c.nodes[A5 as usize].clone();
    await_until("A compacts T' away", Duration::from_secs(20), || {
        a.command_status(outcome.txn).is_none()
    })
    .await;

    phase.store(4, Ordering::SeqCst);
    if !joined_early {
        let _ = tokio::time::timeout(Duration::from_secs(20), join).await;
    }
    // Unless R already holds T', C compacts it once R's reports pass it.
    let cc = c.nodes[C5 as usize].clone();
    let r_store = Arc::clone(&c.stores[R5 as usize]);
    await_until(
        "C compacts T' away, or R holds it",
        Duration::from_secs(20),
        || cc.command_status(outcome.txn).is_none() || holds(&r_store, &forgotten),
    )
    .await;

    phase.store(5, Ordering::SeqCst);
    await_until("R holds T'", Duration::from_secs(10), || {
        holds(&c.stores[R5 as usize], &forgotten)
    })
    .await;
}

/// A join over a lost journal waits for its contact's report to pass the
/// fence. With A away, B can prove no coverage past it, so the join gives
/// up — installing nothing. Once A is back it goes through, and R votes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_join_waits_for_a_vouch_past_its_fence() {
    let mut c = Cluster::start_with(
        3,
        NodeConfig {
            amnesia_join_timeout: Duration::from_secs(1),
            ..config()
        },
        clock,
    );
    c.baseline().await;
    c.cut_off(A);
    let join = c.reopen_amnesiac(R, B).await;
    let err = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("the join gives up")
        .unwrap()
        .expect_err("no contact can vouch past the fence while A is away");
    assert!(err.downcast_ref::<FenceNotCovered>().is_some(), "{err:#}");
    assert!(
        c.stores[R as usize].committed_events().is_empty(),
        "nothing was installed"
    );
    assert_eq!(c.nodes[R as usize].metrics().amnesia_joins_refused, 1);

    c.heal();
    tokio::time::timeout(Duration::from_secs(20), c.join(R, B))
        .await
        .expect("the join completes once A is back")
        .unwrap()
        .expect("joined");
    assert!(
        c.nodes[R as usize]
            .amnesia_fence()
            .is_some_and(|fence| fence.joined),
        "the fence records the join"
    );

    // B away: A's write needs R's vote.
    c.cut_off(B);
    c.nodes[A as usize]
        .write(vec![event("acc", 1)])
        .await
        .expect("A and R commit");
}

/// The fence is journaled before anything else, and survives a crash before
/// the join: a plain open then refuses, a second join keeps the same fence,
/// and once joined the node opens plainly again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fence_survives_a_crash_before_the_join() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    c.stop(R);
    let journal = Arc::new(InMemoryJournal::new());
    let first = c.build(
        R,
        &Arc::new(InMemoryDataStore::new()),
        &journal,
        &clock(NodeId(R)),
    );
    first
        .recover_state_for_join()
        .await
        .expect("recover a lost journal for a join");
    let fence = journal
        .load_amnesia_fence()
        .await
        .unwrap()
        .expect("the fence is journaled before the join starts");
    assert!(!fence.joined);
    drop(first);

    let plain = c.build(
        R,
        &Arc::new(InMemoryDataStore::new()),
        &journal,
        &clock(NodeId(R)),
    );
    let err = plain
        .recover_state()
        .await
        .expect_err("a plain open over a join never completed");
    assert_eq!(
        err.downcast_ref::<LostJournal>(),
        Some(&LostJournal { join_pending: true }),
        "{err:#}"
    );

    let join = c
        .reopen_joining(R, Arc::clone(&journal), clock(NodeId(R)), A)
        .await;
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes")
        .unwrap()
        .expect("joined");
    assert_eq!(
        journal.load_amnesia_fence().await.unwrap(),
        Some(AmnesiaFence {
            joined: true,
            ..fence
        }),
        "the same fence, now joined"
    );

    c.restart(R).await;
    assert!(c.nodes[R as usize]
        .amnesia_fence()
        .is_some_and(|fence| fence.joined));
}

/// A journal whose default methods keep no fence: a node opened joining over
/// it, empty, cannot fence itself, so the open fails.
struct ForgetfulJournal(InMemoryJournal);

#[async_trait::async_trait]
impl Journal for ForgetfulJournal {
    async fn record(&self, state: &evento_accord::CommandState) -> anyhow::Result<()> {
        self.0.record(state).await
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<evento_accord::CommandState>> {
        self.0.load(txn).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_journal_that_cannot_keep_the_fence_refuses_an_amnesiac_open() {
    let c = Cluster::start(3);
    c.stop(R);
    let node = c.build(
        R,
        &Arc::new(InMemoryDataStore::new()),
        &Arc::new(ForgetfulJournal(InMemoryJournal::new())),
        &clock(NodeId(R)),
    );
    node.recover_state_for_join()
        .await
        .expect_err("a journal that cannot keep the fence must refuse");
}

/// A replica that lost its journal forgot its config-Paxos promises and
/// accepts: it never answers config Paxos again under its node id — not
/// after its join, not after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_acceptor_never_answers_config_paxos_again() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    let join = c.reopen_amnesiac(R, B).await;
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes")
        .unwrap()
        .expect("joined");
    let ballot = |micros: u64| {
        Ballot(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        })
    };
    let layout = vec![(0..3).map(NodeId).collect::<Vec<_>>()];
    let now = system_micros();

    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: ballot(now),
        },
    )
    .await;
    c.send(
        9,
        R,
        Message::ConfigAccept {
            epoch: 1,
            ballot: ballot(now + 1),
            layout: layout.clone(),
        },
    )
    .await;
    let reply = next_within(&mut c.coordinator, Duration::from_millis(500)).await;
    assert!(reply.is_none(), "R answered config Paxos: {reply:?}");
    assert_eq!(c.nodes[R as usize].metrics().config_votes_withheld, 2);

    c.restart(R).await;
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 2,
            ballot: ballot(now + 2),
        },
    )
    .await;
    let reply = next_within(&mut c.coordinator, Duration::from_millis(500)).await;
    assert!(
        reply.is_none(),
        "R answered config Paxos after a restart: {reply:?}"
    );
}

/// A transaction dated more than two skews past a replica's clock comes from
/// a clock beyond the bound: no vote. One within the bound gets one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_vote_for_a_future_dated_transaction() {
    let mut c = Cluster::start(3);
    let at = |micros: u64| {
        TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        })
    };
    let preaccept = |txn: TxnId, aggregate_id: &str| Message::PreAccept {
        txn,
        keys: vec![key(aggregate_id)],
        events: vec![event(aggregate_id, 1)],
    };

    let far = at(system_micros() + 3 * SKEW_MICROS + 50_000);
    c.send(9, A, preaccept(far, "far")).await;
    let reply = next_within(&mut c.coordinator, Duration::from_millis(300)).await;
    assert!(reply.is_none(), "a vote for a far-future t0: {reply:?}");
    assert_eq!(c.nodes[A as usize].metrics().votes_future_dated, 1);
    assert_eq!(c.nodes[A as usize].command_status(far), None);

    let near = at(system_micros() + SKEW_MICROS / 2);
    c.send(9, A, preaccept(near, "near")).await;
    match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
        Some(Message::PreAcceptOk { txn, .. }) => assert_eq!(txn, near),
        other => panic!("a t0 within the bound gets a vote: {other:?}"),
    }
}

/// T' was accepted at R and B — a decision its coordinator holds, but no
/// replica knows — and A never saw it. R loses its journal. A recovery of T'
/// then hears A, which never witnessed it, B, which accepted it, and R,
/// which forgot it: T' may have committed, so it must not be invalidated on
/// R's word. Once its coordinator commits it at B, R's join goes through,
/// with T'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_never_lets_a_recovery_drop_what_may_have_committed() {
    // No sweep recovers T' on its own: this test drives every recovery.
    let mut c = Cluster::start_with(
        3,
        NodeConfig {
            recovery_timeout: Duration::from_secs(60),
            ..config()
        },
        clock,
    );
    c.baseline().await;
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let decided = event("acc", 1);
    for to in [R, B] {
        c.send(
            9,
            to,
            Message::PreAccept {
                txn,
                keys: vec![key("acc")],
                events: vec![decided.clone()],
            },
        )
        .await;
    }
    for _ in 0..2 {
        match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
            Some(Message::PreAcceptOk { .. }) => {}
            other => panic!("R and B pre-accept: {other:?}"),
        }
    }
    for to in [R, B] {
        c.send(
            9,
            to,
            Message::Accept {
                txn,
                ballot: Ballot(txn.0),
                execute_at: txn.0,
                deps: Vec::new(),
                keys: vec![key("acc")],
            },
        )
        .await;
    }
    for _ in 0..2 {
        match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
            Some(Message::AcceptOk { .. }) => {}
            other => panic!("R and B accept: {other:?}"),
        }
    }
    // A's vote fence passes T', which it never saw: it never votes for it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let join = c.reopen_amnesiac(R, B).await;
    let recovered =
        tokio::time::timeout(Duration::from_secs(5), c.nodes[B as usize].recover(txn)).await;
    assert!(
        !matches!(recovered, Ok(Ok(ref outcome)) if outcome.conflict),
        "T' was invalidated on the word of a replica that may have accepted it: {recovered:?}"
    );
    assert_eq!(
        c.nodes[B as usize].command_status(txn),
        Some(evento_accord::Status::Accepted),
        "B still holds T' undecided"
    );

    // Its coordinator commits it at B: decided, it applies, and R's join
    // goes through with it.
    c.send(
        9,
        B,
        Message::Commit {
            txn,
            execute_at: txn.0,
            deps: Vec::new(),
            keys: vec![key("acc")],
            events: vec![decided.clone()],
            reply_to: NodeId(9),
        },
    )
    .await;
    loop {
        match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
            Some(Message::ReadOk { ok, .. }) => {
                assert!(ok, "acc is at version 0");
                break;
            }
            Some(_) => continue,
            None => panic!("B reads T' once it is committed"),
        }
    }
    c.send(9, B, Message::Apply { txn, commit: true }).await;
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes once T' is decided")
        .unwrap()
        .expect("joined");
    await_until("every store holds T'", Duration::from_secs(10), || {
        c.stores.iter().all(|store| holds(store, &decided))
    })
    .await;
}

/// Under a topology that changes epochs, the node that lost its journal
/// lost its metadata log too, and reopens at the epoch it was built with. A
/// contact's vouch counts only under the epoch the node installed: its join
/// fetches the decided epochs while it waits, and joins at the current one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dynamic_amnesiac_catches_up_its_epoch_before_joining() {
    use evento_accord::{DynamicTopology, Topology};

    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let spawn = |id: NodeId| {
        let store = Arc::new(InMemoryDataStore::new());
        let topology = Arc::new(DynamicTopology::new(id, 0, vec![ids.clone()]));
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::clone(&topology) as Arc<dyn Topology>,
            clock(id),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        )
        .with_config(config());
        (node, store, topology, inbox)
    };
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for &id in &ids {
        let (node, _store, _topology, inbox) = spawn(id);
        tasks.push(node.start(inbox));
        tasks.push(node.start_recovery());
        nodes.push(node);
    }
    let base = event("acc", 1);
    nodes[0]
        .write(vec![base.clone()])
        .await
        .expect("baseline write");
    nodes[0]
        .change_topology(1, vec![ids.clone()])
        .await
        .expect("epoch 1");

    // R loses its journal and store, and comes back built at epoch 0.
    for task in tasks.drain(4..) {
        task.abort();
    }
    let (node, store, topology, inbox) = spawn(NodeId(R));
    node.recover_state_for_join()
        .await
        .expect("an amnesiac open");
    tasks.push(node.start(inbox));
    tokio::time::timeout(Duration::from_secs(20), node.join(NodeId(A)))
        .await
        .expect("the join completes")
        .expect("joined");
    assert_eq!(topology.epoch(), 1, "it joined at the current epoch");
    assert!(node.amnesia_fence().is_some_and(|fence| fence.joined));
    assert!(holds(&store, &base), "it copied the write");
}

/// While a node joins — and a join over a lost journal waits for a vouch
/// past its fence — its store is partial: a read barrier there is refused,
/// whatever its probes say. Here they say nothing is in the way: every
/// write was compacted away on its peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_refuses_a_read_barrier() {
    let c = Cluster::start(3);
    c.baseline().await;
    for node in &c.nodes {
        let node = node.clone();
        await_until("every node compacts", Duration::from_secs(10), move || {
            node.command_count() == 0
        })
        .await;
    }
    c.nodes[R as usize].begin_join();
    let read = c.nodes[R as usize].read_barrier(key("base")).await;
    assert!(
        read.is_err(),
        "a read barrier passed on a joining node, which holds part of its store"
    );
}
