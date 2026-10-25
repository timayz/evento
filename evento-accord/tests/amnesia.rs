//! A replica that lost its journal comes back under its own node id (#312).
//!
//! It forgot the votes, promises and reports it made, and the ids it minted.
//! Reopened by joining over an empty journal, it sets an amnesia fence above
//! all of them, and joins only through a contact that vouches past the fence:
//! every write that committed below it is then in the snapshot, and none
//! that did not can commit any more. A plain open over an empty journal but
//! a store holding events is refused. Whatever the journal, a replica never
//! votes for a transaction dated past its clock's skew bound, and one that
//! lost a journal under its node id abstains from config Paxos until it read
//! the other acceptors' state back from a majority of them (#338).
//!
//! A journal rolled back to an older copy of itself forgot the votes and
//! promises made since (#339). A replica opened over a journal that holds
//! something takes part only once its peers confirm the journal holds at
//! least the mark they last saw from it; one they find below it stops, and
//! comes back the way a lost journal does.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::clock::system_micros;
use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, Clock, DataStore, DurableMark, DynamicTopology, Envelope,
    FenceNotCovered, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork,
    Joining, Journal, JournalRolledBack, Key, LostJournal, Message, MessageSink, Node, NodeConfig,
    NodeId, OpenUnconfirmed, RecoveryRound, ShardedTopology, StaticTopology, Status, Timestamp,
    Topology, TxnId,
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

/// Asserts `reply` refuses a config ballot for epoch 1, naming `promised`.
fn expect_nack(reply: Option<Message>, promised: Ballot) {
    match reply {
        Some(Message::ConfigNack {
            epoch: 1,
            promised: named,
        }) => assert_eq!(named, promised, "the refusal names another promise"),
        other => panic!("not a refusal naming the promise: {other:?}"),
    }
}

/// A node's running tasks, shared with a join in flight so it can add the
/// recovery sweep it starts once joined.
type Tasks = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// How a cluster's nodes know their layout: fixed, decided by config
/// Paxos and the metadata log (a `DynamicTopology` at epoch 0), or fixed
/// over several shards.
#[derive(Clone, PartialEq, Eq)]
enum Kind {
    Static,
    Dynamic,
    Sharded(Vec<Vec<NodeId>>),
}

/// Two shards of three: `{0, 1, 2}`, the one R, A and B are in, and
/// `{3, 4, 5}`.
fn two_shards() -> Vec<Vec<NodeId>> {
    vec![(0..3).map(NodeId).collect(), (3..6).map(NodeId).collect()]
}

/// `n` real replicas `{0..n}` of one shard, each running its inbox loop and
/// recovery sweep over an in-memory store and journal, plus the inbox of an
/// outside coordinator (node 9) the tests play.
struct Cluster {
    net: Arc<InMemoryNetwork>,
    n: u64,
    kind: Kind,
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

    /// A cluster whose layout config Paxos decides, at epoch 0.
    fn start_dynamic(n: u64) -> Self {
        Self::start_dynamic_with(n, config())
    }

    fn start_dynamic_with(n: u64, config: NodeConfig) -> Self {
        Self::start_as(Kind::Dynamic, n, config, clock)
    }

    /// Six replicas over [`two_shards`], with no sweep recovering a stuck
    /// transaction on its own: the tests decide when one is recovered.
    fn start_sharded() -> Self {
        let layout = two_shards();
        let n = layout.iter().map(Vec::len).sum::<usize>() as u64;
        Self::start_as(
            Kind::Sharded(layout),
            n,
            NodeConfig {
                recovery_timeout: Duration::from_secs(60),
                ..config()
            },
            clock,
        )
    }

    fn start_with(
        n: u64,
        config: NodeConfig,
        clocks: impl Fn(NodeId) -> Arc<HybridLogicalClock>,
    ) -> Self {
        Self::start_as(Kind::Static, n, config, clocks)
    }

    fn start_as(
        kind: Kind,
        n: u64,
        config: NodeConfig,
        clocks: impl Fn(NodeId) -> Arc<HybridLogicalClock>,
    ) -> Self {
        let net = InMemoryNetwork::new();
        let mut cluster = Self {
            coordinator: net.register(NodeId(9)),
            net,
            n,
            kind,
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
        let nodes: Vec<NodeId> = (0..self.n).map(NodeId).collect();
        let topology: Arc<dyn Topology> = match &self.kind {
            Kind::Static => Arc::new(StaticTopology::new(id, nodes)),
            Kind::Dynamic => Arc::new(DynamicTopology::new(id, 0, vec![nodes])),
            Kind::Sharded(layout) => Arc::new(ShardedTopology::new(id, layout.clone())),
        };
        Node::new(
            id,
            topology,
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

    /// What a host does that restores node `i`'s journal from `copy`, an
    /// older backup of it: stops the node and opens it again, plainly, over
    /// the copy and its kept store.
    async fn reopen_over(&mut self, i: u64, copy: InMemoryJournal) {
        self.stop(i);
        self.journals[i as usize] = Arc::new(copy);
        let inbox = self.net.register(NodeId(i));
        let node = self.build(
            i,
            &self.stores[i as usize],
            &self.journals[i as usize],
            &self.clocks[i as usize],
        );
        node.recover_state()
            .await
            .expect("a plain open over the copy");
        let mut tasks = self.tasks[i as usize].lock().unwrap();
        tasks.push(node.start(inbox));
        tasks.push(node.start_recovery());
        drop(tasks);
        self.nodes[i as usize] = node;
    }

    /// A backup of node `i`'s journal taken now, once it is moving past
    /// it: writes go on, and its peers hear a mark from it the backup does
    /// not hold.
    async fn backup_left_behind(&self, i: u64) -> InMemoryJournal {
        // Taken from a node at work: one that never reported nor minted has
        // left no clock bound in its journal yet.
        while self.journals[i as usize]
            .load_clock_bound()
            .await
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let backup = self.journals[i as usize].snapshot();
        let held = DurableMark {
            life: 0,
            clock_bound: backup.load_clock_bound().await.unwrap().unwrap_or(0),
            vote_fence: backup
                .load_vote_fence()
                .await
                .unwrap()
                .unwrap_or(Timestamp::MIN),
        };
        for version in 1..=3 {
            self.nodes[A as usize]
                .write(vec![event("later", version)])
                .await
                .expect("a write after the backup");
        }
        let node = &self.nodes[i as usize];
        await_until(
            "the journal moves past the backup",
            Duration::from_secs(5),
            || !held.covers(&node.durable_mark()),
        )
        .await;
        // Reports go out every sweep.
        tokio::time::sleep(Duration::from_millis(200)).await;
        backup
    }

    async fn send(&self, from: u64, to: u64, message: Message) {
        self.net
            .sink(NodeId(from))
            .send(NodeId(to), message)
            .await
            .unwrap();
    }

    /// The one shard every node is in: the layout a config change repeats.
    fn layout(&self) -> Vec<Vec<NodeId>> {
        vec![(0..self.n).map(NodeId).collect()]
    }

    /// Node `i`'s journaled config-acceptor state for `epoch`.
    async fn acceptor(&self, i: u64, epoch: u64) -> Option<AcceptorRecord> {
        self.journals[i as usize]
            .load_acceptors()
            .await
            .unwrap()
            .into_iter()
            .find(|(e, _)| *e == epoch)
            .map(|(_, record)| record)
    }

    /// Whether node `i`, back over a lost journal, restored its config
    /// votes (#338).
    fn votes_restored(&self, i: u64) -> bool {
        self.nodes[i as usize]
            .amnesia_fence()
            .is_some_and(|fence| fence.votes_restored)
    }

    /// Reopens node `i` over a lost journal through `contact`, and waits
    /// for its join.
    async fn lose_and_rejoin(&mut self, i: u64, contact: u64) {
        let join = self.reopen_amnesiac(i, contact).await;
        tokio::time::timeout(Duration::from_secs(20), join)
            .await
            .expect("the join completes")
            .unwrap()
            .expect("joined");
    }

    /// A write every replica applies: one per shard.
    async fn baseline(&self) {
        let shards: Vec<Vec<NodeId>> = match &self.kind {
            Kind::Sharded(layout) => layout.clone(),
            _ => vec![(0..self.n).map(NodeId).collect()],
        };
        for (shard, replicas) in shards.iter().enumerate() {
            let base = event(&self.id_in_shard(shard, "base"), 1);
            self.nodes[replicas[0].0 as usize]
                .write(vec![base.clone()])
                .await
                .expect("baseline write");
            for replica in replicas {
                let store = &self.stores[replica.0 as usize];
                await_until(
                    "every store holds the baseline",
                    Duration::from_secs(5),
                    || holds(store, &base),
                )
                .await;
            }
        }
    }

    /// An aggregate id whose key `shard` owns: any, on one shard.
    fn id_in_shard(&self, shard: usize, prefix: &str) -> String {
        let Kind::Sharded(layout) = &self.kind else {
            return prefix.to_string();
        };
        let probe = ShardedTopology::new(NodeId(0), layout.clone());
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|id| probe.shard_of(&Key(id.clone())) == shard)
            .expect("an id in the shard")
    }

    /// The stores of `shard`'s replicas.
    fn shard_stores(&self, shard: usize) -> Vec<Arc<InMemoryDataStore>> {
        let Kind::Sharded(layout) = &self.kind else {
            return self.stores.clone();
        };
        layout[shard]
            .iter()
            .map(|id| Arc::clone(&self.stores[id.0 as usize]))
            .collect()
    }

    /// Sends `inner` about a transaction over `route` to `to`, as its
    /// coordinator (node 9) would.
    async fn send_routed(&self, to: u64, route: &[Key], inner: Message) {
        self.send(
            9,
            to,
            Message::Routed {
                route: route.to_vec(),
                inner: Box::new(inner),
            },
        )
        .await;
    }

    /// Expects `n` replies of the kind `is` names from the cluster.
    async fn expect_replies(&mut self, n: usize, what: &str, is: impl Fn(&Message) -> bool) {
        for _ in 0..n {
            match next_within(&mut self.coordinator, Duration::from_secs(5)).await {
                Some(reply) if is(&reply) => {}
                other => panic!("expected {what}: {other:?}"),
            }
        }
    }

    /// Waits until node `i` holds `txn` at `status` or beyond.
    async fn await_status(&self, i: u64, txn: TxnId, status: Status) {
        let node = self.nodes[i as usize].clone();
        await_until(
            "the node holds the transaction at the status",
            Duration::from_secs(5),
            || node.command_status(txn).is_some_and(|s| s >= status),
        )
        .await;
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

/// A replica that lost its journal forgot the config-Paxos promises and
/// accepts it made. It abstains — a prepare or accept gets no answer, and
/// a read of its state says so — until it read the others' state back from
/// a majority of all the acceptors (#338): then it holds what they hold,
/// and votes again only above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_amnesiac_acceptor_answers_config_paxos_again_once_restored() {
    let mut c = Cluster::start_dynamic(3);
    c.baseline().await;
    c.nodes[A as usize]
        .change_topology(1, c.layout())
        .await
        .expect("epoch 1");
    let held = c.acceptor(A, 1).await.expect("A accepted epoch 1");
    let (decided, entry) = held.accepted.clone().expect("A accepted a value");
    let ballot = |micros: u64| {
        Ballot(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        })
    };

    let join = c.reopen_amnesiac(R, B).await;
    // Still joining: it answers the read with nothing, abstaining.
    c.send(9, R, Message::ConfigRead { id: 7 }).await;
    match next_within(&mut c.coordinator, Duration::from_secs(2)).await {
        Some(Message::ConfigReadReply {
            id: 7,
            acceptors,
            voting: false,
        }) => assert!(acceptors.is_empty(), "it forgot everything: {acceptors:?}"),
        other => panic!("not an abstaining answer: {other:?}"),
    }
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: ballot(decided.0.micros - 1),
        },
    )
    .await;
    let reply = next_within(&mut c.coordinator, Duration::from_millis(300)).await;
    assert!(
        reply.is_none(),
        "R answered config Paxos unrestored: {reply:?}"
    );
    assert_eq!(c.nodes[R as usize].metrics().config_votes_withheld, 1);
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("the join completes")
        .unwrap()
        .expect("joined");

    await_until(
        "R restores its config votes",
        Duration::from_secs(10),
        || c.votes_restored(R),
    )
    .await;
    assert_eq!(c.nodes[R as usize].metrics().config_votes_restored, 1);
    assert_eq!(
        c.acceptor(R, 1).await,
        Some(held.clone()),
        "R journaled what A holds for epoch 1"
    );
    // Below what the others promised: refused, naming it.
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: ballot(decided.0.micros - 1),
        },
    )
    .await;
    expect_nack(
        next_within(&mut c.coordinator, Duration::from_secs(2)).await,
        held.promised,
    );
    c.send(
        9,
        R,
        Message::ConfigAccept {
            epoch: 1,
            ballot: ballot(decided.0.micros - 1),
            entry: entry.clone(),
        },
    )
    .await;
    expect_nack(
        next_within(&mut c.coordinator, Duration::from_secs(2)).await,
        held.promised,
    );
    // Above it: promised, reporting the value the others accepted.
    let above = ballot(system_micros());
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: above,
        },
    )
    .await;
    match next_within(&mut c.coordinator, Duration::from_secs(2)).await {
        Some(Message::ConfigPromise {
            epoch: 1,
            ballot,
            accepted_ballot,
            accepted_entry,
        }) => {
            assert_eq!(ballot, above);
            assert_eq!(accepted_ballot, decided);
            assert_eq!(accepted_entry, Some(entry));
        }
        other => panic!("not a promise of the value the others accepted: {other:?}"),
    }
    assert_eq!(c.nodes[R as usize].metrics().config_votes_withheld, 1);
}

/// A read that hears fewer voting acceptors than a majority of all of
/// them restores nothing: the node keeps abstaining, and reads again each
/// sweep until it hears enough (#338).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_that_hears_too_few_acceptors_waits_for_them() {
    // Long collection windows: the read waits that long past the fence,
    // which leaves time to cut a node off once the join is done.
    let mut c = Cluster::start_dynamic_with(
        3,
        NodeConfig {
            collect_timeout: Duration::from_secs(3),
            ..config()
        },
    );
    c.baseline().await;
    c.nodes[A as usize]
        .change_topology(1, c.layout())
        .await
        .expect("epoch 1");
    c.lose_and_rejoin(R, B).await;
    assert!(!c.votes_restored(R), "restored before the read could run");
    c.cut_off(A);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        !c.votes_restored(R),
        "restored on B's answer alone: one of three acceptors is no majority"
    );
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: Ballot(Timestamp {
                micros: system_micros(),
                logical: 0,
                node: NodeId(9),
            }),
        },
    )
    .await;
    let reply = next_within(&mut c.coordinator, Duration::from_millis(300)).await;
    assert!(
        reply.is_none(),
        "R answered config Paxos unrestored: {reply:?}"
    );
    assert_eq!(c.nodes[R as usize].metrics().config_votes_withheld, 1);

    c.heal();
    await_until(
        "R restores its config votes",
        Duration::from_secs(15),
        || c.votes_restored(R),
    )
    .await;
    assert_eq!(
        c.acceptor(R, 1).await,
        c.acceptor(A, 1).await,
        "R journaled what A holds for epoch 1"
    );
}

/// In a group of three, each of two replicas that lost their journals
/// before either restored its votes needs the other's answer, which counts
/// for nothing: neither restores, and the group's config is frozen (#338,
/// OPERATIONS §7). The way out is a new node id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_overlapping_losses_in_a_group_of_three_never_restore() {
    let mut c = Cluster::start_dynamic_with(
        3,
        NodeConfig {
            collect_timeout: Duration::from_secs(3),
            ..config()
        },
    );
    c.baseline().await;
    c.nodes[A as usize]
        .change_topology(1, c.layout())
        .await
        .expect("epoch 1");
    c.lose_and_rejoin(R, B).await;
    c.lose_and_rejoin(B, A).await;
    assert!(!c.votes_restored(R) && !c.votes_restored(B));
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(
        !c.votes_restored(R) && !c.votes_restored(B),
        "an abstaining acceptor's answer counted"
    );
    for i in [R, B] {
        c.send(
            9,
            i,
            Message::ConfigPrepare {
                epoch: 1,
                ballot: Ballot(Timestamp {
                    micros: system_micros(),
                    logical: 0,
                    node: NodeId(9),
                }),
            },
        )
        .await;
        let reply = next_within(&mut c.coordinator, Duration::from_millis(300)).await;
        assert!(
            reply.is_none(),
            "node {i} answered config Paxos unrestored: {reply:?}"
        );
        assert_eq!(c.nodes[i as usize].metrics().config_votes_withheld, 1);
    }
}

/// The restored votes are journaled before the fence records them: a plain
/// restart keeps both (#338).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_config_vote_outlives_a_restart() {
    let mut c = Cluster::start_dynamic(3);
    c.baseline().await;
    c.nodes[A as usize]
        .change_topology(1, c.layout())
        .await
        .expect("epoch 1");
    let held = c.acceptor(A, 1).await.expect("A accepted epoch 1");
    c.lose_and_rejoin(R, B).await;
    await_until(
        "R restores its config votes",
        Duration::from_secs(10),
        || c.votes_restored(R),
    )
    .await;

    c.restart(R).await;
    let node = c.nodes[R as usize].clone();
    await_until("R's restart is confirmed", Duration::from_secs(5), || {
        node.confirmed()
    })
    .await;
    assert!(c.votes_restored(R), "the restart forgot the restoration");
    c.send(
        9,
        R,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: Ballot(Timestamp {
                micros: held.promised.0.micros - 1,
                logical: 0,
                node: NodeId(9),
            }),
        },
    )
    .await;
    expect_nack(
        next_within(&mut c.coordinator, Duration::from_secs(2)).await,
        held.promised,
    );
    assert_eq!(c.nodes[R as usize].metrics().config_votes_withheld, 0);
}

/// Every acceptor loses its journal in turn, each restoring its votes
/// before the next: a restored acceptor's answer counts, so each read
/// hears a majority, and the group still changes its layout (#338).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acceptors_that_lose_their_journals_one_after_another_still_change_the_topology() {
    let mut c = Cluster::start_dynamic(3);
    c.baseline().await;
    c.nodes[A as usize]
        .change_topology(1, c.layout())
        .await
        .expect("epoch 1");
    for i in [R, A, B] {
        c.lose_and_rejoin(i, (i + 1) % 3).await;
        await_until(
            "the node restores its config votes",
            Duration::from_secs(10),
            || c.votes_restored(i),
        )
        .await;
    }
    c.nodes[A as usize]
        .change_topology(2, c.layout())
        .await
        .expect("epoch 2, decided by acceptors that all lost a journal");
    for i in 0..3 {
        assert!(
            c.acceptor(i, 2)
                .await
                .is_some_and(|acc| acc.accepted.is_some()),
            "node {i} holds no accept for epoch 2"
        );
    }
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
        epoch: 0,
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
/// R's word. A never vouched past T' — R and B held it unapplied — so A
/// votes, and the recovery commits it (#337). Its coordinator's late Commit
/// finds it decided, and R's join goes through, with T'.
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
                epoch: 0,
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
                epoch: 0,
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
    // Long past the compaction margin, which A's clock passed for T'.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let join = c.reopen_amnesiac(R, B).await;
    let recovered =
        tokio::time::timeout(Duration::from_secs(5), c.nodes[B as usize].recover(txn)).await;
    assert!(
        matches!(recovered, Ok(Ok(ref outcome)) if !outcome.conflict),
        "T' may have committed, and A never vouched past it: a recovery commits it, \
         never invalidating it on the word of a replica that may have accepted it: {recovered:?}"
    );

    // Its coordinator's late Commit finds it decided at B, and R's join goes
    // through with it.
    c.send(
        9,
        B,
        Message::Commit {
            epoch: 0,
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

/// #337: T was accepted at R and A, and its Commit reached R alone before its
/// coordinator vanished. B never witnessed it, and the compaction margin
/// passed long ago. R loses its journal. A recovery then hears A, which
/// accepted T, R, which forgot it, and B, which never saw it: T may have
/// committed, so only committing it is safe, and that takes B's vote. B never
/// vouched past T — A and R held it unapplied all along — so B gives it, and
/// R's join, which waits for a report past T, goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_decides_what_an_amnesiac_and_one_replica_accepted() {
    // No sweep recovers T before R loses its journal: R, holding the
    // Commit, would decide it at once.
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
    for to in [R, A] {
        c.send(
            9,
            to,
            Message::PreAccept {
                epoch: 0,
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
            other => panic!("R and A pre-accept: {other:?}"),
        }
    }
    for to in [R, A] {
        c.send(
            9,
            to,
            Message::Accept {
                epoch: 0,
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
            other => panic!("R and A accept: {other:?}"),
        }
    }
    // The Commit reaches R alone; R reads and waits for a decision that
    // never comes.
    c.send(
        9,
        R,
        Message::Commit {
            epoch: 0,
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
            Some(Message::ReadOk { .. }) => break,
            Some(_) => continue,
            None => panic!("R reads T once it is committed"),
        }
    }
    // Well past the margin: B's clock less the margin passed T long ago.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let join = c.reopen_amnesiac(R, A).await;
    let recovered = tokio::time::timeout(Duration::from_secs(10), c.nodes[A as usize].recover(txn))
        .await
        .expect("the recovery ends");
    match recovered {
        Ok(outcome) => assert!(!outcome.conflict, "T may have committed: {outcome:?}"),
        Err(err) => panic!("no recovery decided T, which pins every report below it: {err:#}"),
    }
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes once T is decided")
        .unwrap()
        .expect("joined");
    await_until("every store holds T", Duration::from_secs(10), || {
        c.stores.iter().all(|store| holds(store, &decided))
    })
    .await;
    assert!(stores_agree(&c.stores), "the stores agree");
}

/// #377, the multi-shard shape of #337: T spans both shards. It was accepted
/// at R and A on the first shard and at every replica of the second, and
/// its Commit reached R alone before its coordinator vanished. B never
/// witnessed it, and the compaction margin passed long ago. R loses its
/// journal. A routed recovery then hears A (accepted), R (forgot it) and B
/// (late: it never saw it, and is past the deadline) on the first shard: T
/// may have committed, so only committing it is safe, and that takes B's
/// vote — for its shard's part, which A's report carries. B gives it, T
/// commits on both shards, and R's join goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_routed_recovery_decides_what_an_amnesiac_and_one_replica_accepted() {
    let mut c = Cluster::start_sharded();
    c.baseline().await;
    let (ea, eb) = (
        event(&c.id_in_shard(0, "a"), 1),
        event(&c.id_in_shard(1, "b"), 1),
    );
    let route = vec![Key::of(&ea), Key::of(&eb)];
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let voters = [R, A, 3, 4, 5];
    for to in voters {
        c.send_routed(
            to,
            &route,
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: route.clone(),
                events: vec![ea.clone(), eb.clone()],
            },
        )
        .await;
    }
    c.expect_replies(voters.len(), "a pre-accept", |m| {
        matches!(m, Message::PreAcceptOk { .. })
    })
    .await;
    for to in voters {
        c.send_routed(
            to,
            &route,
            Message::Accept {
                epoch: 0,
                txn,
                ballot: Ballot(txn.0),
                execute_at: txn.0,
                deps: Vec::new(),
                keys: route.clone(),
            },
        )
        .await;
    }
    c.expect_replies(voters.len(), "an accept", |m| {
        matches!(m, Message::AcceptOk { .. })
    })
    .await;
    // The Commit reaches R alone.
    c.send_routed(
        R,
        &route,
        Message::Commit {
            epoch: 0,
            txn,
            execute_at: txn.0,
            deps: Vec::new(),
            keys: route.clone(),
            events: vec![ea.clone(), eb.clone()],
            reply_to: NodeId(9),
        },
    )
    .await;
    c.await_status(R, txn, Status::Committed).await;
    // Well past the margin: B's clock less the margin passed T long ago.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let join = c.reopen_amnesiac(R, A).await;
    let recovered = tokio::time::timeout(Duration::from_secs(10), c.nodes[A as usize].recover(txn))
        .await
        .expect("the recovery ends");
    match recovered {
        Ok(outcome) => assert!(!outcome.conflict, "T may have committed: {outcome:?}"),
        Err(err) => panic!("no recovery decided T, which pins every report below it: {err:#}"),
    }
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes once T is decided")
        .unwrap()
        .expect("joined");
    for (shard, decided) in [(0, &ea), (1, &eb)] {
        await_until(
            "every store of the shard holds T",
            Duration::from_secs(10),
            || {
                c.shard_stores(shard)
                    .iter()
                    .all(|store| holds(store, decided))
            },
        )
        .await;
        assert!(stores_agree(&c.shard_stores(shard)), "the stores agree");
    }
}

/// T spans both shards, and was pre-accepted at R alone on the first and
/// at every replica of the second; its coordinator vanished. R loses its
/// journal. A routed recovery from the second shard hears R (forgot it) and
/// A and B (late): were they to refuse it, no quorum of the first shard
/// could ever have committed it. So they are asked to refuse it for good,
/// and T is invalidated on both shards, rather than left undecided (#377).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_routed_recovery_invalidates_what_only_an_amnesiac_held_on_its_shard() {
    let mut c = Cluster::start_sharded();
    c.baseline().await;
    let (ea, eb) = (
        event(&c.id_in_shard(0, "a"), 1),
        event(&c.id_in_shard(1, "b"), 1),
    );
    let route = vec![Key::of(&ea), Key::of(&eb)];
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let voters = [R, 3, 4, 5];
    for to in voters {
        c.send_routed(
            to,
            &route,
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: route.clone(),
                events: vec![ea.clone(), eb.clone()],
            },
        )
        .await;
    }
    c.expect_replies(voters.len(), "a pre-accept", |m| {
        matches!(m, Message::PreAcceptOk { .. })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let join = c.reopen_amnesiac(R, A).await;
    let recovered = tokio::time::timeout(Duration::from_secs(10), c.nodes[3].recover(txn))
        .await
        .expect("the recovery ends");
    match recovered {
        Ok(outcome) => assert!(outcome.conflict, "T never committed: {outcome:?}"),
        Err(err) => panic!("no recovery decided T: {err:#}"),
    }
    for i in [A, B, 3, 4, 5] {
        c.await_status(i, txn, Status::Applied).await;
    }
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes")
        .unwrap()
        .expect("joined");
    for shard in 0..2 {
        assert!(
            c.shard_stores(shard)
                .iter()
                .all(|store| !holds(store, &ea) && !holds(store, &eb)),
            "T was invalidated: no store holds its events"
        );
    }
}

/// T spans both shards, pre-accepted at R and A on the first and at every
/// replica of the second; its coordinator vanished. R loses its journal. A
/// routed recovery hears A holding T pre-accepted, R (forgot it) and B
/// (late): were B to refuse it, no quorum of the first shard could have
/// committed T — a fast one needs B, a slow one an accept A never gave —
/// nor ever will. B is asked to refuse for good, and T is invalidated on
/// both shards, though one refusal meets no quorum of its shard by itself
/// (#386).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_routed_recovery_invalidates_what_an_amnesiac_and_a_holder_pre_accepted() {
    let mut c = Cluster::start_sharded();
    c.baseline().await;
    let (ea, eb) = (
        event(&c.id_in_shard(0, "a"), 1),
        event(&c.id_in_shard(1, "b"), 1),
    );
    let route = vec![Key::of(&ea), Key::of(&eb)];
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let voters = [R, A, 3, 4, 5];
    for to in voters {
        c.send_routed(
            to,
            &route,
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: route.clone(),
                events: vec![ea.clone(), eb.clone()],
            },
        )
        .await;
    }
    c.expect_replies(voters.len(), "a pre-accept", |m| {
        matches!(m, Message::PreAcceptOk { .. })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let join = c.reopen_amnesiac(R, A).await;
    let recovered = tokio::time::timeout(Duration::from_secs(10), c.nodes[3].recover(txn))
        .await
        .expect("the recovery ends");
    match recovered {
        Ok(outcome) => assert!(outcome.conflict, "T never committed: {outcome:?}"),
        Err(err) => panic!("no recovery decided T: {err:#}"),
    }
    for i in [A, B, 3, 4, 5] {
        c.await_status(i, txn, Status::Applied).await;
    }
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes")
        .unwrap()
        .expect("joined");
    for shard in 0..2 {
        assert!(
            c.shard_stores(shard)
                .iter()
                .all(|store| !holds(store, &ea) && !holds(store, &eb)),
            "T was invalidated: no store holds its events"
        );
    }
}

/// T spans both shards, pre-accepted everywhere on the first and at one
/// replica of the second, and its coordinator is gone. A recovery within
/// the coordinator's deadline hears the second shard's two other replicas
/// promise the ballot, holding nothing: they are handed their shard's part
/// from the holder's report, pre-accept it, and T commits — without
/// waiting for the deadline (#377).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_routed_recovery_gives_a_blind_replica_its_part_within_the_deadline() {
    let mut c = Cluster::start_sharded();
    c.baseline().await;
    let (ea, eb) = (
        event(&c.id_in_shard(0, "a"), 1),
        event(&c.id_in_shard(1, "b"), 1),
    );
    let route = vec![Key::of(&ea), Key::of(&eb)];
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let voters = [A, B, R, 3];
    for to in voters {
        c.send_routed(
            to,
            &route,
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: route.clone(),
                events: vec![ea.clone(), eb.clone()],
            },
        )
        .await;
    }
    c.expect_replies(voters.len(), "a pre-accept", |m| {
        matches!(m, Message::PreAcceptOk { .. })
    })
    .await;

    let recovered = tokio::time::timeout(Duration::from_secs(10), c.nodes[A as usize].recover(txn))
        .await
        .expect("the recovery ends");
    match recovered {
        Ok(outcome) => assert!(!outcome.conflict, "T commits: {outcome:?}"),
        Err(err) => panic!("no recovery decided T within the deadline: {err:#}"),
    }
    for i in [4, 5] {
        c.await_status(i, txn, Status::Applied).await;
        assert_eq!(
            c.nodes[i as usize].metrics().votes_fenced,
            0,
            "node {i} was handed its part, not refused"
        );
    }
    for (shard, decided) in [(0, &ea), (1, &eb)] {
        await_until(
            "every store of the shard holds T",
            Duration::from_secs(10),
            || {
                c.shard_stores(shard)
                    .iter()
                    .all(|store| holds(store, decided))
            },
        )
        .await;
    }
}

/// D was pre-accepted at R alone, and W, a write on the same key, committed
/// with D as a dependency: R's vote saw it. R loses its journal before
/// anything decided D, so no replica holds D any more, and W waits on it at
/// A and B. Their dependency round finds that nothing can vote for D: it
/// never committed, and is invalidated. W applies, and R's join, which
/// waits for a report past W, goes through (#337).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dependency_only_an_amnesiac_held_is_invalidated() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    // R hears the outside coordinator alone, so no recovery decides D
    // through it before it loses its journal.
    c.cut_off(R);
    let now = system_micros();
    let orphan = TxnId(Timestamp {
        micros: now,
        logical: 0,
        node: NodeId(9),
    });
    let txn = TxnId(Timestamp {
        micros: now + 1_000,
        logical: 0,
        node: NodeId(9),
    });
    let (forgotten, write) = (event("acc", 1), event("acc", 1));
    c.send(
        9,
        R,
        Message::PreAccept {
            epoch: 0,
            txn: orphan,
            keys: vec![key("acc")],
            events: vec![forgotten.clone()],
        },
    )
    .await;
    match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
        Some(Message::PreAcceptOk { txn: t, .. }) if t == orphan => {}
        other => panic!("R pre-accepts D: {other:?}"),
    }
    for to in [A, R] {
        c.send(
            9,
            to,
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: vec![key("acc")],
                events: vec![write.clone()],
            },
        )
        .await;
    }
    let mut deps = Vec::new();
    for _ in 0..2 {
        match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
            Some(Message::PreAcceptOk {
                txn: t, deps: vote, ..
            }) if t == txn => {
                deps.extend(vote);
            }
            other => panic!("A and R pre-accept W: {other:?}"),
        }
    }
    deps.sort();
    deps.dedup();
    assert!(deps.contains(&orphan), "R's vote has D as a dependency");
    for to in [A, R] {
        c.send(
            9,
            to,
            Message::Accept {
                epoch: 0,
                txn,
                ballot: Ballot(txn.0),
                execute_at: txn.0,
                deps: deps.clone(),
                keys: vec![key("acc")],
            },
        )
        .await;
    }
    for _ in 0..2 {
        match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
            Some(Message::AcceptOk { .. }) => {}
            other => panic!("A and R accept W: {other:?}"),
        }
    }
    for to in [A, B] {
        c.send(
            9,
            to,
            Message::Commit {
                epoch: 0,
                txn,
                execute_at: txn.0,
                deps: deps.clone(),
                keys: vec![key("acc")],
                events: vec![write.clone()],
                reply_to: NodeId(9),
            },
        )
        .await;
    }

    // R loses its journal and its store with D undecided.
    c.stop(R);
    c.heal();
    let join = c.reopen_amnesiac(R, A).await;
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes once W is decided")
        .unwrap()
        .expect("joined");
    await_until("every store holds W", Duration::from_secs(10), || {
        c.stores.iter().all(|store| holds(store, &write))
    })
    .await;
    assert!(
        c.stores.iter().all(|store| !holds(store, &forgotten)),
        "D never committed"
    );
    assert!(stores_agree(&c.stores), "the stores agree");
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

/// Nor does a joining node coordinate a write: it buffers the replies its
/// coordination would need until its join ends, so the write could not
/// complete, and what it left pre-accepted on a minority of a shard no
/// recovery could decide while it stays silent (#388).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_refuses_a_write() {
    let c = Cluster::start(3);
    c.baseline().await;
    c.nodes[R as usize].begin_join();
    match c.nodes[R as usize].write(vec![event("acc", 1)]).await {
        Err(err) => assert!(err.is::<Joining>(), "refused for another reason: {err:#}"),
        Ok(outcome) => panic!("a joining node coordinated a write: {outcome:?}"),
    }
}

/// A node still joining answers a recovery of a transaction it does not
/// hold at once, as a late replica would, rather than buffering the question
/// until its join ends (#390): asked, it promises the ballot and says it is
/// late; asked to refuse, it refuses for good, and the PreAccept it buffered,
/// replayed after the join, is refused. Buffered, the question would wait on
/// a join that may wait on the shard's watermark, which the transaction
/// pins. Here T is above R's amnesia fence, so no `Amnesiac` answers for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_answers_a_recovery_of_what_it_never_witnessed() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    let join = c.reopen_amnesiac(R, A).await;
    // A's report never reaches R: its join waits, as it would for a vouch
    // the transaction below pins.
    c.cut_off(A);
    // Past the fence (three skews).
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        c.nodes[R as usize].is_joining(),
        "R joined before T was asked about"
    );
    let e = event("acc", 1);
    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    let fence = c.nodes[R as usize]
        .amnesia_fence()
        .expect("R is fenced")
        .fence_micros;
    assert!(txn.0.micros > fence, "T is above the fence");
    let ballot = Ballot(Timestamp {
        logical: 1,
        ..txn.0
    });
    let recovery = |round| Message::BeginRecovery {
        txn,
        ballot,
        epoch: 0,
        keys: vec![Key::of(&e)],
        events: vec![e.clone()],
        round,
    };
    // T's PreAccept reaches R first, buffered until its join ends.
    c.send(
        9,
        R,
        Message::PreAccept {
            epoch: 0,
            txn,
            keys: vec![Key::of(&e)],
            events: vec![e.clone()],
        },
    )
    .await;
    // A recovery asks R about T: late, at once — a promise, no vote.
    c.send(9, R, recovery(RecoveryRound::Ask)).await;
    c.expect_replies(1, "a late answer", |m| matches!(m, Message::Late { .. }))
        .await;
    assert!(
        c.nodes[R as usize].is_joining(),
        "the answer waited for the join"
    );
    // Asked to refuse T for good, it does, at once.
    c.send(9, R, recovery(RecoveryRound::Refuse)).await;
    c.expect_replies(1, "a refusal for good", |m| {
        matches!(
            m,
            Message::NotWitnessed {
                redundant: false,
                ..
            }
        )
    })
    .await;
    assert!(
        c.nodes[R as usize].is_joining(),
        "the refusal waited for the join"
    );
    // Joined, R replays the PreAccept, and refuses it: it never votes for T.
    c.heal();
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes")
        .unwrap()
        .expect("joined");
    let node = c.nodes[R as usize].clone();
    await_until(
        "the replayed PreAccept is refused as fenced",
        Duration::from_secs(5),
        move || node.metrics().votes_fenced >= 1,
    )
    .await;
    assert_eq!(
        c.nodes[R as usize].command_status(txn),
        None,
        "R voted for T after refusing it"
    );
    assert!(
        next_within(&mut c.coordinator, Duration::from_millis(300))
            .await
            .is_none(),
        "R answered T's PreAccept"
    );
}

/// A recovery that asks a node still joining to vote for a transaction it
/// never witnessed waits: the vote needs the snapshot the join installs
/// first. The round is buffered, and once joined the node pre-accepts the
/// part it was handed (#390) — a refusal for good instead would cost the
/// shard a voter it may need, should another replica lose its journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_node_votes_once_joined() {
    // A wide skew bound, and a margin covering it: room to date T above the
    // vouch R joins with — its contact's report, which lags the clock by
    // the margin and passes R's fence (three skews) at the join — yet within
    // the horizon a vote accepts (two skews ahead of the clock).
    const SKEW: u64 = 200_000;
    let mut c = Cluster::start_with(
        3,
        NodeConfig {
            compaction_margin: Duration::from_millis(700),
            ..config()
        },
        |id| Arc::new(HybridLogicalClock::new(id).with_max_skew(SKEW)),
    );
    c.baseline().await;
    let join = c.reopen_amnesiac(R, A).await;
    tokio::time::sleep(Duration::from_micros(2 * SKEW)).await;
    assert!(c.nodes[R as usize].is_joining());
    let e = event("acc", 1);
    let txn = TxnId(Timestamp {
        micros: system_micros() + SKEW + SKEW / 2,
        logical: 0,
        node: NodeId(9),
    });
    c.send(
        9,
        R,
        Message::BeginRecovery {
            txn,
            ballot: Ballot(Timestamp {
                logical: 1,
                ..txn.0
            }),
            epoch: 0,
            keys: vec![Key::of(&e)],
            events: vec![e.clone()],
            round: RecoveryRound::Vote,
        },
    )
    .await;
    assert!(
        next_within(&mut c.coordinator, Duration::from_millis(100))
            .await
            .is_none(),
        "R voted while joining"
    );
    assert!(c.nodes[R as usize].is_joining(), "R joined already");
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes")
        .unwrap()
        .expect("joined");
    c.expect_replies(1, "a vote once joined", |m| {
        matches!(m, Message::BeginRecoveryOk { known: true, .. })
    })
    .await;
    assert!(
        c.nodes[R as usize].command_status(txn) >= Some(Status::PreAccepted),
        "R holds T"
    );
}

/// R's disk is rolled back to an older copy of its journal, which lacks
/// votes and promises it made since. Its peers saw a mark from it above the
/// copy's: they tell it so, and it takes no part — it votes on nothing and
/// takes no write (#339).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reopened_over_an_older_copy_of_its_journal_takes_no_part() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    let backup = c.backup_left_behind(R).await;

    c.reopen_over(R, backup).await;
    let node = c.nodes[R as usize].clone();
    await_until(
        "R hears its journal rolled back",
        Duration::from_secs(5),
        || node.rolled_back(),
    )
    .await;
    assert!(!node.confirmed());
    assert_eq!(node.metrics().journals_rolled_back, 1);

    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    c.send(
        9,
        R,
        Message::PreAccept {
            epoch: 0,
            txn,
            keys: vec![key("acc")],
            events: vec![event("acc", 1)],
        },
    )
    .await;
    let vote = next_within(&mut c.coordinator, Duration::from_millis(300)).await;
    assert!(
        vote.is_none(),
        "R voted over a rolled-back journal: {vote:?}"
    );
    let write = node.write(vec![event("acc", 1)]).await;
    assert!(
        write
            .as_ref()
            .is_err_and(|err| err.downcast_ref::<JournalRolledBack>().is_some()),
        "R took a write over a rolled-back journal: {write:?}"
    );
}

/// A plain restart over the journal as it was left is confirmed, and the
/// node takes part again (#339).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_restart_is_confirmed() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.restart(R).await;
    let node = c.nodes[R as usize].clone();
    await_until("R's restart is confirmed", Duration::from_secs(5), || {
        node.confirmed()
    })
    .await;
    assert!(!node.rolled_back());
    let written = event("acc", 1);
    node.write(vec![written.clone()])
        .await
        .expect("R takes writes again");
    await_until(
        "every store holds the write",
        Duration::from_secs(5),
        || c.stores.iter().all(|store| holds(store, &written)),
    )
    .await;
}

/// Every node restarts at once: each answers the others from the marks its
/// own journal kept, and all are confirmed (#339).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_cluster_restart_confirms_every_node() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..3 {
        c.stop(i);
    }
    for i in 0..3 {
        c.restart(i).await;
    }
    for i in 0..3 {
        let node = c.nodes[i].clone();
        await_until("every restart is confirmed", Duration::from_secs(5), || {
            node.confirmed()
        })
        .await;
        assert!(!node.rolled_back(), "node {i} was found rolled back");
    }
    let written = event("acc", 1);
    c.nodes[R as usize]
        .write(vec![written.clone()])
        .await
        .expect("the cluster takes writes again");
    await_until(
        "every store holds the write",
        Duration::from_secs(5),
        || c.stores.iter().all(|store| holds(store, &written)),
    )
    .await;
}

/// A and B restart while R is down, so they know R's mark only from their
/// journals; R then comes back over an older copy of its own. They still
/// tell it its journal rolled back (#339).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_keep_the_marks_they_saw_across_their_own_restart() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    let backup = c.backup_left_behind(R).await;
    c.stop(R);
    for i in [A, B] {
        c.restart(i).await;
    }
    c.reopen_over(R, backup).await;
    let node = c.nodes[R as usize].clone();
    await_until(
        "R hears its journal rolled back",
        Duration::from_secs(5),
        || node.rolled_back(),
    )
    .await;
}

/// R found rolled back is opened again the way a lost journal is: over an
/// empty journal and store, joining. It gets back every write, and takes
/// part again (#339, #312).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_node_comes_back_through_a_lost_journals_path() {
    let mut c = Cluster::start(3);
    c.baseline().await;
    let backup = c.backup_left_behind(R).await;
    c.reopen_over(R, backup).await;
    let node = c.nodes[R as usize].clone();
    await_until(
        "R hears its journal rolled back",
        Duration::from_secs(5),
        || node.rolled_back(),
    )
    .await;

    node.retire().await;
    let join = c.reopen_amnesiac(R, A).await;
    tokio::time::timeout(Duration::from_secs(20), join)
        .await
        .expect("R's join completes")
        .unwrap()
        .expect("joined");
    let node = c.nodes[R as usize].clone();
    assert!(node.confirmed() && !node.rolled_back());
    let written = event("acc", 1);
    node.write(vec![written.clone()])
        .await
        .expect("R takes writes again");
    await_until(
        "every store holds the write",
        Duration::from_secs(5),
        || c.stores.iter().all(|store| holds(store, &written)),
    )
    .await;
    assert!(stores_agree(&c.stores), "the stores agree");
}

/// R restarts cut off from A and B: no node can confirm its journal. It
/// takes no write, raises no clock bound, and keeps a vote it is asked for
/// until it is confirmed (#339).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfirmed_node_mints_nothing_and_votes_once_confirmed() {
    // A margin past the wait below: the buffered vote is still due when R
    // is confirmed.
    let mut c = Cluster::start_with(
        3,
        NodeConfig {
            compaction_margin: Duration::from_secs(2),
            ..config()
        },
        clock,
    );
    c.baseline().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.cut_off(R);
    c.restart(R).await;
    let node = c.nodes[R as usize].clone();
    let bound = c.journals[R as usize].load_clock_bound().await.unwrap();

    let txn = TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    });
    c.send(
        9,
        R,
        Message::PreAccept {
            epoch: 0,
            txn,
            keys: vec![key("acc")],
            events: vec![event("acc", 1)],
        },
    )
    .await;
    // Well past two skews, after which a node taking part raises its bound.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!node.confirmed());
    let vote = next_within(&mut c.coordinator, Duration::from_millis(10)).await;
    assert!(vote.is_none(), "R voted before it was confirmed: {vote:?}");
    let write = node.write(vec![event("acc", 1)]).await;
    assert!(
        write
            .as_ref()
            .is_err_and(|err| err.downcast_ref::<OpenUnconfirmed>().is_some()),
        "R took a write before it was confirmed: {write:?}"
    );
    assert_eq!(
        c.journals[R as usize].load_clock_bound().await.unwrap(),
        bound,
        "R raised its clock bound before it was confirmed"
    );

    c.heal();
    await_until("R is confirmed", Duration::from_secs(5), || {
        node.confirmed()
    })
    .await;
    match next_within(&mut c.coordinator, Duration::from_secs(5)).await {
        Some(Message::PreAcceptOk { txn: t, .. }) if t == txn => {}
        other => panic!("R votes once confirmed: {other:?}"),
    }
}
