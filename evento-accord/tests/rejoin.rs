//! A replica told to rejoin keeps its consensus journal: its host discards
//! only the event store, and the replica rebuilds it from a contact's
//! snapshot. The votes, promises, config-Paxos acceptor state and clock
//! bound it journaled survive the rejoin; what it applied that the contact
//! had not is applied again; and what it holds below the contact's floor is
//! dropped.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::clock::system_micros;
use evento_accord::{
    Ballot, Clock, DataStore, Envelope, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId, RecoveryRound,
    StaticTopology, Status, Timestamp, TxnId,
};
use evento_core::Event;
use tokio::sync::mpsc;

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

/// The replica that rejoins.
const R: usize = 2;
/// The node it rejoins through.
const CONTACT: NodeId = NodeId(0);
/// Every replica's `collect_timeout`.
const COLLECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Three real replicas `{0, 1, 2}` of one shard, each with a journal kept
/// across a rejoin, plus the inboxes of an outside coordinator (node 9) and
/// recoverer (node 8) the test plays.
struct Trio {
    net: Arc<InMemoryNetwork>,
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    journals: Vec<Arc<InMemoryJournal>>,
    loops: Vec<tokio::task::JoinHandle<()>>,
    coordinator: mpsc::Receiver<Envelope>,
    recoverer: mpsc::Receiver<Envelope>,
}

impl Trio {
    fn start() -> Self {
        let net = InMemoryNetwork::new();
        let mut trio = Self {
            coordinator: net.register(NodeId(9)),
            recoverer: net.register(NodeId(8)),
            net,
            nodes: Vec::new(),
            stores: Vec::new(),
            journals: Vec::new(),
            loops: Vec::new(),
        };
        for id in 0..3 {
            let journal = Arc::new(InMemoryJournal::new());
            let store = Arc::new(InMemoryDataStore::new());
            let inbox = trio.net.register(NodeId(id as u64));
            let clock = Arc::new(HybridLogicalClock::new(NodeId(id as u64)));
            let node = trio.build(id, &store, &journal, clock);
            trio.loops.push(node.start(inbox));
            trio.nodes.push(node);
            trio.stores.push(store);
            trio.journals.push(journal);
        }
        trio
    }

    fn build(
        &self,
        id: usize,
        store: &Arc<InMemoryDataStore>,
        journal: &Arc<InMemoryJournal>,
        clock: Arc<HybridLogicalClock>,
    ) -> Node {
        let id = NodeId(id as u64);
        Node::new(
            id,
            Arc::new(StaticTopology::new(id, (0..3).map(NodeId).collect())),
            clock,
            Arc::new(self.net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(store) as Arc<dyn DataStore>,
            Arc::clone(journal) as Arc<dyn Journal>,
        )
        .with_config(NodeConfig {
            collect_timeout: COLLECT_TIMEOUT,
            ..NodeConfig::default()
        })
    }

    /// What a host does once node `R` is told to rejoin: stops it, keeps
    /// its journal, discards its event store, and opens it again by joining
    /// through [`CONTACT`] (`recover_state_for_join`, `begin_join`, `join`),
    /// on `clock`.
    async fn rejoin(&mut self, clock: Arc<HybridLogicalClock>) {
        let node = self.reopen_joining(clock).await;
        node.join(CONTACT).await.expect("rejoin from a snapshot");
    }

    /// [`rejoin`](Self::rejoin) up to the join itself, which the caller
    /// makes: node `R` reopened joining over its kept journal and an empty
    /// event store.
    async fn reopen_joining(&mut self, clock: Arc<HybridLogicalClock>) -> Node {
        self.nodes[R].retire().await;
        self.loops[R].abort();
        let inbox = self.net.register(NodeId(R as u64));
        let store = Arc::new(InMemoryDataStore::new());
        let node = self.build(R, &store, &self.journals[R], clock);
        node.recover_state_for_join()
            .await
            .expect("recover the kept journal");
        node.begin_join();
        self.loops[R] = node.start(inbox);
        self.nodes[R] = node.clone();
        self.stores[R] = store;
        node
    }

    /// What a host that missed the loss of node `R`'s event store does: a
    /// plain reopen ([`Node::recover_state`]) over its kept journal and an
    /// empty store.
    async fn reopen_over_empty_store(&mut self) {
        self.nodes[R].retire().await;
        self.loops[R].abort();
        let inbox = self.net.register(NodeId(R as u64));
        let store = Arc::new(InMemoryDataStore::new());
        let clock = Arc::new(HybridLogicalClock::new(NodeId(R as u64)));
        let node = self.build(R, &store, &self.journals[R], clock);
        node.recover_state()
            .await
            .expect("recover the kept journal");
        self.loops[R] = node.start(inbox);
        self.nodes[R] = node;
        self.stores[R] = store;
    }

    /// [`rejoin`](Self::rejoin) on a wall clock.
    async fn rejoin_now(&mut self) {
        self.rejoin(Arc::new(HybridLogicalClock::new(NodeId(R as u64))))
            .await;
    }

    /// A write every replica applies, so `R`'s journal holds an applied
    /// command its rebuilt store must not be handed back by a replay.
    async fn applied_baseline(&self) {
        self.nodes[0]
            .write(vec![event("base", 1)])
            .await
            .expect("baseline write");
        for id in 0..3 {
            await_event(&self.stores[id], "base", 1).await;
        }
    }

    async fn send(&self, from: u64, message: Message) {
        self.net
            .sink(NodeId(from))
            .send(NodeId(R as u64), message)
            .await
            .unwrap();
    }

    fn cut_off(&self, id: usize) {
        for peer in (0..3).filter(|&p| p != id) {
            self.net.partition(NodeId(id as u64), NodeId(peer as u64));
        }
    }
}

/// A transaction of the outside coordinator, minted now, on key `acc`.
fn outside_txn() -> TxnId {
    TxnId(Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(9),
    })
}

/// A ballot above `txn`'s, owned by the recoverer.
fn recovery_ballot(txn: TxnId) -> Ballot {
    Ballot(Timestamp {
        micros: txn.0.micros + 1_000,
        logical: 0,
        node: NodeId(8),
    })
}

/// The key a write of aggregate `acc` conflicts on.
fn acc_key() -> Key {
    Key::of(&event("acc", 1))
}

fn preaccept(txn: TxnId) -> Message {
    Message::PreAccept {
        epoch: 0,
        txn,
        keys: vec![acc_key()],
        events: vec![event("acc", 1)],
    }
}

async fn next(inbox: &mut mpsc::Receiver<Envelope>) -> Message {
    tokio::time::timeout(Duration::from_secs(5), inbox.recv())
        .await
        .expect("an answer")
        .expect("inbox open")
        .message
}

fn holds(store: &InMemoryDataStore, aggregate_id: &str, version: u16) -> bool {
    store
        .committed_events()
        .iter()
        .any(|((_, _, id, v), _)| id == aggregate_id && *v == version)
}

async fn await_event(store: &InMemoryDataStore, aggregate_id: &str, version: u16) {
    for _ in 0..400 {
        if holds(store, aggregate_id, version) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the store never held {aggregate_id} v{version}");
}

/// A recovery reached `R` before the original coordinator's PreAccept, and
/// `R` promised its ballot without ever witnessing the transaction. After a
/// rejoin the coordinator, merely slow, must still win no vote there: the
/// recovery may already have decided without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_replica_keeps_its_recovery_promise() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    let txn = outside_txn();
    let b = recovery_ballot(txn);
    t.send(8, Message::Recover { txn, ballot: b }).await;
    assert!(matches!(
        next(&mut t.recoverer).await,
        Message::RecoverOk { known: false, .. }
    ));

    t.rejoin_now().await;

    t.send(9, preaccept(txn)).await;
    match next(&mut t.coordinator).await {
        Message::Nack { promised, .. } => assert_eq!(promised, b),
        other => panic!("a late PreAccept below the promise must be refused: {other:?}"),
    }
    assert_eq!(
        t.nodes[R].command_status(txn),
        None,
        "and it records nothing"
    );
}

/// `R` voted on a transaction only it heard of, then missed a conflicting
/// write. After a rejoin a recovery must receive the vote `R` cast — `t0`,
/// no dependencies — not a fresh one taken over the state it now holds,
/// which would order the transaction after that write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_replica_keeps_its_preaccept_vote() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    let txn = outside_txn();
    t.send(9, preaccept(txn)).await;
    match next(&mut t.coordinator).await {
        Message::PreAcceptOk {
            execute_at, deps, ..
        } => {
            assert_eq!(execute_at, txn.0);
            assert!(deps.is_empty());
        }
        other => panic!("R votes: {other:?}"),
    }

    // A conflicting write commits on the other two while R is cut off.
    t.cut_off(R);
    t.nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("a quorum without R");
    t.net.heal_all_partitions();

    t.rejoin_now().await;

    let b = recovery_ballot(txn);
    t.send(
        8,
        Message::BeginRecovery {
            epoch: 0,
            txn,
            ballot: b,
            keys: vec![acc_key()],
            events: vec![event("acc", 1)],
            round: RecoveryRound::Ask,
        },
    )
    .await;
    match next(&mut t.recoverer).await {
        Message::BeginRecoveryOk {
            known: true,
            status,
            execute_at,
            deps,
            ..
        } => {
            assert_eq!(status, Status::PreAccepted);
            assert_eq!(execute_at, txn.0, "the vote R cast, at t0");
            assert!(deps.is_empty(), "with no dependencies: {deps:?}");
        }
        other => panic!("R reports its vote: {other:?}"),
    }
}

/// `R` promised a config-Paxos ballot. After a rejoin a lower ballot's
/// Accept must be refused, or two layouts could be decided for one epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_acceptor_keeps_its_config_promise() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    let ballot = |micros: u64| {
        Ballot(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        })
    };
    let high = ballot(system_micros());
    t.send(
        9,
        Message::ConfigPrepare {
            epoch: 1,
            ballot: high,
        },
    )
    .await;
    assert!(matches!(
        next(&mut t.coordinator).await,
        Message::ConfigPromise { .. }
    ));

    t.rejoin_now().await;

    t.send(
        9,
        Message::ConfigAccept {
            epoch: 1,
            ballot: ballot(1),
            entry: evento_accord::ConfigEntry::active(vec![(0..3).map(NodeId).collect()]),
        },
    )
    .await;
    match next(&mut t.coordinator).await {
        Message::ConfigNack { promised, .. } => assert_eq!(promised, high),
        other => panic!("a ballot below the promise must be refused: {other:?}"),
    }
}

/// `R` comes back on a host whose wall clock runs behind: the first id it
/// mints after the rejoin must still be above every one it minted before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_node_mints_above_its_clock_bound() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    t.nodes[R]
        .write(vec![event("mine", 1)])
        .await
        .expect("R coordinates a write");
    let bound = t.journals[R]
        .load_clock_bound()
        .await
        .unwrap()
        .expect("R persisted its clock bound");

    let behind = Arc::new(HybridLogicalClock::with_physical(NodeId(R as u64), || {
        system_micros() - 5_000_000
    }));
    t.rejoin(behind).await;

    let minted = t.nodes[R]
        .write(vec![event("mine", 2)])
        .await
        .expect("R coordinates again")
        .txn;
    assert!(
        minted.0.micros >= bound,
        "minted {minted:?} below the bound {bound}"
    );
}

/// The contact missed a write `R` applied. The snapshot lacks it, so `R`
/// applies it again from its journal, and holds what the replica that also
/// applied it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_replica_reapplies_what_its_contact_had_not() {
    let mut t = Trio::start();
    t.cut_off(0);
    let txn = t.nodes[1]
        .write(vec![event("acc", 1)])
        .await
        .expect("a quorum without the contact")
        .txn;
    await_event(&t.stores[R], "acc", 1).await;
    t.net.heal_all_partitions();

    t.rejoin_now().await;

    assert!(!holds(&t.stores[0], "acc", 1), "the contact still lacks it");
    await_event(&t.stores[R], "acc", 1).await;
    assert_eq!(t.nodes[R].command_status(txn), Some(Status::Applied));
    assert_eq!(
        t.stores[R].committed_events().len(),
        t.stores[1].committed_events().len()
    );
}

/// `R` holds a vote on a transaction only it heard of; the others compacted
/// past it meanwhile. The rejoined replica adopts the contact's floor and
/// drops the vote: the shard's watermark passed it, so it can no longer
/// commit, and keeping it would hold `R`'s own floor back for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoined_replica_drops_what_its_contact_compacted() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    let txn = outside_txn();
    t.send(9, preaccept(txn)).await;
    assert!(matches!(
        next(&mut t.coordinator).await,
        Message::PreAcceptOk { .. }
    ));
    let past = HybridLogicalClock::new(NodeId(9)).now();
    for id in [0, 1] {
        t.nodes[id].compact(past).await;
    }

    t.rejoin_now().await;

    assert_eq!(
        t.nodes[R].command_status(txn),
        None,
        "dropped below the floor"
    );
    assert!(
        !t.nodes[R].sweep_pressure().busy,
        "nothing un-applied is left"
    );
    assert!(holds(&t.stores[R], "base", 1), "the snapshot is installed");
}

/// A replica that lost its event store after its peers compacted cannot catch
/// up through anti-entropy — the prefix is no longer held as commands
/// anywhere — so a plain reopen over an empty store recovers nothing. Joining
/// installs a contact's snapshot and floor, the floor is persisted for the
/// next restart, and writes that depend on compacted transactions land on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_with_a_lost_store_rejoins_after_compaction() {
    let mut t = Trio::start();
    for i in 0..3 {
        t.nodes[0]
            .write(vec![event(&format!("acc-{i}"), 1)])
            .await
            .expect("write");
    }
    for id in 0..3 {
        for i in 0..3 {
            await_event(&t.stores[id], &format!("acc-{i}"), 1).await;
        }
    }
    // Not beyond the wall clock: a node mints above its floor, and peers
    // refuse a `t0` past their clock's skew bound.
    let floor = Timestamp {
        micros: system_micros(),
        logical: 0,
        node: NodeId(0),
    };
    for node in &t.nodes {
        node.compact(floor).await;
        assert_eq!(node.command_count(), 0, "every command compacted away");
    }

    t.reopen_over_empty_store().await;
    for _ in 0..3 {
        for node in &t.nodes {
            node.sweep().await;
        }
    }
    assert!(
        t.stores[R].committed_events().is_empty(),
        "anti-entropy shipped a compacted command"
    );

    t.rejoin_now().await;
    let committed = |store: &InMemoryDataStore| {
        let mut events = store.committed_events();
        events.sort();
        events
    };
    assert_eq!(committed(&t.stores[R]), committed(&t.stores[0]));
    let adopted = t.journals[R]
        .load_watermark()
        .await
        .unwrap()
        .expect("the adopted floor is persisted");
    assert!(adopted > Timestamp::MIN);

    // A write that depends on a compacted transaction lands on the joiner,
    // and the joiner can coordinate one too.
    t.nodes[0]
        .write(vec![event("acc-0", 2)])
        .await
        .expect("a write after the join");
    t.nodes[R]
        .write(vec![event("acc-1", 2)])
        .await
        .expect("a write the joiner coordinates");
    for id in 0..3 {
        await_event(&t.stores[id], "acc-0", 2).await;
        await_event(&t.stores[id], "acc-1", 2).await;
    }
}

/// `R` had stopped waiting for the contact and compacted past it, then was
/// told to rejoin before it heard the contact again: its kept journal still
/// holds the contact absent, with that bound. The contact has caught up
/// since. The join asks it to prove it holds what `R` compacted, and goes
/// through it at once. Before, the join header's answer was taken for a
/// message from a peer still behind: answered with `Rejoin` and dropped, the
/// attempt timed out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoin_through_a_contact_it_gave_up_on_asks_it_for_proof_first() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    // Anti-entropy rounds, one interval apart (a sweep runs one at most per
    // interval): the contact's report is past its sync coverage once its
    // stable point, clamped the same way, moves.
    for _ in 0..20 {
        tokio::time::sleep(NodeConfig::default().anti_entropy_interval).await;
        for node in &t.nodes {
            node.sweep().await;
        }
        if t.nodes[CONTACT.0 as usize].stable_micros() > 0 {
            break;
        }
    }
    let compacted = Timestamp {
        micros: 1,
        logical: 0,
        node: NodeId(1),
    };
    t.journals[R]
        .save_rejoin_bounds(&[(CONTACT, compacted)])
        .await
        .unwrap();

    let node = t
        .reopen_joining(Arc::new(HybridLogicalClock::new(NodeId(R as u64))))
        .await;
    assert_eq!(node.absent_peers(), [CONTACT], "the kept journal's bound");
    let started = std::time::Instant::now();
    node.join(CONTACT)
        .await
        .expect("join through the contact it gave up on");
    assert!(
        started.elapsed() < COLLECT_TIMEOUT,
        "the join waited out a timeout: {:?}",
        started.elapsed()
    );
    assert!(
        node.absent_peers().is_empty(),
        "the contact is counted again"
    );
    assert!(holds(&t.stores[R], "base", 1), "the snapshot is installed");
}

/// The same, but the contact cannot prove it: it reports nothing above what
/// `R` compacted past it, so it may lack a write `R` dropped. `R`, which has
/// discarded its store, must not copy the contact's — the write would be
/// gone for good, held as a command nowhere — and joins through a replica it
/// never gave up on instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoin_never_copies_a_contact_that_cannot_prove_what_was_compacted() {
    let mut t = Trio::start();
    t.applied_baseline().await;
    let unprovable = Timestamp {
        micros: system_micros() + 3_600_000_000,
        logical: 0,
        node: NodeId(1),
    };
    t.journals[R]
        .save_rejoin_bounds(&[(CONTACT, unprovable)])
        .await
        .unwrap();

    let node = t
        .reopen_joining(Arc::new(HybridLogicalClock::new(NodeId(R as u64))))
        .await;
    let err = node
        .join(CONTACT)
        .await
        .expect_err("a contact that cannot prove what was compacted past it");
    assert!(
        t.stores[R].committed_events().is_empty(),
        "nothing of the contact's is installed: {err:#}"
    );
    node.join(NodeId(1))
        .await
        .expect("join through a replica it never gave up on");
    assert!(holds(&t.stores[R], "base", 1), "the snapshot is installed");
}
