//! Recovery against scripted peers: a real node takes a transaction over while
//! the other replicas are played by the test, so each one's answers (and what
//! the recovery coordinator sends them) are exact.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    Ballot, Clock, DataStore, Envelope, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, InMemorySink, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId,
    RecoveryRound, ShardedTopology, StaticTopology, Status, Timestamp, Topology, TxnId,
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

/// What a scripted replica answers a recovery with (it plays a replica that
/// ignores the keys a `BeginRecovery` carries).
#[derive(Clone)]
struct Report {
    known: bool,
    status: Status,
    accepted: Ballot,
    execute_at: Timestamp,
    /// Delay before answering, to order the reports the coordinator sees.
    delay: Duration,
}

/// A replica played by the test: answers recovery with `report`, accepts and
/// reads whatever it is asked to (if `cooperate`), and logs every message.
struct Scripted {
    received: Arc<Mutex<Vec<Message>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Scripted {
    fn spawn(
        mut inbox: mpsc::Receiver<Envelope>,
        sink: InMemorySink,
        report: Report,
        keys: Vec<Key>,
        events: Vec<Event>,
        cooperate: bool,
    ) -> Self {
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&received);
        let task = tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                log.lock().unwrap().push(env.message.clone());
                let reply = match env.message {
                    Message::BeginRecovery { txn, ballot, .. } => {
                        tokio::time::sleep(report.delay).await;
                        Some(Message::BeginRecoveryOk {
                            epoch: 0,
                            txn,
                            ballot,
                            known: report.known,
                            status: report.status,
                            accepted: report.accepted,
                            execute_at: report.execute_at,
                            deps: Vec::new(),
                            superseding_rejects: false,
                            wait: false,
                            keys: if report.known {
                                keys.clone()
                            } else {
                                Vec::new()
                            },
                            events: if report.known {
                                events.clone()
                            } else {
                                Vec::new()
                            },
                        })
                    }
                    Message::Accept { txn, ballot, .. } if cooperate => Some(Message::AcceptOk {
                        txn,
                        ballot,
                        deps: Vec::new(),
                    }),
                    Message::Commit { txn, .. } if cooperate => {
                        Some(Message::ReadOk { txn, ok: true })
                    }
                    Message::Apply { txn, .. } if cooperate => Some(Message::Applied { txn }),
                    _ => None,
                };
                if let Some(reply) = reply {
                    let _ = sink.send(env.from, reply).await;
                }
            }
        });
        Self {
            received,
            _task: task,
        }
    }

    fn received(&self) -> Vec<Message> {
        self.received.lock().unwrap().clone()
    }
}

/// A real node 0 in a 3-replica group `{0, 1, 2}` whose peers are scripted,
/// knowing a transaction pre-accepted by an outside coordinator (node 9).
struct Setup {
    node: Node,
    store: Arc<InMemoryDataStore>,
    net: Arc<InMemoryNetwork>,
    txn: TxnId,
    keys: Vec<Key>,
    events: Vec<Event>,
    _loop: tokio::task::JoinHandle<()>,
}

async fn setup() -> Setup {
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let inbox = net.register(NodeId(0));
    let store = Arc::new(InMemoryDataStore::new());
    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(NodeConfig {
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    });
    let task = node.start(inbox);

    // The outside coordinator pre-accepts T at node 0 only, then goes quiet.
    let mut coordinator = net.register(NodeId(9));
    let micros = HybridLogicalClock::new(NodeId(9)).now().micros;
    let txn = TxnId(Timestamp {
        micros,
        logical: 0,
        node: NodeId(9),
    });
    let keys = vec![Key("acc".into())];
    let events = vec![event("acc", 1)];
    net.sink(NodeId(9))
        .send(
            NodeId(0),
            Message::PreAccept {
                epoch: 0,
                txn,
                keys: keys.clone(),
                events: events.clone(),
            },
        )
        .await
        .unwrap();
    let ack = tokio::time::timeout(Duration::from_secs(5), coordinator.recv())
        .await
        .expect("preaccept answered")
        .expect("inbox open");
    assert!(matches!(ack.message, Message::PreAcceptOk { .. }));

    Setup {
        node,
        store,
        net,
        txn,
        keys,
        events,
        _loop: task,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_never_commits_a_value_no_quorum_accepted() {
    let s = setup().await;
    // Both peers answer the recovery (so its quorum is met) but never accept.
    let silent = |id: u64| {
        Scripted::spawn(
            s.net.register(NodeId(id)),
            s.net.sink(NodeId(id)),
            Report {
                known: false,
                status: Status::PreAccepted,
                accepted: Ballot(s.txn.0),
                execute_at: s.txn.0,
                delay: Duration::ZERO,
            },
            s.keys.clone(),
            s.events.clone(),
            false,
        )
    };
    let peers = [silent(1), silent(2)];

    let result = tokio::time::timeout(Duration::from_secs(10), s.node.recover(s.txn))
        .await
        .expect("recovery gives up in bounded time");
    assert!(result.is_err(), "one acceptance is not a quorum");
    for peer in &peers {
        assert!(
            !peer
                .received()
                .iter()
                .any(|m| matches!(m, Message::Commit { .. })),
            "a value only this node accepted must never be committed"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_adopts_the_accepted_value_not_a_preaccept_vote() {
    let s = setup().await;
    // Peer 1 accepted T at the original coordinator's ballot, raised to `at`;
    // peer 2 only pre-accepted it. A pre-accepted report carries `Ballot(t0)`
    // too, which must not be mistaken for an accepted value. Peer 2 answers
    // last, so it would be the one a tie picks.
    let at = Timestamp {
        micros: s.txn.0.micros + 7,
        logical: 0,
        node: NodeId(9),
    };
    let peer = |id: u64, status, execute_at, delay| {
        Scripted::spawn(
            s.net.register(NodeId(id)),
            s.net.sink(NodeId(id)),
            Report {
                known: true,
                status,
                accepted: Ballot(s.txn.0),
                execute_at,
                delay,
            },
            s.keys.clone(),
            s.events.clone(),
            true,
        )
    };
    let accepted = peer(1, Status::Accepted, at, Duration::ZERO);
    let _preaccepted = peer(2, Status::PreAccepted, s.txn.0, Duration::from_millis(20));

    let outcome = tokio::time::timeout(Duration::from_secs(10), s.node.recover(s.txn))
        .await
        .expect("recovery completes")
        .expect("a quorum cooperates");
    assert!(!outcome.conflict);

    let proposed: Vec<Timestamp> = accepted
        .received()
        .iter()
        .filter_map(|m| match m {
            Message::Accept { execute_at, .. } => Some(*execute_at),
            _ => None,
        })
        .collect();
    assert_eq!(
        proposed,
        vec![at],
        "recovery must re-propose the accepted value"
    );
    let applied = s.store.applied_log();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].execute_at, at);
}

/// A real node 0 in a group of `n` replicas — the whole group is the
/// fast-path electorate — whose other replicas are scripted, and two
/// transactions on one key that node 0 pre-accepted in this order: `later`,
/// then `earlier`, whose `t0` is lower. Having seen `later` first, node 0
/// voted above `earlier`'s `t0`: it is in no fast quorum that could have
/// committed `earlier` at `t0`.
struct Votes {
    node: Node,
    net: Arc<InMemoryNetwork>,
    earlier: TxnId,
    later: TxnId,
    keys: Vec<Key>,
    events: Vec<Event>,
    _loop: tokio::task::JoinHandle<()>,
}

async fn votes(n: u64) -> Votes {
    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let inbox = net.register(NodeId(0));
    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(NodeConfig {
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    });
    let task = node.start(inbox);

    let micros = HybridLogicalClock::new(NodeId(9)).now().micros;
    let earlier = TxnId(Timestamp {
        micros,
        logical: 0,
        node: NodeId(7),
    });
    let later = TxnId(Timestamp {
        micros: micros + 1_000,
        logical: 0,
        node: NodeId(9),
    });
    let keys = vec![Key("acc".into())];
    let events = vec![event("acc", 1)];
    // Each outside coordinator pre-accepts its transaction at node 0 and
    // goes quiet: `later`'s (node 9) first, then `earlier`'s (node 7).
    for (coordinator, txn, events) in [
        (9, later, vec![event("acc", 1)]),
        (7, earlier, events.clone()),
    ] {
        let mut inbox = net.register(NodeId(coordinator));
        net.sink(NodeId(coordinator))
            .send(
                NodeId(0),
                Message::PreAccept {
                    epoch: 0,
                    txn,
                    keys: keys.clone(),
                    events,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            next(&mut inbox).await,
            Message::PreAcceptOk { .. }
        ));
    }

    Votes {
        node,
        net,
        earlier,
        later,
        keys,
        events,
        _loop: task,
    }
}

impl Votes {
    /// Scripts replica `id` to report `earlier` pre-accepted, with `vote` as
    /// its execution timestamp, and to cooperate with what follows.
    fn peer(&self, id: u64, vote: Timestamp) -> Scripted {
        Scripted::spawn(
            self.net.register(NodeId(id)),
            self.net.sink(NodeId(id)),
            Report {
                known: true,
                status: Status::PreAccepted,
                accepted: Ballot(self.earlier.0),
                execute_at: vote,
                delay: Duration::ZERO,
            },
            self.keys.clone(),
            self.events.clone(),
            true,
        )
    }

    /// A vote above `later`'s `t0`, as a replica that saw `later` first casts.
    fn raised(&self) -> Timestamp {
        Timestamp {
            micros: self.later.0.micros,
            logical: 0,
            node: NodeId(11),
        }
    }

    /// Recovers `earlier` from node 0 and returns the execution timestamp its
    /// Accept proposed, as `peer` received it.
    async fn proposed(&self, peer: &Scripted) -> Timestamp {
        tokio::time::timeout(Duration::from_secs(10), self.node.recover(self.earlier))
            .await
            .expect("recovery completes")
            .expect("a quorum cooperates");
        let proposed: Vec<Timestamp> = peer
            .received()
            .iter()
            .filter_map(|m| match m {
                Message::Accept { execute_at, .. } => Some(*execute_at),
                _ => None,
            })
            .collect();
        assert_eq!(proposed.len(), 1, "recovery proposes once: {proposed:?}");
        proposed[0]
    }
}

/// Recovery's fast-path check must count the replicas' own PreAccept votes,
/// not only the conflicts they already accepted or committed. Of three
/// replicas a fast quorum is all three, so node 0's vote above `t0` proves
/// `earlier` never committed on the fast path — although both other replicas
/// voted `t0`, and node 0 holds `later` only pre-accepted. `later` may have
/// fast-committed elsewhere without `earlier` among its dependencies:
/// re-proposed at `t0`, `earlier` would order before a write that never
/// waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_counts_preaccept_votes_against_the_fast_path() {
    let v = votes(3).await;
    let peers: Vec<Scripted> = (1..3).map(|id| v.peer(id, v.earlier.0)).collect();

    let proposed = v.proposed(&peers[0]).await;
    assert!(
        proposed > v.later.0,
        "a vote above t0 from the electorate rules the fast path out: proposed {proposed:?}, \
         t0 {:?}, the later conflict {:?}",
        v.earlier.0,
        v.later.0
    );
}

/// The other side of the threshold: of five replicas a fast quorum is four,
/// so the one vote above `t0` (node 0's) leaves the fast path possible — the
/// other four could have formed it — and recovery must keep `t0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_vote_above_t0_of_five_leaves_the_fast_path_possible() {
    let v = votes(5).await;
    let peers: Vec<Scripted> = (1..5).map(|id| v.peer(id, v.earlier.0)).collect();

    assert_eq!(
        v.proposed(&peers[0]).await,
        v.earlier.0,
        "four votes at t0 may have fast-committed it"
    );
}

/// Two votes above `t0` of five leave at most three at `t0`, short of the
/// fast quorum of four: the fast path is ruled out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_votes_above_t0_of_five_rule_the_fast_path_out() {
    let v = votes(5).await;
    let raised = v.raised();
    let peers: Vec<Scripted> = (1..5)
        .map(|id| v.peer(id, if id == 1 { raised } else { v.earlier.0 }))
        .collect();

    let proposed = v.proposed(&peers[0]).await;
    assert!(
        proposed > v.later.0,
        "no fast quorum is left at t0: proposed {proposed:?}, t0 {:?}",
        v.earlier.0
    );
}

/// A real replica (node 0 of `{0, 1, 2}`) with a kept journal, plus the inboxes
/// of an outside coordinator (node 9) and recoverer (node 8) the test plays.
struct Fence {
    net: Arc<InMemoryNetwork>,
    journal: Arc<InMemoryJournal>,
    node: Node,
    task: tokio::task::JoinHandle<()>,
    coordinator: mpsc::Receiver<Envelope>,
    recoverer: mpsc::Receiver<Envelope>,
    txn: TxnId,
    keys: Vec<Key>,
    events: Vec<Event>,
}

fn replica(net: &Arc<InMemoryNetwork>, journal: &Arc<InMemoryJournal>) -> Node {
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::clone(journal) as Arc<dyn Journal>,
    )
}

fn fence() -> Fence {
    let net = InMemoryNetwork::new();
    let journal = Arc::new(InMemoryJournal::new());
    let inbox = net.register(NodeId(0));
    let node = replica(&net, &journal);
    let task = node.start(inbox);
    let micros = HybridLogicalClock::new(NodeId(9)).now().micros;
    Fence {
        coordinator: net.register(NodeId(9)),
        recoverer: net.register(NodeId(8)),
        net,
        journal,
        node,
        task,
        txn: TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(9),
        }),
        keys: vec![Key("acc".into())],
        events: vec![event("acc", 1)],
    }
}

impl Fence {
    /// A ballot above the transaction's, owned by the recoverer.
    fn ballot(&self) -> Ballot {
        Ballot(Timestamp {
            micros: self.txn.0.micros + 1_000,
            logical: 0,
            node: NodeId(8),
        })
    }

    async fn send(&self, from: u64, message: Message) {
        self.net
            .sink(NodeId(from))
            .send(NodeId(0), message)
            .await
            .unwrap();
    }

    fn preaccept(&self) -> Message {
        Message::PreAccept {
            epoch: 0,
            txn: self.txn,
            keys: self.keys.clone(),
            events: self.events.clone(),
        }
    }

    fn accept(&self) -> Message {
        Message::Accept {
            epoch: 0,
            txn: self.txn,
            ballot: Ballot(self.txn.0),
            execute_at: self.txn.0,
            deps: Vec::new(),
            keys: self.keys.clone(),
        }
    }
}

async fn next(inbox: &mut mpsc::Receiver<Envelope>) -> Message {
    tokio::time::timeout(Duration::from_secs(5), inbox.recv())
        .await
        .expect("an answer")
        .expect("inbox open")
        .message
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_that_answered_a_recovery_blind_refuses_the_original_coordinator() {
    let mut f = fence();
    // The recovery reaches node 0 before the original coordinator's PreAccept.
    let b = f.ballot();
    f.send(
        8,
        Message::Recover {
            txn: f.txn,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(
        next(&mut f.recoverer).await,
        Message::RecoverOk { known: false, .. }
    ));

    // The coordinator, merely slow, must win neither a vote nor an Accept
    // here: the recovery decided without them.
    f.send(9, f.preaccept()).await;
    assert!(
        matches!(next(&mut f.coordinator).await, Message::Nack { promised, .. } if promised == b),
        "a late PreAccept below the recovery's promise is refused"
    );
    f.send(9, f.accept()).await;
    assert!(
        matches!(next(&mut f.coordinator).await, Message::Nack { promised, .. } if promised == b),
        "and so is a late Accept"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_promise_survives_a_restart() {
    let mut f = fence();
    f.send(9, f.preaccept()).await;
    assert!(matches!(
        next(&mut f.coordinator).await,
        Message::PreAcceptOk { .. }
    ));
    let b = f.ballot();
    f.send(
        8,
        Message::Recover {
            txn: f.txn,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(
        next(&mut f.recoverer).await,
        Message::RecoverOk { known: true, .. }
    ));

    // Restart node 0 from its journal.
    f.task.abort();
    let inbox = f.net.register(NodeId(0));
    f.node = replica(&f.net, &f.journal);
    f.node.recover_state().await.unwrap();
    f.task = f.node.start(inbox);

    f.send(9, f.accept()).await;
    assert!(
        matches!(next(&mut f.coordinator).await, Message::Nack { promised, .. } if promised == b),
        "the promise was durable before the reply revealed it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_preaccepts_at_replicas_that_never_saw_the_transaction() {
    // Three real nodes; the outside coordinator's PreAccept reached node 0
    // only. Recovery hands the transaction to the other two, which vote on it
    // under the recovery's ballot — so the coordinator's late PreAccept to
    // them is refused.
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    let mut loops = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())),
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );
        loops.push(node.start(inbox));
        nodes.push(node);
        stores.push(store);
    }
    let mut coordinator = net.register(NodeId(9));
    let micros = HybridLogicalClock::new(NodeId(9)).now().micros;
    let txn = TxnId(Timestamp {
        micros,
        logical: 0,
        node: NodeId(9),
    });
    let preaccept = Message::PreAccept {
        epoch: 0,
        txn,
        keys: vec![Key("acc".into())],
        events: vec![event("acc", 1)],
    };
    net.sink(NodeId(9))
        .send(NodeId(0), preaccept.clone())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut coordinator).await,
        Message::PreAcceptOk { .. }
    ));

    let outcome = tokio::time::timeout(Duration::from_secs(5), nodes[0].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovered");
    assert!(!outcome.conflict);
    for store in &stores {
        for _ in 0..1000 {
            if !store.applied_log().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(store.applied_log().len(), 1, "every replica applied it");
    }

    net.sink(NodeId(9))
        .send(NodeId(1), preaccept)
        .await
        .unwrap();
    assert!(
        matches!(next(&mut coordinator).await, Message::Nack { .. }),
        "the original coordinator can no longer gather a vote at node 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicated_vote_never_completes_a_preaccept_quorum() {
    // Five replicas, node 0 real; peer 1's vote is delivered twice — once
    // inside the fast-path window, once after it — and nobody else votes.
    // Two distinct voters are not the slow quorum of three, so the write must
    // never reach Accept.
    let net = InMemoryNetwork::new();
    let ids: Vec<NodeId> = (0..5).map(NodeId).collect();
    let inbox = net.register(NodeId(0));
    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(NodeConfig {
        collect_timeout: Duration::from_millis(300),
        ..NodeConfig::default()
    });
    let _loop = node.start(inbox);

    let accepts = Arc::new(Mutex::new(0usize));
    let mut peers = Vec::new();
    for id in 1..5u64 {
        let mut inbox = net.register(NodeId(id));
        let sink = net.sink(NodeId(id));
        let accepts = Arc::clone(&accepts);
        peers.push(tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                match env.message {
                    Message::PreAccept { txn, .. } if id == 1 => {
                        let vote = Message::PreAcceptOk {
                            txn,
                            execute_at: txn.0,
                            deps: Vec::new(),
                        };
                        let _ = sink.send(env.from, vote.clone()).await;
                        tokio::time::sleep(Duration::from_millis(80)).await;
                        let _ = sink.send(env.from, vote).await;
                    }
                    Message::Accept { txn, ballot, .. } => {
                        *accepts.lock().unwrap() += 1;
                        let _ = sink
                            .send(
                                env.from,
                                Message::AcceptOk {
                                    txn,
                                    ballot,
                                    deps: Vec::new(),
                                },
                            )
                            .await;
                    }
                    _ => {}
                }
            }
        }));
    }

    let result = tokio::time::timeout(Duration::from_secs(10), node.write(vec![event("acc", 1)]))
        .await
        .expect("the write gives up in bounded time");
    assert!(
        result.is_err(),
        "two distinct voters are not a quorum of five"
    );
    assert_eq!(
        *accepts.lock().unwrap(),
        0,
        "the write must never reach Accept on a sub-quorum of votes"
    );
}

/// Two disjoint shards of three real nodes — `A = {0, 1, 2}` and
/// `B = {3, 4, 5}` — each node with its own kept journal so it can be
/// restarted from it.
struct Sharded {
    net: Arc<InMemoryNetwork>,
    layout: Vec<Vec<NodeId>>,
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    journals: Vec<Arc<InMemoryJournal>>,
    loops: Vec<tokio::task::JoinHandle<()>>,
}

impl Sharded {
    fn start() -> Self {
        let mut cluster = Self {
            net: InMemoryNetwork::new(),
            layout: vec![(0..3).map(NodeId).collect(), (3..6).map(NodeId).collect()],
            nodes: Vec::new(),
            stores: Vec::new(),
            journals: Vec::new(),
            loops: Vec::new(),
        };
        for id in 0..6 {
            let journal = Arc::new(InMemoryJournal::new());
            let store = Arc::new(InMemoryDataStore::new());
            let inbox = cluster.net.register(NodeId(id as u64));
            let node = cluster.build(id, &store, &journal);
            cluster.loops.push(node.start(inbox));
            cluster.nodes.push(node);
            cluster.stores.push(store);
            cluster.journals.push(journal);
        }
        cluster
    }

    fn build(
        &self,
        id: usize,
        store: &Arc<InMemoryDataStore>,
        journal: &Arc<InMemoryJournal>,
    ) -> Node {
        let id = NodeId(id as u64);
        Node::new(
            id,
            Arc::new(ShardedTopology::new(id, self.layout.clone())),
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(self.net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(store) as Arc<dyn DataStore>,
            Arc::clone(journal) as Arc<dyn Journal>,
        )
        .with_config(NodeConfig {
            collect_timeout: Duration::from_secs(1),
            ..NodeConfig::default()
        })
    }

    /// Restarts node `id` from its journal, over a fresh data store.
    async fn restart(&mut self, id: usize) {
        self.loops[id].abort();
        let inbox = self.net.register(NodeId(id as u64));
        let store = Arc::new(InMemoryDataStore::new());
        let node = self.build(id, &store, &self.journals[id]);
        node.recover_state().await.unwrap();
        self.loops[id] = node.start(inbox);
        self.nodes[id] = node;
        self.stores[id] = store;
    }

    /// An aggregate id that hashes into `shard`.
    fn id_in_shard(&self, shard: usize, prefix: &str) -> String {
        let probe = ShardedTopology::new(NodeId(0), self.layout.clone());
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|id| probe.shard_of(&Key(id.clone())) == shard)
            .expect("an id in the shard")
    }

    /// The one transaction node `id` has journaled.
    async fn journaled_txn(&self, id: usize) -> TxnId {
        let commands = self.journals[id].load_all().await.unwrap();
        assert_eq!(commands.len(), 1, "node {id} holds exactly one transaction");
        commands[0].txn
    }

    /// Whether node `id` applied `txn` as a commit.
    fn committed(&self, id: usize, txn: TxnId) -> bool {
        self.stores[id]
            .applied_log()
            .iter()
            .any(|e| e.txn == txn && !e.conflict)
    }

    /// Waits until every node in `ids` holds `txn` applied.
    async fn await_applied(&self, ids: impl IntoIterator<Item = usize>, txn: TxnId) {
        for id in ids {
            for _ in 0..400 {
                if self.nodes[id].command_status(txn) == Some(Status::Applied) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                self.nodes[id].command_status(txn),
                Some(Status::Applied),
                "node {id} must settle the transaction"
            );
        }
    }
}

/// The coordinator (node 0) pre-accepts a transaction spanning both shards at
/// shard A only — its frames to shard B are lost — and dies. Shard B never
/// witnessed it, so it never committed: recovery must abort it on both
/// shards, not commit shard A's half alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovery_aborts_a_transaction_a_shard_never_witnessed() {
    let c = Sharded::start();
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    for peer in &c.layout[1] {
        c.net.partition(NodeId(0), *peer);
    }
    let preaccepted = c.nodes[0]
        .coordinate_preaccept(vec![event(&a, 1), event(&b, 1)])
        .await;
    assert!(preaccepted.is_err(), "shard B never answers");
    let txn = c.journaled_txn(1).await;
    c.net.crash(NodeId(0));

    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(
        outcome.conflict,
        "a transaction shard B never saw is aborted"
    );

    c.await_applied([1, 2], txn).await;
    for id in 1..6 {
        assert!(!c.committed(id, txn), "node {id} must not apply its events");
    }
    for id in 3..6 {
        assert_eq!(
            c.nodes[id].command_status(txn),
            None,
            "shard B never holds the transaction"
        );
    }
}

/// Shard B answers slower than the fast-path window and its version
/// condition fails. A recovery must wait for its report: deciding on shard A
/// alone would commit A's half while B aborts its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovery_waits_for_a_slow_shard_before_deciding() {
    let c = Sharded::start();
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    // `b` already exists, so the transaction's `b` v1 fails its condition.
    c.nodes[3].write(vec![event(&b, 1)]).await.unwrap();
    for id in 3..6 {
        for _ in 0..400 {
            if c.stores[id].applied_log().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // Each cross-shard link takes 150 ms one way: three times the fast-path
    // window, well inside the slow-path budget.
    for x in &c.layout[0] {
        for y in &c.layout[1] {
            c.net.set_link_latency(*x, *y, Duration::from_millis(150));
        }
    }
    // The coordinator commits and dies before collecting the reads.
    let txn = c.nodes[0]
        .coordinate_through_commit(vec![event(&a, 1), event(&b, 1)])
        .await
        .expect("both shards commit it");

    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(
        outcome.conflict,
        "shard B's failed condition aborts it everywhere"
    );

    c.await_applied(0..6, txn).await;
    for id in 0..6 {
        assert!(!c.committed(id, txn), "node {id} must not apply its events");
    }
}

/// A shard-B replica that answered a recovery "never witnessed" keeps that
/// promise across a restart: the original coordinator's PreAccept, merely
/// slow, must still be refused there — or the recovery's quorum and the
/// coordinator's would not intersect.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restarted_replica_keeps_its_recovery_promise() {
    let mut c = Sharded::start();
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    // The coordinator's frames to shard B take 1.5 s: they land after the
    // recovery and the restart below.
    for peer in &c.layout[1] {
        c.net
            .set_latency(NodeId(0), *peer, Duration::from_millis(1_500));
    }
    let coordinator = c.nodes[0].clone();
    let (ea, eb) = (event(&a, 1), event(&b, 1));
    let slow = tokio::spawn(async move { coordinator.coordinate_preaccept(vec![ea, eb]).await });
    let mut txn = None;
    for _ in 0..400 {
        if let Ok(commands) = c.journals[1].load_all().await {
            if let Some(cmd) = commands.first() {
                txn = Some(cmd.txn);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let txn = txn.expect("shard A pre-accepted it");

    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(outcome.conflict, "shard B never witnessed it");
    // The recovery decided on a slow quorum of shard B; node 3 may be the
    // replica it did not wait for. Restart it only once its promise is
    // durable, so the restart is what the test exercises.
    let mut promised = false;
    for _ in 0..400 {
        let promises = c.journals[3].load_promises().await.unwrap();
        if promises
            .iter()
            .any(|(promised_txn, _)| *promised_txn == txn)
        {
            promised = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(promised, "node 3 journals its recovery promise");
    c.restart(3).await;

    // The late PreAccept reaches every shard-B replica, the restarted one too.
    let _ = slow.await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    for id in 3..6 {
        assert_eq!(
            c.nodes[id].command_status(txn),
            None,
            "node {id} must refuse the original coordinator's late PreAccept"
        );
    }
}

/// The coordinator's abort reached shard B and one shard-A replica before it
/// died, and shard B has since compacted the transaction away — its failed
/// read is gone. Recovering from shard A, a fresh read there says "commit":
/// the decision must come from the replica that applied it instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovery_takes_the_decision_a_compacted_shard_applied() {
    let c = Sharded::start();
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    c.nodes[3].write(vec![event(&b, 1)]).await.unwrap();
    for id in 3..6 {
        for _ in 0..400 {
            if c.stores[id].applied_log().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let txn = c.nodes[0]
        .coordinate_through_commit(vec![event(&a, 1), event(&b, 1)])
        .await
        .expect("both shards commit it");
    // The abort the coordinator decided reaches shard B and node 0 only.
    let decider = c.net.sink(NodeId(9));
    for id in [3, 4, 5, 0] {
        decider
            .send(NodeId(id), Message::Apply { txn, commit: false })
            .await
            .unwrap();
    }
    c.await_applied([0, 3, 4, 5], txn).await;
    let now = HybridLogicalClock::new(NodeId(9)).now();
    for id in 3..6 {
        c.nodes[id].compact(now).await;
        assert_eq!(c.nodes[id].command_status(txn), None, "compacted away");
    }
    // Node 0's answer is the slowest the recovery hears.
    c.net
        .set_latency(NodeId(0), NodeId(1), Duration::from_millis(100));

    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(outcome.conflict, "the applied abort stands");
    c.await_applied(0..3, txn).await;
    for id in 0..3 {
        assert!(!c.committed(id, txn), "node {id} must not apply its events");
    }
}

/// The coordinator's abort reached shard B only, and two of its replicas
/// have since compacted the transaction away; the third applied it but has
/// not compacted yet. Recovering from shard A, which hears that straggler
/// first: a fresh read on shard A says "commit", so the decision must come
/// from the straggler, even though its shard is skipped as compacted.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovery_takes_the_decision_from_a_compacted_shards_straggler() {
    let c = Sharded::start();
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    c.nodes[3].write(vec![event(&b, 1)]).await.unwrap();
    for id in 3..6 {
        for _ in 0..400 {
            if c.stores[id].applied_log().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let txn = c.nodes[0]
        .coordinate_through_commit(vec![event(&a, 1), event(&b, 1)])
        .await
        .expect("both shards commit it");
    // The abort the coordinator decided reaches shard B only.
    let decider = c.net.sink(NodeId(9));
    for id in [3, 4, 5] {
        decider
            .send(NodeId(id), Message::Apply { txn, commit: false })
            .await
            .unwrap();
    }
    c.await_applied([3, 4, 5], txn).await;
    let now = HybridLogicalClock::new(NodeId(9)).now();
    for id in [4, 5] {
        c.nodes[id].compact(now).await;
        assert_eq!(c.nodes[id].command_status(txn), None, "compacted away");
    }
    // Node 3's Applied report lands first; a compacted one settles shard B.
    for id in [4, 5] {
        c.net
            .set_latency(NodeId(id), NodeId(1), Duration::from_millis(100));
    }

    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(outcome.conflict, "the applied abort stands");
    c.await_applied(0..3, txn).await;
    for id in 0..6 {
        assert!(!c.committed(id, txn), "node {id} must not apply its events");
    }
}

/// Shard B decides its abort, then every replica of it applies the abort and
/// compacts the transaction away; the Apply never reaches shard A. Returns
/// the transaction, held `Reading` on shard A with no decision.
async fn abort_compacted_on_shard_b(c: &Sharded) -> TxnId {
    let a = c.id_in_shard(0, "a");
    let b = c.id_in_shard(1, "b");
    c.nodes[3].write(vec![event(&b, 1)]).await.unwrap();
    for id in 3..6 {
        for _ in 0..400 {
            if c.stores[id].applied_log().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let txn = c.nodes[0]
        .coordinate_through_commit(vec![event(&a, 1), event(&b, 1)])
        .await
        .expect("both shards commit it");
    // The abort the coordinator decided reaches shard B only.
    let decider = c.net.sink(NodeId(9));
    for id in [3, 4, 5] {
        decider
            .send(NodeId(id), Message::Apply { txn, commit: false })
            .await
            .unwrap();
    }
    c.await_applied([3, 4, 5], txn).await;
    let now = HybridLogicalClock::new(NodeId(9)).now();
    for id in 3..6 {
        c.nodes[id].compact(now).await;
        assert_eq!(c.nodes[id].command_status(txn), None, "compacted away");
    }
    txn
}

/// Recovers `txn` from node 1 and checks shard A settles it as an abort.
async fn recover_the_abort_on_shard_a(c: &Sharded, txn: TxnId) {
    let outcome = tokio::time::timeout(Duration::from_secs(10), c.nodes[1].recover(txn))
        .await
        .expect("recovery completes")
        .expect("recovery decides");
    assert!(outcome.conflict, "shard B's abort stands");
    c.await_applied(0..3, txn).await;
    for id in 0..6 {
        assert!(!c.committed(id, txn), "node {id} must not apply its events");
    }
}

/// Every replica of shard B applied the abort and compacted the transaction
/// away, so no replica that applied it is left to report it. Shard A still
/// holds it undecided: recovery must learn the abort from shard B's
/// decision records, not wait forever for an applied replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_decided_transaction_recovers_after_its_shard_compacted_it() {
    let c = Sharded::start();
    let txn = abort_compacted_on_shard_b(&c).await;
    recover_the_abort_on_shard_a(&c, txn).await;
}

/// A decision record outlives a restart: shard B restarted from journals
/// truncated past the transaction still answers with its abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_decision_record_survives_a_restart() {
    let mut c = Sharded::start();
    let txn = abort_compacted_on_shard_b(&c).await;
    for id in 3..6 {
        assert!(
            c.journals[id].load(txn).await.unwrap().is_none(),
            "node {id} truncated the command"
        );
        c.restart(id).await;
        assert_eq!(c.nodes[id].decision_count(), 1, "node {id} kept the record");
    }
    recover_the_abort_on_shard_a(&c, txn).await;
}

/// Once shard A applied the transaction too, its replicas' watermark
/// reports pass it, and shard B drops its decision records — from memory
/// and from the journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_decision_record_is_dropped_once_every_route_shard_passed_it() {
    let c = Sharded::start();
    let txn = abort_compacted_on_shard_b(&c).await;
    for id in 3..6 {
        assert_eq!(c.nodes[id].decision_count(), 1, "node {id} keeps a record");
    }
    recover_the_abort_on_shard_a(&c, txn).await;

    let sweeps: Vec<_> = c.nodes.iter().map(Node::start_recovery).collect();
    for id in 3..6 {
        for _ in 0..1_000 {
            if c.nodes[id].decision_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            c.nodes[id].decision_count(),
            0,
            "node {id} drops its record"
        );
        assert!(
            c.journals[id].load_decisions().await.unwrap().is_empty(),
            "node {id} drops the journaled record"
        );
    }
    for sweep in sweeps {
        sweep.abort();
    }
}

/// A replica whose store already holds a transaction's own events — applied
/// before and forgotten: its journal record lost across a restart, or applied
/// by an anti-entropy import, which is not journaled — reads the transaction's
/// condition as holding. Reading its own write as a conflict would vote to
/// abort a decided commit, and a recovery hearing no applied replica would
/// enact that abort on the replicas yet to apply it (#309).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_reads_its_own_forgotten_write_as_holding() {
    let net = InMemoryNetwork::new();
    let store = Arc::new(InMemoryDataStore::new());
    let inbox = net.register(NodeId(0));
    let mut coordinator = net.register(NodeId(9));
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    let _task = node.start(inbox);
    let txn = TxnId(Timestamp {
        micros: HybridLogicalClock::new(NodeId(9)).now().micros,
        logical: 0,
        node: NodeId(9),
    });
    let events = vec![event("acc", 1)];
    // Its events are in the store; the replica holds no record of it.
    store.apply(txn, txn.0, events.clone(), true).await.unwrap();

    net.sink(NodeId(9))
        .send(
            NodeId(0),
            Message::Commit {
                epoch: 0,
                txn,
                execute_at: txn.0,
                deps: Vec::new(),
                keys: vec![Key("acc".into())],
                events,
                reply_to: NodeId(9),
            },
        )
        .await
        .unwrap();
    loop {
        if let Message::ReadOk { txn: read, ok } = next(&mut coordinator).await {
            if read == txn {
                assert!(ok, "the replica read its own write as a conflict");
                break;
            }
        }
    }
}

/// The vote fence survives a restart — even one whose wall clock stepped
/// back. A node vouches for a sync coverage cut at its clock less the
/// compaction margin; a peer may compact past a transaction below it the node
/// does not hold, trusting the node never to vote for it (#310). So before
/// that coverage leaves the node, the node persists a clock bound past the
/// reading it was cut at: restarted, its clock starts there, and its fence
/// still refuses the transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vote_fence_outlives_a_restart_with_a_clock_stepped_back() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let net = InMemoryNetwork::new();
    let journal = Arc::new(InMemoryJournal::new());
    let physical = Arc::new(AtomicU64::new(10_000_000));
    let mut peer = net.register(NodeId(1));
    let node = |net: &Arc<InMemoryNetwork>| {
        let physical = Arc::clone(&physical);
        let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        Node::new(
            NodeId(0),
            Arc::new(StaticTopology::new(NodeId(0), ids)),
            Arc::new(HybridLogicalClock::with_physical(NodeId(0), move || {
                physical.load(Ordering::SeqCst)
            })),
            Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        )
    };

    // The node answers an anti-entropy round: it vouches for a coverage cut
    // at its clock (10 s) less the compaction margin (1 s).
    let first = node(&net);
    let task = first.start(net.register(NodeId(0)));
    net.sink(NodeId(1))
        .send(
            NodeId(0),
            Message::SyncRequest {
                id: 1,
                known: evento_accord::message::SyncKnown {
                    since: evento_accord::Timestamp::MIN,
                    txns: Vec::new(),
                },
            },
        )
        .await
        .unwrap();
    let coverage = loop {
        if let Message::SyncData { coverage, .. } = next(&mut peer).await {
            break coverage;
        }
    };
    assert!(coverage.micros >= 8_000_000, "coverage {coverage:?}");
    task.abort();

    // It restarts over its journal with its wall clock 5 s back.
    physical.store(5_000_000, Ordering::SeqCst);
    let restarted = node(&net);
    restarted.recover_state().await.unwrap();
    let _task = restarted.start(net.register(NodeId(0)));

    // A transaction just below that coverage, which the node never held: a
    // recovery asks it for its vote. It must refuse, as before the restart.
    let txn = TxnId(Timestamp {
        micros: coverage.micros - 1,
        logical: 0,
        node: NodeId(2),
    });
    net.sink(NodeId(1))
        .send(
            NodeId(0),
            Message::BeginRecovery {
                epoch: 0,
                txn,
                ballot: Ballot(Timestamp {
                    micros: coverage.micros + 1,
                    logical: 0,
                    node: NodeId(1),
                }),
                keys: vec![Key("acc".into())],
                events: vec![event("acc", 1)],
                round: RecoveryRound::Ask,
            },
        )
        .await
        .unwrap();
    loop {
        match next(&mut peer).await {
            Message::NotWitnessed {
                txn: t, redundant, ..
            } if t == txn => {
                assert!(!redundant, "fenced, not compacted");
                break;
            }
            Message::BeginRecoveryOk { txn: t, .. } if t == txn => {
                panic!("the restarted node voted below what it vouched for")
            }
            _ => {}
        }
    }
}
