//! Recovery against scripted peers: a real node takes a transaction over while
//! the other replicas are played by the test, so each one's answers (and what
//! the recovery coordinator sends them) are exact.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    Ballot, Clock, DataStore, Envelope, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, InMemorySink, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId,
    ShardedTopology, StaticTopology, Status, Timestamp, Topology, TxnId,
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
            txn: self.txn,
            keys: self.keys.clone(),
            events: self.events.clone(),
        }
    }

    fn accept(&self) -> Message {
        Message::Accept {
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
