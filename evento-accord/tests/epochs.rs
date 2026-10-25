//! Transactions keep the epoch they were coordinated under (#288, #359): a
//! real node votes on, and recovers, a transaction under that epoch's layout,
//! whatever it installed since, while the test plays its peers.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    Ballot, Clock, DataStore, DynamicTopology, Envelope, HybridLogicalClock, InMemoryDataStore,
    InMemoryJournal, InMemoryNetwork, InMemorySink, Journal, Key, Message, MessageSink, Node,
    NodeConfig, NodeId, Status, Timestamp, Topology, TxnId,
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

fn ids(nodes: &[u64]) -> Vec<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

/// How a peer played by the test answers a recovery.
#[derive(Clone, Copy)]
enum Answer {
    /// It holds the transaction at `status`.
    Known(Status),
    /// It never witnessed the transaction, below its vote fence: it never
    /// voted for it and never will.
    NotWitnessed,
    /// It is down.
    Silent,
}

/// A replica played by the test: answers a recovery as told, accepts, reads
/// and applies whatever it is asked to, and logs every message it receives.
struct Peer {
    received: Arc<Mutex<Vec<Message>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Peer {
    fn spawn(
        net: &Arc<InMemoryNetwork>,
        id: u64,
        answer: Answer,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> Self {
        let mut inbox = net.register(NodeId(id));
        let sink: InMemorySink = net.sink(NodeId(id));
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&received);
        let task = tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                log.lock().unwrap().push(env.message.clone());
                if matches!(answer, Answer::Silent) {
                    continue;
                }
                let (message, route) = match env.message {
                    Message::Routed { route, inner } => (*inner, Some(route)),
                    message => (message, None),
                };
                let reply = match message {
                    Message::BeginRecovery {
                        txn, ballot, epoch, ..
                    } => match answer {
                        Answer::Known(status) => Some(Message::BeginRecoveryOk {
                            txn,
                            ballot,
                            epoch,
                            known: true,
                            status,
                            accepted: Ballot(txn.0),
                            execute_at: txn.0,
                            deps: Vec::new(),
                            superseding_rejects: false,
                            wait: false,
                            keys: keys.clone(),
                            events: events.clone(),
                        }),
                        Answer::NotWitnessed => Some(Message::NotWitnessed {
                            txn,
                            ballot,
                            redundant: false,
                        }),
                        Answer::Silent => None,
                    },
                    Message::Accept { txn, ballot, .. } => Some(Message::AcceptOk {
                        txn,
                        ballot,
                        deps: Vec::new(),
                    }),
                    Message::Commit { txn, .. } => Some(Message::ReadOk { txn, ok: true }),
                    Message::Apply { txn, .. } => Some(Message::Applied { txn }),
                    _ => None,
                };
                let reply = match (reply, route) {
                    // A holder of a routed transaction answers routed.
                    (Some(report @ Message::BeginRecoveryOk { .. }), Some(route)) => {
                        Some(Message::Routed {
                            route,
                            inner: Box::new(report),
                        })
                    }
                    (reply, _) => reply,
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

    fn got(&self, matches: impl Fn(&Message) -> bool) -> bool {
        self.received().iter().any(|m| match m {
            Message::Routed { inner, .. } => matches(inner),
            m => matches(m),
        })
    }
}

/// A real node 0 over a [`DynamicTopology`] starting at epoch 0 with
/// `shards`, the coordinator played by the test as node 9.
struct Setup {
    node: Node,
    topology: Arc<DynamicTopology>,
    journal: Arc<InMemoryJournal>,
    net: Arc<InMemoryNetwork>,
    coordinator: mpsc::Receiver<Envelope>,
    _loop: tokio::task::JoinHandle<()>,
}

fn setup(shards: Vec<Vec<NodeId>>) -> Setup {
    let net = InMemoryNetwork::new();
    let inbox = net.register(NodeId(0));
    let topology = Arc::new(DynamicTopology::new(NodeId(0), 0, shards));
    let journal = Arc::new(InMemoryJournal::new());
    let node = Node::new(
        NodeId(0),
        Arc::clone(&topology) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::clone(&journal) as Arc<dyn Journal>,
    )
    .with_config(NodeConfig {
        collect_timeout: Duration::from_millis(500),
        ..NodeConfig::default()
    });
    let task = node.start(inbox);
    let coordinator = net.register(NodeId(9));
    Setup {
        node,
        topology,
        journal,
        net,
        coordinator,
        _loop: task,
    }
}

impl Setup {
    /// A transaction id the coordinator mints now: within every vote fence.
    fn fresh_txn(&self) -> TxnId {
        TxnId(Timestamp {
            micros: HybridLogicalClock::new(NodeId(9)).now().micros,
            logical: 0,
            node: NodeId(9),
        })
    }

    async fn send(&self, from: u64, message: Message) {
        self.net
            .sink(NodeId(from))
            .send(NodeId(0), message)
            .await
            .unwrap();
    }

    /// The next message node 0 sends the coordinator, if one comes soon.
    async fn reply(&mut self) -> Option<Message> {
        tokio::time::timeout(Duration::from_millis(300), self.coordinator.recv())
            .await
            .ok()
            .flatten()
            .map(|env| env.message)
    }

    /// Node 0 installs `layout` as epoch `epoch`, decided elsewhere.
    async fn install(&self, epoch: u64, layout: Vec<Vec<NodeId>>) {
        self.send(1, Message::ConfigCommit { epoch, layout }).await;
        for _ in 0..200 {
            if self.topology.epoch() >= epoch {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("epoch {epoch} not installed");
    }
}

/// #359: a membership change must not let recovery count the new replicas
/// for a transaction the old ones voted on. T was accepted under epoch 0's
/// `{0, 1, 2}`; node 2 is down, and epoch 1 replaces it with node 3, which
/// never saw T. Recovering under the installed `{0, 1, 3}` would count 1 and
/// 3 as fenced — more than the one replica a quorum can spare — and
/// invalidate T, which `{0, 2}` may have committed. Under T's own layout only
/// node 1 is fenced: recovery cannot decide without node 2, and waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_asks_the_replicas_a_transaction_was_coordinated_under() {
    let mut s = setup(vec![ids(&[0, 1, 2])]);
    let keys = vec![Key("acc".into())];
    let events = vec![event("acc", 1)];
    let peers = [
        Peer::spawn(
            &s.net,
            1,
            Answer::NotWitnessed,
            keys.clone(),
            events.clone(),
        ),
        Peer::spawn(&s.net, 2, Answer::Silent, keys.clone(), events.clone()),
        Peer::spawn(
            &s.net,
            3,
            Answer::NotWitnessed,
            keys.clone(),
            events.clone(),
        ),
    ];
    let txn = s.fresh_txn();
    s.send(
        9,
        Message::PreAccept {
            txn,
            epoch: 0,
            keys: keys.clone(),
            events: events.clone(),
        },
    )
    .await;
    assert!(matches!(s.reply().await, Some(Message::PreAcceptOk { .. })));
    s.send(
        9,
        Message::Accept {
            txn,
            ballot: Ballot(txn.0),
            epoch: 0,
            execute_at: txn.0,
            deps: Vec::new(),
            keys: keys.clone(),
        },
    )
    .await;
    assert!(matches!(s.reply().await, Some(Message::AcceptOk { .. })));
    s.install(1, vec![ids(&[0, 1, 3])]).await;

    let result = tokio::time::timeout(Duration::from_secs(10), s.node.recover(txn))
        .await
        .expect("recovery gives up in bounded time");
    assert!(
        !matches!(result, Ok(ref outcome) if outcome.conflict),
        "an accepted transaction was invalidated: {result:?}"
    );
    assert!(result.is_err(), "no quorum of T's replicas answered");
    assert!(
        peers[1].got(|m| matches!(m, Message::BeginRecovery { .. })),
        "T's replica 2 is asked"
    );
    assert!(
        !peers[2].got(|m| matches!(m, Message::BeginRecovery { .. })),
        "node 3 was no replica of T"
    );
    for peer in &peers {
        assert!(!peer.got(|m| matches!(m, Message::Invalidate { .. })));
    }
    assert_eq!(s.node.command_status(txn), Some(Status::Accepted));
}

/// #288: a routed recovery counts a shard's reports under the layout the
/// transaction was coordinated under. T spans both shards of epoch 0, and
/// was pre-accepted by `{0, 1}` and `{3, 4}`; epoch 1 swaps nodes 2 and 3.
/// Under the installed layout the second shard is `{2, 4, 5}`, where 2 and 5
/// never saw T: a slow quorum of "never witnessed", so the live layout
/// invalidates T. Under T's own `{3, 4, 5}` two replicas hold it, and
/// recovery commits it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_routed_recovery_counts_shards_under_the_transactions_epoch() {
    let shards = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
    let mut s = setup(shards.clone());
    let probe = DynamicTopology::new(NodeId(0), 0, shards);
    let events: Vec<Event> = (0..16).map(|i| event(&format!("acc{i}"), 1)).collect();
    let (a, b): (Vec<Event>, Vec<Event>) = events
        .into_iter()
        .partition(|e| probe.shard_of(&Key::of(e)) == 0);
    let (a, b) = (a[0].clone(), b[0].clone());
    let route = vec![Key::of(&a), Key::of(&b)];
    let known = |id| {
        Peer::spawn(
            &s.net,
            id,
            Answer::Known(Status::PreAccepted),
            vec![Key::of(&b)],
            vec![b.clone()],
        )
    };
    let peers = [
        Peer::spawn(
            &s.net,
            1,
            Answer::Known(Status::PreAccepted),
            vec![Key::of(&a)],
            vec![a.clone()],
        ),
        Peer::spawn(&s.net, 2, Answer::NotWitnessed, Vec::new(), Vec::new()),
        known(3),
        known(4),
        Peer::spawn(&s.net, 5, Answer::NotWitnessed, Vec::new(), Vec::new()),
    ];
    let txn = s.fresh_txn();
    s.send(
        9,
        Message::Routed {
            route: route.clone(),
            inner: Box::new(Message::PreAccept {
                txn,
                epoch: 0,
                keys: vec![Key::of(&a)],
                events: vec![a.clone()],
            }),
        },
    )
    .await;
    assert!(matches!(s.reply().await, Some(Message::PreAcceptOk { .. })));
    s.install(1, vec![ids(&[0, 1, 3]), ids(&[2, 4, 5])]).await;

    let outcome = tokio::time::timeout(Duration::from_secs(10), s.node.recover(txn))
        .await
        .expect("recovery ends in bounded time")
        .expect("T's replicas answer");
    assert!(!outcome.conflict, "T was invalidated");
    for peer in &peers {
        assert!(!peer.got(|m| matches!(m, Message::Invalidate { .. })));
    }
    assert!(
        peers[2].got(|m| matches!(m, Message::Commit { .. })),
        "T's replica 3 commits it"
    );
}

/// A coordinator that planned a transaction before a change, under a layout
/// whose replica set for a key differs from the one installed since, gets no
/// vote for it from a replica that never saw it: the commit it could reach
/// through the old replicas would miss the new one. Planned under the
/// installed epoch, it gets one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordinator_that_planned_before_a_change_gets_no_vote() {
    let mut s = setup(vec![ids(&[0, 1, 2])]);
    s.install(1, vec![ids(&[0, 1, 3])]).await;
    let keys = vec![Key("acc".into())];
    let events = vec![event("acc", 1)];

    let stale = s.fresh_txn();
    s.send(
        9,
        Message::PreAccept {
            txn: stale,
            epoch: 0,
            keys: keys.clone(),
            events: events.clone(),
        },
    )
    .await;
    assert!(s.reply().await.is_none(), "no vote under a retired layout");
    assert_eq!(s.node.command_status(stale), None, "nothing recorded");
    assert_eq!(s.node.metrics().votes_retired, 1);

    let current = TxnId(Timestamp {
        logical: 1,
        ..stale.0
    });
    s.send(
        9,
        Message::PreAccept {
            txn: current,
            epoch: 1,
            keys,
            events,
        },
    )
    .await;
    assert!(matches!(s.reply().await, Some(Message::PreAcceptOk { .. })));
}

/// The same holds for a linearizable read's probe: one planned under a
/// layout retired since for its key is not answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_probe_planned_before_a_change_gets_no_answer() {
    let mut s = setup(vec![ids(&[0, 1, 2])]);
    s.install(1, vec![ids(&[0, 1, 3])]).await;
    let key = Key("acc".into());
    let probe = s.fresh_txn();
    s.send(
        9,
        Message::ReadProbe {
            txn: probe,
            epoch: 0,
            key: key.clone(),
        },
    )
    .await;
    assert!(
        s.reply().await.is_none(),
        "no answer under a retired layout"
    );
    s.send(
        9,
        Message::ReadProbe {
            txn: probe,
            epoch: 1,
            key,
        },
    )
    .await;
    assert!(matches!(s.reply().await, Some(Message::ReadProbeOk { .. })));
}

/// A message about a transaction planned under an epoch this node has not
/// installed yet is not answered: the node asks the sender for the metadata
/// it is missing first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_preaccept_from_a_later_epoch_fetches_the_metadata_first() {
    let mut s = setup(vec![ids(&[0, 1, 2])]);
    let txn = s.fresh_txn();
    s.send(
        9,
        Message::PreAccept {
            txn,
            epoch: 1,
            keys: vec![Key("acc".into())],
            events: vec![event("acc", 1)],
        },
    )
    .await;
    match s.reply().await {
        Some(Message::MetadataFetch { after_epoch: 0 }) => {}
        other => panic!("expected a metadata fetch, got {other:?}"),
    }
    assert!(s.reply().await.is_none(), "no vote");
    assert_eq!(s.node.command_status(txn), None);
    assert_eq!(s.node.metrics().messages_future_epoch, 1);
}

/// The layout a node started from is journaled with the first epoch decided
/// after it: a restart whose topology is built from a later layout still
/// holds it, to recover what was coordinated under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_starting_layout_survives_a_restart_on_a_later_layout() {
    let s = setup(vec![ids(&[0, 1, 2])]);
    s.install(1, vec![ids(&[0, 1, 3])]).await;
    let journaled = s.journal.load_metadata().await.unwrap();
    assert_eq!(
        journaled,
        vec![(0, vec![ids(&[0, 1, 2])]), (1, vec![ids(&[0, 1, 3])])]
    );

    let net = InMemoryNetwork::new();
    let topology = Arc::new(DynamicTopology::new(NodeId(0), 1, vec![ids(&[0, 1, 3])]));
    let restarted = Node::new(
        NodeId(0),
        Arc::clone(&topology) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::clone(&s.journal) as Arc<dyn Journal>,
    );
    restarted.recover_state().await.unwrap();
    let start = topology.layout_at(0).expect("the starting layout is kept");
    assert_eq!(start.shards(), &[ids(&[0, 1, 2])]);
    assert_eq!(topology.epoch(), 1);
}

/// A change for an epoch past the next one is refused before any vote: its
/// acceptors would be the installed epoch's members, and its entry would wait
/// for good for the epoch it skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_change_skipping_an_epoch_is_refused() {
    let s = setup(vec![ids(&[0])]);
    let err = s
        .node
        .change_topology(2, vec![ids(&[0])])
        .await
        .expect_err("epoch 1 is skipped");
    assert!(err.to_string().contains("skips"), "{err}");
    assert_eq!(s.topology.epoch(), 0);
    assert!(s.journal.load_metadata().await.unwrap().is_empty());

    s.node.change_topology(1, vec![ids(&[0])]).await.unwrap();
    assert_eq!(s.topology.epoch(), 1);
}
