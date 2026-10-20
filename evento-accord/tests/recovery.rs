//! Recovery against scripted peers: a real node takes a transaction over while
//! the other replicas are played by the test, so each one's answers (and what
//! the recovery coordinator sends them) are exact.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    Ballot, Clock, DataStore, Envelope, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, InMemorySink, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId,
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

/// What a scripted replica answers a recovery with.
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
                    Message::Recover { txn, ballot } => {
                        tokio::time::sleep(report.delay).await;
                        Some(Message::RecoverOk {
                            txn,
                            ballot,
                            known: report.known,
                            status: report.status,
                            accepted: report.accepted,
                            execute_at: report.execute_at,
                            deps: Vec::new(),
                            superseding_rejects: false,
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
