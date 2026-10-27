//! Transactions left stalled are recovered in the order they wait on each
//! other, and a recovery ends once the transaction is decided, whoever
//! decided it (#326). Under paused time and a virtual clock, so each bound
//! below is exact: waiting out one `collect_timeout` (5 s) would miss it.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    Ballot, DataStore, Envelope, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, InMemorySink, Journal, Key, Message, MessageSink, Node, NodeConfig, NodeId,
    StaticTopology, Status, Timestamp, TxnId,
};
use evento_core::Event;
use tokio::sync::mpsc;

/// A coordinator outside the cluster: the test plays it, and it vanishes.
const GONE: NodeId = NodeId(9);

/// A tracing subscriber over `RUST_LOG`, to debug a test; nothing without it.
fn init_tracing() {
    if std::env::var_os("RUST_LOG").is_none() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .with_writer(std::io::stderr)
        .try_init();
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

fn acc() -> Key {
    Key("acc".into())
}

/// Microseconds of virtual time since `base`: the nodes' physical clock.
fn micros_since(base: tokio::time::Instant) -> u64 {
    (tokio::time::Instant::now() - base).as_micros() as u64
}

/// A real node of the group `{0, 1, 2}`, over virtual time.
fn node(
    net: &Arc<InMemoryNetwork>,
    id: NodeId,
    base: tokio::time::Instant,
) -> (Node, Arc<InMemoryDataStore>) {
    node_with(net, id, base, NodeConfig::default())
}

/// [`node`] under `config`.
fn node_with(
    net: &Arc<InMemoryNetwork>,
    id: NodeId,
    base: tokio::time::Instant,
    config: NodeConfig,
) -> (Node, Arc<InMemoryDataStore>) {
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let store = Arc::new(InMemoryDataStore::new());
    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids)),
        Arc::new(HybridLogicalClock::with_physical(id, move || {
            micros_since(base)
        })),
        Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(config);
    (node, store)
}

/// A transaction id the outside coordinator mints now.
fn mint(base: tokio::time::Instant) -> TxnId {
    TxnId(Timestamp {
        micros: micros_since(base),
        logical: 0,
        node: GONE,
    })
}

/// Sends `message` from the outside coordinator to each of `to`.
async fn send_all(gone: &InMemorySink, to: &[NodeId], message: Message) {
    for &node in to {
        let _ = gone.send(node, message.clone()).await;
    }
}

#[tokio::test(start_paused = true)]
async fn the_sweep_recovers_what_a_stalled_read_waits_on_first() {
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();
    let ids = [NodeId(0), NodeId(1), NodeId(2)];
    let (mut stores, mut tasks) = (Vec::new(), Vec::new());
    for id in ids {
        let (node, store) = node(&net, id, base);
        tasks.push(node.start(net.register(id)));
        // One sweep: another one would race it to every recovery, and the
        // duel, not the wait, would decide how long they take.
        if id == NodeId(0) {
            tasks.push(node.start_recovery());
        }
        stores.push(store);
    }
    let _replies = net.register(GONE);
    let gone = net.sink(GONE);

    // The outside coordinator leaves two writes to one account undecided:
    // `first` took the slow path to execute after `second`, so its
    // dependencies name it; `second` only pre-accepted. Then it is gone.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let first = mint(base);
    let preaccept = |txn, version| Message::PreAccept {
        epoch: 0,
        txn,
        keys: vec![acc()],
        events: vec![event("acc", version)],
    };
    send_all(&gone, &ids, preaccept(first, 2)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let second = mint(base);
    send_all(&gone, &ids, preaccept(second, 1)).await;
    let accept = Message::Accept {
        epoch: 0,
        txn: first,
        ballot: Ballot(first.0),
        execute_at: Timestamp {
            micros: second.0.micros + 1,
            ..second.0
        },
        deps: vec![second],
        keys: vec![acc()],
    };
    send_all(&gone, &ids, accept).await;

    // The sweep takes `first` over, the lower `t0`: once committed, its read
    // waits on `second`, which must be recovered first and executes first.
    let applied_both = async {
        loop {
            let done = stores.iter().all(|store| {
                let log: Vec<(TxnId, bool)> = store
                    .applied_log()
                    .iter()
                    .map(|e| (e.txn, e.conflict))
                    .collect();
                log == [(second, false), (first, false)]
            });
            if done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), applied_both)
        .await
        .expect("both writes applied everywhere well within one collect_timeout");
}

/// How the scripted peers of node 0 treat a recovery of a write they hold
/// committed: they report it, then answer as if another node recovered it
/// too, and committed it after node 0 — replicas answer the last committer.
#[derive(Clone, Copy)]
enum Peers {
    /// They read it for node 0, but acknowledge the apply to the other node.
    ApplyElsewhere,
    /// They read it for the other node, whose decision then reaches node 0.
    ReadElsewhere,
    /// Before their report, they send a Nack naming a ballot below the
    /// recovery's, as a late answer to an earlier attempt would; then as
    /// [`ApplyElsewhere`](Self::ApplyElsewhere).
    NackBelowBallotFirst,
}

/// Plays replica `id` of a write it holds committed (see [`Peers`]).
fn scripted(
    mut inbox: mpsc::Receiver<Envelope>,
    sink: InMemorySink,
    peers: Peers,
    keys: Vec<Key>,
    events: Vec<Event>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(env) = inbox.recv().await {
            let mut replies = Vec::new();
            match env.message {
                Message::BeginRecovery { txn, ballot, .. } => {
                    if matches!(peers, Peers::NackBelowBallotFirst) {
                        replies.push(Message::Nack {
                            txn,
                            promised: Ballot(txn.0),
                        });
                    }
                    replies.push(Message::BeginRecoveryOk {
                        epoch: 0,
                        txn,
                        ballot,
                        known: true,
                        status: Status::Committed,
                        accepted: Ballot(txn.0),
                        execute_at: txn.0,
                        deps: Vec::new(),
                        superseding_rejects: false,
                        wait: false,
                        keys: keys.clone(),
                        events: events.clone(),
                    });
                }
                Message::Commit { txn, .. } => replies.push(match peers {
                    Peers::ApplyElsewhere | Peers::NackBelowBallotFirst => {
                        Message::ReadOk { txn, ok: true }
                    }
                    // The other node decided, from the reads it got.
                    Peers::ReadElsewhere => Message::Apply { txn, commit: true },
                }),
                _ => {}
            }
            for reply in replies {
                let _ = sink.send(env.from, reply).await;
            }
        }
    })
}

/// Node 0 recovers a write its scripted peers hold committed, treated as
/// `peers` says: it must end once node 0 applied the write.
async fn recover_against(peers: Peers) {
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();
    let (node, store) = node(&net, NodeId(0), base);
    let _loop = node.start(net.register(NodeId(0)));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let txn = mint(base);
    let (keys, events) = (vec![acc()], vec![event("acc", 1)]);
    let _peers: Vec<_> = [NodeId(1), NodeId(2)]
        .into_iter()
        .map(|id| {
            scripted(
                net.register(id),
                net.sink(id),
                peers,
                keys.clone(),
                events.clone(),
            )
        })
        .collect();
    // The outside coordinator pre-accepted it here before it was gone.
    let _replies = net.register(GONE);
    let message = Message::PreAccept {
        epoch: 0,
        txn,
        keys,
        events,
    };
    send_all(&net.sink(GONE), &[NodeId(0)], message).await;

    let outcome = tokio::time::timeout(Duration::from_secs(1), node.recover(txn))
        .await
        .expect("the recovery ends well within one collect_timeout")
        .expect("the recovery reports the outcome");
    assert_eq!((outcome.txn, outcome.conflict), (txn, false));
    let log: Vec<(TxnId, bool)> = store
        .applied_log()
        .iter()
        .map(|e| (e.txn, e.conflict))
        .collect();
    assert_eq!(log, [(txn, false)]);
}

#[tokio::test(start_paused = true)]
async fn a_recovery_ends_once_it_applied_the_write_whoever_heard_the_acks() {
    recover_against(Peers::ApplyElsewhere).await;
}

#[tokio::test(start_paused = true)]
async fn a_recovery_ends_once_another_node_decided_the_write() {
    recover_against(Peers::ReadElsewhere).await;
}

/// A Nack naming a ballot below the recovery's is a late answer to an
/// earlier attempt, not a refusal of this one (#441): the recovery goes on
/// under its ballot.
#[tokio::test(start_paused = true)]
async fn a_nack_below_the_ballot_does_not_end_a_recovery() {
    recover_against(Peers::NackBelowBallotFirst).await;
}

/// Plays replica `id` of a write it holds pre-accepted, which another node's
/// recovery outbids: every Accept is refused for a higher ballot of node 1's.
/// Records the ballot of each `BeginRecovery` it is sent.
fn outbidding(
    mut inbox: mpsc::Receiver<Envelope>,
    sink: InMemorySink,
    begins: Arc<std::sync::Mutex<Vec<Ballot>>>,
    keys: Vec<Key>,
    events: Vec<Event>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(env) = inbox.recv().await {
            let reply = match env.message {
                Message::BeginRecovery { txn, ballot, .. } => {
                    begins.lock().expect("begins poisoned").push(ballot);
                    Some(Message::BeginRecoveryOk {
                        epoch: 0,
                        txn,
                        ballot,
                        known: true,
                        status: Status::PreAccepted,
                        accepted: Ballot(txn.0),
                        execute_at: txn.0,
                        deps: Vec::new(),
                        superseding_rejects: false,
                        wait: false,
                        keys: keys.clone(),
                        events: events.clone(),
                    })
                }
                Message::Accept { txn, ballot, .. } => Some(Message::Nack {
                    txn,
                    promised: Ballot(Timestamp {
                        micros: ballot.0.micros + 1,
                        logical: 0,
                        node: NodeId(1),
                    }),
                }),
                _ => None,
            };
            if let Some(reply) = reply {
                let _ = sink.send(env.from, reply).await;
            }
        }
    })
}

/// A recovery outbid by another node's leaves the transaction to that one
/// (#441): it does not outbid it back, which would have the other outbid
/// it again, two sweeps in step for ever. The ballot that outbid it is
/// witnessed, so a later recovery here outbids it on its first attempt.
#[tokio::test(start_paused = true)]
async fn a_recovery_outbid_by_another_node_leaves_the_transaction_to_it() {
    init_tracing();
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();
    let (node, _store) = node(&net, NodeId(0), base);
    let _loop = node.start(net.register(NodeId(0)));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let txn = mint(base);
    let (keys, events) = (vec![acc()], vec![event("acc", 1)]);
    let begins = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _peers: Vec<_> = [NodeId(1), NodeId(2)]
        .into_iter()
        .map(|id| {
            outbidding(
                net.register(id),
                net.sink(id),
                Arc::clone(&begins),
                keys.clone(),
                events.clone(),
            )
        })
        .collect();
    let _replies = net.register(GONE);
    let message = Message::PreAccept {
        epoch: 0,
        txn,
        keys,
        events,
    };
    send_all(&net.sink(GONE), &[NodeId(0)], message).await;
    // A moment later, so the recovery's ballot is above the transaction's.
    tokio::time::sleep(Duration::from_millis(1)).await;

    let err = tokio::time::timeout(Duration::from_secs(1), node.recover(txn))
        .await
        .expect("the recovery ends well within one collect_timeout")
        .expect_err("the recovery is outbid");
    assert!(
        err.to_string().contains("outbid by node 1"),
        "the recovery ends for another reason: {err:#}"
    );
    let first: Vec<Ballot> = begins.lock().expect("begins poisoned").clone();
    assert_eq!(
        first.len(),
        2,
        "the recovery was outbid, and asked the peers again: {first:?}"
    );
    let outbid_by = Ballot(Timestamp {
        micros: first[0].0.micros + 1,
        logical: 0,
        node: NodeId(1),
    });

    // Asked for again, with the other node's recovery not done: the first
    // attempt's ballot is above the one that outbid the last.
    let _ = tokio::time::timeout(Duration::from_secs(1), node.recover(txn))
        .await
        .expect("the recovery ends well within one collect_timeout");
    let again: Vec<Ballot> = begins.lock().expect("begins poisoned").clone();
    assert!(
        again.len() > 2 && again[2] > outbid_by,
        "the next recovery begins below the ballot that outbid the last: {again:?}"
    );
}
