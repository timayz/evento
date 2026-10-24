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
    .with_config(NodeConfig::default());
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
        txn,
        keys: vec![acc()],
        events: vec![event("acc", version)],
    };
    send_all(&gone, &ids, preaccept(first, 2)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let second = mint(base);
    send_all(&gone, &ids, preaccept(second, 1)).await;
    let accept = Message::Accept {
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
            let reply = match env.message {
                Message::BeginRecovery { txn, ballot, .. } => Some(Message::BeginRecoveryOk {
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
                }),
                Message::Commit { txn, .. } => match peers {
                    Peers::ApplyElsewhere => Some(Message::ReadOk { txn, ok: true }),
                    // The other node decided, from the reads it got.
                    Peers::ReadElsewhere => Some(Message::Apply { txn, commit: true }),
                },
                _ => None,
            };
            if let Some(reply) = reply {
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
    let message = Message::PreAccept { txn, keys, events };
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
