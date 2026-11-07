//! A coordinator's PreAccept that outlives the compaction margin on a replica
//! that never witnessed the write is refused as late (#466). The replica says
//! so, instead of staying silent, so the coordinator settles the write at
//! once rather than after `collect_timeout`; a write that settles as
//! invalidated says it never committed, rather than passing for a version
//! conflict; and a node's own replica is never late for a write the node is
//! still coordinating — alone in its group, its inbox *is* the message delay,
//! and a slow second of fsyncs must not fail every write of the burst behind
//! it. Under paused time and a virtual clock, so each bound below is exact.

use std::sync::Arc;
use std::time::Duration;

use evento_cluster::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::Event;

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

/// Microseconds of virtual time since `base`: the nodes' physical clock.
fn micros_since(base: tokio::time::Instant) -> u64 {
    (tokio::time::Instant::now() - base).as_micros() as u64
}

/// A short margin: the deadline a message outlives within the test.
const MARGIN: Duration = Duration::from_millis(30);

/// The margin, under a fast-path timeout a delayed message still fits: the
/// recovery that settles a late write collects its late replicas' refusals
/// for `fast_timeout`, as any recovery round; a delay past it is a network
/// no margin is sized for, not what is tested here. The recovery interval
/// is shorter than the delay, so a first recovery round re-asks the
/// replicas still silent and hears each twice: the straggler's answer must
/// not end the round that settles them.
fn config() -> NodeConfig {
    NodeConfig {
        compaction_margin: MARGIN,
        fast_timeout: Duration::from_millis(100),
        recovery_interval: Duration::from_millis(40),
        ..NodeConfig::default()
    }
}

/// A real node `id` of the group `ids`, over virtual time.
fn node(
    net: &Arc<InMemoryNetwork>,
    id: NodeId,
    ids: &[NodeId],
    base: tokio::time::Instant,
) -> (Node, Arc<InMemoryDataStore>) {
    let store = Arc::new(InMemoryDataStore::new());
    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids.to_vec())),
        Arc::new(HybridLogicalClock::with_physical(id, move || {
            micros_since(base)
        })),
        Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    )
    .with_config(config());
    (node, store)
}

/// A node alone in its group whose inbox stalls past the margin — every
/// message to itself takes longer than the margin, as a batch of applies to
/// a slow store holds up its own PreAccept — still commits its write: its
/// replica is the write's coordinator, and the deadline that keeps a stale
/// coordinator's message from landing after a recovery protects no one
/// against it. Meanwhile nothing the node vouches for passes the write:
/// its stable point waits for it.
#[tokio::test(start_paused = true)]
async fn a_nodes_own_write_outlives_the_margin_and_still_commits() {
    init_tracing();
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();
    let id = NodeId(0);
    let (node, store) = node(&net, id, &[id], base);
    let _loop = node.start(net.register(id));
    // Every hop to itself outlives the margin.
    net.set_latency(id, id, MARGIN * 5 / 2);

    tokio::time::sleep(Duration::from_millis(10)).await;
    let minted_at = micros_since(base);
    let write = tokio::spawn({
        let node = node.clone();
        async move { node.write(vec![event("acc", 1)]).await }
    });

    // Past the margin, the write's own PreAccept still in flight: the node's
    // stable point has not passed the write (the write's `t0` is `minted_at`:
    // the task minted it before virtual time moved).
    tokio::time::sleep(MARGIN * 3 / 2).await;
    let stable = node.stable_micros();
    assert!(
        stable <= minted_at,
        "the stable point {stable} passed a write minted at {minted_at} and still coordinated"
    );

    let outcome = tokio::time::timeout(Duration::from_secs(4), write)
        .await
        .expect("the write ends well within one collect_timeout")
        .expect("the write task")
        .expect("the write succeeds");
    assert!(
        !outcome.conflict && !outcome.invalidated,
        "a node alone refused its own write as late: {outcome:?}"
    );
    assert_eq!(node.metrics().votes_late, 0, "its own replica voted");
    let log = store.applied_log();
    assert_eq!(log.len(), 1, "applied once: {log:?}");
    assert!(!log[0].conflict);
}

/// A coordinator whose PreAccept lands on the other replicas past the margin
/// is told so at once, and settles the write within a few message delays
/// rather than after `collect_timeout`: recovered, and — too few replicas
/// holding it — invalidated, which the outcome says, since nothing was
/// written and no version condition judged. The group goes on taking writes.
#[tokio::test(start_paused = true)]
async fn a_coordinator_refused_late_settles_its_write_at_once() {
    init_tracing();
    let net = InMemoryNetwork::new();
    let base = tokio::time::Instant::now();
    let ids = [NodeId(0), NodeId(1), NodeId(2)];
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    let mut tasks = Vec::new();
    for id in ids {
        let (node, store) = node(&net, id, &ids, base);
        tasks.push(node.start(net.register(id)));
        nodes.push(node);
        stores.push(store);
    }
    // Node 0's messages reach its peers past the margin; theirs are prompt.
    let delay = MARGIN * 2;
    net.set_latency(NodeId(0), NodeId(1), delay);
    net.set_latency(NodeId(0), NodeId(2), delay);

    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = tokio::time::Instant::now();
    let outcome = nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("the write settles");
    let took = started.elapsed();
    assert!(
        outcome.invalidated && outcome.conflict,
        "a write too few replicas held in time passed for decided: {outcome:?}"
    );
    // PreAccept out, Late back; then the recovery's rounds — ask, refuse —
    // and the Invalidate, each a delay: a handful of them, no 5 s wait.
    assert!(
        took < Duration::from_secs(3),
        "the write took {took:?}: the coordinator waited out collect_timeout"
    );
    assert_eq!(nodes[1].metrics().votes_late, 1);
    assert_eq!(nodes[2].metrics().votes_late, 1);
    let aborted = outcome.txn;
    for store in &stores {
        assert!(
            store
                .applied_log()
                .iter()
                .all(|entry| entry.txn != aborted || entry.conflict),
            "an invalidated write was applied as a commit"
        );
    }

    // With the delay gone, the next write commits everywhere.
    net.clear_latency();
    let outcome = nodes[0]
        .write(vec![event("acc", 1)])
        .await
        .expect("the write commits");
    assert!(!outcome.conflict && !outcome.invalidated, "{outcome:?}");
    let committed = outcome.txn;
    for store in &stores {
        let applied = async {
            while !store
                .applied_log()
                .iter()
                .any(|entry| entry.txn == committed && !entry.conflict)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(2), applied)
            .await
            .expect("every replica applies the committed write");
    }
}
