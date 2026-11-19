//! Multi-shard `ClusterExecutor`: reads for a key a node does not own are routed
//! to an owner, so the executor works on a sharded cluster (not just a single
//! shard where every node holds the whole log).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::claim::claim_type;
use evento_cluster::protocol::Key;
use evento_cluster::{
    ClusterExecutor, DataStore, DynamicTopology, ExecutorDataStore, HybridLogicalClock,
    InMemoryJournal, InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, Topology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor, WriteError};
use evento_fjall::Fjall;

struct ShardExec {
    execs: HashMap<NodeId, ClusterExecutor<Fjall>>,
    /// Each node's own store, to see what it holds without a routed read.
    stores: HashMap<NodeId, Fjall>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl ShardExec {
    fn start(shards: Vec<Vec<u64>>) -> Self {
        Self::start_with(shards, NodeConfig::default())
    }

    fn start_with(shards: Vec<Vec<u64>>, config: NodeConfig) -> Self {
        let shard_ids: Vec<Vec<NodeId>> = shards
            .iter()
            .map(|s| s.iter().copied().map(NodeId).collect())
            .collect();
        let net = InMemoryNetwork::new();

        let mut execs = HashMap::new();
        let mut stores = HashMap::new();
        let mut loops = Vec::new();

        for id in shard_ids.iter().flatten().copied() {
            let fjall = Fjall::temporary().unwrap();

            let inbox = net.register(id);
            let clock = Arc::new(HybridLogicalClock::new(id));
            let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
            let datastore: Arc<dyn DataStore> = Arc::new(ExecutorDataStore::new(fjall.clone()));
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(DynamicTopology::new(id, 0, shard_ids.clone()));
            let node = Node::new(
                id,
                topology as Arc<dyn Topology>,
                clock,
                sink,
                datastore,
                journal,
            )
            .with_config(config.clone());

            loops.push(node.start(inbox));
            stores.insert(id, fjall.clone());
            execs.insert(id, ClusterExecutor::new(node, fjall));
        }

        ShardExec {
            execs,
            stores,
            _loops: loops,
        }
    }

    async fn read_all(&self, node: u64, id: &str) -> Vec<Event> {
        let result = self.execs[&NodeId(node)]
            .read(
                Some([EventFilter::by_id_raw("test/Account", id)].into()),
                None,
                evento_core::TenantFilter::Only(String::new()),
                Args::forward(50, None),
                None,
            )
            .await
            .unwrap();
        result.edges.into_iter().map(|e| e.node).collect()
    }

    /// What `node`'s own store holds of the stream `aggregate_type`/`id`.
    async fn held(&self, node: u64, aggregate_type: &str, id: &str) -> Vec<Event> {
        let result = self.stores[&NodeId(node)]
            .read(
                Some([EventFilter::by_id_raw(aggregate_type, id)].into()),
                None,
                evento_core::TenantFilter::Only(String::new()),
                Args::forward(50, None),
                None,
            )
            .await
            .unwrap();
        result.edges.into_iter().map(|e| e.node).collect()
    }

    /// Waits until every node of `nodes` holds `len` events of the stream.
    async fn await_held(&self, nodes: &[u64], aggregate_type: &str, id: &str, len: usize) {
        for &node in nodes {
            let mut held = 0;
            for _ in 0..1000 {
                held = self.held(node, aggregate_type, id).await.len();
                if held == len {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                held, len,
                "node {node} holds {held} events of {aggregate_type}/{id}"
            );
        }
    }

    async fn await_read(&self, node: u64, id: &str, len: usize) -> Vec<Event> {
        for _ in 0..1000 {
            let events = self.read_all(node, id).await;
            if events.len() >= len {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("node {node} never saw {len} events for {id}");
    }
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    static SEQ: AtomicU32 = AtomicU32::new(1);
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        timestamp: 1,
        timestamp_subsec: SEQ.fetch_add(1, Ordering::SeqCst),
        ..Default::default()
    }
}

fn id_in_shard(layout: &[Vec<NodeId>], shard: usize, prefix: &str) -> String {
    let probe = DynamicTopology::new(NodeId(0), 1, layout.to_vec());
    for i in 0..100_000 {
        let id = format!("{prefix}-{i}");
        if probe.shard_of(&Key(id.clone())) == shard {
            return id;
        }
    }
    panic!("no id found for shard {shard}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn reads_route_to_the_owning_shard() {
    let cluster = ShardExec::start(vec![vec![0, 1, 2], vec![3, 4, 5]]);
    let layout = vec![
        vec![NodeId(0), NodeId(1), NodeId(2)],
        vec![NodeId(3), NodeId(4), NodeId(5)],
    ];

    // A key owned by shard 1 ({3,4,5}).
    let key = id_in_shard(&layout, 1, "k");

    // Write it through a coordinator in shard 0 — it routes to shard 1, whose
    // nodes apply it to their local backend; shard 0's nodes never store it.
    cluster.execs[&NodeId(0)]
        .write(vec![event(&key, 1, "Opened")])
        .await
        .unwrap();

    // Read from a shard-1 owner directly (served locally).
    let owner_view = cluster.await_read(3, &key, 1).await;
    assert_eq!(owner_view.len(), 1);
    assert_eq!(owner_view[0].version, 1);

    // Read the same key from a shard-0 node that does NOT own it — the executor
    // forwards the read to an owner and returns its result.
    let routed = cluster.await_read(0, &key, 1).await;
    assert_eq!(routed.len(), 1, "read was routed to the owning shard");
    assert_eq!(routed[0].version, 1);
    assert_eq!(routed[0].name, "Opened");
}

// ----- stream claims (#570) -------------------------------------------------

const SHARDS: [[u64; 3]; 2] = [[0, 1, 2], [3, 4, 5]];

fn two_shards() -> Vec<Vec<u64>> {
    SHARDS.iter().map(|s| s.to_vec()).collect()
}

fn two_shards_layout() -> Vec<Vec<NodeId>> {
    SHARDS
        .iter()
        .map(|s| s.iter().copied().map(NodeId).collect())
        .collect()
}

fn claiming() -> NodeConfig {
    NodeConfig {
        stream_claims: true,
        ..NodeConfig::default()
    }
}

fn routed(aggregate_id: &str, version: u16, routing_key: Option<&str>) -> Event {
    Event {
        routing_key: routing_key.map(str::to_owned),
        ..event(aggregate_id, version, "Opened")
    }
}

/// A routing key of each shard, and the id of a stream whose claim lives in
/// `claim_shard`.
fn keys_apart(claim_shard: usize) -> (String, String, String) {
    let layout = two_shards_layout();
    (
        id_in_shard(&layout, 0, "eu"),
        id_in_shard(&layout, 1, "us"),
        id_in_shard(&layout, claim_shard, "account"),
    )
}

/// The default, and what #570 reports: two creations of one stream under
/// routing keys of two shards share no replica, so both commit and each shard
/// ends with a stream of that id.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn without_claims_a_stream_is_created_once_in_each_shard() {
    let cluster = ShardExec::start(two_shards());
    let (eu, us, id) = keys_apart(0);

    cluster.execs[&NodeId(0)]
        .write(vec![routed(&id, 1, Some(&eu))])
        .await
        .unwrap();
    cluster.execs[&NodeId(3)]
        .write(vec![routed(&id, 1, Some(&us))])
        .await
        .unwrap();

    cluster.await_held(&SHARDS[0], "test/Account", &id, 1).await;
    cluster.await_held(&SHARDS[1], "test/Account", &id, 1).await;
}

/// With claims, the second creation meets the first in the shard of the
/// stream's id, whichever shard that is and whichever node coordinates: a
/// version conflict, and nothing written anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_stream_claimed_under_one_routing_key_is_not_created_under_another() {
    for claim_shard in 0..2 {
        let cluster = ShardExec::start_with(two_shards(), claiming());
        let (eu, us, id) = keys_apart(claim_shard);

        cluster.execs[&NodeId(0)]
            .write(vec![routed(&id, 1, Some(&eu))])
            .await
            .unwrap();
        for coordinator in [0, 3] {
            // Under a routing key of the other shard, and under none.
            for routing_key in [Some(us.as_str()), None] {
                let again = cluster.execs[&NodeId(coordinator)]
                    .write(vec![routed(&id, 1, routing_key)])
                    .await;
                assert!(
                    matches!(again, Err(WriteError::InvalidOriginalVersion)),
                    "claim in shard {claim_shard}, node {coordinator}, under {routing_key:?}: {again:?}"
                );
            }
        }

        // The stream is in its routing key's shard alone, its claim in its
        // id's — holding the routing key it was created under.
        cluster.await_held(&SHARDS[0], "test/Account", &id, 1).await;
        cluster.await_held(&SHARDS[1], "test/Account", &id, 0).await;
        let claim = claim_type("test/Account");
        cluster
            .await_held(&SHARDS[claim_shard], &claim, &id, 1)
            .await;
        cluster
            .await_held(&SHARDS[1 - claim_shard], &claim, &id, 0)
            .await;
        let held = cluster
            .held(SHARDS[claim_shard][0], &claim, &id)
            .await
            .remove(0);
        assert_eq!(held.data, eu.as_bytes());
        assert_eq!(held.name, "test/Account");

        // The stream goes on under its routing key: only a creation claims.
        cluster.execs[&NodeId(3)]
            .write(vec![routed(&id, 2, Some(&eu))])
            .await
            .unwrap();
        cluster.await_held(&SHARDS[0], "test/Account", &id, 2).await;
        cluster
            .await_held(&SHARDS[claim_shard], &claim, &id, 1)
            .await;
    }
}

/// A stream created with no routing key claims too, in its own shard: a
/// later creation under a routing key of the other shard is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_stream_created_without_a_routing_key_is_claimed_too() {
    let cluster = ShardExec::start_with(two_shards(), claiming());
    let (_, us, id) = keys_apart(0);

    cluster.execs[&NodeId(4)]
        .write(vec![routed(&id, 1, None)])
        .await
        .unwrap();
    let again = cluster.execs[&NodeId(1)]
        .write(vec![routed(&id, 1, Some(&us))])
        .await;
    assert!(
        matches!(again, Err(WriteError::InvalidOriginalVersion)),
        "{again:?}"
    );
    cluster.await_held(&SHARDS[0], "test/Account", &id, 1).await;
    cluster.await_held(&SHARDS[1], "test/Account", &id, 0).await;
}

/// Created at once under a routing key of each shard, by a node of each: one
/// creation commits, the other conflicts, and one shard holds the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn one_stream_created_at_once_under_routing_keys_of_two_shards_is_created_once() {
    let cluster = ShardExec::start_with(two_shards(), claiming());
    let layout = two_shards_layout();
    let (eu, us, _) = keys_apart(0);

    for round in 0..20 {
        let id = id_in_shard(&layout, round % 2, &format!("race{round}"));
        let (in_eu, in_us) = tokio::join!(
            cluster.execs[&NodeId(0)].write(vec![routed(&id, 1, Some(&eu))]),
            cluster.execs[&NodeId(3)].write(vec![routed(&id, 1, Some(&us))]),
        );
        let (winner, loser) = match (&in_eu, &in_us) {
            (Ok(()), Err(WriteError::InvalidOriginalVersion)) => (0, 1),
            (Err(WriteError::InvalidOriginalVersion), Ok(())) => (1, 0),
            other => panic!("round {round}: {other:?}"),
        };
        cluster
            .await_held(&SHARDS[winner], "test/Account", &id, 1)
            .await;
        cluster
            .await_held(&SHARDS[loser], "test/Account", &id, 0)
            .await;
        cluster
            .await_held(&SHARDS[round % 2], &claim_type("test/Account"), &id, 1)
            .await;
    }
}

/// Two aggregate types under one id are two streams: each is created, under
/// whatever routing key.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn another_type_under_a_claimed_id_is_still_created() {
    let cluster = ShardExec::start_with(two_shards(), claiming());
    let (eu, us, id) = keys_apart(1);

    cluster.execs[&NodeId(0)]
        .write(vec![routed(&id, 1, Some(&eu))])
        .await
        .unwrap();
    cluster.execs[&NodeId(3)]
        .write(vec![Event {
            aggregate_type: "test/Owner".into(),
            ..routed(&id, 1, Some(&us))
        }])
        .await
        .unwrap();
    cluster.await_held(&SHARDS[0], "test/Account", &id, 1).await;
    cluster.await_held(&SHARDS[1], "test/Owner", &id, 1).await;
}
