//! Multi-shard `AccordExecutor`: reads for a key a node does not own are routed
//! to an owner, so the executor works on a sharded cluster (not just a single
//! shard where every node holds the whole log).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AccordExecutor, DataStore, DynamicTopology, ExecutorDataStore, HybridLogicalClock,
    InMemoryJournal, InMemoryNetwork, Journal, Key, MessageSink, Node, NodeId, Topology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor};
use evento_fjall::Fjall;
use tempfile::TempDir;

struct ShardExec {
    execs: HashMap<NodeId, AccordExecutor<Fjall>>,
    _temps: Vec<TempDir>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

impl ShardExec {
    fn start(shards: Vec<Vec<u64>>) -> Self {
        let shard_ids: Vec<Vec<NodeId>> = shards
            .iter()
            .map(|s| s.iter().copied().map(NodeId).collect())
            .collect();
        let net = InMemoryNetwork::new();

        let mut execs = HashMap::new();
        let mut temps = Vec::new();
        let mut loops = Vec::new();

        for id in shard_ids.iter().flatten().copied() {
            let temp = tempfile::Builder::new()
                .prefix("evento_accord_shard_exec")
                .tempdir()
                .unwrap();
            let fjall = Fjall::open(temp.path()).unwrap();

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
            );

            loops.push(node.start(inbox));
            execs.insert(id, AccordExecutor::new(node, fjall));
            temps.push(temp);
        }

        ShardExec {
            execs,
            _temps: temps,
            _loops: loops,
        }
    }

    async fn read_all(&self, node: u64, id: &str) -> Vec<Event> {
        let result = self.execs[&NodeId(node)]
            .read(
                Some(vec![EventFilter::by_id("test/Account", id)]),
                None,
                Args::forward(50, None),
            )
            .await
            .unwrap();
        result.edges.into_iter().map(|e| e.node).collect()
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
