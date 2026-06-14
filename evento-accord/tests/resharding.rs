//! M4 — range movement: a key's ownership moves to a new shard, and that shard's
//! new nodes take over its state.
//!
//! Starting from a single shard, the cluster re-shards into two (the config
//! change goes through the same Paxos as join/leave). Keys re-hash; the ones now
//! owned by the new shard are bootstrapped onto its nodes (each imports *only* its
//! own range), after which writes to a moved key route to — and are served by —
//! its new owners. Garbage-collecting a moved range from its old owners is future
//! work; the old replicas simply stop being contacted for it.

use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, DynamicTopology, HybridLogicalClock, InMemoryDataStore, InMemoryJournal,
    InMemoryNetwork, Journal, Key, MessageSink, Node, NodeId, Topology,
};
use evento_core::Event;

struct TestNode {
    node: Node,
    store: Arc<InMemoryDataStore>,
    topology: Arc<DynamicTopology>,
    _loop: tokio::task::JoinHandle<()>,
}

fn spawn_node(id: NodeId, shards: Vec<Vec<NodeId>>, net: &Arc<InMemoryNetwork>) -> TestNode {
    let inbox = net.register(id);
    let clock = Arc::new(HybridLogicalClock::new(id));
    let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
    let store = Arc::new(InMemoryDataStore::new());
    let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
    let topology = Arc::new(DynamicTopology::new(id, 0, shards));
    let node = Node::new(
        id,
        topology.clone() as Arc<dyn Topology>,
        clock,
        sink,
        Arc::clone(&store) as Arc<dyn DataStore>,
        journal,
    );
    let _loop = node.start(inbox);
    TestNode {
        node,
        store,
        topology,
        _loop,
    }
}

fn event(aggregator_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregator_type: "test/Account".into(),
        aggregator_id: aggregator_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

async fn await_len(store: &InMemoryDataStore, len: usize) {
    for _ in 0..1000 {
        if store.applied_log().len() >= len {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("store did not reach {len} applied entries");
}

async fn await_epoch(topology: &DynamicTopology, epoch: u64) {
    for _ in 0..1000 {
        if topology.epoch() >= epoch {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("topology did not reach epoch {epoch}");
}

/// An aggregate id that hashes into `shard` under the given (multi-shard) layout.
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
async fn a_key_range_moves_to_a_new_shard() {
    let net = InMemoryNetwork::new();
    let abc = vec![NodeId(0), NodeId(1), NodeId(2)];
    let def = vec![NodeId(3), NodeId(4), NodeId(5)];
    let two_shards = vec![abc.clone(), def.clone()];

    // Pick a key that stays in shard 0 and one that moves to shard 1.
    let stay = id_in_shard(&two_shards, 0, "stay");
    let moved = id_in_shard(&two_shards, 1, "move");

    // Epoch 0: a single shard {A, B, C} owns everything; write both keys.
    let founders: Vec<TestNode> = abc
        .iter()
        .map(|&id| spawn_node(id, vec![abc.clone()], &net))
        .collect();
    founders[0]
        .node
        .write(vec![event(&stay, 1, "Opened")])
        .await
        .unwrap();
    founders[0]
        .node
        .write(vec![event(&moved, 1, "Opened")])
        .await
        .unwrap();
    for n in &founders {
        await_len(&n.store, 2).await;
    }

    // The new shard's nodes come up and begin buffering.
    let newcomers: Vec<TestNode> = def
        .iter()
        .map(|&id| spawn_node(id, vec![abc.clone()], &net))
        .collect();
    for n in &newcomers {
        n.node.begin_join();
    }

    // Re-shard into two via the config Paxos, and let it propagate.
    founders[0]
        .node
        .change_topology(1, two_shards.clone())
        .await
        .unwrap();
    for n in founders.iter().chain(newcomers.iter()) {
        await_epoch(&n.topology, 1).await;
    }

    // Each new node bootstraps from A — importing only the moved range (1 key),
    // not the key that stayed behind.
    for n in &newcomers {
        let imported = n.node.join(NodeId(0)).await.unwrap();
        assert_eq!(imported, 1, "new shard imports only the moved key");
    }

    // A write to the moved key now routes to and is served by the new shard.
    let outcome = newcomers[0]
        .node
        .write(vec![event(&moved, 2, "Deposited")])
        .await
        .unwrap();
    assert!(!outcome.conflict);
    for n in &newcomers {
        await_len(&n.store, 2).await; // bootstrapped v1 + new v2
    }

    // The stayed key still works on the original shard.
    let stayed = founders[0]
        .node
        .write(vec![event(&stay, 2, "Deposited")])
        .await
        .unwrap();
    assert!(!stayed.conflict);

    // The moved key's *version* state came with it: a stale v2 write is rejected
    // by the new owners.
    let stale = newcomers[1]
        .node
        .write(vec![event(&moved, 2, "Again")])
        .await
        .unwrap();
    assert!(stale.conflict, "the moved key's version state followed it");
}
