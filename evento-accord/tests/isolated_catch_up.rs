//! A node cut off from the cluster while writes continue catches up once the
//! partition heals, even under steady load (#291).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Key, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::Event;

const KEYS: u64 = 4;

fn event(key: u64, version: u16) -> Event {
    Event {
        aggregate_type: "test/Reg".into(),
        aggregate_id: key.to_string(),
        version,
        name: "Appended".into(),
        ..Default::default()
    }
}

/// Writes to `node` until `stop`, cycling over the keys; each write's version
/// is read from the node's own store, so most commit and some conflict.
fn load(
    node: Node,
    store: Arc<InMemoryDataStore>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicU64>,
    seed: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut i = seed;
        while !stop.load(Ordering::Relaxed) {
            i += 1;
            let key = i % KEYS;
            let version = store
                .version("", "test/Reg", &key.to_string())
                .await
                .unwrap_or(0);
            let write = tokio::time::timeout(
                Duration::from_secs(5),
                node.write(vec![event(key, version + 1)]),
            );
            if let Ok(Ok(_)) = write.await {
                done.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_node_catches_up_under_load_after_the_heal() {
    let ids: Vec<NodeId> = (0..5).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    // Jepsen's node settings: defaults, with linearizable reads.
    let config = NodeConfig {
        linearizable_reads: true,
        ..Default::default()
    };

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
        )
        .with_config(config);
        loops.push(node.start(inbox));
        loops.push(node.start_recovery());
        nodes.push(node);
        stores.push(store);
    }

    // Two writers per node, like Jepsen's clients.
    let stop = Arc::new(AtomicBool::new(false));
    let done: Vec<Arc<AtomicU64>> = ids.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut writers = Vec::new();
    for (n, node) in nodes.iter().enumerate() {
        for w in 0..2 {
            writers.push(load(
                node.clone(),
                Arc::clone(&stores[n]),
                Arc::clone(&stop),
                Arc::clone(&done[n]),
                (n * 2 + w) as u64,
            ));
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    // Cut node 4 off while every node keeps writing: it misses the survivors'
    // commits, and its own writes are left behind un-committed.
    net.crash(NodeId(4));
    tokio::time::sleep(Duration::from_secs(3)).await;
    net.heal(NodeId(4));
    let healed = Instant::now();
    let committed = |n: usize| done[n].load(Ordering::Relaxed);
    let before: Vec<u64> = (0..ids.len()).map(committed).collect();

    // Under continued load, node 4 must serve again: a linearizable read
    // barrier completes, and its own writes commit.
    let deadline = healed + Duration::from_secs(20);
    let mut barrier_at = None;
    while Instant::now() < deadline {
        let barrier = tokio::time::timeout(
            Duration::from_secs(5),
            nodes[4].read_barrier(Key("0".into())),
        )
        .await;
        if let Ok(Ok(())) = barrier {
            barrier_at = Some(healed.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    while Instant::now() < deadline && committed(4) < before[4] + 10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let writes_at = healed.elapsed();
    let since: Vec<u64> = (0..ids.len()).map(|n| committed(n) - before[n]).collect();

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        let _ = tokio::time::timeout(Duration::from_secs(6), w).await;
    }
    for l in loops {
        l.abort();
    }
    eprintln!(
        "after the heal: barrier served after {barrier_at:?}; writes committed by \
         {writes_at:?}, per node {since:?}"
    );
    let barrier_at = barrier_at.expect("node 4 never served a read barrier after the heal");
    assert!(
        since[4] >= 10,
        "node 4 committed only {} writes in {writes_at:?} after the heal \
         (read barrier served after {barrier_at:?})",
        since[4]
    );
    for (n, &count) in since.iter().enumerate().take(4) {
        assert!(count > 0, "node {n} stopped committing after the heal");
    }
}
