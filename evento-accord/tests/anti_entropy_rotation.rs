//! Anti-entropy rotates over every peer. A node repairs from (and proves its
//! sync coverage with) whichever peer a round contacts, so a rotation stuck on
//! one peer leaves the node unable to repair — or to vouch for its window —
//! whenever that peer is down or slow.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Message, MessageSink, Node, NodeConfig, NodeId, StaticTopology, Timestamp,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anti_entropy_contacts_every_peer_in_turn() {
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
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
        // A round every sweep, answered promptly.
        anti_entropy_interval: Duration::from_millis(500),
        ..NodeConfig::default()
    });
    let _loop = node.start(inbox);

    // Both peers answer every sync request with an empty, in-sync state, and
    // record that they were asked.
    let asked: Arc<Mutex<Vec<NodeId>>> = Arc::new(Mutex::new(Vec::new()));
    let mut peers = Vec::new();
    for id in [NodeId(1), NodeId(2)] {
        let mut inbox = net.register(id);
        let sink = net.sink(id);
        let asked = Arc::clone(&asked);
        peers.push(tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                if let Message::SyncRequest { id: request, .. } = env.message {
                    asked.lock().unwrap().push(id);
                    let _ = sink
                        .send(
                            env.from,
                            Message::SyncData {
                                id: request,
                                watermark: Timestamp::MIN,
                                coverage: Timestamp::MIN,
                                snapshot: Vec::new(),
                                commands: Vec::new(),
                            },
                        )
                        .await;
                }
            }
        }));
    }

    for _ in 0..4 {
        node.sweep().await;
        // Past the anti-entropy pacing, so the next sweep runs a round too.
        tokio::time::sleep(Duration::from_millis(510)).await;
    }

    let asked = asked.lock().unwrap().clone();
    let distinct: HashSet<NodeId> = asked.iter().copied().collect();
    assert_eq!(asked.len(), 4, "one round per sweep");
    assert_eq!(
        distinct,
        HashSet::from([NodeId(1), NodeId(2)]),
        "rounds must rotate over every peer, not stick to one: {asked:?}"
    );
}
