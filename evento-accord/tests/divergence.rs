//! A replica whose anti-entropy contact compacted past what it vouched for
//! may no longer be repairable by anti-entropy: the window in between is
//! gone from every peer's commands. It is counted and logged, not told to
//! rejoin (#453): under a layout change a replica that copied its slots
//! holds everything below the contact's floor while its fence, raised by its
//! own reports alone, lies below it — the re-sharding swarm has 184 such
//! seeds in 24,000. A gap that is real surfaces at the next apply on the
//! aggregate (`StoreBehind`), which does ask for a rejoin.

use std::sync::Arc;

use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Message, MessageSink, Node, NodeId, StaticTopology, Timestamp,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_contact_truncated_above_the_vouch_is_counted_not_a_rejoin() {
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), ids)),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(net.sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    let _inbox = node.start(net.register(NodeId(0)));
    let _silent = net.register(NodeId(2));
    // Node 1 answers every sync request as a contact that compacted far
    // above anything node 0 can have vouched for.
    let mut inbox = net.register(NodeId(1));
    let sink = net.sink(NodeId(1));
    let _contact = tokio::spawn(async move {
        while let Some(env) = inbox.recv().await {
            if let Message::SyncRequest { id, .. } = env.message {
                let reply = Message::SyncData {
                    id,
                    watermark: Timestamp {
                        micros: u64::MAX / 2,
                        logical: 0,
                        node: NodeId(1),
                    },
                    coverage: Timestamp::MIN,
                    live_since: 0,
                    commands: Vec::new(),
                };
                let _ = sink.send(env.from, reply).await;
            }
        }
    });

    // The sweep's anti-entropy round asks node 1 first.
    node.sweep().await;
    assert_eq!(
        node.metrics().sync_contact_truncated,
        1,
        "the contact's floor above the vouch was not noticed"
    );
    assert!(
        !node.rejoin_required(),
        "a floor above the vouch alone had the node rejoin: a replica that copied its slots is in \
         that position legitimately"
    );
}
