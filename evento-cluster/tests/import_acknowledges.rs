//! Regression test for #549: a write that waited out `collect_timeout` on a
//! group where every node answered.
//!
//! A replica applies a transaction two ways: its applier enacts the decision
//! the coordinator's `Apply` brought, and acknowledges it; or an anti-entropy
//! round imports it, applied, from a peer that got there first. The import
//! acknowledged nothing. So when two of three replicas imported a transaction
//! before their appliers reached it, its coordinator held one acknowledgement
//! of the two it needs and sat out the whole `collect_timeout` — five seconds
//! on a write whose every replica had applied it within milliseconds.
//!
//! Here both peers lose the coordinator's `Apply`, so the import is the only
//! way they apply the write: it must come back as fast as anti-entropy runs,
//! not at the deadline.

use std::sync::Arc;
use std::time::{Duration, Instant};

use evento_cluster::protocol::Message;
use evento_cluster::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::Event;

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_its_replicas_imported_is_acknowledged() {
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(5),
        recovery_interval: Duration::from_millis(20),
        anti_entropy_interval: Duration::from_millis(50),
        // No peer takes the write over meanwhile: its coordinator is alive,
        // and only the import is to apply it on them.
        recovery_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let mut nodes = Vec::new();
    let mut stores: Vec<Arc<InMemoryDataStore>> = Vec::new();
    let mut loops = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let clock = Arc::new(HybridLogicalClock::new(id));
        let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let topology = Arc::new(StaticTopology::new(id, ids.clone()));
        let store = Arc::new(InMemoryDataStore::new());
        stores.push(Arc::clone(&store));
        let datastore: Arc<dyn DataStore> = store;
        let node = Node::new(id, topology, clock, sink, datastore, journal).with_config(config);
        loops.push(node.start(inbox));
        loops.push(node.start_recovery());
        nodes.push(node);
    }

    // The coordinator's decision reaches no peer: each holds the write
    // committed, and applies it only by importing it from node 0.
    net.set_link_filter(Some(Arc::new(|from, to, message| {
        from != to && matches!(message, Message::Apply { .. })
    })));

    let asked = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        nodes[0].write(vec![event("acct", 1)]),
    )
    .await
    .expect("the write is acknowledged once its replicas imported it, not at the deadline")
    .expect("the write commits");
    assert!(!outcome.conflict);
    assert!(
        asked.elapsed() < config.collect_timeout,
        "the write waited out the collect timeout"
    );
    // Acknowledged by an import: at least one peer's store holds the write.
    assert_eq!(stores[0].applied_log().len(), 1);
    assert!(stores[1..].iter().any(|s| s.applied_log().len() == 1));

    for l in loops {
        l.abort();
    }
}
