//! Regression test for #582: a write a peer's recovery completed kept its
//! live coordinator waiting out `collect_timeout`.
//!
//! A replica acknowledges an apply to whoever committed the transaction
//! there last. A peer's sweep that takes a slow write over (it outlasted
//! `recovery_timeout`) commits it again, in its own name, so the
//! acknowledgements went to the recovery alone: the coordinator, alive and
//! in its apply round, stayed short of its quorum for the whole
//! `collect_timeout` — five seconds on a write the recovery had applied
//! everywhere.
//!
//! Here both peers lose the coordinator's `Apply`, so they apply the write
//! only once a peer's sweep recovered it: the coordinator must answer as
//! soon as the recovery did, not at the deadline.

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
async fn a_write_a_peer_recovered_is_acknowledged_to_its_coordinator() {
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let config = NodeConfig {
        collect_timeout: Duration::from_secs(5),
        recovery_interval: Duration::from_millis(20),
        // A peer's sweep takes the write over, long before the coordinator's
        // deadline; no import applies it meanwhile.
        recovery_timeout: Duration::from_millis(200),
        anti_entropy_interval: Duration::from_secs(60),
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
    // committed, and applies it only by the decision of a recovery.
    net.set_link_filter(Some(Arc::new(|from, to, message| {
        from == NodeId(0) && to != from && matches!(message, Message::Apply { .. })
    })));

    // Each node's first anti-entropy round runs at once, whatever the
    // interval: let it pass, so none imports the write.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let asked = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        nodes[0].write(vec![event("acct", 1)]),
    )
    .await
    .expect("the write is acknowledged once a peer recovered it, not at the deadline")
    .expect("the write commits");
    assert!(!outcome.conflict);
    assert!(
        asked.elapsed() < config.collect_timeout,
        "the write waited out the collect timeout"
    );
    // Acknowledged after a recovery: a peer took the write over, and at
    // least one peer's store holds it.
    assert!(nodes[1..].iter().any(|n| n.metrics().recoveries > 0));
    assert_eq!(stores[0].applied_log().len(), 1);
    assert!(stores[1..].iter().any(|s| s.applied_log().len() == 1));

    for l in loops {
        l.abort();
    }
}
