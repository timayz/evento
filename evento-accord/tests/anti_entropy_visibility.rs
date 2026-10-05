//! Regression test for a stale-read window in anti-entropy repair.
//!
//! An anti-entropy round used to record an imported command as `Applied` in the
//! replica *before* writing its events to the local data store. `Applied` is the
//! promise the read barrier (`Replica::deps_applied`) and the execution gate
//! (`Replica::is_ready`) rely on, so in that window a linearizable read served
//! the local store without the events (a stale read), and a dependent write
//! could execute ahead of the import (a version gap the backend rejects — a
//! lasting divergence). Both surfaced as random failures of the
//! `linearizable_stress` test under churn.
//!
//! Here the lagging node's store parks the import's `apply` on a gate, so the
//! window is held open deterministically: while the events are not yet in the
//! store, a read barrier on that node must keep waiting rather than complete.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use evento_accord::{
    DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal,
    Key, MessageSink, Node, NodeConfig, NodeId, StaticTopology, Timestamp, TxnId,
};
use evento_core::Event;

/// An [`InMemoryDataStore`] whose `apply` can be parked: while `hold` is set,
/// an apply announces itself (`in_flight`) and waits for `release` before it
/// writes — the exact window between "the import has the command" and "its
/// events are in the store".
struct GatedStore {
    inner: InMemoryDataStore,
    hold: AtomicBool,
    in_flight: AtomicUsize,
    release: tokio::sync::watch::Sender<bool>,
}

impl GatedStore {
    fn new() -> Self {
        Self {
            inner: InMemoryDataStore::new(),
            hold: AtomicBool::new(false),
            in_flight: AtomicUsize::new(0),
            release: tokio::sync::watch::channel(false).0,
        }
    }

    fn open(&self) {
        self.hold.store(false, Ordering::SeqCst);
        self.release.send_replace(true);
    }
}

#[async_trait]
impl DataStore for GatedStore {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if self.hold.load(Ordering::SeqCst) {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let mut released = self.release.subscribe();
            while !*released.borrow_and_update() {
                released.changed().await.expect("gate dropped");
            }
        }
        self.inner.apply(txn, execute_at, events, commit).await
    }
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

async fn poll_until(what: &str, mut ready: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_barrier_waits_until_imported_events_are_in_the_store() {
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let config = NodeConfig {
        linearizable_reads: true,
        recovery_interval: Duration::from_millis(20),
        anti_entropy_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let gated = Arc::new(GatedStore::new());
    let mut nodes = Vec::new();
    let mut stores: Vec<Arc<InMemoryDataStore>> = Vec::new();
    let mut loops = Vec::new();
    for &id in &ids {
        let inbox = net.register(id);
        let clock = Arc::new(HybridLogicalClock::new(id));
        let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let topology = Arc::new(StaticTopology::new(id, ids.clone()));
        // Node 2 is the lagging replica whose repair we hold open.
        let datastore: Arc<dyn DataStore> = if id == NodeId(2) {
            gated.clone()
        } else {
            let store = Arc::new(InMemoryDataStore::new());
            stores.push(Arc::clone(&store));
            store
        };
        let node = Node::new(id, topology, clock, sink, datastore, journal).with_config(config);
        loops.push(node.start(inbox));
        loops.push(node.start_recovery());
        nodes.push(node);
    }

    // Node 2 misses a write entirely (crashed: nothing in or out), which the
    // survivors commit and apply.
    net.crash(NodeId(2));
    let outcome = nodes[0]
        .write(vec![event("acct", 1, "Opened")])
        .await
        .expect("write commits on the surviving quorum");
    assert!(!outcome.conflict);
    poll_until("survivors to apply the write", || {
        stores.iter().all(|s| s.applied_log().len() == 1)
    })
    .await;

    // Heal node 2 with its store gated: anti-entropy ships the missed command and
    // its apply parks — the command is known to the import, its events are not
    // yet in the store.
    gated.hold.store(true, Ordering::SeqCst);
    net.heal(NodeId(2));
    poll_until("anti-entropy to reach the gated apply", || {
        gated.in_flight.load(Ordering::SeqCst) >= 1
    })
    .await;
    assert!(
        gated.inner.applied_log().is_empty(),
        "the gate must hold the events out of the store"
    );

    // Oracle: a linearizable read on node 2 must not get past its barrier while
    // the write's events are not in its store. The barrier learns the write from
    // the survivors' probe replies and must wait for it locally — so it is still
    // waiting when this bounded wait expires. (The bug marked the command applied
    // before the store write, letting the barrier through to a stale read.)
    let key = Key("acct".to_string());
    let barrier = tokio::time::timeout(
        Duration::from_millis(500),
        nodes[2].read_barrier(key.clone()),
    )
    .await;
    assert!(
        barrier.is_err(),
        "read barrier completed while the imported events were not yet in the store: {barrier:?}"
    );
    assert!(gated.inner.applied_log().is_empty());

    // Release the gate: the import lands, and the barrier now completes with the
    // write visible locally.
    gated.open();
    tokio::time::timeout(Duration::from_secs(5), nodes[2].read_barrier(key))
        .await
        .expect("read barrier completes once the import landed")
        .expect("read barrier succeeds");
    assert_eq!(gated.inner.applied_log().len(), 1);
    assert_eq!(
        gated.inner.version("test/Account", "acct").await.unwrap(),
        1
    );

    for l in loops {
        l.abort();
    }
}
