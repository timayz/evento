//! Storage-fault coverage for the durability path: a node whose journal cannot
//! `flush` (fsync) must **withhold its ack**, so a write is reported committed only
//! if a *durable* quorum persisted it. Modelled with `FaultJournal` — a staged record
//! becomes durable only after a successful flush, so an un-flushed record is exactly
//! the data a crash would lose.
//!
//! This is the invariant behind the flush-failure gate: **acked ⇒ durable on f+1
//! nodes**. Without the gate, replicas would ack despite a failed fsync and a write
//! could be "committed" while persisted on fewer than a quorum — lost if those nodes
//! restart. The test asserts the invariant directly (no restart needed): if a write
//! returns committed, at least a slow quorum holds it durably.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use evento_accord::{
    AcceptorRecord, CommandState, DataStore, HybridLogicalClock, InMemoryDataStore,
    InMemoryJournal, InMemoryNetwork, Journal, MessageSink, Node, NodeId, StaticTopology,
    Timestamp, Topology, TxnId,
};
use evento_core::Event;

/// A journal whose `flush` (fsync) can be made to fail. A `stage`d record is held in
/// a pending buffer and only becomes **durable** (loadable, survives a restart) once
/// a flush succeeds; while `failing`, flush errors and the pending records stay
/// non-durable — exactly what a disk failure / crash would lose.
struct FaultJournal {
    durable: InMemoryJournal,
    pending: Mutex<Vec<CommandState>>,
    failing: AtomicBool,
}

impl FaultJournal {
    fn new() -> Self {
        Self {
            durable: InMemoryJournal::new(),
            pending: Mutex::new(Vec::new()),
            failing: AtomicBool::new(false),
        }
    }

    fn set_failing(&self, v: bool) {
        self.failing.store(v, Ordering::SeqCst);
    }

    /// Whether `txn` is **durably** recorded (survived a flush) — the real
    /// post-crash state, ignoring any non-durable pending writes.
    async fn durable_has(&self, txn: TxnId) -> bool {
        self.durable.load(txn).await.unwrap().is_some()
    }
}

#[async_trait]
impl Journal for FaultJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage(state).await?;
        self.flush().await
    }
    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        self.pending.lock().unwrap().push(state.clone());
        Ok(())
    }
    async fn flush(&self) -> anyhow::Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            anyhow::bail!("simulated fsync failure");
        }
        let batch = std::mem::take(&mut *self.pending.lock().unwrap());
        for state in batch {
            self.durable.record(&state).await?;
        }
        Ok(())
    }
    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        self.durable.load(txn).await
    }
    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        self.durable.load_all().await
    }
    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        self.durable.truncate(before).await
    }
    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        self.durable.load_watermark().await
    }
    async fn append_metadata(&self, epoch: u64, layout: &[Vec<NodeId>]) -> anyhow::Result<()> {
        self.durable.append_metadata(epoch, layout).await
    }
    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
        self.durable.load_metadata().await
    }
    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        self.durable.record_acceptor(epoch, state).await
    }
    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        self.durable.load_acceptors().await
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

struct Cluster {
    nodes: Vec<Node>,
    journals: Vec<Arc<FaultJournal>>,
    stores: Vec<Arc<InMemoryDataStore>>,
    _loops: Vec<tokio::task::JoinHandle<()>>,
}

fn start(n: u64) -> Cluster {
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let (mut nodes, mut journals, mut stores, mut loops) = (vec![], vec![], vec![], vec![]);
    for &id in &ids {
        let inbox = net.register(id);
        let journal = Arc::new(FaultJournal::new());
        let store = Arc::new(InMemoryDataStore::new());
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::clone(&store) as Arc<dyn DataStore>,
            Arc::clone(&journal) as Arc<dyn Journal>,
        );
        loops.push(node.start(inbox));
        loops.push(node.start_recovery());
        nodes.push(node);
        journals.push(journal);
        stores.push(store);
    }
    Cluster {
        nodes,
        journals,
        stores,
        _loops: loops,
    }
}

async fn await_version(store: &InMemoryDataStore, agg: &str, want: u16) {
    for _ in 0..500 {
        if store.version("test/Account", agg).await.unwrap_or(0) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("store did not reach version {want}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_write_is_always_durable_on_a_quorum() {
    // N=3 (f=1), so a slow quorum is 2.
    let cluster = start(3);
    let slow_quorum = 2;

    // Sanity (positive control): with healthy journals a write commits and is durable
    // on all three nodes.
    let outcome = cluster.nodes[0]
        .write(vec![event("acc", 1, "Opened")])
        .await
        .expect("healthy write commits");
    assert!(!outcome.conflict);
    for n in &cluster.stores {
        await_version(n, "acc", 1).await;
    }
    let durable = count_durable(&cluster.journals, outcome.txn).await;
    assert_eq!(durable, 3, "a healthy commit is durable everywhere");

    // Now fail fsync on a quorum (nodes 0 and 1 — node 0 is also the coordinator).
    cluster.journals[0].set_failing(true);
    cluster.journals[1].set_failing(true);

    // Attempt a write. With the durability gate, nodes 0 and 1 withhold their acks
    // (they could not persist), so the coordinator cannot reach a durable quorum and
    // the write must NOT report a durable commit.
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        cluster.nodes[0].write(vec![event("acc", 2, "Deposited")]),
    )
    .await
    .expect("write did not hang");

    if let Ok(outcome) = result {
        if !outcome.conflict {
            // If the cluster *did* report this write committed, the invariant is that
            // a slow quorum must hold it durably — otherwise a restart of the faulted
            // nodes would lose an acked write.
            let durable = count_durable(&cluster.journals, outcome.txn).await;
            assert!(
                durable >= slow_quorum,
                "acked write durable on only {durable} node(s) (< quorum {slow_quorum}) \
                 — a flush-failing node acked a write it did not persist"
            );
        }
    }
    // (A write that fails to commit under a flush-failing quorum is the correct,
    // safe outcome — the node refused to ack what it could not persist.)
}

async fn count_durable(journals: &[Arc<FaultJournal>], txn: TxnId) -> usize {
    let mut n = 0;
    for j in journals {
        if j.durable_has(txn).await {
            n += 1;
        }
    }
    n
}
