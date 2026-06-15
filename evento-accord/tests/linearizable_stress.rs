//! Regression test for the high-contention stale-read anomaly Jepsen found (now
//! fixed in `Replica::import_applied` — a node that received a txn's Commit but
//! missed its Apply used to stay stuck at `Committed` forever, so anti-entropy
//! never converged it and reads served off it were stale).
//!
//! Writes only ever *append* (a key's event list only grows), so reads of one key
//! are **monotonic** under linearizability: if read A finishes (in real time)
//! before read B starts, B must observe a list at least as long as A's. A
//! violation is a stale read — a read that missed a write which had already been
//! observed (hence committed) before it began. This drives many concurrent
//! writers + readers across nodes with `linearizable_reads` on and checks that
//! oracle, recording enough detail to debug a failure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeConfig, NodeId, StaticTopology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor};
use evento_fjall::Fjall;
use tokio::time::Instant;
use ulid::Ulid;

type Cluster = (
    Vec<AccordExecutor<Fjall>>,
    Vec<Fjall>,
    Arc<InMemoryNetwork>,
    Vec<tempfile::TempDir>,
);

fn cluster(n: u64) -> Cluster {
    let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
    let net = InMemoryNetwork::new();
    let mut execs = Vec::new();
    let mut backends = Vec::new();
    let mut temps = Vec::new();
    for &id in &ids {
        let temp = tempfile::Builder::new().prefix("lin").tempdir().unwrap();
        let fjall = Fjall::open(temp.path()).unwrap();
        let inbox = net.register(id);
        let clock = Arc::new(HybridLogicalClock::new(id));
        let sink: Arc<dyn MessageSink> = Arc::new(net.sink(id));
        let datastore: Arc<dyn DataStore> = Arc::new(ExecutorDataStore::new(fjall.clone()));
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let topology = Arc::new(StaticTopology::new(id, ids.clone()));
        let node =
            Node::new(id, topology, clock, sink, datastore, journal).with_config(NodeConfig {
                linearizable_reads: true,
                // Short timeouts so a read/write that can't reach a quorum (its node is
                // crashed by churn) fails fast instead of blocking the whole test.
                collect_timeout: std::time::Duration::from_millis(200),
                ..Default::default()
            });
        node.start(inbox);
        node.start_recovery();
        execs.push(AccordExecutor::new(node, fjall.clone()));
        backends.push(fjall);
        temps.push(temp);
    }
    (execs, backends, net, temps)
}

/// Reads a key's value count directly from a node's local backend — bypassing the
/// read barrier — to inspect raw replicated state.
async fn local_len(backend: &Fjall, key: &str) -> usize {
    backend
        .read(
            Some(vec![EventFilter::by_id("lin/Reg", key)]),
            None,
            Args::forward(u16::MAX - 1, None),
        )
        .await
        .map(|r| r.edges.len())
        .unwrap_or(usize::MAX)
}

/// Decodes the `u64` values an aggregate's events carry, in version order.
/// `None` if the (linearizable) read was unavailable — a crashed node can't reach
/// a quorum, exactly like a Jepsen `:info`; such reads are excluded from the oracle.
async fn read_values(exec: &AccordExecutor<Fjall>, key: &str) -> Option<Vec<u64>> {
    let r = exec
        .read(
            Some(vec![EventFilter::by_id("lin/Reg", key)]),
            None,
            Args::forward(u16::MAX - 1, None),
        )
        .await
        .ok()?;
    let mut events: Vec<&Event> = r.edges.iter().map(|e| &e.node).collect();
    events.sort_by_key(|e| e.version);
    Some(
        events
            .iter()
            .map(|e| u64::from_le_bytes(e.data.clone().try_into().unwrap_or([0; 8])))
            .collect(),
    )
}

async fn read_len(exec: &AccordExecutor<Fjall>, key: &str) -> Option<usize> {
    exec.read(
        Some(vec![EventFilter::by_id("lin/Reg", key)]),
        None,
        Args::forward(u16::MAX - 1, None),
    )
    .await
    .ok()
    .map(|r| r.edges.len())
}

fn event(key: &str, version: u16, value: u64) -> Event {
    Event {
        id: Ulid::new(),
        aggregate_type: "lin/Reg".into(),
        aggregate_id: key.into(),
        version,
        name: "A".into(),
        data: value.to_le_bytes().to_vec(),
        timestamp: 1,
        timestamp_subsec: version as u32,
        ..Default::default()
    }
}

/// One recorded read: when it started/finished (real time), the key, the list
/// length, and the set of values it observed.
struct Obs {
    key: usize,
    start: Instant,
    end: Instant,
    values: Vec<u64>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizable_reads_hold_under_contention_and_churn() {
    const KEYS: usize = 4;
    const WRITERS: usize = 8;
    const READERS: usize = 8;
    const OPS: usize = 80;

    let (execs, backends, net, _temps) = cluster(5);
    let execs = Arc::new(execs);
    let value = Arc::new(AtomicU64::new(1));
    // value -> (key index, time the writing op started). A committed value's write
    // must have *started* before any read that observes it finishes.
    let started: Arc<Mutex<HashMap<u64, (usize, Instant)>>> = Arc::new(Mutex::new(HashMap::new()));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Churn: repeatedly crash then heal a minority node (3 or 4), so a quorum of
    // {0,1,2} always survives — mimicking the quorum-preserving `:one` partition.
    let churn = {
        let net = net.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut i = 0u64;
            while !done.load(Ordering::Relaxed) {
                let n = NodeId(3 + (i % 2));
                net.crash(n);
                tokio::time::sleep(std::time::Duration::from_millis(8)).await;
                net.heal(n);
                tokio::time::sleep(std::time::Duration::from_millis(8)).await;
                i += 1;
            }
        })
    };

    let mut writer_tasks = Vec::new();
    for w in 0..WRITERS {
        let execs = execs.clone();
        let value = value.clone();
        let started = started.clone();
        writer_tasks.push(tokio::spawn(async move {
            for i in 0..OPS {
                let k = (w + i) % KEYS;
                let key = format!("k{k}");
                let node = (w + i) % execs.len();
                let Some(cur) = read_len(&execs[node], &key).await else {
                    continue;
                };
                let v = value.fetch_add(1, Ordering::Relaxed);
                started.lock().unwrap().insert(v, (k, Instant::now()));
                let _ = execs[node]
                    .write(vec![event(&key, cur as u16 + 1, v)])
                    .await;
            }
        }));
    }

    let mut reader_tasks = Vec::new();
    for r in 0..READERS {
        let execs = execs.clone();
        reader_tasks.push(tokio::spawn(async move {
            let mut obs = Vec::new();
            for i in 0..OPS {
                let k = (r + i) % KEYS;
                let key = format!("k{k}");
                let node = (r + i) % execs.len();
                let start = Instant::now();
                let Some(values) = read_values(&execs[node], &key).await else {
                    continue;
                };
                let end = Instant::now();
                obs.push(Obs {
                    key: k,
                    start,
                    end,
                    values,
                });
            }
            obs
        }));
    }

    for t in writer_tasks {
        t.await.unwrap();
    }
    let mut all: Vec<Obs> = Vec::new();
    for t in reader_tasks {
        all.extend(t.await.unwrap());
    }
    done.store(true, Ordering::Relaxed);
    let _ = churn.await;

    // Convergence probe: with churn stopped, do all nodes hold the same committed
    // state per key? If yes, the stale reads were transient (a barrier-time
    // linearizability gap, not a durability/safety divergence).
    net.heal_all_partitions();
    for n in 0..5 {
        net.heal(NodeId(n));
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    for k in 0..KEYS {
        let key = format!("k{k}");
        let mut lens = Vec::new();
        for b in &backends {
            lens.push(local_len(b, &key).await);
        }
        eprintln!("converge k{k}: per-node local lens = {lens:?}");
    }

    let started = started.lock().unwrap();
    let mut stale = 0;
    let mut premature = 0;

    // Oracle 1 (stale): per key, read A ending before read B starts ⟹ B saw ≥ as
    // many values. Writes only append, so a shorter later read missed a write.
    for k in 0..KEYS {
        let mut reads: Vec<&Obs> = all.iter().filter(|o| o.key == k).collect();
        reads.sort_by_key(|o| o.start);
        for a in &reads {
            for b in &reads {
                if a.end < b.start && b.values.len() < a.values.len() {
                    stale += 1;
                    if stale <= 5 {
                        eprintln!(
                            "STALE k{k}: later read saw {} values; earlier read saw {}",
                            b.values.len(),
                            a.values.len()
                        );
                    }
                }
            }
        }
    }

    // Oracle 2 (premature): a read must not observe a value whose write *started*
    // after the read finished — that would be reading the future.
    for o in &all {
        for v in &o.values {
            if let Some(&(_, wstart)) = started.get(v) {
                if wstart > o.end {
                    premature += 1;
                    if premature <= 5 {
                        eprintln!(
                            "PREMATURE k{}: read saw value {v} whose write started {:?} after the read ended",
                            o.key,
                            wstart.duration_since(o.end)
                        );
                    }
                }
            }
        }
    }

    assert_eq!(
        (stale, premature),
        (0, 0),
        "{stale} stale + {premature} premature read violations under contention+churn"
    );
}
