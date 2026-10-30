//! Whole-group restart soak (#452): three Accord nodes over real TCP, each
//! on one SQLite file holding its store and its journal (as timada runs
//! them: WAL, `synchronous = FULL`, one writer connection), under a steady
//! write load with read barriers; then every node is stopped and reopened
//! over its files, and the load goes on. Prints the write latency per
//! second before and after the restart, and the errors.
//!
//! Every journal, store and transport call slower than 50 ms is logged at
//! `WARN` (`RUST_LOG=restart_soak=warn`), with the node and what it was:
//! on a disk whose fsync stalls, that is where a write's seconds go.
//!
//! ```text
//! cargo run --release -p restart-soak
//! RUST_LOG=restart_soak=warn,evento_accord=debug cargo run --release -p restart-soak 2> soak.log
//! ```
//!
//! Env: `SOAK_DIR` (`/tmp/restart-soak`; emptied first unless `FRESH=0`),
//! `PORT_BASE` (17000), `WRITERS` (4), `INTERVAL_MS` (500, per writer),
//! `AGGREGATES` (8), `BEFORE_SECS` (20), `AFTER_SECS` (120), `COORD` (2:
//! the node every writer writes through, or `any`), `BARRIERS` (1: a read
//! barrier before each write, as timada's pages do), `RESTART` (1),
//! `SYNC` (`full`, or `normal` for `synchronous = NORMAL`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use evento::Event;
use evento_accord::{
    serve, DataStore, ExecutorDataStore, HybridLogicalClock, Journal, Key, MessageSink, Node,
    NodeConfig, NodeId, StaticTopology, TcpTransport, Topology,
};
use evento_sql::{RwSqlite, Sql, SqlJournal};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use sqlx_migrator::{Migrate, Plan};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// ----- timing wrappers: log any store, journal or transport call over SLOW ---

const SLOW: Duration = Duration::from_millis(50);

macro_rules! timed {
    ($node:expr, $what:expr, $fut:expr) => {{
        let started = Instant::now();
        let out = $fut.await;
        let took = started.elapsed();
        if took >= SLOW {
            tracing::warn!(
                node = $node,
                what = $what,
                took_ms = took.as_millis() as u64,
                "slow call"
            );
        }
        out
    }};
}

struct TimedJournal<J> {
    node: u64,
    inner: J,
}

#[async_trait::async_trait]
impl<J: Journal> Journal for TimedJournal<J> {
    async fn record(&self, state: &evento_accord::CommandState) -> anyhow::Result<()> {
        timed!(self.node, "journal.record", self.inner.record(state))
    }
    async fn stage(&self, state: &evento_accord::CommandState) -> anyhow::Result<()> {
        timed!(self.node, "journal.stage", self.inner.stage(state))
    }
    async fn flush(&self) -> anyhow::Result<()> {
        timed!(self.node, "journal.flush", self.inner.flush())
    }
    async fn truncate(&self, before: evento_accord::Timestamp) -> anyhow::Result<()> {
        timed!(self.node, "journal.truncate", self.inner.truncate(before))
    }
    async fn load_watermark(&self) -> anyhow::Result<Option<evento_accord::Timestamp>> {
        self.inner.load_watermark().await
    }
    async fn save_rejoin_bounds(
        &self,
        bounds: &[(NodeId, evento_accord::Timestamp)],
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_rejoin_bounds",
            self.inner.save_rejoin_bounds(bounds)
        )
    }
    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, evento_accord::Timestamp)>> {
        self.inner.load_rejoin_bounds().await
    }
    async fn save_route(&self, txn: evento_accord::TxnId, route: &[Key]) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_route",
            self.inner.save_route(txn, route)
        )
    }
    async fn load_routes(&self) -> anyhow::Result<Vec<(evento_accord::TxnId, Vec<Key>)>> {
        self.inner.load_routes().await
    }
    async fn save_promise(
        &self,
        txn: evento_accord::TxnId,
        ballot: evento_accord::Ballot,
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_promise",
            self.inner.save_promise(txn, ballot)
        )
    }
    async fn load_promises(
        &self,
    ) -> anyhow::Result<Vec<(evento_accord::TxnId, evento_accord::Ballot)>> {
        self.inner.load_promises().await
    }
    async fn save_decision(&self, record: &evento_accord::DecisionRecord) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_decision",
            self.inner.save_decision(record)
        )
    }
    async fn load_decisions(&self) -> anyhow::Result<Vec<evento_accord::DecisionRecord>> {
        self.inner.load_decisions().await
    }
    async fn drop_decisions(&self, txns: &[evento_accord::TxnId]) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.drop_decisions",
            self.inner.drop_decisions(txns)
        )
    }
    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_clock_bound",
            self.inner.save_clock_bound(micros)
        )
    }
    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        self.inner.load_clock_bound().await
    }
    async fn save_born(&self, micros: u64) -> anyhow::Result<()> {
        timed!(self.node, "journal.save_born", self.inner.save_born(micros))
    }
    async fn load_born(&self) -> anyhow::Result<Option<u64>> {
        self.inner.load_born().await
    }
    async fn save_vote_fence(&self, fence: evento_accord::Timestamp) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_vote_fence",
            self.inner.save_vote_fence(fence)
        )
    }
    async fn load_vote_fence(&self) -> anyhow::Result<Option<evento_accord::Timestamp>> {
        self.inner.load_vote_fence().await
    }
    async fn save_peer_marks(
        &self,
        marks: &[(NodeId, evento_accord::DurableMark)],
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_peer_marks",
            self.inner.save_peer_marks(marks)
        )
    }
    async fn load_peer_marks(&self) -> anyhow::Result<Vec<(NodeId, evento_accord::DurableMark)>> {
        self.inner.load_peer_marks().await
    }
    async fn save_copy_state(&self, state: &evento_accord::CopyState) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_copy_state",
            self.inner.save_copy_state(state)
        )
    }
    async fn load_copy_state(&self) -> anyhow::Result<Option<evento_accord::CopyState>> {
        self.inner.load_copy_state().await
    }
    async fn save_slot_gc(&self, epoch: u64) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_slot_gc",
            self.inner.save_slot_gc(epoch)
        )
    }
    async fn load_slot_gc(&self) -> anyhow::Result<Option<u64>> {
        self.inner.load_slot_gc().await
    }
    async fn save_amnesia_fence(&self, fence: &evento_accord::AmnesiaFence) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_amnesia_fence",
            self.inner.save_amnesia_fence(fence)
        )
    }
    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<evento_accord::AmnesiaFence>> {
        self.inner.load_amnesia_fence().await
    }
    async fn load(
        &self,
        txn: evento_accord::TxnId,
    ) -> anyhow::Result<Option<evento_accord::CommandState>> {
        timed!(self.node, "journal.load", self.inner.load(txn))
    }
    async fn load_all(&self) -> anyhow::Result<Vec<evento_accord::CommandState>> {
        self.inner.load_all().await
    }
    async fn append_metadata(
        &self,
        epoch: u64,
        entry: &evento_accord::ConfigEntry,
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.append_metadata",
            self.inner.append_metadata(epoch, entry)
        )
    }
    async fn append_metadata_batch(
        &self,
        entries: &[(u64, evento_accord::ConfigEntry)],
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.append_metadata_batch",
            self.inner.append_metadata_batch(entries)
        )
    }
    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, evento_accord::ConfigEntry)>> {
        self.inner.load_metadata().await
    }
    async fn save_fences(&self, fences: &[evento_accord::FenceRecord]) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.save_fences",
            self.inner.save_fences(fences)
        )
    }
    async fn load_fences(&self) -> anyhow::Result<Vec<evento_accord::FenceRecord>> {
        self.inner.load_fences().await
    }
    async fn record_acceptor(
        &self,
        epoch: u64,
        state: &evento_accord::AcceptorRecord,
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "journal.record_acceptor",
            self.inner.record_acceptor(epoch, state)
        )
    }
    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, evento_accord::AcceptorRecord)>> {
        self.inner.load_acceptors().await
    }
}

struct TimedStore<S> {
    node: u64,
    inner: S,
}

#[async_trait::async_trait]
impl<S: DataStore> DataStore for TimedStore<S> {
    async fn version(
        &self,
        tenant: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<u16> {
        timed!(
            self.node,
            "store.version",
            self.inner.version(tenant, aggregate_type, aggregate_id)
        )
    }
    async fn apply(
        &self,
        txn: evento_accord::TxnId,
        execute_at: evento_accord::Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "store.apply",
            self.inner.apply(txn, execute_at, events, commit)
        )
    }
    async fn holds(&self, events: &[Event]) -> anyhow::Result<bool> {
        timed!(self.node, "store.holds", self.inner.holds(events))
    }
    async fn read(
        &self,
        aggregators: Option<Arc<[evento::EventFilter]>>,
        routing_key: Option<evento::RoutingKey>,
        tenant: evento::TenantFilter,
        args: evento::cursor::Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<evento::cursor::ReadResult<Event>> {
        timed!(
            self.node,
            "store.read",
            self.inner
                .read(aggregators, routing_key, tenant, args, to_micros)
        )
    }
    async fn snapshot_page(
        &self,
        after: Option<evento::cursor::Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<evento_accord::SnapshotPage> {
        timed!(
            self.node,
            "store.snapshot_page",
            self.inner.snapshot_page(after, max_events, max_bytes)
        )
    }
    async fn snapshot_page_since(
        &self,
        after: Option<evento::cursor::Value>,
        since: Option<u64>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<evento_accord::SnapshotPage> {
        timed!(
            self.node,
            "store.snapshot_page_since",
            self.inner
                .snapshot_page_since(after, since, max_events, max_bytes)
        )
    }
    async fn drop_slots(&self, slots: &[u16]) -> anyhow::Result<usize> {
        timed!(self.node, "store.drop_slots", self.inner.drop_slots(slots))
    }
}

struct TimedSink<S> {
    node: u64,
    inner: S,
}

#[async_trait::async_trait]
impl<S: MessageSink> MessageSink for TimedSink<S> {
    async fn send(&self, to: NodeId, message: evento_accord::Message) -> anyhow::Result<()> {
        timed!(self.node, "sink.send", self.inner.send(to, message))
    }
    async fn broadcast(
        &self,
        nodes: &[NodeId],
        message: evento_accord::Message,
    ) -> anyhow::Result<()> {
        timed!(
            self.node,
            "sink.broadcast",
            self.inner.broadcast(nodes, message)
        )
    }
}

fn env_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Opened {
    node: Node,
    rw: RwSqlite,
    pools: Vec<SqlitePool>,
    tasks: Vec<JoinHandle<()>>,
}

async fn pool(path: &Path, writer: bool) -> anyhow::Result<SqlitePool> {
    let mut opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(5));
    if writer {
        opts = opts.journal_mode(SqliteJournalMode::Wal).synchronous(
            match std::env::var("SYNC").as_deref() {
                Ok("normal") => SqliteSynchronous::Normal,
                _ => SqliteSynchronous::Full,
            },
        );
    }
    let pool = SqlitePoolOptions::new()
        .max_connections(if writer { 1 } else { 4 })
        .connect_with(opts)
        .await?;
    Ok(pool)
}

async fn migrate(path: &Path) -> anyhow::Result<()> {
    let pool = pool(path, true).await?;
    let mut conn = pool.acquire().await?;
    let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    drop(conn);
    pool.close().await;
    Ok(())
}

async fn open(
    id: NodeId,
    ids: &[NodeId],
    peers: &HashMap<NodeId, SocketAddr>,
    path: &Path,
    config: NodeConfig,
) -> anyhow::Result<Opened> {
    let write_pool = pool(path, true).await?;
    let read_pool = pool(path, false).await?;
    let rw: RwSqlite = (
        Sql::<sqlx::Sqlite>::from(read_pool.clone()),
        Sql::<sqlx::Sqlite>::from(write_pool.clone()),
    )
        .into();
    let store = Arc::new(TimedStore {
        node: id.0,
        inner: ExecutorDataStore::new(rw.clone()),
    }) as Arc<dyn DataStore>;
    let journal = Arc::new(TimedJournal {
        node: id.0,
        inner: SqlJournal::<sqlx::Sqlite>::new(write_pool.clone()),
    }) as Arc<dyn Journal>;
    let topology = Arc::new(StaticTopology::new(id, ids.to_vec())) as Arc<dyn Topology>;
    let listener = tokio::net::TcpListener::bind(peers[&id]).await?;
    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    let mut tasks = vec![serve(listener, inbox_tx)];
    let sink = Arc::new(TimedSink {
        node: id.0,
        inner: TcpTransport::new(id, peers.clone()),
    }) as Arc<dyn MessageSink>;
    let node = Node::new(
        id,
        topology,
        Arc::new(HybridLogicalClock::new(id)),
        sink,
        store,
        journal,
    )
    .with_config(config);
    node.recover_state().await?;
    tasks.push(node.start(inbox_rx));
    tasks.push(node.start_recovery());
    Ok(Opened {
        node,
        rw,
        pools: vec![write_pool, read_pool],
        tasks,
    })
}

async fn close(opened: Opened) {
    opened.node.retire().await;
    for task in &opened.tasks {
        task.abort();
    }
    for task in opened.tasks {
        let _ = task.await;
    }
    drop(opened.node);
    drop(opened.rw);
    for pool in opened.pools {
        pool.close().await;
    }
}

async fn until_confirmed(nodes: &[Opened]) {
    let started = Instant::now();
    loop {
        if nodes.iter().all(|n| n.node.confirmed()) {
            println!("# all confirmed after {} ms", started.elapsed().as_millis());
            return;
        }
        if started.elapsed() > Duration::from_secs(60) {
            panic!("nodes not confirmed within 60 s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One second of a phase: the latencies of the writes that ended in it,
/// in milliseconds, and the errors.
#[derive(Default)]
struct Second {
    latencies: Vec<f64>,
    errors: Vec<String>,
}

#[derive(Default)]
struct Stats {
    // second since phase start -> what ended in it
    buckets: Mutex<HashMap<u64, Second>>,
    writes: AtomicU64,
}

impl Stats {
    fn record(&self, since: &Instant, latency: Duration, err: Option<String>) {
        let s = since.elapsed().as_secs();
        let mut b = self.buckets.lock().unwrap();
        let e = b.entry(s).or_default();
        match err {
            None => e.latencies.push(latency.as_secs_f64() * 1000.0),
            Some(err) => e.errors.push(err),
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self, label: &str) {
        let b = self.buckets.lock().unwrap();
        let mut keys: Vec<&u64> = b.keys().collect();
        keys.sort();
        println!("## {label}");
        let mut errors: HashMap<String, usize> = HashMap::new();
        for k in keys {
            let Second {
                latencies,
                errors: errs,
            } = &b[k];
            let mut l = latencies.clone();
            l.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p50 = l.get(l.len() / 2).copied().unwrap_or(0.0);
            let max = l.last().copied().unwrap_or(0.0);
            for e in errs {
                *errors.entry(e.clone()).or_default() += 1;
            }
            println!(
                "{k:4}s ok={:3} err={:2} p50={p50:8.1}ms max={max:8.1}ms",
                l.len(),
                errs.len()
            );
        }
        for (e, n) in errors {
            println!("error x{n}: {e}");
        }
    }
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "soak/Cart".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

fn spawn_load(
    nodes: &[Opened],
    stats: Arc<Stats>,
    writers: usize,
    interval: Duration,
    aggregates: usize,
    coord: Option<usize>,
    barriers: bool,
) -> Vec<JoinHandle<()>> {
    let since = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..writers {
        let node_ix = coord.unwrap_or(w % nodes.len());
        let node = nodes[node_ix].node.clone();
        let rw = nodes[node_ix].rw.clone();
        let stats = Arc::clone(&stats);
        tasks.push(tokio::spawn(async move {
            let store = ExecutorDataStore::new(rw);
            let mut i = w;
            loop {
                let agg = format!("cart-{}", i % aggregates);
                i += writers;
                let started = Instant::now();
                let v = match store.version("", "soak/Cart", &agg).await {
                    Ok(v) => v,
                    Err(err) => {
                        stats.record(&since, started.elapsed(), Some(format!("version: {err:#}")));
                        tokio::time::sleep(interval).await;
                        continue;
                    }
                };
                let ev = event(&agg, v + 1);
                if barriers {
                    if let Err(err) = node.read_barrier(Key::of(&ev)).await {
                        stats.record(&since, started.elapsed(), Some(format!("barrier: {err:#}")));
                    }
                }
                let outcome = node.write(vec![ev]).await;
                match outcome {
                    Ok(_) => stats.record(&since, started.elapsed(), None),
                    Err(err) => stats.record(&since, started.elapsed(), Some(format!("{err:#}"))),
                }
                tokio::time::sleep(interval).await;
            }
        }));
    }
    tasks
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let dir: PathBuf = env_or("SOAK_DIR", String::from("/tmp/restart-soak")).into();
    let port_base: u16 = env_or("PORT_BASE", 17000);
    let writers: usize = env_or("WRITERS", 4);
    let interval = Duration::from_millis(env_or("INTERVAL_MS", 500));
    let aggregates: usize = env_or("AGGREGATES", 8);
    let before = Duration::from_secs(env_or("BEFORE_SECS", 20));
    let after = Duration::from_secs(env_or("AFTER_SECS", 120));
    let coord: Option<usize> = std::env::var("COORD")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(Some(2))
        .filter(|_| std::env::var("COORD").map(|v| v != "any").unwrap_or(true));
    let barriers = env_or::<u8>("BARRIERS", 1) == 1;
    let restart = env_or::<u8>("RESTART", 1) == 1;
    let fresh = env_or::<u8>("FRESH", 1) == 1;

    if fresh && dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    let ids: Vec<NodeId> = (0..3).map(NodeId).collect();
    let peers: HashMap<NodeId, SocketAddr> = ids
        .iter()
        .map(|id| {
            (
                *id,
                format!("127.0.0.1:{}", port_base + id.0 as u16)
                    .parse()
                    .unwrap(),
            )
        })
        .collect();
    let paths: Vec<PathBuf> = ids
        .iter()
        .map(|id| dir.join(format!("node{}.db", id.0)))
        .collect();
    let config = NodeConfig {
        linearizable_reads: true,
        ..NodeConfig::default()
    };

    for p in &paths {
        migrate(p).await?;
    }
    let mut nodes = Vec::new();
    for (ix, id) in ids.iter().enumerate() {
        nodes.push(open(*id, &ids, &peers, &paths[ix], config).await?);
    }
    until_confirmed(&nodes).await;

    let stats = Arc::new(Stats::default());
    let load = spawn_load(
        &nodes,
        Arc::clone(&stats),
        writers,
        interval,
        aggregates,
        coord,
        barriers,
    );
    tokio::time::sleep(before).await;
    for t in &load {
        t.abort();
    }
    for t in load {
        let _ = t.await;
    }
    stats.report("before the restart");

    if restart {
        let stopped = Instant::now();
        for n in nodes.drain(..) {
            close(n).await;
        }
        println!("# all stopped in {} ms", stopped.elapsed().as_millis());
        let t = Instant::now();
        for (ix, id) in ids.iter().enumerate() {
            nodes.push(open(*id, &ids, &peers, &paths[ix], config).await?);
        }
        println!("# all reopened in {} ms", t.elapsed().as_millis());
        until_confirmed(&nodes).await;
    }

    let stats = Arc::new(Stats::default());
    let load = spawn_load(
        &nodes,
        Arc::clone(&stats),
        writers,
        interval,
        aggregates,
        coord,
        barriers,
    );
    tokio::time::sleep(after).await;
    for t in &load {
        t.abort();
    }
    for t in load {
        let _ = t.await;
    }
    stats.report(if restart {
        "after the restart"
    } else {
        "second phase, no restart"
    });
    for n in nodes.drain(..) {
        close(n).await;
    }
    Ok(())
}
