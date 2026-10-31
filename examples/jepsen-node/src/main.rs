//! A generic evento-accord node for the Jepsen harness.
//!
//! One process = one Accord replica over real TCP, backed by a durable Fjall
//! event store and a durable `FjallJournal` consensus log, exposing a small,
//! domain-free HTTP API that the Clojure/Jepsen client drives. It is the
//! generic-API sibling of `examples/bank-axum-accord`: the cluster wiring is the
//! same, but instead of bank endpoints it speaks an Elle list-append protocol.
//!
//! ## Why durable journal + `recover_state`
//!
//! Unlike the bank demo (in-memory journal), this node uses `FjallJournal` and
//! calls `Node::recover_state()` on boot, so a Jepsen process kill/restart
//! genuinely exercises restart recovery — a stale commit must never resurrect.
//!
//! ## Config (env)
//!
//! - `NODE_ID` — this node's id, an integer `0..N`.
//! - `PEERS` — full membership, `id=host:port` comma-list incl. self, e.g.
//!   `0=n1:7000,1=n2:7000,2=n3:7000`. Hostnames are resolved.
//! - `LISTEN` — Accord inter-node listen addr, e.g. `0.0.0.0:7000`.
//! - `HTTP_PORT` — Jepsen client API port (default 8080).
//! - `DATA_DIR` — durable directory for the event store + journal.
//!
//! ## Opening (OPERATIONS §7)
//!
//! The node is opened the way a host opens one, and its process restarts are
//! left to the launcher, which runs the binary again whenever it exits with
//! 75 (`EX_TEMPFAIL`):
//!
//! - `DATA_DIR/join` present: the node comes back joining. Its store is
//!   emptied, its journal opened as it is (`recover_state_for_join`; an empty
//!   one makes it an amnesiac, #312), and it joins through each other node in
//!   turn until one vouches for it. Joined, it drops the marker.
//! - `DATA_DIR/member` present but the journal gone: the node lost its
//!   journal. A plain open over a store that holds events is refused
//!   (`LostJournal`); either way it writes the join marker and exits 75.
//! - Otherwise: `recover_state`, then serve. `member` marks that it opened
//!   once: the host's knowledge, out of band, that the node was a member.
//!
//! Running, a node told to rejoin, or whose journal its peers found rolled
//! back to an older copy (#339), is retired; the binary deletes the journal
//! if it was rolled back, writes the join marker, and exits 75.
//!
//! ## HTTP API
//!
//! - `GET  /health` — `ready`, or `joining` while the node is not serving.
//! - `POST /txn`    — one Elle list-append transaction. Body
//!   `{"ops": [["append","3",4], ["r","3",null]]}`. Appends are applied as one
//!   atomic `executor.write(...)` (atomic across keys — evento's multi-aggregate
//!   conditional append); reads return each key's values in version order.
//!   Response: `200 {"type":"ok","ops":[...reads filled...]}` on commit,
//!   `409 {"type":"fail"}` on optimistic-concurrency conflict, `503
//!   {"type":"fail"}` while the node is not serving (joining, unconfirmed, or
//!   refusing writes for now: nothing was submitted), or `500
//!   {"type":"info",...}` on any indeterminate error (the client maps this to
//!   Jepsen `:info` — an indeterminate write may still have committed).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use evento::accord::{
    serve, AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, Journal,
    JournalRolledBack, LostJournal, MessageSink, Node, NodeConfig, NodeId, PartialSnapshot,
    StaticTopology, TcpTransport, Topology,
};
use evento::fjall::FjallJournal;
use evento::{Event, Executor, Fjall, WriteError};
use serde_json::{json, Value as JsonValue};
use tokio::sync::mpsc;
use ulid::Ulid;

/// The fixed aggregate type every Elle key lives under. Each Elle integer key is
/// an aggregate id; each appended value is one event.
const AGGREGATE_TYPE: &str = "jepsen/Reg";
const EVENT_NAME: &str = "Appended";

type Exec = AccordExecutor<Fjall>;

/// The exit code that has the launcher run the binary again (`EX_TEMPFAIL`).
const EX_TEMPFAIL: i32 = 75;

#[derive(Clone)]
struct AppState {
    executor: Arc<Exec>,
    /// Set once the node is open and, if it came back joining, joined.
    ready: Arc<AtomicBool>,
}

impl AppState {
    /// Whether the node serves: open, joined, confirmed by its peers
    /// (#339), and neither told to rejoin nor found rolled back.
    fn serving(&self) -> bool {
        let node = self.executor.node();
        self.ready.load(Ordering::Acquire)
            && node.confirmed()
            && !node.rejoin_required()
            && !node.rolled_back()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let id = NodeId(
        std::env::var("NODE_ID")
            .expect("NODE_ID is required")
            .parse::<u64>()
            .expect("NODE_ID must be an integer"),
    );
    let listen: SocketAddr = std::env::var("LISTEN")
        .expect("LISTEN is required")
        .parse()
        .expect("LISTEN must be a socket address, e.g. 0.0.0.0:7000");
    let http_port: u16 = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);
    let data_dir: PathBuf = std::env::var("DATA_DIR")
        .unwrap_or_else(|_| "/var/lib/jepsen-node".into())
        .into();

    // `PEERS` is the full membership, including self. Hostnames (n1, n2, …) are
    // resolved to addresses for the TCP transport.
    let peers = parse_peers(&std::env::var("PEERS").expect("PEERS is required")).await?;
    let ids: Vec<NodeId> = {
        let mut ids: Vec<NodeId> = peers.keys().copied().collect();
        ids.sort();
        ids
    };
    assert!(peers.contains_key(&id), "PEERS must contain NODE_ID {id:?}");

    std::fs::create_dir_all(&data_dir)?;
    let store_path = data_dir.join("store");
    let journal_path = data_dir.join("journal");
    let member = data_dir.join("member");
    let join_marker = data_dir.join("join");

    // A member whose journal is gone lost it (OPERATIONS §7): it comes back
    // joining. Checked before the journal is opened, which creates it empty.
    let lost_journal = !join_marker.exists() && member.exists() && !journal_path.exists();
    let joining = join_marker.exists();

    // Durable consensus journal (so a restart recovers, not resurrects) and
    // durable local event store (serves reads, applied by the Accord data
    // store). The journal is opened first: it locks the node's data, so a
    // second instance started over a running one fails here, before it
    // touches the store.
    let journal: Arc<dyn Journal> = Arc::new(FjallJournal::open(&journal_path)?);
    if joining {
        // The join rebuilds the store from a contact's snapshot.
        if store_path.exists() {
            std::fs::remove_dir_all(&store_path)?;
        }
    }
    let local = Fjall::open(&store_path)?;

    // Inbound: accept peer connections, decode frames, feed this node's inbox.
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    serve(listener, inbox_tx);

    // LINEARIZABLE_READS=1 fences each owned single-key read with a read-barrier
    // consensus round (linearizable, strict-serializable). Off → local-only reads
    // (serializable, faster).
    let config = NodeConfig {
        linearizable_reads: std::env::var("LINEARIZABLE_READS")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false),
        ..Default::default()
    };
    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(TcpTransport::new(id, peers)) as Arc<dyn MessageSink>,
        Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
        journal,
    )
    .with_config(config);

    let state = AppState {
        executor: Arc::new(AccordExecutor::new(node.clone(), local)),
        ready: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/txn", post(txn))
        .with_state(state.clone());
    let http_addr: SocketAddr = format!("0.0.0.0:{http_port}").parse().unwrap();
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

    if joining {
        // Rebuild what the journal holds, without replaying it into the
        // emptied store, then join: through each other node in turn until
        // one vouches for this one. The HTTP API answers meanwhile, refusing.
        node.recover_state_for_join().await?;
        node.begin_join();
        node.start(inbox_rx);
        tokio::spawn(async move { axum::serve(http_listener, app).await });
        tracing::info!(?id, %listen, %http_addr, "jepsen-node up, joining");
        let others: Vec<NodeId> = ids.iter().copied().filter(|&n| n != id).collect();
        let mut attempt = 0usize;
        loop {
            let contact = others[attempt % others.len()];
            attempt += 1;
            let started = std::time::Instant::now();
            match node.join(contact).await {
                Ok(imported) => {
                    tracing::info!(
                        ?id,
                        ?contact,
                        imported,
                        attempts = attempt,
                        took_ms = started.elapsed().as_millis() as u64,
                        "joined"
                    );
                    break;
                }
                // A join that broke after installing a page leaves a store
                // no contact can complete: emptied on the restart (the join
                // marker stays).
                Err(err) if err.is::<PartialSnapshot>() => {
                    tracing::warn!(?id, ?contact, %err, "a join broke mid-snapshot");
                    std::process::exit(EX_TEMPFAIL);
                }
                // The journal it rejoins over is an older copy of itself
                // (#339): it comes back over an empty one.
                Err(err) if err.is::<JournalRolledBack>() => {
                    tracing::error!(
                        ?id,
                        "its journal was rolled back: this node comes back joining"
                    );
                    node.retire().await;
                    come_back_joining(&join_marker, Some(&journal_path));
                }
                Err(err) => {
                    tracing::warn!(?id, ?contact, attempts = attempt, %err, "cannot join yet");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        std::fs::remove_file(&join_marker)?;
        node.start_recovery();
    } else {
        // Rebuild consensus + applied state from the durable journal before
        // serving, so a process restart resumes rather than starts fresh. Over
        // a lost journal, a store that holds events has the open refused
        // (`LostJournal`); an empty one would open a former member fresh,
        // forgetting what it voted. Either way the node comes back joining,
        // over an empty journal.
        match node.recover_state().await {
            Err(err) if err.is::<LostJournal>() => {
                tracing::error!(?id, %err, "a plain open was refused: this node comes back joining");
                come_back_joining(&join_marker, Some(&journal_path));
            }
            Err(err) => return Err(err),
            Ok(()) if lost_journal => {
                tracing::error!(?id, "the journal is gone: this node comes back joining");
                come_back_joining(&join_marker, Some(&journal_path));
            }
            Ok(()) => {}
        }
        node.start(inbox_rx);
        node.start_recovery();
        tokio::spawn(async move { axum::serve(http_listener, app).await });
        tracing::info!(?id, %listen, %http_addr, "jepsen-node up");
    }
    std::fs::write(&member, b"")?;
    state.ready.store(true, Ordering::Release);

    // A node told to rejoin, or whose journal was rolled back (#339), takes
    // no part any more: it is retired and comes back joining — over an
    // empty journal if it was rolled back, over its own otherwise.
    let mut rejoin = node.rejoin_signal();
    let mut rolled_back = node.rolled_back_signal();
    loop {
        if node.rolled_back() {
            tracing::error!(
                ?id,
                "its journal was rolled back: this node comes back joining"
            );
            node.retire().await;
            come_back_joining(&join_marker, Some(&journal_path));
        }
        if node.rejoin_required() {
            tracing::error!(?id, "told to rejoin: this node comes back joining");
            node.retire().await;
            come_back_joining(&join_marker, None);
        }
        tokio::select! {
            changed = rejoin.changed() => { changed?; }
            changed = rolled_back.changed() => { changed?; }
        }
    }
}

/// Leaves the join marker, deleting `journal` first if given, and exits so
/// the launcher opens the node again, joining.
fn come_back_joining(join_marker: &Path, journal: Option<&Path>) -> ! {
    if let Some(journal) = journal.filter(|journal| journal.exists()) {
        std::fs::remove_dir_all(journal).expect("delete the journal");
    }
    std::fs::write(join_marker, b"").expect("leave the join marker");
    std::process::exit(EX_TEMPFAIL);
}

/// `ready` once the node serves, `joining` otherwise.
async fn health(State(state): State<AppState>) -> &'static str {
    if state.serving() {
        "ready"
    } else {
        "joining"
    }
}

/// Parses `PEERS` (`id=host:port,…`), resolving each host to a `SocketAddr`.
async fn parse_peers(raw: &str) -> anyhow::Result<HashMap<NodeId, SocketAddr>> {
    let mut peers = HashMap::new();
    for pair in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = pair
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("bad PEERS entry {pair:?}, want id=host:port"))?;
        let id = NodeId(id.trim().parse::<u64>()?);
        let addr = tokio::net::lookup_host(addr.trim())
            .await?
            .next()
            .ok_or_else(|| anyhow::anyhow!("could not resolve peer address {addr:?}"))?;
        peers.insert(id, addr);
    }
    Ok(peers)
}

/// One Elle list-append transaction.
///
/// Appends are collected into a single atomic `write` (atomic across keys); then
/// reads are served from the now-committed local store. The generator emits
/// read-only or append-only transactions, so there is no intra-transaction
/// read-after-own-write ordering to honour.
async fn txn(State(state): State<AppState>, Json(body): Json<JsonValue>) -> Response {
    // Not serving yet: nothing is submitted, so a definite failure.
    if !state.serving() {
        return not_serving();
    }
    let ops = match body.get("ops").and_then(JsonValue::as_array) {
        Some(ops) => ops.clone(),
        None => return error_response(StatusCode::BAD_REQUEST, "missing ops"),
    };

    // Build events for every append, assigning contiguous versions per key
    // starting from each key's current version.
    let mut next_version: HashMap<String, u16> = HashMap::new();
    let mut events: Vec<Event> = Vec::new();
    for op in &ops {
        let Some((kind, key, value)) = parse_op(op) else {
            return error_response(StatusCode::BAD_REQUEST, "malformed op");
        };
        if kind != "append" {
            continue;
        }
        let base = match next_version.get(&key).copied() {
            Some(v) => v,
            None => match current_version(&state.executor, &key).await {
                Ok(v) => v,
                Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
        };
        let version = base + 1;
        next_version.insert(key.clone(), version);
        events.push(append_event(
            &key,
            version,
            value.expect("append needs a value"),
        ));
    }

    // Atomic conditional multi-aggregate append.
    if !events.is_empty() {
        match state.executor.write(events).await {
            Ok(()) => {}
            Err(WriteError::InvalidOriginalVersion) => {
                return (StatusCode::CONFLICT, Json(json!({"type": "fail"}))).into_response();
            }
            // Refused before anything was submitted: the node is not taking
            // writes right now (unconfirmed, told to rejoin, rolled back,
            // retired, at its backlog cap).
            Err(WriteError::Unavailable(_)) => return not_serving(),
            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    // Serve reads from the committed local store, filling each `r` op's values.
    let mut out: Vec<JsonValue> = Vec::with_capacity(ops.len());
    for op in &ops {
        let (kind, key, _) = parse_op(op).expect("already validated");
        if kind == "r" {
            match read_values(&state.executor, &key).await {
                Ok(vals) => out.push(json!(["r", key, vals])),
                Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        } else {
            out.push(op.clone());
        }
    }

    (StatusCode::OK, Json(json!({"type": "ok", "ops": out}))).into_response()
}

/// Parses one `[kind, key, value]` op into `(kind, key, value)`.
fn parse_op(op: &JsonValue) -> Option<(String, String, Option<i64>)> {
    let arr = op.as_array()?;
    let kind = arr.first()?.as_str()?.to_string();
    let key = match arr.get(1)? {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        _ => return None,
    };
    let value = arr.get(2).and_then(JsonValue::as_i64);
    Some((kind, key, value))
}

/// The aggregate's current version: the max version among its events (mirrors
/// `ExecutorDataStore::version`).
async fn current_version(exec: &Exec, key: &str) -> anyhow::Result<u16> {
    let events = evento::read_raw(AGGREGATE_TYPE, key).execute(exec).await?;
    Ok(events.iter().map(|e| e.version).max().unwrap_or(0))
}

/// All appended values for a key, ordered by version (the true append order the
/// CAS enforces — robust regardless of the store's timestamp-based read order).
async fn read_values(exec: &Exec, key: &str) -> anyhow::Result<Vec<i64>> {
    let mut events = evento::read_raw(AGGREGATE_TYPE, key).execute(exec).await?;
    events.sort_by_key(|e| e.version);
    events
        .iter()
        .map(|e| {
            std::str::from_utf8(&e.data)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| anyhow::anyhow!("corrupt value for {key}"))
        })
        .collect()
}

/// Builds one append event: value stored as its decimal string in `data`.
fn append_event(key: &str, version: u16, value: i64) -> Event {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Event {
        id: Ulid::generate(),
        aggregate_id: key.to_string(),
        aggregate_type: AGGREGATE_TYPE.to_string(),
        version,
        name: EVENT_NAME.to_string(),
        data: value.to_string().into_bytes(),
        timestamp: now.as_secs(),
        timestamp_subsec: now.subsec_millis(),
        ..Default::default()
    }
}

/// The node is not serving: nothing was submitted, so the client maps this to
/// Jepsen `:fail`.
fn not_serving() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"type": "fail", "error": "not serving"})),
    )
        .into_response()
}

/// An indeterminate failure: the client maps a non-409 error to Jepsen `:info`.
fn error_response(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"type": "info", "error": msg}))).into_response()
}
