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
//! ## HTTP API
//!
//! - `GET  /health` — readiness probe.
//! - `POST /txn`    — one Elle list-append transaction. Body
//!   `{"ops": [["append","3",4], ["r","3",null]]}`. Appends are applied as one
//!   atomic `executor.write(...)` (atomic across keys — evento's multi-aggregate
//!   conditional append); reads return each key's values in version order.
//!   Response: `200 {"type":"ok","ops":[...reads filled...]}` on commit,
//!   `409 {"type":"fail"}` on optimistic-concurrency conflict, or
//!   `500 {"type":"info",...}` on any indeterminate error (the client maps this
//!   to Jepsen `:info` — an indeterminate write may still have committed).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use evento::cursor::Args;
use evento::{Event, EventFilter, Executor, Fjall, WriteError};
use evento_accord::{
    serve, AccordExecutor, DataStore, ExecutorDataStore, FjallJournal, HybridLogicalClock, Journal,
    MessageSink, Node, NodeConfig, NodeId, StaticTopology, TcpTransport, Topology,
};
use serde_json::{json, Value as JsonValue};
use tokio::sync::mpsc;
use ulid::Ulid;

/// The fixed aggregate type every Elle key lives under. Each Elle integer key is
/// an aggregate id; each appended value is one event.
const AGGREGATE_TYPE: &str = "jepsen/Reg";
const EVENT_NAME: &str = "Appended";

type Exec = AccordExecutor<Fjall>;

#[derive(Clone)]
struct AppState {
    executor: Arc<Exec>,
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

    // Durable local event store (serves reads, applied by the Accord data store)
    // and durable consensus journal (so a restart recovers, not resurrects).
    let local = Fjall::open(&store_path)?;
    let journal: Arc<dyn Journal> = Arc::new(FjallJournal::open(&journal_path)?);

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
        Arc::new(StaticTopology::new(id, ids)) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(TcpTransport::new(id, peers)) as Arc<dyn MessageSink>,
        Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
        journal,
    )
    .with_config(config);

    // Rebuild consensus + applied state from the durable journal before serving,
    // so a process restart resumes rather than starts fresh.
    node.recover_state().await?;
    node.start(inbox_rx);
    node.start_recovery();

    let state = AppState {
        executor: Arc::new(AccordExecutor::new(node, local)),
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/txn", post(txn))
        .with_state(state);

    let http_addr: SocketAddr = format!("0.0.0.0:{http_port}").parse().unwrap();
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!(?id, %listen, %http_addr, "jepsen-node up");
    axum::serve(http_listener, app).await?;

    Ok(())
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
    let result = exec
        .read(
            Some(vec![EventFilter::by_id(AGGREGATE_TYPE, key)]),
            None,
            Args::forward(u16::MAX - 1, None),
        )
        .await?;
    Ok(result
        .edges
        .iter()
        .map(|e| e.node.version)
        .max()
        .unwrap_or(0))
}

/// All appended values for a key, ordered by version (the true append order the
/// CAS enforces — robust regardless of the store's timestamp-based read order).
async fn read_values(exec: &Exec, key: &str) -> anyhow::Result<Vec<i64>> {
    let result = exec
        .read(
            Some(vec![EventFilter::by_id(AGGREGATE_TYPE, key)]),
            None,
            Args::forward(u16::MAX - 1, None),
        )
        .await?;
    let mut events: Vec<&Event> = result.edges.iter().map(|e| &e.node).collect();
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
        id: Ulid::new(),
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

/// An indeterminate failure: the client maps a non-409 error to Jepsen `:info`.
fn error_response(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"type": "info", "error": msg}))).into_response()
}
