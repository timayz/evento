//! Client side: an [`Executor`] that forwards every call to a remote server.
//!
//! Each pooled connection ([`ClientBuilder::connections`], default 1) is owned
//! by one actor: it multiplexes requests dispatched to it over its TCP stream,
//! demultiplexes replies by correlation id, and keeps the connection alive
//! with capped exponential backoff (eager reconnect — the push channel must
//! stay alive even while the client is idle-subscribed). Requests are
//! round-robined across connections; pushed [`ServerFrame::Notify`] frames
//! feed a local `write_watch` channel (from the first connection only — the
//! watch is level-triggered, duplicates from every connection add nothing).
//!
//! **At-most-once semantics.** A request in flight when the connection drops
//! fails at the caller, but may still have executed on the server — a retried
//! `write` can then surface `InvalidOriginalVersion`. This is the same
//! contract as any RPC store. Requests on different pooled connections have no
//! ordering relative to each other; the executor contract is request/response,
//! and callers await a reply before issuing a dependent call.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use evento_core::cursor::{Args, ReadResult, Value};
use evento_core::{Event, EventFilter, Executor, RoutingKey, WriteError};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use ulid::Ulid;

use crate::wire::{
    decode_tagged, encode_tagged, ClientFrame, RecordKind, Request, Response, ServerFrame,
    MAX_FRAME_LENGTH,
};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);
/// Correlation id of the per-connection `Hello` exchange; regular ids start at 1.
const HELLO_ID: u64 = 0;
/// Capacity of each connection's outbound request queue.
const REQ_CAPACITY: usize = 1024;
/// Capacity of the per-connection encoded-frame queue feeding the writer task.
const OUT_CAPACITY: usize = 1024;

/// The cached stability watermark pushed by the server, max-merged across
/// every connection's responses and notifies (so cross-connection reordering
/// can never move it backwards). `0` = not reported yet — the watermark is
/// microseconds since the Unix epoch, so a real report is never 0.
type Stable = Arc<AtomicU64>;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}

/// A request dispatched to a connection actor: the reply channel travels with
/// it, and the actor owns its own pending map — no shared lock.
type Dispatch = (u64, Request, oneshot::Sender<Response>);

/// A remote [`Executor`]: forwards every call to a [`serve`](crate::serve)d
/// executor over TCP. Cheap to clone; all clones share the connection pool.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    /// One request queue per pooled connection.
    lanes: Vec<mpsc::Sender<Dispatch>>,
    round_robin: AtomicUsize,
    next_id: AtomicU64,
    watch_tx: Arc<watch::Sender<u64>>,
    /// Fixed at server executor construction; snapshotted once from the first
    /// connection's `Hello` (it is borrowed as `&str`, so it must stay fixed).
    default_routing_key: Option<String>,
    stable: Stable,
    request_timeout: Duration,
}

/// Builds a [`Client`] with non-default settings.
pub struct ClientBuilder {
    addr: SocketAddr,
    request_timeout: Duration,
    connections: usize,
}

impl ClientBuilder {
    /// How long a single request may wait for its reply before failing
    /// (default 30s). Raise for servers that can stall, e.g. an Accord-backed
    /// store during recovery.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Number of TCP connections in the pool (default 1, clamped to ≥ 1).
    /// Requests are round-robined across them, so concurrent callers stop
    /// sharing one socket's head-of-line and one server-side in-flight budget.
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = n.max(1);
        self
    }

    /// Connects and performs the `Hello` exchange on every pooled connection.
    /// Fails fast when the server is unreachable; after that each connection
    /// is kept alive with automatic reconnect.
    pub async fn connect(self) -> anyhow::Result<Client> {
        let stable: Stable = Arc::new(AtomicU64::new(0));
        let (watch_tx, _) = watch::channel(0u64);
        let watch_tx = Arc::new(watch_tx);

        let mut lanes = Vec::with_capacity(self.connections);
        let mut default_routing_key = None;
        for lane in 0..self.connections {
            let (framed, routing_key) =
                connect_and_hello(self.addr, &stable, self.request_timeout).await?;
            if lane == 0 {
                default_routing_key = routing_key;
            }
            let (req_tx, req_rx) = mpsc::channel(REQ_CAPACITY);
            tokio::spawn(
                Actor {
                    addr: self.addr,
                    req_rx,
                    // One forwarder is enough: the watch is a level-triggered
                    // wakeup, and every connection receives the same notifies.
                    watch_tx: (lane == 0).then(|| Arc::clone(&watch_tx)),
                    stable: Arc::clone(&stable),
                    request_timeout: self.request_timeout,
                }
                .run(framed),
            );
            lanes.push(req_tx);
        }

        Ok(Client {
            inner: Arc::new(Inner {
                lanes,
                round_robin: AtomicUsize::new(0),
                next_id: AtomicU64::new(HELLO_ID + 1),
                watch_tx,
                default_routing_key,
                stable,
                request_timeout: self.request_timeout,
            }),
        })
    }
}

impl Client {
    /// Connects with default settings. See [`Client::builder`] for knobs.
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Client> {
        Client::builder(addr).connect().await
    }

    /// Starts building a client for `addr` (request timeout, connection count).
    pub fn builder(addr: SocketAddr) -> ClientBuilder {
        ClientBuilder {
            addr,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connections: 1,
        }
    }

    async fn request(&self, request: Request) -> anyhow::Result<Response> {
        // Ids only need uniqueness per connection (they are per-connection on
        // the wire); a global counter is a superset of that.
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let lane = self.inner.round_robin.fetch_add(1, Ordering::Relaxed) % self.inner.lanes.len();
        let (tx, rx) = oneshot::channel();
        if self.inner.lanes[lane]
            .send((id, request, tx))
            .await
            .is_err()
        {
            anyhow::bail!("remote executor connection is closed");
        }
        match tokio::time::timeout(self.inner.request_timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => anyhow::bail!("remote executor connection lost"),
            Err(_) => anyhow::bail!(
                "remote request timed out after {:?}",
                self.inner.request_timeout
            ),
        }
    }
}

fn protocol_err(what: &str) -> anyhow::Error {
    anyhow::anyhow!("remote protocol error: unexpected response to {what}")
}

#[async_trait::async_trait]
impl Executor for Client {
    fn default_routing_key(&self) -> Option<&str> {
        self.inner.default_routing_key.as_deref()
    }

    fn write_watch(&self) -> Option<watch::Receiver<u64>> {
        Some(self.inner.watch_tx.subscribe())
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // The cached watermark is refreshed by every response and notify frame;
        // a polling subscription issues several requests per pass, so the cache
        // is at most one round-trip stale by the time the gate consults it.
        match self.inner.stable.load(Ordering::Acquire) {
            0 => Ok(None),
            v => Ok(Some(v)),
        }
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        match self.request(Request::Write { events }).await {
            Ok(Response::Write(Ok(()))) => Ok(()),
            Ok(Response::Write(Err(e))) => Err(e.into()),
            Ok(_) => Err(WriteError::Unknown(protocol_err("write"))),
            Err(e) => Err(WriteError::Unknown(e)),
        }
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        match self.request(Request::Replicate { events }).await {
            Ok(Response::Write(Ok(()))) => Ok(()),
            Ok(Response::Write(Err(e))) => Err(e.into()),
            Ok(_) => Err(WriteError::Unknown(protocol_err("replicate"))),
            Err(e) => Err(WriteError::Unknown(e)),
        }
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        match self
            .request(Request::Read {
                aggregators: aggregators.map(|a| a.to_vec()),
                routing_key,
                args,
                to_micros,
            })
            .await?
        {
            Response::Read(Ok(wire)) => Ok(wire.into()),
            Response::Read(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("read")),
        }
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        match self
            .request(Request::LatestTimestamp {
                aggregators: aggregators.map(|a| a.to_vec()),
                routing_key,
            })
            .await?
        {
            Response::LatestTimestamp(Ok(v)) => Ok(v),
            Response::LatestTimestamp(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("latest_timestamp")),
        }
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        match self.request(Request::GetSubscriberCursor { key }).await? {
            Response::SubscriberCursor(Ok(v)) => Ok(v),
            Response::SubscriberCursor(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("get_subscriber_cursor")),
        }
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        match self
            .request(Request::IsSubscriberRunning { key, worker_id })
            .await?
        {
            Response::SubscriberRunning(Ok(v)) => Ok(v),
            Response::SubscriberRunning(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("is_subscriber_running")),
        }
    }

    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<evento_core::SubscriberStatus> {
        match self
            .request(Request::SubscriberStatus { key, worker_id })
            .await?
        {
            Response::SubscriberStatus(Ok(v)) => Ok(v),
            Response::SubscriberStatus(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("subscriber_status")),
        }
    }

    async fn latest_version(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        match self
            .request(Request::LatestVersion {
                aggregate_type,
                aggregate_id,
            })
            .await?
        {
            Response::LatestVersion(Ok(v)) => Ok(v),
            Response::LatestVersion(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("latest_version")),
        }
    }

    async fn stream_routing_key(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        match self
            .request(Request::StreamRoutingKey {
                aggregate_type,
                aggregate_id,
            })
            .await?
        {
            Response::StreamRoutingKey(Ok(v)) => Ok(v),
            Response::StreamRoutingKey(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("stream_routing_key")),
        }
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        match self
            .request(Request::UpsertSubscriber { key, worker_id })
            .await?
        {
            Response::Unit(Ok(())) => Ok(()),
            Response::Unit(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("upsert_subscriber")),
        }
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        match self
            .request(Request::Acknowledge {
                key,
                worker_id,
                cursor,
                lag,
            })
            .await?
        {
            Response::Acknowledge(Ok(v)) => Ok(v),
            Response::Acknowledge(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("acknowledge")),
        }
    }

    async fn get_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        match self
            .request(Request::GetSnapshot {
                aggregate_type,
                aggregate_revision,
                id,
            })
            .await?
        {
            Response::Snapshot(Ok(v)) => Ok(v),
            Response::Snapshot(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("get_snapshot")),
        }
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        match self
            .request(Request::SaveSnapshot {
                aggregate_type,
                aggregate_revision,
                id,
                data,
                cursor,
            })
            .await?
        {
            Response::Unit(Ok(())) => Ok(()),
            Response::Unit(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("save_snapshot")),
        }
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        match self
            .request(Request::DeleteSnapshot { aggregate_type, id })
            .await?
        {
            Response::Unit(Ok(())) => Ok(()),
            Response::Unit(Err(msg)) => Err(anyhow::anyhow!(msg)),
            _ => Err(protocol_err("delete_snapshot")),
        }
    }
}

/// Owns one framed connection of the pool: sends dispatched requests,
/// demultiplexes replies via its **own** pending map (the reply channel
/// arrives with each request, so there is no shared lock), optionally feeds
/// pushed notifications into the watch channel, reconnects with capped
/// exponential backoff. Exits when every [`Client`] clone is dropped.
struct Actor {
    addr: SocketAddr,
    req_rx: mpsc::Receiver<Dispatch>,
    /// `Some` on the notify-forwarding connection only.
    watch_tx: Option<Arc<watch::Sender<u64>>>,
    stable: Stable,
    request_timeout: Duration,
}

impl Actor {
    async fn run(mut self, initial: Framed<TcpStream, LengthDelimitedCodec>) {
        let mut conn = Some(initial);
        // In-flight requests on the current connection. Cleared (failing the
        // callers) on disconnect: a possibly-executed request must never be
        // replayed on the next connection — the caller may retry it under a
        // fresh id. Requests still queued in `req_rx` at that point were never
        // sent, so their callers keep waiting and they go out on the new
        // connection safely.
        let mut pending: HashMap<u64, oneshot::Sender<Response>> = HashMap::new();
        loop {
            let framed = match conn.take() {
                Some(framed) => framed,
                None => match self.reconnect().await {
                    Some(framed) => framed,
                    None => return, // all clients dropped
                },
            };
            let (mut sink, mut inbound) = framed.split();
            // Dedicated writer task: the read side must keep draining even
            // while an outbound send is blocked on TCP backpressure, or a
            // pipelining client and a slow server can deadlock head-of-line
            // (each side blocked sending, neither reading).
            let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(OUT_CAPACITY);
            let writer = tokio::spawn(async move {
                // Coalesce: feed this frame plus everything already queued,
                // then flush once — one syscall per burst instead of one per
                // frame (`send` = `feed` + `flush`).
                'writer: while let Some(bytes) = out_rx.recv().await {
                    if sink.feed(bytes).await.is_err() {
                        break;
                    }
                    while let Ok(bytes) = out_rx.try_recv() {
                        if sink.feed(bytes).await.is_err() {
                            break 'writer;
                        }
                    }
                    if sink.flush().await.is_err() {
                        break;
                    }
                }
            });
            loop {
                tokio::select! {
                    item = self.req_rx.recv() => {
                        let Some((id, request, reply_tx)) = item else { break };
                        let frame = ClientFrame::Request { id, request };
                        let Ok(bytes) = encode_tagged(RecordKind::ClientFrame, &frame) else {
                            continue; // dropping reply_tx fails the caller
                        };
                        pending.insert(id, reply_tx);
                        if out_tx.send(Bytes::from(bytes)).await.is_err() {
                            pending.remove(&id);
                            break;
                        }
                    }
                    item = inbound.next() => {
                        let Some(Ok(bytes)) = item else { break };
                        match decode_tagged::<ServerFrame>(RecordKind::ServerFrame, &bytes) {
                            Ok(ServerFrame::Response { id, response, stable_timestamp }) => {
                                self.update_stable(stable_timestamp);
                                // An unknown id is a reply to a request that
                                // already timed out (its caller is gone — the
                                // send below just fails harmlessly).
                                if let Some(tx) = pending.remove(&id) {
                                    let _ = tx.send(response);
                                }
                            }
                            Ok(ServerFrame::Notify { stable_timestamp, .. }) => {
                                self.update_stable(stable_timestamp);
                                if let Some(watch_tx) = &self.watch_tx {
                                    watch_tx.send_modify(|g| *g += 1);
                                }
                            }
                            Err(err) => {
                                tracing::debug!(?err, "undecodable frame, reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            // Disconnected (or all clients dropped): stop the writer, fail
            // everything in flight (dropping the reply senders surfaces
            // "connection lost" at each caller), then reconnect.
            drop(out_tx);
            let _ = writer.await;
            pending.clear();
            if self.req_rx.is_closed() {
                return;
            }
        }
    }

    /// Reconnects with capped exponential backoff. `None` when every client
    /// clone has been dropped (nothing left to serve).
    async fn reconnect(&mut self) -> Option<Framed<TcpStream, LengthDelimitedCodec>> {
        let mut backoff = RECONNECT_MIN;
        loop {
            if self.req_rx.is_closed() {
                return None;
            }
            match connect_and_hello(self.addr, &self.stable, self.request_timeout).await {
                // The routing key from a re-Hello is ignored: it is fixed at
                // server construction, and the cached one must stay stable for
                // the lifetime of this Client (it is borrowed as &str).
                Ok((framed, _)) => return Some(framed),
                Err(err) => {
                    tracing::debug!(?err, addr = %self.addr, "reconnect failed, backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                }
            }
        }
    }

    fn update_stable(&self, stable_timestamp: Option<u64>) {
        if let Some(v) = stable_timestamp {
            self.stable.fetch_max(v, Ordering::AcqRel);
        }
    }
}

/// Establishes one connection and performs the `Hello` exchange, returning the
/// framed stream and the server's `default_routing_key`. Seeds the stable
/// watermark from the reply. Frames pushed before the reply (notifies) only
/// update the watermark.
async fn connect_and_hello(
    addr: SocketAddr,
    stable: &AtomicU64,
    timeout: Duration,
) -> anyhow::Result<(Framed<TcpStream, LengthDelimitedCodec>, Option<String>)> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    let mut framed = Framed::new(stream, codec());

    let hello = ClientFrame::Request {
        id: HELLO_ID,
        request: Request::Hello,
    };
    framed
        .send(Bytes::from(encode_tagged(RecordKind::ClientFrame, &hello)?))
        .await?;

    let default_routing_key = tokio::time::timeout(timeout, async {
        loop {
            let Some(item) = framed.next().await else {
                anyhow::bail!("connection closed during hello");
            };
            match decode_tagged::<ServerFrame>(RecordKind::ServerFrame, &item?)? {
                ServerFrame::Response {
                    id: HELLO_ID,
                    response:
                        Response::Hello {
                            default_routing_key,
                        },
                    stable_timestamp,
                } => {
                    if let Some(v) = stable_timestamp {
                        stable.fetch_max(v, Ordering::AcqRel);
                    }
                    return Ok(default_routing_key);
                }
                ServerFrame::Notify {
                    stable_timestamp, ..
                } => {
                    if let Some(v) = stable_timestamp {
                        stable.fetch_max(v, Ordering::AcqRel);
                    }
                }
                _ => anyhow::bail!("unexpected frame during hello"),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("hello timed out after {timeout:?}"))??;

    Ok((framed, default_routing_key))
}
