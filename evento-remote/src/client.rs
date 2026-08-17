//! Client side: an [`Executor`] that forwards every call to a remote server.
//!
//! One connection actor owns the TCP stream: it multiplexes requests from all
//! [`Client`] clones over the single connection, demultiplexes replies by
//! correlation id, feeds pushed [`ServerFrame::Notify`] frames into a local
//! `write_watch` channel, and keeps the connection alive with capped
//! exponential backoff (eager reconnect — the push channel must stay alive
//! even while the client is idle-subscribed).
//!
//! **At-most-once semantics.** A request in flight when the connection drops
//! fails at the caller, but may still have executed on the server — a retried
//! `write` can then surface `InvalidOriginalVersion`. This is the same
//! contract as any RPC store.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
/// Capacity of the client's outbound request queue.
const REQ_CAPACITY: usize = 1024;
/// Capacity of the per-connection encoded-frame queue feeding the writer task.
const OUT_CAPACITY: usize = 1024;

/// The cached stability watermark pushed by the server (`None` until/unless the
/// server reports one). A plain `Option` behind a mutex — no sentinel value can
/// collide with a real watermark.
type Stable = Arc<Mutex<Option<u64>>>;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Response>>>>;

/// A remote [`Executor`]: forwards every call to a [`serve`](crate::serve)d
/// executor over TCP. Cheap to clone; all clones share one connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    req_tx: mpsc::Sender<(u64, Request)>,
    pending: Pending,
    next_id: AtomicU64,
    watch_tx: Arc<watch::Sender<u64>>,
    /// Fixed at server executor construction; snapshotted once at connect.
    default_routing_key: Option<String>,
    stable: Stable,
    request_timeout: Duration,
}

/// Builds a [`Client`] with non-default settings.
pub struct ClientBuilder {
    addr: SocketAddr,
    request_timeout: Duration,
}

impl ClientBuilder {
    /// How long a single request may wait for its reply before failing
    /// (default 30s). Raise for servers that can stall, e.g. an Accord-backed
    /// store during recovery.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Connects and performs the `Hello` exchange. Fails fast when the server
    /// is unreachable; after that the connection is kept alive with automatic
    /// reconnect.
    pub async fn connect(self) -> anyhow::Result<Client> {
        let stable: Stable = Arc::new(Mutex::new(None));
        let (framed, default_routing_key) =
            connect_and_hello(self.addr, &stable, self.request_timeout).await?;

        let (req_tx, req_rx) = mpsc::channel(REQ_CAPACITY);
        let (watch_tx, _) = watch::channel(0u64);
        let watch_tx = Arc::new(watch_tx);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

        tokio::spawn(
            Actor {
                addr: self.addr,
                req_rx,
                pending: Arc::clone(&pending),
                watch_tx: Arc::clone(&watch_tx),
                stable: Arc::clone(&stable),
                request_timeout: self.request_timeout,
            }
            .run(framed),
        );

        Ok(Client {
            inner: Arc::new(Inner {
                req_tx,
                pending,
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

    pub fn builder(addr: SocketAddr) -> ClientBuilder {
        ClientBuilder {
            addr,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    async fn request(&self, request: Request) -> anyhow::Result<Response> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .expect("pending poisoned")
            .insert(id, tx);
        if self.inner.req_tx.send((id, request)).await.is_err() {
            self.remove_pending(id);
            anyhow::bail!("remote executor connection is closed");
        }
        match tokio::time::timeout(self.inner.request_timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => anyhow::bail!("remote executor connection lost"),
            Err(_) => {
                self.remove_pending(id);
                anyhow::bail!(
                    "remote request timed out after {:?}",
                    self.inner.request_timeout
                )
            }
        }
    }

    fn remove_pending(&self, id: u64) {
        self.inner
            .pending
            .lock()
            .expect("pending poisoned")
            .remove(&id);
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
        Ok(*self.inner.stable.lock().expect("stable poisoned"))
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
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        match self
            .request(Request::Read {
                aggregators,
                routing_key,
                args,
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
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        match self
            .request(Request::LatestTimestamp {
                aggregators,
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

/// Owns the framed connection: sends queued requests, demultiplexes replies,
/// feeds pushed notifications into the local watch channel, reconnects with
/// capped exponential backoff. Exits when every [`Client`] clone is dropped.
struct Actor {
    addr: SocketAddr,
    req_rx: mpsc::Receiver<(u64, Request)>,
    pending: Pending,
    watch_tx: Arc<watch::Sender<u64>>,
    stable: Stable,
    request_timeout: Duration,
}

impl Actor {
    async fn run(mut self, initial: Framed<TcpStream, LengthDelimitedCodec>) {
        let mut conn = Some(initial);
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
                        let Some((id, request)) = item else { break };
                        // A request queued before a disconnect whose caller was
                        // already failed over the reconnect must NOT be sent on
                        // the new connection: the caller may have retried it
                        // under a fresh id, and replaying the stale one would
                        // execute the write twice.
                        if !self.pending.lock().expect("pending poisoned").contains_key(&id) {
                            continue;
                        }
                        let frame = ClientFrame::Request { id, request };
                        let Ok(bytes) = encode_tagged(RecordKind::ClientFrame, &frame) else {
                            self.fail(id);
                            continue;
                        };
                        if out_tx.send(Bytes::from(bytes)).await.is_err() {
                            self.fail(id);
                            break;
                        }
                    }
                    item = inbound.next() => {
                        let Some(Ok(bytes)) = item else { break };
                        match decode_tagged::<ServerFrame>(RecordKind::ServerFrame, &bytes) {
                            Ok(ServerFrame::Response { id, response, stable_timestamp }) => {
                                self.update_stable(stable_timestamp);
                                // An unknown id is a reply to a request that
                                // already timed out or failed over a reconnect.
                                if let Some(tx) = self
                                    .pending
                                    .lock()
                                    .expect("pending poisoned")
                                    .remove(&id)
                                {
                                    let _ = tx.send(response);
                                }
                            }
                            Ok(ServerFrame::Notify { stable_timestamp, .. }) => {
                                self.update_stable(stable_timestamp);
                                self.watch_tx.send_modify(|g| *g += 1);
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
            // everything in flight, then reconnect.
            drop(out_tx);
            let _ = writer.await;
            if self.req_rx.is_closed() && self.pending.lock().expect("pending poisoned").is_empty()
            {
                return;
            }
            self.fail_pending();
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
            *self.stable.lock().expect("stable poisoned") = Some(v);
        }
    }

    fn fail(&self, id: u64) {
        self.pending.lock().expect("pending poisoned").remove(&id);
    }

    fn fail_pending(&self) {
        // Dropping the senders surfaces "connection lost" at every caller.
        self.pending.lock().expect("pending poisoned").clear();
    }
}

/// Establishes one connection and performs the `Hello` exchange, returning the
/// framed stream and the server's `default_routing_key`. Seeds the stable
/// watermark from the reply. Frames pushed before the reply (notifies) only
/// update the watermark.
async fn connect_and_hello(
    addr: SocketAddr,
    stable: &Mutex<Option<u64>>,
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
                        *stable.lock().expect("stable poisoned") = Some(v);
                    }
                    return Ok(default_routing_key);
                }
                ServerFrame::Notify {
                    stable_timestamp, ..
                } => {
                    if let Some(v) = stable_timestamp {
                        *stable.lock().expect("stable poisoned") = Some(v);
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
