//! Server side: expose executors over framed TCP, plain or TLS.
//!
//! Every connection starts with a [`Hello`] exchange: the client presents an
//! optional token, and a [`Resolver`] decides which executor that connection
//! is served with — or refuses it with a [`Rejection`]. [`serve`] is the
//! one-executor case (an open server, every connection gets the same store);
//! [`serve_with`] takes the resolver that a host authenticating credentials
//! and multiplexing stores needs. The `_tls` variants complete a rustls
//! handshake first, mirroring `evento-cluster`'s `serve`/`serve_tls` split.
//!
//! Requests on one connection are handled **concurrently** (spawn per
//! request) — multiple application tasks share one client connection, and a
//! slow `write` (e.g. a consensus round on an Accord-backed server) must not
//! head-of-line-block unrelated reads; correlation ids make out-of-order
//! replies safe.
//!
//! A connection is told of the writes its client watches for, and of no
//! other: the client registers each watch with the filter its reads use
//! ([`ClientFrame::Watch`]), and a [`ServerFrame::Notify`] names the watches a
//! write wakes. A watch is fed from the executor's own `write_watch_for` when
//! it has a write signal, otherwise from a server-wide signal fired after
//! each successful `write` handled by any connection (still low-latency for
//! writes through this server; writers bypassing it are covered by the
//! client's poll-interval fallback, the same caveat every `write_watch`
//! backend documents). A connection without a write signal may thus be woken
//! by a write to another executor; the watch is level-triggered, so a
//! spurious wakeup costs one empty poll. On a connection serving a tenant a
//! watch follows that tenant's writes alone.
//!
//! A session may serve one tenant ([`Session::tenant`]): every request on the
//! connection then acts in that tenant, whatever tenant it names, and the
//! requests that would reach past it are refused.

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use evento_core::{Executor, TenantError, TenantFilter, WriteInterest, WriteSignal};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::either::Either;

use crate::wire::{
    encode_tagged, ClientFrame, HelloOk, RecordKind, Rejection, Request, Response, ServerFrame,
    WireReadResult, WireWriteError, MAX_FRAME_LENGTH, MAX_HELLO_LENGTH,
};

/// Capacity of a connection's outbound queue. Sends into it are awaited, so a
/// slow client backpressures its own request handlers instead of shedding
/// replies.
const OUT_CAPACITY: usize = 1024;

/// Maximum concurrently-executing request handlers per connection. Without a
/// cap a client pipelining arbitrarily many large frames could spawn unbounded
/// tasks; at the cap the read loop stops pulling frames (TCP backpressure).
const MAX_INFLIGHT_PER_CONNECTION: usize = 256;

/// Maximum watches a connection may hold, each a task and a receiver on the
/// executor's write signal. Past it a watch is not registered and its
/// watcher is left to its poll interval.
const MAX_WATCHES_PER_CONNECTION: usize = 65_536;

/// Capacity of the queue between a connection's watches and the task that
/// names them in a [`ServerFrame::Notify`]. A watch that finds it full waits:
/// the writes that land meanwhile are covered by the one notify that follows.
const WOKEN_CAPACITY: usize = 1024;

/// A dialer that never completes the TLS handshake, or never sends `Hello`,
/// must not pin a task forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// Inbound connection: plain TCP, or a server-side TLS session over it.
type ServerStream = Either<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;

/// Connections that have not been accepted by the resolver yet, unless
/// [`ServerBuilder::max_pending`] says otherwise. With [`MAX_HELLO_LENGTH`]
/// it bounds what the server holds for peers that have shown no credential.
const MAX_PENDING: usize = 1024;

/// A flood of dials refused at a cap is said once, then counted and said
/// again this often.
const REFUSED_SAID_EVERY: Duration = Duration::from_secs(30);

/// The codec of a connection until the resolver accepts it: a first frame
/// longer than a `Hello` is refused at its head, before any of it is held.
fn hello_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_HELLO_LENGTH)
        .new_codec()
}

/// What a client said when it connected, and where from.
#[derive(Debug, Clone)]
pub struct Hello {
    /// The credential presented, if any. Its meaning is the resolver's.
    pub token: Option<String>,
    /// The peer address (after TLS, if any).
    pub peer: SocketAddr,
}

/// What a connection is served with, as a [`Resolver`] builds it.
pub struct Session<E> {
    executor: E,
    tenant: Option<Arc<str>>,
    guard: Option<Box<dyn Any + Send + Sync>>,
    close: Option<watch::Receiver<bool>>,
}

impl<E> Session<E> {
    /// A session over `executor`, kept for the whole connection.
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            tenant: None,
            guard: None,
            close: None,
        }
    }

    /// Serves the connection in `tenant` alone, whatever tenant its requests
    /// name: reads and loads see only `tenant`'s events, writes land in
    /// `tenant`, and the version, routing-key and snapshot lookups are
    /// `tenant`'s. A host that maps a client's credential to a tenant in its
    /// [`Resolver`] confines the client this way, over one store shared by
    /// every tenant.
    ///
    /// The client sees a store of its own: the events it reads come back in
    /// the default tenant (`""`), so it names no tenant anywhere — its
    /// projections, snapshots and cursors are kept as against a store of a
    /// single tenant.
    ///
    /// Requests that would reach past the tenant are refused: `replicate`,
    /// which keeps the caller's timestamps (an event backdated behind the
    /// cursor of a subscription over the shared store would never reach it),
    /// and the subscriber calls, since a subscriber row belongs to no tenant.
    /// A tenant's application keeps its subscriptions in a store of its own:
    /// a `Sidecar` (evento-core's `rw` feature) with the client as its events
    /// source.
    ///
    /// # Errors
    ///
    /// [`TenantError`] for `""` — the default tenant, which a session serving
    /// no tenant reaches along with every other — or for a tenant id longer
    /// than a store keeps.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Result<Self, TenantError> {
        let tenant = tenant.into();
        TenantError::check(&tenant)?;
        self.tenant = Some(tenant.into());
        Ok(self)
    }

    /// Keeps `guard` alive until the connection ends, then drops it. A host
    /// leases a tenant to the connection this way: the lease is released when
    /// the client goes away, whatever the reason.
    pub fn keep(mut self, guard: impl Any + Send + Sync) -> Self {
        self.guard = Some(Box::new(guard));
        self
    }

    /// Ends the connection from the server's side when `signal` turns `true`
    /// (a credential was revoked). In-flight requests fail at the client, and
    /// its reconnect goes through the resolver again, which can now refuse it.
    pub fn close_when(mut self, signal: watch::Receiver<bool>) -> Self {
        self.close = Some(signal);
        self
    }
}

/// Picks the executor a connection is served with, per connection, from what
/// the client presented at [`Hello`]: authentication and store multiplexing
/// in one hook. Called once per connection (and again on each reconnect of a
/// client, which is a new connection), before any request is handled.
#[async_trait::async_trait]
pub trait Resolver: Send + Sync + 'static {
    /// What every connection of this server is served with.
    type Executor: Executor + Clone;

    /// The session for a connection that said `hello`, or why it is refused.
    /// The refusal reaches the client as the error of its `connect`.
    async fn resolve(&self, hello: &Hello) -> Result<Session<Self::Executor>, Rejection>;
}

/// The trivial resolver: every connection gets the same executor.
pub struct Static<E>(E);

#[async_trait::async_trait]
impl<E: Executor + Clone> Resolver for Static<E> {
    type Executor = E;

    async fn resolve(&self, _hello: &Hello) -> Result<Session<E>, Rejection> {
        Ok(Session::new(self.0.clone()))
    }
}

/// Handle to a running server. Dropping it does **not** stop the server; call
/// [`shutdown`](ServerHandle::shutdown).
pub struct ServerHandle {
    accept_task: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl ServerHandle {
    /// Stops accepting connections and closes every established one. In-flight
    /// request handlers may still complete against the executor, but their
    /// replies are no longer delivered.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
    }
}

/// Serves `executor` to every connection on `listener`, plaintext, until
/// [`ServerHandle::shutdown`]. Tokens presented at `Hello` are ignored.
pub fn serve<E: Executor + Clone>(listener: TcpListener, executor: E) -> ServerHandle {
    serve_with(listener, Static(executor))
}

/// Serves each connection on `listener` with the executor `resolver` picks
/// for it, plaintext, until [`ServerHandle::shutdown`].
pub fn serve_with<R: Resolver>(listener: TcpListener, resolver: R) -> ServerHandle {
    ServerBuilder::new(resolver).serve(listener)
}

/// Like [`serve`], completing a TLS handshake with `acceptor` on each
/// connection first. Client-certificate verification is the acceptor's
/// configuration; the resolver does not see the certificate.
pub fn serve_tls<E: Executor + Clone>(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    executor: E,
) -> ServerHandle {
    serve_tls_with(listener, acceptor, Static(executor))
}

/// Like [`serve_with`], completing a TLS handshake with `acceptor` on each
/// connection first.
pub fn serve_tls_with<R: Resolver>(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    resolver: R,
) -> ServerHandle {
    ServerBuilder::new(resolver).tls(acceptor).serve(listener)
}

/// A server and its limits, for a listener that anyone can dial. The `serve*`
/// functions are this builder with its defaults.
///
/// ```rust,no_run
/// # async fn run(resolver: impl evento_remote::Resolver) -> anyhow::Result<()> {
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
/// let handle = evento_remote::ServerBuilder::new(resolver)
///     .max_connections(10_000)
///     .max_pending(256)
///     .serve(listener);
/// # let _ = handle;
/// # Ok(())
/// # }
/// ```
pub struct ServerBuilder<R> {
    resolver: R,
    tls: Option<TlsAcceptor>,
    max_connections: Option<usize>,
    max_pending: usize,
}

impl<E: Executor + Clone> ServerBuilder<Static<E>> {
    /// A server of `executor` for every connection, as [`serve`]: tokens
    /// presented at `Hello` are ignored.
    pub fn open(executor: E) -> Self {
        Self::new(Static(executor))
    }
}

impl<R: Resolver> ServerBuilder<R> {
    /// A server of the executor `resolver` picks for each connection, as
    /// [`serve_with`].
    pub fn new(resolver: R) -> Self {
        Self {
            resolver,
            tls: None,
            max_connections: None,
            max_pending: MAX_PENDING,
        }
    }

    /// Completes a TLS handshake with `acceptor` on each connection first, as
    /// [`serve_tls_with`].
    pub fn tls(mut self, acceptor: TlsAcceptor) -> Self {
        self.tls = Some(acceptor);
        self
    }

    /// The connections the server holds at once, pending or established.
    /// Past it a dial is accepted and closed at once, and the client's
    /// reconnect finds a place when one is free. No cap by default.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// The connections the server holds that its resolver has not accepted
    /// yet: in their TLS handshake, yet to say `Hello`, or being resolved.
    /// Past it a dial is accepted and closed at once. 1024 by default.
    ///
    /// Such a connection has shown no credential, and the server reads no
    /// more than a `Hello` from it (16 KiB), for ten seconds at most: this
    /// cap times that is what anonymous peers can make the server hold. A
    /// flood of silent dials that fills it keeps honest clients out for as
    /// long as it lasts, and the connections already established are not
    /// touched.
    pub fn max_pending(mut self, max: usize) -> Self {
        self.max_pending = max;
        self
    }

    /// Serves the connections of `listener` until [`ServerHandle::shutdown`].
    pub fn serve(self, listener: TcpListener) -> ServerHandle {
        start(listener, self)
    }
}

/// The dials refused at a cap: said once, then every [`REFUSED_SAID_EVERY`]
/// with how many were refused meanwhile.
#[derive(Default)]
struct Refused {
    unsaid: u64,
    said: Option<Instant>,
}

impl Refused {
    fn refuse(&mut self, peer: SocketAddr, cap: &'static str, max: usize) {
        self.unsaid += 1;
        if self
            .said
            .is_some_and(|said| said.elapsed() < REFUSED_SAID_EVERY)
        {
            return;
        }
        tracing::warn!(
            %peer,
            cap,
            max,
            refused = self.unsaid,
            "connection refused: the server holds as many as it may"
        );
        self.unsaid = 0;
        self.said = Some(Instant::now());
    }
}

/// The accept loop behind every `serve*` flavour.
fn start<R: Resolver>(listener: TcpListener, builder: ServerBuilder<R>) -> ServerHandle {
    let ServerBuilder {
        resolver,
        tls,
        max_connections,
        max_pending,
    } = builder;
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    // Wakes the connections whose executor has no `write_watch` of its own.
    let bump = WriteSignal::new();
    let resolver = Arc::new(resolver);
    let semaphore = |max: usize| Arc::new(Semaphore::new(max.min(Semaphore::MAX_PERMITS)));
    let connections = max_connections.map(semaphore);
    let pending = semaphore(max_pending);

    let accept_task = tokio::spawn(async move {
        let mut refused = Refused::default();
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        // A place among all the connections, kept until this
                        // one ends, and one among the pending, kept until the
                        // resolver accepts it. Without both the stream is
                        // dropped here: closed before a byte of it is read.
                        let place = match &connections {
                            None => None,
                            Some(connections) => {
                                match Arc::clone(connections).try_acquire_owned() {
                                    Ok(place) => Some(place),
                                    Err(_) => {
                                        let max = max_connections.unwrap_or_default();
                                        refused.refuse(peer, "max_connections", max);
                                        continue;
                                    }
                                }
                            }
                        };
                        let Ok(pending) = Arc::clone(&pending).try_acquire_owned() else {
                            refused.refuse(peer, "max_pending", max_pending);
                            continue;
                        };
                        let _ = stream.set_nodelay(true);
                        let (resolver, tls) = (Arc::clone(&resolver), tls.clone());
                        let (bump, shutdown) = (bump.clone(), shutdown.clone());
                        tokio::spawn(async move {
                            let _place = place;
                            accept(stream, peer, resolver, tls, pending, bump, shutdown).await;
                        });
                    }
                    // Transient errors (EMFILE, ECONNABORTED, …) must not end
                    // the accept loop.
                    Err(err) => {
                        tracing::warn!(error = %err, "accept failed; retrying");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                _ = shutdown.changed() => break,
            }
        }
    });

    ServerHandle {
        accept_task,
        shutdown_tx,
    }
}

/// Completes the (optional) TLS handshake, then serves the connection.
async fn accept<R: Resolver>(
    stream: TcpStream,
    peer: SocketAddr,
    resolver: Arc<R>,
    tls: Option<TlsAcceptor>,
    pending: OwnedSemaphorePermit,
    bump: WriteSignal,
    shutdown: watch::Receiver<bool>,
) {
    let stream = match tls {
        None => Either::Left(stream),
        Some(acceptor) => {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(session)) => Either::Right(session),
                Ok(Err(err)) => {
                    tracing::debug!(%peer, ?err, "TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "TLS handshake timed out");
                    return;
                }
            }
        }
    };
    connection(stream, peer, resolver, pending, bump, shutdown).await;
}

type Wire = Framed<ServerStream, LengthDelimitedCodec>;

/// Reads the first frame, which must be `Hello`: its correlation id and token.
/// `None` closes the connection (timeout, garbage, or a protocol violation).
async fn hello_phase(wire: &mut Wire, peer: SocketAddr) -> Option<(u64, Option<String>)> {
    let item = match tokio::time::timeout(HELLO_TIMEOUT, wire.next()).await {
        Ok(Some(Ok(bytes))) => bytes,
        Ok(Some(Err(err))) => {
            // A transport error, or a first frame longer than a hello.
            tracing::debug!(%peer, %err, "no hello read, closing connection");
            return None;
        }
        Ok(None) => return None,
        Err(_) => {
            tracing::debug!(%peer, "no hello within the timeout, closing connection");
            return None;
        }
    };
    let frame: ClientFrame = match crate::wire::decode_tagged(RecordKind::ClientFrame, &item) {
        Ok(frame) => frame,
        Err(err) => {
            tracing::debug!(%peer, ?err, "undecodable first frame, closing connection");
            return None;
        }
    };
    match frame {
        ClientFrame::Request {
            id,
            request: Request::Hello { token },
        } => Some((id, token.map(|t| t.0))),
        _ => {
            tracing::debug!(%peer, "first frame is not hello, closing connection");
            None
        }
    }
}

/// Resolves when the session's close signal turns `true`; never, without one
/// (or once its sender is gone).
async fn closed(close: &mut Option<watch::Receiver<bool>>) {
    if let Some(rx) = close {
        if rx.wait_for(|closed| *closed).await.is_ok() {
            return;
        }
    }
    std::future::pending::<()>().await
}

/// One connection: the `Hello` exchange that resolves its executor, then a
/// writer task draining the outbound queue, a notify task forwarding write
/// notifications, and the inbound read loop spawning a handler per request.
/// `pending` is its place among the connections not accepted yet.
async fn connection<R: Resolver>(
    stream: ServerStream,
    peer: SocketAddr,
    resolver: Arc<R>,
    pending: OwnedSemaphorePermit,
    bump: WriteSignal,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut framed = Framed::new(stream, hello_codec());

    // Hello: the first frame names the credential; the resolver turns it into
    // the executor this connection is served with, or a refusal.
    let hello = tokio::select! {
        hello = hello_phase(&mut framed, peer) => hello,
        _ = shutdown.changed() => None,
    };
    let Some((hello_id, token)) = hello else {
        return;
    };
    let session = match resolver.resolve(&Hello { token, peer }).await {
        Ok(session) => session,
        Err(rejection) => {
            tracing::debug!(%peer, %rejection, "connection refused");
            let frame = ServerFrame::Response {
                id: hello_id,
                response: Response::Hello(Err(rejection)),
                stable_timestamp: None,
            };
            // The limit is the encoder's too, and a refusal may say more
            // than a hello; nothing is read from the connection any more.
            framed.codec_mut().set_max_frame_length(MAX_FRAME_LENGTH);
            if let Ok(bytes) = encode_tagged(RecordKind::ServerFrame, &frame) {
                let _ = framed.send(Bytes::from(bytes)).await;
            }
            return;
        }
    };
    // Accepted: the connection may send frames of any length the wire
    // allows, and leaves its place to the next dial.
    framed.codec_mut().set_max_frame_length(MAX_FRAME_LENGTH);
    drop(pending);

    let (mut sink, mut inbound) = framed.split();
    let (out_tx, mut out_rx) = mpsc::channel::<ServerFrame>(OUT_CAPACITY);
    let inflight = Arc::new(Semaphore::new(MAX_INFLIGHT_PER_CONNECTION));

    let writer = tokio::spawn(async move {
        // Coalesce: feed this frame plus everything already queued, then flush
        // once — one syscall per burst instead of one per frame.
        'writer: while let Some(frame) = out_rx.recv().await {
            let Ok(bytes) = encode_tagged(RecordKind::ServerFrame, &frame) else {
                continue;
            };
            if sink.feed(Bytes::from(bytes)).await.is_err() {
                break;
            }
            while let Ok(frame) = out_rx.try_recv() {
                let Ok(bytes) = encode_tagged(RecordKind::ServerFrame, &frame) else {
                    continue;
                };
                if sink.feed(Bytes::from(bytes)).await.is_err() {
                    break 'writer;
                }
            }
            if sink.flush().await.is_err() {
                break;
            }
        }
    });

    let Session {
        executor,
        tenant,
        guard: _guard,
        mut close,
    } = session;
    let hello_ok = HelloOk {
        default_routing_key: executor.default_routing_key().map(str::to_owned),
    };
    let frame = ServerFrame::Response {
        id: hello_id,
        response: Response::Hello(Ok(hello_ok.clone())),
        stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
    };
    if out_tx.send(frame).await.is_err() {
        let _ = writer.await;
        return;
    }

    // A backend with its own write signal covers writes from every process
    // sharing the store; the watches are fed from it. Otherwise the Write
    // handlers fire the server-wide signal (see `handle`).
    let bump_on_write = executor.write_watch().is_none();
    // The watches of the connection, by the client's id: each a task that
    // queues its id when a write it follows lands. The notifier turns what
    // is queued into one frame.
    let mut watches: HashMap<u64, AbortHandle> = HashMap::new();
    let (woken_tx, mut woken_rx) = mpsc::channel::<u64>(WOKEN_CAPACITY);
    let notifier = {
        let out_tx = out_tx.clone();
        let executor = executor.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    woken = woken_rx.recv() => {
                        let Some(watch) = woken else { break };
                        // One write often wakes several watches: name them
                        // together.
                        let mut woken = vec![watch];
                        while let Ok(watch) = woken_rx.try_recv() {
                            if !woken.contains(&watch) {
                                woken.push(watch);
                            }
                        }
                        let stable_timestamp =
                            executor.stable_timestamp().await.ok().flatten();
                        let frame = ServerFrame::Notify {
                            watches: woken,
                            stable_timestamp,
                        };
                        if out_tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        })
    };

    loop {
        let item = tokio::select! {
            item = inbound.next() => item,
            _ = closed(&mut close) => {
                tracing::debug!(%peer, "connection closed by the server");
                break;
            }
            _ = shutdown.changed() => break,
        };
        let Some(Ok(bytes)) = item else {
            break; // closed, or a transport error
        };
        let frame: ClientFrame = match crate::wire::decode_tagged(RecordKind::ClientFrame, &bytes) {
            Ok(frame) => frame,
            Err(err) => {
                // An RPC peer speaking garbage is unrecoverable; drop the link.
                tracing::debug!(%peer, ?err, "undecodable frame, closing connection");
                break;
            }
        };
        let (id, request) = match frame {
            ClientFrame::Request { id, request } => (id, request),
            ClientFrame::Watch {
                watch,
                filters,
                routing_key,
                tenant: named,
            } => {
                let interest = WriteInterest {
                    filters: filters.map(Arc::from),
                    routing_key,
                    // A session serving a tenant watches that tenant's
                    // writes, whatever tenant the watch names (see `confine`).
                    tenant: match &tenant {
                        Some(tenant) => TenantFilter::Only(tenant.to_string()),
                        None => named,
                    },
                };
                // An id registered twice is the client's mistake; the later
                // filter replaces the earlier.
                if let Some(replaced) = watches.remove(&watch) {
                    replaced.abort();
                }
                if watches.len() >= MAX_WATCHES_PER_CONNECTION {
                    tracing::warn!(
                        %peer,
                        max = MAX_WATCHES_PER_CONNECTION,
                        "too many watches on one connection: the watch is not registered \
                         and its subscription is left to its poll interval"
                    );
                    continue;
                }
                let mut rx = executor
                    .write_watch_for(&interest)
                    .unwrap_or_else(|| bump.subscribe_to(interest));
                let woken_tx = woken_tx.clone();
                let task = tokio::spawn(async move {
                    // Named once before any write: the watcher reads what
                    // was written before the watch was in place.
                    while woken_tx.send(watch).await.is_ok() {
                        if rx.changed().await.is_err() {
                            break;
                        }
                        rx.borrow_and_update();
                    }
                });
                watches.insert(watch, task.abort_handle());
                continue;
            }
            ClientFrame::Unwatch { watch } => {
                if let Some(dropped) = watches.remove(&watch) {
                    dropped.abort();
                }
                continue;
            }
        };

        // A repeated hello is answered from the established session, never
        // resolved again.
        if let Request::Hello { .. } = request {
            let frame = ServerFrame::Response {
                id,
                response: Response::Hello(Ok(hello_ok.clone())),
                stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
            };
            if out_tx.send(frame).await.is_err() {
                break;
            }
            continue;
        }

        // Bound concurrent handlers: at the cap, stop pulling frames until one
        // finishes (backpressure), instead of spawning without limit.
        let Ok(permit) = Arc::clone(&inflight).acquire_owned().await else {
            break;
        };
        let executor = executor.clone();
        let tenant = tenant.clone();
        let out_tx = out_tx.clone();
        let bump = bump_on_write.then(|| bump.clone());
        tokio::spawn(async move {
            let _permit = permit;
            let response = handle(&executor, tenant.as_deref(), request, bump.as_ref()).await;
            let frame = ServerFrame::Response {
                id,
                response,
                stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
            };
            let _ = out_tx.send(frame).await;
        });
    }

    notifier.abort();
    for (_, watch) in watches {
        watch.abort();
    }
    drop(out_tx);
    let _ = writer.await;
}

fn err_string<T>(result: anyhow::Result<T>) -> Result<T, String> {
    result.map_err(|e| format!("{e:#}"))
}

/// Why a session serving a tenant refuses `replicate`.
const REPLICATE_REFUSED: &str = "replicate is refused on a connection serving a tenant";

/// The answer a session serving a tenant gives the subscriber calls.
fn subscribers_refused<T>() -> Result<T, String> {
    Err(
        "subscriptions are not served on a connection serving a tenant: keep their \
         cursors in a store of your own (a Sidecar)"
            .to_owned(),
    )
}

/// Puts a request of a session serving `tenant` in that tenant, whatever
/// tenant it names, or answers it with a refusal when it would reach past the
/// tenant (see [`Session::tenant`]).
fn confine(request: &mut Request, tenant: &str) -> Option<Response> {
    match request {
        Request::Hello { .. } => None,
        Request::Write { events } => {
            for event in events {
                event.tenant = tenant.to_owned();
            }
            None
        }
        Request::Replicate { .. } => Some(Response::Write(Err(WireWriteError::Other(
            REPLICATE_REFUSED.to_owned(),
        )))),
        Request::Read { tenant: named, .. } | Request::LatestTimestamp { tenant: named, .. } => {
            *named = TenantFilter::Only(tenant.to_owned());
            None
        }
        Request::LatestVersion { tenant: named, .. }
        | Request::StreamRoutingKey { tenant: named, .. }
        | Request::GetSnapshot { tenant: named, .. }
        | Request::SaveSnapshot { tenant: named, .. }
        | Request::DeleteSnapshot { tenant: named, .. } => {
            *named = tenant.to_owned();
            None
        }
        Request::GetSubscriberCursor { .. } => {
            Some(Response::SubscriberCursor(subscribers_refused()))
        }
        Request::IsSubscriberRunning { .. } => {
            Some(Response::SubscriberRunning(subscribers_refused()))
        }
        Request::SubscriberStatus { .. } => Some(Response::SubscriberStatus(subscribers_refused())),
        Request::UpsertSubscriber { .. } => Some(Response::Unit(subscribers_refused())),
        Request::Acknowledge { .. } => Some(Response::Acknowledge(subscribers_refused())),
    }
}

/// What a client of a session serving a tenant reads: that tenant's events, in
/// the default tenant (see [`Session::tenant`]).
fn present(response: &mut Response) {
    if let Response::Read(Ok(page)) = response {
        for event in &mut page.events {
            event.tenant.clear();
        }
    }
}

/// Serves one request with the session's executor, in the session's tenant
/// when it serves one (see [`confine`] and [`present`]). A write that went
/// through fires `bump`, the server-wide signal of the executors that have
/// no `write_watch` of their own.
async fn handle<E: Executor>(
    executor: &E,
    tenant: Option<&str>,
    mut request: Request,
    bump: Option<&WriteSignal>,
) -> Response {
    if let Some(tenant) = tenant {
        if let Some(refusal) = confine(&mut request, tenant) {
            return refusal;
        }
    }
    // Picked once the events carry the tenant they are written in.
    let wake = match (&request, bump) {
        (Request::Write { events } | Request::Replicate { events }, Some(bump)) => {
            Some(bump.prepare(events))
        }
        _ => None,
    };
    let mut response = dispatch(executor, request).await;
    if let (Some(wake), Response::Write(Ok(()))) = (wake, &response) {
        wake.fire();
    }
    if tenant.is_some() {
        present(&mut response);
    }
    response
}

/// Serves one request with the session's executor, its tenants as given.
async fn dispatch<E: Executor>(executor: &E, request: Request) -> Response {
    match request {
        Request::Hello { .. } => unreachable!("Hello is answered inline by the read loop"),
        Request::Write { events } => Response::Write(
            executor
                .write(events)
                .await
                .map_err(|e| WireWriteError::from(&e)),
        ),
        Request::Replicate { events } => Response::Write(
            executor
                .replicate(events)
                .await
                .map_err(|e| WireWriteError::from(&e)),
        ),
        Request::Read {
            aggregators,
            routing_key,
            tenant,
            args,
            to_micros,
        } => {
            // The bound comes from the client's cached watermark, which may
            // have been reported by another node (a reconnect through a load
            // balancer) or be a round trip stale. This node only vouches for
            // its own watermark, so a bounded read never goes past it: events
            // below the client's bound that this node has not applied yet are
            // read on a later pass instead of being skipped.
            let to_micros = match to_micros {
                Some(bound) => match executor.stable_timestamp().await {
                    Ok(Some(own)) => Some(bound.min(own)),
                    _ => Some(bound),
                },
                None => None,
            };
            Response::Read(err_string(
                executor
                    .read(
                        aggregators.map(Arc::from),
                        routing_key,
                        tenant,
                        args,
                        to_micros,
                    )
                    .await
                    .map(WireReadResult::from),
            ))
        }
        Request::LatestTimestamp {
            aggregators,
            routing_key,
            tenant,
        } => Response::LatestTimestamp(err_string(
            executor
                .latest_timestamp(aggregators.map(Arc::from), routing_key, tenant)
                .await,
        )),
        Request::GetSubscriberCursor { key } => {
            Response::SubscriberCursor(err_string(executor.get_subscriber_cursor(key).await))
        }
        Request::IsSubscriberRunning { key, worker_id } => Response::SubscriberRunning(err_string(
            executor.is_subscriber_running(key, worker_id).await,
        )),
        Request::SubscriberStatus { key, worker_id } => {
            Response::SubscriberStatus(err_string(executor.subscriber_status(key, worker_id).await))
        }
        Request::LatestVersion {
            tenant,
            aggregate_type,
            aggregate_id,
        } => Response::LatestVersion(err_string(
            executor
                .latest_version(tenant, aggregate_type, aggregate_id)
                .await,
        )),
        Request::StreamRoutingKey {
            tenant,
            aggregate_type,
            aggregate_id,
        } => Response::StreamRoutingKey(err_string(
            executor
                .stream_routing_key(tenant, aggregate_type, aggregate_id)
                .await,
        )),
        Request::UpsertSubscriber { key, worker_id } => {
            Response::Unit(err_string(executor.upsert_subscriber(key, worker_id).await))
        }
        Request::Acknowledge {
            key,
            worker_id,
            cursor,
            lag,
        } => Response::Acknowledge(err_string(
            executor.acknowledge(key, worker_id, cursor, lag).await,
        )),
        Request::GetSnapshot {
            tenant,
            aggregate_type,
            projection,
            aggregate_revision,
            id,
        } => Response::Snapshot(err_string(
            executor
                .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
                .await,
        )),
        Request::SaveSnapshot {
            tenant,
            aggregate_type,
            projection,
            aggregate_revision,
            id,
            data,
            cursor,
        } => Response::Unit(err_string(
            executor
                .save_snapshot(
                    tenant,
                    aggregate_type,
                    projection,
                    aggregate_revision,
                    id,
                    data,
                    cursor,
                )
                .await,
        )),
        Request::DeleteSnapshot {
            tenant,
            aggregate_type,
            projection,
            id,
        } => Response::Unit(err_string(
            executor
                .delete_snapshot(tenant, aggregate_type, projection, id)
                .await,
        )),
    }
}

#[cfg(test)]
mod tests {
    use evento_core::cursor::{Args, Value};
    use evento_core::{Event, TenantFilter};
    use ulid::Ulid;

    use super::{confine, present};
    use crate::wire::{Request, Response, WireReadResult, WireWriteError};

    fn acme() -> String {
        "acme".to_owned()
    }

    #[test]
    fn a_tenant_session_acts_in_its_tenant_whatever_a_request_names() {
        let mut write = Request::Write {
            events: vec![
                Event::default(),
                Event {
                    tenant: "other".to_owned(),
                    ..Default::default()
                },
            ],
        };
        assert!(confine(&mut write, "acme").is_none());
        let Request::Write { events } = write else {
            unreachable!()
        };
        assert!(events.iter().all(|e| e.tenant == "acme"));

        for tenant in [TenantFilter::All, TenantFilter::Only("other".to_owned())] {
            let mut read = Request::Read {
                aggregators: None,
                routing_key: None,
                tenant: tenant.clone(),
                args: Args::forward(1, None),
                to_micros: None,
            };
            assert!(confine(&mut read, "acme").is_none());
            assert!(
                matches!(&read, Request::Read { tenant: TenantFilter::Only(t), .. } if *t == acme())
            );
            let mut latest = Request::LatestTimestamp {
                aggregators: None,
                routing_key: None,
                tenant,
            };
            assert!(confine(&mut latest, "acme").is_none());
            assert!(
                matches!(&latest, Request::LatestTimestamp { tenant: TenantFilter::Only(t), .. } if *t == acme())
            );
        }

        let other = || "other".to_owned();
        let mut requests = [
            Request::LatestVersion {
                tenant: other(),
                aggregate_type: other(),
                aggregate_id: other(),
            },
            Request::StreamRoutingKey {
                tenant: other(),
                aggregate_type: other(),
                aggregate_id: other(),
            },
            Request::GetSnapshot {
                tenant: other(),
                aggregate_type: other(),
                projection: other(),
                aggregate_revision: other(),
                id: other(),
            },
            Request::SaveSnapshot {
                tenant: other(),
                aggregate_type: other(),
                projection: other(),
                aggregate_revision: other(),
                id: other(),
                data: vec![],
                cursor: Value(String::new()),
            },
            Request::DeleteSnapshot {
                tenant: other(),
                aggregate_type: other(),
                projection: other(),
                id: other(),
            },
        ];
        for request in &mut requests {
            assert!(confine(request, "acme").is_none());
            let (Request::LatestVersion { tenant, .. }
            | Request::StreamRoutingKey { tenant, .. }
            | Request::GetSnapshot { tenant, .. }
            | Request::SaveSnapshot { tenant, .. }
            | Request::DeleteSnapshot { tenant, .. }) = &*request
            else {
                unreachable!()
            };
            assert_eq!(*tenant, acme(), "{request:?}");
        }
    }

    #[test]
    fn a_tenant_session_reads_its_events_in_the_default_tenant() {
        let event = |tenant: &str| Event {
            tenant: tenant.to_owned(),
            ..Default::default()
        };
        let mut response = Response::Read(Ok(WireReadResult {
            cursors: vec![Value(String::new()), Value(String::new())],
            events: vec![event("acme"), event("acme")],
            page_info: Default::default(),
        }));
        present(&mut response);
        let Response::Read(Ok(page)) = response else {
            unreachable!()
        };
        assert!(page.events.iter().all(|e| e.tenant.is_empty()));
    }

    #[test]
    fn a_tenant_session_refuses_replicate_and_the_subscriber_calls() {
        let key = || "key".to_owned();
        let worker_id = Ulid::nil();
        let refusal = |mut request: Request| confine(&mut request, "acme");

        assert!(matches!(
            refusal(Request::Replicate {
                events: vec![Event::default()]
            }),
            Some(Response::Write(Err(WireWriteError::Other(_))))
        ));
        assert!(matches!(
            refusal(Request::GetSubscriberCursor { key: key() }),
            Some(Response::SubscriberCursor(Err(_)))
        ));
        assert!(matches!(
            refusal(Request::IsSubscriberRunning {
                key: key(),
                worker_id
            }),
            Some(Response::SubscriberRunning(Err(_)))
        ));
        assert!(matches!(
            refusal(Request::SubscriberStatus {
                key: key(),
                worker_id
            }),
            Some(Response::SubscriberStatus(Err(_)))
        ));
        assert!(matches!(
            refusal(Request::UpsertSubscriber {
                key: key(),
                worker_id
            }),
            Some(Response::Unit(Err(_)))
        ));
        assert!(matches!(
            refusal(Request::Acknowledge {
                key: key(),
                worker_id,
                cursor: Value(String::new()),
                lag: 0
            }),
            Some(Response::Acknowledge(Err(_)))
        ));
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;
    use evento_core::{Event, EventFilter, RoutingKey};

    type Wire = Framed<TcpStream, LengthDelimitedCodec>;

    async fn send(wire: &mut Wire, frame: ClientFrame) {
        let bytes = encode_tagged(RecordKind::ClientFrame, &frame).unwrap();
        wire.send(Bytes::from(bytes)).await.unwrap();
    }

    async fn next(wire: &mut Wire) -> ServerFrame {
        let bytes = tokio::time::timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("a frame within 5s")
            .expect("the connection is open")
            .unwrap();
        crate::wire::decode_tagged(RecordKind::ServerFrame, &bytes).unwrap()
    }

    /// The watches of the next `Notify`; a response met on the way is skipped.
    async fn notified(wire: &mut Wire) -> Vec<u64> {
        loop {
            if let ServerFrame::Notify { mut watches, .. } = next(wire).await {
                watches.sort_unstable();
                return watches;
            }
        }
    }

    /// A `Hello` and its reply: the frames sent before it have been taken in
    /// by then, the read loop handles them in order.
    async fn settle(wire: &mut Wire, id: u64) {
        send(
            wire,
            ClientFrame::Request {
                id,
                request: Request::Hello { token: None },
            },
        )
        .await;
        loop {
            if let ServerFrame::Response { id: answered, .. } = next(wire).await {
                if answered == id {
                    return;
                }
            }
        }
    }

    /// A client's side of a connection, nothing said yet.
    async fn dial(addr: SocketAddr) -> Wire {
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LENGTH)
            .new_codec();
        Framed::new(TcpStream::connect(addr).await.unwrap(), codec)
    }

    fn watch(watch: u64, aggregate_type: &str) -> ClientFrame {
        ClientFrame::Watch {
            watch,
            filters: Some(vec![EventFilter::by_type_raw(aggregate_type)]),
            routing_key: Some(RoutingKey::Value(None)),
            tenant: TenantFilter::All,
        }
    }

    fn event(aggregate_type: &str, id: &str) -> Event {
        Event {
            id: ulid::Ulid::generate(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: id.to_owned(),
            version: 1,
            name: "Probed".to_owned(),
            ..Default::default()
        }
    }

    /// Issue #535: a `Notify` names the watches a write wakes, a connection
    /// that watches nothing is sent none, and a watch dropped with `Unwatch`
    /// is no longer named.
    #[tokio::test]
    async fn a_notify_names_the_watches_a_write_wakes() {
        let store = evento_fjall::Fjall::temporary().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = serve(listener, store.clone());
        let mut wire = dial(addr).await;
        settle(&mut wire, 0).await;

        // Nothing is watched yet: the write is answered by nothing.
        store
            .write(vec![event("test/Account", "a0")])
            .await
            .unwrap();
        let quiet = tokio::time::timeout(Duration::from_millis(300), wire.next()).await;
        assert!(
            quiet.is_err(),
            "a connection that watches nothing is sent nothing"
        );

        // A watch is named once as it is registered.
        send(&mut wire, watch(1, "test/Account")).await;
        assert_eq!(notified(&mut wire).await, [1]);
        send(&mut wire, watch(2, "test/Account")).await;
        assert_eq!(notified(&mut wire).await, [2]);
        send(&mut wire, watch(3, "test/Card")).await;
        assert_eq!(notified(&mut wire).await, [3]);

        store
            .write(vec![event("test/Account", "a1")])
            .await
            .unwrap();
        let mut woken = notified(&mut wire).await;
        if woken.len() < 2 {
            // The two watches were queued either side of the notifier's wake.
            woken.extend(notified(&mut wire).await);
            woken.sort_unstable();
        }
        assert_eq!(woken, [1, 2]);

        send(&mut wire, ClientFrame::Unwatch { watch: 2 }).await;
        settle(&mut wire, 1).await;
        store
            .write(vec![event("test/Account", "a2")])
            .await
            .unwrap();
        assert_eq!(notified(&mut wire).await, [1]);
        store.write(vec![event("test/Card", "c1")]).await.unwrap();
        assert_eq!(notified(&mut wire).await, [3]);
    }
}

/// Issue #615: what a connection may make the server hold before its
/// resolver has accepted it, and how many connections the server holds.
#[cfg(test)]
mod limit_tests {
    use super::*;
    use evento_core::Event;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Wire = Framed<TcpStream, LengthDelimitedCodec>;

    fn hello(id: u64) -> Bytes {
        let frame = ClientFrame::Request {
            id,
            request: Request::Hello { token: None },
        };
        Bytes::from(encode_tagged(RecordKind::ClientFrame, &frame).unwrap())
    }

    /// Dials and says hello: the connection once the server has answered, or
    /// `None` when the server closed it instead.
    async fn connect(addr: SocketAddr) -> Option<Wire> {
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LENGTH)
            .new_codec();
        let mut wire = Framed::new(TcpStream::connect(addr).await.unwrap(), codec);
        wire.send(hello(0)).await.ok()?;
        let reply = tokio::time::timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("an answer or a close within 5s");
        reply?.ok().map(|_| wire)
    }

    /// Dials until the server answers a hello: a place freed by a connection
    /// that went away is free once the server has seen it go.
    async fn connect_in_time(addr: SocketAddr) -> Wire {
        for _ in 0..100 {
            if let Some(wire) = connect(addr).await {
                return wire;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no place for a connection within 5s");
    }

    /// Whether the server closes `stream` within two seconds, well short of
    /// the hello timeout.
    async fn closed(stream: &mut TcpStream) -> bool {
        let mut buf = [0u8; 16];
        // A close with bytes left unread is a reset: an error, like the end.
        matches!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await,
            Ok(Ok(0) | Err(_))
        )
    }

    async fn listen() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    #[tokio::test]
    async fn a_first_frame_longer_than_a_hello_closes_the_connection() {
        let (listener, addr) = listen().await;
        let _server = serve(listener, evento_fjall::Fjall::temporary().unwrap());

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let announced = u32::try_from(MAX_FRAME_LENGTH).unwrap();
        stream.write_all(&announced.to_be_bytes()).await.unwrap();
        assert!(
            closed(&mut stream).await,
            "the head alone is refused, none of the frame is waited for"
        );

        // One byte past a hello is past the limit too.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let announced = u32::try_from(MAX_HELLO_LENGTH + 1).unwrap();
        stream.write_all(&announced.to_be_bytes()).await.unwrap();
        assert!(closed(&mut stream).await);
    }

    #[tokio::test]
    async fn an_accepted_connection_sends_frames_longer_than_a_hello() {
        let (listener, addr) = listen().await;
        let _server = serve(listener, evento_fjall::Fjall::temporary().unwrap());
        let mut wire = connect(addr).await.expect("the server answers a hello");

        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".to_owned(),
            aggregate_id: "a0".to_owned(),
            version: 1,
            name: "Probed".to_owned(),
            data: vec![7; 64 * MAX_HELLO_LENGTH],
            ..Default::default()
        };
        let frame = ClientFrame::Request {
            id: 1,
            request: Request::Write {
                events: vec![event],
            },
        };
        let bytes = encode_tagged(RecordKind::ClientFrame, &frame).unwrap();
        assert!(bytes.len() > MAX_HELLO_LENGTH);
        wire.send(Bytes::from(bytes)).await.unwrap();
        let bytes = tokio::time::timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("a reply within 5s")
            .expect("the connection is open")
            .unwrap();
        let reply: ServerFrame =
            crate::wire::decode_tagged(RecordKind::ServerFrame, &bytes).unwrap();
        assert!(
            matches!(
                reply,
                ServerFrame::Response {
                    id: 1,
                    response: Response::Write(Ok(())),
                    ..
                }
            ),
            "{reply:?}"
        );
    }

    #[tokio::test]
    async fn a_dial_past_the_pending_cap_is_closed_at_once() {
        let (listener, addr) = listen().await;
        let _server = ServerBuilder::open(evento_fjall::Fjall::temporary().unwrap())
            .max_pending(2)
            .serve(listener);

        // Accepted connections hold no pending place.
        let mut established = Vec::new();
        for _ in 0..3 {
            established.push(connect(addr).await.expect("the server answers a hello"));
        }

        let silent = [
            TcpStream::connect(addr).await.unwrap(),
            TcpStream::connect(addr).await.unwrap(),
        ];
        let mut refused = TcpStream::connect(addr).await.unwrap();
        assert!(
            closed(&mut refused).await,
            "a third connection that has not said hello is refused"
        );
        assert!(
            connect(addr).await.is_none(),
            "a hello has no place either while two are pending"
        );

        // A pending connection that goes away leaves its place.
        let [gone, _kept] = silent;
        drop(gone);
        connect_in_time(addr).await;
    }

    #[tokio::test]
    async fn a_dial_past_the_connection_cap_is_closed_at_once() {
        let (listener, addr) = listen().await;
        let _server = ServerBuilder::open(evento_fjall::Fjall::temporary().unwrap())
            .max_connections(2)
            .serve(listener);

        let first = connect(addr).await.expect("the server answers a hello");
        let _second = connect(addr).await.expect("the server answers a hello");
        let mut refused = TcpStream::connect(addr).await.unwrap();
        assert!(closed(&mut refused).await, "a third connection is refused");

        drop(first);
        connect_in_time(addr).await;
    }
}
