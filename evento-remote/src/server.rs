//! Server side: expose executors over framed TCP, plain or TLS.
//!
//! Every connection starts with a [`Hello`] exchange: the client presents an
//! optional token, and a [`Resolver`] decides which executor that connection
//! is served with — or refuses it with a [`Rejection`]. [`serve`] is the
//! one-executor case (an open server, every connection gets the same store);
//! [`serve_with`] takes the resolver that a host authenticating credentials
//! and multiplexing stores needs. The `_tls` variants complete a rustls
//! handshake first, mirroring `evento-accord`'s `serve`/`serve_tls` split.
//!
//! Requests on one connection are handled **concurrently** (spawn per
//! request) — multiple application tasks share one client connection, and a
//! slow `write` (e.g. a consensus round on an Accord-backed server) must not
//! head-of-line-block unrelated reads; correlation ids make out-of-order
//! replies safe.
//!
//! Writes are pushed to each connection as [`ServerFrame::Notify`]: from its
//! executor's own `write_watch` when it has one, otherwise from a server-wide
//! channel bumped after each successful `write` handled by any connection
//! (still low-latency for writes through this server; writers bypassing it
//! are covered by the client's poll-interval fallback, the same caveat every
//! `write_watch` backend documents). A connection without a `write_watch`
//! may thus be woken by a write to another executor; the watch is
//! level-triggered, so a spurious wakeup costs one empty poll.

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use evento_core::Executor;
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::either::Either;

use crate::wire::{
    encode_tagged, ClientFrame, HelloOk, RecordKind, Rejection, Request, Response, ServerFrame,
    WireReadResult, WireWriteError, MAX_FRAME_LENGTH,
};

/// Capacity of a connection's outbound queue. Sends into it are awaited, so a
/// slow client backpressures its own request handlers instead of shedding
/// replies.
const OUT_CAPACITY: usize = 1024;

/// Maximum concurrently-executing request handlers per connection. Without a
/// cap a client pipelining arbitrarily many large frames could spawn unbounded
/// tasks; at the cap the read loop stops pulling frames (TCP backpressure).
const MAX_INFLIGHT_PER_CONNECTION: usize = 256;

/// A dialer that never completes the TLS handshake, or never sends `Hello`,
/// must not pin a task forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// Inbound connection: plain TCP, or a server-side TLS session over it.
type ServerStream = Either<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
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
    guard: Option<Box<dyn Any + Send + Sync>>,
    close: Option<watch::Receiver<bool>>,
}

impl<E> Session<E> {
    /// A session over `executor`, kept for the whole connection.
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            guard: None,
            close: None,
        }
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
struct Static<E>(E);

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
    start(listener, resolver, None)
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
    start(listener, resolver, Some(acceptor))
}

/// The accept loop behind every `serve*` flavour.
fn start<R: Resolver>(
    listener: TcpListener,
    resolver: R,
    tls: Option<TlsAcceptor>,
) -> ServerHandle {
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    // Wakes the connections whose executor has no `write_watch` of its own.
    let (bump_tx, _) = watch::channel(0u64);
    let resolver = Arc::new(resolver);

    let accept_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        tokio::spawn(accept(
                            stream,
                            peer,
                            Arc::clone(&resolver),
                            tls.clone(),
                            bump_tx.clone(),
                            shutdown.clone(),
                        ));
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
    bump_tx: watch::Sender<u64>,
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
    connection(stream, peer, resolver, bump_tx, shutdown).await;
}

type Inbound = SplitStream<Framed<ServerStream, LengthDelimitedCodec>>;

/// Reads the first frame, which must be `Hello`: its correlation id and token.
/// `None` closes the connection (timeout, garbage, or a protocol violation).
async fn hello_phase(inbound: &mut Inbound, peer: SocketAddr) -> Option<(u64, Option<String>)> {
    let item = match tokio::time::timeout(HELLO_TIMEOUT, inbound.next()).await {
        Ok(Some(Ok(bytes))) => bytes,
        Ok(_) => return None, // closed, or a transport error
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
        ClientFrame::Request { .. } => {
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

/// One established connection: a writer task draining the outbound queue, the
/// `Hello` exchange that resolves its executor, a notify task forwarding write
/// notifications, and the inbound read loop spawning a handler per request.
async fn connection<R: Resolver>(
    stream: ServerStream,
    peer: SocketAddr,
    resolver: Arc<R>,
    bump_tx: watch::Sender<u64>,
    mut shutdown: watch::Receiver<bool>,
) {
    let framed = Framed::new(stream, codec());
    let (mut sink, mut inbound) = framed.split();
    let (out_tx, mut out_rx) = mpsc::channel::<ServerFrame>(OUT_CAPACITY);
    let inflight = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_PER_CONNECTION));

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

    // Hello: the first frame names the credential; the resolver turns it into
    // the executor this connection is served with, or a refusal.
    let hello = tokio::select! {
        hello = hello_phase(&mut inbound, peer) => hello,
        _ = shutdown.changed() => None,
    };
    let Some((hello_id, token)) = hello else {
        drop(out_tx);
        let _ = writer.await;
        return;
    };
    let session = match resolver.resolve(&Hello { token, peer }).await {
        Ok(session) => session,
        Err(rejection) => {
            tracing::debug!(%peer, %rejection, "connection refused");
            let _ = out_tx
                .send(ServerFrame::Response {
                    id: hello_id,
                    response: Response::Hello(Err(rejection)),
                    stable_timestamp: None,
                })
                .await;
            drop(out_tx);
            let _ = writer.await;
            return;
        }
    };
    let Session {
        executor,
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

    // A backend with its own write_watch covers writes from every process
    // sharing the store; forward it. Otherwise the Write handlers bump the
    // server-wide channel (see `handle`).
    let bump_on_write = executor.write_watch().is_none();
    let mut notify_rx = executor
        .write_watch()
        .unwrap_or_else(|| bump_tx.subscribe());
    let notifier = {
        let out_tx = out_tx.clone();
        let executor = executor.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = notify_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let generation = *notify_rx.borrow_and_update();
                        let stable_timestamp =
                            executor.stable_timestamp().await.ok().flatten();
                        let frame = ServerFrame::Notify {
                            generation,
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
        let ClientFrame::Request { id, request } = frame;

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
        let out_tx = out_tx.clone();
        let bump_tx = bump_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let is_write = matches!(request, Request::Write { .. } | Request::Replicate { .. });
            let response = handle(&executor, request).await;
            if bump_on_write && is_write && matches!(response, Response::Write(Ok(()))) {
                bump_tx.send_modify(|g| *g += 1);
            }
            let frame = ServerFrame::Response {
                id,
                response,
                stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
            };
            let _ = out_tx.send(frame).await;
        });
    }

    notifier.abort();
    drop(out_tx);
    let _ = writer.await;
}

fn err_string<T>(result: anyhow::Result<T>) -> Result<T, String> {
    result.map_err(|e| format!("{e:#}"))
}

async fn handle<E: Executor>(executor: &E, request: Request) -> Response {
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
            args,
            to_micros,
        } => Response::Read(err_string(
            executor
                .read(aggregators.map(Arc::from), routing_key, args, to_micros)
                .await
                .map(WireReadResult::from),
        )),
        Request::LatestTimestamp {
            aggregators,
            routing_key,
        } => Response::LatestTimestamp(err_string(
            executor
                .latest_timestamp(aggregators.map(Arc::from), routing_key)
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
            aggregate_type,
            aggregate_id,
        } => Response::LatestVersion(err_string(
            executor.latest_version(aggregate_type, aggregate_id).await,
        )),
        Request::StreamRoutingKey {
            aggregate_type,
            aggregate_id,
        } => Response::StreamRoutingKey(err_string(
            executor
                .stream_routing_key(aggregate_type, aggregate_id)
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
            aggregate_type,
            projection,
            aggregate_revision,
            id,
        } => Response::Snapshot(err_string(
            executor
                .get_snapshot(aggregate_type, projection, aggregate_revision, id)
                .await,
        )),
        Request::SaveSnapshot {
            aggregate_type,
            projection,
            aggregate_revision,
            id,
            data,
            cursor,
        } => Response::Unit(err_string(
            executor
                .save_snapshot(
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
            aggregate_type,
            projection,
            id,
        } => Response::Unit(err_string(
            executor
                .delete_snapshot(aggregate_type, projection, id)
                .await,
        )),
    }
}
