//! Production transport: length-delimited framed TCP behind the
//! [`MessageSink`] trait, for a real multi-process cluster.
//!
//! Each frame is a bitcode-serialised [`Frame`] (sender id + [`Message`]) over a
//! [`LengthDelimitedCodec`] stream. Outbound delivery is per-peer and lazy: the
//! first message to a peer spawns a writer task that connects on demand and
//! reconnects after a drop. Delivery is best-effort and unacknowledged — exactly
//! what the consensus layer assumes, since quorums and recovery tolerate loss.
//!
//! Membership is static: construct a [`TcpTransport`] with a fixed
//! `NodeId → SocketAddr` map. Inbound frames are decoded and forwarded to a
//! node's inbox by [`serve`].
//!
//! **Mutual TLS (optional).** Build with [`TcpTransport::with_tls`] and serve with
//! [`serve_tls`] to encrypt and authenticate inter-node traffic: every connection
//! is wrapped in rustls, with each side presenting a certificate the other
//! verifies against a shared root (the operator supplies the rustls
//! [`TlsConnector`]/[`TlsAcceptor`]). Plain [`new`](TcpTransport::new)/[`serve`]
//! stay available for trusted networks; the framing and consensus layers are
//! identical either way.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::either::Either;

use crate::api::MessageSink;
use crate::clock::NodeId;
use crate::message::Message;
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::transport::Envelope;

/// Outbound connection: plain TCP, or a client-side TLS session over it.
type ClientStream = Either<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;
/// Inbound connection: plain TCP, or a server-side TLS session over it.
type ServerStream = Either<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;

/// Client-side TLS settings: the rustls connector and the server name presented
/// for certificate verification.
#[derive(Clone)]
pub struct TlsClient {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl TlsClient {
    /// Builds client TLS settings from a rustls connector and the peer server
    /// name to verify against.
    pub fn new(connector: TlsConnector, server_name: ServerName<'static>) -> Self {
        Self {
            connector,
            server_name,
        }
    }
}

/// A wire frame: the originating node plus the message. The sender id travels in
/// the frame so the receiver can reply without a separate handshake.
#[derive(Serialize, Deserialize)]
struct Frame {
    from: NodeId,
    message: Message,
}

fn encode(frame: &Frame) -> anyhow::Result<Vec<u8>> {
    crate::format::encode_tagged(crate::format::RecordKind::WireFrame, frame)
}

fn decode(bytes: &[u8]) -> anyhow::Result<Frame> {
    crate::format::decode_tagged(crate::format::RecordKind::WireFrame, bytes)
}

/// Capacity of a per-peer outbound queue and of an inbound inbox — the
/// backpressure bound. When a peer is slow or unreachable its queue fills and
/// further frames are dropped (loss is tolerated), so memory stays bounded
/// instead of growing without limit.
pub const CHANNEL_CAPACITY: usize = 1024;

/// A framed-TCP [`MessageSink`] over a static membership map, optionally over TLS.
pub struct TcpTransport {
    from: NodeId,
    peers: Arc<HashMap<NodeId, SocketAddr>>,
    /// Per-peer outbound queues (bounded); each backed by a lazily-spawned writer.
    senders: Mutex<HashMap<NodeId, mpsc::Sender<Frame>>>,
    /// Client TLS settings; `None` for plaintext.
    tls: Option<TlsClient>,
    /// Observability counters; bumps `messages_shed` when a full peer queue drops a
    /// frame. Defaults to a private set; share the node's via [`with_metrics`](Self::with_metrics).
    metrics: Arc<Metrics>,
}

impl TcpTransport {
    /// Builds a plaintext transport for `from` that can reach the nodes in `peers`.
    pub fn new(from: NodeId, peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            from,
            peers: Arc::new(peers),
            senders: Mutex::new(HashMap::new()),
            tls: None,
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Builds a transport whose outbound connections use mutual TLS (`tls`), for
    /// an untrusted network. Pair with [`serve_tls`] on the inbound side.
    pub fn with_tls(from: NodeId, peers: HashMap<NodeId, SocketAddr>, tls: TlsClient) -> Self {
        Self {
            from,
            peers: Arc::new(peers),
            senders: Mutex::new(HashMap::new()),
            tls: Some(tls),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Shares an external [`Metrics`] with this transport so its shed count lands in
    /// the same snapshot as the node's counters. Use the node's
    /// [`metrics_handle`](crate::node::Node::metrics_handle):
    ///
    /// ```ignore
    /// let transport = TcpTransport::new(id, peers).with_metrics(node.metrics_handle());
    /// ```
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// A point-in-time snapshot of this transport's observability counters (only the
    /// shed count is transport-driven; the rest come from a shared node, if any).
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// The outbound queue for `to`, spawning its writer task on first use.
    /// `None` if `to` is not a known peer.
    fn writer_for(&self, to: NodeId) -> Option<mpsc::Sender<Frame>> {
        let mut senders = self.senders.lock().expect("senders poisoned");
        if let Some(tx) = senders.get(&to) {
            return Some(tx.clone());
        }
        let addr = *self.peers.get(&to)?;
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(peer_writer(addr, rx, self.tls.clone()));
        senders.insert(to, tx.clone());
        Some(tx)
    }
}

#[async_trait]
impl MessageSink for TcpTransport {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        if let Some(tx) = self.writer_for(to) {
            // `try_send` never blocks the caller: a full queue (slow/unreachable
            // peer) sheds the frame, which the protocol tolerates. Count a full-queue
            // shed for backpressure observability (a closed queue is a dropped
            // connection, reconnected lazily — not counted).
            if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(Frame {
                from: self.from,
                message,
            }) {
                self.metrics.record_shed();
            }
        }
        Ok(())
    }
}

/// Establishes one outbound connection to `addr`, wrapping it in client TLS when
/// configured. `None` on a connect/handshake failure.
async fn connect(addr: SocketAddr, tls: &Option<TlsClient>) -> Option<ClientStream> {
    let stream = TcpStream::connect(addr).await.ok()?;
    let _ = stream.set_nodelay(true);
    match tls {
        None => Some(Either::Left(stream)),
        Some(tls) => {
            let session = tls
                .connector
                .connect(tls.server_name.clone(), stream)
                .await
                .ok()?;
            Some(Either::Right(session))
        }
    }
}

/// Drains a peer's outbound queue to a (TLS or plain) connection, connecting on
/// demand and reconnecting after a failure. Frames sent while the peer is
/// unreachable are dropped (loss is tolerated by the protocol).
async fn peer_writer(addr: SocketAddr, mut rx: mpsc::Receiver<Frame>, tls: Option<TlsClient>) {
    let mut conn: Option<Framed<ClientStream, LengthDelimitedCodec>> = None;
    while let Some(frame) = rx.recv().await {
        if conn.is_none() {
            match connect(addr, &tls).await {
                Some(stream) => conn = Some(Framed::new(stream, LengthDelimitedCodec::new())),
                None => continue,
            }
        }
        let bytes = match encode(&frame) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if let Some(framed) = conn.as_mut() {
            if framed.send(Bytes::from(bytes)).await.is_err() {
                conn = None;
            }
        }
    }
}

/// Accepts inbound connections on `listener`, decoding frames and forwarding
/// each as an [`Envelope`] to `inbox`. Returns the accept-loop task handle.
pub fn serve(listener: TcpListener, inbox: mpsc::Sender<Envelope>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(read_connection(Either::Left(stream), inbox.clone()));
        }
    })
}

/// Like [`serve`] but completes a TLS handshake (`acceptor`, with client-cert
/// verification for mutual TLS) on each inbound connection before reading frames.
pub fn serve_tls(
    listener: TcpListener,
    inbox: mpsc::Sender<Envelope>,
    acceptor: TlsAcceptor,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            let acceptor = acceptor.clone();
            let inbox = inbox.clone();
            tokio::spawn(async move {
                if let Ok(session) = acceptor.accept(stream).await {
                    read_connection(Either::Right(session), inbox).await;
                }
            });
        }
    })
}

/// Reads framed messages from one inbound connection until it closes.
async fn read_connection(stream: ServerStream, inbox: mpsc::Sender<Envelope>) {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(item) = framed.next().await {
        let bytes = match item {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        match decode(&bytes) {
            Ok(frame) => {
                let envelope = Envelope {
                    from: frame.from,
                    message: frame.message,
                };
                match inbox.try_send(envelope) {
                    Ok(()) => {}
                    // A full inbox is backpressure — shed this frame but keep the
                    // connection; only a closed inbox (node gone) ends the loop.
                    Err(mpsc::error::TrySendError::Full(_)) => continue,
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Err(_) => continue,
        }
    }
}
