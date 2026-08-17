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
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
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

/// Per-node certificate pins: each node's expected **leaf** certificate, by id.
/// The operator supplies the same map cluster-wide (every node controls all node
/// certs). Identity is enforced by DER byte-comparison of the presented leaf —
/// no x509 parsing — so a node can only act as the id whose cert it holds.
pub type PeerCerts = std::collections::HashMap<NodeId, CertificateDer<'static>>;

/// The id whose pinned leaf certificate equals the one presented (the chain's
/// leaf, `[0]`), if any — the authenticated identity of a verified connection.
fn authenticated_node(
    presented: Option<&[CertificateDer<'_>]>,
    pins: &PeerCerts,
) -> Option<NodeId> {
    let leaf = presented?.first()?;
    pins.iter()
        .find(|(_, pinned)| pinned.as_ref() == leaf.as_ref())
        .map(|(id, _)| *id)
}

/// Whether the presented chain's leaf matches the `expected` pinned certificate.
fn leaf_matches(
    presented: Option<&[CertificateDer<'_>]>,
    expected: &CertificateDer<'static>,
) -> bool {
    presented
        .and_then(|c| c.first())
        .is_some_and(|leaf| leaf.as_ref() == expected.as_ref())
}

/// Client-side TLS settings: the rustls connector and the server name presented
/// for certificate verification. Optionally **pins** each peer's leaf certificate
/// (`with_peer_certs`), so the client only trusts a dialed peer if it presents
/// exactly that node's certificate — defeating a CA-valid impostor at the address.
#[derive(Clone)]
pub struct TlsClient {
    connector: TlsConnector,
    server_name: ServerName<'static>,
    peer_certs: Option<Arc<PeerCerts>>,
}

impl TlsClient {
    /// Builds client TLS settings from a rustls connector and the peer server
    /// name to verify against.
    pub fn new(connector: TlsConnector, server_name: ServerName<'static>) -> Self {
        Self {
            connector,
            server_name,
            peer_certs: None,
        }
    }

    /// Pins each peer's expected leaf certificate (see [`PeerCerts`]). A dialed
    /// connection is dropped unless the server presents the pinned certificate for
    /// that node id.
    pub fn with_peer_certs(mut self, peer_certs: PeerCerts) -> Self {
        self.peer_certs = Some(Arc::new(peer_certs));
        self
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

/// Maximum frame size, matching `evento-remote`. The codec default (8 MB) is
/// too small for a bootstrap `SyncData`, which carries the contact's whole
/// materialised event log — an oversized encode would fail forever and the
/// joining node could never bootstrap.
pub const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

/// A length-delimited codec with the raised frame cap, for both directions.
fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}

/// Bound on a TLS handshake, so a hung or malicious dialer cannot pin an accept
/// task forever.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A framed-TCP [`MessageSink`] over a static membership map, optionally over TLS.
pub struct TcpTransport {
    from: NodeId,
    peers: Arc<HashMap<NodeId, SocketAddr>>,
    /// Per-peer outbound queues (bounded); each backed by a lazily-spawned
    /// writer. Carries pre-encoded frames (`Bytes`), so a broadcast encodes
    /// once and every peer's queue shares the same buffer.
    senders: Mutex<HashMap<NodeId, mpsc::Sender<Bytes>>>,
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
    fn writer_for(&self, to: NodeId) -> Option<mpsc::Sender<Bytes>> {
        let mut senders = self.senders.lock().expect("senders poisoned");
        if let Some(tx) = senders.get(&to) {
            return Some(tx.clone());
        }
        let addr = *self.peers.get(&to)?;
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(peer_writer(to, addr, rx, self.tls.clone()));
        senders.insert(to, tx.clone());
        Some(tx)
    }

    /// Enqueues a pre-encoded frame for `to`.
    ///
    /// `try_send` never blocks the caller: a full queue (slow/unreachable
    /// peer) sheds the frame, which the protocol tolerates and the shed
    /// counter records. A closed queue means the writer task died — remove its
    /// entry so the next send respawns it, instead of black-holing this peer
    /// forever.
    fn enqueue(&self, to: NodeId, bytes: Bytes) {
        if let Some(tx) = self.writer_for(to) {
            match tx.try_send(bytes) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.metrics.record_shed();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.senders.lock().expect("senders poisoned").remove(&to);
                }
            }
        }
    }
}

#[async_trait]
impl MessageSink for TcpTransport {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        let bytes = encode(&Frame {
            from: self.from,
            message,
        })?;
        self.enqueue(to, Bytes::from(bytes));
        Ok(())
    }

    async fn broadcast(&self, nodes: &[NodeId], message: Message) -> anyhow::Result<()> {
        // One encode for the whole fan-out; `Bytes` clones share the buffer.
        // Valid because `Frame.from` is constant for this transport, so every
        // peer receives the identical frame.
        let bytes = Bytes::from(encode(&Frame {
            from: self.from,
            message,
        })?);
        for &to in nodes {
            self.enqueue(to, bytes.clone());
        }
        Ok(())
    }
}

/// Establishes one outbound connection to `addr`, wrapping it in client TLS when
/// configured. `None` on a connect/handshake failure.
async fn connect(to: NodeId, addr: SocketAddr, tls: &Option<TlsClient>) -> Option<ClientStream> {
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
            // If `to`'s certificate is pinned, the server must present exactly it —
            // otherwise this is a CA-valid impostor at the address; drop the link.
            if let Some(pins) = &tls.peer_certs {
                if let Some(expected) = pins.get(&to) {
                    if !leaf_matches(session.get_ref().1.peer_certificates(), expected) {
                        return None;
                    }
                }
            }
            Some(Either::Right(session))
        }
    }
}

/// Drains a peer's outbound queue to a (TLS or plain) connection, connecting on
/// demand and reconnecting after a failure. Frames sent while the peer is
/// unreachable are dropped (loss is tolerated by the protocol).
async fn peer_writer(
    to: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Bytes>,
    tls: Option<TlsClient>,
) {
    let mut conn: Option<Framed<ClientStream, LengthDelimitedCodec>> = None;
    while let Some(bytes) = rx.recv().await {
        if conn.is_none() {
            match connect(to, addr, &tls).await {
                Some(stream) => conn = Some(Framed::new(stream, codec())),
                None => continue,
            }
        }
        let Some(framed) = conn.as_mut() else {
            continue;
        };
        // Coalesce: feed this frame plus everything already queued, then flush
        // once — one syscall per burst instead of one per frame.
        if framed.feed(bytes).await.is_err() {
            conn = None;
            continue;
        }
        let mut failed = false;
        while let Ok(bytes) = rx.try_recv() {
            if framed.feed(bytes).await.is_err() {
                failed = true;
                break;
            }
        }
        if failed || framed.flush().await.is_err() {
            conn = None;
        }
    }
}

/// Accepts inbound connections on `listener`, decoding frames and forwarding
/// each as an [`Envelope`] to `inbox`. Returns the accept-loop task handle.
pub fn serve(listener: TcpListener, inbox: mpsc::Sender<Envelope>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    tokio::spawn(read_connection(Either::Left(stream), inbox.clone(), None));
                }
                // Transient errors (EMFILE, ECONNABORTED, …) must not end the
                // accept loop — a node that stops accepting looks alive to
                // peers (it still sends) but silently receives nothing.
                Err(err) => {
                    tracing::warn!(error = %err, "accept failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Like [`serve`] but completes a TLS handshake (`acceptor`, with client-cert
/// verification for mutual TLS) on each inbound connection before reading frames.
/// The wire `from` is **trusted** — for a trusted network, or where every node
/// shares one certificate. Use [`serve_tls_verified`] to authenticate per-node
/// identity instead.
pub fn serve_tls(
    listener: TcpListener,
    inbox: mpsc::Sender<Envelope>,
    acceptor: TlsAcceptor,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    let acceptor = acceptor.clone();
                    let inbox = inbox.clone();
                    tokio::spawn(async move {
                        // Bounded handshake: a hung dialer must not pin this
                        // task forever.
                        let Ok(Ok(session)) =
                            tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                        else {
                            return;
                        };
                        read_connection(Either::Right(session), inbox, None).await;
                    });
                }
                Err(err) => {
                    tracing::warn!(error = %err, "accept failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Like [`serve_tls`] but **authenticates each peer's identity** by pinning: after
/// the handshake the client's presented leaf certificate must equal one of `peers`,
/// and the matched [`NodeId`] — *not* the self-declared wire `from` — is stamped on
/// every [`Envelope`]. A connection whose certificate matches no pin is dropped, so
/// a CA-valid outsider cannot join, and no node can frame messages as another.
pub fn serve_tls_verified(
    listener: TcpListener,
    inbox: mpsc::Sender<Envelope>,
    acceptor: TlsAcceptor,
    peers: PeerCerts,
) -> JoinHandle<()> {
    let peers = Arc::new(peers);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    let acceptor = acceptor.clone();
                    let inbox = inbox.clone();
                    let peers = Arc::clone(&peers);
                    tokio::spawn(async move {
                        let Ok(Ok(session)) =
                            tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                        else {
                            return;
                        };
                        // Authenticate the peer by its pinned leaf certificate; an unknown
                        // certificate (CA-valid but un-pinned) is refused.
                        let Some(id) =
                            authenticated_node(session.get_ref().1.peer_certificates(), &peers)
                        else {
                            return;
                        };
                        read_connection(Either::Right(session), inbox, Some(id)).await;
                    });
                }
                Err(err) => {
                    tracing::warn!(error = %err, "accept failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Reads framed messages from one inbound connection until it closes. When
/// `identity` is `Some`, every envelope is stamped with that **authenticated** id
/// (the wire `from` is ignored — impersonation-proof); otherwise the wire `from` is
/// used.
async fn read_connection(
    stream: ServerStream,
    inbox: mpsc::Sender<Envelope>,
    identity: Option<NodeId>,
) {
    let mut framed = Framed::new(stream, codec());
    while let Some(item) = framed.next().await {
        let bytes = match item {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        match decode(&bytes) {
            Ok(frame) => {
                let envelope = Envelope {
                    from: identity.unwrap_or(frame.from),
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
