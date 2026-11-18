//! Production transport: length-delimited framed TCP behind the
//! [`MessageSink`] trait, for a real multi-process cluster.
//!
//! Each frame is a bitcode-serialised `Frame` (sender id + [`Message`]) over a
//! [`LengthDelimitedCodec`] stream. Outbound delivery is per-peer and lazy: the
//! first message to a peer spawns a writer task that connects on demand and
//! reconnects after a drop. Delivery is best-effort and unacknowledged — exactly
//! what the consensus layer assumes, since quorums and recovery tolerate loss.
//!
//! Membership is static: construct a [`TcpTransport`] with a fixed
//! `NodeId → PeerAddr` map. A peer is an address or a `host:port` name
//! ([`PeerAddr`]); a name is resolved at each dial, so a peer that comes back
//! at another address is found again. Inbound frames are decoded and forwarded
//! to a node's inbox by [`serve`].
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
use std::time::{Duration, Instant};

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
use crate::format::{decode_tagged, encode_tagged, RecordKind};
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

/// Where a peer is dialled: a socket address, or a `host:port` name.
///
/// A name is resolved **at each dial**, not once at start-up: a peer that
/// comes back at another address (a pod rescheduled under the same name) is
/// reached again without restarting the others, and a name that does not
/// resolve yet is a peer that cannot be reached for now, redialled like a
/// refused connection.
///
/// Parses from and serializes as the string a configuration file holds:
///
/// ```
/// use evento_cluster::PeerAddr;
///
/// let ip: PeerAddr = "10.0.0.1:7000".parse().unwrap();
/// assert!(matches!(ip, PeerAddr::Socket(_)));
/// let name: PeerAddr = "node-1.cluster.internal:7000".parse().unwrap();
/// assert_eq!(name.to_string(), "node-1.cluster.internal:7000");
/// assert!("node-1.cluster.internal".parse::<PeerAddr>().is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PeerAddr {
    /// An IP address and a port, dialled as given.
    Socket(SocketAddr),
    /// A host name and a port, resolved at each dial.
    Host {
        /// The name to resolve.
        host: String,
        /// The port.
        port: u16,
    },
}

impl PeerAddr {
    /// The socket address, when this is one; `None` for a name.
    pub fn as_socket(&self) -> Option<SocketAddr> {
        match self {
            Self::Socket(addr) => Some(*addr),
            Self::Host { .. } => None,
        }
    }

    /// The port.
    pub fn port(&self) -> u16 {
        match self {
            Self::Socket(addr) => addr.port(),
            Self::Host { port, .. } => *port,
        }
    }
}

impl From<SocketAddr> for PeerAddr {
    fn from(addr: SocketAddr) -> Self {
        Self::Socket(addr)
    }
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Socket(addr) => addr.fmt(f),
            Self::Host { host, port } => write!(f, "{host}:{port}"),
        }
    }
}

impl std::str::FromStr for PeerAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(Self::Socket(addr));
        }
        let (host, port) = s
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("{s:?} is not `host:port` or `ip:port`"))?;
        // What is left with a colon or a bracket is an IPv6 address written
        // wrong, not a name.
        anyhow::ensure!(
            !host.is_empty()
                && !host
                    .contains(|c: char| c.is_whitespace() || matches!(c, ':' | '[' | ']' | '/')),
            "{s:?} is not `host:port` or `ip:port`"
        );
        let port = port
            .parse()
            .map_err(|_| anyhow::anyhow!("{s:?} has no valid port after the last `:`"))?;
        Ok(Self::Host {
            host: host.to_owned(),
            port,
        })
    }
}

impl Serialize for PeerAddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PeerAddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
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
    encode_tagged(RecordKind::WireFrame, frame)
}

/// Decodes an inbound wire frame into `(from, message)`.
fn decode_wire(bytes: &[u8]) -> anyhow::Result<(NodeId, Message)> {
    let frame: Frame = decode_tagged(RecordKind::WireFrame, bytes)?;
    Ok((frame.from, frame.message))
}

/// Capacity of a per-peer outbound queue and of an inbound inbox — the
/// backpressure bound. When a peer is slow or unreachable its queue fills and
/// further frames are dropped (loss is tolerated), so memory stays bounded
/// instead of growing without limit.
pub const CHANNEL_CAPACITY: usize = 1024;

/// Maximum frame size, matching `evento-remote`: a sanity bound well above
/// anything the protocol sends on purpose. A bootstrap snapshot crosses in
/// pages budgeted far below it (`NodeConfig::snapshot_page_bytes`); a frame
/// over the cap is shed and counted at `enqueue` rather than handed to the
/// codec, whose refusal would otherwise tear down the connection and take
/// the frames batched with it.
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

/// Bound on an outbound dial: the TCP connect plus, over TLS, the client
/// handshake. Without it a peer behind a dropping firewall (a partition) pins
/// its writer for the kernel's SYN retries — about two minutes — while every
/// frame queued for it is shed.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Bound on writing one batch. A peer that stops acknowledging (partitioned, or
/// paused) lets writes fill the kernel's send buffer, then blocks the flush on
/// TCP's retransmit backoff — minutes, even after the network heals. Past this
/// bound the connection is treated as dead and redialled. A full sync page
/// (`NodeConfig::snapshot_page_bytes`) flushes far faster on any live link.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// First wait before redialling a peer that could not be reached; doubles per
/// failed dial up to [`RECONNECT_BACKOFF_MAX`], and resets on a connect. Batches
/// queued meanwhile are dropped without dialling, so a dead peer costs one
/// dial per backoff instead of one per batch.
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(100);

/// Cap on the redial backoff: small, so a restarted peer is reached again
/// within about a second.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// The outbound side of a [`TcpTransport`]: one lazily-connected, bounded
/// writer queue per peer.
struct PeerLinks {
    from: NodeId,
    peers: Arc<HashMap<NodeId, PeerAddr>>,
    /// Per-peer outbound queues (bounded); each backed by a lazily-spawned
    /// writer. Carries pre-encoded frames (`Bytes`), so a broadcast encodes
    /// once and every peer's queue shares the same buffer.
    senders: Mutex<HashMap<NodeId, mpsc::Sender<Bytes>>>,
    /// Client TLS settings; `None` for plaintext.
    tls: Option<TlsClient>,
    /// Observability counters; bumps `messages_shed` when a full peer queue drops a
    /// frame.
    metrics: Arc<Metrics>,
    /// Per-peer queue bound (see [`CHANNEL_CAPACITY`]).
    queue_capacity: usize,
}

impl PeerLinks {
    fn new<A: Into<PeerAddr>>(
        from: NodeId,
        peers: HashMap<NodeId, A>,
        tls: Option<TlsClient>,
    ) -> Self {
        let peers = peers.into_iter().map(|(id, addr)| (id, addr.into()));
        Self {
            from,
            peers: Arc::new(peers.collect()),
            senders: Mutex::new(HashMap::new()),
            tls,
            metrics: Arc::new(Metrics::new()),
            queue_capacity: CHANNEL_CAPACITY,
        }
    }

    /// The outbound queue for `to`, spawning its writer task on first use.
    /// `None` if `to` is not a known peer.
    fn writer_for(&self, to: NodeId) -> Option<mpsc::Sender<Bytes>> {
        let mut senders = self.senders.lock().expect("senders poisoned");
        if let Some(tx) = senders.get(&to) {
            return Some(tx.clone());
        }
        let addr = self.peers.get(&to)?.clone();
        let (tx, rx) = mpsc::channel(self.queue_capacity);
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
        if bytes.len() > MAX_FRAME_LENGTH {
            tracing::warn!(
                peer = to.0,
                len = bytes.len(),
                cap = MAX_FRAME_LENGTH,
                "frame over the transport cap; dropped"
            );
            self.metrics.record_shed();
            return;
        }
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

    /// Enqueues one pre-encoded frame for every node in `nodes` (the `Bytes`
    /// clones share the buffer).
    fn broadcast(&self, nodes: &[NodeId], bytes: Bytes) {
        for &to in nodes {
            self.enqueue(to, bytes.clone());
        }
    }
}

/// A framed-TCP [`MessageSink`] over a static membership map, optionally over TLS.
pub struct TcpTransport {
    links: PeerLinks,
}

impl TcpTransport {
    /// Builds a plaintext transport for `from` that can reach the nodes in
    /// `peers`, each a `SocketAddr` or a [`PeerAddr`].
    pub fn new<A: Into<PeerAddr>>(from: NodeId, peers: HashMap<NodeId, A>) -> Self {
        Self {
            links: PeerLinks::new(from, peers, None),
        }
    }

    /// Builds a transport whose outbound connections use mutual TLS (`tls`), for
    /// an untrusted network. Pair with [`serve_tls`] on the inbound side.
    pub fn with_tls<A: Into<PeerAddr>>(
        from: NodeId,
        peers: HashMap<NodeId, A>,
        tls: TlsClient,
    ) -> Self {
        Self {
            links: PeerLinks::new(from, peers, Some(tls)),
        }
    }

    /// Shares an external [`Metrics`] with this transport so its shed count lands in
    /// the same snapshot as the node's counters. Use the node's
    /// [`metrics_handle`](crate::node::Node::metrics_handle):
    ///
    /// ```rust,no_run
    /// # use std::collections::HashMap;
    /// # use std::sync::Arc;
    /// # use evento_cluster::{
    /// #     HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Node,
    /// #     NodeId, StaticTopology, TcpTransport,
    /// # };
    /// # let id = NodeId(0);
    /// # let peers: HashMap<NodeId, std::net::SocketAddr> = HashMap::new();
    /// # let net = InMemoryNetwork::new();
    /// # let node = Node::new(
    /// #     id,
    /// #     Arc::new(StaticTopology::new(id, vec![id])),
    /// #     Arc::new(HybridLogicalClock::new(id)),
    /// #     Arc::new(net.sink(id)),
    /// #     Arc::new(InMemoryDataStore::new()),
    /// #     Arc::new(InMemoryJournal::new()),
    /// # );
    /// let transport = TcpTransport::new(id, peers).with_metrics(node.metrics_handle());
    /// ```
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.links.metrics = metrics;
        self
    }

    /// A point-in-time snapshot of this transport's observability counters (only the
    /// shed count is transport-driven; the rest come from a shared node, if any).
    pub fn metrics(&self) -> MetricsSnapshot {
        self.links.metrics.snapshot()
    }
}

#[async_trait]
impl MessageSink for TcpTransport {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        let bytes = encode(&Frame {
            from: self.links.from,
            message,
        })?;
        self.links.enqueue(to, Bytes::from(bytes));
        Ok(())
    }

    async fn broadcast(&self, nodes: &[NodeId], message: Message) -> anyhow::Result<()> {
        // One encode for the whole fan-out; `Bytes` clones share the buffer.
        // Valid because `Frame.from` is constant for this transport, so every
        // peer receives the identical frame.
        let bytes = Bytes::from(encode(&Frame {
            from: self.links.from,
            message,
        })?);
        self.links.broadcast(nodes, bytes);
        Ok(())
    }
}

/// Establishes one outbound connection to `addr`, wrapping it in client TLS when
/// configured, within [`CONNECT_TIMEOUT`]. `None` on a resolution, connect or
/// handshake failure, or a timeout.
async fn connect(to: NodeId, addr: &PeerAddr, tls: &Option<TlsClient>) -> Option<ClientStream> {
    match tokio::time::timeout(CONNECT_TIMEOUT, dial(to, addr, tls)).await {
        Ok(Ok(stream)) => Some(stream),
        Ok(Err(err)) => {
            tracing::debug!(peer = to.0, %addr, %err, "connect failed");
            None
        }
        Err(_) => {
            tracing::debug!(peer = to.0, %addr, "connect timed out");
            None
        }
    }
}

/// The unbounded dial behind [`connect`].
async fn dial(
    to: NodeId,
    addr: &PeerAddr,
    tls: &Option<TlsClient>,
) -> std::io::Result<ClientStream> {
    let stream = match addr {
        PeerAddr::Socket(addr) => TcpStream::connect(addr).await?,
        // Resolved here, at each dial, and every address it resolves to is
        // tried: the peer may have come back at another one.
        PeerAddr::Host { host, port } => TcpStream::connect((host.as_str(), *port)).await?,
    };
    let _ = stream.set_nodelay(true);
    match tls {
        None => Ok(Either::Left(stream)),
        Some(tls) => {
            let session = tls
                .connector
                .connect(tls.server_name.clone(), stream)
                .await?;
            // If `to`'s certificate is pinned, the server must present exactly it —
            // otherwise this is a CA-valid impostor at the address; drop the link.
            if let Some(pins) = &tls.peer_certs {
                if let Some(expected) = pins.get(&to) {
                    if !leaf_matches(session.get_ref().1.peer_certificates(), expected) {
                        return Err(std::io::Error::other(
                            "peer certificate does not match its pin",
                        ));
                    }
                }
            }
            Ok(Either::Right(session))
        }
    }
}

/// Drains a peer's outbound queue to a (TLS or plain) connection, connecting on
/// demand and reconnecting after a failure. Frames are written in batches (the
/// first queued frame plus everything already waiting, one flush), so a burst
/// costs one syscall instead of one per frame.
///
/// A write that fails is **retried once over a fresh connection**: the usual
/// cause is a stale connection to a peer that restarted, where the first write
/// after the restart fails. Without the retry that frame was dropped and only
/// the *next* one reconnected — on a multiplexed transport one connection
/// carries every group, so one stale connection cost every tenant a frame. A
/// batch whose peer cannot be reached at all (connect fails) is dropped; loss is
/// tolerated by quorums and recovery.
///
/// A peer named by host is resolved at each dial, so the redial after a
/// failure finds a peer that came back at another address; a name that does
/// not resolve is a failed dial like any other.
///
/// Nothing here waits on the network unbounded: a dial gives up after
/// [`CONNECT_TIMEOUT`] and a batch write after [`WRITE_TIMEOUT`] (which counts
/// as a failed write), and after a failed dial the peer is not redialled until
/// its backoff ([`RECONNECT_BACKOFF_MIN`]..[`RECONNECT_BACKOFF_MAX`]) elapses.
/// Otherwise a partitioned peer could pin this writer for minutes after the
/// partition heals, shedding every frame meant for it.
async fn peer_writer(
    to: NodeId,
    addr: PeerAddr,
    mut rx: mpsc::Receiver<Bytes>,
    tls: Option<TlsClient>,
) {
    let mut conn: Option<Framed<ClientStream, LengthDelimitedCodec>> = None;
    let mut backoff = RECONNECT_BACKOFF_MIN;
    // While set and in the future, batches are dropped without dialling.
    let mut retry_at: Option<Instant> = None;
    // Logged on the transition only, not once per dropped batch.
    let mut unreachable = false;
    while let Some(first) = rx.recv().await {
        // Coalesce this frame with everything already queued.
        let mut batch = vec![first];
        while let Ok(bytes) = rx.try_recv() {
            batch.push(bytes);
        }
        for attempt in 0..2 {
            if conn.is_none() {
                if retry_at.is_some_and(|at| Instant::now() < at) {
                    // Still backing off: drop the batch, dial on a later one.
                    break;
                }
                let Some(stream) = connect(to, &addr, &tls).await else {
                    // Unreachable right now: drop the batch, redial after the
                    // backoff.
                    retry_at = Some(Instant::now() + backoff);
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                    if !unreachable {
                        unreachable = true;
                        tracing::warn!(peer = to.0, %addr, "peer unreachable; dropping its frames");
                    }
                    break;
                };
                conn = Some(Framed::new(stream, codec()));
                backoff = RECONNECT_BACKOFF_MIN;
                retry_at = None;
                if unreachable {
                    unreachable = false;
                    tracing::info!(peer = to.0, %addr, "peer reachable again");
                }
            }
            let framed = conn.as_mut().expect("connected above");
            let err = match tokio::time::timeout(WRITE_TIMEOUT, write_batch(framed, &batch)).await {
                Ok(Ok(())) => break,
                Ok(Err(err)) => err,
                Err(_) => std::io::Error::new(std::io::ErrorKind::TimedOut, "write timed out"),
            };
            // Stale, broken or stalled connection: reconnect and resend once.
            conn = None;
            if attempt == 1 {
                tracing::warn!(
                    peer = to.0,
                    frames = batch.len(),
                    %err,
                    "write failed again after reconnecting; dropping the batch"
                );
            }
        }
    }
}

/// Feeds every frame of `batch`, then flushes once.
async fn write_batch(
    framed: &mut Framed<ClientStream, LengthDelimitedCodec>,
    batch: &[Bytes],
) -> std::io::Result<()> {
    for bytes in batch {
        framed.feed(bytes.clone()).await?;
    }
    // `LengthDelimitedCodec` encodes both `Bytes` and `&[u8]` (tokio-util 0.7.20+),
    // so `Framed` is a `Sink` for two item types; name the one we feed.
    SinkExt::<Bytes>::flush(framed).await
}

// ---------------------------------------------------------------------------
// Inbound
// ---------------------------------------------------------------------------

/// Server-side TLS for an accept loop: the acceptor, and the per-node pins when
/// identities are verified (`None` trusts the wire `from`).
type ServerTls = (TlsAcceptor, Option<Arc<PeerCerts>>);

/// The one accept loop behind every `serve*` flavour: accepts connections on
/// `listener`, optionally completes a (verified) TLS handshake, then reads frames
/// into `inbox`. Returns the accept-loop task handle.
fn accept_loop(
    listener: TcpListener,
    inbox: mpsc::Sender<Envelope>,
    tls: Option<ServerTls>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    let inbox = inbox.clone();
                    match tls.clone() {
                        None => {
                            tokio::spawn(read_connection(Either::Left(stream), inbox, None));
                        }
                        Some((acceptor, pins)) => {
                            tokio::spawn(async move {
                                // Bounded handshake: a hung dialer must not pin this
                                // task forever.
                                let Ok(Ok(session)) = tokio::time::timeout(
                                    HANDSHAKE_TIMEOUT,
                                    acceptor.accept(stream),
                                )
                                .await
                                else {
                                    return;
                                };
                                // With pins, authenticate the peer by its leaf
                                // certificate; an unknown certificate (CA-valid but
                                // un-pinned) is refused.
                                let identity = match &pins {
                                    None => None,
                                    Some(pins) => {
                                        let Some(id) = authenticated_node(
                                            session.get_ref().1.peer_certificates(),
                                            pins,
                                        ) else {
                                            return;
                                        };
                                        Some(id)
                                    }
                                };
                                read_connection(Either::Right(session), inbox, identity).await;
                            });
                        }
                    }
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

/// Accepts inbound connections on `listener`, decoding frames and forwarding
/// each as an [`Envelope`] to `inbox`. Returns the accept-loop task handle.
pub fn serve(listener: TcpListener, inbox: mpsc::Sender<Envelope>) -> JoinHandle<()> {
    accept_loop(listener, inbox, None)
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
    accept_loop(listener, inbox, Some((acceptor, None)))
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
    accept_loop(listener, inbox, Some((acceptor, Some(Arc::new(peers)))))
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
        let Ok((from, message)) = decode_wire(&bytes) else {
            continue;
        };
        let envelope = Envelope {
            from: identity.unwrap_or(from),
            message,
        };
        match inbox.try_send(envelope) {
            Ok(()) => {}
            // A full inbox is backpressure — shed this frame but keep the
            // connection; only a closed inbox (node gone) ends the loop.
            Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Timestamp, TxnId};

    fn applied(n: u64) -> Message {
        Message::Applied {
            txn: TxnId(Timestamp {
                micros: n,
                logical: 0,
                node: NodeId(1),
            }),
        }
    }

    /// A frame over the cap never reaches the codec (whose refusal would tear
    /// the connection down with the batch around it): it is shed and counted.
    #[test]
    fn an_oversized_frame_is_shed_not_queued() {
        let links = PeerLinks::new(NodeId(0), HashMap::<NodeId, PeerAddr>::new(), None);
        links.enqueue(NodeId(1), Bytes::from_static(b"small"));
        assert_eq!(links.metrics.snapshot().messages_shed, 0);
        links.enqueue(NodeId(1), Bytes::from(vec![0u8; MAX_FRAME_LENGTH + 1]));
        assert_eq!(links.metrics.snapshot().messages_shed, 1);
    }

    #[test]
    fn a_peer_address_parses_as_an_address_or_a_name() {
        let ip: PeerAddr = "10.0.0.1:7000".parse().unwrap();
        assert_eq!(ip, PeerAddr::Socket("10.0.0.1:7000".parse().unwrap()));
        let v6: PeerAddr = "[::1]:7000".parse().unwrap();
        assert_eq!(v6.as_socket(), Some("[::1]:7000".parse().unwrap()));
        let name: PeerAddr = "timada-1.timada:7000".parse().unwrap();
        assert_eq!(
            name,
            PeerAddr::Host {
                host: "timada-1.timada".into(),
                port: 7000
            }
        );
        assert_eq!(name.as_socket(), None);
        assert_eq!(name.port(), 7000);
        assert_eq!(name.to_string(), "timada-1.timada:7000");

        for bad in [
            "timada-1",
            ":7000",
            "timada-1:",
            "timada-1:port",
            "::1:7000",
            "a b:7000",
        ] {
            assert!(bad.parse::<PeerAddr>().is_err(), "{bad}");
        }
    }

    #[test]
    fn a_wire_frame_decodes_and_a_journal_record_does_not() {
        let frame = encode(&Frame {
            from: NodeId(4),
            message: applied(1),
        })
        .unwrap();
        let (from, message) = decode_wire(&frame).unwrap();
        assert_eq!(from, NodeId(4));
        assert!(matches!(message, Message::Applied { txn } if txn.0.micros == 1));

        let journal_record = encode_tagged(RecordKind::Command, &1u64).unwrap();
        assert!(decode_wire(&journal_record).is_err());
    }
}
