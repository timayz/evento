//! Production transport: length-delimited framed TCP behind the
//! [`MessageSink`] trait, for a real multi-process cluster.
//!
//! Each frame is a bitcode-serialised `Frame` (sender id + [`Message`]) over a
//! [`LengthDelimitedCodec`] stream. Outbound delivery is per-peer and lazy: the
//! first message to a peer spawns a writer task that connects on demand and
//! reconnects after a drop. A frame for a peer whose connection is being
//! dialled waits a moment for it ([`FRAME_HOLD`]) before it is dropped.
//! Delivery is best-effort and unacknowledged — exactly what the consensus
//! layer assumes, since quorums and recovery tolerate loss.
//!
//! Construct a [`TcpTransport`] with a `NodeId → PeerAddr` map, which takes
//! a node added since ([`TcpTransport::set_peer`]). A peer is an address or a `host:port` name
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
//! identical either way. [`TcpTransport::with_tls_named`] with
//! [`serve_tls_named`] is the same over a [`NodeTls`]: each node is who its
//! certificate names, a certificate is renewed while the node runs, and a
//! link a certificate keeps down is said apart from a peer that is away
//! ([`TcpTransport::refusal`]). It is what the host opens a node with.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
use crate::tls::{self, LinkRefusal, NodeTls};
use crate::transport::Envelope;

/// Outbound connection: plain TCP, or a client-side TLS session over it.
type ClientStream = Either<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;
/// An outbound connection, framed.
type ClientLink = Framed<ClientStream, LengthDelimitedCodec>;
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

/// How a transport's outbound connections are secured.
#[derive(Clone)]
enum ClientTls {
    /// One connector and one server name for every peer, with their
    /// certificates pinned or not.
    Pinned(TlsClient),
    /// Each peer under the name it is dialled at.
    Named(NodeTls),
}

/// How long a refusal of a peer's inbound connection stands when it is not
/// said again: the peer redials within its backoff, so one that stopped
/// being refused either got through or went away.
const REFUSAL_STANDS: Duration = Duration::from_secs(10);

/// The least time between two log lines about inbound connections refused
/// before they said which node they are.
const UNNAMED_SAID: Duration = Duration::from_secs(30);

/// Why the link with a peer is refused, each way.
#[derive(Default)]
struct Refused {
    /// What the last dial of the peer met.
    dialled: Option<LinkRefusal>,
    /// What the peer's last connection to this node was refused for, and when.
    dialling: Option<(LinkRefusal, Instant)>,
}

/// The links of a transport that a certificate keeps down, by peer: what
/// tells them from peers that are away. Said in the log when a refusal
/// begins or changes, not at each redial.
#[derive(Default)]
struct Refusals {
    by_peer: Mutex<HashMap<NodeId, Refused>>,
    /// When an inbound connection refused before it named its node was last
    /// said in the log.
    unnamed: Mutex<Option<Instant>>,
}

impl Refusals {
    /// What the last dial of `peer` met: `None` when it connected, or met
    /// the network.
    fn dialled(&self, peer: NodeId, refusal: Option<LinkRefusal>) {
        let mut by_peer = self.by_peer.lock().expect("refusals poisoned");
        let held = by_peer.entry(peer).or_default();
        if held.dialled == refusal {
            return;
        }
        match &refusal {
            // A node waiting to be added: its host says so.
            Some(LinkRefusal::NotKnown) => tracing::info!(
                peer = peer.0,
                "the peer does not know this node yet and refuses its connection"
            ),
            Some(refusal) => tracing::warn!(
                peer = peer.0,
                %refusal,
                "the link to a peer is refused, and the peer is not away"
            ),
            None => {}
        }
        held.dialled = refusal;
    }

    /// What `peer`'s connection to this node was refused for; `None` when
    /// it was accepted.
    fn dialling(&self, peer: NodeId, refusal: Option<LinkRefusal>) {
        let mut by_peer = self.by_peer.lock().expect("refusals poisoned");
        let held = by_peer.entry(peer).or_default();
        let before = held.dialling.take().map(|(refusal, _)| refusal);
        match (&before, &refusal) {
            (before, Some(refusal)) if before.as_ref() != Some(refusal) => tracing::warn!(
                peer = peer.0,
                %refusal,
                "a peer's connection is refused, and the peer is not away"
            ),
            (Some(_), None) => {
                tracing::info!(peer = peer.0, "a peer's connection is accepted again")
            }
            _ => {}
        }
        held.dialling = refusal.map(|refusal| (refusal, Instant::now()));
    }

    /// An inbound connection from `remote` failed its handshake before it
    /// said which node it is.
    fn unnamed(&self, remote: SocketAddr, err: &std::io::Error) {
        // Anything else is the network, or a probe that sent nothing.
        let Some(refusal) = LinkRefusal::of(err) else {
            tracing::debug!(%remote, %err, "inbound handshake failed");
            return;
        };
        let mut said = self.unnamed.lock().expect("refusals poisoned");
        if said.is_some_and(|at| at.elapsed() < UNNAMED_SAID) {
            return;
        }
        *said = Some(Instant::now());
        tracing::warn!(
            %remote,
            %refusal,
            "a connection to this node is refused before it said which node it is"
        );
    }

    /// Why the link with `peer` is refused, if it is: what dialling it met,
    /// else what its own connection was lately refused for.
    fn of(&self, peer: NodeId) -> Option<LinkRefusal> {
        let by_peer = self.by_peer.lock().expect("refusals poisoned");
        let held = by_peer.get(&peer)?;
        held.dialled.clone().or_else(|| {
            held.dialling
                .as_ref()
                .filter(|(_, at)| at.elapsed() < REFUSAL_STANDS)
                .map(|(refusal, _)| refusal.clone())
        })
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

/// Bound on a TLS handshake, on both sides: a hung or malicious dialler
/// cannot pin an accept task forever, nor a peer that went silent once
/// connected its writer. The dialler's covers the peer's answer to its first
/// frame as well ([`TcpTransport::with_tls_named`]).
///
/// Its own bound, well above [`CONNECT_TIMEOUT`]: a handshake is work both
/// processes do, and on a machine short of CPU it takes seconds. Under the
/// connect's two seconds it was given up midway and begun again at every
/// dial, each one costing both nodes the CPU they were short of, and the
/// link never came up (#610). A peer that took the connection is there; it
/// is given the time to finish.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on the TCP connect of an outbound dial. Without it a peer behind a
/// dropping firewall (a partition) pins its writer for the kernel's SYN
/// retries — about two minutes — while every frame queued for it is shed.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Bound on resolving the name of a peer named by host, at each dial: the
/// time a resolver gives one server by default.
pub const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

/// The least time between two log lines about a peer whose dials keep
/// failing: said when it begins, then again this often with what the last
/// dial met, for as long as it lasts.
pub const DIAL_SAID: Duration = Duration::from_secs(30);

/// Bound on writing one batch. A peer that stops acknowledging (partitioned, or
/// paused) lets writes fill the kernel's send buffer, then blocks the flush on
/// TCP's retransmit backoff — minutes, even after the network heals. Past this
/// bound the connection is treated as dead and redialled. A full sync page
/// (`NodeConfig::snapshot_page_bytes`) flushes far faster on any live link.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// First wait before redialling a peer that could not be reached; doubles per
/// failed dial up to [`RECONNECT_BACKOFF_MAX`], and resets on a connect. Frames
/// queued meanwhile wait for the redial ([`FRAME_HOLD`]), so a dead peer costs
/// one dial per backoff instead of one per batch.
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(100);

/// Cap on the redial backoff: small, so a restarted peer is reached again
/// within about a second.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// How long a frame waits for its peer's connection, from when it was queued:
/// one capped backoff and a dial. A node just back from a partition asks its
/// first round into links that come up a few hundred milliseconds later; sent
/// then, the round is answered, where dropped it ran out its timeout with
/// every peer reachable (#603). Past this the frame is dropped: its round has
/// given up or is about to, and a peer that stays away is not owed a backlog.
pub const FRAME_HOLD: Duration = Duration::from_secs(2);

/// A frame in a peer's outbound queue, and when it was put there.
struct Queued {
    at: Instant,
    bytes: Bytes,
}

/// A peer's outbound queue, and whether its writer holds a connection.
#[derive(Clone)]
struct Link {
    tx: mpsc::Sender<Queued>,
    up: Arc<AtomicBool>,
}

/// The outbound side of a [`TcpTransport`]: one lazily-connected, bounded
/// writer queue per peer.
struct PeerLinks {
    from: NodeId,
    /// Where each peer is dialled. A peer is added, or moved, while the
    /// transport runs ([`TcpTransport::set_peer`]).
    peers: RwLock<HashMap<NodeId, PeerAddr>>,
    /// Per-peer outbound queues (bounded); each backed by a lazily-spawned
    /// writer. Carries pre-encoded frames (`Bytes`), so a broadcast encodes
    /// once and every peer's queue shares the same buffer.
    senders: Mutex<HashMap<NodeId, Link>>,
    /// Client TLS settings; `None` for plaintext.
    tls: Option<ClientTls>,
    /// The links a certificate keeps down.
    refusals: Arc<Refusals>,
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
        tls: Option<ClientTls>,
    ) -> Self {
        let peers = peers.into_iter().map(|(id, addr)| (id, addr.into()));
        Self {
            from,
            peers: RwLock::new(peers.collect()),
            senders: Mutex::new(HashMap::new()),
            tls,
            refusals: Arc::default(),
            metrics: Arc::new(Metrics::new()),
            queue_capacity: CHANNEL_CAPACITY,
        }
    }

    /// The outbound queue for `to`, spawning its writer task on first use.
    /// `None` if `to` is not a known peer.
    fn writer_for(&self, to: NodeId) -> Option<mpsc::Sender<Queued>> {
        let mut senders = self.senders.lock().expect("senders poisoned");
        if let Some(link) = senders.get(&to) {
            return Some(link.tx.clone());
        }
        let addr = self.peers.read().expect("peers poisoned").get(&to)?.clone();
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        let up = Arc::new(AtomicBool::new(false));
        tokio::spawn(peer_writer(
            self.from,
            to,
            addr,
            rx,
            self.tls.clone(),
            up.clone(),
            self.refusals.clone(),
        ));
        senders.insert(to, Link { tx: tx.clone(), up });
        Some(tx)
    }

    /// Whether the writer for `to` holds a connection.
    fn reaches(&self, to: NodeId) -> bool {
        self.senders
            .lock()
            .expect("senders poisoned")
            .get(&to)
            .is_some_and(|link| link.up.load(Ordering::Relaxed))
    }

    /// Dials `to` at `addr` from now on; `true` when that is news. A writer
    /// on another address ends with its queue (the frames it holds are shed,
    /// as on any broken link), and the next send dials the new one.
    fn set_peer(&self, to: NodeId, addr: PeerAddr) -> bool {
        // Both locks, senders first as in `writer_for`: a send between the
        // two would spawn a writer on the address being replaced.
        let mut senders = self.senders.lock().expect("senders poisoned");
        let mut peers = self.peers.write().expect("peers poisoned");
        if peers.get(&to) == Some(&addr) {
            return false;
        }
        peers.insert(to, addr);
        senders.remove(&to);
        true
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
            let queued = Queued {
                at: Instant::now(),
                bytes,
            };
            match tx.try_send(queued) {
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

/// A framed-TCP [`MessageSink`] over a map of where each node is dialled,
/// optionally over TLS. The map takes a node while the transport runs
/// ([`set_peer`](TcpTransport::set_peer)).
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
            links: PeerLinks::new(from, peers, Some(ClientTls::Pinned(tls))),
        }
    }

    /// Builds a transport whose outbound connections use mutual TLS where
    /// each peer is who its certificate names ([`NodeTls`]): a peer is
    /// accepted when its certificate, issued by the cluster's authority, is
    /// valid for the host or the address it is dialled at, and once the peer
    /// accepted this node's the same way. Pair with [`serve_tls_named`] on
    /// the inbound side, over this same transport.
    pub fn with_tls_named<A: Into<PeerAddr>>(
        from: NodeId,
        peers: HashMap<NodeId, A>,
        tls: NodeTls,
    ) -> Self {
        Self {
            links: PeerLinks::new(from, peers, Some(ClientTls::Named(tls))),
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

    /// Reaches `peer` at `addr` from now on: a node added to the cluster
    /// since this transport was built, or one that moved. Returns whether
    /// that is news; a peer already dialled elsewhere is dialled again, at
    /// `addr`, on the next message to it.
    pub fn set_peer(&self, peer: NodeId, addr: impl Into<PeerAddr>) -> bool {
        self.links.set_peer(peer, addr.into())
    }

    /// Where `peer` is dialled, if this transport knows it.
    pub fn peer(&self, peer: NodeId) -> Option<PeerAddr> {
        self.links
            .peers
            .read()
            .expect("peers poisoned")
            .get(&peer)
            .cloned()
    }

    /// Whether this transport holds a connection to `peer`: its own frames
    /// get through, as far as it can tell. `false` for a peer it never sent
    /// to, and while a peer is being dialled — which, after a partition, is
    /// a moment longer than it takes to hear from the peer again, as the
    /// peer dials too. A connection whose peer went silent still counts
    /// until a write on it fails or stalls.
    pub fn reaches(&self, peer: NodeId) -> bool {
        self.links.reaches(peer)
    }

    /// Why the link with `peer` is down while the peer answers, over TLS:
    /// a certificate one side does not accept, where
    /// [`reaches`](Self::reaches) alone would read as a peer that is away.
    /// What dialling the peer last met, else what the peer's own connection
    /// was refused for in the last seconds; `None` once a dial connects.
    pub fn refusal(&self, peer: NodeId) -> Option<LinkRefusal> {
        self.links.refusals.of(peer)
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

/// Why a dial gave no connection.
enum DialError {
    /// The network: no route, no listener, a name that does not resolve, a
    /// peer that closed.
    Unreachable(std::io::Error),
    /// The peer answered, and one side refused the other.
    Refused(LinkRefusal),
}

impl From<std::io::Error> for DialError {
    /// A failure met once TLS began says what was refused when it is TLS's.
    fn from(err: std::io::Error) -> Self {
        match LinkRefusal::of(&err) {
            Some(refusal) => Self::Refused(refusal),
            None => Self::Unreachable(err),
        }
    }
}

/// What a node dialled under [`ClientTls::Named`] answers the dialler's
/// first frame (its id) with, as that frame's first byte; a refusal's reason
/// follows.
const WELCOME: u8 = 0;
const NOT_NAMED: u8 = 1;
const NOT_KNOWN: u8 = 2;

/// The frame that answers a dialler with `verdict`.
fn answer(verdict: &Result<(), LinkRefusal>) -> Bytes {
    let why = match verdict {
        Ok(()) => return Bytes::from_static(&[WELCOME]),
        Err(LinkRefusal::NotKnown) => return Bytes::from_static(&[NOT_KNOWN]),
        Err(
            LinkRefusal::PeerCertificate(why)
            | LinkRefusal::OwnCertificate(why)
            | LinkRefusal::Handshake(why),
        ) => why,
    };
    let mut frame = vec![NOT_NAMED];
    frame.extend_from_slice(why.as_bytes());
    Bytes::from(frame)
}

/// What the dialled node's answer says to the dialler.
fn answered(frame: &[u8]) -> Result<(), LinkRefusal> {
    match frame.split_first() {
        Some((&WELCOME, _)) => Ok(()),
        Some((&NOT_KNOWN, _)) => Err(LinkRefusal::NotKnown),
        Some((&NOT_NAMED, why)) => Err(LinkRefusal::OwnCertificate(
            String::from_utf8_lossy(why).into_owned(),
        )),
        _ => Err(LinkRefusal::Handshake(
            "the peer's answer is none this node knows".into(),
        )),
    }
}

/// Establishes one outbound connection to `addr`, wrapping it in client TLS
/// when configured. Each step has its bound, and a step that runs out of it
/// says which it was: the resolution of a name ([`RESOLVE_TIMEOUT`]), the
/// connect ([`CONNECT_TIMEOUT`]), the handshake ([`HANDSHAKE_TIMEOUT`]).
async fn connect(
    from: NodeId,
    to: NodeId,
    addr: &PeerAddr,
    tls: &Option<ClientTls>,
) -> Result<ClientLink, DialError> {
    let outcome = dial(from, to, addr, tls).await;
    match &outcome {
        Ok(_) => {}
        Err(DialError::Unreachable(err)) => {
            tracing::debug!(peer = to.0, %addr, %err, "connect failed")
        }
        Err(DialError::Refused(refusal)) => {
            tracing::debug!(peer = to.0, %addr, %refusal, "connect refused")
        }
    }
    outcome
}

/// `step`, given up after `bound` with an error that names it (`what`).
async fn within<T>(
    bound: Duration,
    what: &str,
    step: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    match tokio::time::timeout(bound, step).await {
        Ok(outcome) => outcome,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{what} took more than {bound:?}"),
        )),
    }
}

/// The dial behind [`connect`].
async fn dial(
    from: NodeId,
    to: NodeId,
    addr: &PeerAddr,
    tls: &Option<ClientTls>,
) -> Result<ClientLink, DialError> {
    let stream = match addr {
        PeerAddr::Socket(addr) => {
            within(CONNECT_TIMEOUT, "the connect", TcpStream::connect(addr)).await
        }
        // Resolved here, at each dial, and every address it resolves to is
        // tried: the peer may have come back at another one.
        PeerAddr::Host { host, port } => {
            let resolved = tokio::net::lookup_host((host.as_str(), *port));
            match within(RESOLVE_TIMEOUT, "resolving the name", resolved).await {
                Ok(addrs) => {
                    let addrs: Vec<SocketAddr> = addrs.collect();
                    within(
                        CONNECT_TIMEOUT,
                        "the connect",
                        TcpStream::connect(&addrs[..]),
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        }
    }
    .map_err(DialError::Unreachable)?;
    let _ = stream.set_nodelay(true);
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, secure(from, to, addr, tls, stream)).await {
        Ok(outcome) => outcome,
        Err(_) => Err(DialError::Unreachable(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("the TLS handshake took more than {HANDSHAKE_TIMEOUT:?}"),
        ))),
    }
}

/// What a dial does once connected: over TLS, the client handshake, and
/// under [`ClientTls::Named`] the peer's answer to this node's id.
async fn secure(
    from: NodeId,
    to: NodeId,
    addr: &PeerAddr,
    tls: &Option<ClientTls>,
    stream: TcpStream,
) -> Result<ClientLink, DialError> {
    match tls {
        None => Ok(Framed::new(Either::Left(stream), codec())),
        Some(ClientTls::Pinned(tls)) => {
            let session = tls
                .connector
                .connect(tls.server_name.clone(), stream)
                .await?;
            // If `to`'s certificate is pinned, the server must present exactly it —
            // otherwise this is a CA-valid impostor at the address; drop the link.
            if let Some(pins) = &tls.peer_certs {
                if let Some(expected) = pins.get(&to) {
                    if !leaf_matches(session.get_ref().1.peer_certificates(), expected) {
                        return Err(DialError::Refused(LinkRefusal::PeerCertificate(
                            "it is not the certificate pinned for the node".into(),
                        )));
                    }
                }
            }
            Ok(Framed::new(Either::Right(session), codec()))
        }
        Some(ClientTls::Named(tls)) => {
            // The handshake accepts the peer's certificate for the name it
            // is dialled at, as any TLS client's does.
            let name = tls::name_of(addr)
                .map_err(|why| DialError::Refused(LinkRefusal::PeerCertificate(why)))?;
            let session = tls.connector().connect(name, stream).await?;
            // Then the peer says whether it accepts this node's: a handshake
            // is done, for the dialler, before the peer read its certificate.
            let mut link = Framed::new(Either::Right(session), codec());
            SinkExt::<Bytes>::send(&mut link, Bytes::copy_from_slice(&from.0.to_le_bytes()))
                .await?;
            match link.next().await {
                Some(Ok(frame)) => answered(&frame).map_err(DialError::Refused)?,
                Some(Err(err)) => return Err(err.into()),
                None => {
                    return Err(DialError::Unreachable(
                        std::io::ErrorKind::UnexpectedEof.into(),
                    ))
                }
            }
            Ok(link)
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
/// carries every group, so one stale connection cost every tenant a frame.
///
/// A frame whose peer cannot be reached **waits for the redial**, up to
/// [`FRAME_HOLD`] from when it was queued, and the redial comes when the
/// backoff elapses, not with the next frame: what was asked a moment before
/// the peer came back is asked once it is. A frame that waited longer is
/// dropped; loss is tolerated by quorums and recovery.
///
/// A peer named by host is resolved at each dial, so the redial after a
/// failure finds a peer that came back at another address; a name that does
/// not resolve is a failed dial like any other.
///
/// Nothing here waits on the network unbounded: each step of a dial has its
/// bound ([`RESOLVE_TIMEOUT`], [`CONNECT_TIMEOUT`], [`HANDSHAKE_TIMEOUT`]), a
/// batch write gives up after [`WRITE_TIMEOUT`] (which counts as a failed
/// write), and after a failed dial the peer is not redialled until its
/// backoff ([`RECONNECT_BACKOFF_MIN`]..[`RECONNECT_BACKOFF_MAX`]) elapses.
/// Otherwise a partitioned peer could pin this writer for minutes after the
/// partition heals, shedding every frame meant for it.
///
/// A peer whose dials keep failing is said in the log when that begins, with
/// what the dial met, and again every [`DIAL_SAID`] for as long as it lasts:
/// which link is down, since when, and why.
async fn peer_writer(
    from: NodeId,
    to: NodeId,
    addr: PeerAddr,
    mut rx: mpsc::Receiver<Queued>,
    tls: Option<ClientTls>,
    up: Arc<AtomicBool>,
    refusals: Arc<Refusals>,
) {
    let mut conn: Option<ClientLink> = None;
    let mut backoff = RECONNECT_BACKOFF_MIN;
    // While set and in the future, the peer is not dialled.
    let mut retry_at: Option<Instant> = None;
    // Logged on the transition only, not once per dropped frame.
    let mut unreachable = false;
    // Whether the last dial was refused by a certificate.
    let mut refused = false;
    // The dials that failed since the last one that connected.
    let mut failing: Option<Failing> = None;
    // Frames taken off the queue and not written yet, oldest first.
    let mut held: VecDeque<Queued> = VecDeque::new();
    // Whether what is held failed a write already.
    let mut resending = false;
    loop {
        if held.is_empty() {
            resending = false;
            match rx.recv().await {
                Some(frame) => held.push_back(frame),
                None => return,
            }
        }
        // Coalesce with everything already queued.
        while let Ok(frame) = rx.try_recv() {
            held.push_back(frame);
        }
        if conn.is_none() {
            if let Some(at) = retry_at.filter(|at| Instant::now() < *at) {
                // Still backing off: keep what is fresh, and take what comes,
                // until the redial.
                tokio::select! {
                    _ = tokio::time::sleep_until(at.into()) => {}
                    frame = rx.recv() => match frame {
                        Some(frame) => held.push_back(frame),
                        None => return,
                    },
                }
                drop_stale(&mut held);
                continue;
            }
            let link = match connect(from, to, &addr, &tls).await {
                Ok(link) => link,
                Err(err) => {
                    // No link right now: redial after the backoff, with what
                    // is still fresh then.
                    let now = Instant::now();
                    retry_at = Some(now + backoff);
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                    let run = failing.get_or_insert_with(|| Failing::begins(now));
                    run.dials += 1;
                    match err {
                        // The peer answers: said apart, when it begins, from
                        // a peer that is away.
                        DialError::Refused(refusal) => {
                            // A node waiting to be added: its host says so.
                            if refused && refusal != LinkRefusal::NotKnown && run.says_again(now) {
                                tracing::warn!(
                                    peer = to.0,
                                    %addr,
                                    %refusal,
                                    for_secs = run.lasted(now).as_secs(),
                                    dials = run.dials,
                                    "the link to a peer is still refused"
                                );
                            }
                            refusals.dialled(to, Some(refusal));
                            refused = true;
                            unreachable = false;
                        }
                        DialError::Unreachable(err) => {
                            refusals.dialled(to, None);
                            refused = false;
                            if !unreachable {
                                unreachable = true;
                                run.said = now;
                                tracing::warn!(
                                    peer = to.0,
                                    %addr,
                                    %err,
                                    hold_ms = FRAME_HOLD.as_millis() as u64,
                                    "peer unreachable; its frames wait for a redial, then are dropped"
                                );
                            } else if run.says_again(now) {
                                tracing::warn!(
                                    peer = to.0,
                                    %addr,
                                    %err,
                                    for_secs = run.lasted(now).as_secs(),
                                    dials = run.dials,
                                    "peer still unreachable: no dial of it connected"
                                );
                            }
                        }
                    }
                    drop_stale(&mut held);
                    continue;
                }
            };
            conn = Some(link);
            up.store(true, Ordering::Relaxed);
            refusals.dialled(to, None);
            backoff = RECONNECT_BACKOFF_MIN;
            retry_at = None;
            failing = None;
            if unreachable || refused {
                unreachable = false;
                refused = false;
                tracing::info!(peer = to.0, %addr, "peer reachable again");
            }
            drop_stale(&mut held);
            if held.is_empty() {
                continue;
            }
        }
        let framed = conn.as_mut().expect("connected above");
        let err = match tokio::time::timeout(WRITE_TIMEOUT, write_batch(framed, &held)).await {
            Ok(Ok(())) => {
                held.clear();
                continue;
            }
            Ok(Err(err)) => err,
            Err(_) => std::io::Error::new(std::io::ErrorKind::TimedOut, "write timed out"),
        };
        // Stale, broken or stalled connection: reconnect and resend once.
        conn = None;
        up.store(false, Ordering::Relaxed);
        if resending {
            tracing::warn!(
                peer = to.0,
                frames = held.len(),
                %err,
                "write failed again after reconnecting; dropping the batch"
            );
            held.clear();
        }
        resending = !resending;
    }
}

/// The dials of one peer that failed one after another, and when that was
/// last said in the log.
struct Failing {
    since: Instant,
    said: Instant,
    dials: u32,
}

impl Failing {
    /// A run that begins at `now`, said as it begins.
    fn begins(now: Instant) -> Self {
        Self {
            since: now,
            said: now,
            dials: 0,
        }
    }

    /// Whether the run is to be said again at `now`: [`DIAL_SAID`] after it
    /// last was, which this counts as.
    fn says_again(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.said) < DIAL_SAID {
            return false;
        }
        self.said = now;
        true
    }

    /// How long the run lasted at `now`.
    fn lasted(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.since)
    }
}

/// Drops the frames that waited for a connection longer than [`FRAME_HOLD`],
/// and the oldest past a queue's worth.
fn drop_stale(held: &mut VecDeque<Queued>) {
    let now = Instant::now();
    while held.front().is_some_and(|frame| {
        held.len() > CHANNEL_CAPACITY || now.saturating_duration_since(frame.at) > FRAME_HOLD
    }) {
        held.pop_front();
    }
}

/// Feeds every frame of `batch`, then flushes once.
async fn write_batch(framed: &mut ClientLink, batch: &VecDeque<Queued>) -> std::io::Result<()> {
    for frame in batch {
        framed.feed(frame.bytes.clone()).await?;
    }
    // `LengthDelimitedCodec` encodes both `Bytes` and `&[u8]` (tokio-util 0.7.20+),
    // so `Framed` is a `Sink` for two item types; name the one we feed.
    SinkExt::<Bytes>::flush(framed).await
}

// ---------------------------------------------------------------------------
// Inbound
// ---------------------------------------------------------------------------

/// Server-side TLS for an accept loop.
#[derive(Clone)]
enum ServerTls {
    /// The acceptor, and the per-node pins when identities are verified
    /// (`None` trusts the wire `from`).
    Pinned(TlsAcceptor, Option<Arc<PeerCerts>>),
    /// Each peer under the name the transport dials it at.
    Named(NodeTls, Arc<TcpTransport>),
}

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
                Ok((stream, remote)) => {
                    let _ = stream.set_nodelay(true);
                    let inbox = inbox.clone();
                    match tls.clone() {
                        None => {
                            tokio::spawn(read_connection(
                                Framed::new(Either::Left(stream), codec()),
                                inbox,
                                None,
                            ));
                        }
                        Some(ServerTls::Named(tls, transport)) => {
                            tokio::spawn(accept_named(stream, remote, inbox, tls, transport));
                        }
                        Some(ServerTls::Pinned(acceptor, pins)) => {
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
                                let framed = Framed::new(Either::Right(session), codec());
                                read_connection(framed, inbox, identity).await;
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
    accept_loop(listener, inbox, Some(ServerTls::Pinned(acceptor, None)))
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
    let pins = Some(Arc::new(peers));
    accept_loop(listener, inbox, Some(ServerTls::Pinned(acceptor, pins)))
}

/// Like [`serve_tls_verified`] with no certificate handed around: the inbound
/// side of a transport built [`with_tls_named`](TcpTransport::with_tls_named),
/// over the same [`NodeTls`]. A peer presents a certificate issued by the
/// cluster's authority and says which node it is; it is accepted when the
/// certificate is valid for the host or the address `transport` dials that
/// node at, and that id is stamped on every [`Envelope`]. A node added to
/// the transport since ([`TcpTransport::set_peer`]) is accepted from then on,
/// and a certificate renewed on a peer the next time the peer dials.
///
/// A connection that is refused is told why before it is closed, so the
/// peer says so itself ([`LinkRefusal::OwnCertificate`]), and is kept for
/// [`TcpTransport::refusal`] here.
pub fn serve_tls_named(
    listener: TcpListener,
    inbox: mpsc::Sender<Envelope>,
    tls: NodeTls,
    transport: Arc<TcpTransport>,
) -> JoinHandle<()> {
    accept_loop(listener, inbox, Some(ServerTls::Named(tls, transport)))
}

/// One inbound connection under [`ServerTls::Named`]: the handshake, which
/// checks the peer's certificate against the authority; the peer's first
/// frame, its id; the check that the certificate names that node, answered to
/// the peer; then its frames.
async fn accept_named(
    stream: TcpStream,
    remote: SocketAddr,
    inbox: mpsc::Sender<Envelope>,
    tls: NodeTls,
    transport: Arc<TcpTransport>,
) {
    let refusals = &transport.links.refusals;
    // Bounded, as the peer's first frame is: a hung dialler must not pin
    // this task forever.
    let session = match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.acceptor().accept(stream)).await
    {
        Ok(Ok(session)) => session,
        Ok(Err(err)) => {
            refusals.unnamed(remote, &err);
            return;
        }
        Err(_) => return,
    };
    let presented = session.get_ref().1.peer_certificates().map(<[_]>::to_vec);
    let mut framed = Framed::new(Either::Right(session), codec());
    let Ok(Some(Ok(hello))) = tokio::time::timeout(HANDSHAKE_TIMEOUT, framed.next()).await else {
        return;
    };
    let Ok(id) = <[u8; 8]>::try_from(&hello[..]) else {
        return;
    };
    let from = NodeId(u64::from_le_bytes(id));
    let verdict = match transport.peer(from) {
        None => Err(LinkRefusal::NotKnown),
        Some(addr) => tls::names(presented.as_deref(), &addr).map_err(LinkRefusal::PeerCertificate),
    };
    let told = SinkExt::<Bytes>::send(&mut framed, answer(&verdict)).await;
    match verdict {
        Ok(()) => {
            refusals.dialling(from, None);
            if told.is_ok() {
                read_connection(framed, inbox, Some(from)).await;
            }
        }
        // No node of this transport's: nothing to say of it in its place.
        Err(LinkRefusal::NotKnown) => {
            tracing::debug!(node = from.0, %remote, "a node not known here was refused");
        }
        Err(refusal) => refusals.dialling(from, Some(refusal)),
    }
}

/// Reads framed messages from one inbound connection until it closes. When
/// `identity` is `Some`, every envelope is stamped with that **authenticated** id
/// (the wire `from` is ignored — impersonation-proof); otherwise the wire `from` is
/// used.
async fn read_connection(
    mut framed: Framed<ServerStream, LengthDelimitedCodec>,
    inbox: mpsc::Sender<Envelope>,
    identity: Option<NodeId>,
) {
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

    /// A peer whose dials keep failing is said again every [`DIAL_SAID`],
    /// not at each dial.
    #[test]
    fn a_failing_dial_is_said_again_from_time_to_time() {
        let began = Instant::now();
        let mut run = Failing::begins(began);
        assert!(!run.says_again(began + Duration::from_secs(1)));
        assert!(!run.says_again(began + DIAL_SAID - Duration::from_millis(1)));
        assert!(run.says_again(began + DIAL_SAID));
        assert!(!run.says_again(began + DIAL_SAID + Duration::from_secs(1)));
        assert!(run.says_again(began + DIAL_SAID * 2));
        assert_eq!(run.lasted(began + DIAL_SAID * 2), DIAL_SAID * 2);
    }

    /// A step of a dial that runs out of its bound says which step it was.
    #[tokio::test(start_paused = true)]
    async fn a_step_out_of_its_bound_says_which_it_was() {
        let never = std::future::pending::<std::io::Result<()>>();
        let err = within(CONNECT_TIMEOUT, "the connect", never)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(err.to_string(), "the connect took more than 2s");
        assert!(within(CONNECT_TIMEOUT, "the connect", async { Ok(7) })
            .await
            .is_ok_and(|n| n == 7));
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
