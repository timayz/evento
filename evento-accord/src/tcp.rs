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
//! `NodeId → SocketAddr` map. Inbound frames are decoded and forwarded to a
//! node's inbox by [`serve`].
//!
//! **Many groups per process.** [`MuxTransport`] carries any number of independent
//! consensus groups (one `Node` each — e.g. one per tenant, each with its own
//! database) over a *single* connection set per process, stamping a [`GroupId`] on
//! each frame and demuxing inbound frames to per-group inboxes. The `Node` and the
//! protocol are unchanged; see the type docs.
//!
//! **Mutual TLS (optional).** Build with [`TcpTransport::with_tls`] and serve with
//! [`serve_tls`] to encrypt and authenticate inter-node traffic: every connection
//! is wrapped in rustls, with each side presenting a certificate the other
//! verifies against a shared root (the operator supplies the rustls
//! [`TlsConnector`]/[`TlsAcceptor`]). Plain [`new`](TcpTransport::new)/[`serve`]
//! stay available for trusted networks; the framing and consensus layers are
//! identical either way.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::ops::ControlFlow;
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
use crate::format::{decode_tagged, encode_tagged, peek_kind, RecordKind};
use crate::message::Message;
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::transport::{Envelope, GroupId, DEFAULT_INBOX_CAPACITY};

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

/// A multiplexed wire frame: like [`Frame`] but naming the consensus group the
/// message belongs to, so one connection set can carry many independent groups
/// (see [`MuxTransport`]). Encoded under its own [`RecordKind::GroupFrame`] tag —
/// additive, so a legacy [`Frame`] still decodes and a legacy reader sheds this.
#[derive(Serialize, Deserialize)]
struct GroupFrame {
    from: NodeId,
    group: GroupId,
    message: Message,
}

fn encode(frame: &Frame) -> anyhow::Result<Vec<u8>> {
    encode_tagged(RecordKind::WireFrame, frame)
}

fn encode_group(frame: &GroupFrame) -> anyhow::Result<Vec<u8>> {
    encode_tagged(RecordKind::GroupFrame, frame)
}

/// Decodes an inbound frame of either kind into `(from, group, message)`; an
/// un-grouped legacy frame lands in [`GroupId::DEFAULT`].
fn decode_wire(bytes: &[u8]) -> anyhow::Result<(NodeId, GroupId, Message)> {
    match peek_kind(bytes) {
        Some(RecordKind::WireFrame) => {
            let frame: Frame = decode_tagged(RecordKind::WireFrame, bytes)?;
            Ok((frame.from, GroupId::DEFAULT, frame.message))
        }
        Some(RecordKind::GroupFrame) => {
            let frame: GroupFrame = decode_tagged(RecordKind::GroupFrame, bytes)?;
            Ok((frame.from, frame.group, frame.message))
        }
        other => anyhow::bail!("not a wire frame: {other:?}"),
    }
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

/// The outbound side shared by every transport flavour: one lazily-connected,
/// bounded writer queue per peer. [`TcpTransport`] owns one for a single node;
/// [`MuxTransport`] shares one across every group a process hosts, so a thousand
/// groups cost one connection set, not a thousand.
struct PeerLinks {
    from: NodeId,
    peers: Arc<HashMap<NodeId, SocketAddr>>,
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
    fn new(from: NodeId, peers: HashMap<NodeId, SocketAddr>, tls: Option<TlsClient>) -> Self {
        Self {
            from,
            peers: Arc::new(peers),
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
        let addr = *self.peers.get(&to)?;
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
    /// Builds a plaintext transport for `from` that can reach the nodes in `peers`.
    pub fn new(from: NodeId, peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            links: PeerLinks::new(from, peers, None),
        }
    }

    /// Builds a transport whose outbound connections use mutual TLS (`tls`), for
    /// an untrusted network. Pair with [`serve_tls`] on the inbound side.
    pub fn with_tls(from: NodeId, peers: HashMap<NodeId, SocketAddr>, tls: TlsClient) -> Self {
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
    /// # use evento_accord::{
    /// #     HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Node,
    /// #     NodeId, StaticTopology, TcpTransport,
    /// # };
    /// # let id = NodeId(0);
    /// # let peers = HashMap::new();
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
async fn peer_writer(
    to: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Bytes>,
    tls: Option<TlsClient>,
) {
    let mut conn: Option<Framed<ClientStream, LengthDelimitedCodec>> = None;
    while let Some(first) = rx.recv().await {
        // Coalesce this frame with everything already queued.
        let mut batch = vec![first];
        while let Ok(bytes) = rx.try_recv() {
            batch.push(bytes);
        }
        for _attempt in 0..2 {
            if conn.is_none() {
                conn = connect(to, addr, &tls)
                    .await
                    .map(|stream| Framed::new(stream, codec()));
            }
            let Some(framed) = conn.as_mut() else {
                // Unreachable right now: drop the batch, reconnect on the next.
                break;
            };
            if write_batch(framed, &batch).await.is_ok() {
                break;
            }
            // Stale or broken connection: reconnect and resend once.
            conn = None;
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
    framed.flush().await
}

// ---------------------------------------------------------------------------
// Inbound
// ---------------------------------------------------------------------------

/// Where an inbound frame goes once decoded.
#[derive(Clone)]
enum Inbox {
    /// A legacy single-group listener ([`serve`] and friends): one node's inbox.
    /// Only [`GroupId::DEFAULT`] frames are accepted; grouped frames for any other
    /// group are dropped (this host does not multiplex).
    Single(mpsc::Sender<Envelope>),
    /// A multiplexed listener ([`MuxTransport::serve`]): route by group.
    Mux(Arc<GroupTable>),
}

impl Inbox {
    /// Delivers one decoded frame. `Break` only when the connection should end —
    /// the legacy inbox is closed (its node is gone), so there is nobody to read for.
    fn deliver(&self, group: GroupId, envelope: Envelope) -> ControlFlow<()> {
        match self {
            Inbox::Single(inbox) => {
                if group != GroupId::DEFAULT {
                    tracing::debug!(
                        group = group.0,
                        "dropping grouped frame on a single-group listener"
                    );
                    return ControlFlow::Continue(());
                }
                match inbox.try_send(envelope) {
                    Ok(()) => ControlFlow::Continue(()),
                    // A full inbox is backpressure — shed this frame but keep the
                    // connection; only a closed inbox (node gone) ends the loop.
                    Err(mpsc::error::TrySendError::Full(_)) => ControlFlow::Continue(()),
                    Err(mpsc::error::TrySendError::Closed(_)) => ControlFlow::Break(()),
                }
            }
            Inbox::Mux(table) => {
                table.deliver(group, envelope);
                ControlFlow::Continue(())
            }
        }
    }
}

/// Default bounds of the pending-register buffer (see
/// [`MuxTransport::with_pending_buffer`]): frames parked per group, groups parked
/// at once, and how long a parked frame stays deliverable.
pub const PENDING_FRAMES_PER_GROUP: usize = 64;
/// See [`PENDING_FRAMES_PER_GROUP`].
pub const PENDING_GROUPS: usize = 1024;
/// See [`PENDING_FRAMES_PER_GROUP`].
pub const PENDING_TTL: Duration = Duration::from_secs(5);

/// How often, per group, the unrouted-group handler is invoked at most.
const UNROUTED_NOTICE_INTERVAL: Duration = Duration::from_secs(1);
/// Bound on the per-group notice map, so a flood of garbage group ids cannot grow it
/// without limit (it is cleared, not evicted — the only cost is an extra notice).
const UNROUTED_NOTICE_CAP: usize = 4096;

/// Called (rate-limited, once per group per second) when a frame arrives for a group
/// this host has not registered. The hook lets an application open a group lazily —
/// e.g. a tenant that was evicted — but should decide from its *own* catalog whether
/// the group exists: the wire must never be able to create one. It runs on the
/// connection's read task, so it must return immediately (spawn the actual open).
pub type UnroutedHandler = Arc<dyn Fn(GroupId) + Send + Sync>;

/// Frames parked for groups that are not (yet) registered, replayed on
/// [`MuxTransport::register`]. Bounded in three dimensions (frames per group,
/// groups, age) so an unregistered flood cannot hold memory.
struct PendingBuffer {
    per_group: usize,
    groups: usize,
    ttl: Duration,
    parked: HashMap<GroupId, VecDeque<(Instant, Envelope)>>,
}

impl PendingBuffer {
    /// Parks `envelope` for `group`. `false` if it had to be shed (bounds reached).
    fn park(&mut self, group: GroupId, envelope: Envelope, now: Instant) -> bool {
        if !self.parked.contains_key(&group) && self.parked.len() >= self.groups {
            // At the group cap: make room by dropping groups whose frames have all
            // expired; if none have, shed.
            let ttl = self.ttl;
            self.parked.retain(|_, q| {
                while q
                    .front()
                    .is_some_and(|(at, _)| now.duration_since(*at) > ttl)
                {
                    q.pop_front();
                }
                !q.is_empty()
            });
            if self.parked.len() >= self.groups {
                return false;
            }
        }
        let queue = self.parked.entry(group).or_default();
        while queue
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > self.ttl)
        {
            queue.pop_front();
        }
        if queue.len() >= self.per_group {
            return false;
        }
        queue.push_back((now, envelope));
        true
    }

    /// Takes every still-live parked frame for `group`, returning how many had
    /// expired meanwhile (counted as unrouted by the caller).
    fn take(&mut self, group: GroupId, now: Instant) -> (Vec<Envelope>, u64) {
        let Some(queue) = self.parked.remove(&group) else {
            return (Vec::new(), 0);
        };
        let mut expired = 0;
        let live = queue
            .into_iter()
            .filter_map(|(at, env)| {
                if now.duration_since(at) > self.ttl {
                    expired += 1;
                    None
                } else {
                    Some(env)
                }
            })
            .collect();
        (live, expired)
    }
}

/// The per-host routing table of a [`MuxTransport`]: which groups are registered
/// (and their inboxes), plus the pending buffer for the ones that are not yet.
struct GroupTable {
    groups: Mutex<HashMap<GroupId, mpsc::Sender<Envelope>>>,
    pending: Mutex<PendingBuffer>,
    /// Last time the unrouted handler was invoked for a group (rate limiting).
    notices: Mutex<HashMap<GroupId, Instant>>,
    unrouted: Option<UnroutedHandler>,
    metrics: Arc<Metrics>,
    inbox_capacity: usize,
}

impl GroupTable {
    /// Routes `envelope` to `group`'s inbox, or parks it if the group is not
    /// registered (and notifies the unrouted handler, rate-limited).
    fn deliver(&self, group: GroupId, envelope: Envelope) {
        let inbox = {
            let groups = self.groups.lock().expect("groups poisoned");
            groups.get(&group).cloned()
        };
        let Some(tx) = inbox else {
            self.unrouted(group, envelope);
            return;
        };
        match tx.try_send(envelope) {
            // Delivered, or shed for a full inbox (backpressure, tolerated).
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            // The group's node is gone; forget it and treat the frame as
            // unrouted. Other groups keep using this connection.
            Err(mpsc::error::TrySendError::Closed(envelope)) => {
                self.groups.lock().expect("groups poisoned").remove(&group);
                self.unrouted(group, envelope);
            }
        }
    }

    fn unrouted(&self, group: GroupId, envelope: Envelope) {
        let now = Instant::now();
        let parked = self
            .pending
            .lock()
            .expect("pending poisoned")
            .park(group, envelope, now);
        if !parked {
            self.metrics.record_unrouted();
        }
        if let Some(handler) = &self.unrouted {
            let due = {
                let mut notices = self.notices.lock().expect("notices poisoned");
                if notices.len() >= UNROUTED_NOTICE_CAP {
                    notices.clear();
                }
                match notices.get(&group) {
                    Some(at) if now.duration_since(*at) < UNROUTED_NOTICE_INTERVAL => false,
                    _ => {
                        notices.insert(group, now);
                        true
                    }
                }
            };
            if due {
                handler(group);
            }
        }
    }

    fn register(&self, group: GroupId) -> mpsc::Receiver<Envelope> {
        let (tx, rx) = mpsc::channel(self.inbox_capacity);
        self.groups
            .lock()
            .expect("groups poisoned")
            .insert(group, tx.clone());
        // Replay what arrived before the group was registered, in arrival order.
        let (live, expired) = self
            .pending
            .lock()
            .expect("pending poisoned")
            .take(group, Instant::now());
        for _ in 0..expired {
            self.metrics.record_unrouted();
        }
        for envelope in live {
            let _ = tx.try_send(envelope);
        }
        rx
    }

    fn unregister(&self, group: GroupId) {
        self.groups.lock().expect("groups poisoned").remove(&group);
    }

    fn is_registered(&self, group: GroupId) -> bool {
        self.groups
            .lock()
            .expect("groups poisoned")
            .contains_key(&group)
    }
}

/// Server-side TLS for an accept loop: the acceptor, and the per-node pins when
/// identities are verified (`None` trusts the wire `from`).
type ServerTls = (TlsAcceptor, Option<Arc<PeerCerts>>);

/// The one accept loop behind every `serve*` flavour: accepts connections on
/// `listener`, optionally completes a (verified) TLS handshake, then reads frames
/// into `inbox`. Returns the accept-loop task handle.
fn accept_loop(listener: TcpListener, inbox: Inbox, tls: Option<ServerTls>) -> JoinHandle<()> {
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
    accept_loop(listener, Inbox::Single(inbox), None)
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
    accept_loop(listener, Inbox::Single(inbox), Some((acceptor, None)))
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
    accept_loop(
        listener,
        Inbox::Single(inbox),
        Some((acceptor, Some(Arc::new(peers)))),
    )
}

/// Reads framed messages from one inbound connection until it closes. When
/// `identity` is `Some`, every envelope is stamped with that **authenticated** id
/// (the wire `from` is ignored — impersonation-proof); otherwise the wire `from` is
/// used. Either way the identity applies to every group carried on the connection.
async fn read_connection(stream: ServerStream, inbox: Inbox, identity: Option<NodeId>) {
    let mut framed = Framed::new(stream, codec());
    while let Some(item) = framed.next().await {
        let bytes = match item {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        let Ok((from, group, message)) = decode_wire(&bytes) else {
            continue;
        };
        let envelope = Envelope {
            from: identity.unwrap_or(from),
            message,
        };
        if inbox.deliver(group, envelope).is_break() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Multiplexed transport: many consensus groups over one connection set
// ---------------------------------------------------------------------------

/// A framed-TCP transport that carries **many independent consensus groups** over
/// one connection set — the production shape for hosting one Accord group per
/// tenant (each with its own database) on a shared set of hosts.
///
/// One `MuxTransport` per process. Every group's `Node` gets its own
/// [`GroupSink`] (via [`sink`](Self::sink), which stamps the [`GroupId`] on each
/// frame) and its own inbox (via [`register`](Self::register)); the single accept
/// loop started by [`serve`](Self::serve) demuxes inbound frames by group. The
/// `Node`, the protocol, and the [`Envelope`] type are unchanged — only the wire
/// frame gains a group id ([`RecordKind::GroupFrame`], additive).
///
/// Frames for a group that is **not registered** are parked briefly (see
/// [`with_pending_buffer`](Self::with_pending_buffer)) and replayed when it
/// registers — so a group that is being opened on this host (a tenant just created
/// elsewhere, or one evicted and re-opened on demand) does not lose the first
/// write's fast path — and the optional
/// [`with_unrouted_handler`](Self::with_unrouted_handler) hook is told, so the
/// application can open it. Parked frames that expire or overflow are counted in
/// `messages_unrouted`.
///
/// Legacy interop: an un-grouped frame from a classic [`TcpTransport`] is delivered
/// to [`GroupId::DEFAULT`].
pub struct MuxTransport {
    links: Arc<PeerLinks>,
    table: Arc<GroupTable>,
}

impl MuxTransport {
    /// Builds a plaintext multiplexed transport for `from` reaching the hosts in
    /// `peers`. Every group hosted here shares this membership — the same `NodeId`s
    /// on the same hosts for every group.
    pub fn new(from: NodeId, peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self::build(PeerLinks::new(from, peers, None))
    }

    /// Like [`new`](Self::new) but with mutual TLS on every outbound connection.
    /// Pair with [`serve_tls`](Self::serve_tls) / [`serve_tls_verified`](Self::serve_tls_verified).
    pub fn with_tls(from: NodeId, peers: HashMap<NodeId, SocketAddr>, tls: TlsClient) -> Self {
        Self::build(PeerLinks::new(from, peers, Some(tls)))
    }

    fn build(links: PeerLinks) -> Self {
        let metrics = Arc::clone(&links.metrics);
        Self {
            links: Arc::new(links),
            table: Arc::new(GroupTable {
                groups: Mutex::new(HashMap::new()),
                pending: Mutex::new(PendingBuffer {
                    per_group: PENDING_FRAMES_PER_GROUP,
                    groups: PENDING_GROUPS,
                    ttl: PENDING_TTL,
                    parked: HashMap::new(),
                }),
                notices: Mutex::new(HashMap::new()),
                unrouted: None,
                metrics,
                inbox_capacity: DEFAULT_INBOX_CAPACITY,
            }),
        }
    }

    /// Mutable access for the builder methods, which must run before any
    /// [`sink`](Self::sink) is handed out or [`serve`](Self::serve) is started.
    fn parts(&mut self) -> (&mut PeerLinks, &mut GroupTable) {
        let links = Arc::get_mut(&mut self.links)
            .expect("configure MuxTransport before handing out sinks or serving");
        let table = Arc::get_mut(&mut self.table)
            .expect("configure MuxTransport before handing out sinks or serving");
        (links, table)
    }

    /// Shares an external [`Metrics`] with this transport, so its shed/unrouted
    /// counts land in a snapshot of your choosing. Call before [`sink`](Self::sink).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        let (links, table) = self.parts();
        links.metrics = Arc::clone(&metrics);
        table.metrics = metrics;
        self
    }

    /// Bound of each per-peer **outbound** queue, shared by every group (default
    /// [`CHANNEL_CAPACITY`]). Raise it when many groups burst at once, since a full
    /// queue sheds frames for all of them.
    pub fn with_queue_capacity(mut self, per_peer: usize) -> Self {
        self.parts().0.queue_capacity = per_peer.max(1);
        self
    }

    /// Bound of each group's **inbound** inbox (default [`DEFAULT_INBOX_CAPACITY`]).
    /// Applies to groups registered after the call.
    pub fn with_inbox_capacity(mut self, per_group: usize) -> Self {
        self.parts().1.inbox_capacity = per_group.max(1);
        self
    }

    /// Bounds of the pending-register buffer: frames parked per unregistered group,
    /// groups parked at once, and how long a parked frame stays deliverable
    /// (defaults [`PENDING_FRAMES_PER_GROUP`], [`PENDING_GROUPS`], [`PENDING_TTL`]).
    /// Size the TTL above how long opening a group takes on this host (migrating and
    /// opening a tenant database, say) so its first frames survive until
    /// [`register`](Self::register). Zero frames or groups disables parking.
    pub fn with_pending_buffer(
        mut self,
        frames_per_group: usize,
        groups: usize,
        ttl: Duration,
    ) -> Self {
        let pending = self.parts().1.pending.get_mut().expect("pending poisoned");
        pending.per_group = frames_per_group;
        pending.groups = groups;
        pending.ttl = ttl;
        self
    }

    /// Installs the [`UnroutedHandler`] invoked when frames arrive for a group this
    /// host has not registered.
    pub fn with_unrouted_handler(
        mut self,
        handler: impl Fn(GroupId) + Send + Sync + 'static,
    ) -> Self {
        self.parts().1.unrouted = Some(Arc::new(handler));
        self
    }

    /// This host's node id — the `from` stamped on every frame it sends.
    pub fn node_id(&self) -> NodeId {
        self.links.from
    }

    /// A point-in-time snapshot of this transport's counters (`messages_shed`,
    /// `messages_unrouted`; the rest belong to the nodes).
    pub fn metrics(&self) -> MetricsSnapshot {
        self.links.metrics.snapshot()
    }

    /// Registers `group` on this host and returns its inbox receiver (hand it to
    /// `Node::start`). Frames parked for the group while it was unregistered are
    /// replayed into the inbox first, in arrival order. Re-registering replaces the
    /// previous inbox.
    pub fn register(&self, group: GroupId) -> mpsc::Receiver<Envelope> {
        self.table.register(group)
    }

    /// Forgets `group`: subsequent frames for it are treated as unrouted (parked /
    /// counted / reported to the handler). The group's `Node` should be stopped too.
    pub fn unregister(&self, group: GroupId) {
        self.table.unregister(group)
    }

    /// Whether `group` currently has a registered inbox on this host.
    pub fn is_registered(&self, group: GroupId) -> bool {
        self.table.is_registered(group)
    }

    /// An outbound [`MessageSink`] for `group`: every frame it sends is stamped with
    /// the group id and travels over the shared connection set. Cheap — a handle.
    pub fn sink(&self, group: GroupId) -> GroupSink {
        GroupSink {
            group,
            links: Arc::clone(&self.links),
        }
    }

    /// Accepts inbound connections on `listener` and demuxes their frames to the
    /// registered groups. One per process, not per group.
    pub fn serve(&self, listener: TcpListener) -> JoinHandle<()> {
        accept_loop(listener, Inbox::Mux(Arc::clone(&self.table)), None)
    }

    /// Like [`serve`](Self::serve) with a TLS handshake per connection; the wire
    /// `from` is trusted (see [`serve_tls`]).
    pub fn serve_tls(&self, listener: TcpListener, acceptor: TlsAcceptor) -> JoinHandle<()> {
        accept_loop(
            listener,
            Inbox::Mux(Arc::clone(&self.table)),
            Some((acceptor, None)),
        )
    }

    /// Like [`serve_tls`](Self::serve_tls) but authenticates each connection's node
    /// by certificate pin (see [`serve_tls_verified`]). The pinned identity is stamped
    /// on every group's envelopes from that connection.
    pub fn serve_tls_verified(
        &self,
        listener: TcpListener,
        acceptor: TlsAcceptor,
        peers: PeerCerts,
    ) -> JoinHandle<()> {
        accept_loop(
            listener,
            Inbox::Mux(Arc::clone(&self.table)),
            Some((acceptor, Some(Arc::new(peers)))),
        )
    }
}

/// One group's outbound handle into a [`MuxTransport`]; see
/// [`MuxTransport::sink`].
pub struct GroupSink {
    group: GroupId,
    links: Arc<PeerLinks>,
}

impl GroupSink {
    /// The group this sink speaks for.
    pub fn group(&self) -> GroupId {
        self.group
    }

    fn encode(&self, message: Message) -> anyhow::Result<Bytes> {
        Ok(Bytes::from(encode_group(&GroupFrame {
            from: self.links.from,
            group: self.group,
            message,
        })?))
    }
}

#[async_trait]
impl MessageSink for GroupSink {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        let bytes = self.encode(message)?;
        self.links.enqueue(to, bytes);
        Ok(())
    }

    async fn broadcast(&self, nodes: &[NodeId], message: Message) -> anyhow::Result<()> {
        // One encode for the whole fan-out (`from` and `group` are constant per
        // sink, so every peer receives the identical frame).
        let bytes = self.encode(message)?;
        self.links.broadcast(nodes, bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Timestamp, TxnId};

    fn applied(n: u64) -> Envelope {
        Envelope {
            from: NodeId(1),
            message: Message::Applied {
                txn: TxnId(Timestamp {
                    micros: n,
                    logical: 0,
                    node: NodeId(1),
                }),
            },
        }
    }

    fn mux() -> MuxTransport {
        MuxTransport::new(NodeId(0), HashMap::new())
    }

    #[tokio::test]
    async fn frames_for_an_unregistered_group_replay_on_register() {
        let mux = mux();
        let g = GroupId(7);
        mux.table.deliver(g, applied(1));
        mux.table.deliver(g, applied(2));
        assert_eq!(mux.metrics().messages_unrouted, 0, "parked, not shed");

        let mut inbox = mux.register(g);
        let first = inbox.try_recv().expect("replayed");
        let second = inbox.try_recv().expect("replayed");
        assert!(matches!(first.message, Message::Applied { txn } if txn.0.micros == 1));
        assert!(matches!(second.message, Message::Applied { txn } if txn.0.micros == 2));
        assert!(inbox.try_recv().is_err());

        // Once registered, frames route straight through.
        mux.table.deliver(g, applied(3));
        assert!(inbox.try_recv().is_ok());
    }

    #[tokio::test]
    async fn pending_buffer_bounds_are_enforced_and_counted() {
        let mux = mux().with_pending_buffer(2, 1, Duration::from_secs(60));
        mux.table.deliver(GroupId(1), applied(1));
        mux.table.deliver(GroupId(1), applied(2));
        mux.table.deliver(GroupId(1), applied(3)); // over per-group cap
        mux.table.deliver(GroupId(2), applied(4)); // over group cap
        assert_eq!(mux.metrics().messages_unrouted, 2);

        let mut inbox = mux.register(GroupId(1));
        assert!(inbox.try_recv().is_ok());
        assert!(inbox.try_recv().is_ok());
        assert!(inbox.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_parked_frames_are_dropped_and_counted() {
        let mux = mux().with_pending_buffer(8, 8, Duration::ZERO);
        let now = Instant::now();
        // Park directly with an old timestamp so the TTL (zero) is exceeded.
        mux.table.pending.lock().unwrap().park(
            GroupId(3),
            applied(1),
            now - Duration::from_millis(5),
        );
        let mut inbox = mux.register(GroupId(3));
        assert!(inbox.try_recv().is_err(), "expired frame is not replayed");
        assert_eq!(mux.metrics().messages_unrouted, 1);
    }

    #[tokio::test]
    async fn a_closed_group_inbox_is_forgotten_and_the_handler_is_told() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let hook = Arc::clone(&seen);
        let mux = mux().with_unrouted_handler(move |g| hook.lock().unwrap().push(g));
        let g = GroupId(9);
        let inbox = mux.register(g);
        assert!(mux.is_registered(g));
        drop(inbox); // the node went away
        mux.table.deliver(g, applied(1));
        assert!(!mux.is_registered(g));
        assert_eq!(seen.lock().unwrap().as_slice(), &[g]);
        // Rate-limited: a second frame within the interval does not re-notify.
        mux.table.deliver(g, applied(2));
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_single_group_listener_drops_grouped_frames() {
        let (tx, mut rx) = mpsc::channel(4);
        let inbox = Inbox::Single(tx);
        assert!(inbox.deliver(GroupId(5), applied(1)).is_continue());
        assert!(rx.try_recv().is_err());
        assert!(inbox.deliver(GroupId::DEFAULT, applied(2)).is_continue());
        assert!(rx.try_recv().is_ok());
        drop(rx);
        assert!(inbox.deliver(GroupId::DEFAULT, applied(3)).is_break());
    }

    #[test]
    fn legacy_and_grouped_frames_both_decode() {
        let legacy = encode(&Frame {
            from: NodeId(4),
            message: applied(1).message,
        })
        .unwrap();
        let (from, group, _) = decode_wire(&legacy).unwrap();
        assert_eq!((from, group), (NodeId(4), GroupId::DEFAULT));

        let grouped = encode_group(&GroupFrame {
            from: NodeId(4),
            group: GroupId(42),
            message: applied(1).message,
        })
        .unwrap();
        let (from, group, _) = decode_wire(&grouped).unwrap();
        assert_eq!((from, group), (NodeId(4), GroupId(42)));

        let journal_record = encode_tagged(RecordKind::Command, &1u64).unwrap();
        assert!(decode_wire(&journal_record).is_err());
    }
}
