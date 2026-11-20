//! Client side: an [`Executor`] that forwards every call to a remote server.
//!
//! Each pooled connection ([`ClientBuilder::connections`], default 1) is owned
//! by one actor: it multiplexes requests dispatched to it over its TCP stream,
//! demultiplexes replies by correlation id, and keeps the connection alive
//! with capped exponential backoff (eager reconnect — the push channel must
//! stay alive even while the client is idle-subscribed). The delay before
//! each attempt is drawn at random below the backoff, so the clients a server
//! restart dropped together do not come back together
//! ([`ClientBuilder::reconnect_backoff`]). Requests are
//! round-robined across connections.
//!
//! **Before the server answers.** [`ClientBuilder::connect`] returns once
//! every connection said `Hello`, and fails when the server does not answer.
//! [`ClientBuilder::connect_lazy`] gives the client at once and connects in
//! the background, on the reconnect schedule: for an application that must
//! open while its store is away. Until the first `Hello` is answered every
//! call fails with [`NotConnected`] instead of waiting — a failure a
//! subscription's retries, and a supervised one's restarts, go on from — and
//! the default routing key, which that `Hello` brings, is not known:
//! [`Executor::resolve_default_routing_key`] fails the same way, so nothing
//! is written or named under no key meanwhile. A credential the server
//! refuses before it ever accepted it is final: the client stops dialling
//! and every call fails with [`Rejection::Unauthorized`]. [`Client::state`]
//! says which of these it is.
//!
//! **Write wakes.** A watcher ([`Executor::write_watch_for`], what a
//! subscription waits on) registers its filter with the server, on the first
//! connection, and is woken by the pushed [`ServerFrame::Notify`] frames that
//! name it: the writes that hold an event it would read, and no other. The
//! server also names a watch when it registers it, so a watcher is woken once
//! at the start and once after every reconnect, and reads what it could have
//! missed.
//!
//! **At-most-once semantics.** A request in flight when the connection drops
//! fails at the caller, but may still have executed on the server — a retried
//! `write` can then surface `InvalidOriginalVersion`. This is the same
//! contract as any RPC store. Requests on different pooled connections have no
//! ordering relative to each other; the executor contract is request/response,
//! and callers await a reply before issuing a dependent call.
//!
//! **Addresses.** The server is named by an [`Address`]: an IP and port, or a
//! `host:port` looked up again on every connection attempt, each address it
//! resolves to tried in turn. A reconnect therefore follows the name — a load
//! balancer's current address, a DNS failover, a geo-routed record — rather
//! than redialling the node it first reached.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use bytes::Bytes;
use evento_core::cursor::{Args, ReadResult, Value};
use evento_core::{
    Event, EventFilter, Executor, RoutingKey, TenantFilter, WriteError, WriteInterest,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::either::Either;
use ulid::Ulid;

use crate::wire::{
    decode_tagged, encode_tagged, ClientFrame, HelloOk, RecordKind, Rejection, Request, Response,
    ServerFrame, Token, MAX_FRAME_LENGTH,
};

/// Outbound connection: plain TCP, or a client-side TLS session over it.
type ClientStream = Either<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

/// Client-side TLS settings: the rustls connector and the server name
/// presented for certificate verification.
#[derive(Clone)]
struct Tls {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

/// Where a [`Client`] connects: an IP and port, or a `host:port` looked up
/// again on every connection attempt (the first connect and each reconnect).
///
/// ```
/// # use evento_remote::Address;
/// let ip: Address = "127.0.0.1:4321".into();
/// let name: Address = "store.example.com:4321".into();
/// let socket: Address = std::net::SocketAddr::from(([127, 0, 0, 1], 4321)).into();
/// # let _ = (ip, name, socket);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address(String);

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        Address(addr.to_string())
    }
}

impl From<&str> for Address {
    fn from(addr: &str) -> Self {
        Address(addr.to_owned())
    }
}

impl From<String> for Address {
    fn from(addr: String) -> Self {
        Address(addr)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Everything a connection attempt needs; shared by the first connect and
/// every reconnect.
#[derive(Clone)]
struct Dial {
    addr: Address,
    token: Option<Token>,
    tls: Option<Tls>,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// The reconnect schedule: the bound on the delay starts at `min` and doubles
/// up to `max` with every failed attempt. The delay itself is drawn uniformly
/// in `[0, bound]` (full jitter): a fixed schedule would bring every client
/// that lost its connection at the same instant back at the same instants.
#[derive(Clone, Copy)]
struct Backoff {
    min: Duration,
    max: Duration,
}

impl Backoff {
    /// The delay before the next attempt, drawn below `bound`, which then
    /// doubles. The first attempt is delayed like the others.
    fn next_delay(&self, bound: &mut Duration) -> Duration {
        let delay = bound.mul_f64(rand::random_range(0.0..=1.0));
        *bound = bound.saturating_mul(2).min(self.max);
        delay
    }
}
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

/// How a [`Client`] stands with its server ([`Client::state`]).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ClientState {
    /// A connection of the pool is not up: the client has not reached the
    /// server yet, or lost a connection and dials again.
    Connecting {
        /// Why the last attempt failed; `None` while none has. A refusal of
        /// the server is found in it as a [`Rejection`]
        /// (`error.downcast_ref::<Rejection>()`).
        error: Option<Arc<anyhow::Error>>,
        /// Since when (Unix seconds).
        since: u64,
    },
    /// Every connection of the pool is up.
    Connected,
    /// The server refused the credential before it ever accepted it
    /// ([`Rejection::Unauthorized`]). This is final: the client dials no
    /// more, and every call fails with the rejection.
    Refused,
}

impl ClientState {
    /// Whether every connection of the pool is up.
    pub fn is_connected(&self) -> bool {
        matches!(self, ClientState::Connected)
    }
}

impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientState::Connecting { error: None, .. } => f.write_str("connecting"),
            ClientState::Connecting {
                error: Some(error), ..
            } => write!(f, "connecting: {error:#}"),
            ClientState::Connected => f.write_str("connected"),
            ClientState::Refused => f.write_str("refused"),
        }
    }
}

/// The failure of a call made on a [`Client`] whose server has not answered
/// yet ([`ClientBuilder::connect_lazy`]): nothing was sent. Found in the
/// error of the call (`err.downcast_ref::<NotConnected>()`).
#[derive(Clone, Debug)]
pub struct NotConnected {
    /// Why the last connection attempt failed; `None` while none has.
    pub error: Option<Arc<anyhow::Error>>,
}

impl fmt::Display for NotConnected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the server has not answered yet")?;
        match &self.error {
            Some(error) => write!(f, ": {error:#}"),
            None => Ok(()),
        }
    }
}

impl std::error::Error for NotConnected {}

/// How the connections of the pool stand, as their actors report it.
#[derive(Clone)]
struct Status {
    /// The connections that are up.
    up: usize,
    /// Since when one is not (Unix seconds).
    since: u64,
    /// Why the last attempt failed, until every connection is up again.
    error: Option<Arc<anyhow::Error>>,
    /// The server refused the credential before it ever accepted it.
    refused: bool,
}

/// What the client and the actors of its connections share.
struct Link {
    /// The size of the pool.
    lanes: usize,
    status: watch::Sender<Status>,
    /// Fixed at server executor construction; kept from the first `Hello`
    /// answered and never replaced (it is borrowed as `&str`, so it must
    /// stay fixed). Unset until then.
    default_routing_key: OnceLock<Option<String>>,
}

impl Link {
    fn state(&self) -> ClientState {
        let status = self.status.borrow();
        if status.refused {
            ClientState::Refused
        } else if status.up == self.lanes {
            ClientState::Connected
        } else {
            ClientState::Connecting {
                error: status.error.clone(),
                since: status.since,
            }
        }
    }

    /// Fails while no `Hello` was ever answered: there is no connection to
    /// send on, and no routing key to key by.
    fn check(&self) -> anyhow::Result<()> {
        let status = self.status.borrow();
        if status.refused {
            return Err(refused());
        }
        if self.default_routing_key.get().is_none() {
            return Err(anyhow::Error::new(NotConnected {
                error: status.error.clone(),
            }));
        }
        Ok(())
    }
}

/// The error of a call on a client whose credential was refused, shaped like
/// the one [`ClientBuilder::connect`] returns.
fn refused() -> anyhow::Error {
    anyhow::Error::new(Rejection::Unauthorized).context("the server refused the connection")
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A request dispatched to a connection actor: the reply channel travels with
/// it, and the actor owns its own pending map — no shared lock.
type Dispatch = (u64, Request, oneshot::Sender<Response>);

/// What a watcher asks of the connection that carries the watches.
enum WatchCommand {
    /// Register `interest` under `watch` and wake `tx` when the server names
    /// it.
    Add {
        watch: u64,
        interest: WriteInterest,
        tx: watch::Sender<u64>,
    },
    /// The last receiver of `watch` is gone.
    Remove { watch: u64 },
}

/// The [`ClientFrame::Watch`] that registers `interest` under `watch`.
fn watch_frame(watch: u64, interest: &WriteInterest) -> ClientFrame {
    ClientFrame::Watch {
        watch,
        filters: interest.filters.as_deref().map(<[EventFilter]>::to_vec),
        routing_key: interest.routing_key.clone(),
        tenant: interest.tenant.clone(),
    }
}

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
    /// The watches, handed to the first connection's actor.
    watches: mpsc::UnboundedSender<WatchCommand>,
    next_watch: AtomicU64,
    /// The channel of the unfiltered [`write_watch`](Executor::write_watch),
    /// registered with the server by its first caller and kept from then on.
    watch_all: std::sync::OnceLock<watch::Sender<u64>>,
    /// Where the tasks that wait for a watcher to go away are spawned.
    runtime: tokio::runtime::Handle,
    link: Arc<Link>,
    stable: Stable,
    request_timeout: Duration,
}

/// Builds a [`Client`] with non-default settings.
pub struct ClientBuilder {
    dial: Dial,
    request_timeout: Duration,
    connections: usize,
    backoff: Backoff,
}

impl ClientBuilder {
    /// The credential presented at `Hello`, on every connection of the pool
    /// and again on each reconnect. What it means is the server's resolver's
    /// business; an open server ignores it.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.dial.token = Some(Token(token.into()));
        self
    }

    /// Wraps every connection in TLS: `connector` carries the trust roots
    /// (and the client certificate, for mutual TLS), `server_name` is what the
    /// server's certificate is verified against.
    pub fn tls(mut self, connector: TlsConnector, server_name: ServerName<'static>) -> Self {
        self.dial.tls = Some(Tls {
            connector,
            server_name,
        });
        self
    }

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

    /// Bounds of the delay before a reconnect attempt (default 100ms and 5s;
    /// `max` is raised to `min` when below it). The bound starts at `min`
    /// and doubles up to `max` with every failed attempt; each delay, the one
    /// before the first attempt included, is drawn at random in `[0, bound]`,
    /// so clients dropped together spread out instead of redialling in step. Raise both when many clients share one
    /// server: the wider the bound, the thinner the wave of `Hello`s a
    /// restarted server takes.
    pub fn reconnect_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.backoff = Backoff {
            min,
            max: max.max(min),
        };
        self
    }

    /// Connects and performs the `Hello` exchange on every pooled connection.
    /// Fails fast when the server is unreachable or refuses the credential
    /// (the error then carries the [`Rejection`]); after that each connection
    /// is kept alive with automatic reconnect.
    pub async fn connect(self) -> anyhow::Result<Client> {
        let stable: Stable = Arc::new(AtomicU64::new(0));
        let mut connections = Vec::with_capacity(self.connections);
        let default_routing_key = OnceLock::new();
        for _ in 0..self.connections {
            let (framed, hello) =
                connect_and_hello(&self.dial, &stable, self.request_timeout).await?;
            default_routing_key.get_or_init(|| hello.default_routing_key);
            connections.push(Some(framed));
        }
        Ok(self.spawn(stable, default_routing_key, connections))
    }

    /// Gives the client at once and connects in the background: for an
    /// application that must open while its store is away.
    ///
    /// Each connection of the pool is dialled until the server answers, the
    /// first attempt at once and the following ones on the schedule of a
    /// reconnect ([`reconnect_backoff`](Self::reconnect_backoff)). Until the
    /// first `Hello` is answered:
    ///
    /// - every call fails with [`NotConnected`], at once and with nothing
    ///   sent: a write returns to its caller, a subscription's pass fails
    ///   and its retries go on from there;
    /// - the default routing key is not known, the server's `Hello` being
    ///   what brings it. [`Executor::default_routing_key`] says `None` and
    ///   [`Executor::resolve_default_routing_key`], which a write of a new
    ///   aggregate and a subscription ask first, fails with
    ///   [`NotConnected`]. Once learned the key stays fixed, as on a client
    ///   that connected first;
    /// - a credential the server refuses ([`Rejection::Unauthorized`]) is
    ///   final: the client stops dialling, [`Client::state`] says
    ///   [`ClientState::Refused`] and every call fails with the rejection.
    ///   Any other failure — the server unreachable, or answering
    ///   [`Rejection::Unavailable`] — is tried again for as long as the
    ///   client lives.
    ///
    /// A subscription started on such a client fails its `start` while the
    /// server is away, unless it is `supervised()`: that one starts, and
    /// handles its first event once the server answers.
    ///
    /// [`Client::state`] says how the client stands and
    /// [`Client::connected`] waits for the server.
    ///
    /// ```rust,no_run
    /// # async fn run() -> anyhow::Result<()> {
    /// use evento_remote::{Client, ClientState};
    ///
    /// let client = Client::builder("store.example.com:4321")
    ///     .token("api-key")
    ///     .connect_lazy();
    ///
    /// // On a status page, next to the subscriptions' own states:
    /// match client.state() {
    ///     ClientState::Connected => {}
    ///     ClientState::Connecting { error, since } => {
    ///         tracing::warn!(?error, since, "the store is away");
    ///     }
    ///     ClientState::Refused => anyhow::bail!("the store refuses this key"),
    ///     _ => {}
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime: the connections are tasks of the one it is
    /// called in.
    pub fn connect_lazy(self) -> Client {
        let connections = (0..self.connections).map(|_| None).collect();
        self.spawn(Arc::new(AtomicU64::new(0)), OnceLock::new(), connections)
    }

    /// Spawns the actor of each connection — the ones in `connections` that
    /// are up already, the others to be dialled — and hands out the client.
    fn spawn(
        self,
        stable: Stable,
        default_routing_key: OnceLock<Option<String>>,
        connections: Vec<Option<Framed<ClientStream, LengthDelimitedCodec>>>,
    ) -> Client {
        let (status, _) = watch::channel(Status {
            up: connections.iter().flatten().count(),
            since: unix_seconds(),
            error: None,
            refused: false,
        });
        let link = Arc::new(Link {
            lanes: connections.len(),
            status,
            default_routing_key,
        });
        let (watches, watch_rx) = mpsc::unbounded_channel();
        let mut watch_rx = Some(watch_rx);

        let mut lanes = Vec::with_capacity(connections.len());
        for (lane, connection) in connections.into_iter().enumerate() {
            let (req_tx, req_rx) = mpsc::channel(REQ_CAPACITY);
            tokio::spawn(
                Actor {
                    dial: self.dial.clone(),
                    req_rx,
                    // One connection carries the watches: a watch is a
                    // level-triggered wakeup, registering it on every
                    // connection would add nothing.
                    watch_rx: if lane == 0 { watch_rx.take() } else { None },
                    watches: HashMap::new(),
                    link: Arc::clone(&link),
                    stable: Arc::clone(&stable),
                    request_timeout: self.request_timeout,
                    backoff: self.backoff,
                }
                .run(connection),
            );
            lanes.push(req_tx);
        }

        Client {
            inner: Arc::new(Inner {
                lanes,
                round_robin: AtomicUsize::new(0),
                next_id: AtomicU64::new(HELLO_ID + 1),
                watches,
                next_watch: AtomicU64::new(0),
                watch_all: std::sync::OnceLock::new(),
                runtime: tokio::runtime::Handle::current(),
                link,
                stable,
                request_timeout: self.request_timeout,
            }),
        }
    }
}

impl Client {
    /// Connects with default settings. See [`Client::builder`] for knobs.
    pub async fn connect(addr: impl Into<Address>) -> anyhow::Result<Client> {
        Client::builder(addr).connect().await
    }

    /// Starts building a client for `addr` (token, TLS, request timeout,
    /// connection count, reconnect backoff). A host name in `addr` is looked up on every
    /// connection attempt, not once here.
    pub fn builder(addr: impl Into<Address>) -> ClientBuilder {
        ClientBuilder {
            dial: Dial {
                addr: addr.into(),
                token: None,
                tls: None,
            },
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connections: 1,
            backoff: Backoff {
                min: RECONNECT_MIN,
                max: RECONNECT_MAX,
            },
        }
    }

    /// How the client stands with its server: connecting (the last error,
    /// since when), connected, or refused for good.
    ///
    /// A non-blocking snapshot, for a status page or a health check;
    /// [`connected`](Self::connected) waits.
    pub fn state(&self) -> ClientState {
        self.inner.link.state()
    }

    /// Waits until every connection of the pool is up. Fails once the
    /// server refused the credential for good ([`ClientState::Refused`]),
    /// the error carrying the [`Rejection`]; any other failure is waited
    /// through. Returns at once on a client that is connected.
    pub async fn connected(&self) -> anyhow::Result<()> {
        let link = &self.inner.link;
        let mut status = link.status.subscribe();
        let status = status
            .wait_for(|status| status.refused || status.up == link.lanes)
            .await
            .map_err(|_| anyhow::anyhow!("remote executor connection is closed"))?;
        if status.refused {
            return Err(refused());
        }
        Ok(())
    }

    async fn request(&self, request: Request) -> anyhow::Result<Response> {
        // A client that never reached its server has nothing to wait for on
        // a connection: the call fails now.
        self.inner.link.check()?;
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

impl Inner {
    /// Registers `interest` with the server: the id of the watch and the
    /// sender of the channel its notifies wake.
    fn watch(&self, interest: WriteInterest) -> (u64, watch::Sender<u64>) {
        let (tx, _) = watch::channel(0u64);
        let watch = self.next_watch.fetch_add(1, Ordering::Relaxed);
        // A closed channel means the actor is gone, and with it every
        // connection: nothing would wake the watcher anyway.
        let _ = self.watches.send(WatchCommand::Add {
            watch,
            interest,
            tx: tx.clone(),
        });
        (watch, tx)
    }
}

fn protocol_err(what: &str) -> anyhow::Error {
    anyhow::anyhow!("remote protocol error: unexpected response to {what}")
}

#[async_trait::async_trait]
impl Executor for Client {
    fn default_routing_key(&self) -> Option<&str> {
        self.inner.link.default_routing_key.get()?.as_deref()
    }

    async fn resolve_default_routing_key(&self) -> anyhow::Result<Option<String>> {
        self.inner.link.check()?;
        Ok(self.default_routing_key().map(str::to_owned))
    }

    fn write_watch(&self) -> Option<watch::Receiver<u64>> {
        let all = self.inner.watch_all.get_or_init(|| {
            self.inner
                .watch(WriteInterest {
                    filters: None,
                    routing_key: None,
                    tenant: TenantFilter::All,
                })
                .1
        });
        Some(all.subscribe())
    }

    fn write_watch_for(&self, interest: &WriteInterest) -> Option<watch::Receiver<u64>> {
        let (watch, tx) = self.inner.watch(interest.clone());
        let rx = tx.subscribe();
        // The server keeps a watch until it is told to drop it: say so once
        // the last receiver is gone.
        let watches = self.inner.watches.clone();
        self.inner.runtime.spawn(async move {
            tx.closed().await;
            let _ = watches.send(WatchCommand::Remove { watch });
        });
        Some(rx)
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
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        match self
            .request(Request::Read {
                aggregators: aggregators.map(|a| a.to_vec()),
                routing_key,
                tenant,
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
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        match self
            .request(Request::LatestTimestamp {
                aggregators: aggregators.map(|a| a.to_vec()),
                routing_key,
                tenant,
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
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        match self
            .request(Request::LatestVersion {
                tenant,
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
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        match self
            .request(Request::StreamRoutingKey {
                tenant,
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
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        match self
            .request(Request::GetSnapshot {
                tenant,
                aggregate_type,
                projection,
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
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        match self
            .request(Request::SaveSnapshot {
                tenant,
                aggregate_type,
                projection,
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

    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()> {
        match self
            .request(Request::DeleteSnapshot {
                tenant,
                aggregate_type,
                projection,
                id,
            })
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
/// arrives with each request, so there is no shared lock), on the first
/// connection registers the watches and wakes the ones a pushed notification
/// names, reconnects with capped exponential backoff. Exits when every
/// [`Client`] clone is dropped.
struct Actor {
    dial: Dial,
    req_rx: mpsc::Receiver<Dispatch>,
    /// `Some` on the connection that carries the watches only.
    watch_rx: Option<mpsc::UnboundedReceiver<WatchCommand>>,
    /// The watches registered through this connection, by id. They outlive
    /// a connection: each is registered again on the next one.
    watches: HashMap<u64, (WriteInterest, watch::Sender<u64>)>,
    link: Arc<Link>,
    stable: Stable,
    request_timeout: Duration,
    backoff: Backoff,
}

impl Actor {
    /// `initial` is the connection when it is up already; without one the
    /// actor dials first, at once.
    async fn run(mut self, initial: Option<Framed<ClientStream, LengthDelimitedCodec>>) {
        // The delay before an attempt spreads the clients a server dropped
        // together; a client that never connected was dropped with no one.
        let mut at_once = initial.is_none();
        let mut conn = initial;
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
                None => match self.reconnect(std::mem::take(&mut at_once)).await {
                    Some(framed) => framed,
                    None => return, // all clients dropped, or refused
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
            // The server knows the watches of a connection, not of a client:
            // a new connection is told all of them again. It names each one
            // as it registers it, which wakes the watchers for whatever was
            // written while there was no connection.
            'connected: {
                for (watch, (interest, _)) in &self.watches {
                    let Ok(bytes) =
                        encode_tagged(RecordKind::ClientFrame, &watch_frame(*watch, interest))
                    else {
                        continue;
                    };
                    if out_tx.send(Bytes::from(bytes)).await.is_err() {
                        break 'connected;
                    }
                }
                loop {
                    tokio::select! {
                        command = next_watch_command(&mut self.watch_rx) => {
                            let frame = match command {
                                WatchCommand::Add { watch, interest, tx } => {
                                    let frame = watch_frame(watch, &interest);
                                    self.watches.insert(watch, (interest, tx));
                                    frame
                                }
                                WatchCommand::Remove { watch } => {
                                    self.watches.remove(&watch);
                                    ClientFrame::Unwatch { watch }
                                }
                            };
                            let Ok(bytes) = encode_tagged(RecordKind::ClientFrame, &frame) else {
                                continue;
                            };
                            if out_tx.send(Bytes::from(bytes)).await.is_err() {
                                break;
                            }
                        }
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
                                Ok(ServerFrame::Notify { watches, stable_timestamp }) => {
                                    self.update_stable(stable_timestamp);
                                    for watch in watches {
                                        // An unknown id is a watch dropped while
                                        // its notify was on the way.
                                        if let Some((_, tx)) = self.watches.get(&watch) {
                                            tx.send_modify(|g| *g += 1);
                                        }
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
            }
            // Disconnected (or all clients dropped): stop the writer, fail
            // everything in flight (dropping the reply senders surfaces
            // "connection lost" at each caller), then reconnect.
            drop(out_tx);
            let _ = writer.await;
            pending.clear();
            self.link.status.send_modify(|status| {
                if status.up == self.link.lanes {
                    status.since = unix_seconds();
                }
                status.up -= 1;
            });
            if self.req_rx.is_closed() {
                return;
            }
        }
    }

    /// Connects with capped, jittered exponential backoff, the first attempt
    /// without a delay when `at_once`. `None` when every client clone has
    /// been dropped (nothing left to serve), or when the server refused a
    /// credential it never accepted.
    async fn reconnect(
        &mut self,
        mut at_once: bool,
    ) -> Option<Framed<ClientStream, LengthDelimitedCodec>> {
        let mut bound = self.backoff.min;
        loop {
            // The first attempt of a reconnect waits too: where a load
            // balancer moved the name, it succeeds, and every client dropped
            // with this one would otherwise say `Hello` to the new node at
            // the same instant.
            if !std::mem::take(&mut at_once) {
                tokio::time::sleep(self.backoff.next_delay(&mut bound)).await;
            }
            // Refused on another connection of the pool: the credential is
            // the same on this one.
            if self.req_rx.is_closed() || self.link.status.borrow().refused {
                return None;
            }
            match connect_and_hello(&self.dial, &self.stable, self.request_timeout).await {
                Ok((framed, hello)) => {
                    // The routing key of the first `Hello` answered is kept
                    // and the one of every other ignored: it is fixed at
                    // server construction, and the cached one must stay
                    // stable for the lifetime of this Client (it is borrowed
                    // as &str). Set before the status says so: what waits for
                    // the connection finds the key.
                    self.link
                        .default_routing_key
                        .get_or_init(|| hello.default_routing_key);
                    self.link.status.send_modify(|status| {
                        status.up += 1;
                        if status.up == self.link.lanes {
                            status.error = None;
                        }
                    });
                    return Some(framed);
                }
                Err(err) => {
                    // A refusal is the server's decision (the credential was
                    // revoked, the store is down), worth more than a debug
                    // line.
                    let rejection = err.downcast_ref::<Rejection>();
                    if let Some(rejection) = rejection {
                        tracing::warn!(%rejection, addr = %self.dial.addr, "the server refused the connection");
                    } else {
                        tracing::debug!(?err, addr = %self.dial.addr, "connection failed, backing off");
                    }
                    // A credential the server never accepted will never do:
                    // the client stops here. One it accepted before is tried
                    // again like any failure, the resolver may accept again.
                    let never_accepted = self.link.default_routing_key.get().is_none();
                    if rejection == Some(&Rejection::Unauthorized) && never_accepted {
                        self.link.status.send_modify(|status| status.refused = true);
                        return None;
                    }
                    self.link
                        .status
                        .send_modify(|status| status.error = Some(Arc::new(err)));
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

/// The next watch command; never resolves on a connection that carries no
/// watches, or once every [`Client`] clone is gone (the request queue closing
/// is what ends the actor).
async fn next_watch_command(
    rx: &mut Option<mpsc::UnboundedReceiver<WatchCommand>>,
) -> WatchCommand {
    if let Some(commands) = rx {
        if let Some(command) = commands.recv().await {
            return command;
        }
        *rx = None;
    }
    std::future::pending().await
}

/// Establishes one connection (TLS handshake included) and performs the
/// `Hello` exchange, returning the framed stream and what the server answered
/// (its `default_routing_key`). Seeds the stable watermark from the reply.
/// Frames
/// pushed before the reply (notifies) only update the watermark. A refusal
/// comes back as an error whose source is the [`Rejection`].
async fn connect_and_hello(
    dial: &Dial,
    stable: &AtomicU64,
    timeout: Duration,
) -> anyhow::Result<(Framed<ClientStream, LengthDelimitedCodec>, HelloOk)> {
    // A host name is looked up here, on every attempt, and each address it
    // resolves to is tried in turn; an IP is used as is.
    let stream = TcpStream::connect(dial.addr.0.as_str())
        .await
        .with_context(|| format!("cannot connect to {}", dial.addr))?;
    let _ = stream.set_nodelay(true);
    let stream = match &dial.tls {
        None => Either::Left(stream),
        Some(tls) => Either::Right(
            tokio::time::timeout(
                timeout,
                tls.connector.connect(tls.server_name.clone(), stream),
            )
            .await
            .map_err(|_| anyhow::anyhow!("TLS handshake timed out after {timeout:?}"))??,
        ),
    };
    let mut framed = Framed::new(stream, codec());

    let hello = ClientFrame::Request {
        id: HELLO_ID,
        request: Request::Hello {
            token: dial.token.clone(),
        },
    };
    framed
        .send(Bytes::from(encode_tagged(RecordKind::ClientFrame, &hello)?))
        .await?;

    let hello = tokio::time::timeout(timeout, async {
        loop {
            let Some(item) = framed.next().await else {
                anyhow::bail!("connection closed during hello");
            };
            match decode_tagged::<ServerFrame>(RecordKind::ServerFrame, &item?)? {
                ServerFrame::Response {
                    id: HELLO_ID,
                    response: Response::Hello(reply),
                    stable_timestamp,
                } => {
                    if let Some(v) = stable_timestamp {
                        stable.fetch_max(v, Ordering::AcqRel);
                    }
                    let ok = reply
                        .map_err(anyhow::Error::new)
                        .context("the server refused the connection")?;
                    return Ok(ok);
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

    Ok((framed, hello))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #535: a watcher's filter is sent to the server, and the server
    /// is told once its last receiver is gone. The unfiltered watch is
    /// registered by its first caller only.
    #[tokio::test]
    async fn a_watch_is_registered_and_dropped_with_its_receivers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = Framed::new(stream, codec());
            let mut seen = Vec::new();
            while seen.len() < 4 {
                let bytes = wire.next().await.unwrap().unwrap();
                match decode_tagged::<ClientFrame>(RecordKind::ClientFrame, &bytes).unwrap() {
                    ClientFrame::Request { id, .. } => {
                        let reply = ServerFrame::Response {
                            id,
                            response: Response::Hello(Ok(HelloOk {
                                default_routing_key: None,
                            })),
                            stable_timestamp: None,
                        };
                        let bytes = encode_tagged(RecordKind::ServerFrame, &reply).unwrap();
                        wire.send(Bytes::from(bytes)).await.unwrap();
                    }
                    frame => seen.push(frame),
                }
            }
            seen
        });

        let client = Client::connect(addr).await.unwrap();
        let interest = WriteInterest {
            filters: Some([EventFilter::by_type_raw("test/Account")].into()),
            routing_key: Some(RoutingKey::Value(None)),
            tenant: TenantFilter::All,
        };
        let accounts = client.write_watch_for(&interest).unwrap();
        let clone = accounts.clone();
        let all = client.write_watch().unwrap();
        let all_again = client.write_watch().unwrap();
        drop(accounts);
        let cards = client
            .write_watch_for(&WriteInterest {
                filters: Some([EventFilter::by_type_raw("test/Card")].into()),
                ..interest
            })
            .unwrap();
        drop(clone);

        let seen = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("four watch frames within 5s")
            .unwrap();
        let [ClientFrame::Watch {
            watch: accounts,
            filters: Some(filters),
            routing_key: Some(RoutingKey::Value(None)),
            tenant: TenantFilter::All,
        }, ClientFrame::Watch {
            watch: unfiltered,
            filters: None,
            routing_key: None,
            tenant: TenantFilter::All,
        }, ClientFrame::Watch {
            watch: cards_id, ..
        }, ClientFrame::Unwatch { watch: dropped }] = &seen[..]
        else {
            panic!("unexpected frames: {seen:?}");
        };
        assert_eq!(filters, &[EventFilter::by_type_raw("test/Account")]);
        assert_eq!(dropped, accounts);
        assert!(accounts != unfiltered && unfiltered != cards_id && accounts != cards_id);
        drop((all, all_again, cards));
    }

    #[test]
    fn the_reconnect_delay_stays_below_a_doubling_bound() {
        let backoff = Backoff {
            min: Duration::from_millis(100),
            max: Duration::from_secs(5),
        };
        let mut bound = backoff.min;
        let mut bounds = Vec::new();
        for _ in 0..8 {
            let before = bound;
            assert!(backoff.next_delay(&mut bound) <= before);
            bounds.push(before.as_millis());
        }
        assert_eq!(bounds, [100, 200, 400, 800, 1600, 3200, 5000, 5000]);
    }

    #[test]
    fn two_clients_do_not_share_a_reconnect_schedule() {
        let backoff = Backoff {
            min: Duration::from_secs(5),
            max: Duration::from_secs(5),
        };
        let schedule = || {
            let mut bound = backoff.min;
            (0..16)
                .map(|_| backoff.next_delay(&mut bound))
                .collect::<Vec<_>>()
        };
        assert_ne!(schedule(), schedule());
    }

    #[test]
    fn a_reconnect_maximum_below_the_minimum_is_raised_to_it() {
        let builder = Client::builder("127.0.0.1:1")
            .reconnect_backoff(Duration::from_secs(2), Duration::from_secs(1));
        assert_eq!(builder.backoff.min, Duration::from_secs(2));
        assert_eq!(builder.backoff.max, Duration::from_secs(2));
    }
}
