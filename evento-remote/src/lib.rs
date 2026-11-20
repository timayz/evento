//! Client/server remote executor for evento.
//!
//! Splits an evento application from its event store across a network boundary:
//!
//! - [`serve`] exposes **any** [`Executor`](evento_core::Executor) (Sql, Fjall,
//!   Accord, …) over length-delimited framed TCP.
//! - [`Client`] implements [`Executor`](evento_core::Executor) by forwarding
//!   every call to a server, so `evento::create`, projections, and
//!   subscriptions work unchanged against a remote store.
//!
//! ```rust,no_run
//! # async fn run() -> anyhow::Result<()> {
//! // Server process: serve an existing executor.
//! let executor = evento_fjall::Fjall::open("./data")?;
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
//! let handle = evento_remote::serve(listener, executor);
//!
//! // Client process: connect and use like any executor.
//! let client = evento_remote::Client::connect("127.0.0.1:4321").await?;
//! # let _ = (handle, client);
//! # Ok(())
//! # }
//! ```
//!
//! [`Client::connect`] fails when the server does not answer. An application
//! that must open while its store is away builds its client with
//! [`ClientBuilder::connect_lazy`] instead: the client exists at once and
//! connects in the background, its calls fail with [`NotConnected`] until the
//! server answers, a `supervised()` subscription started on it handles its
//! first event when it does, and [`Client::state`] says how it stands —
//! connecting, connected, or refused for good.
//!
//! A client-side subscription registers what it follows with the server and
//! is pushed a notification for the writes that hold an event it would read,
//! so it wakes with the same low latency as over a local `write_watch_for`,
//! and a write wakes no subscription that does not follow it. The server's `stable_timestamp` watermark
//! piggybacks on every frame, so fronting a replicated backend (Accord) keeps
//! subscription safety intact.
//!
//! **Credentials, tenants.** A host authenticating its clients implements
//! [`Resolver`]: each connection opens with a `Hello` carrying the token the
//! client was built with ([`ClientBuilder::token`]), and the resolver answers
//! with the [`Session`] to serve it — the executor, something to keep alive
//! for the connection's lifetime, a signal to end it (a revoked credential) —
//! or a [`Rejection`]. [`serve_with`] takes the resolver; [`serve`] is the open
//! single-store case.
//!
//! Which tenant a client belongs to is the host application's decision — its
//! resolver maps the token (an API key, say) to one. To confine the client to
//! that tenant over a store shared by every tenant, serve the connection in it,
//! `Session::new(store.clone()).tenant(tenant)?` ([`Session::tenant`]): every
//! request then acts in that tenant whatever it names — what it reads and
//! loads, the tenant its writes are stamped with — and `replicate` and the
//! subscriber calls are refused, so the client never reaches another tenant's
//! data. The client itself stays tenant-agnostic: the events it reads come
//! back in the default tenant, as from a store of its own, and a tenant
//! application following the store through a `Sidecar` (evento-core's `rw`
//! feature) keeps its subscriptions' cursors and its snapshots in its own
//! store. The write notifications follow the session too: a client serving a
//! tenant is told of that tenant's writes, never of when or how often the
//! rest of the store is written.
//!
//! **Addresses.** A client names its server by an [`Address`]: an IP and port,
//! or a `host:port` looked up again on every connection attempt, so a
//! reconnect follows a load balancer or a DNS (geo-)failover to whichever node
//! now answers. Each server bounds a subscription's read by its own stability
//! watermark, so a client reconnecting to a node that is further behind never
//! skips events.
//!
//! **TLS.** [`serve_tls`] / [`serve_tls_with`] complete a rustls handshake
//! on each connection (the operator supplies the `TlsAcceptor`), and
//! [`ClientBuilder::tls`] the matching connector, mirroring `evento-cluster`'s
//! `serve`/`serve_tls` split. Plain TCP stays for trusted networks: a token
//! then travels in clear text.

mod client;
mod server;
mod wire;

pub use client::{Address, Client, ClientBuilder, ClientState, NotConnected};
pub use server::{
    serve, serve_tls, serve_tls_with, serve_with, Hello, Resolver, ServerHandle, Session,
};
pub use wire::Rejection;
