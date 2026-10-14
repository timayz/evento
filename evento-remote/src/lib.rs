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
//! Writes through the server bump a notification channel that is pushed to
//! every connected client, so client-side subscriptions wake with the same low
//! latency as a local `write_watch`. The server's `stable_timestamp` watermark
//! piggybacks on every frame, so fronting a replicated backend (Accord) keeps
//! subscription safety intact.
//!
//! **Many stores, credentials.** A host serving one store per tenant implements
//! [`Resolver`]: each connection opens with a `Hello` carrying the token the
//! client was built with ([`ClientBuilder::token`]), and the resolver answers
//! with the [`Session`] to serve it — the executor, something to keep alive
//! for the connection's lifetime (a tenant lease), a signal to end it (a
//! revoked credential) — or a [`Rejection`]. [`serve_with`] takes the
//! resolver; [`serve`] is the open single-store case.
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
//! [`ClientBuilder::tls`] the matching connector, mirroring `evento-accord`'s
//! `serve`/`serve_tls` split. Plain TCP stays for trusted networks: a token
//! then travels in clear text.

mod client;
mod server;
mod wire;

pub use client::{Address, Client, ClientBuilder};
pub use server::{
    serve, serve_tls, serve_tls_with, serve_with, Hello, Resolver, ServerHandle, Session,
};
pub use wire::Rejection;
