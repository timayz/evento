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
//! let client = evento_remote::Client::connect("127.0.0.1:4321".parse()?).await?;
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
//! v1 is plaintext TCP for trusted networks; TLS variants are the designed
//! extension point (mirroring `evento-accord`'s `serve`/`serve_tls` split).

mod client;
mod server;
mod wire;

pub use client::{Client, ClientBuilder};
pub use server::{serve, ServerHandle};
