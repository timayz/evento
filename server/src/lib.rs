//! gRPC server exposing an evento [`Executor`] over the wire.
//!
//! [`EventStoreService`] wraps any `evento_core::Executor` and implements the
//! generated `EventStore` gRPC service (`Write` / `Read` / `LatestTimestamp` /
//! `Subscribe`), so non-Rust clients can use evento as an event store. Event
//! payloads and metadata values are treated as opaque bytes — the server never
//! decodes them.
//!
//! ```rust,ignore
//! use evento_server::{EventStoreService, proto::event_store_server::EventStoreServer};
//! use tonic::transport::Server;
//!
//! let svc = EventStoreService::new(executor); // executor: impl evento_core::Executor
//! Server::builder()
//!     .add_service(EventStoreServer::new(svc))
//!     .serve(addr)
//!     .await?;
//! ```
//!
//! [`Executor`]: evento_core::Executor

// `tonic::Status` is a large error type, but it is the mandatory error for every
// gRPC method — the generated trait signatures return `Result<_, Status>`, so it
// can't be boxed away.
#![allow(clippy::result_large_err)]

mod convert;
mod error;
mod service;
mod subscribe;
mod write;

/// Generated protobuf/tonic types for the `evento.v1` package.
pub mod proto {
    // Doc comments here are generated from the .proto and don't satisfy clippy's
    // doc-formatting lints.
    #![allow(clippy::doc_lazy_continuation)]
    tonic::include_proto!("evento.v1");
}

pub use service::EventStoreService;

/// Convenience re-export of the generated gRPC server wrapper.
pub use proto::event_store_server::{EventStore, EventStoreServer};
