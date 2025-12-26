//! Transport layer for inter-node communication.
//!
//! This module provides abstractions for network communication between
//! ACCORD nodes. Two implementations are provided:
//!
//! - [`TcpTransport`]: Production TCP-based transport
//! - [`ChannelTransport`]: In-memory transport for testing

pub mod channel;
pub mod framing;
pub mod tcp;
pub mod traits;

pub use channel::ChannelTransport;
pub use tcp::{TcpServer, TcpTransport};
pub use traits::{NodeAddr, NodeId, Transport};
