//! OpenTelemetry tracing extensions for ACCORD.
//!
//! This module provides tracing span helpers and OpenTelemetry integration.
//! Enable the `opentelemetry` feature to use OpenTelemetry exporters.
//!
//! # Usage
//!
//! The ACCORD protocol is already instrumented with `tracing` spans.
//! To export traces to OpenTelemetry:
//!
//! ```ignore
//! use evento_accord::tracing_ext;
//!
//! // Install OpenTelemetry layer (requires `opentelemetry` feature)
//! #[cfg(feature = "opentelemetry")]
//! tracing_ext::install_opentelemetry_layer()?;
//! ```
//!
//! # Spans
//!
//! The following spans are emitted:
//!
//! - `accord.protocol.run` - Full protocol execution
//! - `accord.protocol.preaccept` - PreAccept phase
//! - `accord.protocol.accept` - Accept phase (slow path)
//! - `accord.protocol.commit` - Commit phase
//! - `accord.protocol.execute` - Transaction execution
//! - `accord.transport.send` - Message send
//! - `accord.transport.broadcast` - Message broadcast

use tracing::{info_span, Span};

/// Span names used by ACCORD.
pub mod span_names {
    /// Full protocol execution.
    pub const PROTOCOL_RUN: &str = "accord.protocol.run";
    /// PreAccept phase.
    pub const PREACCEPT: &str = "accord.protocol.preaccept";
    /// Accept phase.
    pub const ACCEPT: &str = "accord.protocol.accept";
    /// Commit phase.
    pub const COMMIT: &str = "accord.protocol.commit";
    /// Transaction execution.
    pub const EXECUTE: &str = "accord.protocol.execute";
    /// Message send.
    pub const TRANSPORT_SEND: &str = "accord.transport.send";
    /// Message broadcast.
    pub const TRANSPORT_BROADCAST: &str = "accord.transport.broadcast";
}

/// Attribute keys for spans.
pub mod attributes {
    /// Transaction ID.
    pub const TXN_ID: &str = "accord.txn_id";
    /// Node ID.
    pub const NODE_ID: &str = "accord.node_id";
    /// Message type.
    pub const MESSAGE_TYPE: &str = "accord.message_type";
    /// Whether fast path was taken.
    pub const FAST_PATH: &str = "accord.fast_path";
    /// Number of dependencies.
    pub const DEPS_COUNT: &str = "accord.deps_count";
    /// Execution timestamp.
    pub const EXECUTE_AT: &str = "accord.execute_at";
    /// Error type.
    pub const ERROR_TYPE: &str = "accord.error_type";
}

/// Create a span for protocol execution.
pub fn protocol_span(txn_id: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.protocol.run",
        txn_id = %txn_id,
        otel.name = span_names::PROTOCOL_RUN,
    )
}

/// Create a span for PreAccept phase.
pub fn preaccept_span(txn_id: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.protocol.preaccept",
        txn_id = %txn_id,
        otel.name = span_names::PREACCEPT,
    )
}

/// Create a span for Accept phase.
pub fn accept_span(txn_id: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.protocol.accept",
        txn_id = %txn_id,
        otel.name = span_names::ACCEPT,
    )
}

/// Create a span for Commit phase.
pub fn commit_span(txn_id: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.protocol.commit",
        txn_id = %txn_id,
        otel.name = span_names::COMMIT,
    )
}

/// Create a span for transaction execution.
pub fn execute_span(txn_id: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.protocol.execute",
        txn_id = %txn_id,
        otel.name = span_names::EXECUTE,
    )
}

/// Create a span for sending a message.
pub fn send_span(node_id: u16, message_type: &str) -> Span {
    info_span!(
        target: "accord",
        "accord.transport.send",
        node_id = %node_id,
        message_type = %message_type,
        otel.name = span_names::TRANSPORT_SEND,
    )
}

/// Create a span for broadcasting a message.
pub fn broadcast_span(message_type: &str, peer_count: usize) -> Span {
    info_span!(
        target: "accord",
        "accord.transport.broadcast",
        message_type = %message_type,
        peer_count = %peer_count,
        otel.name = span_names::TRANSPORT_BROADCAST,
    )
}

/// Record the result of a protocol operation on the current span.
pub fn record_result(fast_path: bool, deps_count: usize) {
    Span::current().record(attributes::FAST_PATH, fast_path);
    Span::current().record(attributes::DEPS_COUNT, deps_count);
}

/// Record an error on the current span.
pub fn record_error(error_type: &str) {
    Span::current().record(attributes::ERROR_TYPE, error_type);
}

#[cfg(feature = "opentelemetry")]
pub use otel::*;

#[cfg(feature = "opentelemetry")]
mod otel {
    //! OpenTelemetry integration.

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::TracerProvider;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Error type for OpenTelemetry setup.
    #[derive(Debug)]
    pub struct OtelError(pub String);

    impl std::fmt::Display for OtelError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "OpenTelemetry error: {}", self.0)
        }
    }

    impl std::error::Error for OtelError {}

    /// Install OpenTelemetry tracing layer with the given provider.
    ///
    /// This configures the global tracing subscriber to export spans
    /// to OpenTelemetry.
    pub fn install_opentelemetry_layer(provider: TracerProvider) -> Result<(), OtelError> {
        let tracer = provider.tracer("evento-accord");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(otel_layer)
            .try_init()
            .map_err(|e: tracing_subscriber::util::TryInitError| OtelError(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_span() {
        // Spans work even without a subscriber (they're just no-ops)
        let _span = protocol_span("test-txn-123");
    }

    #[test]
    fn test_preaccept_span() {
        let _span = preaccept_span("test-txn-456");
    }

    #[test]
    fn test_send_span() {
        let _span = send_span(1, "PreAccept");
    }

    #[test]
    fn test_broadcast_span() {
        let _span = broadcast_span("Commit", 3);
    }

    #[test]
    fn test_span_names() {
        assert_eq!(span_names::PROTOCOL_RUN, "accord.protocol.run");
        assert_eq!(span_names::PREACCEPT, "accord.protocol.preaccept");
        assert_eq!(span_names::ACCEPT, "accord.protocol.accept");
        assert_eq!(span_names::COMMIT, "accord.protocol.commit");
        assert_eq!(span_names::EXECUTE, "accord.protocol.execute");
    }

    #[test]
    fn test_attributes() {
        assert_eq!(attributes::TXN_ID, "accord.txn_id");
        assert_eq!(attributes::FAST_PATH, "accord.fast_path");
    }
}
