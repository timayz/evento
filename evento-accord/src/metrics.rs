//! Metrics for ACCORD consensus protocol.
//!
//! This module provides metrics using the `metrics` crate facade.
//! To export metrics to Prometheus, enable the `prometheus` feature and
//! call `install_prometheus_recorder()`.
//!
//! # Metrics
//!
//! ## Counters
//! - `accord_transactions_total` - Total transactions by status (preaccept, commit, execute)
//! - `accord_protocol_messages_total` - Protocol messages by type (preaccept, accept, commit)
//! - `accord_errors_total` - Errors by type
//!
//! ## Histograms
//! - `accord_transaction_latency_seconds` - Transaction latency by phase
//! - `accord_protocol_round_latency_seconds` - Protocol round latency
//!
//! ## Gauges
//! - `accord_pending_transactions` - Number of pending transactions
//! - `accord_active_connections` - Number of active peer connections
//! - `accord_execution_queue_depth` - Depth of execution queue
//!
//! # Example
//!
//! ```ignore
//! use evento_accord::metrics;
//!
//! // Install Prometheus recorder (requires `prometheus` feature)
//! #[cfg(feature = "prometheus")]
//! let handle = metrics::install_prometheus_recorder()?;
//!
//! // Metrics are automatically recorded during operation
//! // Access the Prometheus endpoint via the handle
//! ```

use metrics::{counter, gauge, histogram};
use std::time::Instant;

/// Metric names as constants for consistency.
pub mod names {
    /// Total transactions counter.
    pub const TRANSACTIONS_TOTAL: &str = "accord_transactions_total";
    /// Protocol messages counter.
    pub const MESSAGES_TOTAL: &str = "accord_protocol_messages_total";
    /// Errors counter.
    pub const ERRORS_TOTAL: &str = "accord_errors_total";
    /// Transaction latency histogram.
    pub const TRANSACTION_LATENCY: &str = "accord_transaction_latency_seconds";
    /// Protocol round latency histogram.
    pub const ROUND_LATENCY: &str = "accord_protocol_round_latency_seconds";
    /// Pending transactions gauge.
    pub const PENDING_TRANSACTIONS: &str = "accord_pending_transactions";
    /// Active connections gauge.
    pub const ACTIVE_CONNECTIONS: &str = "accord_active_connections";
    /// Execution queue depth gauge.
    pub const EXECUTION_QUEUE_DEPTH: &str = "accord_execution_queue_depth";
    /// Fast path ratio.
    pub const FAST_PATH_TOTAL: &str = "accord_fast_path_total";
    /// Slow path ratio.
    pub const SLOW_PATH_TOTAL: &str = "accord_slow_path_total";
}

/// Transaction status labels.
pub mod status {
    pub const PREACCEPTED: &str = "preaccepted";
    pub const ACCEPTED: &str = "accepted";
    pub const COMMITTED: &str = "committed";
    pub const EXECUTED: &str = "executed";
}

/// Message type labels.
pub mod message_type {
    pub const PREACCEPT: &str = "preaccept";
    pub const PREACCEPT_OK: &str = "preaccept_ok";
    pub const PREACCEPT_NACK: &str = "preaccept_nack";
    pub const ACCEPT: &str = "accept";
    pub const ACCEPT_OK: &str = "accept_ok";
    pub const ACCEPT_NACK: &str = "accept_nack";
    pub const COMMIT: &str = "commit";
    pub const SYNC_REQUEST: &str = "sync_request";
    pub const SYNC_RESPONSE: &str = "sync_response";
}

/// Error type labels.
pub mod error_type {
    pub const TIMEOUT: &str = "timeout";
    pub const QUORUM_NOT_REACHED: &str = "quorum_not_reached";
    pub const NODE_UNREACHABLE: &str = "node_unreachable";
    pub const BALLOT_TOO_LOW: &str = "ballot_too_low";
    pub const STORAGE: &str = "storage";
    pub const INTERNAL: &str = "internal";
}

/// Record a transaction status change.
pub fn record_transaction(status: &str) {
    counter!(names::TRANSACTIONS_TOTAL, "status" => status.to_string()).increment(1);
}

/// Record a protocol message sent or received.
pub fn record_message(msg_type: &str, direction: &str) {
    counter!(names::MESSAGES_TOTAL, "type" => msg_type.to_string(), "direction" => direction.to_string()).increment(1);
}

/// Record an error occurrence.
pub fn record_error(error_type: &str) {
    counter!(names::ERRORS_TOTAL, "type" => error_type.to_string()).increment(1);
}

/// Record a fast path completion.
pub fn record_fast_path() {
    counter!(names::FAST_PATH_TOTAL).increment(1);
}

/// Record a slow path completion.
pub fn record_slow_path() {
    counter!(names::SLOW_PATH_TOTAL).increment(1);
}

/// Record transaction latency.
pub fn record_transaction_latency(phase: &str, duration_secs: f64) {
    histogram!(names::TRANSACTION_LATENCY, "phase" => phase.to_string()).record(duration_secs);
}

/// Record protocol round latency.
pub fn record_round_latency(round: &str, duration_secs: f64) {
    histogram!(names::ROUND_LATENCY, "round" => round.to_string()).record(duration_secs);
}

/// Set the number of pending transactions.
pub fn set_pending_transactions(count: f64) {
    gauge!(names::PENDING_TRANSACTIONS).set(count);
}

/// Set the number of active connections.
pub fn set_active_connections(count: f64) {
    gauge!(names::ACTIVE_CONNECTIONS).set(count);
}

/// Set the execution queue depth.
pub fn set_execution_queue_depth(depth: f64) {
    gauge!(names::EXECUTION_QUEUE_DEPTH).set(depth);
}

/// Timer for measuring durations.
///
/// Records the elapsed time when dropped.
pub struct Timer {
    start: Instant,
    metric_name: &'static str,
    labels: Vec<(&'static str, String)>,
}

impl Timer {
    /// Start a new timer for transaction latency.
    pub fn transaction(phase: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric_name: names::TRANSACTION_LATENCY,
            labels: vec![("phase", phase.to_string())],
        }
    }

    /// Start a new timer for protocol round latency.
    pub fn round(round: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric_name: names::ROUND_LATENCY,
            labels: vec![("round", round.to_string())],
        }
    }

    /// Get elapsed time without recording.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    /// Stop the timer and record the duration.
    pub fn stop(self) {
        // Drop will record
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        match self.labels.as_slice() {
            [(k1, v1)] => {
                histogram!(self.metric_name, *k1 => v1.clone()).record(duration);
            }
            [(k1, v1), (k2, v2)] => {
                histogram!(self.metric_name, *k1 => v1.clone(), *k2 => v2.clone()).record(duration);
            }
            _ => {
                histogram!(self.metric_name).record(duration);
            }
        }
    }
}

/// Install Prometheus metrics recorder.
///
/// This sets up the global metrics recorder to export to Prometheus format.
/// Call this once at application startup.
///
/// Returns a handle that can be used to access the Prometheus endpoint.
#[cfg(feature = "prometheus")]
pub fn install_prometheus_recorder(
) -> Result<metrics_exporter_prometheus::PrometheusHandle, metrics_exporter_prometheus::BuildError>
{
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder.install_recorder()
}

/// Install Prometheus metrics recorder with custom configuration.
#[cfg(feature = "prometheus")]
pub fn install_prometheus_recorder_with_builder(
    builder: metrics_exporter_prometheus::PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusHandle, metrics_exporter_prometheus::BuildError>
{
    builder.install_recorder()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_transaction() {
        // Just verify it doesn't panic
        record_transaction(status::PREACCEPTED);
        record_transaction(status::COMMITTED);
        record_transaction(status::EXECUTED);
    }

    #[test]
    fn test_record_message() {
        record_message(message_type::PREACCEPT, "sent");
        record_message(message_type::PREACCEPT_OK, "received");
    }

    #[test]
    fn test_record_error() {
        record_error(error_type::TIMEOUT);
        record_error(error_type::NODE_UNREACHABLE);
    }

    #[test]
    fn test_record_latency() {
        record_transaction_latency("preaccept", 0.001);
        record_round_latency("preaccept", 0.002);
    }

    #[test]
    fn test_gauges() {
        set_pending_transactions(10.0);
        set_active_connections(3.0);
        set_execution_queue_depth(5.0);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::transaction("test");
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(timer.elapsed().as_millis() >= 1);
        timer.stop();
    }

    #[test]
    fn test_fast_slow_path() {
        record_fast_path();
        record_slow_path();
    }
}
