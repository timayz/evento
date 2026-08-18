//! Lightweight runtime counters for observability.
//!
//! A [`Metrics`] holds per-node event counts as plain atomics — cheap to bump on
//! the hot path. Read a point-in-time view with [`Metrics::snapshot`] (e.g. to
//! export to a metrics backend, or to assert behaviour in tests). Paired with the
//! `tracing` events the node emits at the same decision points.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-node event counters.
#[derive(Default)]
pub struct Metrics {
    /// Writes this node coordinated that committed.
    pub writes_committed: AtomicU64,
    /// Writes this node coordinated that lost the optimistic-version condition.
    pub writes_conflicted: AtomicU64,
    /// Coordinated writes decided on the one-round-trip fast path.
    pub fast_path: AtomicU64,
    /// Coordinated writes that fell back to the slow (Accept) path.
    pub slow_path: AtomicU64,
    /// Stalled transactions this node took over via the recovery sweep.
    pub recoveries: AtomicU64,
    /// Compaction rounds that advanced the redundancy watermark.
    pub compactions: AtomicU64,
    /// Watermark reports clamped below the local applied-through point because
    /// sync coverage hadn't (yet) proven the window — e.g. after a partition
    /// heal or restart, until anti-entropy re-covers it.
    pub watermark_clamps: AtomicU64,
    /// Group-commit journal flushes performed.
    pub journal_flushes: AtomicU64,
    /// Inbound messages handled.
    pub messages_handled: AtomicU64,
    /// Outbound messages shed because a peer's queue was full (backpressure).
    pub messages_shed: AtomicU64,
}

impl Metrics {
    /// Creates a zeroed metrics set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a coordinated write's outcome: committed vs conflicted, and whether
    /// it took the fast path.
    pub fn record_outcome(&self, conflict: bool, fast: bool) {
        if conflict {
            self.writes_conflicted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.writes_committed.fetch_add(1, Ordering::Relaxed);
        }
        let path = if fast {
            &self.fast_path
        } else {
            &self.slow_path
        };
        path.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that the recovery sweep took over a stalled transaction.
    pub fn record_recovery(&self) {
        self.recoveries.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a compaction round that advanced the watermark.
    pub fn record_compaction(&self) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a watermark report clamped by missing sync coverage.
    pub fn record_watermark_clamp(&self) {
        self.watermark_clamps.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a group-commit journal flush.
    pub fn record_flush(&self) {
        self.journal_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one inbound message handled.
    pub fn record_message(&self) {
        self.messages_handled.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one outbound message shed because the peer's queue was full.
    pub fn record_shed(&self) {
        self.messages_shed.fetch_add(1, Ordering::Relaxed);
    }

    /// A consistent-enough point-in-time view of all counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            writes_committed: self.writes_committed.load(Ordering::Relaxed),
            writes_conflicted: self.writes_conflicted.load(Ordering::Relaxed),
            fast_path: self.fast_path.load(Ordering::Relaxed),
            slow_path: self.slow_path.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            compactions: self.compactions.load(Ordering::Relaxed),
            watermark_clamps: self.watermark_clamps.load(Ordering::Relaxed),
            journal_flushes: self.journal_flushes.load(Ordering::Relaxed),
            messages_handled: self.messages_handled.load(Ordering::Relaxed),
            messages_shed: self.messages_shed.load(Ordering::Relaxed),
        }
    }
}

/// An immutable point-in-time view of [`Metrics`], field-for-field; see the
/// counter docs there.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub writes_committed: u64,
    pub writes_conflicted: u64,
    pub fast_path: u64,
    pub slow_path: u64,
    pub recoveries: u64,
    pub compactions: u64,
    pub watermark_clamps: u64,
    pub journal_flushes: u64,
    pub messages_handled: u64,
    pub messages_shed: u64,
}

impl MetricsSnapshot {
    /// The counters, each as `(prometheus name, help text, value)`. The single
    /// source of truth for both [`to_prometheus`](Self::to_prometheus) and
    /// [`to_prometheus_labeled`](Self::to_prometheus_labeled); add a counter here and
    /// both renderings pick it up.
    fn counters(&self) -> [(&'static str, &'static str, u64); 10] {
        [
            (
                "accord_writes_committed_total",
                "Writes this node coordinated that committed.",
                self.writes_committed,
            ),
            (
                "accord_writes_conflicted_total",
                "Writes this node coordinated that lost the optimistic-version condition.",
                self.writes_conflicted,
            ),
            (
                "accord_fast_path_total",
                "Coordinated writes decided on the one-round-trip fast path.",
                self.fast_path,
            ),
            (
                "accord_slow_path_total",
                "Coordinated writes that fell back to the slow (Accept) path.",
                self.slow_path,
            ),
            (
                "accord_recoveries_total",
                "Stalled transactions this node took over via the recovery sweep.",
                self.recoveries,
            ),
            (
                "accord_compactions_total",
                "Compaction rounds that advanced the redundancy watermark.",
                self.compactions,
            ),
            (
                "accord_watermark_clamps_total",
                "Watermark reports clamped below local applied-through by missing sync coverage.",
                self.watermark_clamps,
            ),
            (
                "accord_journal_flushes_total",
                "Group-commit journal flushes performed.",
                self.journal_flushes,
            ),
            (
                "accord_messages_handled_total",
                "Inbound messages handled.",
                self.messages_handled,
            ),
            (
                "accord_messages_shed_total",
                "Outbound messages shed because a peer's queue was full.",
                self.messages_shed,
            ),
        ]
    }

    /// Renders the counters in Prometheus text exposition format (no labels).
    ///
    /// Hand-rolled — every counter becomes a `# HELP` / `# TYPE … counter` /
    /// `name value` stanza. This keeps the crate dependency-free; the operator wires
    /// the actual `/metrics` HTTP endpoint and serves this string.
    pub fn to_prometheus(&self) -> String {
        self.to_prometheus_labeled(&[])
    }

    /// Like [`to_prometheus`](Self::to_prometheus) but attaches `labels` (e.g. the
    /// node id) to every metric, so a multi-node scrape can disambiguate series:
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use evento_accord::{
    /// #     HybridLogicalClock, InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Node,
    /// #     NodeId, StaticTopology,
    /// # };
    /// # let node_id = NodeId(0);
    /// # let net = InMemoryNetwork::new();
    /// # let node = Node::new(
    /// #     node_id,
    /// #     Arc::new(StaticTopology::new(node_id, vec![node_id])),
    /// #     Arc::new(HybridLogicalClock::new(node_id)),
    /// #     Arc::new(net.sink(node_id)),
    /// #     Arc::new(InMemoryDataStore::new()),
    /// #     Arc::new(InMemoryJournal::new()),
    /// # );
    /// let text = node.metrics().to_prometheus_labeled(&[("node", &node_id.0.to_string())]);
    /// ```
    pub fn to_prometheus_labeled(&self, labels: &[(&str, &str)]) -> String {
        let label_set = if labels.is_empty() {
            String::new()
        } else {
            let inner = labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{inner}}}")
        };

        let mut out = String::new();
        for (name, help, value) in self.counters() {
            out.push_str(&format!("# HELP {name} {help}\n"));
            out.push_str(&format!("# TYPE {name} counter\n"));
            out.push_str(&format!("{name}{label_set} {value}\n"));
        }
        out
    }
}

/// Escapes a Prometheus label value (`\`, `"`, and newlines), per the exposition
/// format spec.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_shed_increments_and_appears_in_snapshot() {
        let m = Metrics::new();
        assert_eq!(m.snapshot().messages_shed, 0);
        m.record_shed();
        m.record_shed();
        assert_eq!(m.snapshot().messages_shed, 2);
    }

    #[test]
    fn prometheus_renders_help_type_and_value_lines() {
        let snap = MetricsSnapshot {
            writes_committed: 7,
            messages_shed: 3,
            ..Default::default()
        };
        let text = snap.to_prometheus();
        assert!(text.contains("# HELP accord_writes_committed_total"));
        assert!(text.contains("# TYPE accord_writes_committed_total counter"));
        assert!(text.contains("\naccord_writes_committed_total 7\n"));
        assert!(text.contains("\naccord_messages_shed_total 3\n"));
        // Every counter renders three lines (HELP, TYPE, sample).
        assert_eq!(text.lines().count(), snap.counters().len() * 3);
    }

    #[test]
    fn prometheus_labels_are_attached_and_escaped() {
        let snap = MetricsSnapshot {
            writes_committed: 1,
            ..Default::default()
        };
        let text = snap.to_prometheus_labeled(&[("node", "n\"1")]);
        // Label set follows the metric name, with the quote escaped.
        assert!(text.contains("accord_writes_committed_total{node=\"n\\\"1\"} 1\n"));
    }
}
