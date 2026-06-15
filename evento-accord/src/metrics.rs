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
    /// Group-commit journal flushes performed.
    pub journal_flushes: AtomicU64,
    /// Inbound messages handled.
    pub messages_handled: AtomicU64,
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
        let path = if fast { &self.fast_path } else { &self.slow_path };
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

    /// Records a group-commit journal flush.
    pub fn record_flush(&self) {
        self.journal_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one inbound message handled.
    pub fn record_message(&self) {
        self.messages_handled.fetch_add(1, Ordering::Relaxed);
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
            journal_flushes: self.journal_flushes.load(Ordering::Relaxed),
            messages_handled: self.messages_handled.load(Ordering::Relaxed),
        }
    }
}

/// An immutable point-in-time view of [`Metrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub writes_committed: u64,
    pub writes_conflicted: u64,
    pub fast_path: u64,
    pub slow_path: u64,
    pub recoveries: u64,
    pub compactions: u64,
    pub journal_flushes: u64,
    pub messages_handled: u64,
}
