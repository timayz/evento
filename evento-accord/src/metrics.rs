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
    /// Same-shard peers this node stopped waiting for because they were silent
    /// past `NodeConfig::peer_absence_limit` (compaction then proceeds without
    /// them).
    pub peers_absent: AtomicU64,
    /// Times a peer told this node to rejoin the group from a snapshot: it had
    /// been absent while transactions it never applied were compacted.
    pub rejoins_required: AtomicU64,
    /// Snapshot pages this node served to joining peers.
    pub snapshot_pages: AtomicU64,
    /// Joins of this node that failed after part of the snapshot was
    /// installed: the store holds a prefix no contact can complete, so it
    /// must be emptied before the next join.
    pub joins_partial: AtomicU64,
    /// Decided-transaction applies and commit-condition reads the data store
    /// failed. The transaction is neither marked nor acknowledged; it is
    /// retried on the next drive, so a persistent count means a store that
    /// cannot take writes.
    pub execution_failures: AtomicU64,
    /// Command pages this node served to joining peers: the applied commands
    /// a join imports into its conflict graph, a page budget at a time.
    pub command_pages: AtomicU64,
    /// Votes refused below the vote fence: a PreAccept, an Accept or a
    /// recovery's pre-accept of a transaction this node never witnessed,
    /// below what it vouched for — a watermark report, sync coverage, a
    /// join's vouch, its stable point — or could vouch for now (#337).
    pub votes_fenced: AtomicU64,
    /// Votes refused to an original coordinator past its deadline: its
    /// PreAccept, or its Accept at the transaction's own ballot, for a
    /// transaction this node never witnessed, older than its compaction
    /// margin. A steady rate means coordinators slower than the margin.
    pub votes_late: AtomicU64,
    /// Decisions learned from peers for transactions this node never
    /// witnessed, but a command committed here waits on (#330).
    pub dependencies_learned: AtomicU64,
    /// Times this node opened joining over an empty journal and set an
    /// amnesia fence: it lost the journal it voted with (#312).
    pub amnesia_fences: AtomicU64,
    /// Joins over a lost journal that found no contact vouching past the
    /// fence ([`FenceNotCovered`](crate::FenceNotCovered)): nothing was
    /// installed. A count that keeps rising means a transaction below the
    /// fence no replica can decide any more (see OPERATIONS §7).
    pub amnesia_joins_refused: AtomicU64,
    /// Times a peer found this node's journal rolled back to an older copy
    /// (#339): the node stops taking part until its host opens it again
    /// over an empty journal and store, joining.
    pub journals_rolled_back: AtomicU64,
    /// Votes refused for a transaction whose `t0` lies more than two clock
    /// skews past this node's clock: its coordinator's clock runs beyond the
    /// skew bound.
    pub votes_future_dated: AtomicU64,
    /// Config-Paxos promises and accepts this node withheld because it lost
    /// a journal under this node id: it abstains from config Paxos for good.
    pub config_votes_withheld: AtomicU64,
    /// PreAccepts and read probes refused because the coordinator planned
    /// them under an older layout and a key they touch changed replicas
    /// since: it planned before it installed the change (it retries under
    /// the new layout).
    pub votes_retired: AtomicU64,
    /// Messages about a transaction planned under an epoch this node has not
    /// installed yet: left unanswered while it fetches the metadata it is
    /// missing from the sender.
    pub messages_future_epoch: AtomicU64,
    /// Votes a layout change refused: for a transaction this node never
    /// witnessed, on a key a transition moves, above this node's fence point
    /// — or planned under the new layout below its activation's clock floor.
    pub votes_change_fenced: AtomicU64,
    /// Writes this node refused to coordinate because a key they touch is
    /// moving ([`TopologyChanging`](crate::TopologyChanging)).
    pub writes_topology_changing: AtomicU64,
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

    /// Records that a silent peer is no longer waited for.
    pub fn record_peer_absent(&self) {
        self.peers_absent.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a peer told this node to rejoin.
    pub fn record_rejoin_required(&self) {
        self.rejoins_required.fetch_add(1, Ordering::Relaxed);
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

    /// Records one snapshot page served to a joining peer.
    pub fn record_snapshot_page(&self) {
        self.snapshot_pages.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one command page served to a joining peer.
    pub fn record_command_page(&self) {
        self.command_pages.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a vote refused below the vote fence.
    pub fn record_vote_fenced(&self) {
        self.votes_fenced.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a vote refused to an original coordinator past its deadline.
    pub fn record_vote_late(&self) {
        self.votes_late.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a decision learned for a dependency never witnessed here.
    pub fn record_dependency_learned(&self) {
        self.dependencies_learned.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an amnesia fence set over a lost journal.
    pub fn record_amnesia_fence(&self) {
        self.amnesia_fences.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a journal a peer found rolled back.
    pub fn record_journal_rolled_back(&self) {
        self.journals_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a join over a lost journal that no contact vouched for.
    pub fn record_amnesia_join_refused(&self) {
        self.amnesia_joins_refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a vote refused for a transaction dated past the skew bound.
    pub fn record_vote_future_dated(&self) {
        self.votes_future_dated.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a PreAccept or read probe refused for a layout retired since.
    pub fn record_vote_retired(&self) {
        self.votes_retired.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a message left unanswered for a future epoch.
    pub fn record_message_future_epoch(&self) {
        self.messages_future_epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a vote a layout change refused.
    pub fn record_vote_change_fenced(&self) {
        self.votes_change_fenced.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a write refused while a key it touches moves.
    pub fn record_write_topology_changing(&self) {
        self.writes_topology_changing
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a config-Paxos vote withheld after a lost journal.
    pub fn record_config_vote_withheld(&self) {
        self.config_votes_withheld.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a join that failed after part of the snapshot was installed.
    pub fn record_join_partial(&self) {
        self.joins_partial.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a data-store apply or commit-condition read that failed.
    pub fn record_execution_failure(&self) {
        self.execution_failures.fetch_add(1, Ordering::Relaxed);
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
            peers_absent: self.peers_absent.load(Ordering::Relaxed),
            rejoins_required: self.rejoins_required.load(Ordering::Relaxed),
            snapshot_pages: self.snapshot_pages.load(Ordering::Relaxed),
            joins_partial: self.joins_partial.load(Ordering::Relaxed),
            execution_failures: self.execution_failures.load(Ordering::Relaxed),
            command_pages: self.command_pages.load(Ordering::Relaxed),
            votes_fenced: self.votes_fenced.load(Ordering::Relaxed),
            votes_late: self.votes_late.load(Ordering::Relaxed),
            dependencies_learned: self.dependencies_learned.load(Ordering::Relaxed),
            amnesia_fences: self.amnesia_fences.load(Ordering::Relaxed),
            amnesia_joins_refused: self.amnesia_joins_refused.load(Ordering::Relaxed),
            journals_rolled_back: self.journals_rolled_back.load(Ordering::Relaxed),
            votes_future_dated: self.votes_future_dated.load(Ordering::Relaxed),
            config_votes_withheld: self.config_votes_withheld.load(Ordering::Relaxed),
            votes_retired: self.votes_retired.load(Ordering::Relaxed),
            messages_future_epoch: self.messages_future_epoch.load(Ordering::Relaxed),
            votes_change_fenced: self.votes_change_fenced.load(Ordering::Relaxed),
            writes_topology_changing: self.writes_topology_changing.load(Ordering::Relaxed),
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
    pub peers_absent: u64,
    pub rejoins_required: u64,
    pub snapshot_pages: u64,
    pub joins_partial: u64,
    pub execution_failures: u64,
    pub command_pages: u64,
    pub votes_fenced: u64,
    pub votes_late: u64,
    pub dependencies_learned: u64,
    pub amnesia_fences: u64,
    pub amnesia_joins_refused: u64,
    pub journals_rolled_back: u64,
    pub votes_future_dated: u64,
    pub config_votes_withheld: u64,
    pub votes_retired: u64,
    pub messages_future_epoch: u64,
    pub votes_change_fenced: u64,
    pub writes_topology_changing: u64,
}

impl MetricsSnapshot {
    /// The counters, each as `(prometheus name, help text, value)`. The single
    /// source of truth for both [`to_prometheus`](Self::to_prometheus) and
    /// [`to_prometheus_labeled`](Self::to_prometheus_labeled); add a counter here and
    /// both renderings pick it up.
    fn counters(&self) -> [(&'static str, &'static str, u64); 28] {
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
            (
                "accord_peers_absent_total",
                "Silent shard peers this node stopped waiting for (compaction proceeds without them).",
                self.peers_absent,
            ),
            (
                "accord_rejoins_required_total",
                "Times a peer told this node to rejoin the group from a snapshot.",
                self.rejoins_required,
            ),
            (
                "accord_snapshot_pages_total",
                "Snapshot pages served to joining peers.",
                self.snapshot_pages,
            ),
            (
                "accord_joins_partial_total",
                "Joins that failed after part of the snapshot was installed (empty the store before retrying).",
                self.joins_partial,
            ),
            (
                "accord_execution_failures_total",
                "Decided-transaction applies and commit-condition reads the data store failed (retried, never acknowledged).",
                self.execution_failures,
            ),
            (
                "accord_command_pages_total",
                "Command pages served to joining peers.",
                self.command_pages,
            ),
            (
                "accord_votes_fenced_total",
                "Votes refused for transactions never witnessed here, below what this node vouched for.",
                self.votes_fenced,
            ),
            (
                "accord_votes_late_total",
                "Votes refused to original coordinators older than the compaction margin, for transactions never witnessed here.",
                self.votes_late,
            ),
            (
                "accord_dependencies_learned_total",
                "Decisions learned from peers for dependencies never witnessed here.",
                self.dependencies_learned,
            ),
            (
                "accord_amnesia_fences_total",
                "Times this node opened joining over an empty journal and set an amnesia fence.",
                self.amnesia_fences,
            ),
            (
                "accord_amnesia_joins_refused_total",
                "Joins over a lost journal that no contact vouched past the fence (nothing installed).",
                self.amnesia_joins_refused,
            ),
            (
                "accord_journals_rolled_back_total",
                "Times a peer found this node's journal rolled back to an older copy.",
                self.journals_rolled_back,
            ),
            (
                "accord_votes_future_dated_total",
                "Votes refused for transactions dated more than two clock skews past this node's clock.",
                self.votes_future_dated,
            ),
            (
                "accord_config_votes_withheld_total",
                "Config-Paxos votes withheld by a node that lost a journal under its node id.",
                self.config_votes_withheld,
            ),
            (
                "accord_votes_retired_total",
                "PreAccepts and read probes refused: planned under a layout whose keys changed replicas since.",
                self.votes_retired,
            ),
            (
                "accord_messages_future_epoch_total",
                "Messages about a transaction planned under an epoch not installed here yet (metadata fetched).",
                self.messages_future_epoch,
            ),
            (
                "accord_votes_change_fenced_total",
                "Votes a layout change refused: never witnessed, on a moved key, past the fence (or below the floor).",
                self.votes_change_fenced,
            ),
            (
                "accord_writes_topology_changing_total",
                "Writes refused while a key they touch moves to other replicas.",
                self.writes_topology_changing,
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
