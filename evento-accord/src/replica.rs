//! Replica-side consensus state machine.
//!
//! A [`Replica`] tracks every transaction it has witnessed and the conflict
//! graph between them. It is deliberately synchronous and side-effect free:
//! [`Node`](crate::node::Node) owns it behind a lock, calls these methods to
//! mutate consensus state, and performs the asynchronous data-store reads and
//! applies that [`next_read`](Replica::next_read) and
//! [`next_apply`](Replica::next_apply) schedule.
//!
//! ## Timestamps and dependencies
//!
//! On PreAccept a replica computes an execution timestamp for the transaction:
//! `t0` if no conflicting transaction has a higher one, otherwise the smallest
//! timestamp (carrying the transaction's own coordinator id) that exceeds every
//! conflict. Its dependencies are the conflicting transactions whose `t0` is
//! below that execution timestamp.
//!
//! ## Ballots and recovery
//!
//! Each command tracks a `promised` ballot (advanced by Accept and Recover) and
//! the `accepted` ballot of its current execute-at/deps. A message carrying a
//! lower ballot is rejected so a stale coordinator cannot overwrite a recovered
//! decision. [`recover`](Replica::recover) reports a replica's knowledge plus
//! `superseding_rejects`: whether a later conflicting transaction failed to
//! witness this one, which proves it could not have committed on the fast path.

use std::collections::{BTreeSet, HashMap};

use evento_core::Event;

use crate::clock::{Ballot, NodeId, Timestamp, TxnId};
use crate::message::{CommandState, Key, Status};

/// Per-replica consensus state.
pub struct Replica {
    /// Every transaction this replica knows about.
    commands: HashMap<TxnId, CommandState>,
    /// Index from key to the transactions touching it, for conflict lookup.
    by_key: HashMap<Key, BTreeSet<TxnId>>,
    /// Execution queue: the `(execute_at, txn)` of every command awaiting
    /// execution — status [`Committed`](Status::Committed) or
    /// [`Reading`](Status::Reading), i.e. final order known but not yet applied.
    /// Ordered, so [`next_apply`](Replica::next_apply)/[`next_read`](Replica::next_read)
    /// scan only the (few) un-applied commands in execution order instead of every
    /// command — the apply pipeline stays O(pending), not O(all commands), as
    /// applied state accumulates between compactions. Kept in lockstep with
    /// `commands` (an entry is present iff its status is Committed or Reading).
    pending: BTreeSet<(Timestamp, TxnId)>,
    /// Redundancy watermark: every transaction with `t0` below this has been
    /// applied on every replica, so it is dropped from `commands`/`by_key` and a
    /// dependency below it counts as already satisfied (its effect is in the data
    /// store). Advances monotonically via [`compact`](Replica::compact).
    redundant_before: Timestamp,
}

impl Default for Replica {
    fn default() -> Self {
        Self {
            commands: HashMap::new(),
            by_key: HashMap::new(),
            pending: BTreeSet::new(),
            redundant_before: Timestamp::MIN,
        }
    }
}

/// Whether a command at `status` is awaiting execution (in the `pending` queue):
/// its execution order is final but it has not been applied yet.
fn awaiting_execution(status: Status) -> bool {
    status >= Status::Committed && status != Status::Applied
}

/// A transaction whose dependencies are satisfied and that the node should now
/// read (and later apply). The [`Reading`](Status::Reading) status, set when this
/// is returned, holds the slot so dependents keep waiting.
pub struct Ready {
    /// The transaction to read/apply.
    pub txn: TxnId,
    /// Its execution timestamp (the global-order position).
    pub execute_at: Timestamp,
    /// The transaction's events (the node filters to its owned subset).
    pub events: Vec<Event>,
    /// Node coordinating the decision, to report the read/apply result to.
    pub reply_to: NodeId,
}

/// A transaction with a recorded decision whose dependencies are satisfied,
/// ready for the node to enact (append if `commit`, else abort).
pub struct ApplyReady {
    /// The transaction to apply.
    pub txn: TxnId,
    /// Its execution timestamp.
    pub execute_at: Timestamp,
    /// The events to append on commit.
    pub events: Vec<Event>,
    /// The coordinator's decision: commit (append) or abort (no-op).
    pub commit: bool,
    /// Node to report the apply acknowledgement to.
    pub reply_to: NodeId,
}

/// What a replica reports when asked to recover a transaction.
pub struct RecoverState {
    /// False when this replica never witnessed the transaction.
    pub known: bool,
    /// Phase the replica had reached.
    pub status: Status,
    /// Ballot at which its execute-at/deps were accepted.
    pub accepted: Ballot,
    /// Its current execution timestamp.
    pub execute_at: Timestamp,
    /// Its current dependency set.
    pub deps: Vec<TxnId>,
    /// A later conflicting transaction did not witness this one — proof the
    /// transaction could not have committed on the fast path.
    pub superseding_rejects: bool,
    /// This replica's owned keys for the transaction.
    pub keys: Vec<Key>,
    /// The events, so a recovery coordinator can drive the apply.
    pub events: Vec<Event>,
}

impl Replica {
    /// Creates an empty replica.
    pub fn new() -> Self {
        Self::default()
    }

    /// Conflicting transactions: those sharing at least one key with `keys`,
    /// excluding `self_txn`.
    fn conflicts(&self, keys: &[Key], self_txn: TxnId) -> Vec<TxnId> {
        let mut out = BTreeSet::new();
        for key in keys {
            if let Some(txns) = self.by_key.get(key) {
                for &txn in txns {
                    if txn != self_txn {
                        out.insert(txn);
                    }
                }
            }
        }
        out.into_iter().collect()
    }

    /// Smallest timestamp owned by `node` that is strictly greater than `t`.
    fn successor_with_node(t: Timestamp, node: NodeId) -> Timestamp {
        if node > t.node {
            Timestamp {
                micros: t.micros,
                logical: t.logical,
                node,
            }
        } else {
            Timestamp {
                micros: t.micros,
                logical: t.logical + 1,
                node,
            }
        }
    }

    /// Indexes a transaction's keys and inserts its command state, keeping the
    /// `pending` execution queue in lockstep (including when overwriting an
    /// existing command, e.g. [`import_applied`](Self::import_applied)).
    fn insert(&mut self, cmd: CommandState) {
        if let Some(old) = self.commands.get(&cmd.txn) {
            if awaiting_execution(old.status) {
                self.pending.remove(&(old.execute_at, old.txn));
            }
        }
        for key in &cmd.keys {
            self.by_key.entry(key.clone()).or_default().insert(cmd.txn);
        }
        if awaiting_execution(cmd.status) {
            self.pending.insert((cmd.execute_at, cmd.txn));
        }
        self.commands.insert(cmd.txn, cmd);
    }

    /// A **read-index probe**: the `(execute_at, deps)` a read at `txn`'s
    /// timestamp over `keys` would witness, computed exactly like [`preaccept`]
    /// but storing **nothing** (a read never enters the conflict graph). The
    /// coordinator unions these across a quorum, then waits for the deps to be
    /// applied locally before serving — so the read reflects every write that
    /// committed before it began.
    ///
    /// [`preaccept`]: Self::preaccept
    pub fn read_probe(&self, txn: TxnId, keys: &[Key]) -> (Timestamp, Vec<TxnId>) {
        let conflicts = self.conflicts(keys, txn);

        let mut execute_at = txn.0;
        for &c in &conflicts {
            let c_exec = self.commands[&c].execute_at;
            let candidate = Self::successor_with_node(c_exec, txn.0.node);
            if candidate > execute_at {
                execute_at = candidate;
            }
        }

        let deps: Vec<TxnId> = conflicts
            .iter()
            .copied()
            .filter(|c| c.0 < execute_at)
            .collect();

        (execute_at, deps)
    }

    /// Whether every transaction in `deps` has been applied locally — i.e. its
    /// effect is in the data store. A dep is satisfied if it is `Applied` (an
    /// abort is also marked applied), or compacted away below the redundancy
    /// watermark (which means every replica had applied it). A dep this node has
    /// not yet received, or has not yet executed, is **not** satisfied — the
    /// read barrier waits for it.
    pub fn deps_applied(&self, deps: &[TxnId]) -> bool {
        deps.iter().all(|dep| match self.commands.get(dep) {
            Some(d) => d.status == Status::Applied,
            None => dep.0 < self.redundant_before,
        })
    }

    /// Handles PreAccept: records the transaction and returns the execution
    /// timestamp and dependencies this replica witnesses. Idempotent.
    pub fn preaccept(
        &mut self,
        txn: TxnId,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> (Timestamp, Vec<TxnId>) {
        if let Some(existing) = self.commands.get(&txn) {
            return (existing.execute_at, existing.deps.clone());
        }

        let conflicts = self.conflicts(&keys, txn);

        let mut execute_at = txn.0;
        for &c in &conflicts {
            let c_exec = self.commands[&c].execute_at;
            let candidate = Self::successor_with_node(c_exec, txn.0.node);
            if candidate > execute_at {
                execute_at = candidate;
            }
        }

        let deps: Vec<TxnId> = conflicts
            .iter()
            .copied()
            .filter(|c| c.0 < execute_at)
            .collect();

        self.insert(CommandState {
            txn,
            status: Status::PreAccepted,
            promised: Ballot(txn.0),
            accepted: Ballot(txn.0),
            execute_at,
            deps: deps.clone(),
            keys,
            events,
            reply_to: txn.0.node,
            decision: None,
            applied_conflict: None,
        });

        (execute_at, deps)
    }

    /// Handles Accept under `ballot`: adopts the coordinator's execution
    /// timestamp and dependency set. Returns the replica's dependencies, or
    /// `Err(promised)` if it has already promised a higher ballot.
    pub fn accept(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
    ) -> Result<Vec<TxnId>, Ballot> {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            if ballot < cmd.promised {
                return Err(cmd.promised);
            }
            // Commit is terminal: a (possibly reordered/stale) Accept must never
            // downgrade an already-committed transaction's final timestamp/deps —
            // doing so would also desync the `pending` execution queue. Answer with
            // the committed deps instead.
            if cmd.status >= Status::Committed {
                return Ok(cmd.deps.clone());
            }
            cmd.promised = ballot;
            cmd.accepted = ballot;
            cmd.status = Status::Accepted;
            cmd.execute_at = execute_at;
            cmd.deps = deps;
            Ok(cmd.deps.clone())
        } else {
            // The replica missed PreAccept; record what it was told (events
            // arrive with Commit).
            self.insert(CommandState {
                txn,
                status: Status::Accepted,
                promised: ballot,
                accepted: ballot,
                execute_at,
                deps: deps.clone(),
                keys: Vec::new(),
                events: Vec::new(),
                reply_to: txn.0.node,
                decision: None,
                applied_conflict: None,
            });
            Ok(deps)
        }
    }

    /// Handles Commit: finalises the execution timestamp, dependencies, events,
    /// and the node to report the result to. Idempotent — once a command is
    /// Committed or Applied its decision is not downgraded, but `reply_to` is
    /// refreshed so a recoverer can still collect the outcome.
    pub fn commit(
        &mut self,
        txn: TxnId,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        events: Vec<Event>,
        reply_to: NodeId,
    ) {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.reply_to = reply_to;
            if cmd.status >= Status::Committed {
                return;
            }
            cmd.status = Status::Committed;
            cmd.execute_at = execute_at;
            cmd.deps = deps;
            if cmd.events.is_empty() {
                cmd.events = events;
            }
            // Newly executable: enter the execution queue at its final order.
            self.pending.insert((execute_at, txn));
        } else {
            self.insert(CommandState {
                txn,
                status: Status::Committed,
                promised: Ballot(txn.0),
                accepted: Ballot(txn.0),
                execute_at,
                deps,
                keys: Vec::new(),
                events,
                reply_to,
                decision: None,
                applied_conflict: None,
            });
        }
    }

    /// The stored apply outcome for `txn`, if it has already been applied. Lets
    /// a node answer a late Commit (e.g. from recovery) without re-executing.
    pub fn applied_result(&self, txn: TxnId) -> Option<bool> {
        self.commands
            .get(&txn)
            .filter(|cmd| cmd.status == Status::Applied)
            .and_then(|cmd| cmd.applied_conflict)
    }

    /// Handles Recover under `ballot`: promises the ballot and reports the
    /// replica's knowledge, or returns `Err(promised)` if a higher ballot was
    /// already promised.
    pub fn recover(&mut self, txn: TxnId, ballot: Ballot) -> Result<RecoverState, Ballot> {
        if !self.commands.contains_key(&txn) {
            return Ok(RecoverState {
                known: false,
                status: Status::PreAccepted,
                accepted: Ballot(txn.0),
                execute_at: txn.0,
                deps: Vec::new(),
                superseding_rejects: false,
                keys: Vec::new(),
                events: Vec::new(),
            });
        }

        {
            let cmd = self.commands.get_mut(&txn).expect("present");
            if ballot < cmd.promised {
                return Err(cmd.promised);
            }
            cmd.promised = ballot;
        }

        let superseding_rejects = self.superseding_rejects(txn);
        let cmd = &self.commands[&txn];
        Ok(RecoverState {
            known: true,
            status: cmd.status,
            accepted: cmd.accepted,
            execute_at: cmd.execute_at,
            deps: cmd.deps.clone(),
            superseding_rejects,
            keys: cmd.keys.clone(),
            events: cmd.events.clone(),
        })
    }

    /// The node coordinating `txn`'s decision (set by Commit), to report results
    /// to when applying.
    pub fn reply_to(&self, txn: TxnId) -> Option<NodeId> {
        self.commands.get(&txn).map(|cmd| cmd.reply_to)
    }

    /// Whether any *later* conflicting transaction (higher `t0`) has reached
    /// Accepted+ without `txn` in its dependencies. Such a transaction proves
    /// `txn` did not commit on the fast path, since a fast-path commit at `t0`
    /// forces every later conflict to witness it.
    fn superseding_rejects(&self, txn: TxnId) -> bool {
        let cmd = &self.commands[&txn];
        self.conflicts(&cmd.keys, txn).into_iter().any(|g| {
            let gc = &self.commands[&g];
            g.0 > txn.0 && gc.status >= Status::Accepted && !gc.deps.contains(&txn)
        })
    }

    /// Whether `txn`'s dependencies permit it to execute on this shard.
    ///
    /// The barrier is the transaction's dependency set, which the coordinator
    /// scoped to **this shard** (so it never names a transaction this replica
    /// cannot witness). Each dependency must be either applied (it orders before
    /// and its effect is visible) or committed with a strictly higher order
    /// position (it orders after). A dependency not yet known or committed means
    /// wait — its final timestamp could place it before this transaction.
    fn is_ready(&self, txn: TxnId) -> bool {
        let cmd = match self.commands.get(&txn) {
            Some(cmd) if cmd.status >= Status::Committed && cmd.status != Status::Applied => cmd,
            _ => return false,
        };

        // Order by (execute_at, txn): a unique global total order.
        let here = (cmd.execute_at, cmd.txn);
        cmd.deps.iter().all(|dep| match self.commands.get(dep) {
            Some(d) if d.status == Status::Applied => true,
            Some(d) if d.status >= Status::Committed && (d.execute_at, *dep) > here => true,
            // A dependency we no longer hold but that is below the redundancy
            // watermark was applied here and garbage-collected: it orders before
            // this transaction and its effect is already in the data store. (A
            // committed transaction's deps are the quorum union, so a peer can
            // legitimately name a transaction we have already compacted away.)
            None if dep.0 < self.redundant_before => true,
            _ => false,
        })
    }

    /// Picks the lowest-order committed transaction that is ready to read and has
    /// no decision yet, marking it [`Reading`](Status::Reading) so it keeps
    /// blocking dependents until applied. Decided transactions go through
    /// [`next_apply`](Replica::next_apply) instead.
    pub fn next_read(&mut self) -> Option<Ready> {
        // The `pending` queue is ordered by `(execute_at, txn)`, so the first
        // entry that is ready and undecided is the lowest-order one to read.
        let txn = self.pending.iter().map(|&(_, txn)| txn).find(|&txn| {
            let cmd = &self.commands[&txn];
            cmd.status == Status::Committed && cmd.decision.is_none() && self.is_ready(txn)
        })?;

        let cmd = &self.commands[&txn];
        let ready = Ready {
            txn,
            execute_at: cmd.execute_at,
            events: cmd.events.clone(),
            reply_to: cmd.reply_to,
        };
        // Committed → Reading: still awaiting execution at the same order, so its
        // `pending` entry is unchanged.
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.status = Status::Reading;
        }
        Some(ready)
    }

    /// Records the coordinator's commit/abort decision for `txn` (from Apply).
    /// Does not apply it — [`next_apply`](Replica::next_apply) enacts it once the
    /// dependencies clear, so a replica that was behind still converges.
    pub fn record_decision(&mut self, txn: TxnId, commit: bool) {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            if cmd.decision.is_none() {
                cmd.decision = Some(commit);
            }
        }
    }

    /// Picks the lowest-order transaction that has a decision and whose
    /// dependencies are satisfied, to be applied next.
    pub fn next_apply(&mut self) -> Option<ApplyReady> {
        // `pending` holds exactly the un-applied (Committed/Reading) commands in
        // execution order; the first that is decided and ready applies next.
        // (A command decided before it committed isn't in `pending` yet — it was
        // never ready either, so the old full scan would have skipped it too.)
        let txn = self
            .pending
            .iter()
            .map(|&(_, txn)| txn)
            .find(|&txn| self.commands[&txn].decision.is_some() && self.is_ready(txn))?;

        let cmd = &self.commands[&txn];
        Some(ApplyReady {
            txn,
            execute_at: cmd.execute_at,
            events: cmd.events.clone(),
            commit: cmd.decision.expect("decision present"),
            reply_to: cmd.reply_to,
        })
    }

    /// Records that a transaction's decision has been enacted, storing its
    /// conflict outcome and unblocking dependents.
    pub fn mark_applied(&mut self, txn: TxnId, conflict: bool) {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            let at = cmd.execute_at;
            cmd.status = Status::Applied;
            cmd.applied_conflict = Some(conflict);
            self.pending.remove(&(at, txn));
        }
    }

    /// A snapshot of a command's durable state, for the [`Journal`](crate::api::Journal).
    pub fn snapshot(&self, txn: TxnId) -> Option<CommandState> {
        self.commands.get(&txn).cloned()
    }

    /// Restores a command from the journal on restart, preserving its actual
    /// status, ballots, and decision (unlike [`import_applied`], which is for
    /// bootstrap and forces `Applied`).
    pub fn restore(&mut self, cmd: CommandState) {
        self.insert(cmd);
    }

    /// Transactions this replica knows but has not applied, whose `t0` is older
    /// than `cutoff` — i.e. stalled long enough to warrant recovery (the
    /// coordinator is presumed dead). Drives automatic progress.
    pub fn stuck(&self, cutoff: Timestamp) -> Vec<TxnId> {
        let mut stuck: Vec<TxnId> = self
            .commands
            .values()
            .filter(|cmd| cmd.status != Status::Applied && cmd.txn.0 < cutoff)
            .map(|cmd| cmd.txn)
            .collect();
        // Sorted so the recovery sweep drives transactions in a deterministic
        // order (the raw HashMap iteration order is not reproducible).
        stuck.sort();
        stuck
    }

    /// Every applied command, in execution-timestamp order — the committed state
    /// a joining node bootstraps from.
    pub fn export_applied(&self) -> Vec<CommandState> {
        let mut applied: Vec<CommandState> = self
            .commands
            .values()
            .filter(|cmd| cmd.status == Status::Applied)
            .cloned()
            .collect();
        applied.sort_by_key(|cmd| (cmd.execute_at, cmd.txn));
        applied
    }

    /// Imports an already-applied command during bootstrap: records it (so this
    /// replica's conflict graph and dependency barriers see it) unless already
    /// known. Returns whether it was newly inserted.
    pub fn import_applied(&mut self, mut cmd: CommandState) -> bool {
        // Already applied here — nothing to do.
        if matches!(self.commands.get(&cmd.txn), Some(c) if c.status == Status::Applied) {
            return false;
        }
        // Either new, or known but **not yet applied** — e.g. this node received the
        // transaction's Commit but missed its Apply (it was briefly down), so it is
        // stuck at `Committed` with no decision: normal execution won't apply it
        // (that needs a decision) and, before this, anti-entropy skipped it because
        // it was "already present", leaving the node permanently behind. Adopt the
        // contact's applied state (overwriting the stale entry) so its events get
        // applied locally and the node converges.
        cmd.status = Status::Applied;
        self.insert(cmd);
        true
    }

    /// The current redundancy watermark (see the field docs).
    pub fn redundant_before(&self) -> Timestamp {
        self.redundant_before
    }

    /// How many commands this replica currently holds — the in-memory consensus
    /// state whose growth [`compact`](Replica::compact) bounds.
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    /// The largest watermark `<= cutoff` that is *locally* safe: no transaction
    /// this replica holds below the result is still un-applied. (Below it, every
    /// command here is `Applied`, so its effect is durable in the data store.)
    /// Reported to peers; the cluster-safe watermark is the min across the shard.
    pub fn applied_through(&self, cutoff: Timestamp) -> Timestamp {
        match self
            .commands
            .values()
            .filter(|cmd| cmd.status != Status::Applied)
            .map(|cmd| cmd.txn.0)
            .min()
        {
            Some(oldest_unapplied) => cutoff.min(oldest_unapplied),
            None => cutoff,
        }
    }

    /// Drops every `Applied` command with `t0 < before` from `commands` and prunes
    /// `by_key`, advancing the redundancy watermark. `before` is first clamped to
    /// the locally-safe point ([`applied_through`](Replica::applied_through)) so a
    /// not-yet-applied transaction is never declared redundant; the *caller* is
    /// responsible for the cross-replica guarantee (that every replica has applied
    /// everything below `before`). Monotonic and idempotent.
    pub fn compact(&mut self, before: Timestamp) {
        let safe = self.applied_through(before);
        if safe <= self.redundant_before {
            return;
        }
        let drop: Vec<TxnId> = self
            .commands
            .values()
            .filter(|cmd| cmd.status == Status::Applied && cmd.txn.0 < safe)
            .map(|cmd| cmd.txn)
            .collect();
        for txn in &drop {
            if let Some(cmd) = self.commands.remove(txn) {
                for key in &cmd.keys {
                    if let Some(set) = self.by_key.get_mut(key) {
                        set.remove(txn);
                        if set.is_empty() {
                            self.by_key.remove(key);
                        }
                    }
                }
            }
        }
        self.redundant_before = safe;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{NodeId, Timestamp};

    fn txn(micros: u64) -> TxnId {
        TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(0),
        })
    }

    fn event(aggregate_id: &str, version: u16) -> Event {
        Event {
            aggregate_id: aggregate_id.into(),
            aggregate_type: "test/T".into(),
            version,
            ..Default::default()
        }
    }

    /// A decision (Apply) that arrives before a transaction's dependencies are
    /// satisfied must still be enacted once they clear — a lagging replica
    /// converges instead of silently dropping the transaction.
    #[test]
    fn applies_a_decided_transaction_once_dependencies_clear() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        let b = txn(20);

        replica.preaccept(a, vec![key.clone()], vec![event("k", 1)]);
        replica.preaccept(b, vec![key.clone()], vec![event("k", 2)]);

        // B commits and its decision arrives while A is still unapplied.
        replica.commit(b, b.0, vec![a], vec![event("k", 2)], NodeId(0));
        replica.record_decision(b, true);
        assert!(
            replica.next_apply().is_none(),
            "B must wait for its dependency A"
        );

        // A commits, decides, and applies.
        replica.commit(a, a.0, vec![], vec![event("k", 1)], NodeId(0));
        replica.record_decision(a, true);
        let ready_a = replica.next_apply().expect("A is ready");
        assert_eq!(ready_a.txn, a);
        replica.mark_applied(a, false);

        // Now B — already decided — becomes applicable without a fresh Apply.
        let ready_b = replica
            .next_apply()
            .expect("B becomes ready after A applies");
        assert_eq!(ready_b.txn, b);
        assert!(ready_b.commit);
    }

    /// Applies `txn` end to end (preaccept → commit → decide → apply) on `key`.
    fn apply(replica: &mut Replica, t: TxnId, key: &Key, version: u16) {
        replica.preaccept(t, vec![key.clone()], vec![event("k", version)]);
        replica.commit(t, t.0, vec![], vec![event("k", version)], NodeId(0));
        replica.record_decision(t, true);
        let ready = replica.next_apply().expect("ready to apply");
        assert_eq!(ready.txn, t);
        replica.mark_applied(t, false);
    }

    /// `compact` drops applied commands below the watermark, prunes the conflict
    /// index, and advances `redundant_before`.
    #[test]
    fn compact_drops_applied_commands_and_prunes_the_index() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        apply(&mut replica, a, &key, 1);
        assert_eq!(replica.command_count(), 1);

        replica.compact(txn(20).0);

        assert_eq!(
            replica.command_count(),
            0,
            "the applied command was dropped"
        );
        assert!(replica.snapshot(a).is_none());
        assert_eq!(replica.redundant_before(), txn(20).0);

        // The index was pruned: a later transaction on the same key sees no
        // conflict from the compacted-away one.
        let b = txn(30);
        let (_, deps) = replica.preaccept(b, vec![key.clone()], vec![event("k", 2)]);
        assert!(
            deps.is_empty(),
            "compacted command must not be a dependency"
        );
    }

    /// `compact` never advances past a still-unapplied transaction: the watermark
    /// is clamped to the oldest un-applied `t0`.
    #[test]
    fn compact_clamps_to_the_oldest_unapplied_transaction() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let old = txn(5);
        apply(&mut replica, old, &key, 1);
        // A second transaction stays un-applied, below the requested watermark.
        let pending = txn(10);
        replica.preaccept(pending, vec![key.clone()], vec![event("k", 2)]);

        replica.compact(txn(20).0);

        assert_eq!(
            replica.redundant_before(),
            txn(10).0,
            "watermark clamped to the oldest un-applied t0"
        );
        assert!(
            replica.snapshot(old).is_none(),
            "old applied command dropped"
        );
        assert!(
            replica.snapshot(pending).is_some(),
            "un-applied command retained"
        );
    }

    /// The redundancy-watermark fix for the dependency-union hazard: a committed
    /// transaction whose dependency was already compacted away still applies,
    /// because a missing dependency below `redundant_before` counts as satisfied.
    #[test]
    fn dependency_compacted_below_the_watermark_counts_as_satisfied() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let c = txn(10);
        let d = txn(20);
        replica.preaccept(c, vec![key.clone()], vec![event("k", 1)]);
        replica.preaccept(d, vec![key.clone()], vec![event("k", 2)]);

        // Apply and compact away C.
        replica.commit(c, c.0, vec![], vec![event("k", 1)], NodeId(0));
        replica.record_decision(c, true);
        assert_eq!(replica.next_apply().expect("C ready").txn, c);
        replica.mark_applied(c, false);
        replica.compact(txn(15).0);
        assert!(replica.snapshot(c).is_none());

        // D commits with C still in its deps (the quorum union); it must apply.
        replica.commit(d, d.0, vec![c], vec![event("k", 2)], NodeId(0));
        replica.record_decision(d, true);
        let ready = replica
            .next_apply()
            .expect("D applies despite its dependency C being compacted away");
        assert_eq!(ready.txn, d);
    }

    /// The watermark only ever moves forward; a lower or equal `compact` is a
    /// no-op.
    #[test]
    fn compact_is_monotonic() {
        let mut replica = Replica::new();
        replica.compact(txn(30).0);
        assert_eq!(replica.redundant_before(), txn(30).0);

        replica.compact(txn(20).0);
        assert_eq!(replica.redundant_before(), txn(30).0, "lower is a no-op");

        replica.compact(txn(30).0);
        assert_eq!(replica.redundant_before(), txn(30).0, "equal is a no-op");
    }
}
