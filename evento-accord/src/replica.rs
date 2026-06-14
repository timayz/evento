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
#[derive(Default)]
pub struct Replica {
    /// Every transaction this replica knows about.
    commands: HashMap<TxnId, CommandState>,
    /// Index from key to the transactions touching it, for conflict lookup.
    by_key: HashMap<Key, BTreeSet<TxnId>>,
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

    /// Indexes a transaction's keys and inserts its command state.
    fn insert(&mut self, cmd: CommandState) {
        for key in &cmd.keys {
            self.by_key.entry(key.clone()).or_default().insert(cmd.txn);
        }
        self.commands.insert(cmd.txn, cmd);
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
            _ => false,
        })
    }

    /// Picks the lowest-order committed transaction that is ready to read and has
    /// no decision yet, marking it [`Reading`](Status::Reading) so it keeps
    /// blocking dependents until applied. Decided transactions go through
    /// [`next_apply`](Replica::next_apply) instead.
    pub fn next_read(&mut self) -> Option<Ready> {
        let next = self
            .commands
            .values()
            .filter(|cmd| cmd.status == Status::Committed && cmd.decision.is_none())
            .filter(|cmd| self.is_ready(cmd.txn))
            .min_by_key(|cmd| (cmd.execute_at, cmd.txn))
            .map(|cmd| (cmd.txn, cmd.execute_at, cmd.events.clone(), cmd.reply_to));

        let (txn, execute_at, events, reply_to) = next?;
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.status = Status::Reading;
        }
        Some(Ready {
            txn,
            execute_at,
            events,
            reply_to,
        })
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
        let next = self
            .commands
            .values()
            .filter(|cmd| cmd.decision.is_some() && cmd.status != Status::Applied)
            .filter(|cmd| self.is_ready(cmd.txn))
            .min_by_key(|cmd| (cmd.execute_at, cmd.txn))
            .map(|cmd| {
                (
                    cmd.txn,
                    cmd.execute_at,
                    cmd.events.clone(),
                    cmd.decision.expect("decision present"),
                    cmd.reply_to,
                )
            });

        let (txn, execute_at, events, commit, reply_to) = next?;
        Some(ApplyReady {
            txn,
            execute_at,
            events,
            commit,
            reply_to,
        })
    }

    /// Records that a transaction's decision has been enacted, storing its
    /// conflict outcome and unblocking dependents.
    pub fn mark_applied(&mut self, txn: TxnId, conflict: bool) {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.status = Status::Applied;
            cmd.applied_conflict = Some(conflict);
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
        self.commands
            .values()
            .filter(|cmd| cmd.status != Status::Applied && cmd.txn.0 < cutoff)
            .map(|cmd| cmd.txn)
            .collect()
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
        if self.commands.contains_key(&cmd.txn) {
            return false;
        }
        cmd.status = Status::Applied;
        self.insert(cmd);
        true
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

    fn event(aggregator_id: &str, version: u16) -> Event {
        Event {
            aggregator_id: aggregator_id.into(),
            aggregator_type: "test/T".into(),
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
}
