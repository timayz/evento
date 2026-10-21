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

use std::collections::{BTreeSet, HashMap, HashSet};

use evento_core::Event;

use crate::api::floor_millis;
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
    /// Transactions handed out by [`next_apply`](Replica::next_apply) whose apply
    /// is in flight (the node performs it outside the lock). Excluded from
    /// further `next_apply` picks so two concurrent drivers cannot double-apply
    /// the same transaction.
    applying: HashSet<TxnId>,
    /// Every un-applied command's watermark bound: `t0` floored to a
    /// millisecond (see [`stable_event_micros`](Replica::stable_event_micros)),
    /// keyed by txn so it can be removed when the command's status changes.
    /// Kept in lockstep with `commands` by
    /// [`reindex_unapplied`](Replica::reindex_unapplied).
    unapplied_bounds: HashMap<TxnId, u64>,
    /// The bounds above, ordered — `stable_event_micros` reads the minimum in
    /// O(log n) instead of scanning every command (it runs on every remote
    /// response frame).
    unapplied_by_bound: BTreeSet<(u64, TxnId)>,
    /// Un-applied txns in `t0` order, serving `applied_through` and `stuck`
    /// without a full command scan.
    unapplied_by_t0: BTreeSet<TxnId>,
    /// Applied (un-compacted) txns in `t0` order, serving the anti-entropy
    /// digest and missing-command export without a full command scan.
    applied_by_t0: BTreeSet<TxnId>,
    /// The version-condition result of every [`Reading`](Status::Reading)
    /// command, so a Commit from a recovery coordinator that took over is
    /// answered with it: the read happens once, and its result stands while
    /// the command holds its slot. In memory only — a restart turns `Reading`
    /// back into `Committed` (see [`restore`](Replica::restore)), which reads
    /// again.
    read_results: HashMap<TxnId, bool>,
    /// Ballots promised to a recovery for transactions this replica never
    /// witnessed. A replica answering such a recovery "unknown" still fences
    /// the original coordinator: a PreAccept or Accept below the promise is
    /// refused here, as it would be for a known command — otherwise the
    /// recovery's quorum would not intersect the coordinator's, and both could
    /// decide. Never in the un-applied indexes (they would pin the watermark);
    /// dropped below the redundancy watermark. Journaled by the node
    /// (`Journal::save_promise`) and restored on restart
    /// ([`restore_promise`](Replica::restore_promise)).
    unknown_promises: HashMap<TxnId, Ballot>,
    /// The route — full key set — of every multi-shard transaction held
    /// here, as its coordinator sent it (`Message::Routed`): `keys` above is
    /// only this shard's part, and a recovery must reach every shard the
    /// transaction touches, including one that never witnessed it. Dropped
    /// with the command below the redundancy watermark.
    routes: HashMap<TxnId, Vec<Key>>,
}

impl Default for Replica {
    fn default() -> Self {
        Self {
            commands: HashMap::new(),
            by_key: HashMap::new(),
            pending: BTreeSet::new(),
            redundant_before: Timestamp::MIN,
            applying: HashSet::new(),
            unapplied_bounds: HashMap::new(),
            unapplied_by_bound: BTreeSet::new(),
            unapplied_by_t0: BTreeSet::new(),
            applied_by_t0: BTreeSet::new(),
            read_results: HashMap::new(),
            unknown_promises: HashMap::new(),
            routes: HashMap::new(),
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

/// What a replica answers a routed recovery (`Message::Routed` around a
/// `BeginRecovery`) with: a report on a transaction it holds, or — never
/// pre-accepting one it does not — that it never witnessed it.
pub enum RoutedRecovery {
    /// The replica holds the transaction.
    Known(RecoverState),
    /// The replica holds nothing of the transaction. Unless `redundant`, it
    /// has promised the ballot (the node journals it) and so will never
    /// witness the transaction below it. `redundant`: the transaction is below
    /// the redundancy watermark — applied and compacted away, or never seen;
    /// no evidence either way, and no promise is kept.
    NotWitnessed {
        /// Whether `t0` is below this replica's redundancy watermark.
        redundant: bool,
    },
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
    /// A conflicting transaction ordered after this one's `t0` did not witness
    /// it — proof the transaction could not have committed on the fast path.
    pub superseding_rejects: bool,
    /// An earlier conflict was *accepted* (not yet committed) to execute after
    /// this transaction's `t0` without witnessing it here: its committed
    /// dependencies may still name this transaction (another replica's Accept
    /// reply), so the fast path can be neither ruled in nor out until it
    /// commits. Recovery waits.
    pub wait: bool,
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

    /// Adds `keys` to a known command and indexes them in the conflict graph.
    /// Repairs commands first learned via Accept/Commit (which used to leave
    /// `keys` empty, hiding the transaction from later conflict lookups).
    fn index_keys(&mut self, txn: TxnId, keys: &[Key]) {
        // An applied command is final: nothing left to repair (and an
        // invalidated one must not grow keys it was never committed with).
        let Some(cmd) = self
            .commands
            .get_mut(&txn)
            .filter(|cmd| cmd.status != Status::Applied)
        else {
            return;
        };
        for key in keys {
            if !cmd.keys.contains(key) {
                cmd.keys.push(key.clone());
            }
            self.by_key.entry(key.clone()).or_default().insert(txn);
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
        let txn = cmd.txn;
        self.commands.insert(txn, cmd);
        self.reindex_unapplied(txn);
    }

    /// Re-derives `txn`'s entry in the un-applied indexes from its current
    /// command state. Must be called whenever a command's applied status or
    /// event set changes — including the late event-repair paths in
    /// [`preaccept`](Self::preaccept) and [`commit`](Self::commit), where
    /// events attach after first insertion.
    fn reindex_unapplied(&mut self, txn: TxnId) {
        if let Some(old) = self.unapplied_bounds.remove(&txn) {
            self.unapplied_by_bound.remove(&(old, txn));
            self.unapplied_by_t0.remove(&txn);
        }
        let bound = match self.commands.get(&txn) {
            Some(cmd) if cmd.status != Status::Applied => floor_millis(cmd.txn.0.micros),
            Some(_) => {
                self.applied_by_t0.insert(txn);
                return;
            }
            None => {
                self.applied_by_t0.remove(&txn);
                return;
            }
        };
        self.applied_by_t0.remove(&txn);
        self.unapplied_bounds.insert(txn, bound);
        self.unapplied_by_bound.insert((bound, txn));
        self.unapplied_by_t0.insert(txn);
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

    /// An upper bound (µs) on what a subscription may safely process: `cutoff`,
    /// clamped by every un-applied command's `t0` **floored to a millisecond**.
    /// A command's events are stamped at apply from its `execute_at` (never
    /// below `t0`), floored to the millisecond the backends store — so an
    /// event of a transaction at `t0 = 1.5 ms` lands at `1.0 ms`, below an
    /// unfloored `t0` bound, and a subscription that advanced to `t0` would
    /// skip it. The provisional stamps the writer put on the events play no
    /// part: none survives the apply. (`cutoff` itself is floored by the
    /// caller, [`Node::stable_micros`](crate::Node::stable_micros).)
    pub fn stable_event_micros(&self, cutoff: Timestamp) -> u64 {
        // Served from the incrementally-maintained index: O(1) instead of a
        // scan of every command — this runs on every remote response frame,
        // while holding the replica lock the inbox loop contends on.
        let bound = match self.unapplied_by_bound.first() {
            Some(&(bound, _)) => cutoff.micros.min(bound),
            None => cutoff.micros,
        };
        #[cfg(debug_assertions)]
        {
            let mut scan = cutoff.micros;
            for cmd in self.commands.values() {
                if cmd.status == Status::Applied {
                    continue;
                }
                scan = scan.min(floor_millis(cmd.txn.0.micros));
            }
            debug_assert_eq!(bound, scan, "unapplied bound index out of sync");
        }
        bound
    }

    /// Handles PreAccept: records the transaction and returns the execution
    /// timestamp and dependencies this replica witnesses. Idempotent.
    ///
    /// A PreAccept is the original coordinator's, at ballot `Ballot(t0)`: once
    /// a recovery has promised a higher ballot here — for a known command, or
    /// for one this replica answered "unknown" — it is refused with
    /// `Err(promised)`, so that coordinator can no longer gather a vote the
    /// recovery did not see.
    pub fn preaccept(
        &mut self,
        txn: TxnId,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> Result<(Timestamp, Vec<TxnId>), Ballot> {
        if let Some(promised) = self.promise_of(txn).filter(|p| *p > Ballot(txn.0)) {
            return Err(promised);
        }
        Ok(self.witness(txn, Ballot(txn.0), keys, events))
    }

    /// The ballot this replica has promised for `txn`, known or not.
    fn promise_of(&self, txn: TxnId) -> Option<Ballot> {
        match self.commands.get(&txn) {
            Some(cmd) => Some(cmd.promised),
            None => self.unknown_promises.get(&txn).copied(),
        }
    }

    /// Records `txn` as pre-accepted under `promised` (or repairs the keys and
    /// events of a command already known) and returns its vote: the execution
    /// timestamp and dependencies this replica witnesses.
    fn witness(
        &mut self,
        txn: TxnId,
        promised: Ballot,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> (Timestamp, Vec<TxnId>) {
        if let Some(existing) = self.commands.get(&txn) {
            if existing.status == Status::Applied {
                // Final (possibly invalidated): nothing to repair.
                return (existing.execute_at, existing.deps.clone());
            }
            // Late PreAccept after an Accept/Commit created the entry: repair a
            // keyless/eventless command so the conflict index and recovery's
            // superseding check see it.
            self.index_keys(txn, &keys);
            let existing = self.commands.get_mut(&txn).expect("present");
            let mut events_attached = false;
            if existing.events.is_empty() && !events.is_empty() {
                existing.events = events;
                events_attached = true;
            }
            let result = (existing.execute_at, existing.deps.clone());
            if events_attached {
                // Late-attached events can lower the watermark bound.
                self.reindex_unapplied(txn);
            }
            return result;
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

        self.unknown_promises.remove(&txn);
        self.insert(CommandState {
            txn,
            status: Status::PreAccepted,
            promised,
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
    /// timestamp, and returns the dependencies this replica witnesses **at
    /// that timestamp** — the coordinator's set plus every conflict here with
    /// a lower `t0` — or `Err(promised)` if it has already promised a higher
    /// ballot.
    ///
    /// The slow path raises `execute_at` above `t0`, so a conflict that arrived
    /// after this transaction's PreAccept, with a `t0` in between, now orders
    /// before it. It must become a dependency: it may already have committed
    /// at its own `t0` on the fast path, and nothing else would make this
    /// transaction wait for it. The coordinator unions these replies into the
    /// committed dependency set.
    pub fn accept(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: Vec<TxnId>,
        keys: Vec<Key>,
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
                let deps = cmd.deps.clone();
                self.index_keys(txn, &keys);
                return Ok(deps);
            }
            cmd.promised = ballot;
            cmd.accepted = ballot;
            cmd.status = Status::Accepted;
            cmd.execute_at = execute_at;
            self.index_keys(txn, &keys);
        } else {
            if let Some(&promised) = self.unknown_promises.get(&txn) {
                if ballot < promised {
                    return Err(promised);
                }
            }
            self.unknown_promises.remove(&txn);
            // The replica missed PreAccept; record what it was told — including
            // the keys, so the conflict index and recovery's superseding check
            // still see this transaction (events arrive with Commit).
            self.insert(CommandState {
                txn,
                status: Status::Accepted,
                promised: ballot,
                accepted: ballot,
                execute_at,
                deps: Vec::new(),
                keys,
                events: Vec::new(),
                reply_to: txn.0.node,
                decision: None,
                applied_conflict: None,
            });
        }
        let deps = self.deps_at(txn, execute_at, deps);
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.deps = deps.clone();
        }
        Ok(deps)
    }

    /// `known` plus every transaction conflicting with `txn` here whose `t0`
    /// is below `execute_at` — the dependencies `txn` has at that timestamp.
    fn deps_at(&self, txn: TxnId, execute_at: Timestamp, known: Vec<TxnId>) -> Vec<TxnId> {
        let mut deps: BTreeSet<TxnId> = known.into_iter().collect();
        if let Some(cmd) = self.commands.get(&txn) {
            deps.extend(
                self.conflicts(&cmd.keys, txn)
                    .into_iter()
                    .filter(|c| c.0 < execute_at),
            );
        }
        deps.into_iter().collect()
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
        keys: Vec<Key>,
        reply_to: NodeId,
    ) {
        // A Commit is the decision itself: no ballot fences it.
        self.unknown_promises.remove(&txn);
        if self.commands.contains_key(&txn) {
            // Repair a keyless command first learned via Accept, so the
            // conflict index sees it permanently.
            self.index_keys(txn, &keys);
            let cmd = self.commands.get_mut(&txn).expect("present");
            cmd.reply_to = reply_to;
            if cmd.status >= Status::Committed {
                return;
            }
            cmd.status = Status::Committed;
            cmd.execute_at = execute_at;
            cmd.deps = deps;
            let events_attached = cmd.events.is_empty() && !events.is_empty();
            if events_attached {
                cmd.events = events;
            }
            // Newly executable: enter the execution queue at its final order.
            self.pending.insert((execute_at, txn));
            if events_attached {
                // Late-attached events can lower the watermark bound.
                self.reindex_unapplied(txn);
            }
        } else {
            self.insert(CommandState {
                txn,
                status: Status::Committed,
                promised: Ballot(txn.0),
                accepted: Ballot(txn.0),
                execute_at,
                deps,
                keys,
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
    ///
    /// `keys`/`events` are the owned part of what the recovery coordinator
    /// holds for the transaction (empty for the legacy `Recover`). A replica
    /// that never witnessed it pre-accepts that part now, under the ballot —
    /// its vote is then one the recovery counts, and one the original
    /// coordinator, whose PreAccept may still be in flight, can no longer
    /// get. Owning none of it, the replica only remembers the promise and
    /// answers "unknown".
    pub fn recover(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> Result<RecoverState, Ballot> {
        if let Some(promised) = self.promise_of(txn) {
            if ballot < promised {
                return Err(promised);
            }
        }
        if !self.commands.contains_key(&txn) && keys.is_empty() {
            self.unknown_promises.insert(txn, ballot);
            return Ok(RecoverState {
                known: false,
                status: Status::PreAccepted,
                accepted: Ballot(txn.0),
                execute_at: txn.0,
                deps: Vec::new(),
                superseding_rejects: false,
                wait: false,
                keys: Vec::new(),
                events: Vec::new(),
            });
        }

        // Pre-accepts an unknown transaction (or repairs a known one's keys
        // and events), then promises the ballot.
        self.witness(txn, ballot, keys, events);
        if let Some(cmd) = self.commands.get_mut(&txn) {
            cmd.promised = ballot;
        }

        let (superseding_rejects, wait) = self.fast_path_evidence(txn);
        let cmd = &self.commands[&txn];
        Ok(RecoverState {
            known: true,
            status: cmd.status,
            accepted: cmd.accepted,
            execute_at: cmd.execute_at,
            deps: cmd.deps.clone(),
            superseding_rejects,
            wait,
            keys: cmd.keys.clone(),
            events: cmd.events.clone(),
        })
    }

    /// Handles a **routed** recovery under `ballot`: like
    /// [`recover`](Self::recover) for a transaction held here, but a replica
    /// that never witnessed it does not pre-accept it — the route tells the
    /// recovery coordinator which shards to hear from, and a slow quorum of
    /// one shard answering "never witnessed" (each having durably promised
    /// the ballot, so the original coordinator can no longer win a vote
    /// there) proves the transaction never committed. Pre-accepting instead
    /// would let a later recovery that learned the missing shard's part from
    /// a straggler turn that proof into a commit after it was acted on.
    /// `Err(promised)` if a higher ballot was already promised.
    pub fn recover_routed(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        keys: Vec<Key>,
        events: Vec<Event>,
    ) -> Result<RoutedRecovery, Ballot> {
        if self.commands.contains_key(&txn) {
            return self
                .recover(txn, ballot, keys, events)
                .map(RoutedRecovery::Known);
        }
        if let Some(&promised) = self.unknown_promises.get(&txn) {
            if ballot < promised {
                return Err(promised);
            }
        }
        // Below the watermark an absent command may have been applied and
        // compacted: "not held" is no proof of "never witnessed".
        if txn.0 < self.redundant_before {
            return Ok(RoutedRecovery::NotWitnessed { redundant: true });
        }
        self.unknown_promises.insert(txn, ballot);
        Ok(RoutedRecovery::NotWitnessed { redundant: false })
    }

    /// Records `txn` as applied **as an abort** without executing it: a
    /// recovery proved it never committed and never will (one shard never
    /// witnessed it), and an abort changes nothing, so it needs no place in
    /// the execution order. Marked by the
    /// [`INVALIDATED_AT`](crate::message::INVALIDATED_AT) execution timestamp,
    /// so a later recovery recognizes the decision, and recovery's
    /// superseding check never reads its (un-agreed, local) dependencies as
    /// evidence. Only a command not yet committed is invalidated — a committed
    /// one has a decision of its own (none can exist for a transaction a shard
    /// never witnessed). Returns whether `txn` was invalidated now.
    pub fn invalidate(&mut self, txn: TxnId) -> bool {
        let Some(cmd) = self.commands.get_mut(&txn) else {
            return false;
        };
        if cmd.status >= Status::Committed {
            return false;
        }
        cmd.status = Status::Applied;
        cmd.execute_at = crate::message::INVALIDATED_AT;
        cmd.decision = Some(false);
        cmd.applied_conflict = Some(true);
        cmd.events = Vec::new();
        self.read_results.remove(&txn);
        self.reindex_unapplied(txn);
        true
    }

    /// Keeps `route` as `txn`'s route if this replica holds the transaction
    /// and has none yet. Returns whether it was newly kept (the node then
    /// journals it).
    pub fn set_route(&mut self, txn: TxnId, route: &[Key]) -> bool {
        if !self.commands.contains_key(&txn) || self.routes.contains_key(&txn) {
            return false;
        }
        self.routes.insert(txn, route.to_vec());
        true
    }

    /// `txn`'s route, if it is a multi-shard transaction held here.
    pub fn route(&self, txn: TxnId) -> Option<Vec<Key>> {
        self.routes.get(&txn).cloned()
    }

    /// Restores a journaled route on restart.
    pub fn restore_route(&mut self, txn: TxnId, route: Vec<Key>) {
        if txn.0 >= self.redundant_before {
            self.routes.insert(txn, route);
        }
    }

    /// The ballot promised for `txn` while never witnessing it, if any — what
    /// the node journals.
    pub fn unknown_promise(&self, txn: TxnId) -> Option<Ballot> {
        self.unknown_promises.get(&txn).copied()
    }

    /// Restores a journaled blind promise on restart. Ignored for a
    /// transaction held here (its command carries its own promise) or below
    /// the redundancy watermark.
    pub fn restore_promise(&mut self, txn: TxnId, ballot: Ballot) {
        if self.commands.contains_key(&txn) || txn.0 < self.redundant_before {
            return;
        }
        let promised = self.unknown_promises.entry(txn).or_insert(ballot);
        *promised = (*promised).max(ballot);
    }

    /// The node coordinating `txn`'s decision (set by Commit), to report results
    /// to when applying.
    pub fn reply_to(&self, txn: TxnId) -> Option<NodeId> {
        self.commands.get(&txn).map(|cmd| cmd.reply_to)
    }

    /// What this replica's conflicts reveal about whether `txn` could have
    /// committed on the fast path at its `t0`: `(rejects, wait)`.
    ///
    /// - **Rejects** — a conflict that orders after `t0` reached a decision
    ///   without `txn` among its dependencies:
    ///   - a *later* one (higher `t0`) accepted or committed: its PreAccept
    ///     quorum meets any fast quorum, where it would have witnessed `txn`;
    ///   - an *earlier* one committed to execute after `t0`: its Accept quorum
    ///     meets any fast quorum, whose replies would have named `txn` (see
    ///     [`accept`](Replica::accept)).
    /// - **Wait** — an earlier conflict only *accepted* to execute after `t0`
    ///   without `txn` here: another replica's Accept reply may still put
    ///   `txn` in its committed dependencies, so nothing follows until it
    ///   commits.
    fn fast_path_evidence(&self, txn: TxnId) -> (bool, bool) {
        let cmd = &self.commands[&txn];
        let mut wait = false;
        for g in self.conflicts(&cmd.keys, txn) {
            let gc = &self.commands[&g];
            // An invalidated command's deps are one replica's PreAccept view,
            // never agreed by a quorum: they prove nothing.
            if gc.status < Status::Accepted || gc.deps.contains(&txn) || gc.invalidated() {
                continue;
            }
            if g.0 > txn.0 {
                return (true, false);
            }
            if gc.execute_at > txn.0 {
                if gc.status >= Status::Committed {
                    return (true, false);
                }
                wait = true;
            }
        }
        (false, wait)
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

    /// Records the version-condition result of a read handed out by
    /// [`next_read`](Replica::next_read). Ignored unless the command is still
    /// [`Reading`](Status::Reading) (an anti-entropy import may have applied it
    /// meanwhile).
    pub fn record_read(&mut self, txn: TxnId, ok: bool) {
        if matches!(self.commands.get(&txn), Some(cmd) if cmd.status == Status::Reading) {
            self.read_results.insert(txn, ok);
        }
    }

    /// The recorded read result of a command that is still
    /// [`Reading`](Status::Reading): what this replica answers whoever drives
    /// the commit, even one that took the transaction over after the read.
    pub fn read_result(&self, txn: TxnId) -> Option<bool> {
        match self.commands.get(&txn) {
            Some(cmd) if cmd.status == Status::Reading => self.read_results.get(&txn).copied(),
            _ => None,
        }
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
        // A transaction already handed out (its apply is in flight outside the
        // lock) is skipped, so two concurrent drivers cannot double-apply it.
        let txn = self.pending.iter().map(|&(_, txn)| txn).find(|&txn| {
            !self.applying.contains(&txn)
                && self.commands[&txn].decision.is_some()
                && self.is_ready(txn)
        })?;

        self.applying.insert(txn);
        let cmd = &self.commands[&txn];
        Some(ApplyReady {
            txn,
            execute_at: cmd.execute_at,
            events: cmd.events.clone(),
            commit: cmd.decision.expect("decision present"),
            reply_to: cmd.reply_to,
        })
    }

    /// Hands back an apply reserved by [`next_apply`](Replica::next_apply) that
    /// the data store failed: the command stays decided and un-applied, to be
    /// handed out again on the next drive.
    pub fn release_apply(&mut self, txn: TxnId) {
        self.applying.remove(&txn);
    }

    /// Hands back a read taken by [`next_read`](Replica::next_read) that the
    /// data store failed: the command returns to `Committed`, to be read again
    /// on the next drive, rather than holding its slot with nothing to answer.
    pub fn unread(&mut self, txn: TxnId) {
        if let Some(cmd) = self.commands.get_mut(&txn) {
            if cmd.status == Status::Reading {
                cmd.status = Status::Committed;
            }
        }
        self.read_results.remove(&txn);
    }

    /// Records that a transaction's decision has been enacted, storing its
    /// conflict outcome and unblocking dependents.
    pub fn mark_applied(&mut self, txn: TxnId, conflict: bool) {
        self.applying.remove(&txn);
        self.read_results.remove(&txn);
        if let Some(cmd) = self.commands.get_mut(&txn) {
            let at = cmd.execute_at;
            cmd.status = Status::Applied;
            cmd.applied_conflict = Some(conflict);
            self.pending.remove(&(at, txn));
            self.reindex_unapplied(txn);
        }
    }

    /// How far `txn` has progressed here, if this replica holds it.
    pub fn status(&self, txn: TxnId) -> Option<Status> {
        self.commands.get(&txn).map(|cmd| cmd.status)
    }

    /// A snapshot of a command's durable state, for the [`Journal`](crate::api::Journal).
    pub fn snapshot(&self, txn: TxnId) -> Option<CommandState> {
        self.commands.get(&txn).cloned()
    }

    /// Restores a command from the journal on restart, preserving its actual
    /// status, ballots, and decision (unlike
    /// [`import_applied`](Self::import_applied), which is for bootstrap and
    /// forces `Applied`). A command journaled mid-read comes back `Committed`:
    /// its read result was never persisted, and [`next_read`](Self::next_read)
    /// only hands out `Committed` commands, so a restored `Reading` one would
    /// never answer a read again.
    pub fn restore(&mut self, mut cmd: CommandState) {
        if cmd.status == Status::Reading {
            cmd.status = Status::Committed;
        }
        self.insert(cmd);
    }

    /// Transactions this replica knows but has not applied, whose `t0` is older
    /// than `cutoff` — i.e. stalled long enough to warrant recovery (the
    /// coordinator is presumed dead). Drives automatic progress.
    pub fn stuck(&self, cutoff: Timestamp) -> Vec<TxnId> {
        // The `t0`-ordered un-applied index makes this O(result), already in
        // the deterministic order the recovery sweep needs.
        self.unapplied_by_t0
            .range(..TxnId(cutoff))
            .copied()
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

    /// The anti-entropy digest of this replica's applied state: its redundancy
    /// floor plus the ids of every applied (un-compacted) command. A peer ships
    /// back only the applied commands the digest is missing, instead of its
    /// whole applied set (payloads included) every round.
    pub fn applied_digest(&self) -> (Timestamp, Vec<TxnId>) {
        (
            self.redundant_before,
            self.applied_by_t0.iter().copied().collect(),
        )
    }

    /// Applied commands with `t0 >= since` that are not in `known`, in
    /// execution-timestamp order — what an anti-entropy requester holding
    /// `known` above its floor `since` is missing. O(window), and empty (no
    /// clones at all) when the peers are in sync.
    pub fn export_applied_missing(
        &self,
        since: Timestamp,
        known: &HashSet<TxnId>,
    ) -> Vec<CommandState> {
        let mut missing: Vec<CommandState> = self
            .applied_by_t0
            .range(TxnId(since)..)
            .filter(|txn| !known.contains(txn))
            .map(|txn| self.commands[txn].clone())
            .collect();
        missing.sort_by_key(|cmd| (cmd.execute_at, cmd.txn));
        missing
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
        self.read_results.remove(&cmd.txn);
        self.unknown_promises.remove(&cmd.txn);
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

    /// Whether any known transaction is still un-applied here — in flight, or
    /// stalled and awaiting recovery. The liveness signal a shared sweep
    /// scheduler keys off: a replica with un-applied work must be swept at full
    /// cadence so the recovery bound holds.
    pub fn has_unapplied(&self) -> bool {
        !self.unapplied_by_t0.is_empty()
    }

    /// The largest watermark `<= cutoff` that is *locally* safe: no transaction
    /// this replica holds below the result is still un-applied. (Below it, every
    /// command here is `Applied`, so its effect is durable in the data store.)
    /// Reported to peers; the cluster-safe watermark is the min across the shard.
    pub fn applied_through(&self, cutoff: Timestamp) -> Timestamp {
        match self.unapplied_by_t0.first() {
            Some(oldest_unapplied) => cutoff.min(oldest_unapplied.0),
            None => cutoff,
        }
    }

    /// Drops every `Applied` command with `t0 < before` from `commands` and prunes
    /// `by_key`, advancing the redundancy watermark. `before` is first clamped to
    /// the locally-safe point ([`applied_through`](Replica::applied_through)) so a
    /// not-yet-applied transaction is never declared redundant; the *caller* is
    /// responsible for the cross-replica guarantee (that every replica has applied
    /// everything below `before`). Monotonic and idempotent. Returns the newest
    /// `t0` dropped, if any command was: what a peer that never applied it can no
    /// longer get through anti-entropy.
    pub fn compact(&mut self, before: Timestamp) -> Option<Timestamp> {
        let safe = self.applied_through(before);
        if safe <= self.redundant_before {
            return None;
        }
        let drop: Vec<TxnId> = self
            .commands
            .values()
            .filter(|cmd| cmd.status == Status::Applied && cmd.txn.0 < safe)
            .map(|cmd| cmd.txn)
            .collect();
        let newest = drop.iter().map(|txn| txn.0).max();
        for txn in &drop {
            self.read_results.remove(txn);
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
            self.reindex_unapplied(*txn);
        }
        self.unknown_promises.retain(|txn, _| txn.0 >= safe);
        self.routes.retain(|txn, _| txn.0 >= safe);
        self.redundant_before = safe;
        newest
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

        replica
            .preaccept(a, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica
            .preaccept(b, vec![key.clone()], vec![event("k", 2)])
            .unwrap();

        // B commits and its decision arrives while A is still unapplied.
        replica.commit(b, b.0, vec![a], vec![event("k", 2)], vec![], NodeId(0));
        replica.record_decision(b, true);
        assert!(
            replica.next_apply().is_none(),
            "B must wait for its dependency A"
        );

        // A commits, decides, and applies.
        replica.commit(a, a.0, vec![], vec![event("k", 1)], vec![], NodeId(0));
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

    /// An un-applied command bounds the watermark by its `t0` floored to the
    /// millisecond its events will be stamped at — not by `t0` itself (the
    /// stamp of `t0 = 1.5 ms` is `1.0 ms`), and not by the provisional stamps
    /// the writer put on them (none survives the apply). Applying it lifts the
    /// bound.
    #[test]
    fn an_unapplied_command_bounds_the_watermark_at_its_floored_t0() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let t = txn(1_500);
        let cutoff = txn(10_000).0;
        let provisional = Event {
            timestamp: 0,
            timestamp_subsec: 0,
            ..event("k", 1)
        };

        replica
            .preaccept(t, vec![key.clone()], vec![provisional.clone()])
            .unwrap();
        assert_eq!(replica.stable_event_micros(cutoff), 1_000);

        replica.commit(t, t.0, vec![], vec![provisional], vec![], NodeId(0));
        replica.record_decision(t, true);
        let ready = replica.next_apply().expect("ready to apply");
        replica.mark_applied(ready.txn, false);
        assert_eq!(replica.stable_event_micros(cutoff), cutoff.micros);
    }

    /// Applies `txn` end to end (preaccept → commit → decide → apply) on `key`.
    fn apply(replica: &mut Replica, t: TxnId, key: &Key, version: u16) {
        replica
            .preaccept(t, vec![key.clone()], vec![event("k", version)])
            .unwrap();
        replica.commit(t, t.0, vec![], vec![event("k", version)], vec![], NodeId(0));
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
        let (_, deps) = replica
            .preaccept(b, vec![key.clone()], vec![event("k", 2)])
            .unwrap();
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
        replica
            .preaccept(pending, vec![key.clone()], vec![event("k", 2)])
            .unwrap();

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
        replica
            .preaccept(c, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica
            .preaccept(d, vec![key.clone()], vec![event("k", 2)])
            .unwrap();

        // Apply and compact away C.
        replica.commit(c, c.0, vec![], vec![event("k", 1)], vec![], NodeId(0));
        replica.record_decision(c, true);
        assert_eq!(replica.next_apply().expect("C ready").txn, c);
        replica.mark_applied(c, false);
        replica.compact(txn(15).0);
        assert!(replica.snapshot(c).is_none());

        // D commits with C still in its deps (the quorum union); it must apply.
        replica.commit(d, d.0, vec![c], vec![event("k", 2)], vec![], NodeId(0));
        replica.record_decision(d, true);
        let ready = replica
            .next_apply()
            .expect("D applies despite its dependency C being compacted away");
        assert_eq!(ready.txn, d);
    }

    /// A replica that first learns a transaction via Accept (it missed
    /// PreAccept) must still index its keys: a later conflicting transaction
    /// has to witness it as a dependency, or replicas order the two writes
    /// differently.
    #[test]
    fn accept_without_preaccept_still_enters_the_conflict_index() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        let b = txn(20);

        // A arrives via Accept only — with its keys.
        replica
            .accept(a, Ballot(a.0), a.0, vec![], vec![key.clone()])
            .expect("accepted");

        // A later conflicting transaction must witness A as a dependency.
        let (_, deps) = replica
            .preaccept(b, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        assert_eq!(
            deps,
            vec![a],
            "the Accept-only transaction must be a dependency"
        );
    }

    /// Same repair via Commit: a keyless entry (created by a keyless Accept
    /// from an older peer, or any path that lost the keys) is re-indexed when
    /// Commit supplies them.
    #[test]
    fn commit_repairs_a_keyless_command() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        let b = txn(20);

        replica
            .accept(a, Ballot(a.0), a.0, vec![], vec![])
            .expect("accepted");
        replica.commit(
            a,
            a.0,
            vec![],
            vec![event("k", 1)],
            vec![key.clone()],
            NodeId(0),
        );

        let (_, deps) = replica
            .preaccept(b, vec![key.clone()], vec![event("k", 2)])
            .unwrap();
        assert_eq!(deps, vec![a], "Commit must index the keys it carries");
    }

    /// Two concurrent apply drivers must not hand out the same transaction —
    /// `next_apply` reserves it until `mark_applied`.
    #[test]
    fn next_apply_reserves_the_transaction() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        replica
            .preaccept(a, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica.commit(a, a.0, vec![], vec![event("k", 1)], vec![], NodeId(0));
        replica.record_decision(a, true);

        assert_eq!(replica.next_apply().expect("ready").txn, a);
        assert!(
            replica.next_apply().is_none(),
            "a second driver must not receive the same in-flight apply"
        );
        replica.mark_applied(a, false);
    }

    /// The slow-path dependency hazard: X (t0 = 10) is raised to execute at 31
    /// after T (t0 = 20) was witnessed here at its own t0 — T may have
    /// fast-committed there. X's Accept must name T, or X never waits for T and
    /// both read the same state.
    #[test]
    fn accept_reports_conflicts_below_the_raised_timestamp() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let x = txn(10);
        let t = txn(20);
        replica
            .preaccept(x, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        let (t_at, t_deps) = replica
            .preaccept(t, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        assert_eq!((t_at, t_deps), (t.0, vec![x]), "T stands at its own t0");

        let raised = txn(31).0;
        let deps = replica
            .accept(x, Ballot(x.0), raised, vec![], vec![key.clone()])
            .expect("accepted");
        assert_eq!(
            deps,
            vec![t],
            "X now orders after T, so T is its dependency"
        );

        // Committed as agreed, T (at 20) executes first and X waits for it.
        replica.commit(t, t.0, vec![x], vec![event("k", 1)], vec![], NodeId(0));
        replica.commit(x, raised, deps, vec![event("k", 1)], vec![], NodeId(0));
        assert_eq!(replica.next_read().expect("T is ready").txn, t);
        assert!(replica.next_read().is_none(), "X waits for T to apply");
        replica.record_decision(t, true);
        assert_eq!(replica.next_apply().expect("T decided").txn, t);
        replica.mark_applied(t, false);
        assert_eq!(replica.next_read().expect("X is ready now").txn, x);
    }

    /// The same for a replica that missed X's PreAccept and learns it by Accept.
    #[test]
    fn accept_without_preaccept_reports_conflicts_below_its_timestamp() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let x = txn(10);
        let t = txn(20);
        let later = txn(40);
        replica
            .preaccept(t, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica
            .preaccept(later, vec![key.clone()], vec![event("k", 1)])
            .unwrap();

        let deps = replica
            .accept(x, Ballot(x.0), txn(31).0, vec![], vec![key.clone()])
            .expect("accepted");
        assert_eq!(
            deps,
            vec![t],
            "T orders before X's 31; the t0 = 40 one after"
        );
    }

    fn ballot(micros: u64, node: u64) -> Ballot {
        Ballot(Timestamp {
            micros,
            logical: 0,
            node: NodeId(node),
        })
    }

    /// A replica that answered a recovery blind (it never witnessed the
    /// transaction) is fenced like any other: the original coordinator's
    /// PreAccept and Accept, still in flight, are refused below the promise.
    #[test]
    fn a_blind_recovery_promise_fences_the_original_coordinator() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let t = txn(20);
        let b = ballot(50, 1);

        let report = replica.recover(t, b, vec![], vec![]).expect("promised");
        assert!(!report.known);
        assert_eq!(
            replica.preaccept(t, vec![key.clone()], vec![event("k", 1)]),
            Err(b),
            "the original PreAccept must not get a vote the recovery did not see"
        );
        assert_eq!(
            replica.accept(t, Ballot(t.0), t.0, vec![], vec![key.clone()]),
            Err(b),
            "nor can its Accept win here"
        );
        assert_eq!(
            replica.recover(t, ballot(40, 2), vec![], vec![]).err(),
            Some(b),
            "a lower recovery is refused too"
        );
        // The recovery's own Accept, at its ballot, goes through.
        replica
            .accept(t, b, t.0, vec![], vec![key])
            .expect("the recovery's own Accept");
        assert_eq!(replica.status(t), Some(Status::Accepted));
    }

    /// Handed the transaction's keys and events, a replica that never witnessed
    /// it pre-accepts it under the recovery's ballot: its vote counts, and the
    /// original coordinator's late PreAccept is refused.
    #[test]
    fn a_recovery_preaccepts_what_it_hands_an_unknown_replica() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let t = txn(20);
        let b = ballot(50, 1);

        let report = replica
            .recover(t, b, vec![key.clone()], vec![event("k", 1)])
            .expect("promised");
        assert!(report.known);
        assert_eq!(report.status, Status::PreAccepted);
        assert_eq!(report.execute_at, t.0, "no conflict here: the vote is t0");
        assert_eq!(replica.preaccept(t, vec![key], vec![event("k", 1)]), Err(b));
    }

    /// An earlier conflict committed to execute after T's t0 without T in its
    /// dependencies proves T never took the fast path: that conflict's Accept
    /// quorum meets any fast quorum, whose replies would have named T.
    #[test]
    fn an_earlier_conflict_committed_past_t0_without_the_txn_rejects_the_fast_path() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let x = txn(10);
        let t = txn(20);
        replica
            .preaccept(x, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica.commit(x, txn(31).0, vec![], vec![event("k", 1)], vec![], NodeId(0));
        replica
            .preaccept(t, vec![key.clone()], vec![event("k", 2)])
            .unwrap();

        let report = replica.recover(t, ballot(50, 1), vec![], vec![]).unwrap();
        assert!(report.superseding_rejects);
        assert!(!report.wait);
    }

    /// The same conflict only *accepted* proves nothing yet — another replica's
    /// Accept reply may still put T in its committed dependencies — so recovery
    /// must wait for it to commit.
    #[test]
    fn an_earlier_conflict_only_accepted_past_t0_makes_recovery_wait() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let x = txn(10);
        let t = txn(20);
        replica
            .preaccept(x, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica
            .accept(x, Ballot(x.0), txn(31).0, vec![], vec![key.clone()])
            .unwrap();
        replica
            .preaccept(t, vec![key.clone()], vec![event("k", 2)])
            .unwrap();

        let report = replica.recover(t, ballot(50, 1), vec![], vec![]).unwrap();
        assert!(!report.superseding_rejects);
        assert!(report.wait);
    }

    /// Promises for transactions this replica never witnessed stay out of the
    /// un-applied state (they never pin the watermark) and are dropped once
    /// the watermark passes them.
    #[test]
    fn blind_promises_are_dropped_below_the_watermark() {
        let mut replica = Replica::new();
        let t = txn(20);
        replica.recover(t, ballot(50, 1), vec![], vec![]).unwrap();
        assert!(!replica.has_unapplied());
        assert_eq!(replica.applied_through(txn(100).0), txn(100).0);

        replica.compact(txn(30).0);
        assert!(replica.unknown_promises.is_empty());
    }

    /// A routed recovery never pre-accepts what this replica never witnessed:
    /// it promises the ballot (refusing the original coordinator below it) and
    /// says so — unless the transaction is below the watermark, where "not
    /// held" proves nothing and nothing is promised.
    #[test]
    fn a_routed_recovery_reports_never_witnessed_without_preaccepting() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let t = txn(20);
        let b = ballot(50, 1);

        let report = replica
            .recover_routed(t, b, vec![key.clone()], vec![event("k", 1)])
            .expect("promised");
        assert!(matches!(
            report,
            RoutedRecovery::NotWitnessed { redundant: false }
        ));
        assert_eq!(replica.status(t), None, "nothing was pre-accepted");
        assert_eq!(replica.unknown_promise(t), Some(b));
        assert_eq!(
            replica.preaccept(t, vec![key.clone()], vec![event("k", 1)]),
            Err(b)
        );
        assert!(matches!(
            replica.recover_routed(t, ballot(40, 2), vec![], vec![]),
            Err(promised) if promised == b
        ));

        let old = txn(5);
        replica.compact(txn(10).0);
        assert!(matches!(
            replica.recover_routed(old, b, vec![key], vec![]),
            Ok(RoutedRecovery::NotWitnessed { redundant: true })
        ));
        assert_eq!(replica.unknown_promise(old), None);
    }

    /// An invalidated command is applied as an abort without executing, is
    /// recognizable by its timestamp, and is no evidence in another
    /// transaction's recovery: its deps were never agreed.
    #[test]
    fn an_invalidated_command_is_an_abort_and_no_superseding_evidence() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let x = txn(10);
        let t = txn(20);
        // T is witnessed before X here, so T's (local) deps lack X.
        replica
            .preaccept(t, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica
            .preaccept(x, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        assert!(replica.set_route(t, &[key.clone(), Key("other".into())]));
        assert!(
            !replica.set_route(t, std::slice::from_ref(&key)),
            "kept once"
        );

        assert!(replica.invalidate(t));
        let cmd = replica.snapshot(t).unwrap();
        assert!(cmd.invalidated());
        assert_eq!(cmd.applied_conflict, Some(true));
        assert_eq!(
            replica.stuck(txn(100).0),
            vec![x],
            "T no longer awaits recovery"
        );

        let report = replica.recover(x, ballot(50, 1), vec![], vec![]).unwrap();
        assert!(
            !report.superseding_rejects,
            "an invalidated command proves nothing about X's fast path"
        );

        // A committed command is never invalidated.
        replica.commit(x, x.0, vec![], vec![event("k", 1)], vec![], NodeId(0));
        assert!(!replica.invalidate(x));

        // The route is dropped with the command.
        replica.record_decision(x, true);
        assert_eq!(replica.next_apply().expect("ready").txn, x);
        replica.mark_applied(x, false);
        replica.compact(txn(30).0);
        assert_eq!(replica.route(t), None);
    }

    /// A journaled blind promise is restored on restart — but not over a
    /// command that carries its own, nor below the watermark.
    #[test]
    fn a_restored_promise_fences_like_the_original() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let t = txn(20);
        let b = ballot(50, 1);
        replica.restore_promise(t, b);
        assert_eq!(
            replica.preaccept(t, vec![key.clone()], vec![event("k", 1)]),
            Err(b)
        );

        let known = txn(30);
        replica
            .preaccept(known, vec![key], vec![event("k", 2)])
            .unwrap();
        replica.restore_promise(known, b);
        assert_eq!(replica.unknown_promise(known), None);

        replica.compact(txn(25).0);
        replica.restore_promise(txn(21), b);
        assert_eq!(replica.unknown_promise(txn(21)), None);
    }

    /// A read is taken once: its result stays answerable while the command holds
    /// its slot (so a recovery coordinator that takes over still gets it), and
    /// is forgotten once the decision is enacted.
    #[test]
    fn a_read_result_stays_answerable_until_applied() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        replica
            .preaccept(a, vec![key.clone()], vec![event("k", 1)])
            .unwrap();
        replica.commit(a, a.0, vec![], vec![event("k", 1)], vec![], NodeId(0));
        assert_eq!(replica.read_result(a), None, "not read yet");

        assert_eq!(replica.next_read().expect("ready to read").txn, a);
        replica.record_read(a, true);
        assert!(
            replica.next_read().is_none(),
            "a held slot is not read again"
        );
        assert_eq!(replica.read_result(a), Some(true));

        replica.record_decision(a, true);
        assert_eq!(replica.next_apply().expect("decided").txn, a);
        replica.mark_applied(a, false);
        assert_eq!(
            replica.read_result(a),
            None,
            "applied: the outcome answers now"
        );
        assert_eq!(replica.applied_result(a), Some(false));
    }

    /// A command journaled mid-read (its read result is not durable) comes back
    /// from a restart ready to be read again, instead of holding its slot with
    /// nothing to answer.
    #[test]
    fn a_restored_reading_command_is_read_again() {
        let mut replica = Replica::new();
        let key = Key("k".into());
        let a = txn(10);
        replica.restore(CommandState {
            txn: a,
            status: Status::Reading,
            promised: Ballot(a.0),
            accepted: Ballot(a.0),
            execute_at: a.0,
            deps: vec![],
            keys: vec![key],
            events: vec![event("k", 1)],
            reply_to: NodeId(0),
            decision: None,
            applied_conflict: None,
        });
        assert_eq!(replica.status(a), Some(Status::Committed));
        assert_eq!(replica.next_read().expect("read again").txn, a);
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
