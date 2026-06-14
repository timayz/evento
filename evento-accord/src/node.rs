//! A cluster node: one replica plus the coordinator logic for client writes and
//! for recovering transactions whose coordinator has failed.
//!
//! Each node runs a single inbox loop that processes incoming [`Message`]s
//! sequentially — so replica state mutates without data races — and routes
//! responses (tagged with the responder's id) back to whichever local
//! coordination awaits them.
//!
//! ## M3 scope
//!
//! Multi-shard: keys are partitioned across disjoint shards, and a transaction
//! touching several shards must reach a quorum in **each**. Execution is a
//! two-phase Read → Apply so a multi-aggregate write commits **atomically**: every
//! touched shard reads its owned keys' version condition, the coordinator commits
//! only if all hold, then every shard appends (or all abort). A replica only
//! reads/applies the events for keys it owns.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use evento_core::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::api::{DataStore, Journal, MessageSink, ShardId, Topology};
use crate::clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
use crate::message::{Key, Message, Status};
use crate::replica::Replica;
use crate::transport::Envelope;

/// Wait for the fast quorum before falling back to the slow path. Short so a
/// down node barely delays a write.
const FAST_TIMEOUT: Duration = Duration::from_millis(50);
/// Cap on waiting for a slow quorum at later phases (returns as soon as the
/// quorum is reached; the cap only bounds a genuinely stuck phase).
const COLLECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The result of a coordinated (or recovered) write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitOutcome {
    /// The committed transaction's id.
    pub txn: TxnId,
    /// Whether the write was aborted by the optimistic-version condition
    /// (evento's `InvalidOriginalVersion`) — atomically, across all shards.
    pub conflict: bool,
}

/// The replica set and quorum sizes for one shard a transaction touches.
#[derive(Clone)]
struct ShardPlan {
    shard: ShardId,
    replicas: Vec<NodeId>,
    fast_q: usize,
    slow_q: usize,
}

/// One replica's recovery report, extracted from a `RecoverOk`.
struct RecoverFields {
    known: bool,
    status: Status,
    accepted: Ballot,
    execute_at: Timestamp,
    deps: Vec<TxnId>,
    superseding_rejects: bool,
    keys: Vec<Key>,
    events: Vec<Event>,
}

/// A response to a `Recover`: a report, or a ballot rejection.
enum RecoverResp {
    Ok(RecoverFields),
    Nack(Ballot),
}

/// In-flight coordinations: transaction → channel delivering its tagged
/// responses `(responder, message)`.
type Pending = Arc<Mutex<HashMap<TxnId, mpsc::UnboundedSender<(NodeId, Message)>>>>;

/// A cluster node. Cheap to clone — all state is shared behind `Arc`.
#[derive(Clone)]
pub struct Node {
    id: NodeId,
    clock: Arc<HybridLogicalClock>,
    topology: Arc<dyn Topology>,
    sink: Arc<dyn MessageSink>,
    datastore: Arc<dyn DataStore>,
    journal: Arc<dyn Journal>,
    replica: Arc<Mutex<Replica>>,
    pending: Pending,
}

impl Node {
    /// Builds a node from its injected dependencies.
    pub fn new(
        id: NodeId,
        topology: Arc<dyn Topology>,
        clock: Arc<HybridLogicalClock>,
        sink: Arc<dyn MessageSink>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
    ) -> Self {
        Self {
            id,
            clock,
            topology,
            sink,
            datastore,
            journal,
            replica: Arc::new(Mutex::new(Replica::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Spawns the inbox loop that drives this node until the network closes.
    pub fn start(&self, mut inbox: mpsc::UnboundedReceiver<Envelope>) -> JoinHandle<()> {
        let node = self.clone();
        tokio::spawn(async move {
            while let Some(env) = inbox.recv().await {
                node.handle(env).await;
            }
        })
    }

    // ----- inbox handling -------------------------------------------------

    /// Processes one inbound message.
    async fn handle(&self, env: Envelope) {
        let from = env.from;
        match env.message {
            Message::PreAccept { txn, keys, events } => {
                // Only handle the keys/events this node owns.
                let keys = self.owned_keys(keys);
                let events = self.owned_events(events);
                let (execute_at, deps) = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .preaccept(txn, keys, events);
                self.send(
                    from,
                    Message::PreAcceptOk {
                        txn,
                        execute_at,
                        deps,
                    },
                )
                .await;
            }
            Message::Accept {
                txn,
                ballot,
                execute_at,
                deps,
            } => {
                let reply = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .accept(txn, ballot, execute_at, deps);
                match reply {
                    Ok(deps) => self.send(from, Message::AcceptOk { txn, deps }).await,
                    Err(promised) => self.send(from, Message::Nack { txn, promised }).await,
                }
            }
            Message::Commit {
                txn,
                execute_at,
                deps,
                events,
                reply_to,
            } => {
                let events = self.owned_events(events);
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .commit(txn, execute_at, deps, events, reply_to);
                self.journal_record(txn).await;

                // If already applied (e.g. a second recovery), short-circuit the
                // read phase with the stored outcome.
                let applied = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .applied_result(txn);
                match applied {
                    Some(conflict) => {
                        self.send(reply_to, Message::ReadOk { txn, ok: !conflict })
                            .await;
                    }
                    None => self.drive().await,
                }
            }
            Message::Apply { txn, commit } => {
                // Record the decision (idempotent) and acknowledge if it was
                // already applied; `drive` enacts it now if ready, or later once
                // dependencies clear (so a lagging replica still converges).
                let already_applied = {
                    let mut replica = self.replica.lock().expect("replica poisoned");
                    let already = replica.applied_result(txn).is_some();
                    replica.record_decision(txn, commit);
                    already
                };
                if already_applied {
                    let reply_to = self.replica.lock().expect("replica poisoned").reply_to(txn);
                    if let Some(reply_to) = reply_to {
                        self.send(reply_to, Message::Applied { txn }).await;
                    }
                } else {
                    self.drive().await;
                }
            }
            Message::Recover { txn, ballot } => {
                let reply = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .recover(txn, ballot);
                let message = match reply {
                    Ok(state) => Message::RecoverOk {
                        txn,
                        known: state.known,
                        status: state.status,
                        accepted: state.accepted,
                        execute_at: state.execute_at,
                        deps: state.deps,
                        superseding_rejects: state.superseding_rejects,
                        keys: state.keys,
                        events: state.events,
                    },
                    Err(promised) => Message::Nack { txn, promised },
                };
                self.send(from, message).await;
            }
            // Responses: hand to the waiting coordinator, tagged with the sender.
            response => {
                let txn = response.txn();
                let tx = self
                    .pending
                    .lock()
                    .expect("pending poisoned")
                    .get(&txn)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send((from, response));
                }
            }
        }
    }

    /// Drives execution in execution-timestamp order: applies any transaction
    /// whose decision is known and whose dependencies are satisfied, otherwise
    /// reads (and holds the slot for) the next ready undecided transaction. Re-run
    /// after any Commit or Apply, since either can unblock more work.
    async fn drive(&self) {
        loop {
            // Prefer enacting a decided, ready transaction.
            let apply = self.replica.lock().expect("replica poisoned").next_apply();
            if let Some(apply) = apply {
                let _ = self
                    .datastore
                    .apply(apply.txn, apply.execute_at, apply.events, apply.commit)
                    .await;
                self.replica
                    .lock()
                    .expect("replica poisoned")
                    .mark_applied(apply.txn, !apply.commit);
                self.journal_record(apply.txn).await;
                self.send(apply.reply_to, Message::Applied { txn: apply.txn })
                    .await;
                continue;
            }

            // Otherwise read the next ready, undecided transaction.
            let read = self.replica.lock().expect("replica poisoned").next_read();
            let Some(read) = read else { break };
            let ok = self.read_condition(&read.events).await;
            self.send(read.reply_to, Message::ReadOk { txn: read.txn, ok })
                .await;
        }
    }

    /// Whether every owned event extends its aggregate (version strictly greater
    /// than what is stored) — this node's part of the atomic commit condition.
    async fn read_condition(&self, owned_events: &[Event]) -> bool {
        for event in owned_events {
            let current = self
                .datastore
                .version(&event.aggregator_type, &event.aggregator_id)
                .await
                .unwrap_or(0);
            if event.version <= current {
                return false;
            }
        }
        true
    }

    /// Persists the current durable state of `txn` to the journal.
    async fn journal_record(&self, txn: TxnId) {
        let snapshot = self.replica.lock().expect("replica poisoned").snapshot(txn);
        if let Some(state) = snapshot {
            let _ = self.journal.record(&state).await;
        }
    }

    // ----- coordination ---------------------------------------------------

    /// Coordinates appending `events` as one strictly-serializable, atomic
    /// transaction across every shard it touches.
    pub async fn write(&self, events: Vec<Event>) -> anyhow::Result<CommitOutcome> {
        let keys = Self::keys_of(&events);
        let (plans, union) = self.plan(&keys);

        let t0 = self.clock.now();
        let txn = TxnId(t0);
        let mut rx = self.register(txn);

        // PreAccept: a fast quorum per shard, or whatever arrives before the
        // fast-path timeout.
        self.broadcast(
            &union,
            Message::PreAccept {
                txn,
                keys,
                events: events.clone(),
            },
        )
        .await;
        let pre = self
            .collect_by_shard(
                &mut rx,
                &plans,
                |p| p.fast_q,
                Self::after(FAST_TIMEOUT),
                |m| match m {
                    Message::PreAcceptOk {
                        execute_at, deps, ..
                    } => Some((execute_at, deps)),
                    _ => None,
                },
            )
            .await;
        for plan in &plans {
            if pre[&plan.shard].len() < plan.slow_q {
                self.deregister(txn);
                anyhow::bail!("preaccept quorum not reached for shard {}", plan.shard);
            }
        }

        let all_fast = plans.iter().all(|p| {
            let r = &pre[&p.shard];
            r.len() >= p.fast_q && r.iter().all(|(e, _)| *e == t0)
        });
        let execute_at = pre.values().flatten().map(|(e, _)| *e).max().unwrap_or(t0);
        // Dependencies are kept **per shard** — each shard's deps come only from
        // its own replicas, so a replica never receives (and stalls on) a
        // dependency in a shard it cannot witness.
        let mut shard_deps: HashMap<ShardId, BTreeSet<TxnId>> = plans
            .iter()
            .map(|p| {
                let deps = pre[&p.shard]
                    .iter()
                    .flat_map(|(_, d)| d.iter().copied())
                    .collect();
                (p.shard, deps)
            })
            .collect();

        // Slow path: Accept the raised timestamp and each shard's dependency set.
        if !all_fast {
            for plan in &plans {
                let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                for &node in &plan.replicas {
                    self.send(
                        node,
                        Message::Accept {
                            txn,
                            ballot: Ballot(t0),
                            execute_at,
                            deps: deps.clone(),
                        },
                    )
                    .await;
                }
            }
            let acks = self
                .collect_by_shard(
                    &mut rx,
                    &plans,
                    |p| p.slow_q,
                    Self::after(COLLECT_TIMEOUT),
                    |m| match m {
                        Message::AcceptOk { deps, .. } => Some(deps),
                        _ => None,
                    },
                )
                .await;
            for plan in &plans {
                if acks[&plan.shard].len() < plan.slow_q {
                    self.deregister(txn);
                    anyhow::bail!("accept quorum not reached for shard {}", plan.shard);
                }
            }
            for (shard, deps_lists) in acks {
                if let Some(set) = shard_deps.get_mut(&shard) {
                    for deps in deps_lists {
                        set.extend(deps);
                    }
                }
            }
        }

        let conflict = self
            .execute(&mut rx, &plans, txn, execute_at, shard_deps, events)
            .await?;
        self.deregister(txn);
        Ok(CommitOutcome { txn, conflict })
    }

    /// Commit → Read → Apply: drive the atomic decision across all touched
    /// shards and return whether the write was aborted.
    async fn execute(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        txn: TxnId,
        execute_at: Timestamp,
        shard_deps: HashMap<ShardId, BTreeSet<TxnId>>,
        events: Vec<Event>,
    ) -> anyhow::Result<bool> {
        let union = Self::union_of(plans);

        // Commit each shard with its own dependency set.
        for plan in plans {
            let deps: Vec<TxnId> = shard_deps
                .get(&plan.shard)
                .into_iter()
                .flatten()
                .copied()
                .filter(|d| *d != txn)
                .collect();
            for &node in &plan.replicas {
                self.send(
                    node,
                    Message::Commit {
                        txn,
                        execute_at,
                        deps: deps.clone(),
                        events: events.clone(),
                        reply_to: self.id,
                    },
                )
                .await;
            }
        }

        // Read: a slow quorum per shard reports its version condition.
        let reads = self
            .collect_by_shard(
                rx,
                plans,
                |p| p.slow_q,
                Self::after(COLLECT_TIMEOUT),
                |m| match m {
                    Message::ReadOk { ok, .. } => Some(ok),
                    _ => None,
                },
            )
            .await;
        for plan in plans {
            if reads[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("read quorum not reached for shard {}", plan.shard);
            }
        }
        // Atomic commit only if every shard's condition holds.
        let commit = reads.values().flatten().all(|ok| *ok);

        // Apply: enact the decision and wait for a slow quorum per shard.
        self.broadcast(&union, Message::Apply { txn, commit }).await;
        let acks = self
            .collect_by_shard(
                rx,
                plans,
                |p| p.slow_q,
                Self::after(COLLECT_TIMEOUT),
                |m| match m {
                    Message::Applied { .. } => Some(()),
                    _ => None,
                },
            )
            .await;
        for plan in plans {
            if acks[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("apply quorum not reached for shard {}", plan.shard);
            }
        }

        // Read-your-writes: if this coordinator is itself a replica, wait for its
        // own local apply so a subsequent read on this node sees the write.
        if union.contains(&self.id) {
            let deadline = Self::after(COLLECT_TIMEOUT);
            while self
                .replica
                .lock()
                .expect("replica poisoned")
                .applied_result(txn)
                .is_none()
            {
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        Ok(!commit)
    }

    /// Recovers `txn`: takes it over under a fresh ballot and drives it to an
    /// applied outcome consistent with whatever its failed coordinator decided.
    pub async fn recover(&self, txn: TxnId) -> anyhow::Result<CommitOutcome> {
        let all_nodes = self.topology.nodes();
        let mut ballot = Ballot(self.clock.now());

        for _attempt in 0..3 {
            let mut rx = self.register(txn);
            self.broadcast(&all_nodes, Message::Recover { txn, ballot })
                .await;

            // Gather what every reachable replica knows within a short window,
            // tagged with the responder so deps can be grouped by shard.
            let resp =
                Self::collect_tagged(&mut rx, all_nodes.len(), Self::after(FAST_TIMEOUT), |m| {
                    match m {
                        Message::RecoverOk {
                            known,
                            status,
                            accepted,
                            execute_at,
                            deps,
                            superseding_rejects,
                            keys,
                            events,
                            ..
                        } => Some(RecoverResp::Ok(RecoverFields {
                            known,
                            status,
                            accepted,
                            execute_at,
                            deps,
                            superseding_rejects,
                            keys,
                            events,
                        })),
                        Message::Nack { promised, .. } => Some(RecoverResp::Nack(promised)),
                        _ => None,
                    }
                })
                .await;

            if let Some(promised) = resp.iter().find_map(|(_, r)| match r {
                RecoverResp::Nack(p) => Some(*p),
                _ => None,
            }) {
                self.deregister(txn);
                ballot = self.higher_ballot(promised);
                continue;
            }

            let known: Vec<(NodeId, RecoverFields)> = resp
                .into_iter()
                .filter_map(|(from, r)| match r {
                    RecoverResp::Ok(f) if f.known => Some((from, f)),
                    _ => None,
                })
                .collect();
            if known.is_empty() {
                self.deregister(txn);
                anyhow::bail!("transaction unknown to recovery quorum");
            }

            // Reconstruct the full key/event set and plan the touched shards.
            let mut keys: Vec<Key> = Vec::new();
            for k in known.iter().flat_map(|(_, f)| f.keys.iter()) {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
            let (plans, _union) = self.plan(&keys);
            // Each shard reported only its owned events; reassemble the whole set
            // so every replica can extract its part from the Commit.
            let events_full = self.reassemble_events(&known);

            let max_status = known
                .iter()
                .map(|(_, f)| f.status)
                .max()
                .expect("non-empty");
            // The recovered execution timestamp (global), by max status.
            let execute_at = match max_status {
                Status::Committed | Status::Reading | Status::Applied => {
                    known
                        .iter()
                        .find(|(_, f)| f.status >= Status::Committed)
                        .expect("committed present")
                        .1
                        .execute_at
                }
                Status::Accepted => {
                    known
                        .iter()
                        .max_by_key(|(_, f)| f.accepted)
                        .expect("non-empty")
                        .1
                        .execute_at
                }
                Status::PreAccepted => {
                    if known.iter().any(|(_, f)| f.superseding_rejects) {
                        known
                            .iter()
                            .map(|(_, f)| f.execute_at)
                            .max()
                            .expect("non-empty")
                    } else {
                        txn.0
                    }
                }
            };

            // Per-shard deps: union of the deps reported by each shard's own
            // replicas, so no shard's barrier names a transaction from another.
            let mut shard_deps: HashMap<ShardId, BTreeSet<TxnId>> =
                plans.iter().map(|p| (p.shard, BTreeSet::new())).collect();
            for (from, f) in &known {
                if let Some(shard) = self.topology.node_shard(*from) {
                    if let Some(set) = shard_deps.get_mut(&shard) {
                        set.extend(f.deps.iter().copied());
                    }
                }
            }

            // Re-propose under our ballot (unless already committed), then drive
            // the atomic Commit → Read → Apply.
            if max_status < Status::Committed {
                for plan in &plans {
                    let deps: Vec<TxnId> = shard_deps[&plan.shard].iter().copied().collect();
                    for &node in &plan.replicas {
                        self.send(
                            node,
                            Message::Accept {
                                txn,
                                ballot,
                                execute_at,
                                deps: deps.clone(),
                            },
                        )
                        .await;
                    }
                }
                let acks = self
                    .collect_by_shard(
                        &mut rx,
                        &plans,
                        |p| p.slow_q,
                        Self::after(COLLECT_TIMEOUT),
                        |m| match m {
                            Message::AcceptOk { deps, .. } => Some(Ok(deps)),
                            Message::Nack { promised, .. } => Some(Err(promised)),
                            _ => None,
                        },
                    )
                    .await;
                if let Some(promised) = acks
                    .values()
                    .flatten()
                    .find_map(|a| a.as_ref().err().copied())
                {
                    self.deregister(txn);
                    ballot = self.higher_ballot(promised);
                    continue;
                }
                for (shard, lists) in acks {
                    if let Some(set) = shard_deps.get_mut(&shard) {
                        for a in lists.into_iter().flatten() {
                            set.extend(a);
                        }
                    }
                }
            }

            let conflict = self
                .execute(&mut rx, &plans, txn, execute_at, shard_deps, events_full)
                .await?;
            self.deregister(txn);
            return Ok(CommitOutcome { txn, conflict });
        }

        anyhow::bail!("recovery exhausted its ballot retries")
    }

    /// Runs only PreAccept and returns the transaction id, modelling a
    /// coordinator that crashed before committing. For tests of [`recover`].
    pub async fn coordinate_preaccept(&self, events: Vec<Event>) -> anyhow::Result<TxnId> {
        let keys = Self::keys_of(&events);
        let (plans, union) = self.plan(&keys);
        let t0 = self.clock.now();
        let txn = TxnId(t0);

        let mut rx = self.register(txn);
        self.broadcast(&union, Message::PreAccept { txn, keys, events })
            .await;
        let pre = self
            .collect_by_shard(
                &mut rx,
                &plans,
                |p| p.slow_q,
                Self::after(COLLECT_TIMEOUT),
                |m| match m {
                    Message::PreAcceptOk { .. } => Some(()),
                    _ => None,
                },
            )
            .await;
        self.deregister(txn);

        for plan in &plans {
            if pre[&plan.shard].len() < plan.slow_q {
                anyhow::bail!("preaccept quorum not reached for shard {}", plan.shard);
            }
        }
        Ok(txn)
    }

    // ----- helpers --------------------------------------------------------

    /// Re-assembles the full event set from the owned subsets each shard's
    /// replicas reported during recovery.
    fn reassemble_events(&self, known: &[(NodeId, RecoverFields)]) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::new();
        for event in known.iter().flat_map(|(_, f)| f.events.iter()) {
            if !events
                .iter()
                .any(|e| e.aggregator_id == event.aggregator_id && e.version == event.version)
            {
                events.push(event.clone());
            }
        }
        events
    }

    /// The shards a key set touches, with their replica sets and quorum sizes,
    /// plus the deduplicated union of all their replicas.
    fn plan(&self, keys: &[Key]) -> (Vec<ShardPlan>, Vec<NodeId>) {
        let mut shards: BTreeMap<ShardId, ShardPlan> = BTreeMap::new();
        for key in keys {
            let shard = self.topology.shard_of(key);
            shards.entry(shard).or_insert_with(|| ShardPlan {
                shard,
                replicas: self.topology.replicas(key),
                fast_q: self.topology.fast_quorum(key),
                slow_q: self.topology.slow_quorum(key),
            });
        }
        let plans: Vec<ShardPlan> = shards.into_values().collect();
        let union = Self::union_of(&plans);
        (plans, union)
    }

    /// The deduplicated union of every shard's replica set.
    fn union_of(plans: &[ShardPlan]) -> Vec<NodeId> {
        let mut union: Vec<NodeId> = Vec::new();
        for plan in plans {
            for &node in &plan.replicas {
                if !union.contains(&node) {
                    union.push(node);
                }
            }
        }
        union
    }

    /// The events this node owns (keys in this node's shard).
    fn owned_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .filter(|e| self.topology.owns(self.id, &Key::of(e)))
            .collect()
    }

    /// The keys this node owns.
    fn owned_keys(&self, keys: Vec<Key>) -> Vec<Key> {
        keys.into_iter()
            .filter(|k| self.topology.owns(self.id, k))
            .collect()
    }

    /// The distinct keys a set of events touches.
    fn keys_of(events: &[Event]) -> Vec<Key> {
        let mut keys: Vec<Key> = Vec::new();
        for event in events {
            let key = Key::of(event);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        keys
    }

    /// A ballot strictly greater than `at_least`, owned by this node.
    fn higher_ballot(&self, at_least: Ballot) -> Ballot {
        let now = self.clock.now();
        if now > at_least.0 {
            Ballot(now)
        } else {
            Ballot(Timestamp {
                micros: at_least.0.micros,
                logical: at_least.0.logical + 1,
                node: self.id,
            })
        }
    }

    /// A deadline `d` from now.
    fn after(d: Duration) -> Instant {
        Instant::now() + d
    }

    /// Registers a coordination for `txn` and returns its response channel.
    fn register(&self, txn: TxnId) -> mpsc::UnboundedReceiver<(NodeId, Message)> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(txn, tx);
        rx
    }

    /// Removes a finished coordination.
    fn deregister(&self, txn: TxnId) {
        self.pending.lock().expect("pending poisoned").remove(&txn);
    }

    /// Collects up to `want` items matching `extract`, each tagged with the
    /// responder's id (so the caller can group by shard).
    async fn collect_tagged<T>(
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        want: usize,
        deadline: Instant,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> Vec<(NodeId, T)> {
        let mut out = Vec::new();
        while out.len() < want {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, msg))) => {
                    if let Some(item) = extract(msg) {
                        out.push((from, item));
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        out
    }

    /// Collects matching responses grouped by the responder's shard, stopping
    /// once every planned shard has `target(plan)` items (or at the deadline).
    async fn collect_by_shard<T>(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>,
        plans: &[ShardPlan],
        target: impl Fn(&ShardPlan) -> usize,
        deadline: Instant,
        mut extract: impl FnMut(Message) -> Option<T>,
    ) -> HashMap<ShardId, Vec<T>> {
        let mut got: HashMap<ShardId, Vec<T>> =
            plans.iter().map(|p| (p.shard, Vec::new())).collect();

        loop {
            if plans.iter().all(|p| got[&p.shard].len() >= target(p)) {
                break;
            }
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some((from, msg))) => {
                    if let Some(item) = extract(msg) {
                        if let Some(shard) = self.topology.node_shard(from) {
                            if let Some(bucket) = got.get_mut(&shard) {
                                bucket.push(item);
                            }
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        got
    }

    /// Sends `message` to every node in `nodes`.
    async fn broadcast(&self, nodes: &[NodeId], message: Message) {
        for &node in nodes {
            self.send(node, message.clone()).await;
        }
    }

    /// Sends a single message, ignoring transport-level loss.
    async fn send(&self, to: NodeId, message: Message) {
        let _ = self.sink.send(to, message).await;
    }
}
