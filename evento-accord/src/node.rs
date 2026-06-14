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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use evento_core::{
    cursor::{Args, Edge, ReadResult},
    Event, ReadAggregator, RoutingKey,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::api::{DataStore, Journal, MessageSink, ShardId, Topology};
use crate::clock::{Ballot, Clock, HybridLogicalClock, NodeId, Timestamp, TxnId};
use crate::message::{CommandState, Key, Message, Status};
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

/// An acceptor's single-decree-Paxos state for one config epoch.
#[derive(Clone)]
struct ConfigAcceptor {
    promised: Ballot,
    accepted: Option<(Ballot, Vec<Vec<NodeId>>)>,
}

impl Default for ConfigAcceptor {
    fn default() -> Self {
        Self {
            promised: Ballot(Timestamp {
                micros: 0,
                logical: 0,
                node: NodeId(0),
            }),
            accepted: None,
        }
    }
}

/// Per-epoch config acceptor state.
type ConfigState = Arc<Mutex<HashMap<u64, ConfigAcceptor>>>;

/// Delivers config-Paxos responses to an in-flight epoch change.
type ConfigInbox = Arc<Mutex<Option<mpsc::UnboundedSender<Message>>>>;

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
    /// Delivers the [`SyncData`](Message::SyncData) response to an in-flight
    /// [`join`](Node::join).
    sync_inbox: Arc<Mutex<Option<mpsc::UnboundedSender<Vec<CommandState>>>>>,
    /// True once this node may process consensus messages. A joining node clears
    /// it (via [`begin_join`](Node::begin_join)) so messages that arrive while it
    /// bootstraps are buffered and replayed, not dropped.
    bootstrapped: Arc<AtomicBool>,
    /// Consensus messages received while not yet bootstrapped, replayed by
    /// [`join`](Node::join) after the snapshot is imported.
    join_buffer: Arc<Mutex<Vec<Envelope>>>,
    /// This node's acceptor state per config epoch.
    config: ConfigState,
    /// Delivers config-Paxos responses to an in-flight epoch change.
    config_inbox: ConfigInbox,
    /// Next id for a forwarded read, and the channels awaiting their replies.
    read_seq: Arc<AtomicU64>,
    read_pending: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
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
            sync_inbox: Arc::new(Mutex::new(None)),
            bootstrapped: Arc::new(AtomicBool::new(true)),
            join_buffer: Arc::new(Mutex::new(Vec::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
            config_inbox: Arc::new(Mutex::new(None)),
            read_seq: Arc::new(AtomicU64::new(0)),
            read_pending: Arc::new(Mutex::new(HashMap::new())),
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
        // While bootstrapping, buffer consensus messages for replay after the
        // snapshot is imported. Cluster-management messages (bootstrap + epoch
        // changes) pass through so the node can still sync and learn its layout.
        let control = matches!(
            env.message,
            Message::SyncRequest
                | Message::SyncData { .. }
                | Message::ConfigPrepare { .. }
                | Message::ConfigPromise { .. }
                | Message::ConfigAccept { .. }
                | Message::ConfigAccepted { .. }
                | Message::ConfigCommit { .. }
                | Message::ConfigNack { .. }
                | Message::ReadForward { .. }
                | Message::ReadReply { .. }
        );
        if !control && !self.bootstrapped.load(Ordering::Acquire) {
            self.join_buffer.lock().expect("buffer poisoned").push(env);
            return;
        }

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
            // Bootstrap: a joining node asks for our committed state.
            Message::SyncRequest => {
                let commands = self
                    .replica
                    .lock()
                    .expect("replica poisoned")
                    .export_applied();
                self.send(from, Message::SyncData { commands }).await;
            }
            Message::SyncData { commands } => {
                let tx = self.sync_inbox.lock().expect("sync poisoned").clone();
                if let Some(tx) = tx {
                    let _ = tx.send(commands);
                }
            }
            // Config Paxos — acceptor side.
            Message::ConfigPrepare { epoch, ballot } => {
                let reply = {
                    let mut config = self.config.lock().expect("config poisoned");
                    let acc = config.entry(epoch).or_default();
                    if ballot < acc.promised {
                        Message::ConfigNack {
                            epoch,
                            promised: acc.promised,
                        }
                    } else {
                        acc.promised = ballot;
                        let (accepted_ballot, accepted_layout) = match &acc.accepted {
                            Some((b, l)) => (*b, Some(l.clone())),
                            None => (ConfigAcceptor::default().promised, None),
                        };
                        Message::ConfigPromise {
                            epoch,
                            accepted_ballot,
                            accepted_layout,
                        }
                    }
                };
                self.send(from, reply).await;
            }
            Message::ConfigAccept {
                epoch,
                ballot,
                layout,
            } => {
                let reply = {
                    let mut config = self.config.lock().expect("config poisoned");
                    let acc = config.entry(epoch).or_default();
                    if ballot < acc.promised {
                        Message::ConfigNack {
                            epoch,
                            promised: acc.promised,
                        }
                    } else {
                        acc.promised = ballot;
                        acc.accepted = Some((ballot, layout));
                        Message::ConfigAccepted { epoch }
                    }
                };
                self.send(from, reply).await;
            }
            Message::ConfigCommit { epoch, layout } => {
                self.topology.install(epoch, layout);
            }
            // Config Paxos responses — route to the in-flight epoch change.
            response @ (Message::ConfigPromise { .. }
            | Message::ConfigAccepted { .. }
            | Message::ConfigNack { .. }) => {
                let tx = self.config_inbox.lock().expect("config poisoned").clone();
                if let Some(tx) = tx {
                    let _ = tx.send(response);
                }
            }
            // A forwarded read: serve it from the local backend and reply.
            Message::ReadForward {
                id,
                aggregators,
                routing_key,
                args,
            } => {
                let result = self
                    .datastore
                    .read(aggregators, routing_key, args)
                    .await
                    .unwrap_or_default();
                let page_info = result.page_info;
                let mut cursors = Vec::with_capacity(result.edges.len());
                let mut events = Vec::with_capacity(result.edges.len());
                for edge in result.edges {
                    cursors.push(edge.cursor);
                    events.push(edge.node);
                }
                self.send(
                    from,
                    Message::ReadReply {
                        id,
                        cursors,
                        events,
                        page_info,
                    },
                )
                .await;
            }
            reply @ Message::ReadReply { .. } => {
                let id = match &reply {
                    Message::ReadReply { id, .. } => *id,
                    _ => unreachable!(),
                };
                let tx = self
                    .read_pending
                    .lock()
                    .expect("reads poisoned")
                    .get(&id)
                    .cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(reply);
                }
            }
            // Responses: hand to the waiting coordinator, tagged with the sender.
            response => {
                if let Some(txn) = response.txn() {
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

    /// Marks this node as joining: until [`join`](Node::join) completes,
    /// consensus messages are buffered rather than processed. Call this before
    /// the epoch that adds this node is installed, so nothing is lost in the gap.
    pub fn begin_join(&self) {
        self.bootstrapped.store(false, Ordering::Release);
    }

    /// Bootstraps this (joining) node from `contact`: fetches its committed state,
    /// imports every command for a key this node now owns (recording it in the
    /// conflict graph and applying its events), then replays any consensus
    /// messages buffered since [`begin_join`](Node::begin_join) — so a transaction
    /// committed during the join still lands. Returns how many were imported.
    ///
    /// Coordinating the epoch change across the cluster (rather than each node
    /// installing an agreed layout) remains future work.
    pub async fn join(&self, contact: NodeId) -> anyhow::Result<usize> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        *self.sync_inbox.lock().expect("sync poisoned") = Some(tx);
        self.send(contact, Message::SyncRequest).await;

        let commands = tokio::time::timeout(COLLECT_TIMEOUT, rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("bootstrap sync timed out"))?
            .ok_or_else(|| anyhow::anyhow!("bootstrap sync channel closed"))?;
        *self.sync_inbox.lock().expect("sync poisoned") = None;

        let mut imported = 0;
        for cmd in commands {
            if !cmd.keys.iter().any(|k| self.topology.owns(self.id, k)) {
                continue;
            }
            let (txn, execute_at, events) = (cmd.txn, cmd.execute_at, cmd.events.clone());
            let commit = cmd.applied_conflict == Some(false);
            let inserted = self
                .replica
                .lock()
                .expect("replica poisoned")
                .import_applied(cmd);
            if inserted {
                let _ = self.datastore.apply(txn, execute_at, events, commit).await;
                imported += 1;
            }
        }

        // Resume normal processing and replay anything buffered during bootstrap.
        self.bootstrapped.store(true, Ordering::Release);
        let buffered = std::mem::take(&mut *self.join_buffer.lock().expect("buffer poisoned"));
        for env in buffered {
            Box::pin(self.handle(env)).await;
        }

        Ok(imported)
    }

    /// Coordinates installing `shards` as the new `epoch`'s layout, fault
    /// tolerantly: runs single-decree Paxos over the **current members** so the
    /// decision survives this coordinator failing, then commits it cluster-wide.
    /// Returns the decided layout (which differs from `shards` only if a prior,
    /// interrupted change for this epoch had already been accepted).
    ///
    /// The acceptor set is the members of the current epoch (a node proposing the
    /// next epoch is still on this one), so it tracks membership: changes keep
    /// working after the original founders have left.
    pub async fn change_topology(
        &self,
        epoch: u64,
        shards: Vec<Vec<NodeId>>,
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        let decided = self.run_config_paxos(epoch, shards, &acceptors).await?;
        self.commit_topology(epoch, &decided, &acceptors).await;
        Ok(decided)
    }

    /// Drives Paxos phases 1–2 for `epoch` (no commit), returning the decided
    /// layout. Exposed so tests can simulate a coordinator that fails after the
    /// decision is durable but before committing.
    pub async fn propose_topology(
        &self,
        epoch: u64,
        proposed: Vec<Vec<NodeId>>,
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        self.run_config_paxos(epoch, proposed, &acceptors).await
    }

    /// The config-Paxos coordinator over an explicit `acceptors` set.
    async fn run_config_paxos(
        &self,
        epoch: u64,
        proposed: Vec<Vec<NodeId>>,
        acceptors: &[NodeId],
    ) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let need = acceptors.len() / 2 + 1;
        let mut ballot = Ballot(self.clock.now());

        for _attempt in 0..3 {
            let (tx, mut rx) = mpsc::unbounded_channel();
            *self.config_inbox.lock().expect("config poisoned") = Some(tx);

            // Phase 1: Prepare → adopt the highest already-accepted value, if any.
            for &node in acceptors {
                self.send(node, Message::ConfigPrepare { epoch, ballot })
                    .await;
            }
            let mut promises = 0;
            let mut nack: Option<Ballot> = None;
            let mut adopted: Option<(Ballot, Vec<Vec<NodeId>>)> = None;
            let deadline = Self::after(COLLECT_TIMEOUT);
            while promises < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(Message::ConfigPromise {
                        accepted_ballot,
                        accepted_layout,
                        ..
                    })) => {
                        promises += 1;
                        if let Some(layout) = accepted_layout {
                            if adopted.as_ref().is_none_or(|(b, _)| accepted_ballot > *b) {
                                adopted = Some((accepted_ballot, layout));
                            }
                        }
                    }
                    Ok(Some(Message::ConfigNack { promised, .. })) => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            if let Some(promised) = nack {
                *self.config_inbox.lock().expect("config poisoned") = None;
                ballot = self.higher_ballot(promised);
                continue;
            }
            if promises < need {
                *self.config_inbox.lock().expect("config poisoned") = None;
                anyhow::bail!("config prepare quorum not reached for epoch {epoch}");
            }

            let layout = adopted.map(|(_, l)| l).unwrap_or(proposed.clone());

            // Phase 2: Accept the chosen layout.
            for &node in acceptors {
                self.send(
                    node,
                    Message::ConfigAccept {
                        epoch,
                        ballot,
                        layout: layout.clone(),
                    },
                )
                .await;
            }
            let mut accepts = 0;
            let mut nack: Option<Ballot> = None;
            let deadline = Self::after(COLLECT_TIMEOUT);
            while accepts < need {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(Message::ConfigAccepted { .. })) => accepts += 1,
                    Ok(Some(Message::ConfigNack { promised, .. })) => {
                        nack = Some(promised);
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            *self.config_inbox.lock().expect("config poisoned") = None;

            if let Some(promised) = nack {
                ballot = self.higher_ballot(promised);
                continue;
            }
            if accepts < need {
                anyhow::bail!("config accept quorum not reached for epoch {epoch}");
            }
            return Ok(layout);
        }
        anyhow::bail!("config change for epoch {epoch} exhausted its ballots")
    }

    /// Recovers a possibly-interrupted change for `epoch`: completes whatever
    /// layout was already accepted (or, if none was, keeps the current layout),
    /// then commits it. Any node can call this after the original coordinator
    /// fails.
    pub async fn recover_topology(&self, epoch: u64) -> anyhow::Result<Vec<Vec<NodeId>>> {
        let acceptors = self.topology.nodes();
        let fallback = self.current_layout();
        let decided = self.run_config_paxos(epoch, fallback, &acceptors).await?;
        self.commit_topology(epoch, &decided, &acceptors).await;
        Ok(decided)
    }

    /// Installs the decided layout locally and broadcasts it to the acceptors and
    /// the new members so they install it too.
    async fn commit_topology(&self, epoch: u64, layout: &[Vec<NodeId>], acceptors: &[NodeId]) {
        self.topology.install(epoch, layout.to_vec());
        let mut recipients = acceptors.to_vec();
        for node in layout.iter().flatten() {
            if !recipients.contains(node) {
                recipients.push(*node);
            }
        }
        for &node in &recipients {
            self.send(
                node,
                Message::ConfigCommit {
                    epoch,
                    layout: layout.to_vec(),
                },
            )
            .await;
        }
    }

    /// This node's current shard layout, recovered shard-by-shard from the
    /// topology (used as the no-op fallback when recovering a config change).
    fn current_layout(&self) -> Vec<Vec<NodeId>> {
        // Single contiguous group per shard is sufficient for the fallback; the
        // recovered (accepted) value replaces it whenever one exists.
        let nodes = self.topology.nodes();
        vec![nodes]
    }

    // ----- read routing ---------------------------------------------------

    /// Whether this node replicates `key` (and so can serve reads for it locally).
    pub fn owns_key(&self, key: &Key) -> bool {
        self.topology.owns(self.id, key)
    }

    /// A replica that owns `key`, to forward a read to. Prefers another node but
    /// falls back to self.
    pub fn an_owner_of(&self, key: &Key) -> Option<NodeId> {
        let replicas = self.topology.replicas(key);
        replicas
            .iter()
            .find(|&&n| n != self.id)
            .copied()
            .or_else(|| replicas.first().copied())
    }

    /// Forwards a read to `to` (an owner of the queried range) and returns its
    /// result.
    pub async fn forward_read(
        &self,
        to: NodeId,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        let id = self.read_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.read_pending
            .lock()
            .expect("reads poisoned")
            .insert(id, tx);
        self.send(
            to,
            Message::ReadForward {
                id,
                aggregators,
                routing_key,
                args,
            },
        )
        .await;

        let reply = tokio::time::timeout(COLLECT_TIMEOUT, rx.recv()).await;
        self.read_pending
            .lock()
            .expect("reads poisoned")
            .remove(&id);
        match reply {
            Ok(Some(Message::ReadReply {
                cursors,
                events,
                page_info,
                ..
            })) => {
                let edges = events
                    .into_iter()
                    .zip(cursors)
                    .map(|(node, cursor)| Edge { cursor, node })
                    .collect();
                Ok(ReadResult { edges, page_info })
            }
            _ => anyhow::bail!("forwarded read to {to:?} failed"),
        }
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
