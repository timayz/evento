//! Simulation harness for orchestrating distributed tests.
//!
//! Provides the main `Simulation` type that coordinates nodes,
//! network, and fault injection.

use crate::protocol::{self, Message};
use crate::sim::accord::SimCoordinator;
use crate::sim::checker::{CheckResult, InvariantChecker, LinearizabilityChecker};
use crate::sim::durable::{SharedDurableStorage, SimDurableStore};
use crate::sim::executor::SimExecutor;
use crate::sim::faults::FaultConfig;
use crate::sim::history::History;
use crate::sim::network::VirtualNetwork;
use crate::sim::{SimRng, VirtualClock};
use crate::state::{ConsensusState, DurableStore};
use crate::timestamp::HybridClock;
use crate::transport::NodeId;
use crate::txn::{SerializableEvent, TxnStatus};
use evento_core::{Event, Executor};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A simulated ACCORD node.
pub struct SimNode {
    /// Node identifier.
    pub id: NodeId,
    /// Consensus state machine.
    pub state: Arc<ConsensusState>,
    /// Event storage.
    pub executor: SimExecutor,
    /// Hybrid logical clock.
    pub clock: HybridClock,
    /// ACCORD coordinator for this node.
    pub coordinator: SimCoordinator,
    /// Replicated events received from coordinators (keyed by txn_id).
    pub replicated_events: HashMap<crate::txn::TxnId, Vec<Event>>,
    /// Durable storage for crash recovery.
    pub durable: SimDurableStore,
    /// Whether the node is currently alive.
    pub is_alive: bool,
}

impl SimNode {
    /// Create a new simulated node.
    pub fn new(id: NodeId, cluster: Vec<NodeId>, shared_storage: &SharedDurableStorage) -> Self {
        Self {
            id,
            state: Arc::new(ConsensusState::new()),
            executor: SimExecutor::new(),
            clock: HybridClock::new(id),
            coordinator: SimCoordinator::new(id, cluster),
            replicated_events: HashMap::new(),
            durable: shared_storage.for_node(id),
            is_alive: true,
        }
    }

    /// Handle an incoming protocol message.
    pub async fn handle_message(&self, msg: Message) -> crate::Result<Message> {
        protocol::handle_message(&self.state, msg).await
    }
}

/// Simulation statistics.
#[derive(Clone, Debug, Default)]
pub struct SimStats {
    /// Number of simulation ticks executed.
    pub ticks: u64,
    /// Number of messages sent.
    pub messages_sent: u64,
    /// Number of messages delivered.
    pub messages_delivered: u64,
    /// Number of messages dropped.
    pub messages_dropped: u64,
    /// Number of node crashes.
    pub crashes: u64,
    /// Number of node restarts.
    pub restarts: u64,
    /// Number of partitions created.
    pub partitions_created: u64,
    /// Number of partitions healed.
    pub partitions_healed: u64,
    /// Number of client operations started.
    pub operations_started: u64,
    /// Number of client operations completed.
    pub operations_completed: u64,
}

/// Main simulation harness.
pub struct Simulation {
    /// Virtual clock shared by all components.
    clock: VirtualClock,

    /// Deterministic RNG.
    rng: SimRng,

    /// Virtual network.
    network: VirtualNetwork,

    /// Simulated nodes.
    nodes: HashMap<NodeId, SimNode>,

    /// Node IDs in order.
    node_ids: Vec<NodeId>,

    /// Operation history.
    history: History,

    /// Fault configuration.
    fault_config: FaultConfig,

    /// Statistics.
    stats: SimStats,

    /// Seed used for this simulation.
    seed: u64,

    /// Shared durable storage (survives crashes).
    durable_storage: SharedDurableStorage,

    /// Tokio runtime for async operations.
    runtime: tokio::runtime::Runtime,
}

impl Simulation {
    /// Create a new simulation builder.
    pub fn builder() -> SimulationBuilder {
        SimulationBuilder::default()
    }

    /// Get the simulation seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Get the virtual clock.
    pub fn clock(&self) -> &VirtualClock {
        &self.clock
    }

    /// Get current virtual time.
    pub fn now(&self) -> Duration {
        self.clock.now()
    }

    /// Advance simulation by one tick (1ms).
    pub fn tick(&mut self) {
        self.stats.ticks += 1;

        // Advance virtual time
        self.clock.advance(Duration::from_millis(1));

        // Maybe inject faults
        self.maybe_inject_faults();

        // Deliver ready messages
        self.deliver_messages();
    }

    /// Advance simulation by multiple ticks.
    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    /// Run until no more messages in flight and no pending operations.
    pub fn drain(&mut self) {
        let max_iterations = 100000;
        let mut iterations = 0;

        while (self.network.has_pending() || self.history.has_pending())
            && iterations < max_iterations
        {
            // Advance to next message delivery time if available
            if let Some(next_time) = self.network.next_delivery_time() {
                self.clock.advance_to(next_time);
            }
            self.tick();
            iterations += 1;
        }
    }

    /// Run until a condition is met or max ticks reached.
    pub fn run_until<F>(&mut self, max_ticks: u64, condition: F) -> bool
    where
        F: Fn(&Simulation) -> bool,
    {
        for _ in 0..max_ticks {
            if condition(self) {
                return true;
            }
            self.tick();
        }
        false
    }

    fn maybe_inject_faults(&mut self) {
        // Maybe crash a node
        if self.rng.next_bool(self.fault_config.crash_probability) {
            if let Some(&node_id) = self.rng.choose(&self.node_ids) {
                if self.is_alive(node_id) {
                    self.crash_node(node_id);
                }
            }
        }

        // Maybe restart a crashed node
        if self.rng.next_bool(self.fault_config.restart_probability) {
            let crashed: Vec<_> = self
                .nodes
                .iter()
                .filter(|(_, n)| !n.is_alive)
                .map(|(id, _)| *id)
                .collect();
            if let Some(&node_id) = self.rng.choose(&crashed) {
                self.restart_node(node_id);
            }
        }

        // Maybe create partition
        if self.rng.next_bool(self.fault_config.partition_probability) && self.node_ids.len() >= 2 {
            let a = *self.rng.choose(&self.node_ids).unwrap();
            let b = *self.rng.choose(&self.node_ids).unwrap();
            if a != b && !self.network.is_partitioned(a, b) {
                self.partition(a, b);
            }
        }

        // Maybe heal a partition
        if self.rng.next_bool(self.fault_config.heal_probability) {
            let partitions = self.network.partitions();
            if let Some(&(a, b)) = self.rng.choose(&partitions) {
                self.heal(a, b);
            }
        }
    }

    fn deliver_messages(&mut self) {
        // Collect messages to process (avoid borrow issues)
        let mut messages_to_deliver = Vec::new();
        while let Some(msg) = self.network.next_message() {
            messages_to_deliver.push(msg);
        }

        for msg in messages_to_deliver {
            self.stats.messages_delivered += 1;

            let is_alive = self.nodes.get(&msg.to).map(|n| n.is_alive).unwrap_or(false);

            if !is_alive {
                continue;
            }

            // Record message received
            self.history.message_received(
                msg.to,
                self.clock.now(),
                msg.from,
                msg.message.type_name().to_string(),
            );

            // Check if this is a response message (for coordinator)
            match &msg.message {
                Message::PreAcceptOk(resp) => {
                    self.handle_preaccept_response(msg.to, msg.from, resp.clone());
                }
                Message::PreAcceptNack(_) => {
                    // TODO: Handle ballot conflict
                }
                Message::AcceptOk(resp) => {
                    self.handle_accept_response(msg.to, msg.from, resp.clone());
                }
                Message::AcceptNack(_) => {
                    // TODO: Handle ballot conflict
                }
                Message::CommitOk(_) => {
                    // Commit is fire-and-forget, just record
                }
                Message::Commit(ref commit_req) => {
                    // Handle Commit message - this needs special handling for durability
                    let state = self.nodes.get(&msg.to).unwrap().state.clone();
                    let response = self.runtime.block_on(async {
                        protocol::handle_message(&state, msg.message.clone()).await
                    });

                    if response.is_ok() {
                        // Persist to durable storage for crash recovery
                        let node = self.nodes.get(&msg.to).unwrap();
                        let txn = crate::txn::Transaction {
                            id: commit_req.txn_id,
                            events_data: commit_req.events_data.clone(),
                            keys: commit_req.keys.clone(),
                            deps: commit_req.deps.clone(),
                            status: TxnStatus::Committed,
                            execute_at: commit_req.execute_at,
                            ballot: crate::txn::Ballot::default(),
                        };
                        let _ = self
                            .runtime
                            .block_on(async { node.durable.persist(&txn).await });

                        // Replicate events to this node
                        if let Ok(events) =
                            SerializableEvent::decode_events(&commit_req.events_data)
                        {
                            let node = self.nodes.get_mut(&msg.to).unwrap();
                            node.replicated_events.insert(commit_req.txn_id, events);
                        }
                    }

                    if let Ok(response_msg) = response {
                        self.send_message(msg.to, msg.from, response_msg);
                    }
                }
                _ => {
                    // Request message - process and respond
                    let state = self.nodes.get(&msg.to).unwrap().state.clone();
                    let response = self
                        .runtime
                        .block_on(async { protocol::handle_message(&state, msg.message).await });

                    if let Ok(response_msg) = response {
                        self.send_message(msg.to, msg.from, response_msg);
                    }
                }
            }
        }
    }

    /// Handle a PreAccept response at the coordinator.
    fn handle_preaccept_response(
        &mut self,
        coordinator_id: NodeId,
        from: NodeId,
        resp: crate::protocol::messages::PreAcceptResponse,
    ) {
        let _txn_id = resp.txn_id;

        // Get coordinator and feed response
        let ready_txn = {
            let node = match self.nodes.get_mut(&coordinator_id) {
                Some(n) if n.is_alive => n,
                _ => return,
            };
            node.coordinator
                .handle_response(from, Message::PreAcceptOk(resp))
        };

        // If we have quorum, proceed with fast/slow path
        if let Some(txn_id) = ready_txn {
            self.complete_preaccept_phase(coordinator_id, txn_id);
        }
    }

    /// Complete the PreAccept phase and proceed to Accept or Commit.
    fn complete_preaccept_phase(&mut self, coordinator_id: NodeId, txn_id: crate::txn::TxnId) {
        // Get local deps and check fast path
        let (fast_path, merged_deps, txn_data) = {
            let node = match self.nodes.get(&coordinator_id) {
                Some(n) if n.is_alive => n,
                _ => return,
            };

            let txn = self
                .runtime
                .block_on(async { node.state.get(&txn_id).await });
            let txn = match txn {
                Some(t) => t,
                None => return,
            };

            let local_deps = txn.deps.clone();
            let (fast_path, merged_deps) = node.coordinator.check_fast_path(&txn_id, &local_deps);

            (
                fast_path,
                merged_deps,
                (
                    txn.execute_at,
                    txn.ballot,
                    txn.events_data.clone(),
                    txn.keys.clone(),
                ),
            )
        };

        let (execute_at, ballot, events_data, keys) = txn_data;

        if fast_path {
            // Fast path: go directly to Commit
            self.commit_transaction(
                coordinator_id,
                txn_id,
                merged_deps,
                execute_at,
                events_data,
                keys,
            );
        } else {
            // Slow path: need Accept phase
            let messages = {
                let node = self.nodes.get_mut(&coordinator_id).unwrap();
                node.coordinator.create_accept_messages(
                    txn_id,
                    merged_deps.clone(),
                    execute_at,
                    ballot,
                )
            };

            // Update local state with Accept
            {
                let node = self.nodes.get(&coordinator_id).unwrap();
                let _ = self.runtime.block_on(async {
                    node.state
                        .accept(txn_id, merged_deps, execute_at, ballot)
                        .await
                });
            }

            // Send Accept messages
            for (to, msg) in messages {
                self.send_message(coordinator_id, to, msg);
            }
        }
    }

    /// Handle an Accept response at the coordinator.
    fn handle_accept_response(
        &mut self,
        coordinator_id: NodeId,
        from: NodeId,
        resp: crate::protocol::messages::AcceptResponse,
    ) {
        let _txn_id = resp.txn_id;

        // Get coordinator and feed response
        let ready_txn = {
            let node = match self.nodes.get_mut(&coordinator_id) {
                Some(n) if n.is_alive => n,
                _ => return,
            };
            node.coordinator
                .handle_response(from, Message::AcceptOk(resp))
        };

        // If we have quorum, proceed to Commit
        if let Some(txn_id) = ready_txn {
            // Get transaction data
            let txn_data = {
                let node = self.nodes.get(&coordinator_id).unwrap();
                let txn = self
                    .runtime
                    .block_on(async { node.state.get(&txn_id).await });
                txn.map(|t| {
                    (
                        t.deps.clone(),
                        t.execute_at,
                        t.events_data.clone(),
                        t.keys.clone(),
                    )
                })
            };

            if let Some((deps, execute_at, events_data, keys)) = txn_data {
                self.commit_transaction(
                    coordinator_id,
                    txn_id,
                    deps,
                    execute_at,
                    events_data,
                    keys,
                );
            }
        }
    }

    /// Commit a transaction and broadcast to all nodes.
    fn commit_transaction(
        &mut self,
        coordinator_id: NodeId,
        txn_id: crate::txn::TxnId,
        deps: Vec<crate::txn::TxnId>,
        execute_at: crate::timestamp::Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    ) {
        // Decode events from serialized data
        let events = SerializableEvent::decode_events(&events_data).ok();

        if let Some(ref events) = events {
            // Replicate events to all alive nodes (including coordinator)
            let node_ids: Vec<_> = self.node_ids.clone();
            for node_id in node_ids {
                if self.is_alive(node_id) {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    node.replicated_events.insert(txn_id, events.clone());
                }
            }
        }

        // Commit locally
        {
            let node = self.nodes.get(&coordinator_id).unwrap();
            let _ = self
                .runtime
                .block_on(async { node.state.commit(txn_id).await });
        }

        // Persist to durable storage for crash recovery
        {
            let node = self.nodes.get(&coordinator_id).unwrap();
            let txn = crate::txn::Transaction {
                id: txn_id,
                events_data: events_data.clone(),
                keys: keys.clone(),
                deps: deps.clone(),
                status: TxnStatus::Committed,
                execute_at,
                ballot: crate::txn::Ballot::default(),
            };
            let _ = self
                .runtime
                .block_on(async { node.durable.persist(&txn).await });
        }

        // Create and send Commit messages with serialized events
        let messages = {
            let node = self.nodes.get(&coordinator_id).unwrap();
            node.coordinator
                .create_commit_messages(txn_id, deps, execute_at, events_data, keys)
        };

        for (to, msg) in messages {
            self.send_message(coordinator_id, to, msg);
        }

        // Clean up coordinator state
        {
            let node = self.nodes.get_mut(&coordinator_id).unwrap();
            node.coordinator.complete_transaction(&txn_id);
        }
    }

    /// Send a message between nodes.
    pub fn send_message(&mut self, from: NodeId, to: NodeId, message: Message) {
        self.stats.messages_sent += 1;
        self.history
            .message_sent(from, self.clock.now(), to, message.type_name().to_string());
        self.network.send(from, to, message);
    }

    /// Crash a node.
    pub fn crash_node(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            if node.is_alive {
                node.is_alive = false;
                self.network.crash(id);
                self.stats.crashes += 1;
            }
        }
    }

    /// Restart a crashed node.
    ///
    /// Restores committed transactions from durable storage and syncs with peers.
    pub fn restart_node(&mut self, id: NodeId) {
        let node_ids = self.node_ids.clone();
        let durable_storage = &self.durable_storage;

        if let Some(node) = self.nodes.get_mut(&id) {
            if !node.is_alive {
                node.is_alive = true;

                // Create fresh state
                let new_state = Arc::new(ConsensusState::new());

                // Load committed transactions from durable storage
                let committed_txns = self
                    .runtime
                    .block_on(async { node.durable.load_committed().await.unwrap_or_default() });

                // Track known txn IDs for sync
                let mut known_txn_ids: Vec<crate::txn::TxnId> = Vec::new();

                // Restore each committed transaction
                for txn in &committed_txns {
                    known_txn_ids.push(txn.id);
                    let _ = self.runtime.block_on(async {
                        new_state
                            .create_from_commit(
                                txn.id,
                                txn.deps.clone(),
                                txn.execute_at,
                                txn.events_data.clone(),
                                txn.keys.clone(),
                            )
                            .await
                    });

                    // If transaction was executed, mark it as such
                    if txn.status == TxnStatus::Executed {
                        let _ = self
                            .runtime
                            .block_on(async { new_state.mark_executed(txn.id).await });
                    }
                }

                // Also restore events to executor and replicated_events for executed transactions
                for txn in committed_txns {
                    if let Ok(events) = SerializableEvent::decode_events(&txn.events_data) {
                        node.replicated_events.insert(txn.id, events.clone());

                        // If executed, write to executor
                        if txn.status == TxnStatus::Executed {
                            let _ = self
                                .runtime
                                .block_on(async { node.executor.write(events).await });
                        }
                    }
                }

                node.state = new_state.clone();

                // Reset coordinator but keep cluster info
                node.coordinator = SimCoordinator::new(id, node_ids.clone());

                // Create new durable store handle (same shared storage)
                node.durable = durable_storage.for_node(id);

                self.network.restart(id);
                self.stats.restarts += 1;

                // Sync with alive peers to catch up on missed transactions
                self.sync_with_peers(id, known_txn_ids, new_state);

                // Execute any ready transactions after sync
                self.execute_ready(id);
            }
        }
    }

    /// Sync a restarted node with alive peers.
    fn sync_with_peers(
        &mut self,
        id: NodeId,
        known_txn_ids: Vec<crate::txn::TxnId>,
        state: Arc<ConsensusState>,
    ) {
        use crate::protocol::messages::{Message, SyncRequest};
        use crate::timestamp::Timestamp;

        // Find an alive peer to sync from
        let peer_id = self
            .node_ids
            .iter()
            .find(|&&nid| nid != id && self.is_alive(nid))
            .copied();

        let peer_id = match peer_id {
            Some(pid) => pid,
            None => return, // No peers to sync from
        };

        // Create a sync request
        let request_id =
            crate::txn::TxnId::new(Timestamp::new(self.clock.now().as_millis() as u64, 0, id));
        let sync_req = SyncRequest {
            request_id,
            since: Timestamp::ZERO,
            known_txns: known_txn_ids,
        };

        // Get transactions from peer's state
        let peer_state = self.nodes.get(&peer_id).unwrap().state.clone();
        let response = self
            .runtime
            .block_on(async { crate::protocol::sync::handle(&peer_state, sync_req).await });

        // Apply the response
        if let Ok(Message::SyncResponse(resp)) = response {
            let applied = self.runtime.block_on(async {
                state
                    .apply_sync_transactions(resp.transactions.clone())
                    .await
            });

            if let Ok(count) = applied {
                if count > 0 {
                    // Also update durable storage, replicated events, and executor
                    let node = self.nodes.get_mut(&id).unwrap();
                    for txn in resp.transactions {
                        // Persist to durable storage
                        let _ = self
                            .runtime
                            .block_on(async { node.durable.persist(&txn).await });

                        // Decode and store replicated events, and write to executor if executed
                        if let Ok(events) = SerializableEvent::decode_events(&txn.events_data) {
                            node.replicated_events.insert(txn.id, events.clone());

                            // If the transaction was executed, write events to executor
                            if txn.status == TxnStatus::Executed {
                                let _ = self
                                    .runtime
                                    .block_on(async { node.executor.write(events).await });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Check if a node is alive.
    pub fn is_alive(&self, id: NodeId) -> bool {
        self.nodes.get(&id).map(|n| n.is_alive).unwrap_or(false)
    }

    /// Run anti-entropy sync across all alive nodes.
    ///
    /// Each node syncs with all other alive nodes to ensure consistency.
    /// This should be called periodically or before consistency checks.
    pub fn anti_entropy_sync(&mut self) {
        use crate::timestamp::Timestamp;

        let alive_nodes: Vec<_> = self
            .node_ids
            .iter()
            .filter(|&&id| self.is_alive(id))
            .copied()
            .collect();

        // Collect states for all alive nodes
        let node_states: Vec<_> = alive_nodes
            .iter()
            .map(|&id| (id, self.nodes.get(&id).unwrap().state.clone()))
            .collect();

        // Phase 1: Collect all transactions from all nodes in a single async block
        let all_transactions: Vec<crate::txn::Transaction> = self.runtime.block_on(async {
            let mut txn_map: std::collections::HashMap<crate::txn::TxnId, crate::txn::Transaction> =
                std::collections::HashMap::new();

            for (_node_id, state) in &node_states {
                let empty_set = std::collections::HashSet::new();
                let txns = state
                    .get_committed_for_sync(Timestamp::ZERO, &empty_set)
                    .await;
                for txn in txns {
                    let should_insert = match txn_map.get(&txn.id) {
                        None => true,
                        Some(existing) => txn.status > existing.status,
                    };
                    if should_insert {
                        txn_map.insert(txn.id, txn);
                    }
                }
            }

            txn_map.into_values().collect()
        });

        // Phase 2: Apply transactions to nodes that need them in a single async block
        // Collect all node states first (outside async block)
        let node_states_for_update: Vec<_> = alive_nodes
            .iter()
            .map(|&id| (id, self.nodes.get(&id).unwrap().state.clone()))
            .collect();
        let all_txns = all_transactions.clone();

        let node_updates: Vec<(NodeId, Vec<(crate::txn::Transaction, bool)>)> =
            self.runtime.block_on(async {
                let mut node_updates = Vec::new();

                for (node_id, node_state) in &node_states_for_update {
                    let mut updates = Vec::new();

                    for txn in &all_txns {
                        let existing_status = node_state.get_status(&txn.id).await;

                        let needs_events = match existing_status {
                            None => {
                                // Don't have this transaction - apply it
                                let _ = node_state
                                    .create_from_commit(
                                        txn.id,
                                        txn.deps.clone(),
                                        txn.execute_at,
                                        txn.events_data.clone(),
                                        txn.keys.clone(),
                                    )
                                    .await;

                                if txn.status == TxnStatus::Executed {
                                    let _ = node_state.mark_executed(txn.id).await;
                                }
                                txn.status == TxnStatus::Executed
                            }
                            Some(TxnStatus::PreAccepted) | Some(TxnStatus::Accepted) => {
                                // Upgrade to committed/executed
                                let _ = node_state
                                    .create_from_commit(
                                        txn.id,
                                        txn.deps.clone(),
                                        txn.execute_at,
                                        txn.events_data.clone(),
                                        txn.keys.clone(),
                                    )
                                    .await;

                                if txn.status == TxnStatus::Executed {
                                    let _ = node_state.mark_executed(txn.id).await;
                                }
                                txn.status == TxnStatus::Executed
                            }
                            Some(TxnStatus::Committed) if txn.status == TxnStatus::Executed => {
                                // Node has Committed, source has Executed - upgrade and write events
                                let _ = node_state.mark_executed(txn.id).await;
                                true
                            }
                            Some(TxnStatus::Committed) => {
                                // Both have Committed - no action needed, events will be written when executed
                                false
                            }
                            Some(TxnStatus::Executed) => {
                                // Already executed - events were already written, don't write again
                                false
                            }
                        };

                        updates.push((txn.clone(), needs_events));
                    }

                    node_updates.push((*node_id, updates));
                }

                node_updates
            });

        // Phase 3: Apply durable storage and prepare events for execution
        for (node_id, updates) in node_updates {
            let node = self.nodes.get_mut(&node_id).unwrap();

            for (txn, needs_events) in updates {
                // Persist to durable storage
                let _ = self
                    .runtime
                    .block_on(async { node.durable.persist(&txn).await });

                if let Ok(events) = SerializableEvent::decode_events(&txn.events_data) {
                    if needs_events {
                        // Transaction is Executed - write events directly to executor
                        // Don't store in replicated_events since they're being written now
                        let _ = self
                            .runtime
                            .block_on(async { node.executor.write(events).await });
                    } else {
                        // Transaction is Committed but not yet Executed
                        // Store events for later execution in Phase 4
                        node.replicated_events.insert(txn.id, events);
                    }
                }
            }
        }

        // Phase 4: Execute ready transactions on all nodes
        for &node_id in &alive_nodes {
            self.execute_ready(node_id);
        }
    }

    /// Create a network partition between two nodes.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.network.partition(a, b);
        self.stats.partitions_created += 1;
    }

    /// Heal a network partition.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.network.heal(a, b);
        self.stats.partitions_healed += 1;
    }

    /// Heal all partitions.
    pub fn heal_all(&mut self) {
        let count = self.network.partitions().len();
        self.network.heal_all();
        self.stats.partitions_healed += count as u64;
    }

    /// Isolate a node from all others.
    pub fn isolate(&mut self, node: NodeId) {
        self.network.isolate(node, &self.node_ids);
        self.stats.partitions_created += (self.node_ids.len() - 1) as u64;
    }

    /// Get a node by ID.
    pub fn node(&self, id: NodeId) -> Option<&SimNode> {
        self.nodes.get(&id)
    }

    /// Get mutable reference to a node.
    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut SimNode> {
        self.nodes.get_mut(&id)
    }

    /// Get all node IDs.
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Get count of alive nodes.
    pub fn alive_count(&self) -> usize {
        self.nodes.values().filter(|n| n.is_alive).count()
    }

    /// Get simulation statistics.
    pub fn stats(&self) -> &SimStats {
        &self.stats
    }

    /// Get operation history.
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Get mutable reference to history.
    pub fn history_mut(&mut self) -> &mut History {
        &mut self.history
    }

    /// Get all executors for consistency checking.
    pub fn executors(&self) -> Vec<SimExecutor> {
        self.nodes.values().map(|n| n.executor.clone()).collect()
    }

    /// Check linearizability of the operation history.
    pub fn check_linearizability(&self) -> CheckResult {
        // Build expected state from first alive node
        let executor = self
            .nodes
            .values()
            .find(|n| n.is_alive)
            .map(|n| &n.executor);

        match executor {
            Some(exec) => {
                let checker = LinearizabilityChecker::from_executor(exec);
                checker.check(&self.history)
            }
            None => CheckResult::pass(), // No alive nodes, trivially true
        }
    }

    /// Check consistency across all nodes.
    pub fn check_consistency(&self) -> CheckResult {
        let executors: Vec<_> = self
            .nodes
            .values()
            .filter(|n| n.is_alive)
            .map(|n| n.executor.clone())
            .collect();

        InvariantChecker::check_consistency(&executors)
    }

    /// Check that no operations are stuck.
    pub fn check_no_deadlock(&self) -> CheckResult {
        InvariantChecker::check_no_deadlock(&self.history)
    }

    /// Get network statistics.
    pub fn network_stats(&self) -> &crate::sim::network::NetworkStats {
        self.network.stats()
    }

    /// Submit a transaction through ACCORD protocol.
    ///
    /// This initiates the full ACCORD protocol:
    /// 1. PreAccept at coordinator + broadcast to peers
    /// 2. Accept (if needed for slow path)
    /// 3. Commit broadcast
    /// 4. Execution after dependencies satisfied
    ///
    /// Returns the transaction ID if successful.
    pub fn submit_transaction(
        &mut self,
        coordinator_id: NodeId,
        events: Vec<Event>,
    ) -> Option<crate::txn::TxnId> {
        // Check if coordinator is alive
        if !self.is_alive(coordinator_id) {
            return None;
        }

        // Get timestamp from coordinator's clock
        let timestamp = {
            let node = self.nodes.get(&coordinator_id)?;
            node.clock.now()
        };

        // Start the transaction at coordinator
        let (txn, messages) = {
            let node = self.nodes.get_mut(&coordinator_id)?;
            match node
                .coordinator
                .start_transaction(&node.state, events.clone(), timestamp)
            {
                Ok((txn, msgs)) => (txn, msgs),
                Err(_) => return None,
            }
        };

        let txn_id = txn.id;

        // Send PreAccept messages through virtual network
        for (to, msg) in messages {
            self.send_message(coordinator_id, to, msg);
        }

        Some(txn_id)
    }

    /// Run until a transaction reaches a given status or max ticks.
    pub fn run_until_status(
        &mut self,
        node_id: NodeId,
        txn_id: crate::txn::TxnId,
        target_status: TxnStatus,
        max_ticks: u64,
    ) -> bool {
        for _ in 0..max_ticks {
            if let Some(node) = self.nodes.get(&node_id) {
                let status = self
                    .runtime
                    .block_on(async { node.state.get_status(&txn_id).await });
                if status == Some(target_status) {
                    return true;
                }
            }
            self.tick();
        }
        false
    }

    /// Execute all ready transactions on a node.
    pub fn execute_ready(&mut self, node_id: NodeId) {
        // Get ready transactions
        let ready = {
            let node = match self.nodes.get(&node_id) {
                Some(n) if n.is_alive => n,
                _ => return,
            };
            self.runtime
                .block_on(async { node.state.get_ready_transactions().await })
        };

        for (txn_id, _execute_at) in ready {
            // Get replicated events for this transaction
            let events = {
                let node = self.nodes.get_mut(&node_id).unwrap();
                node.replicated_events.remove(&txn_id)
            };

            if let Some(events) = events {
                // Write events to executor
                let node = self.nodes.get_mut(&node_id).unwrap();
                let _ = self
                    .runtime
                    .block_on(async { node.executor.write(events).await });
            }

            // Mark as executed in consensus state
            let node = self.nodes.get(&node_id).unwrap();
            let _ = self
                .runtime
                .block_on(async { node.state.mark_executed(txn_id).await });

            // Persist executed status to durable storage
            let _ = self
                .runtime
                .block_on(async { node.durable.mark_executed(txn_id).await });
        }
    }

    /// Execute ready transactions on all alive nodes.
    pub fn execute_all_ready(&mut self) {
        let node_ids: Vec<_> = self.node_ids.clone();
        for node_id in node_ids {
            self.execute_ready(node_id);
        }
    }
}

/// Builder for creating simulations.
#[derive(Default)]
pub struct SimulationBuilder {
    seed: u64,
    node_count: usize,
    fault_config: FaultConfig,
}

impl SimulationBuilder {
    /// Set the random seed for deterministic execution.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Set the number of nodes in the cluster.
    pub fn nodes(mut self, count: usize) -> Self {
        self.node_count = count;
        self
    }

    /// Set the fault injection configuration.
    pub fn faults(mut self, config: FaultConfig) -> Self {
        self.fault_config = config;
        self
    }

    /// Build the simulation.
    pub fn build(self) -> Simulation {
        let clock = VirtualClock::new();
        let rng = SimRng::new(self.seed);
        let network = VirtualNetwork::new(clock.clone(), rng.fork(), self.fault_config.clone());
        let durable_storage = SharedDurableStorage::new();

        let mut nodes = HashMap::new();
        let mut node_ids: Vec<NodeId> = Vec::new();

        // First collect all node IDs
        for i in 0..self.node_count {
            node_ids.push(i as NodeId);
        }

        // Then create nodes with full cluster info
        for i in 0..self.node_count {
            let id = i as NodeId;
            nodes.insert(id, SimNode::new(id, node_ids.clone(), &durable_storage));
        }

        // Create a tokio runtime for async operations
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        Simulation {
            clock,
            rng,
            network,
            nodes,
            node_ids,
            history: History::new(),
            fault_config: self.fault_config,
            stats: SimStats::default(),
            seed: self.seed,
            durable_storage,
            runtime,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let sim = Simulation::builder()
            .seed(42)
            .nodes(5)
            .faults(FaultConfig::none())
            .build();

        assert_eq!(sim.seed(), 42);
        assert_eq!(sim.node_ids().len(), 5);
        assert_eq!(sim.alive_count(), 5);
    }

    #[test]
    fn test_tick_advances_time() {
        let mut sim = Simulation::builder().seed(42).nodes(3).build();

        assert_eq!(sim.now(), Duration::ZERO);

        sim.tick();
        assert_eq!(sim.now(), Duration::from_millis(1));

        sim.run(99);
        assert_eq!(sim.now(), Duration::from_millis(100));
    }

    #[test]
    fn test_crash_and_restart() {
        let mut sim = Simulation::builder().seed(42).nodes(3).build();

        assert!(sim.is_alive(0));

        sim.crash_node(0);
        assert!(!sim.is_alive(0));
        assert_eq!(sim.alive_count(), 2);
        assert_eq!(sim.stats().crashes, 1);

        sim.restart_node(0);
        assert!(sim.is_alive(0));
        assert_eq!(sim.alive_count(), 3);
        assert_eq!(sim.stats().restarts, 1);
    }

    #[test]
    fn test_partition_and_heal() {
        let mut sim = Simulation::builder().seed(42).nodes(3).build();

        sim.partition(0, 1);
        assert_eq!(sim.stats().partitions_created, 1);

        sim.heal(0, 1);
        assert_eq!(sim.stats().partitions_healed, 1);
    }

    #[test]
    fn test_isolate() {
        let mut sim = Simulation::builder().seed(42).nodes(5).build();

        sim.isolate(2);

        // Node 2 should be partitioned from all others
        assert_eq!(sim.stats().partitions_created, 4);
    }

    #[test]
    fn test_deterministic_replay() {
        // Two simulations with same seed should behave identically
        let mut sim1 = Simulation::builder()
            .seed(12345)
            .nodes(3)
            .faults(FaultConfig::medium())
            .build();

        let mut sim2 = Simulation::builder()
            .seed(12345)
            .nodes(3)
            .faults(FaultConfig::medium())
            .build();

        sim1.run(1000);
        sim2.run(1000);

        // Same stats
        assert_eq!(sim1.stats().crashes, sim2.stats().crashes);
        assert_eq!(sim1.stats().restarts, sim2.stats().restarts);
        assert_eq!(
            sim1.stats().partitions_created,
            sim2.stats().partitions_created
        );
    }

    #[test]
    fn test_check_no_deadlock_passes() {
        let sim = Simulation::builder().seed(42).nodes(3).build();

        let result = sim.check_no_deadlock();
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_consistency_empty() {
        let sim = Simulation::builder().seed(42).nodes(3).build();

        let result = sim.check_consistency();
        assert!(result.is_ok());
    }
}
