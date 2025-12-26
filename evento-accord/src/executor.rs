//! AccordExecutor - ACCORD consensus wrapper for any Executor.

use crate::config::AccordConfig;
use crate::error::Result;
use crate::keys::Key;
use crate::protocol::execute::{execute_raw, ExecuteConfig};
use crate::protocol::{self, Message};
use crate::state::{ConsensusState, DurableStore};
use crate::timestamp::HybridClock;
use crate::transport::{TcpServer, TcpTransport, Transport};
use crate::txn::{Ballot, SerializableEvent, Transaction, TxnId, TxnStatus};
use async_trait::async_trait;
use evento_core::cursor::{Args, ReadResult, Value};
use evento_core::{Event, Executor, ReadAggregator, RoutingKey, WriteError};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// Handle for graceful shutdown of the AccordExecutor.
///
/// When dropped or when `shutdown()` is called, all background tasks
/// will be signaled to stop and the executor will wait for them to complete.
pub struct ShutdownHandle {
    cancel_token: CancellationToken,
    task_handles: Vec<JoinHandle<()>>,
}

impl ShutdownHandle {
    /// Create a new shutdown handle.
    fn new() -> (Self, CancellationToken) {
        let cancel_token = CancellationToken::new();
        (
            Self {
                cancel_token: cancel_token.clone(),
                task_handles: Vec::new(),
            },
            cancel_token,
        )
    }

    /// Add a task handle to be tracked for shutdown.
    fn add_task(&mut self, handle: JoinHandle<()>) {
        self.task_handles.push(handle);
    }

    /// Initiate graceful shutdown.
    ///
    /// Signals all background tasks to stop and waits for them to complete.
    /// Returns after all tasks have terminated or after the timeout.
    pub async fn shutdown(self) {
        self.shutdown_with_timeout(Duration::from_secs(30)).await
    }

    /// Initiate graceful shutdown with a custom timeout.
    pub async fn shutdown_with_timeout(self, timeout: Duration) {
        tracing::info!("Initiating graceful shutdown...");

        // Signal all tasks to stop
        self.cancel_token.cancel();

        // Wait for all tasks to complete with timeout
        let shutdown_future = async {
            for handle in self.task_handles {
                let _ = handle.await;
            }
        };

        match tokio::time::timeout(timeout, shutdown_future).await {
            Ok(()) => {
                tracing::info!("Graceful shutdown completed");
            }
            Err(_) => {
                tracing::warn!("Shutdown timed out after {:?}", timeout);
            }
        }
    }

    /// Check if shutdown has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}

/// ACCORD consensus wrapper for any Executor.
///
/// This executor wraps an underlying executor and adds distributed consensus
/// using the ACCORD protocol. It can operate in two modes:
///
/// - **Single-node**: Bypasses consensus, writes go directly to the underlying executor
/// - **Cluster**: Runs the full ACCORD protocol for write coordination
///
/// # Example
///
/// ```ignore
/// // Single-node mode (development)
/// let accord = AccordExecutor::single_node(sql_executor);
///
/// // Cluster mode (production)
/// let config = AccordConfig::cluster(local_addr, cluster_nodes);
/// let accord = AccordExecutor::cluster(sql_executor, config).await?;
/// ```
pub struct AccordExecutor<E: Executor> {
    inner: E,
    config: AccordConfig,
    state: Arc<ConsensusState>,
    clock: Arc<HybridClock>,
    mode: ExecutorMode,
}

impl<E: Executor + Clone> Clone for AccordExecutor<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            clock: self.clock.clone(),
            mode: self.mode.clone(),
        }
    }
}

#[derive(Clone)]
enum ExecutorMode {
    /// Single node: bypass consensus, write directly.
    SingleNode,
    /// Cluster: run full ACCORD protocol.
    Cluster { transport: Arc<TcpTransport> },
}

impl<E: Executor + Clone + Send + Sync + 'static> AccordExecutor<E> {
    /// Create a single-node executor (no consensus overhead).
    ///
    /// In this mode, all writes bypass the ACCORD protocol and go directly
    /// to the underlying executor. This is useful for development and testing.
    pub fn single_node(executor: E) -> Self {
        Self {
            inner: executor,
            config: AccordConfig::single_node(),
            state: Arc::new(ConsensusState::new()),
            clock: Arc::new(HybridClock::new(0)),
            mode: ExecutorMode::SingleNode,
        }
    }

    /// Create a clustered executor with full ACCORD consensus.
    ///
    /// This starts the TCP server for handling incoming protocol messages
    /// and spawns background execution workers.
    ///
    /// Returns the executor and a `ShutdownHandle` that can be used to
    /// gracefully shut down all background tasks.
    pub async fn cluster(executor: E, config: AccordConfig) -> Result<(Self, ShutdownHandle)> {
        Self::cluster_with_durable(executor, config, None::<crate::state::MemoryDurableStore>).await
    }

    /// Create a clustered executor with full ACCORD consensus and a durable store.
    ///
    /// The durable store is used to persist executed transaction IDs for crash recovery.
    /// When a node restarts, it can load the executed IDs and skip re-executing them.
    ///
    /// # Arguments
    ///
    /// * `executor` - The underlying executor for writing events
    /// * `config` - Cluster configuration
    /// * `durable` - Optional durable store for crash recovery (use `SqlDurableStore` for production)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use evento_accord::{AccordExecutor, AccordConfig, SqlDurableStore, load_executed_txn_ids};
    ///
    /// // Create SQL durable store from the same pool as the executor
    /// let durable_store = SqlDurableStore::new(pool.clone());
    ///
    /// // Load executed transaction IDs from previous runs
    /// let executed_ids = load_executed_txn_ids(&pool).await?;
    ///
    /// // Create executor with durable store
    /// let (executor, shutdown) = AccordExecutor::cluster_with_durable(
    ///     inner_executor,
    ///     config,
    ///     Some(durable_store),
    /// ).await?;
    ///
    /// // Restore executed transactions (skip re-execution)
    /// executor.restore_executed(executed_ids).await;
    /// ```
    pub async fn cluster_with_durable<D: DurableStore + 'static>(
        executor: E,
        config: AccordConfig,
        durable: Option<D>,
    ) -> Result<(Self, ShutdownHandle)> {
        let clock = Arc::new(HybridClock::new(config.local.id));
        let state = Arc::new(ConsensusState::new());
        let peers = config.peers();

        let transport = Arc::new(TcpTransport::new(config.local.id, peers));

        // Wrap durable store in Arc for sharing across workers
        let durable: Option<Arc<dyn DurableStore>> = durable.map(|d| Arc::new(d) as Arc<dyn DurableStore>);

        // Create shutdown handle
        let (mut shutdown_handle, cancel_token) = ShutdownHandle::new();

        // Start TCP server
        let server = TcpServer::bind(&config.local).await?;
        let state_clone = state.clone();
        let server_cancel = cancel_token.clone();
        let server_handle = tokio::spawn(async move {
            tokio::select! {
                _ = server_cancel.cancelled() => {
                    tracing::debug!("TCP server shutting down");
                }
                result = server.run(move |msg| {
                    let state = state_clone.clone();
                    async move { protocol::handle_message(&state, msg).await }
                }) => {
                    if let Err(e) = result {
                        tracing::error!("TCP server error: {}", e);
                    }
                }
            }
        });
        shutdown_handle.add_task(server_handle);

        // Start execution workers
        let exec_config = ExecuteConfig {
            dependency_timeout: config.timeouts.dependency_wait,
            poll_interval: config.execution.poll_interval,
        };
        let worker_handles = Self::start_execution_workers(
            state.clone(),
            executor.clone(),
            config.execution.workers,
            exec_config,
            cancel_token,
            durable,
            Some(transport.clone()),
            config.local.id,
        );
        for handle in worker_handles {
            shutdown_handle.add_task(handle);
        }

        let accord_executor = Self {
            inner: executor,
            config,
            state,
            clock,
            mode: ExecutorMode::Cluster { transport },
        };

        Ok((accord_executor, shutdown_handle))
    }

    /// Start a background recovery task that periodically checks for and resolves
    /// stuck transactions with missing dependencies.
    ///
    /// Returns a join handle for the background task.
    pub fn start_recovery_task(
        self: &Arc<Self>,
        interval: std::time::Duration,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()>
    where
        E: 'static,
    {
        let executor = Arc::clone(self);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::debug!("Recovery task shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        // Check for and recover missing dependencies
                        match executor.recover_missing_dependencies().await {
                            Ok(recovered) if recovered > 0 => {
                                tracing::debug!("Recovery task resolved {} dependencies", recovered);
                            }
                            Ok(_) => {
                                // No missing dependencies, nothing to do
                            }
                            Err(e) => {
                                tracing::warn!("Recovery task error: {}", e);
                            }
                        }
                    }
                }
            }
        })
    }

    /// Restore executed transaction IDs from durable storage.
    ///
    /// Call this after creating the executor to skip re-executing transactions
    /// that were already executed before a crash/restart.
    ///
    /// Returns the number of transactions restored.
    pub async fn restore_executed(&self, txn_ids: Vec<TxnId>) -> usize {
        self.state.restore_executed(txn_ids).await
    }

    /// Sync with peers to catch up on transactions missed while down.
    ///
    /// This should be called after node startup to recover any transactions
    /// that were committed on other nodes while this node was offline.
    ///
    /// # Arguments
    ///
    /// * `known_txn_ids` - Transaction IDs already executed locally (from durable storage)
    ///
    /// Returns the total number of transactions synced from all peers.
    pub async fn sync_with_peers(&self, known_txn_ids: Vec<TxnId>) -> Result<usize> {
        let transport = match &self.mode {
            ExecutorMode::Cluster { transport } => transport.clone(),
            ExecutorMode::SingleNode => return Ok(0),
        };

        let local_node = self.config.local.id;
        tracing::info!("Node {} starting sync with peers", local_node);
        tracing::debug!(
            "Node {} has {} known executed transactions",
            local_node,
            known_txn_ids.len()
        );

        // Create a unique request ID
        let request_id = TxnId::new(self.clock.now());

        // Create sync request
        let sync_msg = protocol::sync::create_request(
            request_id,
            crate::timestamp::Timestamp::ZERO, // Get all committed transactions
            known_txn_ids,
        );

        // Send to all peers
        let responses = transport.broadcast(&sync_msg).await;

        let mut total_synced = 0;

        for response in responses {
            match response {
                Ok(Message::SyncResponse(resp)) => {
                    let txn_count = resp.transactions.len();
                    if txn_count > 0 {
                        tracing::info!(
                            "Node {} received {} transactions from peer",
                            local_node,
                            txn_count
                        );
                        match protocol::sync::apply_response(&self.state, resp).await {
                            Ok(applied) => {
                                total_synced += applied;
                            }
                            Err(e) => {
                                tracing::error!("Failed to apply sync response: {}", e);
                            }
                        }
                    }
                }
                Ok(other) => {
                    tracing::warn!("Unexpected response to sync request: {:?}", other.type_name());
                }
                Err(e) => {
                    tracing::warn!("Sync request failed: {}", e);
                }
            }
        }

        tracing::info!(
            "Node {} sync completed, {} transactions recovered",
            local_node,
            total_synced
        );

        Ok(total_synced)
    }

    /// Recover missing dependencies by querying peers.
    ///
    /// When transactions are blocked on dependencies that don't exist locally,
    /// this method queries peers to find them. If a dependency was committed
    /// on another node, it's applied locally. If no peer has it, the dependency
    /// is marked as abandoned so blocked transactions can proceed.
    ///
    /// Returns the number of dependencies recovered or abandoned.
    pub async fn recover_missing_dependencies(&self) -> Result<usize> {
        let transport = match &self.mode {
            ExecutorMode::Cluster { transport } => transport.clone(),
            ExecutorMode::SingleNode => return Ok(0),
        };

        let local_node = self.config.local.id;

        // Get missing dependencies
        let missing_deps = self.state.get_missing_dependencies().await;

        if missing_deps.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            "Node {} recovering {} missing dependencies",
            local_node,
            missing_deps.len()
        );

        let mut recovered = 0;

        for dep_id in missing_deps {
            // Query peers for this transaction
            let recover_msg = protocol::recover::create_request(
                dep_id,
                crate::txn::Ballot::initial(local_node),
            );

            let responses = transport.broadcast(&recover_msg).await;
            let result = protocol::recover::coordinate(responses, dep_id).await;

            match result {
                protocol::RecoveryResult::Recovered {
                    txn_id,
                    deps,
                    execute_at,
                    events_data,
                    keys,
                } => {
                    tracing::info!("Recovered transaction {} from peer", txn_id);
                    if let Err(e) = self
                        .state
                        .apply_recovered_transaction(txn_id, deps, execute_at, events_data, keys)
                        .await
                    {
                        tracing::warn!("Failed to apply recovered transaction {}: {}", txn_id, e);
                    } else {
                        recovered += 1;
                    }
                }
                protocol::RecoveryResult::NotFound => {
                    // No peer has this transaction - mark it as abandoned
                    tracing::info!(
                        "Transaction {} not found on any peer, marking as abandoned",
                        dep_id
                    );
                    let unblocked = self.state.mark_dependency_abandoned(dep_id).await;
                    if !unblocked.is_empty() {
                        tracing::info!(
                            "Unblocked {} transactions after abandoning {}",
                            unblocked.len(),
                            dep_id
                        );
                    }
                    recovered += 1;
                }
                protocol::RecoveryResult::Failed => {
                    tracing::warn!("Failed to recover transaction {} (no peer response)", dep_id);
                }
            }
        }

        tracing::info!(
            "Node {} recovery completed, {} dependencies resolved",
            local_node,
            recovered
        );

        Ok(recovered)
    }

    /// Recover a single missing dependency by querying peers.
    ///
    /// Returns true if the dependency was recovered or abandoned, false otherwise.
    pub async fn recover_single_dependency(&self, dep_id: TxnId) -> Result<bool> {
        let transport = match &self.mode {
            ExecutorMode::Cluster { transport } => transport.clone(),
            ExecutorMode::SingleNode => return Ok(false),
        };

        let local_node = self.config.local.id;

        tracing::debug!("Node {} recovering single dependency {}", local_node, dep_id);

        // Query peers for this transaction
        let recover_msg =
            protocol::recover::create_request(dep_id, crate::txn::Ballot::initial(local_node));

        let responses = transport.broadcast(&recover_msg).await;
        let result = protocol::recover::coordinate(responses, dep_id).await;

        match result {
            protocol::RecoveryResult::Recovered {
                txn_id,
                deps,
                execute_at,
                events_data,
                keys,
            } => {
                tracing::info!("Recovered transaction {} from peer", txn_id);
                if let Err(e) = self
                    .state
                    .apply_recovered_transaction(txn_id, deps, execute_at, events_data, keys)
                    .await
                {
                    tracing::warn!("Failed to apply recovered transaction {}: {}", txn_id, e);
                    return Ok(false);
                }
                Ok(true)
            }
            protocol::RecoveryResult::NotFound => {
                // No peer has this transaction - mark it as abandoned
                tracing::info!(
                    "Transaction {} not found on any peer, marking as abandoned",
                    dep_id
                );
                let unblocked = self.state.mark_dependency_abandoned(dep_id).await;
                if !unblocked.is_empty() {
                    tracing::info!(
                        "Unblocked {} transactions after abandoning {}",
                        unblocked.len(),
                        dep_id
                    );
                }
                Ok(true)
            }
            protocol::RecoveryResult::Failed => {
                tracing::warn!("Failed to recover transaction {} (no peer response)", dep_id);
                Ok(false)
            }
        }
    }

    /// Start background execution worker tasks.
    ///
    /// Returns handles to all spawned worker tasks.
    fn start_execution_workers(
        state: Arc<ConsensusState>,
        executor: E,
        count: usize,
        config: ExecuteConfig,
        cancel_token: CancellationToken,
        durable: Option<Arc<dyn DurableStore>>,
        transport: Option<Arc<TcpTransport>>,
        local_node: u16,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::with_capacity(count);

        for worker_id in 0..count {
            let state = state.clone();
            let executor = executor.clone();
            let config = config.clone();
            let cancel = cancel_token.clone();
            let durable = durable.clone();
            let transport = transport.clone();

            let handle = tokio::spawn(async move {
                tracing::debug!("Execution worker {} started", worker_id);

                loop {
                    // Check for shutdown
                    if cancel.is_cancelled() {
                        tracing::debug!("Execution worker {} shutting down", worker_id);
                        break;
                    }

                    // Try to get next executable transaction
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            tracing::debug!("Execution worker {} received shutdown signal", worker_id);
                            break;
                        }
                        txn_opt = state.next_executable() => {
                            if let Some(txn) = txn_opt {
                                // Decode events from transaction
                                let events = SerializableEvent::decode_events(&txn.events_data)
                                    .unwrap_or_default();

                                let txn_id = txn.id;
                                match execute_raw(&state, &executor, events, txn_id, &config).await {
                                    Ok(()) => {
                                        // Persist executed transaction ID to durable storage
                                        if let Some(ref durable) = durable {
                                            if let Err(e) = durable.mark_executed(txn_id).await {
                                                tracing::warn!(
                                                    "Failed to persist executed txn {} to durable store: {}",
                                                    txn_id, e
                                                );
                                            }
                                        }
                                    }
                                    Err(crate::error::AccordError::TxnNotFound(missing_dep_id)) => {
                                        // Missing dependency - try to recover it
                                        tracing::warn!(
                                            "Execution of {} blocked on missing dependency {}, attempting recovery",
                                            txn_id, missing_dep_id
                                        );

                                        if let Some(ref transport) = transport {
                                            // Try to recover the missing dependency
                                            let recover_msg = protocol::recover::create_request(
                                                missing_dep_id,
                                                Ballot::initial(local_node),
                                            );

                                            let responses = transport.broadcast(&recover_msg).await;
                                            let result = protocol::recover::coordinate(responses, missing_dep_id).await;

                                            match result {
                                                protocol::RecoveryResult::Recovered {
                                                    txn_id: recovered_id,
                                                    deps,
                                                    execute_at,
                                                    events_data,
                                                    keys,
                                                } => {
                                                    tracing::info!("Recovered dependency {} from peer", recovered_id);
                                                    if let Err(e) = state
                                                        .apply_recovered_transaction(recovered_id, deps, execute_at, events_data, keys)
                                                        .await
                                                    {
                                                        tracing::warn!("Failed to apply recovered transaction {}: {}", recovered_id, e);
                                                    }
                                                    // Re-queue the original transaction by clearing its in-progress status
                                                    state.requeue_transaction(txn_id).await;
                                                }
                                                protocol::RecoveryResult::NotFound => {
                                                    // Dependency doesn't exist anywhere - mark as abandoned
                                                    tracing::info!(
                                                        "Dependency {} not found on any peer, marking as abandoned",
                                                        missing_dep_id
                                                    );
                                                    let unblocked = state.mark_dependency_abandoned(missing_dep_id).await;
                                                    if !unblocked.is_empty() {
                                                        tracing::info!(
                                                            "Unblocked {} transactions after abandoning {}",
                                                            unblocked.len(),
                                                            missing_dep_id
                                                        );
                                                    }
                                                    // Re-queue the original transaction
                                                    state.requeue_transaction(txn_id).await;
                                                }
                                                protocol::RecoveryResult::Failed => {
                                                    tracing::warn!(
                                                        "Failed to recover dependency {} (no peer response), will retry later",
                                                        missing_dep_id
                                                    );
                                                    // Re-queue the transaction to try again later
                                                    state.requeue_transaction(txn_id).await;
                                                }
                                            }
                                        } else {
                                            // No transport available (shouldn't happen in cluster mode)
                                            tracing::error!("Execution failed for {}: missing dependency {} and no transport for recovery", txn_id, missing_dep_id);
                                            if let Err(mark_err) = state.mark_execution_failed(txn_id, format!("missing dependency {}", missing_dep_id)).await {
                                                tracing::error!("Failed to mark {} as failed: {}", txn_id, mark_err);
                                            }
                                        }
                                    }
                                    Err(crate::error::AccordError::Storage(evento_core::WriteError::InvalidOriginalVersion)) => {
                                        // Version conflict means events were already written before crash
                                        // Treat as successfully executed
                                        tracing::info!(
                                            "Transaction {} already executed (version conflict), marking as executed",
                                            txn_id
                                        );
                                        if let Err(e) = state.mark_executed(txn_id).await {
                                            tracing::warn!("Failed to mark {} as executed: {}", txn_id, e);
                                        }
                                        // Persist to durable storage
                                        if let Some(ref durable) = durable {
                                            if let Err(e) = durable.mark_executed(txn_id).await {
                                                tracing::warn!(
                                                    "Failed to persist executed txn {} to durable store: {}",
                                                    txn_id, e
                                                );
                                            }
                                        }
                                    }
                                    Err(crate::error::AccordError::Timeout(ref msg)) => {
                                        // Timeout waiting for a dependency - trigger recovery and retry
                                        tracing::warn!(
                                            "Execution of {} timed out ({}), triggering recovery",
                                            txn_id, msg
                                        );

                                        if let Some(ref transport) = transport {
                                            // Get missing dependencies and try to recover them
                                            let missing_deps = state.get_missing_dependencies().await;
                                            for dep_id in missing_deps {
                                                let recover_msg = protocol::recover::create_request(
                                                    dep_id,
                                                    Ballot::initial(local_node),
                                                );

                                                let responses = transport.broadcast(&recover_msg).await;
                                                let result = protocol::recover::coordinate(responses, dep_id).await;

                                                match result {
                                                    protocol::RecoveryResult::Recovered {
                                                        txn_id: recovered_id,
                                                        deps,
                                                        execute_at,
                                                        events_data,
                                                        keys,
                                                    } => {
                                                        tracing::info!("Recovered dependency {} from peer", recovered_id);
                                                        let _ = state
                                                            .apply_recovered_transaction(recovered_id, deps, execute_at, events_data, keys)
                                                            .await;
                                                    }
                                                    protocol::RecoveryResult::NotFound => {
                                                        tracing::info!(
                                                            "Dependency {} not found on any peer, marking as abandoned",
                                                            dep_id
                                                        );
                                                        state.mark_dependency_abandoned(dep_id).await;
                                                    }
                                                    protocol::RecoveryResult::Failed => {
                                                        tracing::warn!("Failed to recover dependency {}", dep_id);
                                                    }
                                                }
                                            }
                                        }
                                        // Re-queue the transaction to retry
                                        state.requeue_transaction(txn_id).await;
                                    }
                                    Err(e) => {
                                        tracing::error!("Execution failed for {}: {}", txn_id, e);
                                        // Mark the transaction as failed so callers know
                                        if let Err(mark_err) = state.mark_execution_failed(txn_id, e.to_string()).await {
                                            tracing::error!("Failed to mark {} as failed: {}", txn_id, mark_err);
                                        }
                                    }
                                }
                            } else {
                                // No work available, sleep briefly
                                tokio::time::sleep(config.poll_interval).await;
                            }
                        }
                    }
                }

                tracing::debug!("Execution worker {} stopped", worker_id);
            });

            handles.push(handle);
        }

        handles
    }

    /// Run the ACCORD protocol for a write operation.
    async fn run_protocol(&self, events: Vec<Event>) -> Result<()> {
        let transport = match &self.mode {
            ExecutorMode::Cluster { transport } => transport.clone(),
            _ => {
                return Err(crate::error::AccordError::Internal(anyhow::anyhow!(
                    "run_protocol called in non-cluster mode"
                )))
            }
        };

        let local_node = self.config.local.id;
        tracing::debug!("Node {} coordinating new transaction", local_node);

        // Extract keys for conflict detection
        let keys = Key::from_events(&events);

        // Serialize events for replication to other nodes
        let events_data = SerializableEvent::encode_events(&events);

        // Create transaction
        let ts = self.clock.now();
        let txn = Transaction {
            id: TxnId::new(ts),
            events_data,
            keys,
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(self.config.local.id),
        };

        let txn_id = txn.id;

        // Create PreAccept request
        let preaccept_msg = crate::protocol::messages::PreAcceptRequest {
            txn: txn.clone(),
            ballot: txn.ballot,
        };

        // Broadcast PreAccept to peers and collect responses
        let preaccept_responses = transport
            .broadcast(&Message::PreAccept(preaccept_msg))
            .await;

        // Run the full protocol (PreAccept coordination -> Accept if needed -> Commit)
        let transport_clone = transport.clone();
        let result = protocol::run_protocol(
            &self.state,
            txn,
            preaccept_responses,
            |msg| {
                // Accept broadcast (async)
                tracing::debug!("Node {} broadcasting Accept to peers", local_node);
                async move { transport_clone.broadcast(&msg).await }
            },
            |msg| {
                // Commit broadcast (fire and forget)
                tracing::debug!("Node {} broadcasting Commit to peers", local_node);
                transport.broadcast_no_wait(msg);
            },
            self.config.quorum_size(),
        )
        .await?;

        tracing::debug!(
            "Transaction {} committed (fast_path={})",
            result.txn_id,
            result.fast_path
        );

        // Wait for execution
        self.wait_for_execution(txn_id).await?;

        Ok(())
    }

    /// Wait for a transaction to be executed.
    async fn wait_for_execution(&self, txn_id: TxnId) -> Result<()> {
        let timeout = self.config.timeouts.dependency_wait;
        let poll_interval = self.config.execution.poll_interval;

        let deadline = tokio::time::Instant::now() + timeout;
        let recovery_threshold = tokio::time::Instant::now() + (timeout / 2);
        let mut recovery_attempted = false;

        loop {
            match self.state.get_status(&txn_id).await {
                Some(TxnStatus::Executed) => return Ok(()),
                Some(TxnStatus::ExecutionFailed) => {
                    // Retrieve the error message
                    let error_msg = self
                        .state
                        .get_execution_error(&txn_id)
                        .await
                        .unwrap_or_else(|| "unknown execution error".to_string());
                    return Err(crate::error::AccordError::ExecutionFailed {
                        txn_id,
                        error: error_msg,
                    });
                }
                Some(_) => {
                    let now = tokio::time::Instant::now();

                    // If we've been waiting for half the timeout, try recovery
                    if !recovery_attempted && now > recovery_threshold {
                        recovery_attempted = true;
                        tracing::debug!(
                            "Transaction {} still waiting for execution, triggering recovery",
                            txn_id
                        );
                        // Try to recover any missing dependencies
                        if let Err(e) = self.recover_missing_dependencies().await {
                            tracing::warn!("Recovery during wait failed: {}", e);
                        }
                    }

                    if now > deadline {
                        return Err(crate::error::AccordError::Timeout(format!(
                            "waiting for execution of {}",
                            txn_id
                        )));
                    }
                    tokio::time::sleep(poll_interval).await;
                }
                None => {
                    return Err(crate::error::AccordError::TxnNotFound(txn_id));
                }
            }
        }
    }
}

#[async_trait]
impl<E: Executor + Clone + Send + Sync + 'static> Executor for AccordExecutor<E> {
    async fn write(&self, events: Vec<Event>) -> std::result::Result<(), WriteError> {
        match &self.mode {
            ExecutorMode::SingleNode => {
                // Bypass consensus, write directly
                self.inner.write(events).await
            }
            ExecutorMode::Cluster { .. } => {
                // Run ACCORD protocol
                self.run_protocol(events)
                    .await
                    .map_err(|e| WriteError::Unknown(anyhow::anyhow!("{}", e)))
            }
        }
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        // Reads go directly to inner executor
        // ACCORD only coordinates writes
        self.inner.read(aggregators, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.inner.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.inner.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.inner.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.inner.acknowledge(key, cursor, lag).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct MockExecutor {
        write_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Executor for MockExecutor {
        async fn write(&self, _events: Vec<Event>) -> std::result::Result<(), WriteError> {
            self.write_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn read(
            &self,
            _aggregators: Option<Vec<ReadAggregator>>,
            _routing_key: Option<RoutingKey>,
            _args: Args,
        ) -> anyhow::Result<ReadResult<Event>> {
            Ok(ReadResult {
                edges: vec![],
                page_info: Default::default(),
            })
        }

        async fn get_subscriber_cursor(&self, _key: String) -> anyhow::Result<Option<Value>> {
            Ok(None)
        }

        async fn is_subscriber_running(
            &self,
            _key: String,
            _worker_id: Ulid,
        ) -> anyhow::Result<bool> {
            Ok(false)
        }

        async fn upsert_subscriber(&self, _key: String, _worker_id: Ulid) -> anyhow::Result<()> {
            Ok(())
        }

        async fn acknowledge(&self, _key: String, _cursor: Value, _lag: u64) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_node_write() {
        let inner = MockExecutor::default();
        let executor = AccordExecutor::single_node(inner.clone());

        let events = vec![];
        executor.write(events).await.unwrap();

        assert_eq!(inner.write_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_single_node_read() {
        let inner = MockExecutor::default();
        let executor = AccordExecutor::single_node(inner);

        let result = executor.read(None, None, Args::default()).await.unwrap();
        assert!(result.edges.is_empty());
    }

    #[test]
    fn test_config_single_node() {
        let config = AccordConfig::single_node();
        assert!(config.is_single_node());
    }
}
