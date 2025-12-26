//! AccordExecutor - ACCORD consensus wrapper for any Executor.

use crate::config::AccordConfig;
use crate::error::Result;
use crate::keys::Key;
use crate::protocol::execute::{execute_raw, ExecuteConfig};
use crate::protocol::{self, Message};
use crate::state::ConsensusState;
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
    clock: HybridClock,
    mode: ExecutorMode,
}

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
            clock: HybridClock::new(0),
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
        let clock = HybridClock::new(config.local.id);
        let state = Arc::new(ConsensusState::new());
        let peers = config.peers();

        let transport = Arc::new(TcpTransport::new(config.local.id, peers));

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

    /// Start background execution worker tasks.
    ///
    /// Returns handles to all spawned worker tasks.
    fn start_execution_workers(
        state: Arc<ConsensusState>,
        executor: E,
        count: usize,
        config: ExecuteConfig,
        cancel_token: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::with_capacity(count);

        for worker_id in 0..count {
            let state = state.clone();
            let executor = executor.clone();
            let config = config.clone();
            let cancel = cancel_token.clone();

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

                                if let Err(e) = execute_raw(&state, &executor, events, txn.id, &config).await {
                                    tracing::error!("Execution failed for {}: {}", txn.id, e);
                                    // Mark the transaction as failed so callers know
                                    if let Err(mark_err) = state.mark_execution_failed(txn.id, e.to_string()).await {
                                        tracing::error!("Failed to mark {} as failed: {}", txn.id, mark_err);
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
        let result = protocol::run_protocol(
            &self.state,
            txn,
            preaccept_responses,
            |msg| {
                // Accept broadcast
                let transport = transport.clone();
                futures::executor::block_on(async { transport.broadcast(&msg).await })
            },
            |msg| {
                // Commit broadcast (fire and forget)
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
                    if tokio::time::Instant::now() > deadline {
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
