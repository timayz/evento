//! Execute phase of the ACCORD protocol.
//!
//! This is Phase 4 where committed transactions are applied to the
//! underlying storage once all dependencies have been satisfied.

use crate::error::{AccordError, Result};
use crate::state::ConsensusState;
use crate::txn::{TxnId, TxnStatus};
use evento_core::Executor;
use std::time::Duration;
use tokio::time::timeout;

/// Configuration for the executor.
#[derive(Clone, Debug)]
pub struct ExecuteConfig {
    /// Timeout for waiting on dependencies
    pub dependency_timeout: Duration,
    /// Polling interval when waiting for dependencies
    pub poll_interval: Duration,
}

impl Default for ExecuteConfig {
    fn default() -> Self {
        Self {
            dependency_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(10),
        }
    }
}

/// Trait for decoding events from transaction data.
///
/// This trait allows different serialization formats to be used
/// for encoding events in transactions.
pub trait EventDecoder: Send + Sync {
    /// Decode events from raw bytes.
    fn decode(&self, data: &[u8]) -> Result<Vec<evento_core::Event>>;
}

/// Execute a committed transaction.
///
/// Waits for all dependencies to be executed first, then applies
/// the transaction to the underlying executor.
pub async fn execute<E: Executor, D: EventDecoder>(
    state: &ConsensusState,
    executor: &E,
    decoder: &D,
    txn_id: TxnId,
    config: &ExecuteConfig,
) -> Result<()> {
    let txn = state
        .get(&txn_id)
        .await
        .ok_or(AccordError::TxnNotFound(txn_id))?;

    // Verify status
    if txn.status != TxnStatus::Committed {
        if txn.status == TxnStatus::Executed {
            return Ok(()); // Already executed, idempotent
        }
        return Err(AccordError::InvalidStatus {
            expected: TxnStatus::Committed,
            actual: txn.status,
        });
    }

    // Wait for all dependencies to execute
    for dep_id in &txn.deps {
        wait_for_execution(state, *dep_id, config).await?;
    }

    // Decode events using provided decoder
    let events = decoder.decode(&txn.events_data)?;

    // Write to underlying executor
    executor.write(events).await?;

    // Mark as executed
    state.mark_executed(txn_id).await?;

    Ok(())
}

/// Execute without decoding - just marks as executed.
///
/// This is useful when the events are already written or
/// when testing the execution flow without actual event data.
pub async fn execute_raw<E: Executor>(
    state: &ConsensusState,
    executor: &E,
    events: Vec<evento_core::Event>,
    txn_id: TxnId,
    config: &ExecuteConfig,
) -> Result<()> {
    let txn = state
        .get(&txn_id)
        .await
        .ok_or(AccordError::TxnNotFound(txn_id))?;

    // Verify status
    if txn.status != TxnStatus::Committed {
        if txn.status == TxnStatus::Executed {
            return Ok(()); // Already executed, idempotent
        }
        return Err(AccordError::InvalidStatus {
            expected: TxnStatus::Committed,
            actual: txn.status,
        });
    }

    // Wait for all dependencies to execute
    for dep_id in &txn.deps {
        wait_for_execution(state, *dep_id, config).await?;
    }

    // Write to underlying executor
    executor.write(events).await?;

    // Mark as executed
    state.mark_executed(txn_id).await?;

    Ok(())
}

/// Wait for a transaction to be executed (or fail).
async fn wait_for_execution(
    state: &ConsensusState,
    txn_id: TxnId,
    config: &ExecuteConfig,
) -> Result<()> {
    let wait_future = async {
        loop {
            match state.get_status(&txn_id).await {
                Some(TxnStatus::Executed) => return Ok(()),
                Some(TxnStatus::ExecutionFailed) => {
                    // Dependency failed - we can still proceed
                    // (the failure was already recorded, we just need to know it's "done")
                    return Ok(());
                }
                Some(_) => {
                    // Still waiting
                    tokio::time::sleep(config.poll_interval).await;
                }
                None => {
                    // Unknown transaction - might need recovery
                    return Err(AccordError::TxnNotFound(txn_id));
                }
            }
        }
    };

    timeout(config.dependency_timeout, wait_future)
        .await
        .map_err(|_| AccordError::Timeout(format!("waiting for dependency {}", txn_id)))?
}

/// Background executor that continuously processes ready transactions.
pub struct BackgroundExecutor<E: Executor, D: EventDecoder> {
    state: ConsensusState,
    executor: E,
    decoder: D,
    config: ExecuteConfig,
}

impl<E: Executor + Clone + Send + 'static, D: EventDecoder + Clone + Send + 'static>
    BackgroundExecutor<E, D>
{
    /// Create a new background executor.
    pub fn new(state: ConsensusState, executor: E, decoder: D, config: ExecuteConfig) -> Self {
        Self {
            state,
            executor,
            decoder,
            config,
        }
    }

    /// Run the executor loop.
    ///
    /// Continuously polls for ready transactions and executes them.
    pub async fn run(&self) -> ! {
        loop {
            if let Some(txn) = self.state.next_executable().await {
                let state = &self.state;
                let executor = &self.executor;
                let decoder = &self.decoder;
                let config = &self.config;

                if let Err(e) = execute(state, executor, decoder, txn.id, config).await {
                    tracing::error!("Failed to execute transaction {}: {}", txn.id, e);
                }
            } else {
                // No ready transactions, wait a bit
                tokio::time::sleep(self.config.poll_interval).await;
            }
        }
    }

    /// Spawn worker tasks for parallel execution.
    pub fn spawn_workers(self, count: usize) -> Vec<tokio::task::JoinHandle<()>>
    where
        E: Send + Sync,
        D: Send + Sync,
    {
        let state = std::sync::Arc::new(self.state);
        let executor = std::sync::Arc::new(self.executor);
        let decoder = std::sync::Arc::new(self.decoder);
        let config = std::sync::Arc::new(self.config);

        (0..count)
            .map(|_| {
                let state = state.clone();
                let executor = executor.clone();
                let decoder = decoder.clone();
                let config = config.clone();

                tokio::spawn(async move {
                    loop {
                        if let Some(txn) = state.next_executable().await {
                            if let Err(e) = execute(
                                &state,
                                executor.as_ref(),
                                decoder.as_ref(),
                                txn.id,
                                &config,
                            )
                            .await
                            {
                                tracing::error!("Failed to execute transaction {}: {}", txn.id, e);
                            }
                        } else {
                            tokio::time::sleep(config.poll_interval).await;
                        }
                    }
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction};
    use async_trait::async_trait;
    use evento_core::{
        cursor::{Args, ReadResult, Value},
        Event, ReadAggregator, RoutingKey, WriteError,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock executor for testing
    #[derive(Default, Clone)]
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
            _worker_id: ulid::Ulid,
        ) -> anyhow::Result<bool> {
            Ok(false)
        }

        async fn upsert_subscriber(
            &self,
            _key: String,
            _worker_id: ulid::Ulid,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn acknowledge(&self, _key: String, _cursor: Value, _lag: u64) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn make_txn(time: u64, keys: &[&str]) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);

        Transaction {
            id: TxnId::new(ts),
            events_data: vec![], // Empty for tests
            keys: keys.iter().map(|s| s.to_string()).collect(),
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_execute_raw_simple() {
        let state = ConsensusState::new();
        let executor = MockExecutor::default();
        let config = ExecuteConfig::default();

        // Setup: PreAccept and commit
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();
        state.commit(txn.id).await.unwrap();

        // Execute
        execute_raw(&state, &executor, vec![], txn.id, &config)
            .await
            .unwrap();

        // Verify
        assert_eq!(executor.write_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.get_status(&txn.id).await, Some(TxnStatus::Executed));
    }

    #[tokio::test]
    async fn test_execute_raw_with_deps() {
        let state = ConsensusState::new();
        let executor = MockExecutor::default();
        let config = ExecuteConfig {
            dependency_timeout: Duration::from_millis(100),
            poll_interval: Duration::from_millis(5),
        };

        // Setup: Two transactions where txn2 depends on txn1
        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key1"]);

        state.preaccept(txn1.clone()).await.unwrap();
        state.preaccept(txn2.clone()).await.unwrap();

        state.commit(txn1.id).await.unwrap();
        state.commit(txn2.id).await.unwrap();

        // Execute txn1 first
        execute_raw(&state, &executor, vec![], txn1.id, &config)
            .await
            .unwrap();

        // Now txn2 should be executable
        execute_raw(&state, &executor, vec![], txn2.id, &config)
            .await
            .unwrap();

        assert_eq!(executor.write_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_execute_raw_idempotent() {
        let state = ConsensusState::new();
        let executor = MockExecutor::default();
        let config = ExecuteConfig::default();

        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();
        state.commit(txn.id).await.unwrap();

        // Execute twice
        execute_raw(&state, &executor, vec![], txn.id, &config)
            .await
            .unwrap();
        execute_raw(&state, &executor, vec![], txn.id, &config)
            .await
            .unwrap();

        // Should only write once
        assert_eq!(executor.write_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_execute_raw_not_committed_fails() {
        let state = ConsensusState::new();
        let executor = MockExecutor::default();
        let config = ExecuteConfig::default();

        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();
        // Not committed!

        let result = execute_raw(&state, &executor, vec![], txn.id, &config).await;

        assert!(matches!(result, Err(AccordError::InvalidStatus { .. })));
    }

    #[tokio::test]
    async fn test_execute_raw_dep_timeout() {
        let state = ConsensusState::new();
        let executor = MockExecutor::default();
        let config = ExecuteConfig {
            dependency_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
        };

        // Setup: txn2 depends on txn1, but txn1 is never executed
        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key1"]);

        state.preaccept(txn1.clone()).await.unwrap();
        state.preaccept(txn2.clone()).await.unwrap();
        state.commit(txn1.id).await.unwrap();
        state.commit(txn2.id).await.unwrap();

        // Try to execute txn2 without executing txn1
        // Note: txn2 depends on txn1 because they share key1
        let result = execute_raw(&state, &executor, vec![], txn2.id, &config).await;

        // Should timeout waiting for txn1
        assert!(matches!(result, Err(AccordError::Timeout(_))));
    }
}
