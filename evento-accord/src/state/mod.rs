//! Consensus state management for ACCORD.
//!
//! This module provides the core state machine that tracks transactions
//! through the ACCORD protocol phases.

mod deps;
mod durable;
mod store;
mod waiting;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod sql_durable;

pub use deps::{DependencyGraph, DependencyGraphStats};
pub use durable::{DurableStore, MemoryDurableStore};
pub use store::TxnStore;
pub use waiting::ExecutionQueue;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use sql_durable::{load_executed_txn_ids, SqlDurableStore};

use crate::error::{AccordError, Result};
use crate::timestamp::Timestamp;
use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

/// Thread-safe consensus state for the ACCORD protocol.
///
/// Manages transactions through the protocol phases:
/// - PreAccept: Initial proposal with dependency computation
/// - Accept: Finalize dependencies (slow path)
/// - Commit: Mark transaction as decided
/// - Execute: Apply transaction after dependencies are satisfied
pub struct ConsensusState {
    store: RwLock<TxnStore>,
    deps: RwLock<DependencyGraph>,
    queue: RwLock<ExecutionQueue>,
    /// Stores execution error messages for failed transactions
    execution_errors: RwLock<HashMap<TxnId, String>>,
}

impl ConsensusState {
    /// Create a new empty consensus state
    pub fn new() -> Self {
        Self {
            store: RwLock::new(TxnStore::new()),
            deps: RwLock::new(DependencyGraph::new()),
            queue: RwLock::new(ExecutionQueue::new()),
            execution_errors: RwLock::new(HashMap::new()),
        }
    }

    // ========== PreAccept Phase ==========

    /// Pre-accept a new transaction.
    ///
    /// Computes initial dependencies based on conflicting transactions.
    /// Returns the computed dependencies.
    pub async fn preaccept(&self, mut txn: Transaction) -> Result<Vec<TxnId>> {
        let mut store = self.store.write().await;

        // Check if transaction already exists
        if store.get(&txn.id).is_some() {
            return Err(AccordError::TxnExists(txn.id));
        }

        // Find conflicting transactions
        let conflicts = store.find_conflicts(&txn.keys);

        // Compute dependencies: transactions that conflict and have lower timestamp
        let deps: Vec<TxnId> = conflicts
            .iter()
            .filter(|c| c.id < txn.id)
            .map(|c| c.id)
            .collect();

        txn.deps = deps.clone();
        txn.status = TxnStatus::PreAccepted;

        // Insert into store
        store.upsert(txn);

        Ok(deps)
    }

    /// Handle a PreAccept request from another node.
    ///
    /// Returns our computed dependencies for the transaction.
    pub async fn handle_preaccept(&self, txn: Transaction, ballot: Ballot) -> Result<Vec<TxnId>> {
        let mut store = self.store.write().await;

        // Check ballot if transaction already exists
        if let Some(existing) = store.get(&txn.id) {
            if existing.ballot > ballot {
                return Err(AccordError::BallotTooLow {
                    attempted: ballot,
                    existing: existing.ballot,
                });
            }
        }

        // Compute our local dependencies
        let conflicts = store.find_conflicts(&txn.keys);
        let deps: Vec<TxnId> = conflicts
            .iter()
            .filter(|c| c.id < txn.id)
            .map(|c| c.id)
            .collect();

        // Update our copy
        let mut txn = txn;
        txn.deps = deps.clone();
        txn.status = TxnStatus::PreAccepted;
        txn.ballot = ballot;

        store.upsert(txn);

        Ok(deps)
    }

    // ========== Accept Phase ==========

    /// Accept a transaction with final dependencies and execute_at.
    ///
    /// Used in the slow path when PreAccept responses disagreed on deps.
    pub async fn accept(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        ballot: Ballot,
    ) -> Result<()> {
        let mut store = self.store.write().await;

        let txn = store
            .get_mut(&txn_id)
            .ok_or(AccordError::TxnNotFound(txn_id))?;

        // Check ballot
        if txn.ballot > ballot {
            return Err(AccordError::BallotTooLow {
                attempted: ballot,
                existing: txn.ballot,
            });
        }

        // Update transaction
        txn.deps = deps;
        txn.execute_at = execute_at;
        txn.status = TxnStatus::Accepted;
        txn.ballot = ballot;

        Ok(())
    }

    // ========== Commit Phase ==========

    /// Commit a transaction.
    ///
    /// After this, the transaction will be executed once dependencies are satisfied.
    pub async fn commit(&self, txn_id: TxnId) -> Result<()> {
        let mut store = self.store.write().await;
        let mut deps_graph = self.deps.write().await;
        let mut queue = self.queue.write().await;

        let txn = store.get(&txn_id).ok_or(AccordError::TxnNotFound(txn_id))?;

        // Validate status transition
        if txn.status == TxnStatus::Committed || txn.status == TxnStatus::Executed {
            return Ok(()); // Already committed, idempotent
        }

        let txn_deps: HashSet<TxnId> = txn.deps.iter().copied().collect();
        let execute_at = txn.execute_at;

        // Update status
        store.set_status(&txn_id, TxnStatus::Committed)?;

        // Register in dependency graph
        deps_graph.register(txn_id, txn_deps);

        // Add to execution queue
        let is_ready = deps_graph.is_ready(&txn_id);
        queue.enqueue(txn_id, execute_at, is_ready);

        Ok(())
    }

    /// Create a transaction from a Commit message (when we missed PreAccept).
    pub async fn create_from_commit(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    ) -> Result<()> {
        // First check if transaction already exists
        {
            let store = self.store.read().await;
            if store.get(&txn_id).is_some() {
                // Already have it, just update to committed
                // Drop the read lock before calling commit to avoid deadlock
                drop(store);
                return self.commit(txn_id).await;
            }
        }

        // Transaction doesn't exist, acquire write locks and create it
        let mut store = self.store.write().await;
        let mut deps_graph = self.deps.write().await;
        let mut queue = self.queue.write().await;

        // Double-check (in case another task created it while we waited for write lock)
        if store.get(&txn_id).is_some() {
            drop(store);
            drop(deps_graph);
            drop(queue);
            return self.commit(txn_id).await;
        }

        // Create transaction from commit info
        let txn = Transaction {
            id: txn_id,
            events_data,
            keys,
            deps: deps.clone(),
            status: TxnStatus::Committed,
            execute_at,
            ballot: Ballot::default(),
        };

        store.upsert(txn);

        // Register in dependency graph
        let txn_deps: HashSet<TxnId> = deps.into_iter().collect();
        deps_graph.register(txn_id, txn_deps);

        // Add to execution queue
        let is_ready = deps_graph.is_ready(&txn_id);
        queue.enqueue(txn_id, execute_at, is_ready);

        Ok(())
    }

    // ========== Execute Phase ==========

    /// Get the next transaction ready for execution.
    ///
    /// Returns None if no transactions are ready.
    pub async fn next_executable(&self) -> Option<Transaction> {
        let store = self.store.read().await;
        let mut queue = self.queue.write().await;

        // Claim the transaction so other workers don't try to execute it
        let (txn_id, _) = queue.claim_next()?;
        store.get(&txn_id).cloned()
    }

    /// Mark a transaction as executed.
    ///
    /// Updates dependency graph and may unblock other transactions.
    pub async fn mark_executed(&self, txn_id: TxnId) -> Result<()> {
        let mut store = self.store.write().await;
        let mut deps_graph = self.deps.write().await;
        let mut queue = self.queue.write().await;

        let txn = store.get(&txn_id).ok_or(AccordError::TxnNotFound(txn_id))?;
        let execute_at = txn.execute_at;

        // Update status
        store.set_status(&txn_id, TxnStatus::Executed)?;

        // Remove from queue
        queue.remove(&txn_id, &execute_at);

        // Update dependency graph - this may unblock other transactions
        let newly_ready = deps_graph.mark_executed(txn_id);

        // Mark newly ready transactions in the queue
        for ready_id in newly_ready {
            queue.mark_ready(ready_id);
        }

        Ok(())
    }

    /// Mark a transaction as failed during execution.
    ///
    /// Stores the error message and updates the transaction status.
    /// This unblocks dependent transactions (they may also fail or succeed
    /// depending on application semantics).
    pub async fn mark_execution_failed(&self, txn_id: TxnId, error: String) -> Result<()> {
        let mut store = self.store.write().await;
        let mut deps_graph = self.deps.write().await;
        let mut queue = self.queue.write().await;
        let mut errors = self.execution_errors.write().await;

        let txn = store.get(&txn_id).ok_or(AccordError::TxnNotFound(txn_id))?;
        let execute_at = txn.execute_at;

        // Store the error message
        errors.insert(txn_id, error);

        // Update status to failed
        store.set_status(&txn_id, TxnStatus::ExecutionFailed)?;

        // Remove from queue
        queue.remove(&txn_id, &execute_at);

        // Update dependency graph - this may unblock other transactions
        // (failed transactions are still considered "done" for dependency purposes)
        let newly_ready = deps_graph.mark_executed(txn_id);

        // Mark newly ready transactions in the queue
        for ready_id in newly_ready {
            queue.mark_ready(ready_id);
        }

        Ok(())
    }

    /// Get the execution error for a failed transaction.
    pub async fn get_execution_error(&self, txn_id: &TxnId) -> Option<String> {
        let errors = self.execution_errors.read().await;
        errors.get(txn_id).cloned()
    }

    /// Re-queue a transaction for execution.
    ///
    /// This releases the claim on a transaction that was being executed,
    /// allowing it to be picked up again by an execution worker.
    /// Used when execution fails due to a recoverable error (e.g., missing dependency).
    pub async fn requeue_transaction(&self, txn_id: TxnId) {
        let mut queue = self.queue.write().await;
        queue.release_claim(&txn_id);
    }

    // ========== Queries ==========

    /// Get a transaction by ID.
    pub async fn get(&self, txn_id: &TxnId) -> Option<Transaction> {
        let store = self.store.read().await;
        store.get(txn_id).cloned()
    }

    /// Get the status of a transaction.
    pub async fn get_status(&self, txn_id: &TxnId) -> Option<TxnStatus> {
        let store = self.store.read().await;
        store.get(txn_id).map(|t| t.status)
    }

    /// Check if a transaction is ready to execute.
    pub async fn is_ready(&self, txn_id: &TxnId) -> bool {
        let deps = self.deps.read().await;
        deps.is_ready(txn_id)
    }

    /// Get pending dependencies for a transaction.
    pub async fn get_pending_deps(&self, txn_id: &TxnId) -> Option<Vec<TxnId>> {
        let deps = self.deps.read().await;
        deps.get_pending_deps(txn_id)
            .map(|s| s.iter().copied().collect())
    }

    /// Get all ready transactions in execution order.
    pub async fn get_ready_transactions(&self) -> Vec<(TxnId, Timestamp)> {
        let queue = self.queue.read().await;
        queue.get_ready_ordered()
    }

    /// Get statistics about the consensus state.
    pub async fn stats(&self) -> ConsensusStateStats {
        let store = self.store.read().await;
        let deps = self.deps.read().await;
        let queue = self.queue.read().await;

        ConsensusStateStats {
            total_transactions: store.len(),
            preaccepted: store.count_by_status(TxnStatus::PreAccepted),
            accepted: store.count_by_status(TxnStatus::Accepted),
            committed: store.count_by_status(TxnStatus::Committed),
            executed: store.count_by_status(TxnStatus::Executed),
            queued: queue.len(),
            ready: queue.ready_count(),
            deps_stats: deps.stats(),
        }
    }

    // ========== Recovery ==========

    /// Restore executed transaction IDs from durable storage.
    ///
    /// Call this during node startup to skip re-executing transactions
    /// that were already executed before the crash/restart.
    pub async fn restore_executed(&self, txn_ids: Vec<TxnId>) -> usize {
        let mut store = self.store.write().await;
        let mut deps_graph = self.deps.write().await;

        let mut restored = 0;
        for txn_id in txn_ids {
            // Create a minimal transaction record
            let txn = Transaction {
                id: txn_id,
                events_data: vec![],
                keys: vec![],
                deps: vec![],
                status: TxnStatus::Executed,
                execute_at: txn_id.timestamp,
                ballot: Ballot::default(),
            };

            // Only insert if not already present
            if store.get(&txn_id).is_none() {
                store.upsert(txn);
                // Mark as executed in dependency graph so other transactions
                // don't wait on this one
                deps_graph.mark_executed(txn_id);
                restored += 1;
            }
        }

        tracing::info!("Restored {} executed transactions from durable storage", restored);
        restored
    }

    // ========== State Sync ==========

    /// Get committed transactions for sync.
    ///
    /// Returns transactions with execute_at >= since that are not in the exclude set.
    pub async fn get_committed_for_sync(
        &self,
        since: Timestamp,
        exclude: &HashSet<TxnId>,
    ) -> Vec<Transaction> {
        let store = self.store.read().await;
        store.get_committed_since(since, exclude)
    }

    /// Get all committed transaction IDs (for sync protocol).
    pub async fn get_committed_ids(&self) -> Vec<TxnId> {
        let store = self.store.read().await;
        store.get_committed_ids()
    }

    /// Apply transactions from sync response.
    ///
    /// This imports committed transactions received during state sync.
    pub async fn apply_sync_transactions(&self, transactions: Vec<Transaction>) -> Result<usize> {
        let mut applied = 0;

        for txn in transactions {
            // Only apply if we don't already have it or it's in a lower state
            let should_apply = {
                let store = self.store.read().await;
                match store.get(&txn.id) {
                    None => true,
                    Some(existing) => {
                        // Apply if incoming has higher status
                        txn.status > existing.status
                    }
                }
            };

            if should_apply {
                // Create from commit to properly register dependencies
                // NOTE: We always create as Committed, even if the peer has already
                // executed the transaction. This ensures our local execution workers
                // will actually execute it and write the events to our database.
                self.create_from_commit(
                    txn.id,
                    txn.deps.clone(),
                    txn.execute_at,
                    txn.events_data.clone(),
                    txn.keys.clone(),
                )
                .await?;

                applied += 1;
            }
        }

        Ok(applied)
    }

    // ========== Recovery ==========

    /// Get all transactions that are blocked on unsatisfied dependencies.
    ///
    /// Returns tuples of (txn_id, list of missing dependency IDs).
    pub async fn get_blocked_transactions(&self) -> Vec<(TxnId, Vec<TxnId>)> {
        let deps = self.deps.read().await;
        deps.get_blocked_transactions()
    }

    /// Get all unique missing dependencies across all blocked transactions.
    pub async fn get_missing_dependencies(&self) -> Vec<TxnId> {
        let deps = self.deps.read().await;
        let store = self.store.read().await;

        let blocked = deps.get_blocked_transactions();
        let mut missing: HashSet<TxnId> = HashSet::new();

        for (_, dep_ids) in blocked {
            for dep_id in dep_ids {
                // A dependency is "missing" if we don't have it in our store
                if store.get(&dep_id).is_none() {
                    missing.insert(dep_id);
                }
            }
        }

        missing.into_iter().collect()
    }

    /// Apply a recovered transaction from a peer.
    ///
    /// If the transaction is committed/executed on a peer, we apply it locally
    /// so our blocked transactions can make progress.
    pub async fn apply_recovered_transaction(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    ) -> Result<()> {
        self.create_from_commit(txn_id, deps, execute_at, events_data, keys)
            .await
    }

    /// Mark a dependency as permanently unsatisfiable.
    ///
    /// This is used when recovery determines that a dependency transaction
    /// was never committed on any peer and will never exist.
    /// Blocked transactions waiting on this dependency will become unblocked.
    ///
    /// Also creates a minimal transaction entry with ExecutionFailed status
    /// so that `wait_for_execution` will accept it and not loop infinitely.
    pub async fn mark_dependency_abandoned(&self, dep_id: TxnId) -> Vec<TxnId> {
        let mut store = self.store.write().await;
        let mut deps = self.deps.write().await;
        let mut queue = self.queue.write().await;

        // Create a minimal transaction entry with ExecutionFailed status
        // so that wait_for_execution will find it and proceed
        if store.get(&dep_id).is_none() {
            let abandoned_txn = Transaction {
                id: dep_id,
                events_data: vec![],
                keys: vec![],
                deps: vec![],
                status: TxnStatus::ExecutionFailed,
                execute_at: dep_id.timestamp,
                ballot: Ballot::default(),
            };
            store.upsert(abandoned_txn);
        }

        let newly_ready = deps.mark_dep_satisfied(dep_id);

        // Mark newly ready transactions in the queue
        for txn_id in &newly_ready {
            queue.mark_ready(*txn_id);
        }

        newly_ready
    }
}

impl Default for ConsensusState {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the consensus state.
#[derive(Debug, Clone)]
pub struct ConsensusStateStats {
    pub total_transactions: usize,
    pub preaccepted: usize,
    pub accepted: usize,
    pub committed: usize,
    pub executed: usize,
    pub queued: usize,
    pub ready: usize,
    pub deps_stats: DependencyGraphStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;

    fn make_txn(time: u64, keys: &[&str]) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        let id = TxnId::new(ts);
        Transaction {
            id,
            events_data: vec![1, 2, 3],
            keys: keys.iter().map(|s| s.to_string()).collect(),
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_preaccept_no_conflicts() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        let deps = state.preaccept(txn.clone()).await.unwrap();

        assert!(deps.is_empty());
        assert!(state.get(&txn.id).await.is_some());
    }

    #[tokio::test]
    async fn test_preaccept_with_conflicts() {
        let state = ConsensusState::new();

        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key1"]);

        state.preaccept(txn1.clone()).await.unwrap();
        let deps = state.preaccept(txn2.clone()).await.unwrap();

        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], txn1.id);
    }

    #[tokio::test]
    async fn test_preaccept_duplicate_fails() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        state.preaccept(txn.clone()).await.unwrap();
        let result = state.preaccept(txn).await;

        assert!(matches!(result, Err(AccordError::TxnExists(_))));
    }

    #[tokio::test]
    async fn test_commit_and_execute() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;

        state.preaccept(txn).await.unwrap();
        state.commit(id).await.unwrap();

        // Should be ready immediately (no deps)
        let next = state.next_executable().await;
        assert!(next.is_some());
        assert_eq!(next.unwrap().id, id);

        state.mark_executed(id).await.unwrap();
        assert_eq!(state.get_status(&id).await, Some(TxnStatus::Executed));
    }

    #[tokio::test]
    async fn test_execution_ordering() {
        let state = ConsensusState::new();

        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key1"]); // Depends on txn1

        state.preaccept(txn1.clone()).await.unwrap();
        state.preaccept(txn2.clone()).await.unwrap();

        state.commit(txn1.id).await.unwrap();
        state.commit(txn2.id).await.unwrap();

        // txn1 should be ready, txn2 should not
        assert!(state.is_ready(&txn1.id).await);
        assert!(!state.is_ready(&txn2.id).await);

        // Execute txn1
        state.mark_executed(txn1.id).await.unwrap();

        // Now txn2 should be ready
        assert!(state.is_ready(&txn2.id).await);
    }

    #[tokio::test]
    async fn test_accept_updates_deps() {
        let state = ConsensusState::new();

        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key1"]);
        let txn3 = make_txn(102, &["key1"]);

        state.preaccept(txn1.clone()).await.unwrap();
        state.preaccept(txn2.clone()).await.unwrap();
        state.preaccept(txn3.clone()).await.unwrap();

        // Accept txn3 with additional dependency on txn2
        let new_execute_at = Timestamp::new(200, 0, 1);
        state
            .accept(
                txn3.id,
                vec![txn1.id, txn2.id],
                new_execute_at,
                Ballot::new(1, 1),
            )
            .await
            .unwrap();

        let updated = state.get(&txn3.id).await.unwrap();
        assert_eq!(updated.deps.len(), 2);
        assert_eq!(updated.execute_at, new_execute_at);
        assert_eq!(updated.status, TxnStatus::Accepted);
    }

    #[tokio::test]
    async fn test_create_from_commit() {
        let state = ConsensusState::new();

        let id = TxnId::new(Timestamp::new(100, 0, 1));
        let execute_at = Timestamp::new(100, 0, 1);

        state
            .create_from_commit(
                id,
                vec![],
                execute_at,
                vec![1, 2, 3],
                vec!["key1".to_string()],
            )
            .await
            .unwrap();

        let txn = state.get(&id).await.unwrap();
        assert_eq!(txn.status, TxnStatus::Committed);
        assert!(state.is_ready(&id).await);
    }

    #[tokio::test]
    async fn test_stats() {
        let state = ConsensusState::new();

        let txn1 = make_txn(100, &["key1"]);
        let txn2 = make_txn(101, &["key2"]);

        state.preaccept(txn1.clone()).await.unwrap();
        state.preaccept(txn2.clone()).await.unwrap();
        state.commit(txn1.id).await.unwrap();

        let stats = state.stats().await;
        assert_eq!(stats.total_transactions, 2);
        assert_eq!(stats.preaccepted, 1);
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.queued, 1);
    }
}
