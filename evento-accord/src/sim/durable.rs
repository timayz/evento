//! Simulated durable storage for crash recovery testing.
//!
//! This module provides `SimDurableStore`, a durable store implementation
//! that persists data in shared memory, allowing it to survive simulated
//! node crashes and restarts.

use crate::error::Result;
use crate::state::DurableStore;
use crate::transport::NodeId;
use crate::txn::{Transaction, TxnId, TxnStatus};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Shared storage that survives node crashes in simulation.
///
/// This is shared across all nodes and represents "disk" storage.
/// Each node has its own partition identified by `NodeId`.
#[derive(Clone, Default)]
pub struct SharedDurableStorage {
    /// Per-node transaction storage.
    /// Outer key is NodeId, inner is transaction storage for that node.
    data: Arc<RwLock<HashMap<NodeId, NodeStorage>>>,
}

/// Storage for a single node.
#[derive(Default, Clone)]
struct NodeStorage {
    transactions: HashMap<TxnId, Transaction>,
}

impl SharedDurableStorage {
    /// Create a new shared durable storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a handle for a specific node.
    pub fn for_node(&self, node_id: NodeId) -> SimDurableStore {
        SimDurableStore {
            node_id,
            shared: self.clone(),
        }
    }

    /// Clear all data (for test reset).
    pub fn clear(&self) {
        let mut data = self.data.write().unwrap();
        data.clear();
    }

    /// Get all transactions for a node (for debugging).
    pub fn get_node_transactions(&self, node_id: NodeId) -> Vec<Transaction> {
        let data = self.data.read().unwrap();
        data.get(&node_id)
            .map(|s| s.transactions.values().cloned().collect())
            .unwrap_or_default()
    }
}

/// Simulated durable store for a single node.
///
/// Data is stored in shared memory and survives simulated crashes.
/// On "restart", the node can reload its state from this store.
pub struct SimDurableStore {
    node_id: NodeId,
    shared: SharedDurableStorage,
}

impl SimDurableStore {
    /// Create a new simulated durable store for a node.
    pub fn new(node_id: NodeId, shared: SharedDurableStorage) -> Self {
        Self { node_id, shared }
    }
}

#[async_trait]
impl DurableStore for SimDurableStore {
    async fn persist(&self, txn: &Transaction) -> Result<()> {
        let mut data = self.shared.data.write().unwrap();
        let node_storage = data.entry(self.node_id).or_default();
        node_storage.transactions.insert(txn.id, txn.clone());
        Ok(())
    }

    async fn load_committed(&self) -> Result<Vec<Transaction>> {
        let data = self.shared.data.read().unwrap();
        let txns = data
            .get(&self.node_id)
            .map(|s| {
                s.transactions
                    .values()
                    .filter(|t| t.status == TxnStatus::Committed || t.status == TxnStatus::Executed)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(txns)
    }

    async fn get_last_executed(&self) -> Result<Option<TxnId>> {
        let data = self.shared.data.read().unwrap();
        let last = data.get(&self.node_id).and_then(|s| {
            s.transactions
                .values()
                .filter(|t| t.status == TxnStatus::Executed)
                .max_by_key(|t| t.execute_at)
                .map(|t| t.id)
        });
        Ok(last)
    }

    async fn mark_executed(&self, txn_id: TxnId) -> Result<()> {
        let mut data = self.shared.data.write().unwrap();
        if let Some(node_storage) = data.get_mut(&self.node_id) {
            if let Some(txn) = node_storage.transactions.get_mut(&txn_id) {
                txn.status = TxnStatus::Executed;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::Ballot;

    fn make_txn(time: u64) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
            deps: vec![],
            status: TxnStatus::Committed,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_sim_durable_store_survives_restart() {
        let shared = SharedDurableStorage::new();

        // Node 0 persists a transaction
        let store0 = shared.for_node(0);
        let txn = make_txn(100);
        store0.persist(&txn).await.unwrap();

        // Simulate restart by creating a new store handle
        let store0_restarted = shared.for_node(0);
        let loaded = store0_restarted.load_committed().await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, txn.id);
    }

    #[tokio::test]
    async fn test_sim_durable_store_node_isolation() {
        let shared = SharedDurableStorage::new();

        let store0 = shared.for_node(0);
        let store1 = shared.for_node(1);

        let txn0 = make_txn(100);
        let txn1 = make_txn(200);

        store0.persist(&txn0).await.unwrap();
        store1.persist(&txn1).await.unwrap();

        // Each node should only see its own data
        let loaded0 = store0.load_committed().await.unwrap();
        let loaded1 = store1.load_committed().await.unwrap();

        assert_eq!(loaded0.len(), 1);
        assert_eq!(loaded0[0].id, txn0.id);

        assert_eq!(loaded1.len(), 1);
        assert_eq!(loaded1[0].id, txn1.id);
    }

    #[tokio::test]
    async fn test_sim_durable_store_mark_executed() {
        let shared = SharedDurableStorage::new();
        let store = shared.for_node(0);

        let txn = make_txn(100);
        store.persist(&txn).await.unwrap();

        // Not executed yet
        assert!(store.get_last_executed().await.unwrap().is_none());

        // Mark as executed
        store.mark_executed(txn.id).await.unwrap();

        // Now it should be returned
        let last = store.get_last_executed().await.unwrap();
        assert_eq!(last, Some(txn.id));
    }
}
