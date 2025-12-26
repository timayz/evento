//! Durable storage trait for crash recovery.
//!
//! The `DurableStore` trait abstracts persistent storage of committed transactions,
//! enabling nodes to recover their state after a crash.

use crate::error::Result;
use crate::txn::{Transaction, TxnId};
use async_trait::async_trait;

/// Trait for durable storage of committed transactions.
///
/// Implementations must guarantee that once `persist` returns successfully,
/// the transaction will survive crashes and be available via `load_committed`.
#[async_trait]
pub trait DurableStore: Send + Sync {
    /// Persist a committed transaction to durable storage.
    ///
    /// This is called when a transaction reaches the Committed state.
    /// The implementation must ensure the transaction survives crashes.
    async fn persist(&self, txn: &Transaction) -> Result<()>;

    /// Load all committed/executed transactions from durable storage.
    ///
    /// Called during node startup to restore state after a crash.
    /// Returns transactions in no particular order.
    async fn load_committed(&self) -> Result<Vec<Transaction>>;

    /// Get the highest executed transaction ID.
    ///
    /// Used during recovery to determine what we need to catch up on.
    async fn get_last_executed(&self) -> Result<Option<TxnId>>;

    /// Mark a transaction as executed in durable storage.
    ///
    /// Called after a transaction has been successfully applied.
    async fn mark_executed(&self, txn_id: TxnId) -> Result<()>;
}

/// In-memory durable store (for testing single-node scenarios).
///
/// This implementation stores data in memory and does NOT survive process restarts.
/// Use `SimDurableStore` for simulation testing with crash recovery.
#[derive(Default)]
pub struct MemoryDurableStore {
    transactions: tokio::sync::RwLock<std::collections::HashMap<TxnId, Transaction>>,
}

impl MemoryDurableStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DurableStore for MemoryDurableStore {
    async fn persist(&self, txn: &Transaction) -> Result<()> {
        let mut store = self.transactions.write().await;
        store.insert(txn.id, txn.clone());
        Ok(())
    }

    async fn load_committed(&self) -> Result<Vec<Transaction>> {
        let store = self.transactions.read().await;
        Ok(store.values().cloned().collect())
    }

    async fn get_last_executed(&self) -> Result<Option<TxnId>> {
        let store = self.transactions.read().await;
        let last = store
            .values()
            .filter(|t| t.status == crate::txn::TxnStatus::Executed)
            .max_by_key(|t| t.execute_at);
        Ok(last.map(|t| t.id))
    }

    async fn mark_executed(&self, txn_id: TxnId) -> Result<()> {
        let mut store = self.transactions.write().await;
        if let Some(txn) = store.get_mut(&txn_id) {
            txn.status = crate::txn::TxnStatus::Executed;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, TxnStatus};

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
    async fn test_memory_store_persist_and_load() {
        let store = MemoryDurableStore::new();

        let txn1 = make_txn(100);
        let txn2 = make_txn(200);

        store.persist(&txn1).await.unwrap();
        store.persist(&txn2).await.unwrap();

        let loaded = store.load_committed().await.unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_mark_executed() {
        let store = MemoryDurableStore::new();

        let txn = make_txn(100);
        store.persist(&txn).await.unwrap();

        assert!(store.get_last_executed().await.unwrap().is_none());

        store.mark_executed(txn.id).await.unwrap();

        let last = store.get_last_executed().await.unwrap();
        assert_eq!(last, Some(txn.id));
    }
}
