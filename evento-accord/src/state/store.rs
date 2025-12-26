//! Transaction storage for ACCORD consensus.
//!
//! Provides in-memory storage for transactions with indexes for efficient
//! conflict detection and status queries.

use crate::error::{AccordError, Result};
use crate::timestamp::Timestamp;
use crate::txn::{Transaction, TxnId, TxnStatus};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// In-memory store for transactions.
///
/// Maintains indexes for:
/// - Fast lookup by transaction ID
/// - Fast conflict detection by key
/// - Ordered access by execution timestamp
#[derive(Debug, Default)]
pub struct TxnStore {
    /// All transactions by ID
    txns: HashMap<TxnId, Transaction>,

    /// Index: key -> transaction IDs touching that key
    by_key: HashMap<String, BTreeSet<TxnId>>,

    /// Index: transactions by status
    by_status: HashMap<TxnStatus, BTreeSet<TxnId>>,

    /// Index: committed transactions ordered by execute_at
    by_execute_at: BTreeMap<Timestamp, BTreeSet<TxnId>>,
}

impl TxnStore {
    /// Create a new empty transaction store
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of transactions in the store
    pub fn len(&self) -> usize {
        self.txns.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.txns.is_empty()
    }

    /// Insert a new transaction into the store
    pub fn insert(&mut self, txn: Transaction) -> Result<()> {
        if self.txns.contains_key(&txn.id) {
            return Err(AccordError::TxnExists(txn.id));
        }

        self.add_to_indexes(&txn);
        self.txns.insert(txn.id, txn);
        Ok(())
    }

    /// Update an existing transaction
    pub fn update(&mut self, txn: Transaction) -> Result<()> {
        // Remove old transaction first
        let old_txn = self
            .txns
            .remove(&txn.id)
            .ok_or(AccordError::TxnNotFound(txn.id))?;

        // Remove from old indexes
        self.remove_from_indexes(&old_txn);

        // Add to new indexes
        self.add_to_indexes(&txn);

        // Insert updated transaction
        self.txns.insert(txn.id, txn);
        Ok(())
    }

    /// Insert or update a transaction
    pub fn upsert(&mut self, txn: Transaction) {
        if let Some(old_txn) = self.txns.remove(&txn.id) {
            self.remove_from_indexes(&old_txn);
        }
        self.add_to_indexes(&txn);
        self.txns.insert(txn.id, txn);
    }

    /// Get a transaction by ID
    pub fn get(&self, id: &TxnId) -> Option<&Transaction> {
        self.txns.get(id)
    }

    /// Get a mutable reference to a transaction by ID
    pub fn get_mut(&mut self, id: &TxnId) -> Option<&mut Transaction> {
        self.txns.get_mut(id)
    }

    /// Remove a transaction from the store
    pub fn remove(&mut self, id: &TxnId) -> Option<Transaction> {
        if let Some(txn) = self.txns.remove(id) {
            self.remove_from_indexes(&txn);
            Some(txn)
        } else {
            None
        }
    }

    /// Find all transactions that conflict with the given keys.
    ///
    /// Returns transactions that touch any of the provided keys.
    pub fn find_conflicts(&self, keys: &[String]) -> Vec<&Transaction> {
        let mut conflicting_ids: BTreeSet<TxnId> = BTreeSet::new();

        for key in keys {
            if let Some(ids) = self.by_key.get(key) {
                conflicting_ids.extend(ids);
            }
        }

        conflicting_ids
            .iter()
            .filter_map(|id| self.txns.get(id))
            .collect()
    }

    /// Find all transactions with a specific status
    pub fn find_by_status(&self, status: TxnStatus) -> Vec<&Transaction> {
        self.by_status
            .get(&status)
            .map(|ids| ids.iter().filter_map(|id| self.txns.get(id)).collect())
            .unwrap_or_default()
    }

    /// Find committed transactions ready to execute (execute_at <= threshold).
    ///
    /// Returns transaction IDs in execution order.
    pub fn find_ready_to_execute(&self, up_to: Timestamp) -> Vec<TxnId> {
        self.by_execute_at
            .range(..=up_to)
            .flat_map(|(_, ids)| ids.iter().copied())
            .filter(|id| {
                self.txns
                    .get(id)
                    .map(|t| t.status == TxnStatus::Committed)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Update a transaction's status
    pub fn set_status(&mut self, id: &TxnId, new_status: TxnStatus) -> Result<()> {
        let txn = self.txns.get_mut(id).ok_or(AccordError::TxnNotFound(*id))?;

        let old_status = txn.status;

        // Remove from old status index
        if let Some(ids) = self.by_status.get_mut(&old_status) {
            ids.remove(id);
        }

        // Update status
        txn.status = new_status;

        // Add to new status index
        self.by_status.entry(new_status).or_default().insert(*id);

        Ok(())
    }

    /// Update a transaction's execute_at timestamp
    pub fn set_execute_at(&mut self, id: &TxnId, execute_at: Timestamp) -> Result<()> {
        let txn = self.txns.get_mut(id).ok_or(AccordError::TxnNotFound(*id))?;

        let old_execute_at = txn.execute_at;

        // Remove from old execute_at index
        if let Some(ids) = self.by_execute_at.get_mut(&old_execute_at) {
            ids.remove(id);
            if ids.is_empty() {
                self.by_execute_at.remove(&old_execute_at);
            }
        }

        // Update execute_at
        txn.execute_at = execute_at;

        // Add to new execute_at index
        self.by_execute_at
            .entry(execute_at)
            .or_default()
            .insert(*id);

        Ok(())
    }

    /// Get all transaction IDs
    pub fn all_ids(&self) -> impl Iterator<Item = &TxnId> {
        self.txns.keys()
    }

    /// Get count of transactions by status
    pub fn count_by_status(&self, status: TxnStatus) -> usize {
        self.by_status.get(&status).map(|s| s.len()).unwrap_or(0)
    }

    /// Get all committed or executed transactions since a given timestamp.
    ///
    /// Used for state sync after node restart.
    pub fn get_committed_since(
        &self,
        since: Timestamp,
        exclude: &std::collections::HashSet<TxnId>,
    ) -> Vec<Transaction> {
        self.by_execute_at
            .range(since..)
            .flat_map(|(_, ids)| ids.iter())
            .filter(|id| !exclude.contains(id))
            .filter_map(|id| self.txns.get(id))
            .filter(|t| t.status == TxnStatus::Committed || t.status == TxnStatus::Executed)
            .cloned()
            .collect()
    }

    /// Get all committed or executed transaction IDs.
    pub fn get_committed_ids(&self) -> Vec<TxnId> {
        let mut ids = Vec::new();
        if let Some(committed) = self.by_status.get(&TxnStatus::Committed) {
            ids.extend(committed.iter().copied());
        }
        if let Some(executed) = self.by_status.get(&TxnStatus::Executed) {
            ids.extend(executed.iter().copied());
        }
        ids
    }

    // === Private helper methods ===

    fn add_to_indexes(&mut self, txn: &Transaction) {
        // Add to key index
        for key in &txn.keys {
            self.by_key.entry(key.clone()).or_default().insert(txn.id);
        }

        // Add to status index
        self.by_status.entry(txn.status).or_default().insert(txn.id);

        // Add to execute_at index
        self.by_execute_at
            .entry(txn.execute_at)
            .or_default()
            .insert(txn.id);
    }

    fn remove_from_indexes(&mut self, txn: &Transaction) {
        // Remove from key index
        for key in &txn.keys {
            if let Some(ids) = self.by_key.get_mut(key) {
                ids.remove(&txn.id);
                if ids.is_empty() {
                    self.by_key.remove(key);
                }
            }
        }

        // Remove from status index
        if let Some(ids) = self.by_status.get_mut(&txn.status) {
            ids.remove(&txn.id);
        }

        // Remove from execute_at index
        if let Some(ids) = self.by_execute_at.get_mut(&txn.execute_at) {
            ids.remove(&txn.id);
            if ids.is_empty() {
                self.by_execute_at.remove(&txn.execute_at);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::Ballot;

    fn make_txn(time: u64, keys: &[&str]) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        let id = TxnId::new(ts);
        Transaction {
            id,
            events_data: vec![],
            keys: keys.iter().map(|s| s.to_string()).collect(),
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut store = TxnStore::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;

        store.insert(txn).unwrap();

        assert_eq!(store.len(), 1);
        assert!(store.get(&id).is_some());
    }

    #[test]
    fn test_insert_duplicate_fails() {
        let mut store = TxnStore::new();
        let txn = make_txn(100, &["key1"]);

        store.insert(txn.clone()).unwrap();
        let result = store.insert(txn);

        assert!(matches!(result, Err(AccordError::TxnExists(_))));
    }

    #[test]
    fn test_upsert() {
        let mut store = TxnStore::new();
        let mut txn = make_txn(100, &["key1"]);
        let id = txn.id;

        store.upsert(txn.clone());
        assert_eq!(store.get(&id).unwrap().keys.len(), 1);

        txn.keys.push("key2".to_string());
        store.upsert(txn);
        assert_eq!(store.get(&id).unwrap().keys.len(), 2);
    }

    #[test]
    fn test_remove() {
        let mut store = TxnStore::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;

        store.insert(txn).unwrap();
        assert_eq!(store.len(), 1);

        let removed = store.remove(&id);
        assert!(removed.is_some());
        assert_eq!(store.len(), 0);
        assert!(store.get(&id).is_none());
    }

    #[test]
    fn test_find_conflicts() {
        let mut store = TxnStore::new();

        let txn1 = make_txn(100, &["key1", "key2"]);
        let txn2 = make_txn(101, &["key2", "key3"]);
        let txn3 = make_txn(102, &["key4"]);

        store.insert(txn1).unwrap();
        store.insert(txn2).unwrap();
        store.insert(txn3).unwrap();

        // Find conflicts for key2 - should find txn1 and txn2
        let conflicts = store.find_conflicts(&["key2".to_string()]);
        assert_eq!(conflicts.len(), 2);

        // Find conflicts for key4 - should find only txn3
        let conflicts = store.find_conflicts(&["key4".to_string()]);
        assert_eq!(conflicts.len(), 1);

        // Find conflicts for unknown key - should be empty
        let conflicts = store.find_conflicts(&["key999".to_string()]);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_find_by_status() {
        let mut store = TxnStore::new();

        let txn1 = make_txn(100, &["key1"]);
        let mut txn2 = make_txn(101, &["key2"]);
        txn2.status = TxnStatus::Committed;

        store.insert(txn1).unwrap();
        store.insert(txn2).unwrap();

        let preaccepted = store.find_by_status(TxnStatus::PreAccepted);
        assert_eq!(preaccepted.len(), 1);

        let committed = store.find_by_status(TxnStatus::Committed);
        assert_eq!(committed.len(), 1);
    }

    #[test]
    fn test_set_status() {
        let mut store = TxnStore::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;

        store.insert(txn).unwrap();
        assert_eq!(store.count_by_status(TxnStatus::PreAccepted), 1);

        store.set_status(&id, TxnStatus::Committed).unwrap();
        assert_eq!(store.count_by_status(TxnStatus::PreAccepted), 0);
        assert_eq!(store.count_by_status(TxnStatus::Committed), 1);
    }

    #[test]
    fn test_find_ready_to_execute() {
        let mut store = TxnStore::new();

        let mut txn1 = make_txn(100, &["key1"]);
        txn1.status = TxnStatus::Committed;
        txn1.execute_at = Timestamp::new(100, 0, 1);

        let mut txn2 = make_txn(200, &["key2"]);
        txn2.status = TxnStatus::Committed;
        txn2.execute_at = Timestamp::new(200, 0, 1);

        let mut txn3 = make_txn(150, &["key3"]);
        txn3.status = TxnStatus::PreAccepted; // Not committed
        txn3.execute_at = Timestamp::new(150, 0, 1);

        store.insert(txn1).unwrap();
        store.insert(txn2).unwrap();
        store.insert(txn3).unwrap();

        // Up to 150 - should find txn1 only (txn3 is not committed)
        let ready = store.find_ready_to_execute(Timestamp::new(150, 0, 1));
        assert_eq!(ready.len(), 1);

        // Up to 200 - should find txn1 and txn2
        let ready = store.find_ready_to_execute(Timestamp::new(200, 0, 1));
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn test_indexes_updated_on_remove() {
        let mut store = TxnStore::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;

        store.insert(txn).unwrap();
        assert!(!store.find_conflicts(&["key1".to_string()]).is_empty());

        store.remove(&id);
        assert!(store.find_conflicts(&["key1".to_string()]).is_empty());
    }
}
