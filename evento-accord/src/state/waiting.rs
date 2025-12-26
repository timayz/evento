//! Execution queue for ordering transaction execution.
//!
//! Transactions are executed in order of their execute_at timestamp.
//! This queue manages the ordering and provides the next transaction to execute.

use crate::timestamp::Timestamp;
use crate::txn::TxnId;
use std::collections::{BTreeMap, HashSet};

/// Priority queue for transaction execution.
///
/// Orders transactions by their execute_at timestamp.
/// Only transactions that are ready (deps satisfied) can be dequeued.
#[derive(Debug, Default)]
pub struct ExecutionQueue {
    /// Transactions ordered by execute_at timestamp.
    /// Multiple transactions can have the same execute_at.
    queue: BTreeMap<Timestamp, HashSet<TxnId>>,

    /// All transaction IDs in the queue (for fast lookup)
    all_ids: HashSet<TxnId>,

    /// Transactions that are ready to execute
    ready: HashSet<TxnId>,
}

impl ExecutionQueue {
    /// Create a new empty execution queue
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a transaction to the queue.
    ///
    /// If `is_ready` is true, the transaction can be immediately executed.
    pub fn enqueue(&mut self, txn_id: TxnId, execute_at: Timestamp, is_ready: bool) {
        self.queue.entry(execute_at).or_default().insert(txn_id);
        self.all_ids.insert(txn_id);

        if is_ready {
            self.ready.insert(txn_id);
        }
    }

    /// Mark a transaction as ready for execution.
    pub fn mark_ready(&mut self, txn_id: TxnId) {
        if self.all_ids.contains(&txn_id) {
            self.ready.insert(txn_id);
        }
    }

    /// Get the next transaction to execute.
    ///
    /// Returns the transaction with the lowest execute_at that is ready.
    /// Does NOT remove it from the queue - call `remove` after execution.
    pub fn peek_next(&self) -> Option<(TxnId, Timestamp)> {
        for (execute_at, ids) in &self.queue {
            for id in ids {
                if self.ready.contains(id) {
                    return Some((*id, *execute_at));
                }
            }
        }
        None
    }

    /// Get all ready transactions in execution order.
    pub fn get_ready_ordered(&self) -> Vec<(TxnId, Timestamp)> {
        let mut result = Vec::new();
        for (execute_at, ids) in &self.queue {
            for id in ids {
                if self.ready.contains(id) {
                    result.push((*id, *execute_at));
                }
            }
        }
        result
    }

    /// Remove a transaction from the queue (after execution).
    pub fn remove(&mut self, txn_id: &TxnId, execute_at: &Timestamp) {
        if let Some(ids) = self.queue.get_mut(execute_at) {
            ids.remove(txn_id);
            if ids.is_empty() {
                self.queue.remove(execute_at);
            }
        }
        self.all_ids.remove(txn_id);
        self.ready.remove(txn_id);
    }

    /// Update the execute_at timestamp for a transaction.
    pub fn update_execute_at(
        &mut self,
        txn_id: TxnId,
        old_execute_at: Timestamp,
        new_execute_at: Timestamp,
    ) {
        // Remove from old position
        if let Some(ids) = self.queue.get_mut(&old_execute_at) {
            ids.remove(&txn_id);
            if ids.is_empty() {
                self.queue.remove(&old_execute_at);
            }
        }

        // Add to new position
        self.queue.entry(new_execute_at).or_default().insert(txn_id);
    }

    /// Check if a transaction is in the queue.
    pub fn contains(&self, txn_id: &TxnId) -> bool {
        self.all_ids.contains(txn_id)
    }

    /// Check if a transaction is ready.
    pub fn is_ready(&self, txn_id: &TxnId) -> bool {
        self.ready.contains(txn_id)
    }

    /// Get the number of transactions in the queue.
    pub fn len(&self) -> usize {
        self.all_ids.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.all_ids.is_empty()
    }

    /// Get the number of ready transactions.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Peek at the earliest execute_at timestamp in the queue.
    pub fn peek_earliest_time(&self) -> Option<Timestamp> {
        self.queue.keys().next().copied()
    }

    /// Get all transactions at a specific execute_at timestamp.
    pub fn get_at_timestamp(&self, execute_at: &Timestamp) -> Option<&HashSet<TxnId>> {
        self.queue.get(execute_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_id(time: u64) -> TxnId {
        TxnId::new(Timestamp::new(time, 0, 1))
    }

    fn make_ts(time: u64) -> Timestamp {
        Timestamp::new(time, 0, 1)
    }

    #[test]
    fn test_enqueue_and_peek() {
        let mut queue = ExecutionQueue::new();

        let id1 = make_id(100);
        let id2 = make_id(101);

        queue.enqueue(id1, make_ts(200), true);
        queue.enqueue(id2, make_ts(100), true);

        // Should return id2 (earlier execute_at)
        let (next_id, _) = queue.peek_next().unwrap();
        assert_eq!(next_id, id2);
    }

    #[test]
    fn test_only_ready_returned() {
        let mut queue = ExecutionQueue::new();

        let id1 = make_id(100);
        let id2 = make_id(101);

        queue.enqueue(id1, make_ts(100), false); // Not ready
        queue.enqueue(id2, make_ts(200), true); // Ready

        // Should skip id1 and return id2
        let (next_id, _) = queue.peek_next().unwrap();
        assert_eq!(next_id, id2);
    }

    #[test]
    fn test_mark_ready() {
        let mut queue = ExecutionQueue::new();

        let id = make_id(100);
        queue.enqueue(id, make_ts(100), false);

        assert!(queue.peek_next().is_none());

        queue.mark_ready(id);

        assert!(queue.peek_next().is_some());
    }

    #[test]
    fn test_remove() {
        let mut queue = ExecutionQueue::new();

        let id = make_id(100);
        let ts = make_ts(100);

        queue.enqueue(id, ts, true);
        assert_eq!(queue.len(), 1);

        queue.remove(&id, &ts);
        assert_eq!(queue.len(), 0);
        assert!(queue.peek_next().is_none());
    }

    #[test]
    fn test_update_execute_at() {
        let mut queue = ExecutionQueue::new();

        let id1 = make_id(100);
        let id2 = make_id(101);

        queue.enqueue(id1, make_ts(200), true);
        queue.enqueue(id2, make_ts(100), true);

        // id2 should be first
        assert_eq!(queue.peek_next().unwrap().0, id2);

        // Move id1 to earlier time
        queue.update_execute_at(id1, make_ts(200), make_ts(50));

        // Now id1 should be first
        assert_eq!(queue.peek_next().unwrap().0, id1);
    }

    #[test]
    fn test_get_ready_ordered() {
        let mut queue = ExecutionQueue::new();

        let id1 = make_id(100);
        let id2 = make_id(101);
        let id3 = make_id(102);

        queue.enqueue(id1, make_ts(300), true);
        queue.enqueue(id2, make_ts(100), true);
        queue.enqueue(id3, make_ts(200), false); // Not ready

        let ready = queue.get_ready_ordered();
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].0, id2); // Earliest
        assert_eq!(ready[1].0, id1); // Later
    }

    #[test]
    fn test_peek_earliest_time() {
        let mut queue = ExecutionQueue::new();

        assert!(queue.peek_earliest_time().is_none());

        queue.enqueue(make_id(100), make_ts(200), false);
        queue.enqueue(make_id(101), make_ts(100), false);

        assert_eq!(queue.peek_earliest_time(), Some(make_ts(100)));
    }

    #[test]
    fn test_same_execute_at() {
        let mut queue = ExecutionQueue::new();

        let id1 = make_id(100);
        let id2 = make_id(101);
        let ts = make_ts(100);

        queue.enqueue(id1, ts, true);
        queue.enqueue(id2, ts, true);

        // Both at same timestamp
        let at_ts = queue.get_at_timestamp(&ts).unwrap();
        assert_eq!(at_ts.len(), 2);
    }
}
