//! Dependency graph for transaction ordering.
//!
//! Tracks dependencies between transactions and determines when
//! transactions are ready to execute (all dependencies satisfied).

use crate::txn::TxnId;
use std::collections::{HashMap, HashSet};

/// Dependency graph for tracking transaction execution order.
///
/// A transaction can only execute after all its dependencies have executed.
/// This graph tracks both forward dependencies (what I depend on) and
/// reverse dependencies (what depends on me).
#[derive(Debug, Default)]
pub struct DependencyGraph {
    /// txn_id -> set of transactions it depends on (must execute first)
    deps: HashMap<TxnId, HashSet<TxnId>>,

    /// txn_id -> set of transactions that depend on it (waiting for it)
    reverse_deps: HashMap<TxnId, HashSet<TxnId>>,

    /// Transactions with all dependencies satisfied (ready to execute)
    ready: HashSet<TxnId>,

    /// Transactions that have been executed
    executed: HashSet<TxnId>,
}

impl DependencyGraph {
    /// Create a new empty dependency graph
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a transaction with its dependencies.
    ///
    /// If all dependencies are already executed, the transaction is marked as ready.
    pub fn register(&mut self, txn_id: TxnId, dependencies: HashSet<TxnId>) {
        // Filter out self-dependency and already executed deps
        let pending_deps: HashSet<TxnId> = dependencies
            .into_iter()
            .filter(|dep| *dep != txn_id && !self.executed.contains(dep))
            .collect();

        // Add reverse dependencies
        for dep in &pending_deps {
            self.reverse_deps.entry(*dep).or_default().insert(txn_id);
        }

        // If no pending deps, mark as ready
        if pending_deps.is_empty() {
            self.ready.insert(txn_id);
        }

        self.deps.insert(txn_id, pending_deps);
    }

    /// Update dependencies for an existing transaction.
    ///
    /// Used when Accept phase determines final dependencies.
    pub fn update_deps(&mut self, txn_id: TxnId, new_deps: HashSet<TxnId>) {
        // Remove old reverse dependencies
        if let Some(old_deps) = self.deps.get(&txn_id) {
            for dep in old_deps {
                if let Some(rdeps) = self.reverse_deps.get_mut(dep) {
                    rdeps.remove(&txn_id);
                }
            }
        }

        // Remove from ready set (will re-evaluate)
        self.ready.remove(&txn_id);

        // Re-register with new dependencies
        self.register(txn_id, new_deps);
    }

    /// Mark a transaction as executed.
    ///
    /// Returns the list of transactions that became ready as a result.
    pub fn mark_executed(&mut self, txn_id: TxnId) -> Vec<TxnId> {
        self.ready.remove(&txn_id);
        self.executed.insert(txn_id);

        let mut newly_ready = Vec::new();

        // Update all transactions that were waiting on this one
        if let Some(waiting) = self.reverse_deps.remove(&txn_id) {
            for waiting_id in waiting {
                if let Some(deps) = self.deps.get_mut(&waiting_id) {
                    deps.remove(&txn_id);

                    // Check if this transaction is now ready
                    if deps.is_empty() && !self.executed.contains(&waiting_id) {
                        self.ready.insert(waiting_id);
                        newly_ready.push(waiting_id);
                    }
                }
            }
        }

        // Clean up
        self.deps.remove(&txn_id);

        newly_ready
    }

    /// Check if a transaction is ready to execute.
    pub fn is_ready(&self, txn_id: &TxnId) -> bool {
        self.ready.contains(txn_id)
    }

    /// Check if a transaction has been executed.
    pub fn is_executed(&self, txn_id: &TxnId) -> bool {
        self.executed.contains(txn_id)
    }

    /// Get all transactions that are ready to execute.
    pub fn get_ready(&self) -> impl Iterator<Item = &TxnId> {
        self.ready.iter()
    }

    /// Get the number of ready transactions.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Get pending dependencies for a transaction.
    pub fn get_pending_deps(&self, txn_id: &TxnId) -> Option<&HashSet<TxnId>> {
        self.deps.get(txn_id)
    }

    /// Get transactions waiting on a specific transaction.
    pub fn get_dependents(&self, txn_id: &TxnId) -> Option<&HashSet<TxnId>> {
        self.reverse_deps.get(txn_id)
    }

    /// Remove a transaction completely (e.g., if it was never committed).
    pub fn remove(&mut self, txn_id: &TxnId) {
        self.ready.remove(txn_id);
        self.executed.remove(txn_id);

        // Remove from deps and update reverse deps
        if let Some(deps) = self.deps.remove(txn_id) {
            for dep in deps {
                if let Some(rdeps) = self.reverse_deps.get_mut(&dep) {
                    rdeps.remove(txn_id);
                }
            }
        }

        // Remove from reverse deps
        self.reverse_deps.remove(txn_id);
    }

    /// Check if the graph contains a transaction.
    pub fn contains(&self, txn_id: &TxnId) -> bool {
        self.deps.contains_key(txn_id) || self.executed.contains(txn_id)
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> DependencyGraphStats {
        DependencyGraphStats {
            total_tracked: self.deps.len(),
            ready_count: self.ready.len(),
            executed_count: self.executed.len(),
        }
    }
}

/// Statistics about the dependency graph.
#[derive(Debug, Clone, Copy)]
pub struct DependencyGraphStats {
    pub total_tracked: usize,
    pub ready_count: usize,
    pub executed_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;

    fn make_id(time: u64) -> TxnId {
        TxnId::new(Timestamp::new(time, 0, 1))
    }

    #[test]
    fn test_register_no_deps() {
        let mut graph = DependencyGraph::new();
        let id = make_id(100);

        graph.register(id, HashSet::new());

        assert!(graph.is_ready(&id));
        assert!(!graph.is_executed(&id));
    }

    #[test]
    fn test_register_with_deps() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::from([id1]));

        assert!(graph.is_ready(&id1));
        assert!(!graph.is_ready(&id2)); // Waiting on id1
    }

    #[test]
    fn test_mark_executed_unblocks_dependents() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);
        let id3 = make_id(102);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::from([id1]));
        graph.register(id3, HashSet::from([id1, id2]));

        assert!(graph.is_ready(&id1));
        assert!(!graph.is_ready(&id2));
        assert!(!graph.is_ready(&id3));

        // Execute id1
        let newly_ready = graph.mark_executed(id1);
        assert_eq!(newly_ready, vec![id2]);
        assert!(graph.is_executed(&id1));
        assert!(graph.is_ready(&id2));
        assert!(!graph.is_ready(&id3)); // Still waiting on id2

        // Execute id2
        let newly_ready = graph.mark_executed(id2);
        assert_eq!(newly_ready, vec![id3]);
        assert!(graph.is_ready(&id3));
    }

    #[test]
    fn test_register_with_already_executed_deps() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);

        // Register and execute id1
        graph.register(id1, HashSet::new());
        graph.mark_executed(id1);

        // Register id2 with dep on already-executed id1
        graph.register(id2, HashSet::from([id1]));

        // id2 should be immediately ready
        assert!(graph.is_ready(&id2));
    }

    #[test]
    fn test_update_deps() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);
        let id3 = make_id(102);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::new());
        graph.register(id3, HashSet::from([id1]));

        // id3 is not ready because id1 hasn't been executed yet
        assert!(!graph.is_ready(&id3));

        // Update id3 to also depend on id2
        graph.update_deps(id3, HashSet::from([id1, id2]));

        // Still not ready (depends on both)
        assert!(!graph.is_ready(&id3));

        // After both execute, id3 should be ready
        graph.mark_executed(id1);
        assert!(!graph.is_ready(&id3));
        graph.mark_executed(id2);
        assert!(graph.is_ready(&id3));
    }

    #[test]
    fn test_self_dependency_ignored() {
        let mut graph = DependencyGraph::new();
        let id = make_id(100);

        graph.register(id, HashSet::from([id])); // Self-dependency

        assert!(graph.is_ready(&id)); // Should still be ready
    }

    #[test]
    fn test_remove() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::from([id1]));

        graph.remove(&id2);

        assert!(!graph.contains(&id2));
        assert!(graph
            .get_dependents(&id1)
            .map(|s| s.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn test_get_pending_deps() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::from([id1]));

        let deps = graph.get_pending_deps(&id2).unwrap();
        assert!(deps.contains(&id1));
    }

    #[test]
    fn test_stats() {
        let mut graph = DependencyGraph::new();
        let id1 = make_id(100);
        let id2 = make_id(101);

        graph.register(id1, HashSet::new());
        graph.register(id2, HashSet::from([id1]));
        graph.mark_executed(id1);

        let stats = graph.stats();
        assert_eq!(stats.total_tracked, 1); // Only id2 still tracked
        assert_eq!(stats.ready_count, 1); // id2 is ready
        assert_eq!(stats.executed_count, 1); // id1 executed
    }
}
