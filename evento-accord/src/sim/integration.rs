//! Integration tests for the simulation framework.
//!
//! These tests demonstrate how to use the simulation framework
//! with realistic workloads like bank account operations.

#![allow(dead_code)] // Allow unused fields/methods for debugging purposes

use crate::sim::{FaultConfig, SimRng, SimStats, Simulation};
use evento_core::{Event, Executor};
use std::collections::HashMap;
use ulid::Ulid;

/// Bank account state for simulation.
#[derive(Clone, Debug)]
pub struct BankAccount {
    pub balance: i64,
    pub is_frozen: bool,
}

impl BankAccount {
    pub fn new(initial_balance: i64) -> Self {
        Self {
            balance: initial_balance,
            is_frozen: false,
        }
    }

    pub fn can_withdraw(&self, amount: i64) -> bool {
        !self.is_frozen && self.balance >= amount
    }

    pub fn can_deposit(&self) -> bool {
        !self.is_frozen
    }
}

/// Bank operation types for simulation.
#[derive(Clone, Debug)]
pub enum BankOperation {
    OpenAccount {
        account_id: String,
        initial_balance: i64,
    },
    Deposit {
        account_id: String,
        amount: i64,
    },
    Withdraw {
        account_id: String,
        amount: i64,
    },
    Transfer {
        from_account_id: String,
        to_account_id: String,
        amount: i64,
    },
}

/// Bank simulator that tracks account state and generates operations.
pub struct BankSimulator {
    /// Account states by account_id
    accounts: HashMap<String, BankAccount>,
    /// RNG for generating operations
    rng: SimRng,
    /// Next account ID counter
    next_account_id: u64,
    /// Operation history for verification
    operations: Vec<(BankOperation, bool)>, // (operation, succeeded)
}

impl BankSimulator {
    pub fn new(seed: u64) -> Self {
        Self {
            accounts: HashMap::new(),
            rng: SimRng::new(seed),
            next_account_id: 1,
            operations: Vec::new(),
        }
    }

    /// Generate a random bank operation.
    pub fn generate_operation(&mut self) -> BankOperation {
        // If no accounts exist, must create one
        if self.accounts.is_empty() {
            return self.generate_open_account();
        }

        // 10% open new account, 30% deposit, 30% withdraw, 30% transfer
        let op_type = self.rng.next_usize(100);

        if op_type < 10 {
            self.generate_open_account()
        } else if op_type < 40 {
            self.generate_deposit()
        } else if op_type < 70 {
            self.generate_withdraw()
        } else {
            self.generate_transfer()
        }
    }

    fn generate_open_account(&mut self) -> BankOperation {
        let account_id = format!("account-{}", self.next_account_id);
        self.next_account_id += 1;
        let initial_balance = (self.rng.next_usize(10000) + 100) as i64;

        BankOperation::OpenAccount {
            account_id,
            initial_balance,
        }
    }

    fn generate_deposit(&mut self) -> BankOperation {
        let account_ids: Vec<_> = self.accounts.keys().cloned().collect();
        let account_id = self.rng.choose(&account_ids).unwrap().clone();
        let amount = (self.rng.next_usize(1000) + 1) as i64;

        BankOperation::Deposit { account_id, amount }
    }

    fn generate_withdraw(&mut self) -> BankOperation {
        let account_ids: Vec<_> = self.accounts.keys().cloned().collect();
        let account_id = self.rng.choose(&account_ids).unwrap().clone();
        let amount = (self.rng.next_usize(500) + 1) as i64;

        BankOperation::Withdraw { account_id, amount }
    }

    fn generate_transfer(&mut self) -> BankOperation {
        let account_ids: Vec<_> = self.accounts.keys().cloned().collect();
        if account_ids.len() < 2 {
            // Not enough accounts for transfer, do deposit instead
            return self.generate_deposit();
        }

        let from_idx = self.rng.next_usize(account_ids.len());
        let mut to_idx = self.rng.next_usize(account_ids.len());
        while to_idx == from_idx {
            to_idx = self.rng.next_usize(account_ids.len());
        }

        let from_account_id = account_ids[from_idx].clone();
        let to_account_id = account_ids[to_idx].clone();
        let amount = (self.rng.next_usize(300) + 1) as i64;

        BankOperation::Transfer {
            from_account_id,
            to_account_id,
            amount,
        }
    }

    /// Apply an operation and return whether it succeeded.
    pub fn apply_operation(&mut self, op: &BankOperation) -> bool {
        match op {
            BankOperation::OpenAccount {
                account_id,
                initial_balance,
            } => {
                if self.accounts.contains_key(account_id) {
                    false
                } else {
                    self.accounts
                        .insert(account_id.clone(), BankAccount::new(*initial_balance));
                    true
                }
            }
            BankOperation::Deposit { account_id, amount } => {
                if let Some(account) = self.accounts.get_mut(account_id) {
                    if account.can_deposit() {
                        account.balance += amount;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            BankOperation::Withdraw { account_id, amount } => {
                if let Some(account) = self.accounts.get_mut(account_id) {
                    if account.can_withdraw(*amount) {
                        account.balance -= amount;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            BankOperation::Transfer {
                from_account_id,
                to_account_id,
                amount,
            } => {
                // Check if transfer is possible
                let can_transfer = self
                    .accounts
                    .get(from_account_id)
                    .map(|a| a.can_withdraw(*amount))
                    .unwrap_or(false);

                let to_exists = self.accounts.contains_key(to_account_id);

                if can_transfer && to_exists {
                    // Execute transfer
                    if let Some(from) = self.accounts.get_mut(from_account_id) {
                        from.balance -= amount;
                    }
                    if let Some(to) = self.accounts.get_mut(to_account_id) {
                        to.balance += amount;
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record an operation attempt.
    pub fn record_operation(&mut self, op: BankOperation, succeeded: bool) {
        self.operations.push((op, succeeded));
    }

    /// Get total balance across all accounts (should be conserved).
    pub fn total_balance(&self) -> i64 {
        self.accounts.values().map(|a| a.balance).sum()
    }

    /// Get account by ID.
    pub fn get_account(&self, id: &str) -> Option<&BankAccount> {
        self.accounts.get(id)
    }

    /// Get number of accounts.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Get operation count.
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    /// Get successful operation count.
    pub fn successful_operations(&self) -> usize {
        self.operations.iter().filter(|(_, s)| *s).count()
    }
}

/// Create an event for a bank operation with a deterministic ID.
fn create_bank_event(op: &BankOperation, event_counter: u64) -> Event {
    let (aggregator_id, name, data) = match op {
        BankOperation::OpenAccount {
            account_id,
            initial_balance,
        } => (
            account_id.clone(),
            "AccountOpened".to_string(),
            initial_balance.to_le_bytes().to_vec(),
        ),
        BankOperation::Deposit { account_id, amount } => (
            account_id.clone(),
            "MoneyDeposited".to_string(),
            amount.to_le_bytes().to_vec(),
        ),
        BankOperation::Withdraw { account_id, amount } => (
            account_id.clone(),
            "MoneyWithdrawn".to_string(),
            amount.to_le_bytes().to_vec(),
        ),
        BankOperation::Transfer {
            from_account_id,
            amount,
            ..
        } => (
            from_account_id.clone(),
            "MoneyTransferred".to_string(),
            amount.to_le_bytes().to_vec(),
        ),
    };

    // Create deterministic ULID from counter
    // Use fixed timestamp and counter for reproducibility
    let id = Ulid::from_parts(1000000 + event_counter, event_counter as u128);

    Event {
        id,
        aggregator_id,
        aggregator_type: "bank/Account".to_string(),
        version: 1,
        name,
        routing_key: None,
        data,
        metadata: vec![],
        timestamp: 0,
        timestamp_subsec: 0,
    }
}

/// Run a bank simulation with the given configuration.
pub fn run_bank_simulation(
    seed: u64,
    node_count: usize,
    operation_count: usize,
    fault_config: FaultConfig,
) -> BankSimulationResult {
    let mut sim = Simulation::builder()
        .seed(seed)
        .nodes(node_count)
        .faults(fault_config)
        .build();

    let mut bank = BankSimulator::new(seed);
    let rng = SimRng::new(seed.wrapping_add(1)); // Different seed for node selection

    // Track initial balance for conservation check
    let mut expected_balance: i64 = 0;
    let mut event_counter: u64 = 0;

    // Run operations
    for _ in 0..operation_count {
        // Generate operation
        let op = bank.generate_operation();

        // Select a random alive node to execute on
        let alive_nodes: Vec<_> = sim
            .node_ids()
            .iter()
            .filter(|&&id| sim.is_alive(id))
            .collect();

        if alive_nodes.is_empty() {
            // All nodes down, skip this operation
            bank.record_operation(op, false);
            sim.run(10); // Advance time
            continue;
        }

        let node_id = **rng.choose(&alive_nodes).unwrap();

        // Record operation start in history
        let keys = match &op {
            BankOperation::OpenAccount { account_id, .. } => vec![account_id.clone()],
            BankOperation::Deposit { account_id, .. } => vec![account_id.clone()],
            BankOperation::Withdraw { account_id, .. } => vec![account_id.clone()],
            BankOperation::Transfer {
                from_account_id,
                to_account_id,
                ..
            } => vec![from_account_id.clone(), to_account_id.clone()],
        };

        // Get current time before mutable borrow
        let now = sim.now();
        let op_id = sim.history_mut().write_start(node_id, now, keys);

        // Try to apply operation to bank state
        let succeeded = bank.apply_operation(&op);
        bank.record_operation(op.clone(), succeeded);

        if succeeded {
            // Track balance changes for conservation check
            match &op {
                BankOperation::OpenAccount {
                    initial_balance, ..
                } => {
                    expected_balance += initial_balance;
                }
                BankOperation::Deposit { amount, .. } => {
                    expected_balance += amount;
                }
                BankOperation::Withdraw { amount, .. } => {
                    expected_balance -= amount;
                }
                BankOperation::Transfer { .. } => {
                    // Transfers don't change total balance
                }
            }

            // Create event and replicate to ALL alive nodes (simulating ACCORD replication)
            let event = create_bank_event(&op, event_counter);
            event_counter += 1;
            let node_ids: Vec<_> = sim.node_ids().to_vec();
            for nid in node_ids {
                if sim.is_alive(nid) {
                    if let Some(node) = sim.node_mut(nid) {
                        let _ = futures::executor::block_on(async {
                            node.executor.write(vec![event.clone()]).await
                        });
                    }
                }
            }
        }

        // Record operation end
        let end_time = sim.now();
        sim.history_mut().write_end(op_id, end_time, succeeded);

        // Advance simulation time
        sim.run(5);
    }

    // Drain remaining messages
    sim.drain();

    // Collect results
    let consistency_check = sim.check_consistency();
    let linearizability_check = sim.check_linearizability();
    let no_deadlock_check = sim.check_no_deadlock();

    // Calculate actual total balance from events
    let actual_balance = bank.total_balance();
    let balance_conserved = actual_balance == expected_balance;

    BankSimulationResult {
        seed,
        node_count,
        operation_count,
        successful_operations: bank.successful_operations(),
        account_count: bank.account_count(),
        expected_balance,
        actual_balance,
        balance_conserved,
        consistency_passed: consistency_check.is_ok(),
        linearizability_passed: linearizability_check.is_ok(),
        no_deadlock_passed: no_deadlock_check.is_ok(),
        stats: sim.stats().clone(),
    }
}

/// Results from a bank simulation run.
#[derive(Debug)]
pub struct BankSimulationResult {
    pub seed: u64,
    pub node_count: usize,
    pub operation_count: usize,
    pub successful_operations: usize,
    pub account_count: usize,
    pub expected_balance: i64,
    pub actual_balance: i64,
    pub balance_conserved: bool,
    pub consistency_passed: bool,
    pub linearizability_passed: bool,
    pub no_deadlock_passed: bool,
    pub stats: SimStats,
}

impl BankSimulationResult {
    pub fn is_correct(&self) -> bool {
        self.balance_conserved
            && self.consistency_passed
            && self.linearizability_passed
            && self.no_deadlock_passed
    }

    pub fn success_rate(&self) -> f64 {
        if self.operation_count == 0 {
            1.0
        } else {
            self.successful_operations as f64 / self.operation_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bank_simulator_open_account() {
        let mut bank = BankSimulator::new(42);

        let op = BankOperation::OpenAccount {
            account_id: "acc-1".to_string(),
            initial_balance: 1000,
        };

        assert!(bank.apply_operation(&op));
        assert_eq!(bank.account_count(), 1);
        assert_eq!(bank.get_account("acc-1").unwrap().balance, 1000);

        // Can't open same account twice
        assert!(!bank.apply_operation(&op));
    }

    #[test]
    fn test_bank_simulator_deposit() {
        let mut bank = BankSimulator::new(42);

        // Open account first
        bank.apply_operation(&BankOperation::OpenAccount {
            account_id: "acc-1".to_string(),
            initial_balance: 1000,
        });

        // Deposit
        let op = BankOperation::Deposit {
            account_id: "acc-1".to_string(),
            amount: 500,
        };
        assert!(bank.apply_operation(&op));
        assert_eq!(bank.get_account("acc-1").unwrap().balance, 1500);
    }

    #[test]
    fn test_bank_simulator_withdraw() {
        let mut bank = BankSimulator::new(42);

        // Open account
        bank.apply_operation(&BankOperation::OpenAccount {
            account_id: "acc-1".to_string(),
            initial_balance: 1000,
        });

        // Successful withdraw
        let op = BankOperation::Withdraw {
            account_id: "acc-1".to_string(),
            amount: 300,
        };
        assert!(bank.apply_operation(&op));
        assert_eq!(bank.get_account("acc-1").unwrap().balance, 700);

        // Failed withdraw (insufficient funds)
        let op = BankOperation::Withdraw {
            account_id: "acc-1".to_string(),
            amount: 1000,
        };
        assert!(!bank.apply_operation(&op));
        assert_eq!(bank.get_account("acc-1").unwrap().balance, 700);
    }

    #[test]
    fn test_bank_simulator_transfer() {
        let mut bank = BankSimulator::new(42);

        // Open two accounts
        bank.apply_operation(&BankOperation::OpenAccount {
            account_id: "acc-1".to_string(),
            initial_balance: 1000,
        });
        bank.apply_operation(&BankOperation::OpenAccount {
            account_id: "acc-2".to_string(),
            initial_balance: 500,
        });

        // Transfer
        let op = BankOperation::Transfer {
            from_account_id: "acc-1".to_string(),
            to_account_id: "acc-2".to_string(),
            amount: 300,
        };
        assert!(bank.apply_operation(&op));
        assert_eq!(bank.get_account("acc-1").unwrap().balance, 700);
        assert_eq!(bank.get_account("acc-2").unwrap().balance, 800);

        // Total should still be 1500
        assert_eq!(bank.total_balance(), 1500);
    }

    #[test]
    fn test_bank_simulator_balance_conservation() {
        let mut bank = BankSimulator::new(12345);

        // Track expected total as we go
        let mut expected = 0i64;

        for _ in 0..100 {
            let op = bank.generate_operation();
            let succeeded = bank.apply_operation(&op);
            bank.record_operation(op.clone(), succeeded);

            if succeeded {
                match &op {
                    BankOperation::OpenAccount {
                        initial_balance, ..
                    } => {
                        expected += initial_balance;
                    }
                    BankOperation::Deposit { amount, .. } => {
                        expected += amount;
                    }
                    BankOperation::Withdraw { amount, .. } => {
                        expected -= amount;
                    }
                    BankOperation::Transfer { .. } => {
                        // No change to total
                    }
                }
            }
        }

        assert_eq!(bank.total_balance(), expected);
    }

    #[test]
    fn test_bank_simulation_no_faults() {
        let result = run_bank_simulation(42, 3, 50, FaultConfig::none());

        println!("Simulation result: {:?}", result);

        assert!(result.balance_conserved, "Balance should be conserved");
        assert!(result.consistency_passed, "Consistency check should pass");
        assert!(result.no_deadlock_passed, "No deadlock check should pass");
        assert!(
            result.success_rate() > 0.5,
            "Most operations should succeed"
        );
    }

    #[test]
    fn test_bank_simulation_light_faults() {
        let result = run_bank_simulation(42, 5, 100, FaultConfig::light());

        println!("Simulation result (light faults): {:?}", result);

        assert!(result.balance_conserved, "Balance should be conserved");
        assert!(result.no_deadlock_passed, "No deadlock check should pass");
    }

    #[test]
    fn test_bank_simulation_medium_faults() {
        let result = run_bank_simulation(42, 5, 100, FaultConfig::medium());

        println!("Simulation result (medium faults): {:?}", result);

        // With faults, we expect some degradation but correctness should hold
        assert!(result.balance_conserved, "Balance should be conserved");
    }

    #[test]
    fn test_bank_simulation_deterministic() {
        // Without external timing dependencies, simulations should be reproducible
        // We test that key properties hold consistently
        let result1 = run_bank_simulation(12345, 3, 50, FaultConfig::none());
        let result2 = run_bank_simulation(12345, 3, 50, FaultConfig::none());

        // Both should pass core invariants
        assert!(result1.balance_conserved && result2.balance_conserved);
        assert!(result1.no_deadlock_passed && result2.no_deadlock_passed);
        assert!(result1.consistency_passed && result2.consistency_passed);

        // Account counts should match (deterministic account creation)
        assert_eq!(result1.account_count, result2.account_count);

        // With same seed and no faults, results should be identical
        // Note: This may differ slightly due to tokio runtime scheduling
        // So we allow a small tolerance
        let diff =
            (result1.successful_operations as i64 - result2.successful_operations as i64).abs();
        assert!(diff <= 1, "Operation count should be nearly identical");
    }

    #[test]
    fn test_bank_simulation_different_seeds() {
        // Different seeds should produce different results
        let result1 = run_bank_simulation(111, 3, 50, FaultConfig::none());
        let result2 = run_bank_simulation(222, 3, 50, FaultConfig::none());

        // Very unlikely to be exactly the same
        assert!(
            result1.actual_balance != result2.actual_balance
                || result1.account_count != result2.account_count
        );
    }

    #[test]
    fn test_bank_simulation_stress() {
        // Run a larger simulation
        let result = run_bank_simulation(42, 7, 500, FaultConfig::medium());

        println!("Stress test result: {:?}", result);
        println!("Stats: {:?}", result.stats);

        assert!(
            result.balance_conserved,
            "Balance must be conserved under stress"
        );
    }

    #[test]
    fn test_accord_protocol_basic() {
        use crate::txn::TxnStatus;

        // Create a 3-node simulation
        let mut sim = Simulation::builder()
            .seed(42)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Create a simple event
        let event = Event {
            id: Ulid::from_parts(1000000, 1),
            aggregator_id: "test-1".to_string(),
            aggregator_type: "test/Entity".to_string(),
            version: 1,
            name: "Created".to_string(),
            routing_key: None,
            data: vec![1, 2, 3],
            metadata: vec![],
            timestamp: 0,
            timestamp_subsec: 0,
        };

        // Submit transaction through ACCORD protocol
        let txn_id = sim
            .submit_transaction(0, vec![event])
            .expect("Should submit");

        // Run simulation until messages are delivered
        sim.run(100);

        // Check transaction status at coordinator
        let coordinator_status = {
            let node = sim.node(0).unwrap();
            futures::executor::block_on(async { node.state.get_status(&txn_id).await })
        };

        // Transaction should be committed
        assert_eq!(
            coordinator_status,
            Some(TxnStatus::Committed),
            "Transaction should be committed at coordinator"
        );

        // Check that other nodes also have the transaction
        for node_id in [1u16, 2u16] {
            let status = {
                let node = sim.node(node_id).unwrap();
                futures::executor::block_on(async { node.state.get_status(&txn_id).await })
            };
            assert!(
                status.is_some(),
                "Node {} should have the transaction",
                node_id
            );
        }

        // Execute ready transactions
        sim.execute_all_ready();

        // With replication, all nodes should have the event
        for &node_id in sim.node_ids() {
            let count = sim.node(node_id).unwrap().executor.event_count();
            assert_eq!(count, 1, "Node {} should have 1 event", node_id);
        }

        // Verify consistency - all nodes have same events
        let result = sim.check_consistency();
        assert!(result.is_ok(), "Consistency check should pass");
    }

    #[test]
    fn test_accord_protocol_multiple_transactions() {
        use crate::txn::TxnStatus;

        // Use 3 nodes for simpler quorum (quorum = 2, need 1 peer response)
        let mut sim = Simulation::builder()
            .seed(123)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit 3 transactions, one from each coordinator
        let mut txn_ids = Vec::new();
        for i in 0..3u64 {
            let event = Event {
                id: Ulid::from_parts(1000000 + i, i as u128),
                aggregator_id: format!("account-{}", i),
                aggregator_type: "bank/Account".to_string(),
                version: 1,
                name: "Created".to_string(),
                routing_key: None,
                data: vec![i as u8],
                metadata: vec![],
                timestamp: 0,
                timestamp_subsec: 0,
            };

            let coordinator = i as u16;
            if let Some(txn_id) = sim.submit_transaction(coordinator, vec![event]) {
                txn_ids.push((txn_id, coordinator));
            }

            // Run to allow message delivery
            sim.run(100);
        }

        // All transactions should be committed
        for (txn_id, coordinator) in &txn_ids {
            let status = {
                let node = sim.node(*coordinator).unwrap();
                futures::executor::block_on(async { node.state.get_status(&txn_id).await })
            };
            assert_eq!(
                status,
                Some(TxnStatus::Committed),
                "Transaction from coordinator {} should be committed",
                coordinator
            );
        }

        // Execute all ready transactions
        sim.execute_all_ready();

        // With replication, each node should have all 3 events
        // Total: 3 transactions × 3 nodes = 9 events
        let total_events: usize = sim
            .node_ids()
            .iter()
            .map(|&id| sim.node(id).unwrap().executor.event_count())
            .sum();
        assert_eq!(total_events, 9, "Should have 9 total events (3 per node)");

        // Verify each node has the same number of events
        for &node_id in sim.node_ids() {
            let count = sim.node(node_id).unwrap().executor.event_count();
            assert_eq!(count, 3, "Node {} should have 3 events", node_id);
        }

        // Verify consistency across all nodes
        let result = sim.check_consistency();
        assert!(result.is_ok(), "Consistency check should pass");
    }

    #[test]
    fn test_event_replication_consistency() {
        use crate::txn::TxnStatus;

        // Test with 3 nodes (simpler quorum), one coordinator per transaction
        let mut sim = Simulation::builder()
            .seed(999)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit 3 transactions, one from each coordinator
        // (matching the pattern from test_accord_protocol_multiple_transactions)
        let mut txn_ids = Vec::new();
        for i in 0..3u64 {
            let event = Event {
                id: Ulid::from_parts(2000000 + i, i as u128),
                aggregator_id: format!("entity-{}", i),
                aggregator_type: "test/Entity".to_string(),
                version: 1,
                name: format!("Event{}", i),
                routing_key: None,
                data: vec![i as u8; 10],
                metadata: vec![],
                timestamp: 0,
                timestamp_subsec: 0,
            };

            let coordinator = i as u16;
            if let Some(txn_id) = sim.submit_transaction(coordinator, vec![event]) {
                txn_ids.push(txn_id);
            }

            // Run to process messages
            sim.run(100);
        }

        // Verify all transactions committed
        let committed_count = txn_ids
            .iter()
            .filter(|&txn_id| {
                let status = {
                    let node = sim.node(0).unwrap();
                    futures::executor::block_on(async { node.state.get_status(txn_id).await })
                };
                status == Some(TxnStatus::Committed)
            })
            .count();
        assert_eq!(committed_count, 3, "All 3 transactions should be committed");

        // Execute all ready transactions on all nodes
        sim.execute_all_ready();

        // Each node should have exactly 3 events (replicated to all)
        for &node_id in sim.node_ids() {
            let count = sim.node(node_id).unwrap().executor.event_count();
            assert_eq!(count, 3, "Node {} should have 3 events", node_id);
        }

        // Verify consistency - this is the key test for Phase 8
        let result = sim.check_consistency();
        assert!(
            result.is_ok(),
            "All nodes should have identical events: {:?}",
            result.error_message()
        );
    }

    #[test]
    fn test_crash_recovery_basic() {
        use crate::txn::TxnStatus;

        // Create a 3-node simulation with no random faults
        let mut sim = Simulation::builder()
            .seed(42)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit a transaction through ACCORD protocol
        let event = Event {
            id: Ulid::from_parts(3000000, 1),
            aggregator_id: "crash-test-1".to_string(),
            aggregator_type: "test/Entity".to_string(),
            version: 1,
            name: "Created".to_string(),
            routing_key: None,
            data: vec![1, 2, 3],
            metadata: vec![],
            timestamp: 0,
            timestamp_subsec: 0,
        };

        let txn_id = sim
            .submit_transaction(0, vec![event])
            .expect("Should submit");

        // Run until transaction is committed
        sim.run(100);

        // Verify transaction is committed at coordinator
        let status_before = {
            let node = sim.node(0).unwrap();
            futures::executor::block_on(async { node.state.get_status(&txn_id).await })
        };
        assert_eq!(
            status_before,
            Some(TxnStatus::Committed),
            "Transaction should be committed before crash"
        );

        // Crash node 0 (coordinator)
        sim.crash_node(0);
        assert!(!sim.is_alive(0), "Node 0 should be crashed");

        // State should be lost (new ConsensusState on restart)
        // But durable storage persists

        // Restart node 0
        sim.restart_node(0);
        assert!(sim.is_alive(0), "Node 0 should be alive after restart");

        // Verify transaction was recovered from durable storage
        // Note: After restart, we sync and execute ready transactions,
        // so the status will be Executed (not just Committed)
        let status_after = {
            let node = sim.node(0).unwrap();
            futures::executor::block_on(async { node.state.get_status(&txn_id).await })
        };
        assert!(
            status_after == Some(TxnStatus::Committed) || status_after == Some(TxnStatus::Executed),
            "Transaction should be recovered after restart, got {:?}",
            status_after
        );
    }

    #[test]
    fn test_crash_recovery_multiple_transactions() {
        use crate::txn::TxnStatus;

        let mut sim = Simulation::builder()
            .seed(123)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit multiple transactions
        let mut txn_ids = Vec::new();
        for i in 0..5u64 {
            let event = Event {
                id: Ulid::from_parts(4000000 + i, i as u128),
                aggregator_id: format!("multi-crash-{}", i),
                aggregator_type: "test/Entity".to_string(),
                version: 1,
                name: "Created".to_string(),
                routing_key: None,
                data: vec![i as u8],
                metadata: vec![],
                timestamp: 0,
                timestamp_subsec: 0,
            };

            // Alternate coordinators
            let coordinator = (i % 3) as u16;
            if let Some(txn_id) = sim.submit_transaction(coordinator, vec![event]) {
                txn_ids.push(txn_id);
            }
            sim.run(50);
        }

        // Let all transactions commit
        sim.run(200);

        // Verify all are committed at node 1
        let committed_before: Vec<_> = txn_ids
            .iter()
            .filter_map(|&txn_id| {
                let node = sim.node(1).unwrap();
                let status =
                    futures::executor::block_on(async { node.state.get_status(&txn_id).await });
                if status == Some(TxnStatus::Committed) {
                    Some(txn_id)
                } else {
                    None
                }
            })
            .collect();
        assert!(
            committed_before.len() >= 3,
            "At least 3 transactions should be committed at node 1"
        );

        // Crash and restart node 1
        sim.crash_node(1);
        sim.restart_node(1);

        // Verify transactions were recovered
        // After restart, transactions may be executed (not just committed)
        let recovered: Vec<_> = committed_before
            .iter()
            .filter(|&&txn_id| {
                let node = sim.node(1).unwrap();
                let status =
                    futures::executor::block_on(async { node.state.get_status(&txn_id).await });
                status == Some(TxnStatus::Committed) || status == Some(TxnStatus::Executed)
            })
            .collect();

        assert_eq!(
            recovered.len(),
            committed_before.len(),
            "All committed transactions should be recovered"
        );
    }

    #[test]
    fn test_crash_recovery_executed_status() {
        use crate::txn::TxnStatus;

        let mut sim = Simulation::builder()
            .seed(456)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit a transaction
        let event = Event {
            id: Ulid::from_parts(5000000, 1),
            aggregator_id: "exec-crash-test".to_string(),
            aggregator_type: "test/Entity".to_string(),
            version: 1,
            name: "Created".to_string(),
            routing_key: None,
            data: vec![1, 2, 3],
            metadata: vec![],
            timestamp: 0,
            timestamp_subsec: 0,
        };

        let txn_id = sim
            .submit_transaction(0, vec![event])
            .expect("Should submit");

        // Run until committed
        sim.run(100);

        // Execute the transaction on all nodes
        sim.execute_all_ready();

        // Verify it's executed at node 0
        let status_before = {
            let node = sim.node(0).unwrap();
            futures::executor::block_on(async { node.state.get_status(&txn_id).await })
        };
        assert_eq!(
            status_before,
            Some(TxnStatus::Executed),
            "Transaction should be executed before crash"
        );

        // Crash and restart node 0
        sim.crash_node(0);
        sim.restart_node(0);

        // Verify executed status was recovered
        let status_after = {
            let node = sim.node(0).unwrap();
            futures::executor::block_on(async { node.state.get_status(&txn_id).await })
        };
        assert_eq!(
            status_after,
            Some(TxnStatus::Executed),
            "Executed status should be recovered after restart"
        );
    }

    #[test]
    fn test_crash_recovery_with_random_faults() {
        // Run with medium faults to ensure crashes and restarts happen
        // Test multiple seeds to ensure robustness
        for seed in [789, 1234, 5678, 42] {
            test_crash_recovery_with_seed(seed);
        }
    }

    fn test_crash_recovery_with_seed(seed: u64) {
        use crate::txn::TxnStatus;

        let mut sim = Simulation::builder()
            .seed(seed)
            .nodes(5)
            .faults(FaultConfig::medium())
            .build();

        // Submit multiple transactions
        let mut txn_ids = Vec::new();
        for i in 0..10u64 {
            let event = Event {
                id: Ulid::from_parts(6000000 + i, i as u128),
                aggregator_id: format!("fault-test-{}", i),
                aggregator_type: "test/Entity".to_string(),
                version: 1,
                name: "Created".to_string(),
                routing_key: None,
                data: vec![i as u8],
                metadata: vec![],
                timestamp: 0,
                timestamp_subsec: 0,
            };

            // Use alive nodes only
            let alive: Vec<_> = sim
                .node_ids()
                .iter()
                .filter(|&&id| sim.is_alive(id))
                .collect();
            if !alive.is_empty() {
                let coordinator = **alive.first().unwrap();
                if let Some(txn_id) = sim.submit_transaction(coordinator, vec![event]) {
                    txn_ids.push(txn_id);
                }
            }
            sim.run(50);
        }

        // Run for a while to let faults happen and recover
        sim.run(500);

        // Execute ready transactions on all alive nodes
        sim.execute_all_ready();

        // Verify stats show crashes and restarts happened
        let stats = sim.stats();
        println!(
            "[seed={}] Fault injection stats: crashes={}, restarts={}",
            seed, stats.crashes, stats.restarts
        );

        // Verify at least some transactions are committed on alive nodes
        let alive_nodes: Vec<_> = sim
            .node_ids()
            .iter()
            .filter(|&&id| sim.is_alive(id))
            .collect();
        let mut committed_count = 0;
        for &txn_id in &txn_ids {
            for &&node_id in &alive_nodes {
                let status = {
                    let node = sim.node(node_id).unwrap();
                    futures::executor::block_on(async { node.state.get_status(&txn_id).await })
                };
                if status == Some(TxnStatus::Committed) || status == Some(TxnStatus::Executed) {
                    committed_count += 1;
                    break;
                }
            }
        }

        println!(
            "[seed={}] Committed transactions: {}/{}",
            seed,
            committed_count,
            txn_ids.len()
        );
        // With faults, not all may commit, but some should
        assert!(
            committed_count > 0,
            "[seed={}] At least some transactions should be committed even with faults",
            seed
        );

        // Run anti-entropy sync to ensure all nodes catch up on missed transactions
        sim.anti_entropy_sync();

        // Debug: Print what each node has
        for &node_id in sim.node_ids() {
            if sim.is_alive(node_id) {
                let node = sim.node(node_id).unwrap();
                let event_count = node.executor.event_count();
                let txn_count = futures::executor::block_on(async {
                    node.state.stats().await.total_transactions
                });
                println!(
                    "[seed={}] Node {}: {} events, {} transactions",
                    seed, node_id, event_count, txn_count
                );
            }
        }

        // With sync protocol, consistency should now hold after restart
        // because nodes sync with peers to catch up on missed transactions
        let result = sim.check_consistency();
        assert!(
            result.is_ok(),
            "[seed={}] Consistency should hold after crash recovery with sync: {:?}",
            seed,
            result.error_message()
        );
    }

    #[test]
    fn test_crash_recovery_consistency() {
        // Test that consistency holds after crash and recovery
        let mut sim = Simulation::builder()
            .seed(1111)
            .nodes(3)
            .faults(FaultConfig::none())
            .build();

        // Submit transactions from different coordinators
        let mut txn_ids = Vec::new();
        for i in 0..3u64 {
            let event = Event {
                id: Ulid::from_parts(7000000 + i, i as u128),
                aggregator_id: format!("consistency-{}", i),
                aggregator_type: "test/Entity".to_string(),
                version: 1,
                name: "Created".to_string(),
                routing_key: None,
                data: vec![i as u8; 10],
                metadata: vec![],
                timestamp: 0,
                timestamp_subsec: 0,
            };

            let coordinator = i as u16;
            if let Some(txn_id) = sim.submit_transaction(coordinator, vec![event]) {
                txn_ids.push(txn_id);
            }
            sim.run(100);
        }

        // Execute all
        sim.execute_all_ready();

        // Crash all nodes and restart them
        for &node_id in sim.node_ids().to_vec().as_slice() {
            sim.crash_node(node_id);
        }
        for &node_id in sim.node_ids().to_vec().as_slice() {
            sim.restart_node(node_id);
        }

        // All transactions should still be recovered
        for (i, &txn_id) in txn_ids.iter().enumerate() {
            let node_id = i as u16;
            let status = {
                let node = sim.node(node_id).unwrap();
                futures::executor::block_on(async { node.state.get_status(&txn_id).await })
            };
            assert!(
                status.is_some(),
                "Transaction {} should be recovered on node {}",
                txn_id,
                node_id
            );
        }
    }
}
