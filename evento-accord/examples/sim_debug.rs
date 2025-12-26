//! Debug a specific simulation seed.
//!
//! Run with: cargo run --example sim_debug -p evento-accord -- <SEED>
//!
//! Example: cargo run --example sim_debug -p evento-accord -- 10852234385963990305

#![allow(dead_code)]

use evento_accord::sim::{FaultConfig, SimRng, Simulation};
use evento_core::{Event, Executor};
use std::collections::HashMap;
use std::env;
use ulid::Ulid;

/// Bank account state for simulation.
#[derive(Clone, Debug)]
struct BankAccount {
    balance: i64,
    is_frozen: bool,
}

impl BankAccount {
    fn new(initial_balance: i64) -> Self {
        Self {
            balance: initial_balance,
            is_frozen: false,
        }
    }

    fn can_withdraw(&self, amount: i64) -> bool {
        !self.is_frozen && self.balance >= amount
    }

    fn can_deposit(&self) -> bool {
        !self.is_frozen
    }
}

/// Bank operation types.
#[derive(Clone, Debug)]
enum BankOperation {
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

impl std::fmt::Display for BankOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BankOperation::OpenAccount {
                account_id,
                initial_balance,
            } => {
                write!(
                    f,
                    "OpenAccount({}, balance={})",
                    account_id, initial_balance
                )
            }
            BankOperation::Deposit { account_id, amount } => {
                write!(f, "Deposit({}, +{})", account_id, amount)
            }
            BankOperation::Withdraw { account_id, amount } => {
                write!(f, "Withdraw({}, -{})", account_id, amount)
            }
            BankOperation::Transfer {
                from_account_id,
                to_account_id,
                amount,
            } => {
                write!(
                    f,
                    "Transfer({} -> {}, {})",
                    from_account_id, to_account_id, amount
                )
            }
        }
    }
}

/// Bank simulator.
struct BankSimulator {
    accounts: HashMap<String, BankAccount>,
    rng: SimRng,
    next_account_id: u64,
}

impl BankSimulator {
    fn new(seed: u64) -> Self {
        Self {
            accounts: HashMap::new(),
            rng: SimRng::new(seed),
            next_account_id: 1,
        }
    }

    fn generate_operation(&mut self) -> BankOperation {
        if self.accounts.is_empty() {
            return self.generate_open_account();
        }

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

    fn apply_operation(&mut self, op: &BankOperation) -> bool {
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
                let can_transfer = self
                    .accounts
                    .get(from_account_id)
                    .map(|a| a.can_withdraw(*amount))
                    .unwrap_or(false);

                let to_exists = self.accounts.contains_key(to_account_id);

                if can_transfer && to_exists {
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

    fn total_balance(&self) -> i64 {
        self.accounts.values().map(|a| a.balance).sum()
    }

    fn print_state(&self) {
        println!("  Accounts:");
        let mut accounts: Vec<_> = self.accounts.iter().collect();
        accounts.sort_by_key(|(k, _)| k.as_str());
        for (id, account) in accounts {
            println!(
                "    {}: balance={}, frozen={}",
                id, account.balance, account.is_frozen
            );
        }
        println!("  Total balance: {}", self.total_balance());
    }
}

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

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let seed = if args.len() > 1 {
        args[1]
            .parse::<u64>()
            .expect("Invalid seed - must be a u64")
    } else {
        println!("Usage: cargo run --example sim_debug -p evento-accord -- <SEED>");
        println!("Example: cargo run --example sim_debug -p evento-accord -- 10852234385963990305");
        return;
    };

    println!("===========================================");
    println!("  Debugging Seed: {}", seed);
    println!("===========================================\n");

    // Use same parameters as the runner
    let param_rng = SimRng::new(seed);
    let node_count = 3 + param_rng.next_usize(5); // 3-7 nodes
    let operation_count = 50 + param_rng.next_usize(451); // 50-500 ops

    // Get fault config (random preset)
    let fault_choice = param_rng.next_usize(5);
    let (fault_name, fault_config) = match fault_choice {
        0 => ("none", FaultConfig::none()),
        1 => ("light", FaultConfig::light()),
        2 => ("medium", FaultConfig::medium()),
        3 => ("heavy", FaultConfig::heavy()),
        _ => ("chaos", FaultConfig::chaos()),
    };

    println!("Configuration:");
    println!("  Seed: {}", seed);
    println!("  Nodes: {}", node_count);
    println!("  Operations: {}", operation_count);
    println!("  Fault preset: {}", fault_name);
    println!();

    let mut sim = Simulation::builder()
        .seed(seed)
        .nodes(node_count)
        .faults(fault_config)
        .build();

    let mut bank = BankSimulator::new(seed);
    let rng = SimRng::new(seed.wrapping_add(1));

    let mut expected_balance: i64 = 0;
    let mut event_counter: u64 = 0;
    let mut successful_ops = 0;

    println!("=== Simulation Timeline ===\n");

    for op_num in 0..operation_count {
        let op = bank.generate_operation();

        // Check node status before operation
        let alive_nodes: Vec<_> = sim
            .node_ids()
            .iter()
            .filter(|&&id| sim.is_alive(id))
            .cloned()
            .collect();

        let dead_nodes: Vec<_> = sim
            .node_ids()
            .iter()
            .filter(|&&id| !sim.is_alive(id))
            .cloned()
            .collect();

        // Print tick info with faults
        let stats = sim.stats();
        if stats.crashes > 0 || stats.partitions_created > 0 {
            println!(
                "[Tick {}] Faults: {} crashes, {} restarts, {} partitions",
                sim.stats().ticks,
                stats.crashes,
                stats.restarts,
                stats.partitions_created - stats.partitions_healed
            );
        }

        if alive_nodes.is_empty() {
            println!("[Op {}] {} -> SKIPPED (all nodes down)", op_num, op);
            sim.run(10);
            continue;
        }

        let node_id = *rng.choose(&alive_nodes).unwrap();

        // Apply operation
        let succeeded = bank.apply_operation(&op);

        let status = if succeeded { "OK" } else { "FAILED" };

        // Only print every 50th operation or failures/crashes
        let should_print = op_num % 50 == 0
            || !succeeded
            || !dead_nodes.is_empty()
            || op_num == operation_count - 1;

        if should_print {
            println!(
                "[Op {}] {} on node {} -> {} (alive: {:?}, dead: {:?})",
                op_num, op, node_id, status, alive_nodes, dead_nodes
            );
        }

        if succeeded {
            successful_ops += 1;

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
                BankOperation::Transfer { .. } => {}
            }

            let event = create_bank_event(&op, event_counter);
            event_counter += 1;

            // Replicate to all alive nodes
            let node_ids: Vec<_> = sim.node_ids().to_vec();
            for nid in node_ids {
                if sim.is_alive(nid) {
                    if let Some(node) = sim.node_mut(nid) {
                        let _ = node.executor.write(vec![event.clone()]).await;
                    }
                }
            }
        }

        sim.run(5);
    }

    sim.drain();

    println!("\n=== Final State ===\n");

    // Print bank state
    println!("Bank State:");
    bank.print_state();
    println!("  Expected balance: {}", expected_balance);
    println!();

    // Print node states
    println!("Node States:");
    for &node_id in sim.node_ids() {
        let alive = sim.is_alive(node_id);
        let event_count = sim
            .node(node_id)
            .map(|n| n.executor.event_count())
            .unwrap_or(0);
        println!(
            "  Node {}: alive={}, events={}",
            node_id, alive, event_count
        );
    }
    println!();

    // Print event counts per node
    println!("Events per Node:");
    let mut event_counts: HashMap<usize, usize> = HashMap::new();
    for &node_id in sim.node_ids() {
        if let Some(node) = sim.node(node_id) {
            let count = node.executor.event_count();
            event_counts.insert(node_id as usize, count);
        }
    }
    for (id, count) in &event_counts {
        println!("  Node {}: {} events", id, count);
    }

    // Check if event counts differ (consistency issue)
    let counts: Vec<_> = event_counts.values().collect();
    let all_same = counts.windows(2).all(|w| w[0] == w[1]);
    if !all_same {
        println!("\n  WARNING: Event counts differ between nodes!");
        println!("  This indicates a consistency issue.");
    }
    println!();

    // Run checks
    println!("=== Verification ===\n");

    let actual_balance = bank.total_balance();
    let balance_ok = actual_balance == expected_balance;
    println!(
        "Balance Conservation: {} (expected={}, actual={})",
        if balance_ok { "PASS" } else { "FAIL" },
        expected_balance,
        actual_balance
    );

    let consistency = sim.check_consistency();
    println!(
        "Consistency: {}",
        if consistency.is_ok() { "PASS" } else { "FAIL" }
    );
    if !consistency.is_ok() {
        if let Some(msg) = consistency.error_message() {
            println!("  Reason: {}", msg);
        }
    }

    let no_deadlock = sim.check_no_deadlock();
    println!(
        "No Deadlock: {}",
        if no_deadlock.is_ok() { "PASS" } else { "FAIL" }
    );

    println!();

    // Final stats
    println!("=== Statistics ===\n");
    let stats = sim.stats();
    println!("Ticks: {}", stats.ticks);
    println!(
        "Operations: {} ({} successful, {:.1}% success rate)",
        operation_count,
        successful_ops,
        100.0 * successful_ops as f64 / operation_count as f64
    );
    println!(
        "Messages: {} sent, {} delivered, {} dropped",
        stats.messages_sent, stats.messages_delivered, stats.messages_dropped
    );
    println!("Crashes: {} (restarts: {})", stats.crashes, stats.restarts);
    println!(
        "Partitions: {} created, {} healed",
        stats.partitions_created, stats.partitions_healed
    );

    println!("\n===========================================");
    println!("  Debug Complete");
    println!("===========================================");
}
