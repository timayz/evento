//! Continuous simulation runner for fuzzing ACCORD consensus.
//!
//! Run with: cargo run --example sim_runner -p evento-accord
//!
//! This runs simulations with random seeds until stopped (Ctrl+C),
//! reporting any failures found. Useful for finding edge cases.

#![allow(dead_code)] // Allow unused variants for configuration options

use evento_accord::sim::{FaultConfig, SimRng, SimStats, Simulation};
use evento_core::{Event, Executor};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

/// Result of a single simulation run.
#[derive(Debug)]
struct SimulationResult {
    seed: u64,
    node_count: usize,
    operation_count: usize,
    successful_operations: usize,
    balance_conserved: bool,
    consistency_passed: bool,
    no_deadlock_passed: bool,
    duration: Duration,
    stats: SimStats,
}

impl SimulationResult {
    fn is_success(&self) -> bool {
        self.balance_conserved && self.consistency_passed && self.no_deadlock_passed
    }
}

/// Run a single simulation.
async fn run_simulation(
    seed: u64,
    node_count: usize,
    operation_count: usize,
    fault_config: FaultConfig,
) -> SimulationResult {
    let start = Instant::now();

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

    for _ in 0..operation_count {
        let op = bank.generate_operation();

        let alive_nodes: Vec<_> = sim
            .node_ids()
            .iter()
            .filter(|&&id| sim.is_alive(id))
            .collect();

        if alive_nodes.is_empty() {
            sim.run(10);
            continue;
        }

        let node_id = **rng.choose(&alive_nodes).unwrap();

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

        let now = sim.now();
        let op_id = sim.history_mut().write_start(node_id, now, keys);

        let succeeded = bank.apply_operation(&op);

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
            let node_ids: Vec<_> = sim.node_ids().to_vec();
            for nid in node_ids {
                if sim.is_alive(nid) {
                    if let Some(node) = sim.node_mut(nid) {
                        let _ = node.executor.write(vec![event.clone()]).await;
                    }
                }
            }
        }

        let end_time = sim.now();
        sim.history_mut().write_end(op_id, end_time, succeeded);
        sim.run(5);
    }

    sim.drain();

    let actual_balance = bank.total_balance();

    SimulationResult {
        seed,
        node_count,
        operation_count,
        successful_operations: successful_ops,
        balance_conserved: actual_balance == expected_balance,
        consistency_passed: sim.check_consistency().is_ok(),
        no_deadlock_passed: sim.check_no_deadlock().is_ok(),
        duration: start.elapsed(),
        stats: sim.stats().clone(),
    }
}

/// Configuration for the continuous runner.
struct RunnerConfig {
    /// Minimum nodes per simulation
    min_nodes: usize,
    /// Maximum nodes per simulation
    max_nodes: usize,
    /// Minimum operations per simulation
    min_ops: usize,
    /// Maximum operations per simulation
    max_ops: usize,
    /// Fault configuration preset
    fault_preset: FaultPreset,
}

#[derive(Clone, Copy)]
enum FaultPreset {
    None,
    Light,
    Medium,
    Heavy,
    Chaos,
    Random,
}

impl FaultPreset {
    fn to_config(&self, rng: &SimRng) -> FaultConfig {
        match self {
            FaultPreset::None => FaultConfig::none(),
            FaultPreset::Light => FaultConfig::light(),
            FaultPreset::Medium => FaultConfig::medium(),
            FaultPreset::Heavy => FaultConfig::heavy(),
            FaultPreset::Chaos => FaultConfig::chaos(),
            FaultPreset::Random => {
                let choice = rng.next_usize(5);
                match choice {
                    0 => FaultConfig::none(),
                    1 => FaultConfig::light(),
                    2 => FaultConfig::medium(),
                    3 => FaultConfig::heavy(),
                    _ => FaultConfig::chaos(),
                }
            }
        }
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            min_nodes: 3,
            max_nodes: 7,
            min_ops: 50,
            max_ops: 500,
            fault_preset: FaultPreset::Random,
        }
    }
}

/// Statistics for the continuous runner.
struct RunnerStats {
    total_runs: AtomicU64,
    successful_runs: AtomicU64,
    failed_runs: AtomicU64,
    total_operations: AtomicU64,
}

impl RunnerStats {
    fn new() -> Self {
        Self {
            total_runs: AtomicU64::new(0),
            successful_runs: AtomicU64::new(0),
            failed_runs: AtomicU64::new(0),
            total_operations: AtomicU64::new(0),
        }
    }

    fn record(&self, result: &SimulationResult) {
        self.total_runs.fetch_add(1, Ordering::Relaxed);
        self.total_operations
            .fetch_add(result.operation_count as u64, Ordering::Relaxed);

        if result.is_success() {
            self.successful_runs.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_runs.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn print_summary(&self, elapsed: Duration) {
        let total = self.total_runs.load(Ordering::Relaxed);
        let success = self.successful_runs.load(Ordering::Relaxed);
        let failed = self.failed_runs.load(Ordering::Relaxed);
        let ops = self.total_operations.load(Ordering::Relaxed);

        let rate = if elapsed.as_secs() > 0 {
            total as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        println!("\n========== SUMMARY ==========");
        println!("Total runs:      {}", total);
        println!(
            "Successful:      {} ({:.1}%)",
            success,
            100.0 * success as f64 / total.max(1) as f64
        );
        println!("Failed:          {}", failed);
        println!("Total ops:       {}", ops);
        println!("Duration:        {:.1}s", elapsed.as_secs_f64());
        println!("Rate:            {:.1} runs/sec", rate);
        println!("==============================");
    }
}

#[tokio::main]
async fn main() {
    println!("===========================================");
    println!("  ACCORD Simulation Runner");
    println!("  Press Ctrl+C to stop");
    println!("===========================================\n");

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        println!("\nStopping...");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let config = RunnerConfig::default();
    let stats = Arc::new(RunnerStats::new());
    let start_time = Instant::now();

    // Use current time as base seed for variety
    let base_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let seed_rng = SimRng::new(base_seed);
    let mut run_number = 0u64;

    while running.load(Ordering::SeqCst) {
        run_number += 1;
        let seed = seed_rng.next_u64();
        let param_rng = SimRng::new(seed);

        // Random parameters within bounds
        let node_count =
            config.min_nodes + param_rng.next_usize(config.max_nodes - config.min_nodes + 1);
        let op_count = config.min_ops + param_rng.next_usize(config.max_ops - config.min_ops + 1);
        let fault_config = config.fault_preset.to_config(&param_rng);

        // Run simulation
        let result = run_simulation(seed, node_count, op_count, fault_config).await;

        // Record stats
        stats.record(&result);

        // Print progress
        let status = if result.is_success() { "OK" } else { "FAIL" };
        print!(
            "\r[{}] seed={} nodes={} ops={} success_rate={:.0}% -> {}    ",
            run_number,
            seed,
            result.node_count,
            result.operation_count,
            100.0 * result.successful_operations as f64 / result.operation_count as f64,
            status
        );

        // Print failure details
        if !result.is_success() {
            println!();
            println!("  FAILURE DETECTED!");
            println!("  Seed: {} (use this to reproduce)", seed);
            println!("  Balance conserved: {}", result.balance_conserved);
            println!("  Consistency: {}", result.consistency_passed);
            println!("  No deadlock: {}", result.no_deadlock_passed);
            println!("  Stats: {:?}", result.stats);
            println!();
        }

        // Flush output
        use std::io::Write;
        std::io::stdout().flush().ok();
    }

    stats.print_summary(start_time.elapsed());
}
