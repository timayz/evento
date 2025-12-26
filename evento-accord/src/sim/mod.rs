//! Deterministic simulation testing framework.
//!
//! This module provides tools for testing ACCORD consensus
//! under controlled, reproducible conditions.
//!
//! # Overview
//!
//! The simulation framework enables:
//! - **Deterministic execution**: Same seed = same behavior, every time
//! - **Virtual time**: Tests run instantly without real delays
//! - **Fault injection**: Network partitions, node crashes, message delays
//! - **Property verification**: Linearizability and consistency checking
//!
//! # Example
//!
//! ```ignore
//! use evento_accord::sim::{Simulation, FaultConfig};
//!
//! let mut sim = Simulation::builder()
//!     .seed(42)
//!     .nodes(5)
//!     .faults(FaultConfig::medium())
//!     .build();
//!
//! // Run the simulation
//! sim.run(10000);
//!
//! // Verify correctness
//! assert!(sim.check_linearizability().is_ok());
//! assert!(sim.check_consistency().is_ok());
//! ```
//!
//! # Components
//!
//! - [`VirtualClock`]: Controlled time progression
//! - [`SimRng`]: Seeded random number generator
//! - [`VirtualNetwork`]: Message delivery with fault injection
//! - [`SimExecutor`]: In-memory event storage
//! - [`Simulation`]: Main orchestration harness
//! - [`History`]: Operation recording
//! - [`LinearizabilityChecker`]: Correctness verification
//! - [`SimCoordinator`]: ACCORD protocol coordination for simulation

mod accord;
mod checker;
mod clock;
mod durable;
mod executor;
mod faults;
mod harness;
mod history;
mod network;
mod rng;

#[cfg(test)]
mod integration;

pub use accord::{SimCoordinator, SimProtocolResult, TxnCoordinationState};
pub use checker::{
    CheckResult, InvariantChecker, LinearizabilityChecker, Violation, ViolationType,
};
pub use clock::{TimerQueue, VirtualClock, VirtualTimer};
pub use durable::{SharedDurableStorage, SimDurableStore};
pub use executor::{ExecutorDiff, SimExecutor};
pub use faults::FaultConfig;
pub use harness::{SimNode, SimStats, Simulation, SimulationBuilder};
pub use history::{ClientOperation, History, HistoryExport, Operation, OperationType};
pub use network::{InFlightMessage, NetworkStats, VirtualNetwork};
pub use rng::SimRng;
