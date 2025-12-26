//! Linearizability and invariant checking for simulation.
//!
//! Verifies that operation histories satisfy correctness properties.

use crate::sim::history::History;
use crate::sim::SimExecutor;
use std::collections::HashMap;

/// Result of a correctness check.
#[derive(Debug)]
pub struct CheckResult {
    /// Whether the check passed.
    pub passed: bool,
    /// List of violations found.
    pub violations: Vec<Violation>,
}

impl CheckResult {
    /// Create a passing result.
    pub fn pass() -> Self {
        Self {
            passed: true,
            violations: Vec::new(),
        }
    }

    /// Create a failing result with violations.
    pub fn fail(violations: Vec<Violation>) -> Self {
        Self {
            passed: false,
            violations,
        }
    }

    /// Check if the result passed.
    pub fn is_ok(&self) -> bool {
        self.passed
    }

    /// Get error message if failed.
    pub fn error_message(&self) -> Option<String> {
        if self.passed {
            None
        } else {
            Some(
                self.violations
                    .iter()
                    .map(|v| v.description.clone())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    }
}

/// A specific violation of a correctness property.
#[derive(Debug, Clone)]
pub struct Violation {
    /// Human-readable description of the violation.
    pub description: String,
    /// IDs of operations involved.
    pub operation_ids: Vec<u64>,
    /// Type of violation.
    pub violation_type: ViolationType,
}

/// Types of violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationType {
    /// Read returned stale value.
    StaleRead,
    /// Write was lost.
    LostWrite,
    /// Operations not linearizable.
    NotLinearizable,
    /// Nodes have inconsistent state.
    Inconsistency,
    /// Invariant check failed.
    InvariantViolation,
}

/// Linearizability checker for operation histories.
///
/// Checks that all operations can be arranged in a total order
/// consistent with their real-time ordering.
pub struct LinearizabilityChecker {
    /// Expected final state for verification.
    expected_state: HashMap<String, String>,
}

impl LinearizabilityChecker {
    /// Create a checker with expected final state.
    pub fn new(expected_state: HashMap<String, String>) -> Self {
        Self { expected_state }
    }

    /// Create a checker from an executor's state.
    pub fn from_executor(executor: &SimExecutor) -> Self {
        let events = executor.all_events();
        let mut state = HashMap::new();

        // Build state from events
        for event in events {
            let key = format!("{}:{}", event.aggregator_type, event.aggregator_id);
            state.insert(key, event.id.to_string());
        }

        Self {
            expected_state: state,
        }
    }

    /// Check if the history is linearizable.
    ///
    /// This is a simplified check that verifies:
    /// 1. All writes eventually become visible
    /// 2. Reads return values consistent with some serialization
    pub fn check(&self, history: &History) -> CheckResult {
        let mut violations = Vec::new();

        // Check that successful writes are reflected in final state
        for op in history.writes() {
            if op.success == Some(true) {
                // For each key in the write, it should be in final state
                for key in &op.keys {
                    if !self.expected_state.contains_key(key) {
                        violations.push(Violation {
                            description: format!("Write to key '{}' was lost", key),
                            operation_ids: vec![op.id],
                            violation_type: ViolationType::LostWrite,
                        });
                    }
                }
            }
        }

        // Check read consistency (simplified)
        for op in history.reads() {
            if let Some(result) = &op.result {
                // Find concurrent writes
                let concurrent = history.concurrent_with(op);
                let concurrent_writes: Vec<_> = concurrent.iter().filter(|o| o.is_write).collect();

                // If there were no concurrent writes, the read should match final state
                if concurrent_writes.is_empty() {
                    for (i, key) in op.keys.iter().enumerate() {
                        if let Some(expected) = self.expected_state.get(key) {
                            if result.get(i) != Some(expected) {
                                // This might be a stale read
                                violations.push(Violation {
                                    description: format!(
                                        "Read of key '{}' returned stale value",
                                        key
                                    ),
                                    operation_ids: vec![op.id],
                                    violation_type: ViolationType::StaleRead,
                                });
                            }
                        }
                    }
                }
            }
        }

        if violations.is_empty() {
            CheckResult::pass()
        } else {
            CheckResult::fail(violations)
        }
    }
}

/// Checker for common invariants.
pub struct InvariantChecker;

impl InvariantChecker {
    /// Check that no committed writes were lost.
    ///
    /// Compares expected write count to actual stored events.
    pub fn check_no_lost_writes(expected_writes: usize, actual_events: usize) -> CheckResult {
        if actual_events < expected_writes {
            CheckResult::fail(vec![Violation {
                description: format!(
                    "Lost writes: expected at least {} events but found {}",
                    expected_writes, actual_events
                ),
                operation_ids: vec![],
                violation_type: ViolationType::LostWrite,
            }])
        } else {
            CheckResult::pass()
        }
    }

    /// Check that all nodes have consistent state.
    ///
    /// All nodes should have the same events in the same order.
    pub fn check_consistency(executors: &[SimExecutor]) -> CheckResult {
        if executors.len() < 2 {
            return CheckResult::pass();
        }

        let reference = &executors[0];
        let mut violations = Vec::new();

        for (i, executor) in executors.iter().enumerate().skip(1) {
            let diff = reference.diff(executor);
            if !diff.is_empty() {
                violations.push(Violation {
                    description: format!(
                        "Node {} inconsistent with node 0: {} unique aggregates, {} ordering differences",
                        i,
                        diff.only_in_self.len() + diff.only_in_other.len(),
                        diff.different_order.len()
                    ),
                    operation_ids: vec![],
                    violation_type: ViolationType::Inconsistency,
                });
            }
        }

        if violations.is_empty() {
            CheckResult::pass()
        } else {
            CheckResult::fail(violations)
        }
    }

    /// Check that all operations completed (no deadlock).
    pub fn check_no_deadlock(history: &History) -> CheckResult {
        if history.has_pending() {
            let pending: Vec<_> = history.pending().map(|op| op.id).collect();
            CheckResult::fail(vec![Violation {
                description: format!(
                    "{} operations never completed (possible deadlock)",
                    pending.len()
                ),
                operation_ids: pending,
                violation_type: ViolationType::InvariantViolation,
            }])
        } else {
            CheckResult::pass()
        }
    }

    /// Check that all writes succeeded.
    pub fn check_all_writes_succeeded(history: &History) -> CheckResult {
        let failed: Vec<_> = history
            .writes()
            .filter(|op| op.success == Some(false))
            .map(|op| op.id)
            .collect();

        if failed.is_empty() {
            CheckResult::pass()
        } else {
            CheckResult::fail(vec![Violation {
                description: format!("{} writes failed", failed.len()),
                operation_ids: failed,
                violation_type: ViolationType::InvariantViolation,
            }])
        }
    }

    /// Check success rate is above threshold.
    pub fn check_success_rate(history: &History, min_rate: f64) -> CheckResult {
        let total = history.client_operations().len();
        if total == 0 {
            return CheckResult::pass();
        }

        let succeeded = history
            .client_operations()
            .iter()
            .filter(|op| op.success == Some(true))
            .count();

        let rate = succeeded as f64 / total as f64;

        if rate >= min_rate {
            CheckResult::pass()
        } else {
            CheckResult::fail(vec![Violation {
                description: format!(
                    "Success rate {:.1}% below threshold {:.1}%",
                    rate * 100.0,
                    min_rate * 100.0
                ),
                operation_ids: vec![],
                violation_type: ViolationType::InvariantViolation,
            }])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evento_core::Executor;
    use std::time::Duration;

    #[test]
    fn test_check_result_pass() {
        let result = CheckResult::pass();
        assert!(result.is_ok());
        assert!(result.error_message().is_none());
    }

    #[test]
    fn test_check_result_fail() {
        let result = CheckResult::fail(vec![Violation {
            description: "test failure".to_string(),
            operation_ids: vec![1, 2],
            violation_type: ViolationType::LostWrite,
        }]);

        assert!(!result.is_ok());
        assert!(result.error_message().is_some());
    }

    #[test]
    fn test_no_lost_writes() {
        let result = InvariantChecker::check_no_lost_writes(10, 10);
        assert!(result.is_ok());

        let result = InvariantChecker::check_no_lost_writes(10, 15);
        assert!(result.is_ok());

        let result = InvariantChecker::check_no_lost_writes(10, 5);
        assert!(!result.is_ok());
    }

    #[test]
    fn test_no_deadlock() {
        let mut history = History::new();

        // Complete operation
        let id = history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        history.write_end(id, Duration::from_millis(10), true);

        let result = InvariantChecker::check_no_deadlock(&history);
        assert!(result.is_ok());
    }

    #[test]
    fn test_deadlock_detected() {
        let mut history = History::new();

        // Incomplete operation
        history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);

        let result = InvariantChecker::check_no_deadlock(&history);
        assert!(!result.is_ok());
    }

    #[tokio::test]
    async fn test_consistency_check() {
        let exec1 = SimExecutor::new();
        let exec2 = SimExecutor::new();

        // Same events - using same Ulid for both
        let event_id = ulid::Ulid::new();
        let event = evento_core::Event {
            id: event_id,
            name: "Test".to_string(),
            aggregator_type: "Order".to_string(),
            aggregator_id: "1".to_string(),
            version: 1,
            routing_key: None,
            data: vec![],
            metadata: vec![],
            timestamp: 0,
            timestamp_subsec: 0,
        };

        exec1.write(vec![event.clone()]).await.unwrap();
        exec2.write(vec![event]).await.unwrap();

        let result = InvariantChecker::check_consistency(&[exec1, exec2]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_success_rate() {
        let mut history = History::new();

        // 8 successes, 2 failures = 80% success rate
        for i in 0..10 {
            let id = history.write_start(0, Duration::from_millis(i * 10), vec!["key".to_string()]);
            history.write_end(id, Duration::from_millis(i * 10 + 5), i < 8);
        }

        // 80% threshold should pass
        let result = InvariantChecker::check_success_rate(&history, 0.8);
        assert!(result.is_ok());

        // 90% threshold should fail
        let result = InvariantChecker::check_success_rate(&history, 0.9);
        assert!(!result.is_ok());
    }
}
