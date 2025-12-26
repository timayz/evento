//! Fault injection configuration for simulation.
//!
//! Defines probabilities and parameters for various failure modes.

use std::time::Duration;

/// Configuration for fault injection during simulation.
///
/// All probabilities are in the range [0.0, 1.0] where:
/// - 0.0 = never happens
/// - 1.0 = always happens
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Probability of dropping a message (0.0 - 1.0).
    pub message_drop_probability: f64,

    /// Probability of duplicating a message (0.0 - 1.0).
    pub message_duplicate_probability: f64,

    /// Minimum message delivery delay.
    pub min_message_delay: Duration,

    /// Maximum message delivery delay.
    pub max_message_delay: Duration,

    /// Probability of a node crashing per simulation tick (0.0 - 1.0).
    pub crash_probability: f64,

    /// Probability of a crashed node restarting per tick (0.0 - 1.0).
    pub restart_probability: f64,

    /// Probability of creating a network partition per tick.
    pub partition_probability: f64,

    /// Probability of healing a partition per tick.
    pub heal_probability: f64,

    /// Maximum clock skew between nodes.
    pub max_clock_skew: Duration,

    /// Probability of reordering messages (within delay bounds).
    pub reorder_probability: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self::none()
    }
}

impl FaultConfig {
    /// No faults - perfectly reliable network.
    ///
    /// Use this to verify basic correctness before adding faults.
    pub fn none() -> Self {
        Self {
            message_drop_probability: 0.0,
            message_duplicate_probability: 0.0,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(5),
            crash_probability: 0.0,
            restart_probability: 0.0,
            partition_probability: 0.0,
            heal_probability: 0.0,
            max_clock_skew: Duration::ZERO,
            reorder_probability: 0.0,
        }
    }

    /// Light faults - occasional delays and rare drops.
    ///
    /// Good for catching basic timing issues.
    pub fn light() -> Self {
        Self {
            message_drop_probability: 0.01,
            message_duplicate_probability: 0.001,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(50),
            crash_probability: 0.0,
            restart_probability: 0.0,
            partition_probability: 0.0,
            heal_probability: 0.0,
            max_clock_skew: Duration::from_millis(10),
            reorder_probability: 0.05,
        }
    }

    /// Medium faults - noticeable failures.
    ///
    /// Tests recovery from common failure scenarios.
    pub fn medium() -> Self {
        Self {
            message_drop_probability: 0.05,
            message_duplicate_probability: 0.01,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(100),
            crash_probability: 0.001,
            restart_probability: 0.1,
            partition_probability: 0.0005,
            heal_probability: 0.05,
            max_clock_skew: Duration::from_millis(50),
            reorder_probability: 0.1,
        }
    }

    /// Heavy faults - aggressive failure injection.
    ///
    /// Stress tests the protocol's fault tolerance.
    pub fn heavy() -> Self {
        Self {
            message_drop_probability: 0.1,
            message_duplicate_probability: 0.05,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(200),
            crash_probability: 0.01,
            restart_probability: 0.1,
            partition_probability: 0.005,
            heal_probability: 0.1,
            max_clock_skew: Duration::from_millis(100),
            reorder_probability: 0.2,
        }
    }

    /// Chaos mode - extreme failure injection.
    ///
    /// Maximum stress testing. If it works under chaos, it's robust.
    pub fn chaos() -> Self {
        Self {
            message_drop_probability: 0.2,
            message_duplicate_probability: 0.1,
            min_message_delay: Duration::from_millis(0),
            max_message_delay: Duration::from_millis(500),
            crash_probability: 0.05,
            restart_probability: 0.2,
            partition_probability: 0.02,
            heal_probability: 0.1,
            max_clock_skew: Duration::from_millis(500),
            reorder_probability: 0.3,
        }
    }

    /// Network-only faults (no crashes).
    ///
    /// Tests protocol correctness under network issues only.
    pub fn network_only() -> Self {
        Self {
            message_drop_probability: 0.1,
            message_duplicate_probability: 0.05,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(100),
            crash_probability: 0.0,
            restart_probability: 0.0,
            partition_probability: 0.01,
            heal_probability: 0.1,
            max_clock_skew: Duration::ZERO,
            reorder_probability: 0.2,
        }
    }

    /// Crash-only faults (reliable network).
    ///
    /// Tests crash recovery with perfect network.
    pub fn crash_only() -> Self {
        Self {
            message_drop_probability: 0.0,
            message_duplicate_probability: 0.0,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(10),
            crash_probability: 0.02,
            restart_probability: 0.1,
            partition_probability: 0.0,
            heal_probability: 0.0,
            max_clock_skew: Duration::ZERO,
            reorder_probability: 0.0,
        }
    }

    /// Partition-focused faults.
    ///
    /// Tests behavior during network partitions.
    pub fn partitions() -> Self {
        Self {
            message_drop_probability: 0.01,
            message_duplicate_probability: 0.0,
            min_message_delay: Duration::from_millis(1),
            max_message_delay: Duration::from_millis(50),
            crash_probability: 0.0,
            restart_probability: 0.0,
            partition_probability: 0.05,
            heal_probability: 0.02,
            max_clock_skew: Duration::ZERO,
            reorder_probability: 0.0,
        }
    }

    // Builder methods

    /// Set message drop probability.
    pub fn with_message_drop(mut self, probability: f64) -> Self {
        self.message_drop_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set message duplicate probability.
    pub fn with_message_duplicate(mut self, probability: f64) -> Self {
        self.message_duplicate_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set message delay range.
    pub fn with_message_delay(mut self, min: Duration, max: Duration) -> Self {
        self.min_message_delay = min;
        self.max_message_delay = max;
        self
    }

    /// Set crash probability.
    pub fn with_crash(mut self, probability: f64) -> Self {
        self.crash_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set restart probability.
    pub fn with_restart(mut self, probability: f64) -> Self {
        self.restart_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set partition probability.
    pub fn with_partition(mut self, probability: f64) -> Self {
        self.partition_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set heal probability.
    pub fn with_heal(mut self, probability: f64) -> Self {
        self.heal_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Set maximum clock skew.
    pub fn with_clock_skew(mut self, max_skew: Duration) -> Self {
        self.max_clock_skew = max_skew;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_has_no_faults() {
        let config = FaultConfig::none();
        assert_eq!(config.message_drop_probability, 0.0);
        assert_eq!(config.crash_probability, 0.0);
        assert_eq!(config.partition_probability, 0.0);
    }

    #[test]
    fn test_presets_have_increasing_severity() {
        let none = FaultConfig::none();
        let light = FaultConfig::light();
        let medium = FaultConfig::medium();
        let heavy = FaultConfig::heavy();
        let chaos = FaultConfig::chaos();

        assert!(light.message_drop_probability > none.message_drop_probability);
        assert!(medium.message_drop_probability > light.message_drop_probability);
        assert!(heavy.message_drop_probability > medium.message_drop_probability);
        assert!(chaos.message_drop_probability > heavy.message_drop_probability);
    }

    #[test]
    fn test_builder_methods() {
        let config = FaultConfig::none()
            .with_message_drop(0.5)
            .with_crash(0.1)
            .with_message_delay(Duration::from_millis(10), Duration::from_millis(100));

        assert_eq!(config.message_drop_probability, 0.5);
        assert_eq!(config.crash_probability, 0.1);
        assert_eq!(config.min_message_delay, Duration::from_millis(10));
        assert_eq!(config.max_message_delay, Duration::from_millis(100));
    }

    #[test]
    fn test_probability_clamping() {
        let config = FaultConfig::none()
            .with_message_drop(1.5) // > 1.0
            .with_crash(-0.5); // < 0.0

        assert_eq!(config.message_drop_probability, 1.0);
        assert_eq!(config.crash_probability, 0.0);
    }
}
