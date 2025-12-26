//! Configuration types for ACCORD consensus.

use crate::error::{AccordError, Result};
use crate::transport::NodeAddr;
use std::collections::HashSet;
use std::time::Duration;

/// ACCORD cluster configuration.
#[derive(Clone, Debug)]
pub struct AccordConfig {
    /// This node's address configuration.
    pub local: NodeAddr,

    /// All nodes in the cluster (including self).
    pub nodes: Vec<NodeAddr>,

    /// Operation timeouts.
    pub timeouts: TimeoutConfig,

    /// Execution settings.
    pub execution: ExecutionConfig,

    /// Connection management settings.
    pub connection: ConnectionConfig,
}

/// Timeout configuration for protocol operations.
#[derive(Clone, Debug)]
pub struct TimeoutConfig {
    /// Timeout for PreAccept phase.
    pub preaccept: Duration,

    /// Timeout for Accept phase.
    pub accept: Duration,

    /// Timeout for waiting on dependencies.
    pub dependency_wait: Duration,

    /// Connection timeout.
    pub connect: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            preaccept: Duration::from_secs(5),
            accept: Duration::from_secs(5),
            dependency_wait: Duration::from_secs(30),
            connect: Duration::from_secs(2),
        }
    }
}

/// Execution configuration for background processing.
#[derive(Clone, Debug)]
pub struct ExecutionConfig {
    /// Number of concurrent execution workers.
    pub workers: usize,

    /// How long to keep executed transactions before cleanup.
    pub retention: Duration,

    /// Polling interval for checking ready transactions.
    pub poll_interval: Duration,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            retention: Duration::from_secs(3600), // 1 hour
            poll_interval: Duration::from_millis(10),
        }
    }
}

/// Connection management configuration.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Maximum retry attempts for failed connections.
    pub max_retries: u32,

    /// Initial backoff delay for retries.
    pub initial_backoff: Duration,

    /// Maximum backoff delay.
    pub max_backoff: Duration,

    /// Backoff multiplier (exponential growth factor).
    pub backoff_multiplier: f64,

    /// Health check interval for idle connections.
    pub health_check_interval: Duration,

    /// Connection idle timeout before closing.
    pub idle_timeout: Duration,

    /// Maximum concurrent connections per peer.
    pub max_connections_per_peer: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            health_check_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300), // 5 minutes
            max_connections_per_peer: 1,
        }
    }
}

impl AccordConfig {
    /// Create single-node configuration (no consensus, for dev/test).
    ///
    /// In single-node mode, writes bypass the ACCORD protocol and go
    /// directly to the underlying executor.
    pub fn single_node() -> Self {
        Self {
            local: NodeAddr::new(0, "127.0.0.1", 0),
            nodes: vec![],
            timeouts: TimeoutConfig::default(),
            execution: ExecutionConfig::default(),
            connection: ConnectionConfig::default(),
        }
    }

    /// Create cluster configuration.
    ///
    /// # Arguments
    ///
    /// * `local` - This node's address
    /// * `nodes` - All nodes in the cluster (must include `local`)
    pub fn cluster(local: NodeAddr, nodes: Vec<NodeAddr>) -> Self {
        Self {
            local,
            nodes,
            timeouts: TimeoutConfig::default(),
            execution: ExecutionConfig::default(),
            connection: ConnectionConfig::default(),
        }
    }

    /// Check if running in single-node mode.
    pub fn is_single_node(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// Calculate quorum size.
    ///
    /// For a cluster of size N, quorum is floor(N/2) + 1.
    pub fn quorum_size(&self) -> usize {
        if self.nodes.is_empty() {
            1
        } else {
            self.nodes.len() / 2 + 1
        }
    }

    /// Get peer addresses (all nodes except self).
    pub fn peers(&self) -> Vec<NodeAddr> {
        self.nodes
            .iter()
            .filter(|n| n.id != self.local.id)
            .cloned()
            .collect()
    }

    /// Set custom timeouts.
    pub fn with_timeouts(mut self, timeouts: TimeoutConfig) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Set custom execution config.
    pub fn with_execution(mut self, execution: ExecutionConfig) -> Self {
        self.execution = execution;
        self
    }

    /// Set custom connection config.
    pub fn with_connection(mut self, connection: ConnectionConfig) -> Self {
        self.connection = connection;
        self
    }

    /// Validate the configuration.
    ///
    /// Returns an error if the configuration is invalid.
    ///
    /// # Checks
    ///
    /// - Cluster must have at least one node (unless single-node mode)
    /// - Local node must be present in the cluster
    /// - No duplicate node IDs
    /// - Valid timeout values (non-zero)
    /// - At least one execution worker
    /// - Valid connection config
    pub fn validate(&self) -> Result<()> {
        // Single-node mode has minimal requirements
        if self.nodes.is_empty() {
            return Ok(());
        }

        // Check for duplicate node IDs
        let mut seen_ids = HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(node.id) {
                return Err(AccordError::Config(format!(
                    "duplicate node ID: {}",
                    node.id
                )));
            }
        }

        // Local node must be in the cluster
        if !self.nodes.iter().any(|n| n.id == self.local.id) {
            return Err(AccordError::Config(format!(
                "local node {} not found in cluster",
                self.local.id
            )));
        }

        // Validate timeouts
        self.timeouts.validate()?;

        // Validate execution config
        self.execution.validate()?;

        // Validate connection config
        self.connection.validate()?;

        Ok(())
    }
}

impl TimeoutConfig {
    /// Validate timeout configuration.
    pub fn validate(&self) -> Result<()> {
        if self.preaccept.is_zero() {
            return Err(AccordError::Config(
                "preaccept timeout must be positive".into(),
            ));
        }
        if self.accept.is_zero() {
            return Err(AccordError::Config(
                "accept timeout must be positive".into(),
            ));
        }
        if self.dependency_wait.is_zero() {
            return Err(AccordError::Config(
                "dependency_wait timeout must be positive".into(),
            ));
        }
        if self.connect.is_zero() {
            return Err(AccordError::Config(
                "connect timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

impl ExecutionConfig {
    /// Validate execution configuration.
    pub fn validate(&self) -> Result<()> {
        if self.workers == 0 {
            return Err(AccordError::Config(
                "execution workers must be at least 1".into(),
            ));
        }
        if self.poll_interval.is_zero() {
            return Err(AccordError::Config("poll_interval must be positive".into()));
        }
        Ok(())
    }
}

impl ConnectionConfig {
    /// Validate connection configuration.
    pub fn validate(&self) -> Result<()> {
        if self.initial_backoff.is_zero() {
            return Err(AccordError::Config(
                "initial_backoff must be positive".into(),
            ));
        }
        if self.max_backoff.is_zero() {
            return Err(AccordError::Config("max_backoff must be positive".into()));
        }
        if self.max_backoff < self.initial_backoff {
            return Err(AccordError::Config(
                "max_backoff must be >= initial_backoff".into(),
            ));
        }
        if self.backoff_multiplier < 1.0 {
            return Err(AccordError::Config(
                "backoff_multiplier must be >= 1.0".into(),
            ));
        }
        if self.health_check_interval.is_zero() {
            return Err(AccordError::Config(
                "health_check_interval must be positive".into(),
            ));
        }
        if self.idle_timeout.is_zero() {
            return Err(AccordError::Config("idle_timeout must be positive".into()));
        }
        if self.max_connections_per_peer == 0 {
            return Err(AccordError::Config(
                "max_connections_per_peer must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_config() {
        let config = AccordConfig::single_node();
        assert!(config.is_single_node());
        assert_eq!(config.quorum_size(), 1);
        assert!(config.peers().is_empty());
    }

    #[test]
    fn test_cluster_config() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![
            NodeAddr::new(0, "node0", 5000),
            NodeAddr::new(1, "node1", 5000),
            NodeAddr::new(2, "node2", 5000),
        ];

        let config = AccordConfig::cluster(local, nodes);

        assert!(!config.is_single_node());
        assert_eq!(config.quorum_size(), 2);
        assert_eq!(config.peers().len(), 2);
    }

    #[test]
    fn test_quorum_sizes() {
        // 1 node -> quorum 1
        let c1 = AccordConfig::cluster(NodeAddr::new(0, "n", 0), vec![NodeAddr::new(0, "n", 0)]);
        assert_eq!(c1.quorum_size(), 1);

        // 3 nodes -> quorum 2
        let c3 = AccordConfig::cluster(
            NodeAddr::new(0, "n", 0),
            vec![
                NodeAddr::new(0, "n", 0),
                NodeAddr::new(1, "n", 0),
                NodeAddr::new(2, "n", 0),
            ],
        );
        assert_eq!(c3.quorum_size(), 2);

        // 5 nodes -> quorum 3
        let c5 = AccordConfig::cluster(
            NodeAddr::new(0, "n", 0),
            (0..5).map(|i| NodeAddr::new(i, "n", 0)).collect(),
        );
        assert_eq!(c5.quorum_size(), 3);
    }

    #[test]
    fn test_with_timeouts() {
        let custom = TimeoutConfig {
            preaccept: Duration::from_secs(10),
            accept: Duration::from_secs(10),
            dependency_wait: Duration::from_secs(60),
            connect: Duration::from_secs(5),
        };

        let config = AccordConfig::single_node().with_timeouts(custom.clone());
        assert_eq!(config.timeouts.preaccept, Duration::from_secs(10));
    }

    #[test]
    fn test_validate_single_node() {
        let config = AccordConfig::single_node();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_cluster_ok() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![
            NodeAddr::new(0, "node0", 5000),
            NodeAddr::new(1, "node1", 5000),
            NodeAddr::new(2, "node2", 5000),
        ];
        let config = AccordConfig::cluster(local, nodes);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_duplicate_node_id() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![
            NodeAddr::new(0, "node0", 5000),
            NodeAddr::new(0, "node0-dup", 5001), // Duplicate ID
            NodeAddr::new(2, "node2", 5000),
        ];
        let config = AccordConfig::cluster(local, nodes);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate"));
    }

    #[test]
    fn test_validate_local_not_in_cluster() {
        let local = NodeAddr::new(99, "node99", 5000);
        let nodes = vec![
            NodeAddr::new(0, "node0", 5000),
            NodeAddr::new(1, "node1", 5000),
        ];
        let config = AccordConfig::cluster(local, nodes);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_validate_zero_timeout() {
        // Single-node skips validation, so we need a cluster
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![NodeAddr::new(0, "node0", 5000)];
        let config = AccordConfig::cluster(local, nodes).with_timeouts(TimeoutConfig {
            preaccept: Duration::ZERO,
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("preaccept"));
    }

    #[test]
    fn test_validate_zero_workers() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![NodeAddr::new(0, "node0", 5000)];
        let config = AccordConfig::cluster(local, nodes).with_execution(ExecutionConfig {
            workers: 0,
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("workers"));
    }

    #[test]
    fn test_validate_invalid_backoff() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![NodeAddr::new(0, "node0", 5000)];
        let config = AccordConfig::cluster(local, nodes).with_connection(ConnectionConfig {
            max_backoff: Duration::from_millis(50),
            initial_backoff: Duration::from_millis(100), // max < initial
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_backoff"));
    }

    #[test]
    fn test_validate_invalid_multiplier() {
        let local = NodeAddr::new(0, "node0", 5000);
        let nodes = vec![NodeAddr::new(0, "node0", 5000)];
        let config = AccordConfig::cluster(local, nodes).with_connection(ConnectionConfig {
            backoff_multiplier: 0.5, // < 1.0
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("multiplier"));
    }
}
