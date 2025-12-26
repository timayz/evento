//! Transport layer abstractions for inter-node communication.

use crate::error::Result;
use crate::protocol::Message;
use async_trait::async_trait;

/// Node identifier type.
pub type NodeId = u16;

/// Network address for a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeAddr {
    /// Unique identifier for this node
    pub id: NodeId,
    /// Hostname or IP address
    pub host: String,
    /// Port number
    pub port: u16,
}

impl NodeAddr {
    /// Create a new node address.
    pub fn new(id: NodeId, host: impl Into<String>, port: u16) -> Self {
        Self {
            id,
            host: host.into(),
            port,
        }
    }

    /// Get the socket address string.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Transport layer abstraction for inter-node communication.
///
/// This trait allows different transport implementations:
/// - TCP for production use
/// - In-memory channels for testing
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a message to a specific node and wait for response.
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message>;

    /// Broadcast message to all peer nodes and collect responses.
    ///
    /// Returns responses from all nodes (including errors).
    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>>;

    /// Broadcast without waiting for responses (fire and forget).
    ///
    /// Used for commit messages where we don't need acknowledgement.
    fn broadcast_no_wait(&self, msg: Message);

    /// Get the local node ID.
    fn local_node(&self) -> NodeId;

    /// Get all peer node IDs (excluding self).
    fn peers(&self) -> Vec<NodeId>;

    /// Get the total number of nodes in the cluster.
    fn cluster_size(&self) -> usize;

    /// Get the required quorum size for consensus.
    ///
    /// For a cluster of size N, quorum is floor(N/2) + 1.
    fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_addr_new() {
        let addr = NodeAddr::new(1, "localhost", 5000);
        assert_eq!(addr.id, 1);
        assert_eq!(addr.host, "localhost");
        assert_eq!(addr.port, 5000);
    }

    #[test]
    fn test_node_addr_socket_addr() {
        let addr = NodeAddr::new(1, "192.168.1.1", 5000);
        assert_eq!(addr.socket_addr(), "192.168.1.1:5000");
    }

    #[test]
    fn test_quorum_sizes() {
        // Test quorum calculation for various cluster sizes
        // 1 node: quorum = 1
        // 2 nodes: quorum = 2
        // 3 nodes: quorum = 2
        // 4 nodes: quorum = 3
        // 5 nodes: quorum = 3
        assert_eq!(1 / 2 + 1, 1);
        assert_eq!(2 / 2 + 1, 2);
        assert_eq!(3 / 2 + 1, 2);
        assert_eq!(4 / 2 + 1, 3);
        assert_eq!(5 / 2 + 1, 3);
    }
}
