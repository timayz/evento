//! Virtual network for simulation.
//!
//! Simulates message passing between nodes with configurable
//! delays, drops, duplicates, and partitions.

use crate::protocol::Message;
use crate::sim::{FaultConfig, SimRng, VirtualClock};
use crate::transport::NodeId;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::time::Duration;

/// A message in flight through the virtual network.
#[derive(Clone, Debug)]
pub struct InFlightMessage {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// The message payload.
    pub message: Message,
    /// Virtual time when this message should be delivered.
    pub deliver_at: Duration,
    /// Unique message ID.
    pub id: u64,
}

impl Ord for InFlightMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by delivery time, then by ID for stability
        Reverse((self.deliver_at, self.id)).cmp(&Reverse((other.deliver_at, other.id)))
    }
}

impl PartialOrd for InFlightMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for InFlightMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for InFlightMessage {}

/// Virtual network that simulates message passing with faults.
pub struct VirtualNetwork {
    clock: VirtualClock,
    rng: SimRng,
    config: FaultConfig,

    /// Messages waiting to be delivered.
    in_flight: BinaryHeap<InFlightMessage>,

    /// Network partitions: pairs of nodes that can't communicate.
    /// Stored as (min_id, max_id) for normalization.
    partitions: HashSet<(NodeId, NodeId)>,

    /// Nodes that are crashed.
    crashed_nodes: HashSet<NodeId>,

    /// Message counter for unique IDs.
    message_counter: u64,

    /// All messages ever sent (for debugging/analysis).
    history: Vec<InFlightMessage>,

    /// Statistics.
    stats: NetworkStats,
}

/// Network statistics.
#[derive(Clone, Debug, Default)]
pub struct NetworkStats {
    pub messages_sent: u64,
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub messages_duplicated: u64,
    pub partitions_created: u64,
    pub partitions_healed: u64,
}

impl VirtualNetwork {
    /// Create a new virtual network.
    pub fn new(clock: VirtualClock, rng: SimRng, config: FaultConfig) -> Self {
        Self {
            clock,
            rng,
            config,
            in_flight: BinaryHeap::new(),
            partitions: HashSet::new(),
            crashed_nodes: HashSet::new(),
            message_counter: 0,
            history: Vec::new(),
            stats: NetworkStats::default(),
        }
    }

    /// Send a message from one node to another.
    ///
    /// The message will be queued for delivery after a random delay,
    /// subject to fault injection.
    pub fn send(&mut self, from: NodeId, to: NodeId, message: Message) {
        self.stats.messages_sent += 1;

        // Check if sender or receiver is crashed
        if self.crashed_nodes.contains(&from) || self.crashed_nodes.contains(&to) {
            self.stats.messages_dropped += 1;
            return;
        }

        // Check for partition
        if self.is_partitioned(from, to) {
            self.stats.messages_dropped += 1;
            return;
        }

        // Random message drop
        if self.rng.next_bool(self.config.message_drop_probability) {
            self.stats.messages_dropped += 1;
            return;
        }

        // Calculate delivery delay
        let delay = self
            .rng
            .next_duration(self.config.min_message_delay, self.config.max_message_delay);

        let msg = InFlightMessage {
            from,
            to,
            message: message.clone(),
            deliver_at: self.clock.now() + delay,
            id: self.message_counter,
        };
        self.message_counter += 1;

        // Maybe duplicate the message
        if self
            .rng
            .next_bool(self.config.message_duplicate_probability)
        {
            self.stats.messages_duplicated += 1;

            let mut dup = msg.clone();
            dup.id = self.message_counter;
            self.message_counter += 1;

            // Duplicate arrives with additional delay
            let extra_delay = self.rng.next_duration(Duration::ZERO, delay);
            dup.deliver_at = msg.deliver_at + extra_delay;

            self.history.push(dup.clone());
            self.in_flight.push(dup);
        }

        self.history.push(msg.clone());
        self.in_flight.push(msg);
    }

    /// Get next message ready for delivery.
    ///
    /// Returns None if no messages are ready at the current virtual time.
    pub fn next_message(&mut self) -> Option<InFlightMessage> {
        while let Some(msg) = self.in_flight.peek() {
            if msg.deliver_at <= self.clock.now() {
                let msg = self.in_flight.pop().unwrap();

                // Re-check partition (might have changed since send)
                if self.is_partitioned(msg.from, msg.to) {
                    self.stats.messages_dropped += 1;
                    continue;
                }

                // Re-check if receiver crashed
                if self.crashed_nodes.contains(&msg.to) {
                    self.stats.messages_dropped += 1;
                    continue;
                }

                self.stats.messages_delivered += 1;
                return Some(msg);
            } else {
                break;
            }
        }
        None
    }

    /// Deliver all ready messages, returning them as a vector.
    pub fn drain_ready(&mut self) -> Vec<InFlightMessage> {
        let mut ready = Vec::new();
        while let Some(msg) = self.next_message() {
            ready.push(msg);
        }
        ready
    }

    /// Get time of next scheduled delivery.
    pub fn next_delivery_time(&self) -> Option<Duration> {
        self.in_flight.peek().map(|m| m.deliver_at)
    }

    /// Create a network partition between two nodes.
    ///
    /// Messages between these nodes will be dropped until healed.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        let key = (a.min(b), a.max(b));
        if self.partitions.insert(key) {
            self.stats.partitions_created += 1;
        }
    }

    /// Heal a network partition between two nodes.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        let key = (a.min(b), a.max(b));
        if self.partitions.remove(&key) {
            self.stats.partitions_healed += 1;
        }
    }

    /// Heal all partitions.
    pub fn heal_all(&mut self) {
        let count = self.partitions.len();
        self.partitions.clear();
        self.stats.partitions_healed += count as u64;
    }

    /// Isolate a node from all others.
    pub fn isolate(&mut self, node: NodeId, all_nodes: &[NodeId]) {
        for &other in all_nodes {
            if other != node {
                self.partition(node, other);
            }
        }
    }

    /// Check if two nodes are partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        let key = (a.min(b), a.max(b));
        self.partitions.contains(&key)
    }

    /// Mark a node as crashed.
    ///
    /// Messages to/from this node will be dropped.
    pub fn crash(&mut self, node: NodeId) {
        self.crashed_nodes.insert(node);
    }

    /// Restart a crashed node.
    pub fn restart(&mut self, node: NodeId) {
        self.crashed_nodes.remove(&node);
    }

    /// Check if a node is crashed.
    pub fn is_crashed(&self, node: NodeId) -> bool {
        self.crashed_nodes.contains(&node)
    }

    /// Get count of in-flight messages.
    pub fn pending_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Check if there are any pending messages.
    pub fn has_pending(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// Get message history for debugging.
    pub fn history(&self) -> &[InFlightMessage] {
        &self.history
    }

    /// Get network statistics.
    pub fn stats(&self) -> &NetworkStats {
        &self.stats
    }

    /// Clear message history (but keep stats).
    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    /// Get list of partitioned node pairs.
    pub fn partitions(&self) -> Vec<(NodeId, NodeId)> {
        self.partitions.iter().copied().collect()
    }

    /// Get list of crashed nodes.
    pub fn crashed_nodes(&self) -> Vec<NodeId> {
        self.crashed_nodes.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::PreAcceptRequest;
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};

    fn make_test_message() -> Message {
        let ts = Timestamp::new(100, 0, 1);
        let txn = Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        };
        Message::PreAccept(PreAcceptRequest {
            txn,
            ballot: Ballot::initial(1),
        })
    }

    #[test]
    fn test_send_and_receive() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock.clone(), rng, config);

        let msg = make_test_message();
        network.send(0, 1, msg);

        // Message not ready yet (need to advance time)
        assert!(network.next_message().is_none());
        assert_eq!(network.pending_count(), 1);

        // Advance past delivery time
        clock.advance(Duration::from_millis(10));

        let delivered = network.next_message();
        assert!(delivered.is_some());
        assert_eq!(delivered.unwrap().to, 1);
        assert_eq!(network.pending_count(), 0);
    }

    #[test]
    fn test_partition_blocks_messages() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock.clone(), rng, config);

        // Create partition
        network.partition(0, 1);
        assert!(network.is_partitioned(0, 1));
        assert!(network.is_partitioned(1, 0)); // Symmetric

        // Send message
        let msg = make_test_message();
        network.send(0, 1, msg);

        // Message should be dropped
        assert_eq!(network.pending_count(), 0);
        assert_eq!(network.stats().messages_dropped, 1);

        // Heal and send again
        network.heal(0, 1);
        assert!(!network.is_partitioned(0, 1));

        let msg = make_test_message();
        network.send(0, 1, msg);
        assert_eq!(network.pending_count(), 1);
    }

    #[test]
    fn test_crash_blocks_messages() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock.clone(), rng, config);

        // Crash node 1
        network.crash(1);
        assert!(network.is_crashed(1));

        // Messages to crashed node are dropped
        let msg = make_test_message();
        network.send(0, 1, msg);
        assert_eq!(network.pending_count(), 0);
        assert_eq!(network.stats().messages_dropped, 1);

        // Restart and send again
        network.restart(1);
        let msg = make_test_message();
        network.send(0, 1, msg);
        assert_eq!(network.pending_count(), 1);
    }

    #[test]
    fn test_message_drops() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none().with_message_drop(1.0); // Always drop
        let mut network = VirtualNetwork::new(clock, rng, config);

        let msg = make_test_message();
        network.send(0, 1, msg);

        assert_eq!(network.pending_count(), 0);
        assert_eq!(network.stats().messages_dropped, 1);
    }

    #[test]
    fn test_message_ordering() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock.clone(), rng, config);

        // Send multiple messages
        for _ in 0..5 {
            let msg = make_test_message();
            network.send(0, 1, msg);
        }

        // Advance time and collect all
        clock.advance(Duration::from_millis(100));
        let messages = network.drain_ready();

        assert_eq!(messages.len(), 5);

        // Should be ordered by delivery time
        for i in 1..messages.len() {
            assert!(messages[i - 1].deliver_at <= messages[i].deliver_at);
        }
    }

    #[test]
    fn test_isolate() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock, rng, config);

        let all_nodes = vec![0, 1, 2, 3, 4];
        network.isolate(2, &all_nodes);

        // Node 2 should be partitioned from everyone else
        assert!(network.is_partitioned(2, 0));
        assert!(network.is_partitioned(2, 1));
        assert!(network.is_partitioned(2, 3));
        assert!(network.is_partitioned(2, 4));

        // Other nodes can still communicate
        assert!(!network.is_partitioned(0, 1));
        assert!(!network.is_partitioned(3, 4));
    }

    #[test]
    fn test_heal_all() {
        let clock = VirtualClock::new();
        let rng = SimRng::new(42);
        let config = FaultConfig::none();
        let mut network = VirtualNetwork::new(clock, rng, config);

        network.partition(0, 1);
        network.partition(1, 2);
        network.partition(2, 3);

        assert_eq!(network.partitions().len(), 3);

        network.heal_all();

        assert!(network.partitions().is_empty());
    }
}
