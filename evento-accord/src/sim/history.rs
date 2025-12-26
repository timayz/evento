//! Operation history recording for linearizability checking.
//!
//! Records all operations with timing information for post-hoc analysis.

use crate::transport::NodeId;
use std::time::Duration;

/// Type of operation recorded in history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationType {
    /// Write operation started.
    WriteStart {
        /// Keys being written to.
        keys: Vec<String>,
    },
    /// Write operation completed.
    WriteEnd {
        /// Whether the write succeeded.
        success: bool,
    },
    /// Read operation started.
    ReadStart {
        /// Keys being read.
        keys: Vec<String>,
    },
    /// Read operation completed.
    ReadEnd {
        /// Values read (as strings for simplicity).
        values: Vec<String>,
    },
    /// Protocol message sent.
    MessageSent {
        /// Target node.
        to: NodeId,
        /// Message type name.
        message_type: String,
    },
    /// Protocol message received.
    MessageReceived {
        /// Source node.
        from: NodeId,
        /// Message type name.
        message_type: String,
    },
}

/// A recorded operation in the simulation history.
#[derive(Clone, Debug)]
pub struct Operation {
    /// Unique operation ID.
    pub id: u64,
    /// Node that performed the operation.
    pub node: NodeId,
    /// Virtual time when operation occurred.
    pub time: Duration,
    /// Type of operation.
    pub op_type: OperationType,
}

/// A complete request-response pair for a client operation.
#[derive(Clone, Debug)]
pub struct ClientOperation {
    /// Unique operation ID.
    pub id: u64,
    /// Node that handled the operation.
    pub node: NodeId,
    /// Keys involved.
    pub keys: Vec<String>,
    /// Time operation started.
    pub start_time: Duration,
    /// Time operation completed (None if still in progress).
    pub end_time: Option<Duration>,
    /// Whether operation succeeded.
    pub success: Option<bool>,
    /// Whether this was a write (vs read).
    pub is_write: bool,
    /// Result value for reads.
    pub result: Option<Vec<String>>,
}

impl ClientOperation {
    /// Check if operation is complete.
    pub fn is_complete(&self) -> bool {
        self.end_time.is_some()
    }

    /// Get duration of operation.
    pub fn duration(&self) -> Option<Duration> {
        self.end_time.map(|end| end - self.start_time)
    }

    /// Check if this operation overlaps with another in time.
    pub fn overlaps(&self, other: &ClientOperation) -> bool {
        let self_end = self.end_time.unwrap_or(Duration::MAX);
        let other_end = other.end_time.unwrap_or(Duration::MAX);

        self.start_time < other_end && other.start_time < self_end
    }
}

/// Records all operations for linearizability checking.
pub struct History {
    /// Raw operation log.
    operations: Vec<Operation>,
    /// Client operations (paired start/end).
    client_ops: Vec<ClientOperation>,
    /// Next operation ID.
    next_id: u64,
    /// Pending operations (started but not ended).
    pending: std::collections::HashMap<u64, ClientOperation>,
}

impl History {
    /// Create a new empty history.
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            client_ops: Vec::new(),
            next_id: 0,
            pending: std::collections::HashMap::new(),
        }
    }

    /// Record an operation.
    pub fn record(&mut self, node: NodeId, time: Duration, op_type: OperationType) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        self.operations.push(Operation {
            id,
            node,
            time,
            op_type: op_type.clone(),
        });

        // Track client operations
        match op_type {
            OperationType::WriteStart { keys } => {
                self.pending.insert(
                    id,
                    ClientOperation {
                        id,
                        node,
                        keys,
                        start_time: time,
                        end_time: None,
                        success: None,
                        is_write: true,
                        result: None,
                    },
                );
            }
            OperationType::ReadStart { keys } => {
                self.pending.insert(
                    id,
                    ClientOperation {
                        id,
                        node,
                        keys,
                        start_time: time,
                        end_time: None,
                        success: None,
                        is_write: false,
                        result: None,
                    },
                );
            }
            _ => {}
        }

        id
    }

    /// Record start of a write operation.
    pub fn write_start(&mut self, node: NodeId, time: Duration, keys: Vec<String>) -> u64 {
        self.record(node, time, OperationType::WriteStart { keys })
    }

    /// Record end of a write operation.
    pub fn write_end(&mut self, id: u64, time: Duration, success: bool) {
        self.record(
            self.pending.get(&id).map(|o| o.node).unwrap_or(0),
            time,
            OperationType::WriteEnd { success },
        );

        if let Some(mut op) = self.pending.remove(&id) {
            op.end_time = Some(time);
            op.success = Some(success);
            self.client_ops.push(op);
        }
    }

    /// Record start of a read operation.
    pub fn read_start(&mut self, node: NodeId, time: Duration, keys: Vec<String>) -> u64 {
        self.record(node, time, OperationType::ReadStart { keys })
    }

    /// Record end of a read operation.
    pub fn read_end(&mut self, id: u64, time: Duration, values: Vec<String>) {
        self.record(
            self.pending.get(&id).map(|o| o.node).unwrap_or(0),
            time,
            OperationType::ReadEnd {
                values: values.clone(),
            },
        );

        if let Some(mut op) = self.pending.remove(&id) {
            op.end_time = Some(time);
            op.success = Some(true);
            op.result = Some(values);
            self.client_ops.push(op);
        }
    }

    /// Record a protocol message sent.
    pub fn message_sent(&mut self, node: NodeId, time: Duration, to: NodeId, message_type: String) {
        self.record(node, time, OperationType::MessageSent { to, message_type });
    }

    /// Record a protocol message received.
    pub fn message_received(
        &mut self,
        node: NodeId,
        time: Duration,
        from: NodeId,
        message_type: String,
    ) {
        self.record(
            node,
            time,
            OperationType::MessageReceived { from, message_type },
        );
    }

    /// Get all raw operations.
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    /// Get all completed client operations.
    pub fn client_operations(&self) -> &[ClientOperation] {
        &self.client_ops
    }

    /// Get completed write operations.
    pub fn writes(&self) -> impl Iterator<Item = &ClientOperation> {
        self.client_ops.iter().filter(|op| op.is_write)
    }

    /// Get completed read operations.
    pub fn reads(&self) -> impl Iterator<Item = &ClientOperation> {
        self.client_ops.iter().filter(|op| !op.is_write)
    }

    /// Get operations that overlap with the given operation.
    pub fn concurrent_with(&self, op: &ClientOperation) -> Vec<&ClientOperation> {
        self.client_ops.iter().filter(|o| o.overlaps(op)).collect()
    }

    /// Get pending (incomplete) operations.
    pub fn pending(&self) -> impl Iterator<Item = &ClientOperation> {
        self.pending.values()
    }

    /// Check if there are pending operations.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Get count of operations.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clear all history.
    pub fn clear(&mut self) {
        self.operations.clear();
        self.client_ops.clear();
        self.pending.clear();
        self.next_id = 0;
    }

    /// Export history for external analysis.
    pub fn export(&self) -> HistoryExport {
        HistoryExport {
            operations: self.operations.clone(),
            client_ops: self.client_ops.clone(),
        }
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

/// Exported history data.
#[derive(Clone, Debug)]
pub struct HistoryExport {
    /// All raw operations.
    pub operations: Vec<Operation>,
    /// Client operations.
    pub client_ops: Vec<ClientOperation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_write() {
        let mut history = History::new();

        let id = history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        assert!(history.has_pending());
        assert_eq!(history.pending().count(), 1);

        history.write_end(id, Duration::from_millis(10), true);
        assert!(!history.has_pending());

        let ops: Vec<_> = history.writes().collect();
        assert_eq!(ops.len(), 1);
        assert!(ops[0].success.unwrap());
        assert_eq!(ops[0].duration(), Some(Duration::from_millis(10)));
    }

    #[test]
    fn test_record_read() {
        let mut history = History::new();

        let id = history.read_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        history.read_end(id, Duration::from_millis(5), vec!["value1".to_string()]);

        let ops: Vec<_> = history.reads().collect();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].result, Some(vec!["value1".to_string()]));
    }

    #[test]
    fn test_overlapping_operations() {
        let mut history = History::new();

        // Op 1: 0-10ms
        let id1 = history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        // Op 2: 5-15ms (overlaps with op1)
        let id2 = history.write_start(1, Duration::from_millis(5), vec!["key1".to_string()]);
        // Op 3: 20-25ms (no overlap)
        let id3 = history.write_start(0, Duration::from_millis(20), vec!["key1".to_string()]);

        history.write_end(id1, Duration::from_millis(10), true);
        history.write_end(id2, Duration::from_millis(15), true);
        history.write_end(id3, Duration::from_millis(25), true);

        let ops: Vec<_> = history.writes().collect();
        assert_eq!(ops.len(), 3);

        // Op1 overlaps with op2 but not op3
        let concurrent = history.concurrent_with(&ops[0]);
        assert_eq!(concurrent.len(), 2); // overlaps with itself and op2
    }

    #[test]
    fn test_message_tracking() {
        let mut history = History::new();

        history.message_sent(0, Duration::from_millis(0), 1, "PreAccept".to_string());
        history.message_received(1, Duration::from_millis(5), 0, "PreAccept".to_string());

        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_export() {
        let mut history = History::new();

        let id = history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        history.write_end(id, Duration::from_millis(10), true);

        let export = history.export();
        assert_eq!(export.operations.len(), 2);
        assert_eq!(export.client_ops.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut history = History::new();

        let id = history.write_start(0, Duration::from_millis(0), vec!["key1".to_string()]);
        history.write_end(id, Duration::from_millis(10), true);

        assert!(!history.is_empty());

        history.clear();

        assert!(history.is_empty());
        assert!(!history.has_pending());
    }
}
