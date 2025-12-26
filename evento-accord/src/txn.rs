//! Transaction types for ACCORD consensus.
//!
//! A transaction represents a batch of events that must be atomically
//! written with consensus across the cluster.

use crate::timestamp::Timestamp;

/// Unique transaction identifier.
///
/// The transaction ID is based on the timestamp assigned when the
/// transaction was first proposed. This provides natural ordering
/// and uniqueness across the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bitcode::Encode, bitcode::Decode)]
pub struct TxnId {
    /// The timestamp that uniquely identifies this transaction
    pub timestamp: Timestamp,
}

impl TxnId {
    /// Create a new transaction ID from a timestamp
    pub fn new(timestamp: Timestamp) -> Self {
        Self { timestamp }
    }
}

impl PartialOrd for TxnId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TxnId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl std::fmt::Display for TxnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "txn:{}", self.timestamp)
    }
}

impl std::str::FromStr for TxnId {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.strip_prefix("txn:").unwrap_or(s);
        let timestamp = s.parse::<Timestamp>().map_err(|e| e.to_string())?;
        Ok(TxnId::new(timestamp))
    }
}

/// Transaction status in the ACCORD protocol.
///
/// Transactions progress through these states:
/// PreAccepted -> Accepted -> Committed -> Executed
///
/// If execution fails, the status becomes ExecutionFailed.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, bitcode::Encode, bitcode::Decode,
)]
#[repr(u8)]
pub enum TxnStatus {
    /// Transaction has been pre-accepted (phase 1)
    PreAccepted = 0,
    /// Transaction has been accepted with final deps (phase 2, slow path only)
    Accepted = 1,
    /// Transaction has been committed (phase 3)
    Committed = 2,
    /// Transaction has been executed (phase 4)
    Executed = 3,
    /// Transaction execution failed (e.g., version conflict)
    ExecutionFailed = 4,
}

impl TxnStatus {
    /// Check if the transaction can still be modified
    pub fn is_mutable(&self) -> bool {
        matches!(self, Self::PreAccepted)
    }

    /// Check if the transaction decision is final
    pub fn is_decided(&self) -> bool {
        matches!(
            self,
            Self::Committed | Self::Executed | Self::ExecutionFailed
        )
    }

    /// Check if the transaction has been applied
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Executed)
    }

    /// Check if the transaction execution failed
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::ExecutionFailed)
    }

    /// Check if the transaction is terminal (executed or failed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Executed | Self::ExecutionFailed)
    }
}

impl std::fmt::Display for TxnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreAccepted => write!(f, "pre-accepted"),
            Self::Accepted => write!(f, "accepted"),
            Self::Committed => write!(f, "committed"),
            Self::Executed => write!(f, "executed"),
            Self::ExecutionFailed => write!(f, "execution-failed"),
        }
    }
}

/// Ballot number for Paxos-style leader election within a transaction.
///
/// When multiple nodes try to coordinate the same transaction (e.g., during
/// recovery), the ballot ensures only one can succeed. Higher ballots win.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, bitcode::Encode, bitcode::Decode)]
pub struct Ballot {
    /// Round number (incremented on each attempt)
    pub round: u32,
    /// Node ID (for tie-breaking)
    pub node_id: u16,
}

impl Ballot {
    /// Create a new ballot
    pub fn new(round: u32, node_id: u16) -> Self {
        Self { round, node_id }
    }

    /// Create the initial ballot for a node
    pub fn initial(node_id: u16) -> Self {
        Self { round: 0, node_id }
    }

    /// Create the next ballot for this node (higher round)
    pub fn next(&self, node_id: u16) -> Self {
        Self {
            round: self.round + 1,
            node_id,
        }
    }

    /// Create a ballot that is guaranteed to be higher than this one
    pub fn higher(&self, node_id: u16) -> Self {
        if node_id > self.node_id {
            Self {
                round: self.round,
                node_id,
            }
        } else {
            Self {
                round: self.round + 1,
                node_id,
            }
        }
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.round
            .cmp(&other.round)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl std::fmt::Display for Ballot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.round, self.node_id)
    }
}

/// A transaction in the ACCORD protocol.
///
/// Contains the events to write and all metadata needed for consensus.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct Transaction {
    /// Unique identifier (based on initial timestamp)
    pub id: TxnId,

    /// Serialized events (bitcode encoded Vec<Event>)
    pub events_data: Vec<u8>,

    /// Keys this transaction touches (for conflict detection)
    /// Format: "aggregator_type:aggregator_id"
    pub keys: Vec<String>,

    /// Dependencies: transactions that must execute before this one
    pub deps: Vec<TxnId>,

    /// Current status in the protocol
    pub status: TxnStatus,

    /// Final execution timestamp (may differ from id.timestamp after Accept)
    pub execute_at: Timestamp,

    /// Current ballot (for Paxos-style coordination)
    pub ballot: Ballot,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: TxnId, events_data: Vec<u8>, keys: Vec<String>, node_id: u16) -> Self {
        Self {
            id,
            events_data,
            keys,
            deps: Vec::new(),
            status: TxnStatus::PreAccepted,
            execute_at: id.timestamp,
            ballot: Ballot::initial(node_id),
        }
    }

    /// Check if this transaction conflicts with another (overlapping keys)
    pub fn conflicts_with(&self, other: &Transaction) -> bool {
        // Both key lists are sorted, so we can do O(n+m) merge check
        let mut i = 0;
        let mut j = 0;

        while i < self.keys.len() && j < other.keys.len() {
            match self.keys[i].cmp(&other.keys[j]) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }

        false
    }

    /// Add a dependency (transaction that must execute before this one)
    pub fn add_dep(&mut self, dep: TxnId) {
        if !self.deps.contains(&dep) && dep != self.id {
            self.deps.push(dep);
            self.deps.sort();
        }
    }

    /// Merge dependencies from another set
    pub fn merge_deps(&mut self, other_deps: &[TxnId]) {
        for &dep in other_deps {
            self.add_dep(dep);
        }
    }

    /// Update execute_at to be at least as high as the given timestamp
    pub fn update_execute_at(&mut self, ts: Timestamp) {
        if ts > self.execute_at {
            self.execute_at = ts;
        }
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Transaction({}, status={}, deps={}, keys={})",
            self.id,
            self.status,
            self.deps.len(),
            self.keys.len()
        )
    }
}

/// Serializable event wrapper for bitcode encoding.
///
/// This wraps `evento_core::Event` with a `[u8; 16]` id field instead of `Ulid`
/// since Ulid doesn't implement bitcode traits. Use `.into()` to convert.
#[derive(Clone, Debug, PartialEq, bitcode::Encode, bitcode::Decode)]
pub struct SerializableEvent {
    /// Unique event identifier (ULID bytes)
    pub id: [u8; 16],
    /// ID of the aggregate this event belongs to
    pub aggregator_id: String,
    /// Type name of the aggregate
    pub aggregator_type: String,
    /// Version number of the aggregate after this event
    pub version: u16,
    /// Event type name
    pub name: String,
    /// Optional routing key for event distribution
    pub routing_key: Option<String>,
    /// Serialized event data
    pub data: Vec<u8>,
    /// Serialized event metadata
    pub metadata: Vec<u8>,
    /// Unix timestamp (seconds)
    pub timestamp: u64,
    /// Sub-second precision (milliseconds)
    pub timestamp_subsec: u32,
}

impl From<evento_core::Event> for SerializableEvent {
    fn from(e: evento_core::Event) -> Self {
        Self {
            id: e.id.to_bytes(),
            aggregator_id: e.aggregator_id,
            aggregator_type: e.aggregator_type,
            version: e.version,
            name: e.name,
            routing_key: e.routing_key,
            data: e.data,
            metadata: e.metadata,
            timestamp: e.timestamp,
            timestamp_subsec: e.timestamp_subsec,
        }
    }
}

impl From<SerializableEvent> for evento_core::Event {
    fn from(e: SerializableEvent) -> Self {
        Self {
            id: ulid::Ulid::from_bytes(e.id),
            aggregator_id: e.aggregator_id,
            aggregator_type: e.aggregator_type,
            version: e.version,
            name: e.name,
            routing_key: e.routing_key,
            data: e.data,
            metadata: e.metadata,
            timestamp: e.timestamp,
            timestamp_subsec: e.timestamp_subsec,
        }
    }
}

impl SerializableEvent {
    /// Encode a list of events to bytes.
    pub fn encode_events(events: &[evento_core::Event]) -> Vec<u8> {
        let serializable: Vec<SerializableEvent> = events.iter().cloned().map(Into::into).collect();
        bitcode::encode(&serializable)
    }

    /// Decode a list of events from bytes.
    pub fn decode_events(data: &[u8]) -> Result<Vec<evento_core::Event>, bitcode::Error> {
        let serializable: Vec<SerializableEvent> = bitcode::decode(data)?;
        Ok(serializable.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(time: u64, seq: u32, node: u16) -> Timestamp {
        Timestamp::new(time, seq, node)
    }

    fn make_txn_id(time: u64, seq: u32, node: u16) -> TxnId {
        TxnId::new(make_timestamp(time, seq, node))
    }

    #[test]
    fn test_txn_id_ordering() {
        let id1 = make_txn_id(100, 0, 1);
        let id2 = make_txn_id(100, 1, 1);
        let id3 = make_txn_id(101, 0, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn test_txn_status_progression() {
        assert!(TxnStatus::PreAccepted.is_mutable());
        assert!(!TxnStatus::Accepted.is_mutable());

        assert!(!TxnStatus::PreAccepted.is_decided());
        assert!(TxnStatus::Committed.is_decided());
        assert!(TxnStatus::Executed.is_decided());

        assert!(!TxnStatus::Committed.is_applied());
        assert!(TxnStatus::Executed.is_applied());
    }

    #[test]
    fn test_ballot_ordering() {
        let b1 = Ballot::new(0, 1);
        let b2 = Ballot::new(0, 2);
        let b3 = Ballot::new(1, 0);

        assert!(b1 < b2, "same round, higher node_id should win");
        assert!(b2 < b3, "higher round should win");
    }

    #[test]
    fn test_ballot_next() {
        let b1 = Ballot::new(5, 3);
        let b2 = b1.next(3);

        assert_eq!(b2.round, 6);
        assert_eq!(b2.node_id, 3);
        assert!(b2 > b1);
    }

    #[test]
    fn test_ballot_higher() {
        let b1 = Ballot::new(5, 3);

        // Higher node_id can use same round
        let b2 = b1.higher(5);
        assert_eq!(b2.round, 5);
        assert_eq!(b2.node_id, 5);
        assert!(b2 > b1);

        // Lower node_id must use higher round
        let b3 = b1.higher(1);
        assert_eq!(b3.round, 6);
        assert_eq!(b3.node_id, 1);
        assert!(b3 > b1);
    }

    #[test]
    fn test_transaction_conflicts() {
        let id1 = make_txn_id(100, 0, 1);
        let id2 = make_txn_id(101, 0, 1);

        let txn1 = Transaction::new(
            id1,
            vec![],
            vec!["type:agg1".to_string(), "type:agg2".to_string()],
            1,
        );

        let txn2 = Transaction::new(
            id2,
            vec![],
            vec!["type:agg2".to_string(), "type:agg3".to_string()],
            1,
        );

        let txn3 = Transaction::new(id2, vec![], vec!["type:agg4".to_string()], 1);

        assert!(txn1.conflicts_with(&txn2), "should conflict on agg2");
        assert!(!txn1.conflicts_with(&txn3), "no overlapping keys");
    }

    #[test]
    fn test_transaction_deps() {
        let id1 = make_txn_id(100, 0, 1);
        let id2 = make_txn_id(101, 0, 1);
        let id3 = make_txn_id(102, 0, 1);

        let mut txn = Transaction::new(id3, vec![], vec![], 1);

        txn.add_dep(id2);
        txn.add_dep(id1);
        txn.add_dep(id2); // Duplicate, should be ignored

        assert_eq!(txn.deps.len(), 2);
        assert_eq!(txn.deps[0], id1); // Sorted
        assert_eq!(txn.deps[1], id2);
    }

    #[test]
    fn test_transaction_merge_deps() {
        let id1 = make_txn_id(100, 0, 1);
        let id2 = make_txn_id(101, 0, 1);
        let id3 = make_txn_id(102, 0, 1);
        let id4 = make_txn_id(103, 0, 1);

        let mut txn = Transaction::new(id4, vec![], vec![], 1);
        txn.add_dep(id1);

        txn.merge_deps(&[id2, id3, id1]); // id1 is duplicate

        assert_eq!(txn.deps.len(), 3);
    }

    #[test]
    fn test_transaction_serialization() {
        let id = make_txn_id(100, 5, 3);
        let txn = Transaction::new(
            id,
            vec![1, 2, 3, 4],
            vec!["key1".to_string(), "key2".to_string()],
            3,
        );

        let encoded = bitcode::encode(&txn);
        let decoded: Transaction = bitcode::decode(&encoded).unwrap();

        assert_eq!(decoded.id, txn.id);
        assert_eq!(decoded.events_data, txn.events_data);
        assert_eq!(decoded.keys, txn.keys);
        assert_eq!(decoded.status, txn.status);
    }

    #[test]
    fn test_serializable_event_roundtrip() {
        let event = evento_core::Event {
            id: ulid::Ulid::new(),
            aggregator_id: "agg-123".to_string(),
            aggregator_type: "test/Account".to_string(),
            version: 1,
            name: "AccountOpened".to_string(),
            routing_key: Some("tenant-1".to_string()),
            data: vec![1, 2, 3, 4],
            metadata: vec![5, 6, 7, 8],
            timestamp: 1234567890,
            timestamp_subsec: 500,
        };

        // Single event roundtrip
        let serializable: SerializableEvent = event.clone().into();
        let back: evento_core::Event = serializable.into();
        assert_eq!(back.id, event.id);
        assert_eq!(back.aggregator_id, event.aggregator_id);
        assert_eq!(back.data, event.data);

        // Encode/decode multiple events
        let events = vec![event.clone(), event.clone()];
        let encoded = SerializableEvent::encode_events(&events);
        let decoded = SerializableEvent::decode_events(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].id, event.id);
        assert_eq!(decoded[1].aggregator_type, event.aggregator_type);
    }
}
