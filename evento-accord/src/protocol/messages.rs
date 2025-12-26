//! Protocol messages for ACCORD consensus.
//!
//! All messages exchanged between nodes during the consensus protocol.

use crate::timestamp::Timestamp;
use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};

/// All protocol message types.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub enum Message {
    // Phase 1: PreAccept
    PreAccept(PreAcceptRequest),
    PreAcceptOk(PreAcceptResponse),
    PreAcceptNack(PreAcceptNack),

    // Phase 2: Accept (slow path only)
    Accept(AcceptRequest),
    AcceptOk(AcceptResponse),
    AcceptNack(AcceptNack),

    // Phase 3: Commit
    Commit(CommitRequest),
    CommitOk(CommitResponse),

    // Recovery
    Recover(RecoverRequest),
    RecoverReply(RecoverResponse),

    // State Sync (for catch-up after restart)
    SyncRequest(SyncRequest),
    SyncResponse(SyncResponse),
}

impl Message {
    /// Get the transaction ID this message is about.
    /// Returns None for sync messages which don't relate to a single transaction.
    pub fn txn_id_opt(&self) -> Option<TxnId> {
        match self {
            Message::PreAccept(m) => Some(m.txn.id),
            Message::PreAcceptOk(m) => Some(m.txn_id),
            Message::PreAcceptNack(m) => Some(m.txn_id),
            Message::Accept(m) => Some(m.txn_id),
            Message::AcceptOk(m) => Some(m.txn_id),
            Message::AcceptNack(m) => Some(m.txn_id),
            Message::Commit(m) => Some(m.txn_id),
            Message::CommitOk(m) => Some(m.txn_id),
            Message::Recover(m) => Some(m.txn_id),
            Message::RecoverReply(m) => Some(m.txn_id),
            Message::SyncRequest(_) => None,
            Message::SyncResponse(m) => m.request_id,
        }
    }

    /// Get the transaction ID this message is about.
    /// For SyncRequest, returns the request_id.
    /// For SyncResponse, returns the request_id if present, otherwise panics.
    pub fn txn_id(&self) -> TxnId {
        match self {
            Message::SyncRequest(req) => req.request_id,
            _ => self
                .txn_id_opt()
                .expect("Message should have a txn_id"),
        }
    }

    /// Get the message type name for logging.
    pub fn type_name(&self) -> &'static str {
        match self {
            Message::PreAccept(_) => "PreAccept",
            Message::PreAcceptOk(_) => "PreAcceptOk",
            Message::PreAcceptNack(_) => "PreAcceptNack",
            Message::Accept(_) => "Accept",
            Message::AcceptOk(_) => "AcceptOk",
            Message::AcceptNack(_) => "AcceptNack",
            Message::Commit(_) => "Commit",
            Message::CommitOk(_) => "CommitOk",
            Message::Recover(_) => "Recover",
            Message::RecoverReply(_) => "RecoverReply",
            Message::SyncRequest(_) => "SyncRequest",
            Message::SyncResponse(_) => "SyncResponse",
        }
    }
}

// ============ PreAccept Phase ============

/// PreAccept request: propose a new transaction.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptRequest {
    /// The transaction being proposed
    pub txn: Transaction,
    /// Ballot for this proposal
    pub ballot: Ballot,
}

/// PreAccept response: agreement with computed dependencies.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptResponse {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Dependencies computed by this replica
    pub deps: Vec<TxnId>,
    /// Ballot acknowledged
    pub ballot: Ballot,
}

/// PreAccept rejection: higher ballot seen.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptNack {
    /// Transaction ID
    pub txn_id: TxnId,
    /// The higher ballot that was seen
    pub higher_ballot: Ballot,
}

// ============ Accept Phase ============

/// Accept request: finalize dependencies (slow path).
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptRequest {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Final dependencies
    pub deps: Vec<TxnId>,
    /// Final execution timestamp
    pub execute_at: Timestamp,
    /// Ballot for this proposal
    pub ballot: Ballot,
}

/// Accept response: acknowledgment.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptResponse {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Ballot acknowledged
    pub ballot: Ballot,
}

/// Accept rejection: higher ballot seen.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptNack {
    /// Transaction ID
    pub txn_id: TxnId,
    /// The higher ballot that was seen
    pub higher_ballot: Ballot,
}

// ============ Commit Phase ============

/// Commit request: notify replicas of committed transaction.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct CommitRequest {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Final dependencies
    pub deps: Vec<TxnId>,
    /// Final execution timestamp
    pub execute_at: Timestamp,
    /// Events data (for replicas that missed PreAccept)
    pub events_data: Vec<u8>,
    /// Keys (for replicas that missed PreAccept)
    pub keys: Vec<String>,
}

/// Commit response: acknowledgment.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct CommitResponse {
    /// Transaction ID
    pub txn_id: TxnId,
}

// ============ Recovery ============

/// Recovery request: query status of a transaction.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct RecoverRequest {
    /// Transaction ID to recover
    pub txn_id: TxnId,
    /// Ballot for recovery attempt
    pub ballot: Ballot,
}

/// Recovery response: current state of transaction.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct RecoverResponse {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Current status (None if unknown)
    pub status: Option<TxnStatus>,
    /// Known dependencies
    pub deps: Vec<TxnId>,
    /// Execution timestamp (if known)
    pub execute_at: Option<Timestamp>,
    /// Ballot this replica has seen
    pub ballot: Ballot,
    /// Events data (if known)
    pub events_data: Option<Vec<u8>>,
    /// Keys (if known)
    pub keys: Option<Vec<String>>,
}

// ============ State Sync ============

/// Sync request: request committed transactions for catch-up.
///
/// A node sends this after restart to catch up on transactions
/// it might have missed while down.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct SyncRequest {
    /// Request ID for correlating response (use a txn_id as unique ID)
    pub request_id: TxnId,
    /// Only return transactions with execute_at >= this timestamp
    /// Set to Timestamp::ZERO to get all committed transactions
    pub since: Timestamp,
    /// Set of transaction IDs already known (to avoid duplicates)
    pub known_txns: Vec<TxnId>,
}

/// Sync response: committed transactions for catch-up.
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct SyncResponse {
    /// Request ID for correlating with request
    pub request_id: Option<TxnId>,
    /// Committed transactions the requester is missing
    pub transactions: Vec<Transaction>,
    /// Whether there are more transactions to sync (for pagination)
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_txn() -> Transaction {
        let ts = Timestamp::new(100, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[test]
    fn test_message_serialization() {
        let txn = make_txn();
        let msg = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::new(0, 1),
        });

        let encoded = bitcode::encode(&msg);
        let decoded: Message = bitcode::decode(&encoded).unwrap();

        assert!(matches!(decoded, Message::PreAccept(_)));
        if let Message::PreAccept(req) = decoded {
            assert_eq!(req.txn.id, txn.id);
        }
    }

    #[test]
    fn test_message_txn_id() {
        let txn = make_txn();
        let id = txn.id;

        let msg = Message::PreAccept(PreAcceptRequest {
            txn,
            ballot: Ballot::new(0, 1),
        });

        assert_eq!(msg.txn_id(), id);
    }

    #[test]
    fn test_all_message_types_serialize() {
        let txn = make_txn();
        let id = txn.id;
        let ballot = Ballot::new(1, 1);
        let ts = Timestamp::new(100, 0, 1);

        let messages = vec![
            Message::PreAccept(PreAcceptRequest {
                txn: txn.clone(),
                ballot,
            }),
            Message::PreAcceptOk(PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            }),
            Message::PreAcceptNack(PreAcceptNack {
                txn_id: id,
                higher_ballot: ballot,
            }),
            Message::Accept(AcceptRequest {
                txn_id: id,
                deps: vec![],
                execute_at: ts,
                ballot,
            }),
            Message::AcceptOk(AcceptResponse { txn_id: id, ballot }),
            Message::AcceptNack(AcceptNack {
                txn_id: id,
                higher_ballot: ballot,
            }),
            Message::Commit(CommitRequest {
                txn_id: id,
                deps: vec![],
                execute_at: ts,
                events_data: vec![1, 2, 3],
                keys: vec!["key".to_string()],
            }),
            Message::CommitOk(CommitResponse { txn_id: id }),
            Message::Recover(RecoverRequest { txn_id: id, ballot }),
            Message::RecoverReply(RecoverResponse {
                txn_id: id,
                status: Some(TxnStatus::Committed),
                deps: vec![],
                execute_at: Some(ts),
                ballot,
                events_data: Some(vec![1, 2, 3]),
                keys: Some(vec!["key".to_string()]),
            }),
        ];

        for msg in messages {
            let encoded = bitcode::encode(&msg);
            let decoded: Message = bitcode::decode(&encoded).unwrap();
            assert_eq!(decoded.txn_id(), id);
        }
    }
}
