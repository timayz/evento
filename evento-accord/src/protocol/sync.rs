//! State synchronization protocol for crash recovery.
//!
//! When a node restarts, it may have missed committed transactions.
//! The sync protocol allows it to catch up from peer nodes.

use crate::error::Result;
use crate::protocol::messages::{Message, SyncRequest, SyncResponse};
use crate::state::ConsensusState;
use std::collections::HashSet;

/// Handle a sync request from another node.
///
/// Returns committed transactions the requester is missing.
pub async fn handle(state: &ConsensusState, req: SyncRequest) -> Result<Message> {
    let exclude: HashSet<_> = req.known_txns.into_iter().collect();
    let transactions = state.get_committed_for_sync(req.since, &exclude).await;

    Ok(Message::SyncResponse(SyncResponse {
        request_id: Some(req.request_id),
        transactions,
        has_more: false, // Simple implementation without pagination
    }))
}

/// Create a sync request to get missing transactions.
pub fn create_request(
    request_id: crate::txn::TxnId,
    since: crate::timestamp::Timestamp,
    known_txns: Vec<crate::txn::TxnId>,
) -> Message {
    Message::SyncRequest(SyncRequest {
        request_id,
        since,
        known_txns,
    })
}

/// Apply sync response to local state.
///
/// Returns the number of transactions applied.
pub async fn apply_response(state: &ConsensusState, resp: SyncResponse) -> Result<usize> {
    state.apply_sync_transactions(resp.transactions).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};

    fn make_committed_txn(time: u64, keys: &[&str]) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: keys.iter().map(|s| s.to_string()).collect(),
            deps: vec![],
            status: TxnStatus::Committed,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_sync_request_returns_committed() {
        let state = ConsensusState::new();

        // Add some committed transactions
        let txn1 = make_committed_txn(100, &["key1"]);
        let txn2 = make_committed_txn(200, &["key2"]);

        state
            .create_from_commit(
                txn1.id,
                txn1.deps.clone(),
                txn1.execute_at,
                txn1.events_data.clone(),
                txn1.keys.clone(),
            )
            .await
            .unwrap();
        state
            .create_from_commit(
                txn2.id,
                txn2.deps.clone(),
                txn2.execute_at,
                txn2.events_data.clone(),
                txn2.keys.clone(),
            )
            .await
            .unwrap();

        // Request sync from timestamp 0
        let request_id = TxnId::new(Timestamp::new(999, 0, 1));
        let req = SyncRequest {
            request_id,
            since: Timestamp::ZERO,
            known_txns: vec![],
        };

        let response = handle(&state, req).await.unwrap();

        if let Message::SyncResponse(resp) = response {
            assert_eq!(resp.transactions.len(), 2);
            assert_eq!(resp.request_id, Some(request_id));
        } else {
            panic!("Expected SyncResponse");
        }
    }

    #[tokio::test]
    async fn test_sync_excludes_known() {
        let state = ConsensusState::new();

        let txn1 = make_committed_txn(100, &["key1"]);
        let txn2 = make_committed_txn(200, &["key2"]);

        state
            .create_from_commit(
                txn1.id,
                txn1.deps.clone(),
                txn1.execute_at,
                txn1.events_data.clone(),
                txn1.keys.clone(),
            )
            .await
            .unwrap();
        state
            .create_from_commit(
                txn2.id,
                txn2.deps.clone(),
                txn2.execute_at,
                txn2.events_data.clone(),
                txn2.keys.clone(),
            )
            .await
            .unwrap();

        // Request sync but exclude txn1
        let request_id = TxnId::new(Timestamp::new(999, 0, 1));
        let req = SyncRequest {
            request_id,
            since: Timestamp::ZERO,
            known_txns: vec![txn1.id],
        };

        let response = handle(&state, req).await.unwrap();

        if let Message::SyncResponse(resp) = response {
            assert_eq!(resp.transactions.len(), 1);
            assert_eq!(resp.transactions[0].id, txn2.id);
        } else {
            panic!("Expected SyncResponse");
        }
    }

    #[tokio::test]
    async fn test_apply_response() {
        let state = ConsensusState::new();

        let txn = make_committed_txn(100, &["key1"]);

        let resp = SyncResponse {
            request_id: None,
            transactions: vec![txn.clone()],
            has_more: false,
        };

        let applied = apply_response(&state, resp).await.unwrap();

        assert_eq!(applied, 1);
        assert!(state.get(&txn.id).await.is_some());
        assert_eq!(state.get_status(&txn.id).await, Some(TxnStatus::Committed));
    }
}
