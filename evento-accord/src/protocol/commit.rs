//! Commit phase of the ACCORD protocol.
//!
//! This is Phase 3 where the coordinator notifies replicas that a
//! transaction has been committed. This is fire-and-forget.

use crate::error::Result;
use crate::protocol::messages::{CommitRequest, CommitResponse, Message};
use crate::state::ConsensusState;
use crate::timestamp::Timestamp;
use crate::txn::TxnId;

/// Run Commit phase as coordinator.
///
/// Commits locally and broadcasts to replicas (fire and forget).
pub async fn coordinate(
    state: &ConsensusState,
    txn_id: TxnId,
    deps: Vec<TxnId>,
    execute_at: Timestamp,
    events_data: Vec<u8>,
    keys: Vec<String>,
) -> Result<CommitRequest> {
    // Commit locally
    state.commit(txn_id).await?;

    // Create commit message for broadcast
    Ok(CommitRequest {
        txn_id,
        deps,
        execute_at,
        events_data,
        keys,
    })
}

/// Handle a Commit request as a replica.
///
/// Returns the response message to send back.
pub async fn handle(state: &ConsensusState, request: CommitRequest) -> Result<Message> {
    let txn_id = request.txn_id;

    // Check if we have this transaction
    if state.get(&txn_id).await.is_some() {
        // We have it, just commit
        state.commit(txn_id).await?;
    } else {
        // We missed PreAccept, create from commit
        state
            .create_from_commit(
                txn_id,
                request.deps,
                request.execute_at,
                request.events_data,
                request.keys,
            )
            .await?;
    }

    Ok(Message::CommitOk(CommitResponse { txn_id }))
}

/// Create a Commit request message.
pub fn create_request(
    txn_id: TxnId,
    deps: Vec<TxnId>,
    execute_at: Timestamp,
    events_data: Vec<u8>,
    keys: Vec<String>,
) -> Message {
    Message::Commit(CommitRequest {
        txn_id,
        deps,
        execute_at,
        events_data,
        keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::{Ballot, Transaction, TxnStatus};

    fn make_txn(time: u64, keys: &[&str]) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: keys.iter().map(|s| s.to_string()).collect(),
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_coordinate_commit() {
        let state = ConsensusState::new();

        // Setup: PreAccept first
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();

        // Commit
        let request = coordinate(
            &state,
            txn.id,
            vec![],
            txn.execute_at,
            txn.events_data.clone(),
            txn.keys.clone(),
        )
        .await
        .unwrap();

        assert_eq!(request.txn_id, txn.id);

        // Verify local state
        let status = state.get_status(&txn.id).await.unwrap();
        assert_eq!(status, TxnStatus::Committed);
    }

    #[tokio::test]
    async fn test_handle_commit_existing() {
        let state = ConsensusState::new();

        // Setup: PreAccept first
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();

        // Handle commit
        let request = CommitRequest {
            txn_id: txn.id,
            deps: vec![],
            execute_at: txn.execute_at,
            events_data: txn.events_data.clone(),
            keys: txn.keys.clone(),
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::CommitOk(resp) => {
                assert_eq!(resp.txn_id, txn.id);
            }
            _ => panic!("Expected CommitOk"),
        }

        // Verify state
        let status = state.get_status(&txn.id).await.unwrap();
        assert_eq!(status, TxnStatus::Committed);
    }

    #[tokio::test]
    async fn test_handle_commit_missed_preaccept() {
        let state = ConsensusState::new();

        // Handle commit without prior PreAccept
        let ts = Timestamp::new(100, 0, 1);
        let txn_id = TxnId::new(ts);

        let request = CommitRequest {
            txn_id,
            deps: vec![],
            execute_at: ts,
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::CommitOk(resp) => {
                assert_eq!(resp.txn_id, txn_id);
            }
            _ => panic!("Expected CommitOk"),
        }

        // Verify transaction was created
        let txn = state.get(&txn_id).await.unwrap();
        assert_eq!(txn.status, TxnStatus::Committed);
        assert_eq!(txn.events_data, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_committed_transaction_is_ready() {
        let state = ConsensusState::new();

        // PreAccept and commit a transaction with no deps
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();
        state.commit(txn.id).await.unwrap();

        // Should be ready for execution
        assert!(state.is_ready(&txn.id).await);

        let next = state.next_executable().await;
        assert!(next.is_some());
        assert_eq!(next.unwrap().id, txn.id);
    }
}
