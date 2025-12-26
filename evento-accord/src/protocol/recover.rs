//! Transaction recovery protocol for ACCORD.
//!
//! When a transaction is stuck (committed but dependencies not satisfied),
//! the recovery protocol queries peers to find missing transactions.

use crate::error::Result;
use crate::protocol::messages::{Message, RecoverRequest, RecoverResponse};
use crate::state::ConsensusState;
use crate::txn::{Ballot, TxnId, TxnStatus};

/// Handle a recovery request from another node.
///
/// Returns our knowledge about the requested transaction.
pub async fn handle(state: &ConsensusState, req: RecoverRequest) -> Result<Message> {
    let txn = state.get(&req.txn_id).await;

    let response = match txn {
        Some(t) => RecoverResponse {
            txn_id: req.txn_id,
            status: Some(t.status),
            deps: t.deps.clone(),
            execute_at: Some(t.execute_at),
            ballot: t.ballot,
            events_data: Some(t.events_data.clone()),
            keys: Some(t.keys.clone()),
        },
        None => RecoverResponse {
            txn_id: req.txn_id,
            status: None,
            deps: vec![],
            execute_at: None,
            ballot: Ballot::default(),
            events_data: None,
            keys: None,
        },
    };

    Ok(Message::RecoverReply(response))
}

/// Create a recovery request for a transaction.
pub fn create_request(txn_id: TxnId, ballot: Ballot) -> Message {
    Message::Recover(RecoverRequest { txn_id, ballot })
}

/// Result of attempting to recover a transaction.
#[derive(Debug)]
pub enum RecoveryResult {
    /// Transaction was found and recovered
    Recovered {
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: crate::timestamp::Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    },
    /// Transaction was not found on any peer (can be considered aborted)
    NotFound,
    /// Recovery failed (no quorum)
    Failed,
}

/// Coordinate recovery of a transaction by querying peers.
///
/// Returns the recovered transaction data if found on any peer.
pub async fn coordinate(
    responses: Vec<Result<Message>>,
    txn_id: TxnId,
) -> RecoveryResult {
    let mut best_response: Option<RecoverResponse> = None;
    let mut success_count = 0;

    for response in responses {
        match response {
            Ok(Message::RecoverReply(resp)) if resp.txn_id == txn_id => {
                success_count += 1;

                // Keep the response with the highest status
                if let Some(status) = resp.status {
                    let dominated = match &best_response {
                        None => true,
                        Some(best) => match best.status {
                            None => true,
                            Some(best_status) => status > best_status,
                        },
                    };

                    if dominated && resp.events_data.is_some() {
                        best_response = Some(resp);
                    }
                }
            }
            Ok(_) => {
                // Unexpected response type
            }
            Err(e) => {
                tracing::debug!("Recovery query failed: {}", e);
            }
        }
    }

    // If we found a committed/executed transaction on any peer, recover it
    if let Some(resp) = best_response {
        if let (Some(status), Some(execute_at), Some(events_data), Some(keys)) = (
            resp.status,
            resp.execute_at,
            resp.events_data,
            resp.keys,
        ) {
            if status >= TxnStatus::Committed {
                return RecoveryResult::Recovered {
                    txn_id,
                    deps: resp.deps,
                    execute_at,
                    events_data,
                    keys,
                };
            }
        }
    }

    if success_count == 0 {
        RecoveryResult::Failed
    } else {
        RecoveryResult::NotFound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction};

    fn make_txn(time: u64) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
            deps: vec![],
            status: TxnStatus::Committed,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_handle_known_txn() {
        let state = ConsensusState::new();
        let txn = make_txn(100);
        let id = txn.id;

        state
            .create_from_commit(
                txn.id,
                txn.deps.clone(),
                txn.execute_at,
                txn.events_data.clone(),
                txn.keys.clone(),
            )
            .await
            .unwrap();

        let req = RecoverRequest {
            txn_id: id,
            ballot: Ballot::initial(1),
        };

        let response = handle(&state, req).await.unwrap();

        if let Message::RecoverReply(resp) = response {
            assert_eq!(resp.txn_id, id);
            assert_eq!(resp.status, Some(TxnStatus::Committed));
            assert!(resp.events_data.is_some());
        } else {
            panic!("Expected RecoverReply");
        }
    }

    #[tokio::test]
    async fn test_handle_unknown_txn() {
        let state = ConsensusState::new();

        let id = TxnId::new(Timestamp::new(100, 0, 1));
        let req = RecoverRequest {
            txn_id: id,
            ballot: Ballot::initial(1),
        };

        let response = handle(&state, req).await.unwrap();

        if let Message::RecoverReply(resp) = response {
            assert_eq!(resp.txn_id, id);
            assert!(resp.status.is_none());
        } else {
            panic!("Expected RecoverReply");
        }
    }

    #[test]
    fn test_coordinate_recovered() {
        let txn = make_txn(100);
        let id = txn.id;

        let responses = vec![
            Ok(Message::RecoverReply(RecoverResponse {
                txn_id: id,
                status: Some(TxnStatus::Committed),
                deps: vec![],
                execute_at: Some(txn.execute_at),
                ballot: txn.ballot,
                events_data: Some(txn.events_data.clone()),
                keys: Some(txn.keys.clone()),
            })),
            Ok(Message::RecoverReply(RecoverResponse {
                txn_id: id,
                status: None,
                deps: vec![],
                execute_at: None,
                ballot: Ballot::default(),
                events_data: None,
                keys: None,
            })),
        ];

        let result = futures::executor::block_on(coordinate(responses, id));

        assert!(matches!(result, RecoveryResult::Recovered { .. }));
    }

    #[test]
    fn test_coordinate_not_found() {
        let id = TxnId::new(Timestamp::new(100, 0, 1));

        let responses = vec![
            Ok(Message::RecoverReply(RecoverResponse {
                txn_id: id,
                status: None,
                deps: vec![],
                execute_at: None,
                ballot: Ballot::default(),
                events_data: None,
                keys: None,
            })),
            Ok(Message::RecoverReply(RecoverResponse {
                txn_id: id,
                status: None,
                deps: vec![],
                execute_at: None,
                ballot: Ballot::default(),
                events_data: None,
                keys: None,
            })),
        ];

        let result = futures::executor::block_on(coordinate(responses, id));

        assert!(matches!(result, RecoveryResult::NotFound));
    }
}
