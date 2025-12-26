//! Accept phase of the ACCORD protocol.
//!
//! This is Phase 2 (slow path only) where the coordinator finalizes
//! dependencies when replicas disagreed during PreAccept.

use crate::error::{AccordError, Result};
use crate::protocol::messages::{AcceptNack, AcceptRequest, AcceptResponse, Message};
use crate::state::ConsensusState;
use crate::timestamp::Timestamp;
use crate::txn::{Ballot, TxnId};

/// Run Accept phase as coordinator.
///
/// This is only called when PreAccept resulted in SlowPath.
/// Returns the final execute_at timestamp.
pub async fn coordinate(
    state: &ConsensusState,
    txn_id: TxnId,
    deps: Vec<TxnId>,
    ballot: Ballot,
    responses: Vec<Result<Message>>,
    quorum_size: usize,
) -> Result<Timestamp> {
    // Compute execute_at as max of deps' execute_at + 1
    let execute_at = compute_execute_at(state, &deps).await;

    // Update local state
    state
        .accept(txn_id, deps.clone(), execute_at, ballot)
        .await?;

    // Count successful responses
    let mut ok_count = 1; // Include local

    for response in responses {
        match response {
            Ok(Message::AcceptOk(resp)) => {
                if resp.txn_id == txn_id && resp.ballot == ballot {
                    ok_count += 1;
                }
            }
            Ok(Message::AcceptNack(nack)) => {
                if nack.txn_id == txn_id {
                    return Err(AccordError::BallotTooLow {
                        attempted: ballot,
                        existing: nack.higher_ballot,
                    });
                }
            }
            Err(e) => {
                tracing::warn!("Accept response error: {}", e);
            }
            _ => {
                tracing::warn!("Unexpected message type in Accept");
            }
        }
    }

    // Check quorum
    if ok_count < quorum_size {
        return Err(AccordError::QuorumNotReached {
            got: ok_count,
            need: quorum_size,
        });
    }

    Ok(execute_at)
}

/// Handle an Accept request as a replica.
///
/// Returns the response message to send back.
pub async fn handle(state: &ConsensusState, request: AcceptRequest) -> Result<Message> {
    let txn_id = request.txn_id;
    let ballot = request.ballot;

    // Check if we have this transaction with a higher ballot
    if let Some(existing) = state.get(&txn_id).await {
        if existing.ballot > ballot {
            return Ok(Message::AcceptNack(AcceptNack {
                txn_id,
                higher_ballot: existing.ballot,
            }));
        }
    }

    // Update our state
    state
        .accept(txn_id, request.deps, request.execute_at, ballot)
        .await?;

    Ok(Message::AcceptOk(AcceptResponse { txn_id, ballot }))
}

/// Create an Accept request message.
pub fn create_request(
    txn_id: TxnId,
    deps: Vec<TxnId>,
    execute_at: Timestamp,
    ballot: Ballot,
) -> Message {
    Message::Accept(AcceptRequest {
        txn_id,
        deps,
        execute_at,
        ballot,
    })
}

/// Compute the execute_at timestamp based on dependencies.
///
/// The timestamp is the max of all deps' execute_at, incremented.
async fn compute_execute_at(state: &ConsensusState, deps: &[TxnId]) -> Timestamp {
    let mut max = Timestamp::ZERO;

    for dep_id in deps {
        if let Some(txn) = state.get(dep_id).await {
            if txn.execute_at > max {
                max = txn.execute_at;
            }
        }
    }

    max.increment()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::{Transaction, TxnStatus};

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
    async fn test_handle_accept() {
        let state = ConsensusState::new();

        // First PreAccept the transaction
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();

        // Then Accept
        let request = AcceptRequest {
            txn_id: txn.id,
            deps: vec![],
            execute_at: Timestamp::new(100, 0, 1),
            ballot: Ballot::new(1, 1),
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::AcceptOk(resp) => {
                assert_eq!(resp.txn_id, txn.id);
            }
            _ => panic!("Expected AcceptOk"),
        }

        // Verify state was updated
        let updated = state.get(&txn.id).await.unwrap();
        assert_eq!(updated.status, TxnStatus::Accepted);
    }

    #[tokio::test]
    async fn test_handle_accept_nack_higher_ballot() {
        let state = ConsensusState::new();

        // PreAccept with high ballot
        let mut txn = make_txn(100, &["key1"]);
        txn.ballot = Ballot::new(10, 1);
        state.preaccept(txn.clone()).await.unwrap();

        // Accept with lower ballot
        let request = AcceptRequest {
            txn_id: txn.id,
            deps: vec![],
            execute_at: Timestamp::new(100, 0, 1),
            ballot: Ballot::new(1, 1),
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::AcceptNack(nack) => {
                assert_eq!(nack.txn_id, txn.id);
                assert_eq!(nack.higher_ballot, Ballot::new(10, 1));
            }
            _ => panic!("Expected AcceptNack"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_accept() {
        let state = ConsensusState::new();

        // Setup: PreAccept a dependency
        let dep_txn = make_txn(50, &["key1"]);
        state.preaccept(dep_txn.clone()).await.unwrap();

        // PreAccept the main transaction
        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();

        let ballot = Ballot::new(1, 1);
        let deps = vec![dep_txn.id];

        // Simulate responses
        let responses = vec![
            Ok(Message::AcceptOk(AcceptResponse {
                txn_id: txn.id,
                ballot,
            })),
            Ok(Message::AcceptOk(AcceptResponse {
                txn_id: txn.id,
                ballot,
            })),
        ];

        let execute_at = coordinate(&state, txn.id, deps, ballot, responses, 2)
            .await
            .unwrap();

        // execute_at should be after dep's execute_at
        assert!(execute_at > dep_txn.execute_at);
    }

    #[tokio::test]
    async fn test_coordinate_accept_quorum_not_reached() {
        let state = ConsensusState::new();

        let txn = make_txn(100, &["key1"]);
        state.preaccept(txn.clone()).await.unwrap();

        let ballot = Ballot::new(1, 1);

        // Only errors
        let responses = vec![
            Err(AccordError::NodeUnreachable(2)),
            Err(AccordError::NodeUnreachable(3)),
        ];

        let result = coordinate(&state, txn.id, vec![], ballot, responses, 2).await;

        assert!(matches!(
            result,
            Err(AccordError::QuorumNotReached { got: 1, need: 2 })
        ));
    }

    #[tokio::test]
    async fn test_compute_execute_at() {
        let state = ConsensusState::new();

        // Create some dependencies
        let mut dep1 = make_txn(50, &["key1"]);
        dep1.execute_at = Timestamp::new(100, 5, 1);
        state.preaccept(dep1.clone()).await.unwrap();

        let mut dep2 = make_txn(60, &["key2"]);
        dep2.execute_at = Timestamp::new(200, 0, 1);
        state.preaccept(dep2.clone()).await.unwrap();

        let deps = vec![dep1.id, dep2.id];
        let execute_at = compute_execute_at(&state, &deps).await;

        // Should be after the max (dep2's execute_at)
        assert!(execute_at > dep2.execute_at);
        assert_eq!(execute_at.time, 200);
        assert_eq!(execute_at.seq, 1); // Incremented
    }
}
