//! PreAccept phase of the ACCORD protocol.
//!
//! This is Phase 1 where a coordinator proposes a transaction and
//! replicas compute their dependencies. If all replicas agree on
//! dependencies, we take the fast path and skip Accept.

use crate::error::{AccordError, Result};
use crate::protocol::messages::{Message, PreAcceptNack, PreAcceptRequest, PreAcceptResponse};
use crate::state::ConsensusState;
use crate::txn::{Ballot, Transaction, TxnId};
use std::collections::BTreeSet;

/// Result of the PreAccept phase.
#[derive(Debug)]
pub enum PreAcceptResult {
    /// Fast path: all replicas agreed on dependencies
    FastPath { deps: Vec<TxnId> },
    /// Slow path: replicas disagreed, need Accept phase
    SlowPath { merged_deps: Vec<TxnId> },
}

/// Run PreAccept phase as coordinator.
///
/// Returns the result indicating whether we can take the fast path.
pub async fn coordinate(
    state: &ConsensusState,
    txn: Transaction,
    responses: Vec<Result<Message>>,
    quorum_size: usize,
) -> Result<PreAcceptResult> {
    let txn_id = txn.id;
    let ballot = txn.ballot;

    // Local PreAccept
    let local_deps = state.preaccept(txn).await?;

    // Collect responses
    let mut all_deps: Vec<Vec<TxnId>> = vec![local_deps];
    let mut ok_count = 1; // Include local

    for response in responses {
        match response {
            Ok(Message::PreAcceptOk(resp)) => {
                if resp.txn_id == txn_id && resp.ballot == ballot {
                    all_deps.push(resp.deps);
                    ok_count += 1;
                }
            }
            Ok(Message::PreAcceptNack(nack)) => {
                if nack.txn_id == txn_id {
                    return Err(AccordError::BallotTooLow {
                        attempted: ballot,
                        existing: nack.higher_ballot,
                    });
                }
            }
            Err(e) => {
                tracing::warn!("PreAccept response error: {}", e);
            }
            _ => {
                tracing::warn!("Unexpected message type in PreAccept");
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

    // Check if fast path (all deps agree)
    if all_deps_agree(&all_deps) {
        Ok(PreAcceptResult::FastPath {
            deps: all_deps.into_iter().next().unwrap_or_default(),
        })
    } else {
        Ok(PreAcceptResult::SlowPath {
            merged_deps: merge_deps(all_deps),
        })
    }
}

/// Handle a PreAccept request as a replica.
///
/// Returns the response message to send back.
pub async fn handle(state: &ConsensusState, request: PreAcceptRequest) -> Result<Message> {
    let txn_id = request.txn.id;
    let ballot = request.ballot;

    // Check if we already have this transaction with a higher ballot
    if let Some(existing) = state.get(&txn_id).await {
        if existing.ballot > ballot {
            return Ok(Message::PreAcceptNack(PreAcceptNack {
                txn_id,
                higher_ballot: existing.ballot,
            }));
        }
    }

    // Compute our local dependencies
    let deps = state.handle_preaccept(request.txn, ballot).await?;

    Ok(Message::PreAcceptOk(PreAcceptResponse {
        txn_id,
        deps,
        ballot,
    }))
}

/// Create a PreAccept request message.
pub fn create_request(txn: Transaction, ballot: Ballot) -> Message {
    Message::PreAccept(PreAcceptRequest { txn, ballot })
}

/// Check if all dependency sets are identical.
///
/// Used to determine if fast path can be taken (all replicas agree).
pub fn all_deps_agree(all_deps: &[Vec<TxnId>]) -> bool {
    if all_deps.is_empty() {
        return true;
    }

    let first = &all_deps[0];
    all_deps.iter().skip(1).all(|deps| deps == first)
}

/// Merge all dependency sets into a union.
///
/// Used when slow path is needed (replicas disagreed on deps).
pub fn merge_deps(all_deps: Vec<Vec<TxnId>>) -> Vec<TxnId> {
    let mut merged: BTreeSet<TxnId> = BTreeSet::new();
    for deps in all_deps {
        merged.extend(deps);
    }
    merged.into_iter().collect()
}

/// Analyze collected PreAccept responses to determine fast path eligibility.
///
/// This is useful when responses are collected incrementally (e.g., in simulation).
/// Returns `(can_fast_path, merged_deps)`.
pub fn analyze_responses(
    local_deps: &[TxnId],
    responses: &[(crate::transport::NodeId, PreAcceptResponse)],
) -> (bool, Vec<TxnId>) {
    // Collect all deps including local
    let mut all_deps: Vec<Vec<TxnId>> = vec![local_deps.to_vec()];
    for (_, resp) in responses {
        all_deps.push(resp.deps.clone());
    }

    let can_fast_path = all_deps_agree(&all_deps);
    let merged = merge_deps(all_deps);

    (can_fast_path, merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::TxnStatus;

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

    fn make_id(time: u64) -> TxnId {
        TxnId::new(Timestamp::new(time, 0, 1))
    }

    #[test]
    fn test_all_deps_agree_empty() {
        assert!(all_deps_agree(&[]));
    }

    #[test]
    fn test_all_deps_agree_single() {
        let deps = vec![vec![make_id(100)]];
        assert!(all_deps_agree(&deps));
    }

    #[test]
    fn test_all_deps_agree_matching() {
        let id = make_id(100);
        let deps = vec![vec![id], vec![id], vec![id]];
        assert!(all_deps_agree(&deps));
    }

    #[test]
    fn test_all_deps_agree_different() {
        let id1 = make_id(100);
        let id2 = make_id(101);
        let deps = vec![vec![id1], vec![id1, id2]];
        assert!(!all_deps_agree(&deps));
    }

    #[test]
    fn test_merge_deps() {
        let id1 = make_id(100);
        let id2 = make_id(101);
        let id3 = make_id(102);

        let all_deps = vec![vec![id1, id2], vec![id2, id3], vec![id1]];

        let merged = merge_deps(all_deps);
        assert_eq!(merged.len(), 3);
        assert!(merged.contains(&id1));
        assert!(merged.contains(&id2));
        assert!(merged.contains(&id3));
    }

    #[tokio::test]
    async fn test_handle_preaccept() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let ballot = Ballot::initial(1);

        let request = PreAcceptRequest {
            txn: txn.clone(),
            ballot,
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::PreAcceptOk(resp) => {
                assert_eq!(resp.txn_id, txn.id);
                assert_eq!(resp.ballot, ballot);
            }
            _ => panic!("Expected PreAcceptOk"),
        }
    }

    #[tokio::test]
    async fn test_handle_preaccept_computes_deps() {
        let state = ConsensusState::new();

        // First transaction
        let txn1 = make_txn(100, &["key1"]);
        state.preaccept(txn1.clone()).await.unwrap();

        // Second transaction with conflicting key
        let txn2 = make_txn(101, &["key1"]);
        let request = PreAcceptRequest {
            txn: txn2.clone(),
            ballot: Ballot::initial(1),
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::PreAcceptOk(resp) => {
                assert_eq!(resp.deps.len(), 1);
                assert_eq!(resp.deps[0], txn1.id);
            }
            _ => panic!("Expected PreAcceptOk"),
        }
    }

    #[tokio::test]
    async fn test_handle_preaccept_nack_higher_ballot() {
        let state = ConsensusState::new();

        // First PreAccept with higher ballot
        let mut txn = make_txn(100, &["key1"]);
        txn.ballot = Ballot::new(5, 1);
        state.preaccept(txn.clone()).await.unwrap();

        // Second PreAccept with lower ballot
        let mut txn2 = txn.clone();
        txn2.ballot = Ballot::new(1, 1);
        let request = PreAcceptRequest {
            txn: txn2,
            ballot: Ballot::new(1, 1),
        };

        let response = handle(&state, request).await.unwrap();

        match response {
            Message::PreAcceptNack(nack) => {
                assert_eq!(nack.txn_id, txn.id);
                assert_eq!(nack.higher_ballot, Ballot::new(5, 1));
            }
            _ => panic!("Expected PreAcceptNack"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_fast_path() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;
        let ballot = txn.ballot;

        // Simulate responses from 2 other nodes (all agree on empty deps)
        let responses = vec![
            Ok(Message::PreAcceptOk(PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            })),
            Ok(Message::PreAcceptOk(PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            })),
        ];

        let result = coordinate(&state, txn, responses, 2).await.unwrap();

        match result {
            PreAcceptResult::FastPath { deps } => {
                assert!(deps.is_empty());
            }
            _ => panic!("Expected FastPath"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_slow_path() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;
        let ballot = txn.ballot;

        let dep1 = make_id(50);
        let dep2 = make_id(60);

        // Simulate responses with different deps
        let responses = vec![
            Ok(Message::PreAcceptOk(PreAcceptResponse {
                txn_id: id,
                deps: vec![dep1],
                ballot,
            })),
            Ok(Message::PreAcceptOk(PreAcceptResponse {
                txn_id: id,
                deps: vec![dep1, dep2], // Different!
                ballot,
            })),
        ];

        let result = coordinate(&state, txn, responses, 2).await.unwrap();

        match result {
            PreAcceptResult::SlowPath { merged_deps } => {
                // Should have both deps
                assert!(merged_deps.contains(&dep1));
                assert!(merged_deps.contains(&dep2));
            }
            _ => panic!("Expected SlowPath"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_quorum_not_reached() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        // Only 1 successful response, but need 2
        let responses = vec![
            Err(AccordError::NodeUnreachable(2)),
            Err(AccordError::NodeUnreachable(3)),
        ];

        let result = coordinate(&state, txn, responses, 2).await;

        assert!(matches!(
            result,
            Err(AccordError::QuorumNotReached { got: 1, need: 2 })
        ));
    }

    #[test]
    fn test_analyze_responses_fast_path() {
        let local_deps = vec![make_id(50)];
        let responses = vec![
            (
                1,
                PreAcceptResponse {
                    txn_id: make_id(100),
                    deps: vec![make_id(50)], // Same as local
                    ballot: Ballot::initial(1),
                },
            ),
            (
                2,
                PreAcceptResponse {
                    txn_id: make_id(100),
                    deps: vec![make_id(50)], // Same as local
                    ballot: Ballot::initial(2),
                },
            ),
        ];

        let (can_fast_path, merged) = analyze_responses(&local_deps, &responses);
        assert!(can_fast_path);
        assert_eq!(merged.len(), 1);
        assert!(merged.contains(&make_id(50)));
    }

    #[test]
    fn test_analyze_responses_slow_path() {
        let local_deps = vec![];
        let responses = vec![
            (
                1,
                PreAcceptResponse {
                    txn_id: make_id(100),
                    deps: vec![make_id(50)], // Different from local
                    ballot: Ballot::initial(1),
                },
            ),
            (
                2,
                PreAcceptResponse {
                    txn_id: make_id(100),
                    deps: vec![make_id(60)], // Different from others
                    ballot: Ballot::initial(2),
                },
            ),
        ];

        let (can_fast_path, merged) = analyze_responses(&local_deps, &responses);
        assert!(!can_fast_path);
        // Should merge all deps
        assert!(merged.contains(&make_id(50)));
        assert!(merged.contains(&make_id(60)));
    }

    #[test]
    fn test_analyze_responses_empty() {
        let local_deps = vec![make_id(50)];
        let responses: Vec<(u16, PreAcceptResponse)> = vec![];

        let (can_fast_path, merged) = analyze_responses(&local_deps, &responses);
        // With only local deps, fast path is possible
        assert!(can_fast_path);
        assert_eq!(merged.len(), 1);
        assert!(merged.contains(&make_id(50)));
    }
}
