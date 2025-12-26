//! ACCORD protocol implementation.
//!
//! This module implements the four phases of ACCORD:
//! 1. PreAccept - Propose transaction and compute dependencies
//! 2. Accept - Finalize dependencies (slow path only)
//! 3. Commit - Mark transaction as committed
//! 4. Execute - Apply transaction after dependencies satisfied

pub mod accept;
pub mod commit;
pub mod execute;
pub mod messages;
pub mod preaccept;
pub mod sync;

pub use execute::{BackgroundExecutor, ExecuteConfig};
pub use messages::Message;
pub use preaccept::PreAcceptResult;

use crate::error::{AccordError, Result};
use crate::metrics::{self, status, Timer};
use crate::state::ConsensusState;
use crate::timestamp::Timestamp;
use crate::txn::{Transaction, TxnId};

/// Result of running the full protocol for a transaction.
#[derive(Debug)]
pub struct ProtocolResult {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Final dependencies
    pub deps: Vec<TxnId>,
    /// Final execution timestamp
    pub execute_at: Timestamp,
    /// Whether fast path was taken
    pub fast_path: bool,
}

/// Run the full ACCORD protocol for a transaction (coordinator side).
///
/// This orchestrates all phases:
/// 1. PreAccept - proposes the transaction
/// 2. Accept (if needed) - finalizes dependencies
/// 3. Commit - marks as committed
///
/// After this returns, the transaction is committed and will be executed
/// once dependencies are satisfied.
///
/// # Arguments
/// * `state` - The consensus state
/// * `txn` - The transaction to commit
/// * `preaccept_responses` - Responses from PreAccept broadcast
/// * `accept_broadcast` - Function to broadcast Accept messages (returns responses)
/// * `commit_broadcast` - Function to broadcast Commit messages (fire and forget)
/// * `quorum_size` - Number of nodes needed for quorum
pub async fn run_protocol<F, G>(
    state: &ConsensusState,
    txn: Transaction,
    preaccept_responses: Vec<Result<Message>>,
    accept_broadcast: F,
    commit_broadcast: G,
    quorum_size: usize,
) -> Result<ProtocolResult>
where
    F: FnOnce(Message) -> Vec<Result<Message>>,
    G: FnOnce(Message),
{
    // Start timing the entire protocol
    let _protocol_timer = Timer::transaction("total");

    let txn_id = txn.id;
    let ballot = txn.ballot;
    let events_data = txn.events_data.clone();
    let keys = txn.keys.clone();

    // Phase 1: PreAccept
    metrics::record_transaction(status::PREACCEPTED);
    let preaccept_timer = Timer::round("preaccept");
    let preaccept_result =
        preaccept::coordinate(state, txn, preaccept_responses, quorum_size).await?;
    drop(preaccept_timer);

    let (deps, execute_at, fast_path) = match preaccept_result {
        PreAcceptResult::FastPath { deps } => {
            // Fast path: use original timestamp
            metrics::record_fast_path();
            let execute_at = state
                .get(&txn_id)
                .await
                .map(|t| t.execute_at)
                .unwrap_or(Timestamp::ZERO);
            (deps, execute_at, true)
        }
        PreAcceptResult::SlowPath { merged_deps } => {
            // Phase 2: Accept (slow path)
            metrics::record_slow_path();
            metrics::record_transaction(status::ACCEPTED);
            let accept_timer = Timer::round("accept");

            let accept_msg = accept::create_request(
                txn_id,
                merged_deps.clone(),
                Timestamp::ZERO, // Will be computed
                ballot,
            );
            let accept_responses = accept_broadcast(accept_msg);

            let execute_at = accept::coordinate(
                state,
                txn_id,
                merged_deps.clone(),
                ballot,
                accept_responses,
                quorum_size,
            )
            .await?;

            drop(accept_timer);
            (merged_deps, execute_at, false)
        }
    };

    // Phase 3: Commit
    let commit_timer = Timer::round("commit");
    metrics::record_transaction(status::COMMITTED);
    let commit_request =
        commit::coordinate(state, txn_id, deps.clone(), execute_at, events_data, keys).await?;
    drop(commit_timer);

    // Broadcast commit (fire and forget)
    commit_broadcast(Message::Commit(commit_request));

    Ok(ProtocolResult {
        txn_id,
        deps,
        execute_at,
        fast_path,
    })
}

/// Handle an incoming protocol message.
///
/// Dispatches to the appropriate phase handler.
pub async fn handle_message(state: &ConsensusState, msg: Message) -> Result<Message> {
    // Record incoming message
    let msg_type = msg.type_name();
    metrics::record_message(msg_type, "received");

    let result = match msg {
        Message::PreAccept(req) => preaccept::handle(state, req).await,
        Message::Accept(req) => accept::handle(state, req).await,
        Message::Commit(req) => commit::handle(state, req).await,
        Message::SyncRequest(req) => sync::handle(state, req).await,
        _ => Err(AccordError::Internal(anyhow::anyhow!(
            "Unexpected message type: {}",
            msg_type
        ))),
    };

    // Record response
    match &result {
        Ok(response) => {
            metrics::record_message(response.type_name(), "sent");
        }
        Err(e) => {
            let error_type = match e {
                AccordError::Timeout(_) => metrics::error_type::TIMEOUT,
                AccordError::QuorumNotReached { .. } => metrics::error_type::QUORUM_NOT_REACHED,
                AccordError::NodeUnreachable(_) => metrics::error_type::NODE_UNREACHABLE,
                AccordError::BallotTooLow { .. } => metrics::error_type::BALLOT_TOO_LOW,
                AccordError::Storage(_) => metrics::error_type::STORAGE,
                _ => metrics::error_type::INTERNAL,
            };
            metrics::record_error(error_type);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::{Ballot, TxnStatus};

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
    async fn test_run_protocol_fast_path() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;
        let ballot = txn.ballot;

        // Simulate PreAccept responses (all agree on empty deps)
        let preaccept_responses = vec![
            Ok(Message::PreAcceptOk(messages::PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            })),
            Ok(Message::PreAcceptOk(messages::PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            })),
        ];

        let result = run_protocol(
            &state,
            txn,
            preaccept_responses,
            |_| vec![], // Accept not called
            |_| {},     // Commit broadcast
            2,
        )
        .await
        .unwrap();

        assert!(result.fast_path);
        assert!(result.deps.is_empty());
        assert_eq!(state.get_status(&id).await, Some(TxnStatus::Committed));
    }

    #[tokio::test]
    async fn test_run_protocol_slow_path() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);
        let id = txn.id;
        let ballot = txn.ballot;

        let dep_id = TxnId::new(Timestamp::new(50, 0, 1));

        // Simulate PreAccept responses with different deps
        let preaccept_responses = vec![
            Ok(Message::PreAcceptOk(messages::PreAcceptResponse {
                txn_id: id,
                deps: vec![],
                ballot,
            })),
            Ok(Message::PreAcceptOk(messages::PreAcceptResponse {
                txn_id: id,
                deps: vec![dep_id], // Different!
                ballot,
            })),
        ];

        // Accept responses
        let accept_responses = vec![
            Ok(Message::AcceptOk(messages::AcceptResponse {
                txn_id: id,
                ballot,
            })),
            Ok(Message::AcceptOk(messages::AcceptResponse {
                txn_id: id,
                ballot,
            })),
        ];

        let result = run_protocol(
            &state,
            txn,
            preaccept_responses,
            |_| accept_responses,
            |_| {},
            2,
        )
        .await
        .unwrap();

        assert!(!result.fast_path);
        assert!(result.deps.contains(&dep_id));
        assert_eq!(state.get_status(&id).await, Some(TxnStatus::Committed));
    }

    #[tokio::test]
    async fn test_handle_message_preaccept() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        let msg = Message::PreAccept(messages::PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });

        let response = handle_message(&state, msg).await.unwrap();

        assert!(matches!(response, Message::PreAcceptOk(_)));
    }

    #[tokio::test]
    async fn test_handle_message_accept() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        // First PreAccept
        state.preaccept(txn.clone()).await.unwrap();

        // Then Accept
        let msg = Message::Accept(messages::AcceptRequest {
            txn_id: txn.id,
            deps: vec![],
            execute_at: txn.execute_at,
            ballot: Ballot::new(1, 1),
        });

        let response = handle_message(&state, msg).await.unwrap();

        assert!(matches!(response, Message::AcceptOk(_)));
    }

    #[tokio::test]
    async fn test_handle_message_commit() {
        let state = ConsensusState::new();
        let txn = make_txn(100, &["key1"]);

        // PreAccept first
        state.preaccept(txn.clone()).await.unwrap();

        // Then Commit
        let msg = Message::Commit(messages::CommitRequest {
            txn_id: txn.id,
            deps: vec![],
            execute_at: txn.execute_at,
            events_data: txn.events_data.clone(),
            keys: txn.keys.clone(),
        });

        let response = handle_message(&state, msg).await.unwrap();

        assert!(matches!(response, Message::CommitOk(_)));
        assert_eq!(state.get_status(&txn.id).await, Some(TxnStatus::Committed));
    }
}
