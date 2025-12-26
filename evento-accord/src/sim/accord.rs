//! ACCORD protocol coordination for simulation.
//!
//! This module provides simulation-aware ACCORD protocol execution,
//! where messages flow through the virtual network with proper delays
//! and fault injection.
//!
//! **Note**: This module delegates to the real protocol code in `crate::protocol`
//! to ensure any changes to the protocol are automatically reflected in simulation.

use crate::error::Result;
use crate::protocol::messages::{AcceptResponse, PreAcceptResponse};
use crate::protocol::{self, Message};
use crate::state::ConsensusState;
use crate::timestamp::Timestamp;
use crate::transport::NodeId;
use crate::txn::{Ballot, SerializableEvent, Transaction, TxnId, TxnStatus};
use evento_core::Event;
use std::collections::HashMap;

/// Result of running the ACCORD protocol in simulation.
#[derive(Clone, Debug)]
pub struct SimProtocolResult {
    pub txn_id: TxnId,
    pub deps: Vec<TxnId>,
    pub execute_at: Timestamp,
    pub fast_path: bool,
    pub committed: bool,
}

/// Pending response from a protocol message.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct PendingResponse {
    pub from: NodeId,
    pub to: NodeId,
    pub request_type: String,
}

/// ACCORD coordinator for simulation.
///
/// Unlike the async AccordExecutor, this coordinator works synchronously
/// with the simulation loop, queuing messages through the virtual network.
pub struct SimCoordinator {
    /// Node ID of this coordinator.
    pub node_id: NodeId,
    /// All node IDs in the cluster.
    pub cluster: Vec<NodeId>,
    /// Pending responses we're waiting for.
    pending_responses: HashMap<TxnId, Vec<PendingResponse>>,
    /// Collected PreAccept responses.
    preaccept_responses: HashMap<TxnId, Vec<(NodeId, PreAcceptResponse)>>,
    /// Collected Accept responses.
    accept_responses: HashMap<TxnId, Vec<(NodeId, AcceptResponse)>>,
}

impl SimCoordinator {
    /// Create a new coordinator for a node.
    pub fn new(node_id: NodeId, cluster: Vec<NodeId>) -> Self {
        Self {
            node_id,
            cluster,
            pending_responses: HashMap::new(),
            preaccept_responses: HashMap::new(),
            accept_responses: HashMap::new(),
        }
    }

    /// Get peers (all nodes except self).
    pub fn peers(&self) -> Vec<NodeId> {
        self.cluster
            .iter()
            .filter(|&&id| id != self.node_id)
            .copied()
            .collect()
    }

    /// Quorum size for consensus.
    pub fn quorum_size(&self) -> usize {
        self.cluster.len() / 2 + 1
    }

    /// Start a new transaction and return messages to broadcast.
    ///
    /// Uses `protocol::preaccept::create_request` to construct the message.
    ///
    /// Returns (Transaction, Vec<(to_node, message)>)
    pub fn start_transaction(
        &mut self,
        state: &ConsensusState,
        events: Vec<Event>,
        timestamp: Timestamp,
    ) -> Result<(Transaction, Vec<(NodeId, Message)>)> {
        // Extract keys for conflict detection
        let keys: Vec<String> = events
            .iter()
            .map(|e| format!("{}:{}", e.aggregator_type, e.aggregator_id))
            .collect();

        // Serialize events using SerializableEvent wrapper
        let events_data = SerializableEvent::encode_events(&events);

        // Create transaction with serialized events
        let txn_id = TxnId::new(timestamp);
        let txn = Transaction {
            id: txn_id,
            events_data,
            keys,
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: timestamp,
            ballot: Ballot::initial(self.node_id),
        };

        // PreAccept locally
        futures::executor::block_on(state.preaccept(txn.clone()))?;

        // Use real protocol code to create the request message
        let message = protocol::preaccept::create_request(txn.clone(), txn.ballot);

        let messages: Vec<_> = self
            .peers()
            .into_iter()
            .map(|peer| (peer, message.clone()))
            .collect();

        // Track pending responses
        let pending: Vec<_> = self
            .peers()
            .into_iter()
            .map(|peer| PendingResponse {
                from: peer,
                to: self.node_id,
                request_type: "PreAccept".to_string(),
            })
            .collect();
        self.pending_responses.insert(txn.id, pending);
        self.preaccept_responses.insert(txn.id, vec![]);

        Ok((txn, messages))
    }

    /// Handle a response message for a transaction.
    ///
    /// Returns true if we have enough responses to proceed.
    pub fn handle_response(&mut self, from: NodeId, msg: Message) -> Option<TxnId> {
        match msg {
            Message::PreAcceptOk(resp) => {
                if let Some(responses) = self.preaccept_responses.get_mut(&resp.txn_id) {
                    responses.push((from, resp.clone()));
                    // Remove from pending
                    if let Some(pending) = self.pending_responses.get_mut(&resp.txn_id) {
                        pending.retain(|p| p.from != from);
                    }
                    // Check if we have quorum (excluding self, so quorum - 1)
                    if responses.len() >= self.quorum_size() - 1 {
                        return Some(resp.txn_id);
                    }
                }
            }
            Message::AcceptOk(resp) => {
                if let Some(responses) = self.accept_responses.get_mut(&resp.txn_id) {
                    responses.push((from, resp.clone()));
                    if let Some(pending) = self.pending_responses.get_mut(&resp.txn_id) {
                        pending.retain(|p| p.from != from);
                    }
                    if responses.len() >= self.quorum_size() - 1 {
                        return Some(resp.txn_id);
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Check if a transaction can take the fast path.
    ///
    /// Delegates to `protocol::preaccept::analyze_responses` to use the real
    /// protocol logic, ensuring simulation behavior matches production.
    ///
    /// Returns (can_fast_path, merged_deps)
    pub fn check_fast_path(&self, txn_id: &TxnId, local_deps: &[TxnId]) -> (bool, Vec<TxnId>) {
        let responses = match self.preaccept_responses.get(txn_id) {
            Some(r) => r,
            None => return (false, local_deps.to_vec()),
        };

        // Use real protocol code for fast path analysis
        protocol::preaccept::analyze_responses(local_deps, responses)
    }

    /// Create Accept messages for slow path.
    ///
    /// Uses `protocol::accept::create_request` to construct the message.
    pub fn create_accept_messages(
        &mut self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        ballot: Ballot,
    ) -> Vec<(NodeId, Message)> {
        // Use real protocol code to create the request message
        let message = protocol::accept::create_request(txn_id, deps, execute_at, ballot);

        self.accept_responses.insert(txn_id, vec![]);

        let pending: Vec<_> = self
            .peers()
            .into_iter()
            .map(|peer| PendingResponse {
                from: peer,
                to: self.node_id,
                request_type: "Accept".to_string(),
            })
            .collect();
        self.pending_responses.insert(txn_id, pending);

        self.peers()
            .into_iter()
            .map(|peer| (peer, message.clone()))
            .collect()
    }

    /// Create Commit messages.
    ///
    /// Uses `protocol::commit::create_request` to construct the message.
    pub fn create_commit_messages(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    ) -> Vec<(NodeId, Message)> {
        // Use real protocol code to create the request message
        let message = protocol::commit::create_request(txn_id, deps, execute_at, events_data, keys);

        self.peers()
            .into_iter()
            .map(|peer| (peer, message.clone()))
            .collect()
    }

    /// Clear state for a completed transaction.
    pub fn complete_transaction(&mut self, txn_id: &TxnId) {
        self.pending_responses.remove(txn_id);
        self.preaccept_responses.remove(txn_id);
        self.accept_responses.remove(txn_id);
    }

    /// Check if we're waiting for responses for a transaction.
    pub fn is_pending(&self, txn_id: &TxnId) -> bool {
        self.pending_responses
            .get(txn_id)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of PreAccept responses collected.
    pub fn preaccept_response_count(&self, txn_id: &TxnId) -> usize {
        self.preaccept_responses
            .get(txn_id)
            .map(|r| r.len())
            .unwrap_or(0)
    }
}

/// Transaction state being coordinated in simulation.
#[derive(Clone, Debug)]
pub enum TxnCoordinationState {
    /// Waiting for PreAccept responses.
    PreAccepting {
        txn: Transaction,
        local_deps: Vec<TxnId>,
    },
    /// Waiting for Accept responses (slow path).
    Accepting {
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
        events_data: Vec<u8>,
        keys: Vec<String>,
    },
    /// Committed, waiting for execution.
    Committed { txn_id: TxnId },
    /// Fully complete.
    Done { result: SimProtocolResult },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;
    use crate::txn::Ballot;

    fn make_preaccept_response(txn_id: TxnId, deps: Vec<TxnId>) -> Message {
        Message::PreAcceptOk(PreAcceptResponse {
            txn_id,
            deps,
            ballot: Ballot::initial(1),
        })
    }

    #[test]
    fn test_coordinator_peers() {
        let coord = SimCoordinator::new(0, vec![0, 1, 2]);
        assert_eq!(coord.peers(), vec![1, 2]);
        assert_eq!(coord.quorum_size(), 2);
    }

    #[test]
    fn test_fast_path_check() {
        let mut coord = SimCoordinator::new(0, vec![0, 1, 2]);
        let txn_id = TxnId::new(Timestamp::new(100, 0, 0));

        // Initialize empty responses
        coord.preaccept_responses.insert(txn_id, vec![]);

        // Add matching responses
        let local_deps = vec![];
        coord.preaccept_responses.get_mut(&txn_id).unwrap().push((
            1,
            PreAcceptResponse {
                txn_id,
                deps: vec![],
                ballot: Ballot::initial(1),
            },
        ));

        let (fast_path, merged) = coord.check_fast_path(&txn_id, &local_deps);
        assert!(fast_path);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_slow_path_check() {
        let mut coord = SimCoordinator::new(0, vec![0, 1, 2]);
        let txn_id = TxnId::new(Timestamp::new(100, 0, 0));
        let dep_id = TxnId::new(Timestamp::new(50, 0, 0));

        coord.preaccept_responses.insert(txn_id, vec![]);

        // Add conflicting response
        let local_deps = vec![];
        coord.preaccept_responses.get_mut(&txn_id).unwrap().push((
            1,
            PreAcceptResponse {
                txn_id,
                deps: vec![dep_id], // Different from local!
                ballot: Ballot::initial(1),
            },
        ));

        let (fast_path, merged) = coord.check_fast_path(&txn_id, &local_deps);
        assert!(!fast_path);
        assert_eq!(merged, vec![dep_id]);
    }

    #[test]
    fn test_handle_response_quorum() {
        // 5 nodes: quorum = 3, need 2 peer responses (coordinator counts as 1)
        let mut coord = SimCoordinator::new(0, vec![0, 1, 2, 3, 4]);
        let txn_id = TxnId::new(Timestamp::new(100, 0, 0));

        // Setup pending
        coord.preaccept_responses.insert(txn_id, vec![]);
        coord.pending_responses.insert(
            txn_id,
            vec![
                PendingResponse {
                    from: 1,
                    to: 0,
                    request_type: "PreAccept".to_string(),
                },
                PendingResponse {
                    from: 2,
                    to: 0,
                    request_type: "PreAccept".to_string(),
                },
                PendingResponse {
                    from: 3,
                    to: 0,
                    request_type: "PreAccept".to_string(),
                },
                PendingResponse {
                    from: 4,
                    to: 0,
                    request_type: "PreAccept".to_string(),
                },
            ],
        );

        // First response - not enough (need 2 peer responses for quorum)
        let result = coord.handle_response(1, make_preaccept_response(txn_id, vec![]));
        assert!(result.is_none());

        // Second response - quorum reached (coordinator + 2 peers = 3)
        let result = coord.handle_response(2, make_preaccept_response(txn_id, vec![]));
        assert!(result.is_some());
        assert_eq!(result.unwrap(), txn_id);
    }
}
