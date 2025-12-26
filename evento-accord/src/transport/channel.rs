//! In-memory channel-based transport for testing.
//!
//! This transport uses Tokio channels instead of real network connections,
//! making it ideal for unit and integration tests.

use crate::error::{AccordError, Result};
use crate::protocol::Message;
use crate::transport::traits::{NodeId, Transport};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

/// Type alias for message with response channel.
type MessageWithReply = (Message, oneshot::Sender<Message>);
/// Type alias for sender map.
type SenderMap = Arc<RwLock<HashMap<NodeId, mpsc::Sender<MessageWithReply>>>>;
/// Type alias for receiver.
type MessageReceiver = Arc<RwLock<mpsc::Receiver<MessageWithReply>>>;

/// Channel transport for testing without real network I/O.
pub struct ChannelTransport {
    local_id: NodeId,
    peers: Vec<NodeId>,
    senders: SenderMap,
    receiver: MessageReceiver,
}

impl ChannelTransport {
    /// Create a cluster of connected in-memory transports.
    ///
    /// Returns one transport per node, all connected to each other.
    pub fn create_cluster(node_count: usize) -> Vec<Self> {
        let mut senders: HashMap<NodeId, mpsc::Sender<_>> = HashMap::new();
        let mut receivers: HashMap<NodeId, mpsc::Receiver<_>> = HashMap::new();

        // Create channels for each node
        for i in 0..node_count {
            let (tx, rx) = mpsc::channel(1024);
            senders.insert(i as NodeId, tx);
            receivers.insert(i as NodeId, rx);
        }

        let senders = Arc::new(RwLock::new(senders));

        // Create transports
        (0..node_count)
            .map(|i| {
                let id = i as NodeId;
                let peers: Vec<_> = (0..node_count)
                    .filter(|&j| j != i)
                    .map(|j| j as NodeId)
                    .collect();

                Self {
                    local_id: id,
                    peers,
                    senders: senders.clone(),
                    receiver: Arc::new(RwLock::new(receivers.remove(&id).unwrap())),
                }
            })
            .collect()
    }

    /// Receive the next incoming message.
    ///
    /// Returns the message and a channel to send the response.
    pub async fn recv(&self) -> Option<(Message, oneshot::Sender<Message>)> {
        self.receiver.write().await.recv().await
    }

    /// Try to receive a message without blocking.
    pub fn try_recv(&self) -> Option<(Message, oneshot::Sender<Message>)> {
        self.receiver.try_write().ok()?.try_recv().ok()
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message> {
        let senders = self.senders.read().await;
        let sender = senders
            .get(&node)
            .ok_or(AccordError::NodeUnreachable(node))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send((msg.clone(), reply_tx))
            .await
            .map_err(|_| AccordError::NodeUnreachable(node))?;

        reply_rx
            .await
            .map_err(|_| AccordError::NodeUnreachable(node))
    }

    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>> {
        let futures: Vec<_> = self
            .peers
            .iter()
            .map(|&node| self.send(node, msg))
            .collect();

        futures::future::join_all(futures).await
    }

    fn broadcast_no_wait(&self, msg: Message) {
        let senders = self.senders.clone();
        let peers = self.peers.clone();

        tokio::spawn(async move {
            let senders = senders.read().await;
            for node in peers {
                if let Some(sender) = senders.get(&node) {
                    let (reply_tx, _) = oneshot::channel();
                    let _ = sender.send((msg.clone(), reply_tx)).await;
                }
            }
        });
    }

    fn local_node(&self) -> NodeId {
        self.local_id
    }

    fn peers(&self) -> Vec<NodeId> {
        self.peers.clone()
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{PreAcceptRequest, PreAcceptResponse};
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};

    fn make_test_txn(time: u64) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
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
    fn test_create_cluster() {
        let transports = ChannelTransport::create_cluster(3);

        assert_eq!(transports.len(), 3);

        // Node 0
        assert_eq!(transports[0].local_node(), 0);
        assert_eq!(transports[0].peers(), vec![1, 2]);
        assert_eq!(transports[0].cluster_size(), 3);
        assert_eq!(transports[0].quorum_size(), 2);

        // Node 1
        assert_eq!(transports[1].local_node(), 1);
        assert_eq!(transports[1].peers(), vec![0, 2]);

        // Node 2
        assert_eq!(transports[2].local_node(), 2);
        assert_eq!(transports[2].peers(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test_send_and_respond() {
        let mut transports = ChannelTransport::create_cluster(2);
        let transport1 = transports.pop().unwrap();
        let transport0 = transports.pop().unwrap();

        let txn = make_test_txn(100);
        let request = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });

        // Spawn handler for node 1
        let handle = tokio::spawn(async move {
            let (msg, reply_tx) = transport1.recv().await.unwrap();
            match msg {
                Message::PreAccept(req) => {
                    let response = Message::PreAcceptOk(PreAcceptResponse {
                        txn_id: req.txn.id,
                        deps: vec![],
                        ballot: req.ballot,
                    });
                    reply_tx.send(response).unwrap();
                }
                _ => panic!("Expected PreAccept"),
            }
        });

        // Send from node 0 to node 1
        let response = transport0.send(1, &request).await.unwrap();

        handle.await.unwrap();

        match response {
            Message::PreAcceptOk(resp) => {
                assert_eq!(resp.txn_id, txn.id);
                assert!(resp.deps.is_empty());
            }
            _ => panic!("Expected PreAcceptOk"),
        }
    }

    #[tokio::test]
    async fn test_broadcast() {
        let mut transports = ChannelTransport::create_cluster(3);
        let transport2 = transports.pop().unwrap();
        let transport1 = transports.pop().unwrap();
        let transport0 = transports.pop().unwrap();

        let txn = make_test_txn(100);
        let request = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });

        // Spawn handlers for nodes 1 and 2
        let handle1 = tokio::spawn(async move {
            let (msg, reply_tx) = transport1.recv().await.unwrap();
            if let Message::PreAccept(req) = msg {
                let response = Message::PreAcceptOk(PreAcceptResponse {
                    txn_id: req.txn.id,
                    deps: vec![],
                    ballot: req.ballot,
                });
                reply_tx.send(response).unwrap();
            }
        });

        let handle2 = tokio::spawn(async move {
            let (msg, reply_tx) = transport2.recv().await.unwrap();
            if let Message::PreAccept(req) = msg {
                let response = Message::PreAcceptOk(PreAcceptResponse {
                    txn_id: req.txn.id,
                    deps: vec![],
                    ballot: req.ballot,
                });
                reply_tx.send(response).unwrap();
            }
        });

        // Broadcast from node 0
        let responses = transport0.broadcast(&request).await;

        handle1.await.unwrap();
        handle2.await.unwrap();

        assert_eq!(responses.len(), 2);
        assert!(responses.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_send_to_unknown_node() {
        let transports = ChannelTransport::create_cluster(2);
        let transport0 = &transports[0];

        let txn = make_test_txn(100);
        let msg = Message::PreAccept(PreAcceptRequest {
            txn,
            ballot: Ballot::initial(1),
        });

        // Try to send to non-existent node 99
        let result = transport0.send(99, &msg).await;
        assert!(matches!(result, Err(AccordError::NodeUnreachable(99))));
    }
}
