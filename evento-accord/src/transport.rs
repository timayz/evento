//! In-memory [`MessageSink`] for tests and the simulation harness.
//!
//! All replicas live in one process and exchange [`Message`]s over unbounded
//! channels. This is the deterministic transport twin of the production framed
//! TCP sink (M1); the protocol core cannot tell them apart.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::api::MessageSink;
use crate::clock::NodeId;
use crate::message::Message;

/// A message as delivered to a node's inbox, tagged with its origin so the
/// receiver can reply without a separate addressing scheme.
#[derive(Debug)]
pub struct Envelope {
    /// Sending node.
    pub from: NodeId,
    /// The delivered message.
    pub message: Message,
}

/// Shared in-memory network. Register one inbox per node, then hand each node a
/// [`InMemorySink`] via [`sink`](InMemoryNetwork::sink). A node can be
/// [`crash`](InMemoryNetwork::crash)ed to model a failure: all messages to or
/// from it are dropped until [`heal`](InMemoryNetwork::heal)ed.
#[derive(Default)]
pub struct InMemoryNetwork {
    inboxes: Mutex<HashMap<NodeId, mpsc::UnboundedSender<Envelope>>>,
    crashed: Mutex<HashSet<NodeId>>,
}

impl InMemoryNetwork {
    /// Creates an empty network.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers `node` and returns its inbox receiver. Call once per node
    /// before sending; re-registering replaces the previous inbox.
    pub fn register(&self, node: NodeId) -> mpsc::UnboundedReceiver<Envelope> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inboxes
            .lock()
            .expect("network poisoned")
            .insert(node, tx);
        rx
    }

    /// A sink that sends as `from`.
    pub fn sink(self: &Arc<Self>, from: NodeId) -> InMemorySink {
        InMemorySink {
            from,
            net: Arc::clone(self),
        }
    }

    /// Models `node` failing: messages to or from it are dropped.
    pub fn crash(&self, node: NodeId) {
        self.crashed.lock().expect("network poisoned").insert(node);
    }

    /// Restores a previously [`crash`](InMemoryNetwork::crash)ed node.
    pub fn heal(&self, node: NodeId) {
        self.crashed.lock().expect("network poisoned").remove(&node);
    }

    /// Whether `node` is currently crashed.
    fn is_crashed(&self, node: NodeId) -> bool {
        self.crashed
            .lock()
            .expect("network poisoned")
            .contains(&node)
    }
}

/// A per-node handle into an [`InMemoryNetwork`] that stamps outgoing messages
/// with its origin node.
pub struct InMemorySink {
    from: NodeId,
    net: Arc<InMemoryNetwork>,
}

#[async_trait]
impl MessageSink for InMemorySink {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        // A crashed node neither sends nor receives.
        if self.net.is_crashed(self.from) || self.net.is_crashed(to) {
            return Ok(());
        }

        let inbox = {
            let inboxes = self.net.inboxes.lock().expect("network poisoned");
            inboxes.get(&to).cloned()
        };

        match inbox {
            // A closed/missing inbox models a partitioned or down peer — the
            // protocol tolerates this, so it is not a local error.
            Some(tx) => {
                let _ = tx.send(Envelope {
                    from: self.from,
                    message,
                });
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Timestamp, TxnId};
    use crate::message::Message;

    fn txn(n: u64) -> TxnId {
        TxnId(Timestamp {
            micros: n,
            logical: 0,
            node: NodeId(n),
        })
    }

    #[tokio::test]
    async fn delivers_to_the_addressed_inbox() {
        let net = InMemoryNetwork::new();
        let mut inbox2 = net.register(NodeId(2));
        let sink1 = net.sink(NodeId(1));

        sink1
            .send(NodeId(2), Message::Applied { txn: txn(7) })
            .await
            .unwrap();

        let env = inbox2.recv().await.expect("a message");
        assert_eq!(env.from, NodeId(1));
        match env.message {
            Message::Applied { txn: got } => assert_eq!(got, txn(7)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sending_to_a_down_node_is_not_an_error() {
        let net = InMemoryNetwork::new();
        let sink = net.sink(NodeId(1));
        // No inbox registered for node 9.
        sink.send(NodeId(9), Message::Applied { txn: txn(1) })
            .await
            .unwrap();
    }
}
