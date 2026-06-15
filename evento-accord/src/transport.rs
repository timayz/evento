//! In-memory [`MessageSink`] for tests and the simulation harness.
//!
//! All replicas live in one process and exchange [`Message`]s over bounded
//! channels (capacity [`DEFAULT_INBOX_CAPACITY`]; a full inbox sheds, modelling
//! backpressure). This is the deterministic transport twin of the production
//! framed TCP sink (M1); the protocol core cannot tell them apart.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Inbox channel capacity — the backpressure bound. A node that falls far enough
/// behind that its inbox fills sheds further inbound messages (loss is tolerated
/// by quorums and recovery), so memory stays bounded under a flood or a slow
/// consumer instead of growing without limit.
pub const DEFAULT_INBOX_CAPACITY: usize = 1024;

/// Shared in-memory network. Register one inbox per node, then hand each node a
/// [`InMemorySink`] via [`sink`](InMemoryNetwork::sink). A node can be
/// [`crash`](InMemoryNetwork::crash)ed to model a failure: all messages to or
/// from it are dropped until [`heal`](InMemoryNetwork::heal)ed.
#[derive(Default)]
pub struct InMemoryNetwork {
    inboxes: Mutex<HashMap<NodeId, mpsc::Sender<Envelope>>>,
    crashed: Mutex<HashSet<NodeId>>,
    /// Unordered node pairs that cannot exchange messages (a partition).
    partitions: Mutex<HashSet<(NodeId, NodeId)>>,
    /// Messages shed because the destination inbox was full (backpressure).
    /// Observe-only — it never influences delivery, so the harness stays
    /// deterministic.
    shed: AtomicU64,
}

impl InMemoryNetwork {
    /// Creates an empty network.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers `node` and returns its (bounded) inbox receiver. Call once per
    /// node before sending; re-registering replaces the previous inbox.
    pub fn register(&self, node: NodeId) -> mpsc::Receiver<Envelope> {
        let (tx, rx) = mpsc::channel(DEFAULT_INBOX_CAPACITY);
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

    /// Severs the link between `a` and `b` in both directions (a partition).
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut partitions = self.partitions.lock().expect("network poisoned");
        partitions.insert((a, b));
        partitions.insert((b, a));
    }

    /// Restores a previously [`partition`](InMemoryNetwork::partition)ed link
    /// between `a` and `b` in both directions.
    pub fn heal_partition(&self, a: NodeId, b: NodeId) {
        let mut partitions = self.partitions.lock().expect("network poisoned");
        partitions.remove(&(a, b));
        partitions.remove(&(b, a));
    }

    /// Restores every partitioned link at once. Convenient for scenario
    /// teardown, where a seed-skipped heal must not leave a residual cut.
    pub fn heal_all_partitions(&self) {
        self.partitions.lock().expect("network poisoned").clear();
    }

    /// Whether messages between `from` and `to` are currently partitioned.
    fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions
            .lock()
            .expect("network poisoned")
            .contains(&(from, to))
    }

    /// How many messages have been shed for a full inbox (backpressure) so far.
    /// For observability/tests; the in-memory twin of `Metrics::messages_shed`.
    pub fn shed_count(&self) -> u64 {
        self.shed.load(Ordering::Relaxed)
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
        // A crashed node neither sends nor receives; a partition drops the link.
        if self.net.is_crashed(self.from)
            || self.net.is_crashed(to)
            || self.net.is_partitioned(self.from, to)
        {
            return Ok(());
        }

        let inbox = {
            let inboxes = self.net.inboxes.lock().expect("network poisoned");
            inboxes.get(&to).cloned()
        };

        match inbox {
            // A closed/missing inbox models a partitioned or down peer, and a
            // full inbox is backpressure shedding — both are tolerated loss, not a
            // local error. `try_send` never blocks the sender.
            Some(tx) => {
                if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(Envelope {
                    from: self.from,
                    message,
                }) {
                    // A full inbox is backpressure shedding — count it (a *closed*
                    // inbox is a down/partitioned peer, not backpressure, so it
                    // isn't counted). The drop itself is unchanged; observe-only.
                    self.net.shed.fetch_add(1, Ordering::Relaxed);
                }
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

    #[tokio::test]
    async fn a_partition_drops_the_link_and_healing_restores_it() {
        let net = InMemoryNetwork::new();
        let mut inbox2 = net.register(NodeId(2));
        let sink1 = net.sink(NodeId(1));

        // Cut the 1<->2 link: messages are dropped in both directions.
        net.partition(NodeId(1), NodeId(2));
        sink1
            .send(NodeId(2), Message::Applied { txn: txn(1) })
            .await
            .unwrap();
        assert!(
            inbox2.try_recv().is_err(),
            "a partitioned link must drop the message"
        );

        // Heal the link: delivery resumes.
        net.heal_partition(NodeId(1), NodeId(2));
        sink1
            .send(NodeId(2), Message::Applied { txn: txn(2) })
            .await
            .unwrap();
        let env = inbox2.recv().await.expect("a message after healing");
        match env.message {
            Message::Applied { txn: got } => assert_eq!(got, txn(2)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_full_inbox_sheds_messages_and_never_blocks() {
        let net = InMemoryNetwork::new();
        let mut inbox = net.register(NodeId(2));
        let sink1 = net.sink(NodeId(1));

        // Flood far past capacity while the consumer never drains.
        let flood = DEFAULT_INBOX_CAPACITY + 500;
        for i in 0..flood {
            // `send` never blocks despite the full inbox (it sheds).
            sink1
                .send(NodeId(2), Message::Applied { txn: txn(i as u64) })
                .await
                .unwrap();
        }

        // The inbox buffered only up to its capacity; the rest were shed.
        let mut received = 0;
        while inbox.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(
            received, DEFAULT_INBOX_CAPACITY,
            "the inbox is bounded to its capacity"
        );
        // And every shed message was counted (observe-only backpressure metric).
        assert_eq!(
            net.shed_count() as usize,
            flood - DEFAULT_INBOX_CAPACITY,
            "the shed counter tracks exactly the dropped overflow"
        );
    }

    #[tokio::test]
    async fn heal_all_partitions_restores_every_link() {
        let net = InMemoryNetwork::new();
        let mut inbox2 = net.register(NodeId(2));
        let sink1 = net.sink(NodeId(1));

        net.partition(NodeId(1), NodeId(2));
        net.heal_all_partitions();

        sink1
            .send(NodeId(2), Message::Applied { txn: txn(3) })
            .await
            .unwrap();
        let env = inbox2.recv().await.expect("a message after heal_all");
        match env.message {
            Message::Applied { txn: got } => assert_eq!(got, txn(3)),
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
