//! In-memory [`MessageSink`] for tests and the simulation harness.
//!
//! All replicas live in one process and exchange [`Message`]s over bounded
//! channels (capacity [`DEFAULT_INBOX_CAPACITY`]; a full inbox sheds, modelling
//! backpressure). This is the deterministic transport twin of the production
//! framed TCP sink (M1); the protocol core cannot tell them apart.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::api::{stable_hash_extend, MessageSink, STABLE_HASH_SEED};
use crate::clock::NodeId;
use crate::format::{encode_tagged, RecordKind};
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

/// Identifies one independent consensus group (one `Node` per host, one data store,
/// one journal) among the many a single process can host over a shared
/// multiplexed transport (`crate::tcp::MuxTransport`). Groups never share consensus
/// state: a `TxnId`, `Ballot`, or watermark is only ever compared within one group,
/// so two groups can safely reuse the same `NodeId`s on the same hosts. A natural
/// use is one group per tenant, each with its own database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GroupId(pub u64);

impl GroupId {
    /// The group a legacy single-group transport (`TcpTransport`, `serve`) speaks for.
    /// A multiplexed listener delivers un-grouped frames here, so a classic node and a
    /// multiplexed host interoperate as long as the classic node is in group zero.
    pub const DEFAULT: GroupId = GroupId(0);
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
    /// Per-directed-link delivery latency. **Empty by default**, so the common
    /// path delivers inline exactly as before (the deterministic / bit-reproducible
    /// simulation is byte-identical). A non-zero entry defers delivery by that
    /// duration — a latency model for validating the region-favouring fast-path
    /// electorate (a co-located coordinator commits before a remote replica could
    /// even reply). For tests only; the production TCP transport is untouched.
    latency: Mutex<HashMap<(NodeId, NodeId), Duration>>,
    /// A frame cap like the TCP transport's
    /// ([`MAX_FRAME_LENGTH`](crate::tcp::MAX_FRAME_LENGTH)): a message whose
    /// wire encoding is longer is shed, as `PeerLinks::enqueue` sheds it.
    /// `None` by default (nothing is encoded, so the simulation is unchanged);
    /// set a small one to exercise the protocol against the cap without
    /// building 64 MiB of state.
    frame_cap: Mutex<Option<usize>>,
    /// Messages a receiver cannot read (see
    /// [`set_unreadable`](InMemoryNetwork::set_unreadable)); `None` by default.
    unreadable: Mutex<Option<MessageFilter>>,
    /// The execution trace (see [`enable_trace`](InMemoryNetwork::enable_trace));
    /// `None` by default, so nothing is encoded.
    trace: Mutex<Option<Trace>>,
}

/// A predicate over messages, for [`InMemoryNetwork::set_unreadable`].
pub type MessageFilter = Arc<dyn Fn(&Message) -> bool + Send + Sync>;

/// What became of one message the network was handed, as its
/// [trace](InMemoryNetwork::enable_trace) records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFate {
    /// Put in the receiver's inbox.
    Delivered,
    /// Held back for the link's latency; its arrival is traced on its own.
    Deferred,
    /// Dropped: an end is crashed, the link partitioned, or the receiver has
    /// no open inbox.
    Dropped,
    /// Shed: over the frame cap, unreadable, or the receiver's inbox full.
    Shed,
}

/// One traced step, kept when the trace records its entries (see
/// [`InMemoryNetwork::enable_trace`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    /// Virtual microseconds since tracing began.
    pub micros: u64,
    /// Sending node.
    pub from: NodeId,
    /// Addressed node.
    pub to: NodeId,
    /// What became of the message.
    pub fate: MessageFate,
    /// The message's variant, for reading a trace.
    pub kind: String,
    /// A stable hash of the message's wire encoding.
    pub digest: u64,
}

/// The running trace of an [`InMemoryNetwork`].
struct Trace {
    /// When tracing began: entries are timed from here, so two runs traced
    /// back to back on one clock compare.
    start: tokio::time::Instant,
    /// Every step so far, folded into one stable hash.
    hash: u64,
    /// Every step so far, when recording.
    entries: Option<Vec<TraceEntry>>,
}

/// What a trace keeps of one message: its digest, and its variant when
/// recording. Computed once at send, so a deferred message is not encoded
/// again on arrival.
struct Stamp {
    digest: u64,
    kind: Option<String>,
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

    /// Sets the one-way delivery latency on the directed link `from → to`. Zero
    /// (the default) means inline delivery. Used by the latency model that
    /// validates the region-favouring fast-path electorate.
    pub fn set_latency(&self, from: NodeId, to: NodeId, latency: Duration) {
        self.latency
            .lock()
            .expect("network poisoned")
            .insert((from, to), latency);
    }

    /// Sets the delivery latency on the link between `a` and `b` in both
    /// directions. Convenience over two [`set_latency`](Self::set_latency) calls.
    pub fn set_link_latency(&self, a: NodeId, b: NodeId, latency: Duration) {
        self.set_latency(a, b, latency);
        self.set_latency(b, a, latency);
    }

    /// Sheds every message whose wire encoding is over `cap` bytes from now
    /// on (`None` lifts the cap) — the in-memory twin of the TCP transport's
    /// frame cap. A shed frame counts in [`shed_count`](Self::shed_count).
    pub fn set_frame_cap(&self, cap: Option<usize>) {
        *self.frame_cap.lock().expect("network poisoned") = cap;
    }

    /// Drops every message `filter` matches from now on (`None` delivers
    /// everything again) — a peer on an older build, which sheds a variant
    /// it cannot decode. For rolling-upgrade tests.
    pub fn set_unreadable(&self, filter: Option<MessageFilter>) {
        *self.unreadable.lock().expect("network poisoned") = filter;
    }

    /// Starts tracing this network's execution: from now on every message it
    /// is handed — its virtual send time, its ends, its wire encoding, and
    /// what became of it ([`MessageFate`]) — is folded into one stable hash,
    /// [`trace_hash`](Self::trace_hash). Under the deterministic simulation's
    /// paused clock, two runs of one seed must fold identical traces: two
    /// runs that end in the same state by different paths do not.
    ///
    /// Times are measured from this call, so two runs traced back to back on
    /// one virtual clock compare. With `record`, every step is also kept
    /// ([`trace`](Self::trace)), to find where two runs part. Restarts the
    /// trace if one was running. Off by default: nothing is encoded then.
    pub fn enable_trace(&self, record: bool) {
        *self.trace.lock().expect("network poisoned") = Some(Trace {
            start: tokio::time::Instant::now(),
            hash: STABLE_HASH_SEED,
            entries: record.then(Vec::new),
        });
    }

    /// The hash of every step traced since [`enable_trace`](Self::enable_trace);
    /// `None` if tracing is off.
    pub fn trace_hash(&self) -> Option<u64> {
        self.trace
            .lock()
            .expect("network poisoned")
            .as_ref()
            .map(|trace| trace.hash)
    }

    /// Every step traced since [`enable_trace`](Self::enable_trace) with
    /// `record` on; empty otherwise.
    pub fn trace(&self) -> Vec<TraceEntry> {
        self.trace
            .lock()
            .expect("network poisoned")
            .as_ref()
            .and_then(|trace| trace.entries.clone())
            .unwrap_or_default()
    }

    /// What the trace keeps of `message`, if tracing is on.
    fn stamp(&self, message: &Message) -> Option<Stamp> {
        let record = self
            .trace
            .lock()
            .expect("network poisoned")
            .as_ref()?
            .entries
            .is_some();
        // An encoding error still leaves a digest: the trace records that
        // the message was handed over, whatever it held.
        let digest = encode_tagged(RecordKind::GroupFrame, message)
            .map_or(0, |bytes| stable_hash_extend(STABLE_HASH_SEED, &bytes));
        let kind = record.then(|| {
            let debug = format!("{message:?}");
            debug
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .next()
                .unwrap_or_default()
                .to_string()
        });
        Some(Stamp { digest, kind })
    }

    /// Folds one step into the trace, if tracing is on.
    fn note(&self, from: NodeId, to: NodeId, fate: MessageFate, stamp: Option<&Stamp>) {
        let Some(stamp) = stamp else {
            return;
        };
        let mut guard = self.trace.lock().expect("network poisoned");
        let Some(trace) = guard.as_mut() else {
            return;
        };
        let micros = tokio::time::Instant::now()
            .saturating_duration_since(trace.start)
            .as_micros() as u64;
        let fate_code: u8 = match fate {
            MessageFate::Delivered => 0,
            MessageFate::Deferred => 1,
            MessageFate::Dropped => 2,
            MessageFate::Shed => 3,
        };
        let mut hash = trace.hash;
        hash = stable_hash_extend(hash, &micros.to_le_bytes());
        hash = stable_hash_extend(hash, &from.0.to_le_bytes());
        hash = stable_hash_extend(hash, &to.0.to_le_bytes());
        hash = stable_hash_extend(hash, &[fate_code]);
        hash = stable_hash_extend(hash, &stamp.digest.to_le_bytes());
        trace.hash = hash;
        if let Some(entries) = trace.entries.as_mut() {
            entries.push(TraceEntry {
                micros,
                from,
                to,
                fate,
                kind: stamp.kind.clone().unwrap_or_default(),
                digest: stamp.digest,
            });
        }
    }

    /// Whether `message` fits under the configured frame cap and is not
    /// [unreadable](Self::set_unreadable).
    fn fits(&self, message: &Message) -> bool {
        let unreadable = self.unreadable.lock().expect("network poisoned").clone();
        if unreadable.is_some_and(|unreadable| unreadable(message)) {
            return false;
        }
        let Some(cap) = *self.frame_cap.lock().expect("network poisoned") else {
            return true;
        };
        encode_tagged(RecordKind::GroupFrame, message).is_ok_and(|bytes| bytes.len() <= cap)
    }

    /// The configured delivery latency on `from → to` (zero if unset).
    fn latency_of(&self, from: NodeId, to: NodeId) -> Duration {
        self.latency
            .lock()
            .expect("network poisoned")
            .get(&(from, to))
            .copied()
            .unwrap_or_default()
    }

    /// Enqueues `envelope` into `to`'s inbox via `try_send`, counting a shed on a
    /// full inbox (backpressure). A closed/missing inbox is a down/partitioned
    /// peer — tolerated loss, not counted.
    fn deliver(&self, to: NodeId, envelope: Envelope) -> MessageFate {
        let inbox = {
            let inboxes = self.inboxes.lock().expect("network poisoned");
            inboxes.get(&to).cloned()
        };
        let Some(tx) = inbox else {
            return MessageFate::Dropped;
        };
        match tx.try_send(envelope) {
            Ok(()) => MessageFate::Delivered,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.shed.fetch_add(1, Ordering::Relaxed);
                MessageFate::Shed
            }
            Err(mpsc::error::TrySendError::Closed(_)) => MessageFate::Dropped,
        }
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
        let stamp = self.net.stamp(&message);
        // A crashed node neither sends nor receives; a partition drops the link.
        if self.net.is_crashed(self.from)
            || self.net.is_crashed(to)
            || self.net.is_partitioned(self.from, to)
        {
            self.net
                .note(self.from, to, MessageFate::Dropped, stamp.as_ref());
            return Ok(());
        }
        if !self.net.fits(&message) {
            self.net.shed.fetch_add(1, Ordering::Relaxed);
            self.net
                .note(self.from, to, MessageFate::Shed, stamp.as_ref());
            return Ok(());
        }

        let envelope = Envelope {
            from: self.from,
            message,
        };
        let latency = self.net.latency_of(self.from, to);
        if latency.is_zero() {
            // The common path: deliver inline, byte-identical to before (so the
            // deterministic simulation is unchanged).
            let fate = self.net.deliver(to, envelope);
            self.net.note(self.from, to, fate, stamp.as_ref());
        } else {
            // Latency model: defer delivery. Re-check liveness at arrival so a
            // crash/partition during flight still drops the in-flight message.
            self.net
                .note(self.from, to, MessageFate::Deferred, stamp.as_ref());
            let net = Arc::clone(&self.net);
            let from = self.from;
            tokio::spawn(async move {
                tokio::time::sleep(latency).await;
                if net.is_crashed(from) || net.is_crashed(to) || net.is_partitioned(from, to) {
                    net.note(from, to, MessageFate::Dropped, stamp.as_ref());
                    return;
                }
                let fate = net.deliver(to, envelope);
                net.note(from, to, fate, stamp.as_ref());
            });
        }
        Ok(())
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
    async fn an_untraced_network_keeps_no_trace() {
        let net = InMemoryNetwork::new();
        let _inbox = net.register(NodeId(2));
        net.sink(NodeId(1))
            .send(NodeId(2), Message::Applied { txn: txn(1) })
            .await
            .unwrap();
        assert_eq!(net.trace_hash(), None);
        assert!(net.trace().is_empty());
    }

    /// Two sends a few virtual milliseconds apart, the second over a link
    /// with latency; `cut` partitions the first.
    async fn traced_run(cut: bool) -> (u64, Vec<TraceEntry>) {
        let net = InMemoryNetwork::new();
        let _inbox2 = net.register(NodeId(2));
        let _inbox3 = net.register(NodeId(3));
        net.enable_trace(true);
        let sink = net.sink(NodeId(1));
        if cut {
            net.partition(NodeId(1), NodeId(2));
        }
        sink.send(NodeId(2), Message::Applied { txn: txn(1) })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        net.set_latency(NodeId(1), NodeId(3), Duration::from_millis(2));
        sink.send(NodeId(3), Message::Applied { txn: txn(2) })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        (net.trace_hash().expect("tracing is on"), net.trace())
    }

    #[tokio::test(start_paused = true)]
    async fn the_trace_parts_two_runs_only_where_they_differ() {
        let (first, entries) = traced_run(false).await;
        let (second, _) = traced_run(false).await;
        assert_eq!(first, second, "identical runs fold identical traces");
        let (cut, cut_entries) = traced_run(true).await;
        assert_ne!(first, cut, "a dropped message changes the trace");

        let steps: Vec<_> = entries.iter().map(|e| (e.micros, e.to.0, e.fate)).collect();
        assert_eq!(
            steps,
            vec![
                (0, 2, MessageFate::Delivered),
                (5_000, 3, MessageFate::Deferred),
                (7_000, 3, MessageFate::Delivered),
            ]
        );
        assert!(entries
            .iter()
            .all(|e| e.from == NodeId(1) && e.kind == "Applied"));
        assert_eq!(cut_entries[0].fate, MessageFate::Dropped);
        assert_ne!(
            entries[0].digest, entries[1].digest,
            "different messages digest differently"
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
