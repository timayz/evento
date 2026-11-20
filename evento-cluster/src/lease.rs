//! The lease of a routing key: which node runs the key's subscriptions,
//! agreed through the cluster (#565).
//!
//! Who is up is what each node heard, so two nodes can each decide a key is
//! theirs to follow ([`cover`](crate::cover)). The lease settles it: taking
//! a key is a write through the cluster ([`take`]), which one node wins, and
//! the holder asks the cluster whether the lease is still its own
//! ([`confirmed`]) before it goes on.
//!
//! The lease is ordinary events: one stream per key, of the type
//! [`AGGREGATE_TYPE`], under the key as its id and its routing key, in the
//! default tenant. Each event is one **term**: it names the node that took
//! the key, and the stream's version is the term's number. The latest names
//! the holder. Written under the key, the stream lives in the shard of the
//! key's own streams: the lease can be taken where the key can be written.
//!
//! Nothing is written to renew it. A holder is in its right for
//! [`Cover::lease`](crate::Cover::lease) from the moment it asked and was
//! told the lease is its own: a question the cluster answers only through a
//! majority ([`Node::read_barrier`]), so a node cut off is not answered and
//! stops when that time is up. A node that takes the lease from another
//! waits as long before it starts. A stream holds 65 535 versions: as many
//! terms per key.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use evento_core::cursor::Args;
use evento_core::{Event, EventFilter, Executor, TenantFilter, WriteError};

use crate::clock::NodeId;
use crate::message::Key;
use crate::node::Node;

/// The aggregate type of the lease streams: one per key, the key as its id.
pub const AGGREGATE_TYPE: &str = "evento-cluster/Lease";

/// The event of each term: its data is the id of the node that took the key,
/// eight bytes, little-endian.
pub const TAKEN: &str = "Taken";

/// A term of a key's lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// The node that took the key.
    pub holder: NodeId,
    /// The term's number, from 1: the version of its event.
    pub term: u16,
    /// When it was taken, in microseconds since the Unix epoch: the stamp
    /// the cluster gave its event.
    pub taken_at: u64,
}

/// The key the lease of `key` conflicts on and is fenced by: its own stream,
/// under the routing key.
pub fn key_of(key: &str) -> Key {
    Key::of_stream("", Some(key), key)
}

/// The latest term of `key`'s lease as the store `executor` reads from holds
/// it: a node's own copy, which may trail the cluster. `None` for a key no
/// node took yet.
pub async fn held<E: Executor>(executor: &E, key: &str) -> anyhow::Result<Option<Lease>> {
    let filter: Arc<[EventFilter]> = [EventFilter::by_id_raw(AGGREGATE_TYPE, key)].into();
    let page = executor
        .read(
            Some(filter),
            None,
            TenantFilter::Only(String::new()),
            Args::backward(1, None),
            None,
        )
        .await
        .with_context(|| format!("cannot read the lease of the routing key {key:?}"))?;
    let Some(edge) = page.edges.last() else {
        return Ok(None);
    };
    let holder: [u8; 8] = edge
        .node
        .data
        .as_slice()
        .try_into()
        .with_context(|| format!("the lease of the routing key {key:?} names no node"))?;
    Ok(Some(Lease {
        holder: NodeId(u64::from_le_bytes(holder)),
        term: edge.node.version,
        taken_at: edge.node.timestamp_micros(),
    }))
}

/// The latest term of `key`'s lease as the cluster holds it: every term
/// taken before the call began is seen. Fails while `node` reaches no
/// majority of the key's replicas, which is what a holder cut off learns
/// from. `executor` reads `node`'s own store.
pub async fn confirmed<E: Executor>(
    node: &Node,
    executor: &E,
    key: &str,
) -> anyhow::Result<Option<Lease>> {
    node.read_barrier(key_of(key))
        .await
        .with_context(|| format!("cannot confirm the lease of the routing key {key:?}"))?;
    held(executor, key).await
}

/// Takes `key` for `me`: writes the term after `after` (the first, for
/// `None`). `None` when the lease moved meanwhile: another node took the
/// key, or the store `after` was read from trailed. Through a
/// [`ClusterExecutor`](crate::ClusterExecutor) the write is refused like any
/// other while the node reaches no majority.
pub async fn take<E: Executor>(
    executor: &E,
    key: &str,
    me: NodeId,
    after: Option<Lease>,
) -> anyhow::Result<Option<Lease>> {
    let term = after
        .map_or(0, |lease| lease.term)
        .checked_add(1)
        .with_context(|| format!("the lease of the routing key {key:?} is out of terms"))?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let event = Event {
        id: ulid::Ulid::generate(),
        aggregate_type: AGGREGATE_TYPE.to_owned(),
        aggregate_id: key.to_owned(),
        version: term,
        name: TAKEN.to_owned(),
        routing_key: Some(key.to_owned()),
        data: me.0.to_le_bytes().to_vec(),
        // Provisional, as a `WriteBuilder` stamps it: the store's commit
        // clock replaces it.
        timestamp: now.as_secs(),
        timestamp_subsec: now.subsec_millis(),
        ..Default::default()
    };
    match executor.write(vec![event]).await {
        Ok(()) => Ok(Some(Lease {
            holder: me,
            term,
            taken_at: now.as_micros() as u64,
        })),
        Err(WriteError::InvalidOriginalVersion) => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("cannot take the lease of the routing key {key:?}"))
        }
    }
}
