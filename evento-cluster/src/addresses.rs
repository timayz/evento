//! Where the nodes of a cluster are reached, kept in the store.
//!
//! A node's settings name the peers it was started with
//! ([`Settings::peers`](crate::host::Settings::peers)). A node added to the
//! cluster since ([`Cluster::add_node`](crate::Cluster::add_node)) is in no
//! running member's settings, so its address is written into the store,
//! through the cluster ([`record`]), and every member reads it back from its
//! own copy ([`known`]) and dials it there, without a restart.
//!
//! The addresses are ordinary events, as the routing keys are
//! ([`keys`](crate::keys)): one stream per node, of a type of the cluster's
//! own ([`AGGREGATE_TYPE`]), in the default tenant and without a routing
//! key; a node that moves gets one more event, and the last one stands.
//! They are replicated, copied to a joining node and kept by a backup like
//! any other. They say where a node is, not whether it counts: the members
//! are the layout the cluster agreed on ([`Cluster::members`](crate::Cluster::members)).

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use evento_core::cursor::Args;
use evento_core::{Event, EventFilter, Executor, TenantFilter, WriteError};

use crate::clock::NodeId;
use crate::tcp::PeerAddr;

/// The aggregate type of the streams the addresses are kept in: one per
/// node, its id as the stream's.
pub const AGGREGATE_TYPE: &str = "evento-cluster/Address";

/// The event of such a stream: the node is reached at the address it holds.
pub const REACHED: &str = "Reached";

/// Events read at once when the addresses are read back.
const PAGE: u16 = 500;

/// Times a write that lost its version check to another node's is tried
/// again over what that node wrote.
const ATTEMPTS: usize = 4;

/// Says that `node` is reached at `addr`, unless the store says so already;
/// `true` when this call did. Through a
/// [`ClusterExecutor`](crate::ClusterExecutor) the write is replicated, and
/// refused like any other while the node reaches no majority.
pub async fn record<E: Executor>(
    executor: &E,
    node: NodeId,
    addr: &PeerAddr,
) -> anyhow::Result<bool> {
    let stream = node.0.to_string();
    let filter: std::sync::Arc<[EventFilter]> =
        [EventFilter::by_id_raw(AGGREGATE_TYPE, &stream)].into();
    for _ in 0..ATTEMPTS {
        let mut last: Option<Event> = None;
        let mut after = None;
        loop {
            let page = executor
                .read(
                    Some(filter.clone()),
                    None,
                    TenantFilter::Only(String::new()),
                    Args::forward(PAGE, after),
                    None,
                )
                .await
                .with_context(|| format!("cannot read where node {} is reached", node.0))?;
            for edge in page.edges {
                if last.as_ref().is_none_or(|l| l.version < edge.node.version) {
                    last = Some(edge.node);
                }
            }
            if !page.page_info.has_next_page {
                break;
            }
            after = page.page_info.end_cursor;
        }
        if last.as_ref().and_then(parse).as_ref() == Some(addr) {
            return Ok(false);
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let event = Event {
            id: ulid::Ulid::generate(),
            aggregate_type: AGGREGATE_TYPE.to_owned(),
            aggregate_id: stream.clone(),
            version: last.map_or(1, |last| last.version + 1),
            name: REACHED.to_owned(),
            data: addr.to_string().into_bytes(),
            // Provisional, as a `WriteBuilder` stamps it: the store's commit
            // clock replaces it.
            timestamp: now.as_secs(),
            timestamp_subsec: now.subsec_millis(),
            ..Default::default()
        };
        match executor.write(vec![event]).await {
            Ok(()) => return Ok(true),
            // Another node wrote the stream meanwhile: read what it said.
            Err(WriteError::InvalidOriginalVersion) => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("cannot record where node {} is reached", node.0))
            }
        }
    }
    anyhow::bail!(
        "cannot record where node {} is reached: its address kept changing",
        node.0
    )
}

/// Where each node is reached, as the store `executor` reads from holds it:
/// a node's own copy, which trails the cluster by what it has yet to apply.
pub async fn known<E: Executor>(executor: &E) -> anyhow::Result<BTreeMap<NodeId, PeerAddr>> {
    let filter: std::sync::Arc<[EventFilter]> = [EventFilter::by_type_raw(AGGREGATE_TYPE)].into();
    let mut latest: BTreeMap<NodeId, (u16, PeerAddr)> = BTreeMap::new();
    let mut after = None;
    loop {
        let page = executor
            .read(
                Some(filter.clone()),
                None,
                TenantFilter::Only(String::new()),
                Args::forward(PAGE, after),
                None,
            )
            .await
            .context("cannot read where the nodes are reached")?;
        for edge in page.edges {
            let event = edge.node;
            let (Ok(id), Some(addr)) = (event.aggregate_id.parse::<u64>(), parse(&event)) else {
                continue;
            };
            let node = NodeId(id);
            if latest.get(&node).is_none_or(|(v, _)| *v < event.version) {
                latest.insert(node, (event.version, addr));
            }
        }
        if !page.page_info.has_next_page {
            break;
        }
        after = page.page_info.end_cursor;
    }
    Ok(latest
        .into_iter()
        .map(|(node, (_, addr))| (node, addr))
        .collect())
}

/// The address a [`REACHED`] event holds.
fn parse(event: &Event) -> Option<PeerAddr> {
    std::str::from_utf8(&event.data).ok()?.parse().ok()
}
