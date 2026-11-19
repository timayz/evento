//! The routing keys the nodes of a cluster serve, kept in the store.
//!
//! What a node says with its gossip ([`Builder::serves`](crate::host::Builder::serves))
//! is heard only from the nodes up: a node that starts while another is
//! away, or that copied its store after the other left, never heard that
//! node's key. So each node also writes the keys it serves into the store,
//! through the cluster ([`record`]), and any node reads the list back from
//! its own copy ([`known`]): the keys that may have streams, whoever is up.
//!
//! The list is ordinary events: one stream per key, of a type of the
//! cluster's own ([`AGGREGATE_TYPE`]), in the default tenant and without a
//! routing key. It is replicated, copied to a joining node and kept by a
//! backup like any other, and a write of a key already on it loses its
//! version check, so nodes that say theirs side by side need no retry. A
//! subscription sees these events only if it asks for their type; a read of
//! the whole event table does.
//!
//! A key is on the list from the first start of a node that serves it under
//! a build that keeps the list: the keys of nodes removed before that are
//! [`record`]ed by hand.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use evento_core::cursor::Args;
use evento_core::{Event, EventFilter, Executor, TenantFilter, WriteError};

/// The aggregate type of the streams the list is made of: one per key, the
/// key as its id.
pub const AGGREGATE_TYPE: &str = "evento-cluster/RoutingKey";

/// The one event of each such stream.
pub const SERVED: &str = "Served";

/// Events read at once when the list is read back.
const PAGE: u16 = 500;

/// Puts `key` on the list unless it is there; `true` when this call did.
/// Through a [`ClusterExecutor`](crate::ClusterExecutor) the write is
/// replicated, and refused like any other while the node reaches no
/// majority: the caller says its key again later.
pub async fn record<E: Executor>(executor: &E, key: &str) -> anyhow::Result<bool> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let event = Event {
        id: ulid::Ulid::generate(),
        aggregate_type: AGGREGATE_TYPE.to_owned(),
        aggregate_id: key.to_owned(),
        version: 1,
        name: SERVED.to_owned(),
        // Provisional, as a `WriteBuilder` stamps it: the store's commit
        // clock replaces it.
        timestamp: now.as_secs(),
        timestamp_subsec: now.subsec_millis(),
        ..Default::default()
    };
    match executor.write(vec![event]).await {
        Ok(()) => Ok(true),
        // The stream holds its one event: another node, or an earlier start
        // of this one, said the key.
        Err(WriteError::InvalidOriginalVersion) => Ok(false),
        Err(err) => Err(err).with_context(|| format!("cannot record the routing key {key:?}")),
    }
}

/// The keys on the list, as the store `executor` reads from holds it: a
/// node's own copy, which trails the cluster by what it has yet to apply.
pub async fn known<E: Executor>(executor: &E) -> anyhow::Result<BTreeSet<String>> {
    let filter: std::sync::Arc<[EventFilter]> = [EventFilter::by_type_raw(AGGREGATE_TYPE)].into();
    let mut keys = BTreeSet::new();
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
            .context("cannot read the routing keys the nodes serve")?;
        keys.extend(page.edges.into_iter().map(|edge| edge.node.aggregate_id));
        if !page.page_info.has_next_page {
            return Ok(keys);
        }
        after = page.page_info.end_cursor;
    }
}
