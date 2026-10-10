//! Per-process glue for hosting many consensus groups: a [`GroupHost`] opens and
//! closes one [`Node`] per [`GroupId`] over a shared [`MuxTransport`] and
//! [`SweepScheduler`], so an application hosting one group per tenant does not
//! repeat the wiring — register inbox, build sink, construct node, recover from
//! the journal, start the inbox loop, schedule sweeps — for every tenant.
//!
//! What a group *stores* stays the caller's: it supplies the [`DataStore`] and
//! [`Journal`] (e.g. a tenant's own SQLite file through `evento_sql::Sql` +
//! `evento_sql::SqlJournal`), the [`Topology`], and the [`NodeConfig`]. Which
//! groups exist, and when to evict a cold one, is likewise application policy —
//! see the `bank-axum-accord-tenants` example for a catalog-driven registry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::api::{DataStore, Journal, MessageSink, Topology};
use crate::clock::{HybridLogicalClock, NodeId};
use crate::node::{Node, NodeConfig};
use crate::sweep::SweepScheduler;
use crate::tcp::MuxTransport;
use crate::transport::GroupId;

struct Running {
    node: Node,
    inbox_task: JoinHandle<()>,
}

/// Hosts the groups one process participates in; see the module docs.
pub struct GroupHost {
    id: NodeId,
    mux: Arc<MuxTransport>,
    scheduler: Arc<SweepScheduler>,
    /// A clock shared by every group, if configured; otherwise one per group.
    clock: Option<Arc<HybridLogicalClock>>,
    groups: Mutex<HashMap<GroupId, Running>>,
}

impl GroupHost {
    /// Builds a host for the node id `mux` sends as. `scheduler` should be started
    /// ([`SweepScheduler::start`]) and `mux` served ([`MuxTransport::serve`]) by the
    /// caller, once per process.
    pub fn new(mux: Arc<MuxTransport>, scheduler: Arc<SweepScheduler>) -> Self {
        Self {
            id: mux.node_id(),
            mux,
            scheduler,
            clock: None,
            groups: Mutex::new(HashMap::new()),
        }
    }

    /// Shares one [`HybridLogicalClock`] across every group instead of a clock per
    /// group. Safe — a timestamp is only ever compared within its group, and a
    /// witnessed peer timestamp only moves the clock forward (bounded by its skew
    /// cap) — and it makes stamps monotone across groups, at the cost of one
    /// process-wide lock on every clock read. Off by default.
    pub fn with_shared_clock(mut self, clock: Arc<HybridLogicalClock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// This host's node id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The shared transport.
    pub fn transport(&self) -> &Arc<MuxTransport> {
        &self.mux
    }

    /// The shared scheduler.
    pub fn scheduler(&self) -> &Arc<SweepScheduler> {
        &self.scheduler
    }

    /// Opens `group` on this host: registers its inbox (replaying any frames parked
    /// for it), builds its [`Node`] over the given store/journal/topology,
    /// **recovers** consensus state from the journal, starts the inbox loop, and
    /// schedules its sweeps. Idempotent: an already-open group's node is returned
    /// as is. On a recovery error nothing is left registered.
    pub async fn open(
        &self,
        group: GroupId,
        topology: Arc<dyn Topology>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
        config: NodeConfig,
    ) -> anyhow::Result<Node> {
        if let Some(node) = self.get(group) {
            return Ok(node);
        }
        let inbox = self.mux.register(group);
        let clock = self
            .clock
            .clone()
            .unwrap_or_else(|| Arc::new(HybridLogicalClock::new(self.id)));
        let sink: Arc<dyn MessageSink> = Arc::new(self.mux.sink(group));
        let node =
            Node::new(self.id, topology, clock, sink, datastore, journal).with_config(config);
        if let Err(err) = node.recover_state().await {
            self.mux.unregister(group);
            return Err(err);
        }
        let inbox_task = node.start(inbox);
        self.scheduler.add(group, Arc::new(node.clone()));

        let mut groups = self.groups.lock().expect("groups poisoned");
        // Lost a race with a concurrent `open` of the same group: keep the first.
        if let Some(existing) = groups.get(&group) {
            self.scheduler.add(group, Arc::new(existing.node.clone()));
            inbox_task.abort();
            return Ok(existing.node.clone());
        }
        groups.insert(
            group,
            Running {
                node: node.clone(),
                inbox_task,
            },
        );
        Ok(node)
    }

    /// Closes `group` on this host: stops its sweeps and inbox loop and
    /// unregisters it from the transport (later frames for it are treated as
    /// unrouted). Returns the node, whose store/journal the caller may now close.
    pub fn close(&self, group: GroupId) -> Option<Node> {
        let running = self
            .groups
            .lock()
            .expect("groups poisoned")
            .remove(&group)?;
        self.scheduler.remove(group);
        running.inbox_task.abort();
        self.mux.unregister(group);
        Some(running.node)
    }

    /// The node of an open group.
    pub fn get(&self, group: GroupId) -> Option<Node> {
        self.groups
            .lock()
            .expect("groups poisoned")
            .get(&group)
            .map(|r| r.node.clone())
    }

    /// Whether `group` is open here.
    pub fn is_open(&self, group: GroupId) -> bool {
        self.groups
            .lock()
            .expect("groups poisoned")
            .contains_key(&group)
    }

    /// Open groups, in no particular order.
    pub fn groups(&self) -> Vec<GroupId> {
        self.groups
            .lock()
            .expect("groups poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// Number of open groups.
    pub fn len(&self) -> usize {
        self.groups.lock().expect("groups poisoned").len()
    }

    /// Whether no group is open.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
