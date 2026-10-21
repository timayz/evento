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
use crate::node::{Node, NodeConfig, PartialSnapshot};
use crate::sweep::SweepScheduler;
use crate::tcp::MuxTransport;
use crate::transport::GroupId;

/// Called with a group whose node was told to rejoin (see
/// [`Node::rejoin_required`]): the host should [`close`](GroupHost::close) it,
/// discard its **event store** — but keep its journal — and open it again
/// with [`open_joining`](GroupHost::open_joining) over the same journal. The
/// journal holds the node's votes, promises and clock bound, which a
/// rejoined node must not forget. Called once per open, from a task of its
/// own, so it may block or spawn as it likes.
pub type RejoinHandler = Arc<dyn Fn(GroupId) + Send + Sync>;

struct Running {
    node: Node,
    inbox_task: JoinHandle<()>,
    /// Waits for the node's rejoin signal and calls the handler, if one is set.
    rejoin_task: Option<JoinHandle<()>>,
}

/// A group being opened, until [`opened`](Self::opened) hands its inbox loop
/// to [`Running`]. Dropped before that — the open failed, or the `open`
/// future itself was dropped mid-join — it unregisters the group and stops
/// the loop: nothing else owns them, so no `close` could.
struct Opening<'a> {
    mux: &'a MuxTransport,
    group: GroupId,
    inbox_task: Option<JoinHandle<()>>,
}

impl Opening<'_> {
    /// The group is open: its inbox loop now belongs to the caller.
    fn opened(mut self) -> JoinHandle<()> {
        let task = self.inbox_task.take().expect("the inbox loop was started");
        std::mem::forget(self);
        task
    }

    /// Unregisters the group and returns once its inbox loop has stopped.
    async fn stop(mut self) {
        self.mux.unregister(self.group);
        if let Some(task) = self.inbox_task.take() {
            task.abort();
            let _ = task.await;
        }
        std::mem::forget(self);
    }
}

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        self.mux.unregister(self.group);
        if let Some(task) = self.inbox_task.take() {
            task.abort();
        }
    }
}

/// Releases a group's open/close lock (see [`GroupHost::release_group_lock`])
/// when dropped, also when the open or close holding it is.
struct ReleaseLock<'a> {
    host: &'a GroupHost,
    group: GroupId,
    lock: &'a Arc<tokio::sync::Mutex<()>>,
}

impl Drop for ReleaseLock<'_> {
    fn drop(&mut self) {
        self.host.release_group_lock(self.group, self.lock);
    }
}

/// Hosts the groups one process participates in; see the module docs.
pub struct GroupHost {
    id: NodeId,
    mux: Arc<MuxTransport>,
    scheduler: Arc<SweepScheduler>,
    /// A clock shared by every group, if configured; otherwise one per group.
    clock: Option<Arc<HybridLogicalClock>>,
    rejoin: Option<RejoinHandler>,
    groups: Mutex<HashMap<GroupId, Running>>,
    /// One open or close per group at a time. The transport keeps a single
    /// inbox per group, so a second open registering its own would cut the
    /// first node off from every frame; it waits instead, and finds the
    /// group open. A close holds it too, so an open waits for the node it
    /// replaces to be gone. Removed once nobody holds or awaits it.
    opening: Mutex<HashMap<GroupId, Arc<tokio::sync::Mutex<()>>>>,
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
            rejoin: None,
            groups: Mutex::new(HashMap::new()),
            opening: Mutex::new(HashMap::new()),
        }
    }

    /// Installs the [`RejoinHandler`] called when a group's node is told by a
    /// peer to rejoin: it had been silent past
    /// [`NodeConfig::peer_absence_limit`] while its peers compacted
    /// transactions it never applied, so anti-entropy cannot complete it. The
    /// node drops every message and refuses writes from then on; without a
    /// handler it stays like that until closed.
    pub fn with_rejoin_handler(
        mut self,
        handler: impl Fn(GroupId) + Send + Sync + 'static,
    ) -> Self {
        self.rejoin = Some(Arc::new(handler));
        self
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
    /// as is, and concurrent opens of one group are serialized — the later
    /// ones wait and return the node the first opened. On a recovery error
    /// nothing is left registered.
    pub async fn open(
        &self,
        group: GroupId,
        topology: Arc<dyn Topology>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
        config: NodeConfig,
    ) -> anyhow::Result<Node> {
        self.open_with(group, topology, datastore, journal, config, &[])
            .await
    }

    /// Opens `group` over an empty event store by **joining** it: the node
    /// recovers its journal ([`Node::recover_state_for_join`]) — empty for a
    /// new member, kept for one told to rejoin or whose store was lost —
    /// then buffers consensus traffic while it fetches a peer's data-store
    /// snapshot, compaction floor and recent commands ([`Node::join`]), and
    /// replays what it buffered. A plain [`open`](Self::open) would recover
    /// only the journal tail and leave the prefix its peers already compacted
    /// missing for good.
    ///
    /// Keep the journal: only the event store is discarded. A node that lost
    /// its journal, or got an older one back, forgot votes and promises it
    /// made — it must come back as a new member under a new node id (see
    /// `OPERATIONS.md` §7).
    ///
    /// `contacts` are tried in order until one answers; a contact that is
    /// itself joining does not answer. If none does, nothing is left
    /// registered and the error is returned, so the caller can retry later.
    /// Idempotent like `open`. The store must be empty (`join` refuses
    /// otherwise): the snapshot arrives page by page, and a page that
    /// overlaps existing events is skipped whole as an idempotent re-apply.
    /// For the same reason a join that fails after a page was installed is
    /// not retried on the next contact: the error carries a
    /// [`PartialSnapshot`](crate::PartialSnapshot) and nothing is left
    /// registered — discard the store (keep the journal), then open again.
    pub async fn open_joining(
        &self,
        group: GroupId,
        topology: Arc<dyn Topology>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
        config: NodeConfig,
        contacts: &[NodeId],
    ) -> anyhow::Result<Node> {
        anyhow::ensure!(!contacts.is_empty(), "joining a group needs a contact");
        self.open_with(group, topology, datastore, journal, config, contacts)
            .await
    }

    async fn open_with(
        &self,
        group: GroupId,
        topology: Arc<dyn Topology>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
        config: NodeConfig,
        contacts: &[NodeId],
    ) -> anyhow::Result<Node> {
        if let Some(node) = self.get(group) {
            return Ok(node);
        }
        let lock = self.group_lock(group);
        // Declared first so it drops last, after the lock is unlocked — also
        // when this future is dropped mid-open.
        let _release = ReleaseLock {
            host: self,
            group,
            lock: &lock,
        };
        let _opening = lock.lock().await;
        match self.get(group) {
            Some(node) => Ok(node),
            None => {
                self.open_locked(group, topology, datastore, journal, config, contacts)
                    .await
            }
        }
    }

    /// `group`'s open/close lock (see `opening`).
    fn group_lock(&self, group: GroupId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.opening
                .lock()
                .expect("opening poisoned")
                .entry(group)
                .or_default(),
        )
    }

    /// Drops `group`'s lock from the map once nobody else holds or awaits it.
    fn release_group_lock(&self, group: GroupId, lock: &Arc<tokio::sync::Mutex<()>>) {
        // The map's handle and ours: nobody else is opening or closing.
        let mut opening = self.opening.lock().expect("opening poisoned");
        if Arc::strong_count(lock) == 2 {
            opening.remove(&group);
        }
    }

    /// [`open_with`](Self::open_with)'s work, under the group's open lock.
    async fn open_locked(
        &self,
        group: GroupId,
        topology: Arc<dyn Topology>,
        datastore: Arc<dyn DataStore>,
        journal: Arc<dyn Journal>,
        config: NodeConfig,
        contacts: &[NodeId],
    ) -> anyhow::Result<Node> {
        let inbox = self.mux.register(group);
        let mut opening = Opening {
            mux: &self.mux,
            group,
            inbox_task: None,
        };
        let clock = self
            .clock
            .clone()
            .unwrap_or_else(|| Arc::new(HybridLogicalClock::new(self.id)));
        let sink: Arc<dyn MessageSink> = Arc::new(self.mux.sink(group));
        let node =
            Node::new(self.id, topology, clock, sink, datastore, journal).with_config(config);
        let joining = !contacts.is_empty();
        if joining {
            // The store is to be rebuilt from a snapshot: nothing is
            // replayed into it.
            node.recover_state_for_join().await?;
        } else {
            node.recover_state().await?;
        }
        if joining {
            // Before the inbox loop starts, so no frame (parked ones included)
            // is processed ahead of the snapshot.
            node.begin_join();
        }
        opening.inbox_task = Some(node.start(inbox));
        if joining {
            let mut last = None;
            for &contact in contacts {
                match node.join(contact).await {
                    Ok(_) => {
                        last = None;
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(
                            node = self.id.0,
                            group = group.0,
                            contact = contact.0,
                            err = format!("{err:#}"),
                            "join contact failed"
                        );
                        // Part of the snapshot landed: another contact's
                        // pages would overlap it and be skipped, leaving a
                        // gap. The store has to be emptied first (the
                        // journal is kept).
                        let partial = err.downcast_ref::<PartialSnapshot>().is_some();
                        last = Some((err, partial));
                        if partial {
                            break;
                        }
                    }
                }
            }
            if let Some((err, partial)) = last {
                // Stopped before returning: on a partial snapshot the caller
                // discards the store this loop writes to.
                opening.stop().await;
                return Err(if partial {
                    err
                } else {
                    err.context("no contact could bootstrap the group")
                });
            }
        }
        let inbox_task = opening.opened();
        self.scheduler.add(group, Arc::new(node.clone()));
        let rejoin_task = self.rejoin.clone().map(|handler| {
            let mut signal = node.rejoin_signal();
            tokio::spawn(async move {
                loop {
                    if *signal.borrow_and_update() {
                        handler(group);
                        return;
                    }
                    if signal.changed().await.is_err() {
                        return;
                    }
                }
            })
        });

        self.groups.lock().expect("groups poisoned").insert(
            group,
            Running {
                node: node.clone(),
                inbox_task,
                rejoin_task,
            },
        );
        Ok(node)
    }

    /// Closes `group` on this host: unregisters it from the transport (later
    /// frames for it are treated as unrouted), stops its sweeps and its inbox
    /// loop, and returns the node once neither touches its store or journal
    /// any more — the caller may then close or discard them. The node mints
    /// nothing more: a clone the application still holds fails to write or
    /// recover with [`NodeClosed`](crate::NodeClosed), so it can never mint
    /// an id the group's next node, opened over the same journal, mints too.
    ///
    /// A sweep or a message being handled is dropped at its next await point,
    /// as a crash would drop it. An open of the same group waits until this
    /// returns. Never call it from one of the group's own tasks (its inbox
    /// loop or a sweep): it would wait for itself. A dropped Fjall write may
    /// still finish on its blocking thread, so reopening the same Fjall
    /// directory right away can find it locked a moment longer.
    pub async fn close(&self, group: GroupId) -> Option<Node> {
        let lock = self.group_lock(group);
        let _release = ReleaseLock {
            host: self,
            group,
            lock: &lock,
        };
        let _closing = lock.lock().await;
        self.close_locked(group).await
    }

    /// [`close`](Self::close)'s work, under the group's open/close lock.
    async fn close_locked(&self, group: GroupId) -> Option<Node> {
        let running = self
            .groups
            .lock()
            .expect("groups poisoned")
            .remove(&group)?;
        // Not awaited: the rejoin handler may itself be closing this group.
        if let Some(task) = running.rejoin_task {
            task.abort();
        }
        running.node.retire().await;
        self.scheduler.cancel(group).await;
        // Unregistered first, so frames arriving meanwhile are parked as
        // unrouted rather than sent into an inbox about to close.
        self.mux.unregister(group);
        running.inbox_task.abort();
        let _ = running.inbox_task.await;
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
