//! The node a deployer runs, from its settings: one [`Cluster`] per process,
//! over a local store that is a [`ClusterBackend`].
//!
//! A `[cluster]` section of the application's configuration deserializes
//! into [`Settings`]; without one the node talks to itself in memory, so the
//! same code serves a single node and a cluster. The host does what every
//! deployment otherwise writes by hand (`OPERATIONS.md` §2 and §7): it binds
//! the transport, derives the [`NodeConfig`], opens the journal colocated
//! with the store, recovers the node or — when the store is marked for a
//! copy — empties it and opens it by joining, runs the inbox loop and the
//! recovery sweep, and watches the signals that end a node's part in the
//! cluster (told to rejoin, journal rolled back), marking the store for a
//! copy at the next start and saying so in the log.
//!
//! ```rust,ignore
//! let (cluster, executor) = Cluster::builder(store)
//!     .settings(config.cluster)         // `None`: a single node
//!     .state_dir(&config.data_dir)      // where the rejoin mark lives
//!     .open()
//!     .await?;
//! // `executor` is a `ClusterExecutor<_>`, a drop-in `evento::Executor`;
//! // `cluster.health()` is what a status page shows; `cluster.shutdown()`
//! // ends the node.
//! ```

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::backend::ClusterBackend;
use crate::clock::{HybridLogicalClock, NodeId};
use crate::executor::{ClusterExecutor, ExecutorDataStore};
use crate::node::{LostJournal, Node, NodeConfig, PartialSnapshot};
use crate::tcp::{serve, TcpTransport};
use crate::transport::{Envelope, InMemoryNetwork};
use crate::{DataStore, Journal, MessageSink, StaticTopology, Topology};

/// Frames waiting for the node's inbox loop, from the TCP listener.
const INBOX_FRAMES: usize = 1024;

/// The `[cluster]` section of a deployment's configuration: the membership
/// and the few knobs an operator sets. Everything else in [`NodeConfig`]
/// keeps its default, which a [`Builder::node_config`] hook can still
/// change.
///
/// ```toml
/// [cluster]
/// node_id = 1
/// peers = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]
/// listen = "0.0.0.0:7000"          # optional; defaults to peers[node_id]
/// absent_after_secs = 3600         # optional; 0 never gives a node up
/// write_backlog = 100000           # optional
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// This node's index into `peers`.
    pub node_id: u64,
    /// Every node's consensus address, this one included: `NodeId(i)` is
    /// `peers[i]`. One address is a cluster of one node over TCP.
    pub peers: Vec<SocketAddr>,
    /// The address to bind for inbound consensus traffic. Defaults to
    /// `peers[node_id]`; set it to `0.0.0.0:<port>` in a container.
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    /// A node not heard from for this long is no longer waited for: the
    /// others tidy up finished writes without it
    /// ([`NodeConfig::peer_absence_limit`]). When it returns it is told so,
    /// and comes back by copying the store from a peer at its next start. 0
    /// never gives a node up; then a node that stays down holds every
    /// finished write on the others until `write_backlog` is reached.
    #[serde(default = "default_absent_after_secs")]
    pub absent_after_secs: u64,
    /// Finished writes this node keeps for a node that is away
    /// ([`NodeConfig::max_commands`]). Past it the node refuses new writes
    /// until the node returns or is given up.
    #[serde(default = "default_write_backlog")]
    pub write_backlog: usize,
    /// Whether a read of one aggregate waits for the writes a quorum already
    /// committed ([`NodeConfig::linearizable_reads`]; one consensus round
    /// per read). Unset, it is on with peers and off for a node alone, which
    /// applies its own writes before the commit returns.
    #[serde(default)]
    pub linearizable_reads: Option<bool>,
}

fn default_absent_after_secs() -> u64 {
    3600
}

fn default_write_backlog() -> usize {
    100_000
}

impl Settings {
    /// A cluster of `peers` seen from node `node_id`, with the defaults.
    pub fn new(node_id: u64, peers: Vec<SocketAddr>) -> Self {
        Self {
            node_id,
            peers,
            listen: None,
            absent_after_secs: default_absent_after_secs(),
            write_backlog: default_write_backlog(),
            linearizable_reads: None,
        }
    }

    /// Refuses settings no node can run on: no peers, a `node_id` outside
    /// `peers`, a `write_backlog` of zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.peers.is_empty(),
            "cluster.peers must list at least one node"
        );
        anyhow::ensure!(
            (self.node_id as usize) < self.peers.len(),
            "cluster.node_id {} is out of range for {} peers",
            self.node_id,
            self.peers.len()
        );
        anyhow::ensure!(
            self.write_backlog > 0,
            "cluster.write_backlog must be greater than 0"
        );
        Ok(())
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        NodeId(self.node_id)
    }

    /// Every node's id, in `peers` order.
    pub fn ids(&self) -> Vec<NodeId> {
        (0..self.peers.len() as u64).map(NodeId).collect()
    }

    /// Whether this node has peers.
    pub fn has_peers(&self) -> bool {
        self.peers.len() > 1
    }

    /// The address this node binds.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen
            .unwrap_or_else(|| self.peers[self.node_id as usize])
    }

    /// `absent_after_secs` as a duration; `None` for 0.
    pub fn absent_after(&self) -> Option<Duration> {
        (self.absent_after_secs > 0).then(|| Duration::from_secs(self.absent_after_secs))
    }

    /// The [`NodeConfig`] these settings mean: the defaults, with
    /// linearizable reads and the absence limit only where there are peers
    /// to read from or wait for.
    pub fn node_config(&self) -> NodeConfig {
        let defaults = NodeConfig::default();
        let has_peers = self.has_peers();
        NodeConfig {
            linearizable_reads: self.linearizable_reads.unwrap_or(has_peers),
            peer_absence_limit: if has_peers { self.absent_after() } else { None },
            max_commands: self.write_backlog,
            ..defaults
        }
    }
}

/// What the next open of a marked store does before joining.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejoinMark {
    /// Empty the store, keep the journal: the node was told to rejoin (its
    /// peers tidied up writes it never got), or its store was lost.
    KeepJournal,
    /// Empty the store and discard the journal: the journal was found to be
    /// an older copy of itself ([`JournalRolledBack`](crate::JournalRolledBack)).
    /// The node comes back under the amnesia fence, as over a lost journal.
    DiscardJournal,
}

impl RejoinMark {
    const FILE: &'static str = "rejoin";
    const KEEP: &'static str = "keep-journal";
    const DISCARD: &'static str = "discard-journal";

    /// The mark's file under `state_dir`.
    pub fn path(state_dir: &Path) -> PathBuf {
        state_dir.join(Self::FILE)
    }

    /// The mark under `state_dir`, if any. An unreadable mark is an error:
    /// opening the store as it is would be the wrong default.
    pub fn read(state_dir: &Path) -> anyhow::Result<Option<Self>> {
        let path = Self::path(state_dir);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err).with_context(|| format!("cannot read {}", path.display())),
        };
        match text.trim() {
            // An empty mark, left by an older host or by hand: keep the journal.
            "" | Self::KEEP => Ok(Some(Self::KeepJournal)),
            Self::DISCARD => Ok(Some(Self::DiscardJournal)),
            other => anyhow::bail!(
                "{} holds {other:?}; expected {:?} or {:?}",
                path.display(),
                Self::KEEP,
                Self::DISCARD
            ),
        }
    }

    /// Leaves the mark under `state_dir` (creating the directory), so the
    /// next open copies the store from a peer. A stronger mark is never
    /// weakened: a `DiscardJournal` stays when `KeepJournal` is written over it.
    pub fn write(self, state_dir: &Path) -> anyhow::Result<PathBuf> {
        std::fs::create_dir_all(state_dir)
            .with_context(|| format!("cannot create {}", state_dir.display()))?;
        let path = Self::path(state_dir);
        if self == Self::KeepJournal && Self::read(state_dir)? == Some(Self::DiscardJournal) {
            return Ok(path);
        }
        let text = match self {
            Self::KeepJournal => Self::KEEP,
            Self::DiscardJournal => Self::DISCARD,
        };
        std::fs::write(&path, text).with_context(|| format!("cannot write {}", path.display()))?;
        Ok(path)
    }

    /// Removes the mark under `state_dir`, if any.
    pub fn clear(state_dir: &Path) -> anyhow::Result<()> {
        match std::fs::remove_file(Self::path(state_dir)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

/// Another node, as this one sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerState {
    /// Its id.
    pub id: NodeId,
    /// How long since it was last heard from (since this node opened, if
    /// never).
    pub silent_for: Duration,
    /// Whether this node stopped waiting for it ([`Settings::absent_after_secs`]).
    pub given_up: bool,
}

/// This node, for a status page and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealth {
    /// This node's id.
    pub id: NodeId,
    /// Nodes in the cluster, this one included.
    pub nodes: usize,
    /// The finished writes kept for nodes that are away, and the most that
    /// may be kept before refusing new ones.
    pub backlog: (usize, usize),
    /// The other nodes, as this one sees them.
    pub peers: Vec<PeerState>,
    /// With peers, whether they confirmed the journal is current; alone,
    /// always. Until then the node refuses writes.
    pub confirmed: bool,
    /// The other nodes tidied up writes this node never got: it refuses
    /// writes until it copies the store back (restart it).
    pub rejoin_required: bool,
    /// The journal is an older copy of itself: the node refuses writes
    /// until it copies the store and the journal back (restart it).
    pub rolled_back: bool,
    /// Whether the store is marked for a copy at the next start.
    pub copy_pending: bool,
    /// Whether this start copied the store from a peer.
    pub joined_at_start: bool,
}

impl NodeHealth {
    /// Whether the node takes writes: confirmed, and nothing ended its part.
    pub fn serving(&self) -> bool {
        self.confirmed && !self.rejoin_required && !self.rolled_back && !self.copy_pending
    }
}

/// A future the builder runs for the application.
type Hook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send>;

/// Opens a [`Cluster`]: see [`Cluster::builder`].
pub struct Builder<E: ClusterBackend> {
    store: E,
    settings: Option<Settings>,
    state_dir: Option<PathBuf>,
    config: Option<Box<dyn FnOnce(NodeConfig) -> NodeConfig + Send>>,
    journal: Option<Arc<dyn Journal>>,
    before_copy: Option<Hook>,
}

impl<E: ClusterBackend> Builder<E> {
    /// The `[cluster]` settings; `None` (the default) runs a single node
    /// over an in-memory transport.
    pub fn settings(mut self, settings: Option<Settings>) -> Self {
        self.settings = settings;
        self
    }

    /// The directory the host keeps its state in: the rejoin mark
    /// ([`RejoinMark`]). Required with peers; a node alone needs none.
    pub fn state_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(dir.into());
        self
    }

    /// Adjusts the [`NodeConfig`] derived from the settings
    /// ([`Settings::node_config`]): the timeouts for a WAN, for instance.
    pub fn node_config(
        mut self,
        adjust: impl FnOnce(NodeConfig) -> NodeConfig + Send + 'static,
    ) -> Self {
        self.config = Some(Box::new(adjust));
        self
    }

    /// A journal in place of the store's own ([`ClusterBackend::open_journal`]):
    /// an [`InMemoryJournal`](crate::InMemoryJournal) for a demo that keeps
    /// nothing across restarts. Not for production.
    pub fn journal(mut self, journal: Arc<dyn Journal>) -> Self {
        self.journal = Some(journal);
        self
    }

    /// Runs once the store was emptied for a copy and before the copy
    /// begins: for what the application derives from the events and keeps
    /// next to them (read models in the same database), which the
    /// subscriptions fill again from the copied events.
    pub fn before_copy<F, Fut>(mut self, hook: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.before_copy = Some(Box::new(move || Box::pin(hook())));
        self
    }

    /// Opens the node and starts it. See [`Cluster`] for what happens when
    /// the store is marked for a copy.
    pub async fn open(self) -> anyhow::Result<(Arc<Cluster>, ClusterExecutor<E>)> {
        let Self {
            store,
            settings,
            state_dir,
            config,
            journal,
            before_copy,
        } = self;
        if let Some(settings) = &settings {
            settings.validate()?;
        }
        let (id, ids, peers, listener) = match &settings {
            None => {
                tracing::info!("single node");
                (NodeId(0), vec![NodeId(0)], HashMap::new(), None)
            }
            Some(settings) => {
                let id = settings.id();
                let ids = settings.ids();
                let peers: HashMap<NodeId, SocketAddr> = ids
                    .iter()
                    .copied()
                    .zip(settings.peers.iter().copied())
                    .collect();
                let listen = settings.listen_addr();
                let listener = tokio::net::TcpListener::bind(listen)
                    .await
                    .with_context(|| format!("cannot bind the consensus listener on {listen}"))?;
                tracing::info!(node = id.0, %listen, nodes = ids.len(), "cluster node");
                (id, ids, peers, Some(listener))
            }
        };
        let has_peers = ids.len() > 1;
        if has_peers && state_dir.is_none() {
            anyhow::bail!("a node with peers needs a state directory (`Builder::state_dir`) for its rejoin mark");
        }

        let mark = match &state_dir {
            Some(dir) => RejoinMark::read(dir)?,
            None => None,
        };
        if let (Some(mark), false) = (mark, has_peers) {
            let dir = state_dir.as_deref().unwrap_or(Path::new("."));
            anyhow::bail!(
                "{} asks for a copy of the store ({mark:?}), but a single node has no peer to copy it from; remove the file to open the store as it is",
                RejoinMark::path(dir).display()
            );
        }

        let journal = match journal {
            Some(journal) => journal,
            None => store
                .open_journal()
                .await
                .context("cannot open the consensus journal")?,
        };
        if let Some(mark) = mark {
            tracing::warn!(?mark, "the store is to be copied from a peer");
            if mark == RejoinMark::DiscardJournal {
                store
                    .discard_journal()
                    .await
                    .context("cannot discard the consensus journal before copying the store")?;
            }
            store
                .empty_for_join()
                .await
                .context("cannot empty the store before copying it")?;
            if let Some(hook) = before_copy {
                hook()
                    .await
                    .context("cannot prepare for the copy of the store")?;
            }
        }

        let mut node_config = settings
            .as_ref()
            .map(Settings::node_config)
            .unwrap_or_else(|| Settings::new(0, Vec::new()).node_config());
        if let Some(adjust) = config {
            node_config = adjust(node_config);
        }

        let mut tasks = Vec::new();
        let (sink, inbox): (Arc<dyn MessageSink>, mpsc::Receiver<Envelope>) = match listener {
            None => {
                let net = InMemoryNetwork::new();
                let inbox = net.register(id);
                (Arc::new(net.sink(id)), inbox)
            }
            Some(listener) => {
                let (inbox_tx, inbox_rx) = mpsc::channel(INBOX_FRAMES);
                tasks.push(serve(listener, inbox_tx));
                (Arc::new(TcpTransport::new(id, peers)), inbox_rx)
            }
        };
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            sink,
            Arc::new(ExecutorDataStore::new(store.clone())) as Arc<dyn DataStore>,
            journal,
        )
        .with_config(node_config);

        // Past here an error ends every task started so far, the listener's
        // first: the next open binds the same address.
        let copy_pending = Arc::new(AtomicBool::new(false));
        if mark.is_some() {
            if let Err(err) = node.recover_state_for_join().await {
                abort_all(tasks).await;
                return Err(err.context("cannot recover the journal before copying the store"));
            }
            node.begin_join();
            tasks.push(node.start(inbox));
            if let Err(err) = join_through(&node, &contacts_of(&ids, id)).await {
                abort_all(tasks).await;
                return Err(
                    err.context("cannot copy the store from a peer; the next start tries again")
                );
            }
            if let Some(Err(err)) = state_dir.as_deref().map(RejoinMark::clear) {
                tracing::warn!(%err, "cannot remove the rejoin mark");
            }
            tracing::info!("store copied from a peer");
        } else {
            match node.recover_state().await {
                Ok(()) => {}
                Err(err) if err.chain().any(|cause| cause.is::<LostJournal>()) => {
                    abort_all(tasks).await;
                    // The journal holds nothing and the store holds events: the
                    // node forgot what it voted and must come back by joining.
                    // The mark is left now, so the next start does.
                    let marked = match &state_dir {
                        Some(dir) => RejoinMark::KeepJournal.write(dir).map(Some),
                        None => Ok(None),
                    };
                    return Err(match marked {
                        Ok(Some(path)) => err.context(format!(
                            "the store's journal does not match it; the store is marked for a copy ({}): restart the node to copy it from a peer",
                            path.display()
                        )),
                        Ok(None) => err.context(
                            "the store's journal does not match it: mark the store for a copy (`Cluster::mark_for_rejoin`) and open the node again",
                        ),
                        Err(mark_err) => err.context(format!(
                            "the store's journal does not match it, and the store could not be marked for a copy: {mark_err}"
                        )),
                    });
                }
                Err(err) => {
                    abort_all(tasks).await;
                    return Err(err);
                }
            }
            tasks.push(node.start(inbox));
        }
        tasks.push(node.start_recovery());
        tasks.push(watch_signals(
            node.clone(),
            state_dir.clone(),
            copy_pending.clone(),
        ));

        let executor = ClusterExecutor::new(node.clone(), store);
        let cluster = Arc::new(Cluster {
            node,
            ids,
            tasks: Mutex::new(tasks),
            joined: mark.is_some(),
            copy_pending,
        });
        Ok((cluster, executor))
    }
}

/// One node of a cluster, running: the [`Node`], its tasks, and what a
/// status page shows of it. Opened with [`Cluster::builder`].
///
/// A node that was away longer than [`Settings::absent_after_secs`] while
/// writes went through is told so when it returns: its peers tidied those
/// writes up without it and cannot send them any more. The node then refuses
/// writes ([`NodeHealth::rejoin_required`]) and the host marks its store for
/// a copy ([`RejoinMark::KeepJournal`]); the next open empties the store,
/// keeps the journal, and opens the node by joining, which copies a peer's
/// store page by page before anything is served. A node whose journal is
/// found to be an older copy of itself is marked the same way, with the
/// journal discarded too ([`RejoinMark::DiscardJournal`]).
/// [`Cluster::mark_for_rejoin`] leaves the mark by hand, for a store that was
/// lost. Either way the operator restarts the node; nothing else.
pub struct Cluster {
    node: Node,
    ids: Vec<NodeId>,
    /// The inbox loop, the recovery sweep, the listener and the signal watch.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    joined: bool,
    copy_pending: Arc<AtomicBool>,
}

impl Cluster {
    /// Opens a node over `store`; see [`Builder`].
    pub fn builder<E: ClusterBackend>(store: E) -> Builder<E> {
        Builder {
            store,
            settings: None,
            state_dir: None,
            config: None,
            journal: None,
            before_copy: None,
        }
    }

    /// Marks the store under `state_dir` for a copy from a peer at the next
    /// open, keeping the journal: for a node whose store file was lost, or
    /// that the others gave up on. Run with the node stopped.
    pub fn mark_for_rejoin(state_dir: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
        RejoinMark::KeepJournal.write(state_dir.as_ref())
    }

    /// The node itself, for what the host does not cover.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// Waits, up to `limit`, until this node takes part: a node opened over
    /// a journal that holds something refuses writes until its peers confirm
    /// the journal is current. A command that writes as soon as it opens
    /// waits here first.
    pub async fn until_confirmed(&self, limit: Duration) -> anyhow::Result<()> {
        let started = tokio::time::Instant::now();
        while !self.node.confirmed() {
            anyhow::ensure!(
                started.elapsed() < limit,
                "this node's peers did not confirm its journal within {limit:?}: are a majority of the nodes running?"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// This node's place among the nodes heard from within `within` (it
    /// counts itself), and how many they are: `(index, of)`. What work
    /// spread over the nodes goes by, so a node that stops is covered for
    /// once it has been silent that long. Idle nodes gossip many times a
    /// second.
    pub fn live_nodes(&self, within: Duration) -> (u64, u64) {
        let me = self.node.id();
        let absent = self.node.absent_peers();
        let mut index = 0;
        let mut of = 0;
        for &id in &self.ids {
            if id == me {
                index = of;
                of += 1;
            } else if self.node.silent_for(id) < within && !absent.contains(&id) {
                of += 1;
            }
        }
        (index, of)
    }

    /// This node, as a status page shows it.
    pub fn health(&self) -> NodeHealth {
        let absent = self.node.absent_peers();
        let me = self.node.id();
        NodeHealth {
            id: me,
            nodes: self.ids.len(),
            backlog: (self.node.command_count(), self.node.config().max_commands),
            peers: self
                .ids
                .iter()
                .filter(|&&id| id != me)
                .map(|&id| PeerState {
                    id,
                    silent_for: self.node.silent_for(id),
                    given_up: absent.contains(&id),
                })
                .collect(),
            confirmed: self.node.confirmed(),
            rejoin_required: self.node.rejoin_required(),
            rolled_back: self.node.rolled_back(),
            copy_pending: self.copy_pending.load(Ordering::Acquire),
            joined_at_start: self.joined,
        }
    }

    /// Whether the store is marked for a copy at the next start, and a
    /// watch that turns `true` when it is: a supervisor restarts the node on
    /// it, an operator reads the log line that goes with it.
    pub fn copy_pending(&self) -> bool {
        self.copy_pending.load(Ordering::Acquire)
    }

    /// Warns, every `every`, when this node keeps four fifths or more of the
    /// writes it may keep for a node that is away (once, until it drops
    /// under half), and about each node this node stopped waiting for (once
    /// per node). The warnings are the operator's alert: a node at its limit
    /// refuses writes.
    pub fn watch(self: &Arc<Self>, every: Duration) -> JoinHandle<()> {
        let cluster = Arc::clone(self);
        tokio::spawn(async move {
            let mut warned_backlog = false;
            let mut warned_peers: HashSet<NodeId> = HashSet::new();
            loop {
                tokio::time::sleep(every).await;
                let health = cluster.health();
                let (kept, limit) = health.backlog;
                if kept * 5 >= limit * 4 {
                    if !warned_backlog {
                        warned_backlog = true;
                        tracing::warn!(
                            kept,
                            limit,
                            "this node keeps most of the writes it may for a node that is away; at the limit it refuses writes"
                        );
                    }
                } else if kept * 2 < limit {
                    warned_backlog = false;
                }
                for peer in &health.peers {
                    if peer.given_up {
                        if warned_peers.insert(peer.id) {
                            tracing::warn!(
                                node = peer.id.0,
                                silent_secs = peer.silent_for.as_secs(),
                                "a node has not been heard from and is no longer waited for"
                            );
                        }
                    } else {
                        warned_peers.remove(&peer.id);
                    }
                }
            }
        })
    }

    /// Stops the node: it mints nothing more, and its tasks are ended. Once
    /// this returns nothing touches the store or the journal.
    pub async fn shutdown(&self) {
        self.node.retire().await;
        let tasks: Vec<JoinHandle<()>> = self
            .tasks
            .lock()
            .expect("tasks poisoned")
            .drain(..)
            .collect();
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }
}

/// Ends `tasks` and waits for them to be gone.
async fn abort_all(tasks: Vec<JoinHandle<()>>) {
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

/// The other nodes, starting with the one after this node, so the nodes of
/// a cluster do not all ask the same peer first.
fn contacts_of(ids: &[NodeId], me: NodeId) -> Vec<NodeId> {
    let at = ids.iter().position(|&id| id == me).unwrap_or(0);
    ids[at + 1..]
        .iter()
        .chain(&ids[..at])
        .copied()
        .filter(|&id| id != me)
        .collect()
}

/// Joins through the contacts in turn: the peer that told this node to
/// rejoin first, when the journal remembers one, since it compacted past
/// what this node holds. A join that installed part of a snapshot stops: the
/// next contact's pages would overlap it and be skipped, leaving a gap.
async fn join_through(node: &Node, contacts: &[NodeId]) -> anyhow::Result<()> {
    let mut order: Vec<NodeId> = Vec::with_capacity(contacts.len() + 1);
    if let Some(first) = node.rejoin_contact() {
        order.push(first);
    }
    for &contact in contacts {
        if !order.contains(&contact) {
            order.push(contact);
        }
    }
    let mut outcome = Err(anyhow::anyhow!("no contact to copy the store from"));
    for contact in order {
        outcome = node.join(contact).await.map(|_| ());
        match &outcome {
            Ok(()) => break,
            Err(err) if err.is::<PartialSnapshot>() => break,
            Err(err) => {
                tracing::warn!(contact = contact.0, %err, "cannot copy the store from this node");
            }
        }
    }
    outcome
}

/// Marks the store for a copy when the node is told to rejoin, or its
/// journal is found rolled back; writes already fail.
fn watch_signals(
    node: Node,
    state_dir: Option<PathBuf>,
    copy_pending: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rejoin = node.rejoin_signal();
        let mut rolled_back = node.rolled_back_signal();
        loop {
            let mark = tokio::select! {
                changed = rejoin.changed() => {
                    if changed.is_err() { return; }
                    if !*rejoin.borrow() { continue; }
                    RejoinMark::KeepJournal
                }
                changed = rolled_back.changed() => {
                    if changed.is_err() { return; }
                    if !*rolled_back.borrow() { continue; }
                    RejoinMark::DiscardJournal
                }
            };
            copy_pending.store(true, Ordering::Release);
            let what = match mark {
                RejoinMark::KeepJournal => {
                    "this node fell behind the other nodes and refuses writes"
                }
                RejoinMark::DiscardJournal => {
                    "this node's journal is an older copy of itself and the node refuses writes"
                }
            };
            match state_dir.as_deref().map(|dir| mark.write(dir)) {
                Some(Ok(path)) => tracing::error!(
                    mark = %path.display(),
                    "{what}; restart it to copy the store from a peer"
                ),
                Some(Err(err)) => tracing::error!(
                    %err,
                    "{what}; the store could not be marked: mark it (`Cluster::mark_for_rejoin`) and restart it"
                ),
                None => tracing::error!(
                    "{what}; mark the store (`Cluster::mark_for_rejoin`) and restart it"
                ),
            }
        }
    })
}
