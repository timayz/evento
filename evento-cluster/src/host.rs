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
//! copy at the next start and saying so in the log. A store marked as the
//! one a new group begins with ([`Cluster::mark_for_founding`]) is opened
//! over an empty journal instead: how a group comes back from one node's
//! store, copied while it served.
//!
//! ```rust,ignore
//! let (cluster, executor) = Cluster::builder(store)
//!     .settings(config.cluster)         // `None`: a single node
//!     .state_dir(&config.data_dir)      // where the rejoin mark lives
//!     .open()
//!     .await?;
//! // `executor` is a `ClusterExecutor<_>`, a drop-in `evento::Executor`;
//! // `executor.local_reads()` is one over the same node whose reads of an
//! // aggregate skip the consensus round, for subscriptions and the reads
//! // that need no read-your-writes; `cluster.health()` is what a status
//! // page shows; `cluster.shutdown()` ends the node.
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
use evento_core::cursor::Args;
use evento_core::{Evento, TenantFilter};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::backend::ClusterBackend;
use crate::clock::{HybridLogicalClock, NodeId};
use crate::executor::{ClusterExecutor, ExecutorDataStore};
use crate::node::{LostJournal, Node, NodeConfig, NotAMember, PartialSnapshot};
use crate::share::Share;
use crate::tcp::{serve, PeerAddr, TcpTransport};
use crate::transport::{Envelope, InMemoryNetwork};
use crate::{addresses, DataStore, DynamicTopology, Journal, MessageSink, Topology};

/// Frames waiting for the node's inbox loop, from the TCP listener.
const INBOX_FRAMES: usize = 1024;

/// How often a node reads where the nodes added since it started are
/// reached ([`addresses`]).
const ADDRESS_LOOK: Duration = Duration::from_secs(1);

/// How long a node new to the cluster waits before it asks again whether a
/// member added it, and how often it says in the log that it waits.
const WAIT_LOOK: Duration = Duration::from_millis(500);
const WAIT_SAID: Duration = Duration::from_secs(30);

/// Times a change of members is proposed again when another change, decided
/// meanwhile, took its epoch.
const CHANGE_ATTEMPTS: usize = 4;

/// The `[cluster]` section of a deployment's configuration: the nodes the
/// cluster is founded with, where they are reached, and the few knobs an
/// operator sets. The members themselves are what the cluster agreed on
/// since ([`Cluster::members`]), kept in each node's journal. Everything else in [`NodeConfig`]
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
///
/// A cluster that is to grow says how many of `peers` it was founded with,
/// and keeps saying it: the nodes listed after them are added to the
/// running cluster ([`Cluster::add_node`]), and a new node that finds itself
/// among those joins, and copies the store, instead of founding a cluster
/// of its own.
///
/// ```toml
/// [cluster]
/// node_id = 3
/// founders = 3
/// peers = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000", "10.0.0.4:7000"]
/// ```
///
/// A peer is `ip:port` or `host:port`. A name is resolved each time the
/// peer is dialled, so a node that comes back at another address under the
/// same name (a StatefulSet pod) is found again, and a name that does not
/// resolve yet does not keep this node from starting. A node named in
/// `peers` by host must set `listen`:
///
/// ```toml
/// [cluster]
/// node_id = 1
/// peers = ["app-0.app:7000", "app-1.app:7000", "app-2.app:7000"]
/// listen = "0.0.0.0:7000"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// This node's index into `peers`.
    pub node_id: u64,
    /// Every node's consensus address, this one included: `NodeId(i)` is
    /// `peers[i]`. One address is a cluster of one node over TCP. Each is
    /// `ip:port` or `host:port` ([`PeerAddr`]); a name is resolved at each
    /// dial, not at start-up.
    ///
    /// Where the nodes are reached, not who counts: the members are the
    /// first [`founders`](Self::founders) of them, then what the cluster
    /// agreed on since ([`Cluster::members`]). A node that left keeps its
    /// place in the list, so the ids after it stay what they are.
    pub peers: Vec<PeerAddr>,
    /// How many of `peers`, the first ones, the cluster is founded with;
    /// unset, all of them. A node listed after them is no member until one
    /// adds it ([`Cluster::add_node`]): opened over an empty store, it waits
    /// for that, then copies the store, and founds nothing. The same on
    /// every node, and for as long as the cluster lives: a cluster founded
    /// with three nodes says 3 however many it has now.
    #[serde(default)]
    pub founders: Option<u64>,
    /// The address to bind for inbound consensus traffic. Defaults to
    /// `peers[node_id]` when that is an address, and is required when it is
    /// a name; set it to `0.0.0.0:<port>` in a container.
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
    /// applies its own writes before the commit returns. The node's default
    /// for the executor [`open`](Builder::open) returns; a read that needs
    /// the other goes through a handle made by
    /// [`ClusterExecutor::local_reads`] or
    /// [`ClusterExecutor::linearizable_reads`].
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
    /// Each peer is a `SocketAddr` or a [`PeerAddr`].
    pub fn new(node_id: u64, peers: impl IntoIterator<Item = impl Into<PeerAddr>>) -> Self {
        Self {
            node_id,
            peers: peers.into_iter().map(Into::into).collect(),
            founders: None,
            listen: None,
            absent_after_secs: default_absent_after_secs(),
            write_backlog: default_write_backlog(),
            linearizable_reads: None,
        }
    }

    /// Refuses settings no node can run on: no peers, a `node_id` outside
    /// `peers`, `founders` of none or of more than `peers`, a
    /// `write_backlog` of zero, no address to bind.
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
        if let Some(founders) = self.founders {
            anyhow::ensure!(
                founders >= 1 && founders as usize <= self.peers.len(),
                "cluster.founders {founders} must be between 1 and the {} peers",
                self.peers.len()
            );
        }
        anyhow::ensure!(
            self.write_backlog > 0,
            "cluster.write_backlog must be greater than 0"
        );
        self.listen_addr()?;
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

    /// The nodes the cluster is founded with: the first
    /// [`founders`](Self::founders) of `peers`.
    pub fn founder_ids(&self) -> Vec<NodeId> {
        let founders = self
            .founders
            .map_or(self.peers.len(), |founders| founders as usize);
        (0..founders.min(self.peers.len()) as u64)
            .map(NodeId)
            .collect()
    }

    /// Whether this node has peers.
    pub fn has_peers(&self) -> bool {
        self.peers.len() > 1
    }

    /// The address this node binds: `listen`, or its own entry in `peers`
    /// when that is an address. An error when it is a name and `listen` is
    /// unset (a name is dialled, not bound), or when `node_id` is outside
    /// `peers`.
    pub fn listen_addr(&self) -> anyhow::Result<SocketAddr> {
        if let Some(listen) = self.listen {
            return Ok(listen);
        }
        let own = self.peers.get(self.node_id as usize).with_context(|| {
            format!(
                "cluster.node_id {} is out of range for {} peers",
                self.node_id,
                self.peers.len()
            )
        })?;
        own.as_socket().with_context(|| {
            format!(
                "cluster.listen must be set: this node's entry in cluster.peers ({own}) is a name, not an address to bind (e.g. listen = \"0.0.0.0:{}\")",
                own.port()
            )
        })
    }

    /// `absent_after_secs` as a duration; `None` for 0.
    pub fn absent_after(&self) -> Option<Duration> {
        (self.absent_after_secs > 0).then(|| Duration::from_secs(self.absent_after_secs))
    }

    /// The [`NodeConfig`] these settings mean: the defaults, with
    /// linearizable reads and the absence limit only where there are peers
    /// to read from or wait for.
    pub fn node_config(&self) -> NodeConfig {
        self.node_config_among(self.has_peers())
    }

    /// [`node_config`](Self::node_config) for a node that `has_peers` or
    /// not, whatever `peers` lists: a cluster founded with one node and
    /// grown since has members its first node's settings do not name.
    fn node_config_among(&self, has_peers: bool) -> NodeConfig {
        let defaults = NodeConfig::default();
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

/// The mark that the store under a state directory is what a **new group**
/// begins with (`<state_dir>/found`, [`Cluster::mark_for_founding`]): the
/// next open discards the journal that came with the store and founds the
/// group over it ([`Node::found_over_store`]). Left on every node of the
/// group, each over the same copy.
struct FoundMark;

impl FoundMark {
    const FILE: &'static str = "found";

    fn path(state_dir: &Path) -> PathBuf {
        state_dir.join(Self::FILE)
    }

    fn present(state_dir: &Path) -> anyhow::Result<bool> {
        let path = Self::path(state_dir);
        path.try_exists()
            .with_context(|| format!("cannot read {}", path.display()))
    }

    fn write(state_dir: &Path) -> anyhow::Result<PathBuf> {
        std::fs::create_dir_all(state_dir)
            .with_context(|| format!("cannot create {}", state_dir.display()))?;
        let path = Self::path(state_dir);
        std::fs::write(&path, "").with_context(|| format!("cannot write {}", path.display()))?;
        Ok(path)
    }

    fn clear(state_dir: &Path) -> anyhow::Result<()> {
        match std::fs::remove_file(Self::path(state_dir)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

/// Another node, as this one sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerState {
    /// Its id.
    pub id: NodeId,
    /// How long since it was last heard from (since this node opened, if
    /// never).
    pub silent_for: Duration,
    /// Whether this node stopped waiting for it ([`Settings::absent_after_secs`]).
    pub given_up: bool,
    /// What it last said about itself ([`Builder::label`]), if anything was
    /// heard from it.
    pub label: Option<String>,
    /// The routing keys it last said its subscriptions follow
    /// ([`Builder::serves`]), `None` among them for the events written
    /// without one; `None` until it said.
    pub keys: Option<Vec<Option<String>>>,
}

/// A node heard from lately, as [`Cluster::live`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveNode {
    /// Its id.
    pub id: NodeId,
    /// What it says about itself ([`Builder::label`]): this node's own label,
    /// or the one a peer last reported.
    pub label: Option<String>,
    /// The routing keys its subscriptions follow ([`Builder::serves`]),
    /// `None` among them for the events written without one. `None` for a
    /// peer heard from that has not said yet: it reports a moment after its
    /// first message.
    pub keys: Option<Vec<Option<String>>>,
}

impl LiveNode {
    /// Whether this node's subscriptions follow the events written under
    /// `key` (`None`: without one). `false` for a node that has not said
    /// what it serves yet.
    pub fn serves(&self, key: Option<&str>) -> bool {
        self.keys
            .as_deref()
            .is_some_and(|keys| keys.iter().any(|served| served.as_deref() == key))
    }
}

/// The members of the cluster, as [`Cluster::members`] reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Members {
    /// How many times the members changed since the cluster was founded,
    /// counting each step of a change.
    pub epoch: u64,
    /// The nodes that count, by id: a majority of them decides.
    pub nodes: Vec<NodeId>,
}

/// This node, for a status page and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealth {
    /// This node's id.
    pub id: NodeId,
    /// Nodes in the cluster as it stands ([`Cluster::members`]), this one
    /// included.
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
    /// This node is of another group than its peers: its store is not the
    /// one their group was founded over ([`Cluster::mark_for_founding`]).
    /// It refuses writes until it is founded again over the group's store,
    /// or copies it back ([`RejoinMark::DiscardJournal`]).
    pub founded_apart: bool,
    /// Whether the store is marked for a copy at the next start.
    pub copy_pending: bool,
    /// Whether this start copied the store from a peer.
    pub joined_at_start: bool,
    /// Whether this start founded a group over the store.
    pub founded_at_start: bool,
}

impl NodeHealth {
    /// Whether the node takes writes: confirmed, and nothing ended its part.
    pub fn serving(&self) -> bool {
        self.confirmed
            && !self.rejoin_required
            && !self.rolled_back
            && !self.founded_apart
            && !self.copy_pending
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
    label: Option<String>,
    serves: Option<Vec<String>>,
    serves_no_key: bool,
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

    /// A short word the node tells its peers about itself with its gossip,
    /// for the application: which region it serves, say. Opaque to the
    /// cluster; what the peers last heard is read back through
    /// [`Cluster::live`] and [`PeerState::label`].
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// The routing keys whose events this node's subscriptions follow, which
    /// it tells its peers with its gossip: its own
    /// ([`Executor::default_routing_key`](evento_core::Executor::default_routing_key))
    /// first, then those it follows besides. Left alone, the node says the
    /// store's default routing key, or that it follows the events written
    /// without one when the store has none; a deployment that sets the
    /// default on a wrapper around the executor
    /// ([`Evento::default_routing_key`](evento_core::Evento::default_routing_key))
    /// names it here.
    ///
    /// The cluster routes nothing by it, and every node holds every event
    /// whatever it says. It is what work shared out by key goes by
    /// ([`Cluster::share_serving`], [`LiveNode::serves`]).
    pub fn serves<K: Into<String>>(mut self, keys: impl IntoIterator<Item = K>) -> Self {
        self.serves = Some(keys.into_iter().map(Into::into).collect());
        self
    }

    /// Says that this node's subscriptions follow the events written without
    /// a routing key too, besides the keys of [`serves`](Self::serves).
    pub fn serves_no_key(mut self) -> Self {
        self.serves_no_key = true;
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
    ///
    /// The executor's reads of one aggregate are linearizable as the
    /// settings say ([`Settings::linearizable_reads`]: on with peers);
    /// [`ClusterExecutor::local_reads`] gives a handle over the same node
    /// that reads locally.
    pub async fn open(self) -> anyhow::Result<(Arc<Cluster>, ClusterExecutor<E>)> {
        let Self {
            store,
            settings,
            state_dir,
            config,
            label,
            serves,
            serves_no_key,
            journal,
            before_copy,
        } = self;
        let mut keys: Vec<Option<String>> = match serves {
            Some(keys) => keys.into_iter().map(Some).collect(),
            None => vec![store.default_routing_key().map(str::to_owned)],
        };
        if serves_no_key && !keys.contains(&None) {
            keys.push(None);
        }
        if let Some(settings) = &settings {
            settings.validate()?;
        }
        let (id, ids, founders, peers, listener) = match &settings {
            None => {
                tracing::info!("single node");
                (
                    NodeId(0),
                    vec![NodeId(0)],
                    vec![NodeId(0)],
                    HashMap::new(),
                    None,
                )
            }
            Some(settings) => {
                let id = settings.id();
                let ids = settings.ids();
                let peers: HashMap<NodeId, PeerAddr> = ids
                    .iter()
                    .copied()
                    .zip(settings.peers.iter().cloned())
                    .collect();
                let listen = settings.listen_addr()?;
                let listener = tokio::net::TcpListener::bind(listen)
                    .await
                    .with_context(|| format!("cannot bind the consensus listener on {listen}"))?;
                tracing::info!(node = id.0, %listen, nodes = ids.len(), "cluster node");
                (id, ids, settings.founder_ids(), peers, Some(listener))
            }
        };
        if ids.len() > 1 && state_dir.is_none() {
            anyhow::bail!("a node with peers needs a state directory (`Builder::state_dir`) for its rejoin mark");
        }

        let journal = match journal {
            Some(journal) => journal,
            None => store
                .open_journal()
                .await
                .context("cannot open the consensus journal")?,
        };
        // The members as the journal holds them: the layout the cluster last
        // agreed on, or the founders' while it agreed on none.
        let decided = journal
            .load_metadata()
            .await
            .context("cannot read the layouts the journal holds")?;
        let members: Vec<NodeId> = match decided.last() {
            Some((_, entry)) => entry.live().shards.iter().flatten().copied().collect(),
            None => founders.clone(),
        };
        let member = members.contains(&id);
        let has_peers = ids.len() > 1 || members.len() > 1;

        let mut mark = match &state_dir {
            Some(dir) => RejoinMark::read(dir)?,
            None => None,
        };
        let found = match &state_dir {
            Some(dir) => FoundMark::present(dir)?,
            None => false,
        };
        if let (Some(mark), true, Some(dir)) = (mark, found, state_dir.as_deref()) {
            anyhow::bail!(
                "{} asks for a copy of the store ({mark:?}) and {} for a group founded over it: remove one of them",
                RejoinMark::path(dir).display(),
                FoundMark::path(dir).display()
            );
        }
        if let (Some(mark), false) = (mark, has_peers) {
            let dir = state_dir.as_deref().unwrap_or(Path::new("."));
            anyhow::bail!(
                "{} asks for a copy of the store ({mark:?}), but a single node has no peer to copy it from; remove the file to open the store as it is",
                RejoinMark::path(dir).display()
            );
        }

        if !member && found {
            anyhow::bail!(
                "node {} is none of the {} nodes the cluster is founded with (`cluster.founders`): a group is founded over a store by those, and this node is added to it afterwards (`Cluster::add_node`)",
                id.0,
                founders.len()
            );
        }
        if !member && mark.is_none() {
            // New to the cluster: it comes in by copying the store, once a
            // member adds it, and founds nothing. Only over a journal and a
            // store that hold nothing: anything else is a node that was
            // removed, or another node's files.
            let pristine = decided.is_empty()
                && journal
                    .load_born()
                    .await
                    .context("cannot read the journal's identity")?
                    .is_none();
            let empty = store
                .read(None, None, TenantFilter::All, Args::forward(1, None), None)
                .await
                .context("cannot read the store")?
                .edges
                .is_empty();
            anyhow::ensure!(
                pristine && empty,
                "node {} is no member of the cluster its journal holds (members: {:?}): it was removed, or `cluster.node_id` is another node's. To bring it back as a new node, mark its store for a copy with its journal discarded (`RejoinMark::DiscardJournal`), start it, and add it on a member (`Cluster::add_node`)",
                id.0,
                members.iter().map(|node| node.0).collect::<Vec<_>>()
            );
            let dir = state_dir.as_deref().context(
                "a node new to a cluster needs a state directory (`Builder::state_dir`) for its rejoin mark",
            )?;
            // Marked first: a restart before the copy is done copies again.
            RejoinMark::KeepJournal.write(dir)?;
            mark = Some(RejoinMark::KeepJournal);
            tracing::info!(
                node = id.0,
                "this node is new to the cluster: it copies the store once a member added it"
            );
        }
        if found {
            // The journal that came with the store is its source's: no node
            // of the new group cast those votes.
            tracing::warn!("a new group is to be founded over the store");
            store
                .discard_journal()
                .await
                .context("cannot discard the journal that came with the store")?;
        }
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
            .map(|settings| settings.node_config_among(has_peers))
            .unwrap_or_else(|| Settings::new(0, Vec::<PeerAddr>::new()).node_config());
        if let Some(adjust) = config {
            node_config = adjust(node_config);
        }

        let mut tasks = Tasks::default();
        let named = peers.len();
        let mut transport = None;
        let (sink, inbox): (Arc<dyn MessageSink>, mpsc::Receiver<Envelope>) = match listener {
            None => {
                let net = InMemoryNetwork::new();
                let inbox = net.register(id);
                (Arc::new(net.sink(id)), inbox)
            }
            Some(listener) => {
                let (inbox_tx, inbox_rx) = mpsc::channel(INBOX_FRAMES);
                tasks.push(serve(listener, inbox_tx));
                let tcp = Arc::new(TcpTransport::new(id, peers));
                transport = Some(tcp.clone());
                (tcp, inbox_rx)
            }
        };
        // The members are what the cluster agreed on, which the journal
        // keeps: the founders are the layout it began with, and every layout
        // decided since is installed over it when the journal is restored.
        let topology: Arc<dyn Topology> =
            Arc::new(DynamicTopology::new(id, 0, vec![founders.clone()]));
        let node = Node::new(
            id,
            topology.clone(),
            Arc::new(HybridLogicalClock::new(id)),
            sink,
            Arc::new(ExecutorDataStore::new(store.clone())) as Arc<dyn DataStore>,
            journal,
        )
        .with_config(node_config)
        .with_label(label)
        .with_serves(Some(keys));

        // Past here an error ends every task started so far, the listener's
        // first: the next open binds the same address.
        let copy_pending = Arc::new(AtomicBool::new(false));
        if mark.is_some() {
            if let Err(err) = node.recover_state_for_join().await {
                abort_all(tasks.take()).await;
                return Err(err.context("cannot recover the journal before copying the store"));
            }
            node.begin_join();
            tasks.push(node.start(inbox));
            // The sweep too: it fetches the layouts decided meanwhile, and
            // copies the slots a change in progress gives this node.
            tasks.push(node.start_recovery());
            // The members the journal knows first, then whoever else the
            // settings name: over a discarded journal the layout is still
            // the founders', some of whom may have left.
            let mut known = topology.nodes();
            for &id in &ids {
                if !known.contains(&id) {
                    known.push(id);
                }
            }
            let contacts = contacts_of(&known, id);
            let mut waited = None;
            loop {
                let err = match join_through(&node, &contacts).await {
                    Ok(()) => break,
                    Err(err) => err,
                };
                // No member yet: nothing was copied, and the join is asked
                // again as it is, for as long as it takes a member to add
                // this node.
                if err.is::<NotAMember>() {
                    if waited.is_none_or(|at: tokio::time::Instant| at.elapsed() >= WAIT_SAID) {
                        waited = Some(tokio::time::Instant::now());
                        tracing::warn!(
                            node = id.0,
                            "this node is no member of the cluster yet: add it on a member (`Cluster::add_node`); it copies the store then"
                        );
                    }
                    tokio::time::sleep(WAIT_LOOK).await;
                    continue;
                }
                abort_all(tasks.take()).await;
                return Err(
                    err.context("cannot copy the store from a peer; the next start tries again")
                );
            }
            if let Some(Err(err)) = state_dir.as_deref().map(RejoinMark::clear) {
                tracing::warn!(%err, "cannot remove the rejoin mark");
            }
            tracing::info!("store copied from a peer");
        } else if found {
            match node.found_over_store().await {
                Ok(foundation) => tracing::info!(
                    group = foundation.group,
                    events = foundation.events,
                    "group founded over the store"
                ),
                Err(err) => {
                    abort_all(tasks.take()).await;
                    return Err(err.context("cannot found a group over the store"));
                }
            }
            // The group is in the journal: from here on an open is plain.
            if let Some(Err(err)) = state_dir.as_deref().map(FoundMark::clear) {
                tracing::warn!(%err, "cannot remove the founding mark");
            }
            tasks.push(node.start(inbox));
        } else {
            match node.recover_state().await {
                Ok(()) => {}
                Err(err) if err.chain().any(|cause| cause.is::<LostJournal>()) => {
                    abort_all(tasks.take()).await;
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
                    abort_all(tasks.take()).await;
                    return Err(err);
                }
            }
            tasks.push(node.start(inbox));
        }
        if mark.is_none() {
            tasks.push(node.start_recovery());
        }
        tasks.push(watch_signals(
            node.clone(),
            state_dir.clone(),
            copy_pending.clone(),
        ));

        let executor = ClusterExecutor::new(node.clone(), store);
        if let Some(transport) = &transport {
            tasks.push(follow_addresses(
                Evento::new(executor.clone()),
                transport.clone(),
                id,
                named,
            ));
        }
        let cluster = Arc::new(Cluster {
            node,
            topology,
            transport,
            // Read from this node's own copy: an address is said again, not
            // refused, when the read trails or no barrier can be had.
            executor: Evento::new(executor.local_reads()),
            named,
            tasks: Mutex::new(tasks.take()),
            joined: mark.is_some(),
            founded: found,
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
    /// The layouts the cluster agreed on, as this node installed them.
    topology: Arc<dyn Topology>,
    /// The links to the other nodes; `None` for a node alone, in memory.
    transport: Option<Arc<TcpTransport>>,
    /// The store, through the cluster: where the addresses of the nodes
    /// added since are written.
    executor: Evento,
    /// How many nodes the settings name, whose addresses they stand for.
    named: usize,
    /// The inbox loop, the recovery sweep, the listener and the signal watch.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    joined: bool,
    founded: bool,
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
            label: None,
            serves: None,
            serves_no_key: false,
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

    /// Marks the store under `state_dir` as what a **new group** begins
    /// with: the next open discards the journal that came with it and
    /// founds the group over its events
    /// ([`Node::found_over_store`]). For bringing a group back from **one
    /// node's store**, copied while the group served (a `VACUUM INTO` of a
    /// Turso or SQLite store, say): stop every node, put the same copy
    /// under each, mark each, start them. The nodes take no part until a
    /// majority of them opened over the same events; one founded over
    /// another copy, or left out, is refused ([`NodeHealth::founded_apart`])
    /// rather than left to diverge. Subscriptions go on from the cursors
    /// the copy holds. The mark is removed once the group is founded.
    ///
    /// What the nodes of the old group agreed on after the copy was taken
    /// is gone, as with any backup. A node of the old group that comes back
    /// later over its own files is refused the same way: mark it for a copy
    /// with its journal discarded ([`RejoinMark::DiscardJournal`]).
    pub fn mark_for_founding(state_dir: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
        FoundMark::write(state_dir.as_ref())
    }

    /// The node itself, for what the host does not cover.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// The members the cluster agreed on, as this node holds them: the
    /// layout it was founded with ([`Settings::peers`]), then every change
    /// decided since, which the journal keeps. A node that was away learns
    /// the changes it missed a moment after it is back.
    pub fn members(&self) -> Members {
        let layout = self.topology.layout();
        Members {
            epoch: layout.epoch(),
            nodes: layout.nodes(),
        }
    }

    /// Adds `node`, reached at `addr`, to the cluster: from here on it
    /// counts, and a majority is one of the members with it. Called on any
    /// member, once; the members agree on it among themselves, and none is
    /// restarted. Returns the members as this node then holds them.
    ///
    /// The new node is a process started with its own id in
    /// [`Settings::node_id`], past the [`founders`](Settings::founders),
    /// over an empty store, before or after this call: its
    /// [`open`](Builder::open) waits to be added, copies the store from a
    /// member, and returns once it serves. Until then the cluster runs one
    /// node short of what it counts: add a node when it is about to start,
    /// not long before. `addr` is where the members dial it, kept in the
    /// store ([`addresses`]); the node itself reaches the members through
    /// its own [`Settings::peers`].
    ///
    /// Said twice, by one member or two, it is done once: a node that is a
    /// member already is left as it is. While another change of members is
    /// in progress ([`remove_node`](Self::remove_node),
    /// [`replace_node`](Self::replace_node)) this one is refused: it is
    /// asked again once that one is done.
    pub async fn add_node(
        &self,
        node: NodeId,
        addr: impl Into<PeerAddr>,
    ) -> anyhow::Result<Members> {
        self.reach(node, addr.into()).await?;
        let members = self
            .change(|members| {
                let mut members = members.to_vec();
                if !members.contains(&node) {
                    members.push(node);
                }
                members
            })
            .await
            .with_context(|| format!("cannot add node {} to the cluster", node.0))?;
        tracing::info!(
            node = node.0,
            epoch = members.epoch,
            "node added to the cluster"
        );
        Ok(members)
    }

    /// Keeps where `node` is reached in the store, for every member, and
    /// dials it there from now on unless this node's settings name it.
    async fn reach(&self, node: NodeId, addr: PeerAddr) -> anyhow::Result<()> {
        let transport = self.transport.as_ref().context(
            "a node opened without settings talks to itself in memory: open it with `Settings` to change its members",
        )?;
        addresses::record(&self.executor, node, &addr).await?;
        if node != self.node.id() && (node.0 as usize) >= self.named {
            transport.set_peer(node, addr);
        }
        Ok(())
    }

    /// Changes the members to what `to` makes of them, through the
    /// cluster: decided once whoever asks, and carried through by the nodes
    /// themselves if this one stops meanwhile. Returns once the change is
    /// decided, installed or still draining.
    async fn change(&self, to: impl Fn(&[NodeId]) -> Vec<NodeId>) -> anyhow::Result<Members> {
        let me = self.node.id();
        let mut refused = None;
        for _ in 0..CHANGE_ATTEMPTS {
            let layout = self.topology.layout();
            anyhow::ensure!(
                layout.shards().len() == 1,
                "the cluster has {} shards: its members are changed shard by shard (`Node::change_topology`)",
                layout.shards().len()
            );
            anyhow::ensure!(
                layout.contains(me),
                "node {} is no member of the cluster: ask a member",
                me.0
            );
            let live = layout.nodes();
            let target = to(&live);
            anyhow::ensure!(!target.is_empty(), "a cluster keeps at least one node");
            match self.node.change_in_progress() {
                // The change asked for, decided already: carried through.
                Some(change) if same_nodes(&change.to.concat(), &target) => {}
                Some(change) => {
                    anyhow::bail!(
                    "another change of members is in progress, to {:?}: ask again once it is done",
                    change.to.concat().iter().map(|node| node.0).collect::<Vec<_>>()
                )
                }
                None if same_nodes(&live, &target) => return Ok(self.members()),
                None => {}
            }
            let epoch = self.node.epoch() + 1;
            let err = match self.node.change_topology(epoch, vec![target.clone()]).await {
                Ok(_) => return Ok(self.members()),
                Err(err) => err,
            };
            // Decided, and still draining: the nodes carry it through.
            let draining = self
                .node
                .change_in_progress()
                .is_some_and(|change| same_nodes(&change.to.concat(), &target));
            if draining || same_nodes(&self.topology.nodes(), &target) {
                return Ok(self.members());
            }
            // Another change may have taken the epoch: looked at again.
            tracing::debug!(node = me.0, epoch, %err, "change of members not decided");
            refused = Some(err);
            tokio::time::sleep(WAIT_LOOK).await;
        }
        Err(refused.unwrap_or_else(|| anyhow::anyhow!("the change of members was not decided")))
    }

    /// Where this node dials `node`: its entry in [`Settings::peers`], or,
    /// for a node the settings do not name, the address the cluster keeps
    /// for it ([`addresses`]), read a moment after it is written. `None`
    /// for a node this one cannot reach, and on a node alone, in memory.
    pub fn address(&self, node: NodeId) -> Option<PeerAddr> {
        self.transport.as_ref()?.peer(node)
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

    /// The nodes heard from within `within`, this one included, by id, each
    /// with what it says about itself ([`Builder::label`],
    /// [`Builder::serves`]). What work spread over the nodes goes by, so a
    /// node that stops is covered for once it has been silent that long.
    /// Idle nodes gossip many times a second.
    pub fn live(&self, within: Duration) -> Vec<LiveNode> {
        let me = self.node.id();
        let absent = self.node.absent_peers();
        self.topology
            .nodes()
            .into_iter()
            .filter_map(|id| {
                if id == me {
                    Some(LiveNode {
                        id,
                        label: self.node.label().map(str::to_owned),
                        keys: self.node.serves().map(<[_]>::to_vec),
                    })
                } else if self.node.silent_for(id) < within && !absent.contains(&id) {
                    Some(LiveNode {
                        id,
                        label: self.node.peer_label(id).map(|label| label.to_string()),
                        keys: self.node.peer_serves(id).map(|keys| keys.to_vec()),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// This node's place among the nodes heard from within `within`
    /// ([`live`](Self::live)), and how many they are: `(index, of)`.
    pub fn live_nodes(&self, within: Duration) -> (u64, u64) {
        let share = self.share(within);
        (share.index, share.of)
    }

    /// This node's part of the work spread over the nodes heard from within
    /// `within` ([`live`](Self::live)): the work every node can do, whatever
    /// it serves.
    pub fn share(&self, within: Duration) -> Share {
        Share::among(self.node.id(), &self.live(within))
    }

    /// This node's part of the work spread over the nodes heard from within
    /// `within` whose subscriptions follow the events written under `key`
    /// (`None`: without one): the work a subscription is told about, which
    /// must fall to a node that sees its event. This node always counts, so
    /// it owns its part of what it alone saw.
    pub fn share_serving(&self, key: Option<&str>, within: Duration) -> Share {
        Share::among_serving(self.node.id(), &self.live(within), key)
    }

    /// This node, as a status page shows it.
    pub fn health(&self) -> NodeHealth {
        let absent = self.node.absent_peers();
        let me = self.node.id();
        let members = self.topology.nodes();
        NodeHealth {
            id: me,
            nodes: members.len(),
            backlog: (self.node.command_count(), self.node.config().max_commands),
            peers: members
                .into_iter()
                .filter(|&id| id != me)
                .map(|id| PeerState {
                    id,
                    silent_for: self.node.silent_for(id),
                    given_up: absent.contains(&id),
                    label: self.node.peer_label(id).map(|label| label.to_string()),
                    keys: self.node.peer_serves(id).map(|keys| keys.to_vec()),
                })
                .collect(),
            confirmed: self.node.confirmed(),
            rejoin_required: self.node.rejoin_required(),
            rolled_back: self.node.rolled_back(),
            founded_apart: self.node.founded_apart(),
            copy_pending: self.copy_pending.load(Ordering::Acquire),
            joined_at_start: self.joined,
            founded_at_start: self.founded,
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

/// The tasks of a node being opened. An open that is dropped before it
/// returns — one waiting to be added to the cluster, given up by the
/// application — ends them, the listener's first: the next open binds the
/// same address.
#[derive(Default)]
struct Tasks(Vec<JoinHandle<()>>);

impl Tasks {
    fn push(&mut self, task: JoinHandle<()>) {
        self.0.push(task);
    }

    /// The tasks, for whoever ends them from here on.
    fn take(&mut self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Whether `a` and `b` are the same nodes, in whatever order.
fn same_nodes(a: &[NodeId], b: &[NodeId]) -> bool {
    let (a, b): (HashSet<NodeId>, HashSet<NodeId>) =
        (a.iter().copied().collect(), b.iter().copied().collect());
    a == b
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
            // Asked again shortly: said once by the caller, not per contact.
            Err(err) if err.is::<NotAMember>() => {}
            Err(err) => {
                tracing::warn!(contact = contact.0, %err, "cannot copy the store from this node");
            }
        }
    }
    outcome
}

/// Keeps the links in line with where the cluster says its nodes are
/// reached ([`addresses`]): a node added since this one started is in no
/// settings of its own. The settings stand for the `named` nodes they
/// name, which an operator moves by restarting this node onto another
/// address.
fn follow_addresses(
    executor: Evento,
    transport: Arc<TcpTransport>,
    me: NodeId,
    named: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut failed = false;
        loop {
            match addresses::known(&executor).await {
                Ok(known) => {
                    failed = false;
                    for (node, addr) in known {
                        if node == me || (node.0 as usize) < named {
                            continue;
                        }
                        if transport.set_peer(node, addr.clone()) {
                            tracing::info!(node = node.0, %addr, "a node is reached at an address the cluster keeps");
                        }
                    }
                }
                // Once per run of failures: the store is read again shortly.
                Err(err) if !failed => {
                    failed = true;
                    tracing::warn!(%err, "cannot read where the nodes are reached");
                }
                Err(_) => {}
            }
            tokio::time::sleep(ADDRESS_LOOK).await;
        }
    })
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
        let mut apart = node.founded_apart_signal();
        loop {
            let mark = tokio::select! {
                changed = apart.changed() => {
                    if changed.is_err() { return; }
                    if *apart.borrow() {
                        // Not marked for a copy: when no majority was founded
                        // alike, a copy would empty the stores the group is
                        // to begin with.
                        tracing::error!(
                            "this node is of another group than its peers and refuses writes: its store is not the one their group was founded over. Put the group's store back and mark it (`Cluster::mark_for_founding`), or mark it for a copy with its journal discarded (`RejoinMark::DiscardJournal`), then restart it"
                        );
                    }
                    continue;
                }
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
