//! A node takes on the routing key of a node that is away (#562).
//!
//! A deployment may give each node a routing key of its own, so a node's
//! subscriptions follow the events that node created. While the node is
//! down those events wait for it; removed for good, they wait for ever,
//! since a stream keeps the key it was created with.
//!
//! [`Cluster::cover`] closes the gap. Each node writes the keys it serves
//! onto a list in the store ([`keys`]) and looks, every
//! [`every`](Cover::every), for the keys on it that no node heard from
//! within [`after`](Cover::after) serves. Those are shared out among the
//! nodes up that serve a key ([`to_cover`]), so each is taken on by one: it
//! starts a copy of every subscription it was given, filtered on that key,
//! and stops them once a node that serves the key is heard from again, or
//! the share moved the key to another node.
//!
//! ```rust,ignore
//! let covering = cluster
//!     .cover(executor.local_reads())
//!     .after(Duration::from_secs(60))
//!     .every(Duration::from_secs(10))
//!     .subscription("payments", |_key| payments())
//!     .subscription("mails", |_key| mails())
//!     .start();
//! // `covering.covered()` is what a status page shows;
//! // `covering.shutdown().await` before `cluster.shutdown()`.
//! ```
//!
//! # What the handlers must bear
//!
//! **Being run on two nodes at once.** Who is up is what each node heard,
//! not something the cluster agreed on: two nodes that disagree for a
//! moment both take a key on, and a node that returns overlaps with the one
//! that covered for it until that one's next look. The handlers of a
//! covered subscription are written for several nodes following the same
//! events (a claim written through the cluster, an effect that is the same
//! done twice), as those of a key two nodes serve already are.
//!
//! **Being run again over the key's history.** A subscription's cursor is
//! local to the node that runs it. The first time a node takes a key on,
//! each subscription goes through that key's events from the start, under a
//! cursor of its own (the one a subscription with
//! [`routing_key`](SubscriptionBuilder::routing_key) set to the key has on
//! that node); the next time it goes on from there. `after` is best kept
//! longer than a node takes to restart for an update, so a restart is not
//! covered for.
//!
//! # Which keys, which nodes
//!
//! - The events written without a routing key are not covered for: a
//!   builder is given a key.
//! - A node that serves no key takes none on, and is not counted in the
//!   share.
//! - Nothing is taken on or handed back while a node heard from has not
//!   said what it serves yet: its key is not without a node.
//! - A key whose node never returns is covered like any other, by whichever
//!   node the share gives it to, and moves when the nodes up change.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_core::subscription::{Subscription, SubscriptionBuilder};
use evento_core::Executor;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::clock::NodeId;
use crate::host::{Cluster, LiveNode};
use crate::keys;
use crate::share::Share;

/// Builds one subscription for the key it is given.
type Make<E> = Arc<dyn Fn(&str) -> SubscriptionBuilder<E> + Send + Sync>;

/// The keys covered now, each with its subscriptions running.
type Covered = BTreeMap<String, Vec<(String, Subscription)>>;

/// The keys of `known` whose subscriptions fall to `me`: those no node of
/// `live` (the nodes heard from, `me` included) serves, shared out among the
/// nodes of `live` that serve a key so each is taken on by one. `None` while
/// a node heard from has not said what it serves yet: what is without a node
/// cannot be told.
pub fn to_cover(me: NodeId, live: &[LiveNode], known: &BTreeSet<String>) -> Option<Vec<String>> {
    if live.iter().any(|node| node.keys.is_none()) {
        return None;
    }
    let takers: Vec<NodeId> = live
        .iter()
        .filter(|node| {
            node.keys
                .as_deref()
                .is_some_and(|keys| keys.iter().any(Option::is_some))
        })
        .map(|node| node.id)
        .collect();
    let Some(index) = takers.iter().position(|id| *id == me) else {
        return Some(Vec::new());
    };
    let share = Share {
        index: index as u64,
        of: takers.len() as u64,
    };
    Some(
        known
            .iter()
            .filter(|key| !live.iter().any(|node| node.serves(Some(key))))
            .filter(|key| share.owns(key))
            .cloned()
            .collect(),
    )
}

/// A key this node covers for, as a status page shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoveredKey {
    /// The routing key no node heard from serves.
    pub key: String,
    /// The subscriptions this node runs on it, by the names they were given
    /// ([`Cover::subscription`]).
    pub subscriptions: Vec<String>,
}

/// What takes on the keys without a node: see [`Cluster::cover`].
pub struct Cover<E: Executor + Clone> {
    cluster: Arc<Cluster>,
    executor: E,
    after: Duration,
    every: Duration,
    subscriptions: Vec<(String, Make<E>)>,
}

impl Cluster {
    /// Has this node take on the subscriptions of the routing keys no node
    /// up serves, and hand them back: see the [module](crate::cover) for
    /// what that asks of the handlers.
    ///
    /// `executor` is what the subscriptions run on and where the list of
    /// keys is read: this node's [`ClusterExecutor`](crate::ClusterExecutor),
    /// or a wrapper around it; its
    /// [`local_reads`](crate::ClusterExecutor::local_reads) handle, as for
    /// the node's own subscriptions. This node's keys
    /// ([`Builder::serves`](crate::host::Builder::serves)) are written onto
    /// the list through it.
    pub fn cover<E: Executor + Clone>(self: &Arc<Self>, executor: E) -> Cover<E> {
        Cover {
            cluster: Arc::clone(self),
            executor,
            after: Duration::from_secs(60),
            every: Duration::from_secs(10),
            subscriptions: Vec::new(),
        }
    }
}

impl<E: Executor + Clone> Cover<E> {
    /// How long a key goes without a node heard from before another takes
    /// it on (60 s unless set). Longer than a node takes to restart: the
    /// first time a node takes a key on, each subscription goes through
    /// that key's history. And well above the silences between two nodes
    /// that are both up, which reach seconds on a disk under load (a node
    /// reports once its journal is flushed): under them two nodes take each
    /// other for gone, and both take a key on.
    pub fn after(mut self, after: Duration) -> Self {
        self.after = after;
        self
    }

    /// How often the node looks for keys to take on or hand back (10 s
    /// unless set).
    pub fn every(mut self, every: Duration) -> Self {
        self.every = every;
        self
    }

    /// A subscription to run on each key taken on: `make` builds it for the
    /// key, as the node that serves the key builds its own, and the cover
    /// filters it on the key
    /// ([`routing_key`](SubscriptionBuilder::routing_key)). `name` is what
    /// [`Covering::covered`] and the log call it.
    pub fn subscription(
        mut self,
        name: impl Into<String>,
        make: impl Fn(&str) -> SubscriptionBuilder<E> + Send + Sync + 'static,
    ) -> Self {
        self.subscriptions.push((name.into(), Arc::new(make)));
        self
    }

    /// Starts looking. The handle stops it, and the subscriptions it
    /// started with it.
    pub fn start(self) -> Covering {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let covered = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn(self.run(shutdown_rx, Arc::clone(&covered)));
        Covering {
            shutdown_tx,
            task,
            covered,
        }
    }

    async fn run(self, mut shutdown: watch::Receiver<bool>, shown: Arc<Mutex<Vec<CoveredKey>>>) {
        // This node's keys not on the list yet.
        let mut unsaid: Vec<String> = self
            .cluster
            .node()
            .serves()
            .unwrap_or_default()
            .iter()
            .flatten()
            .cloned()
            .collect();
        let mut covered = Covered::new();
        let mut tick = tokio::time::interval(self.every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                // Stopped, or the handle was dropped.
                _ = shutdown.changed() => break,
            }
            // Refused while this node reaches no majority, as at a start
            // with the others: said at the next look.
            let mut still_unsaid = Vec::new();
            for key in unsaid.drain(..) {
                if let Err(err) = keys::record(&self.executor, &key).await {
                    tracing::warn!(
                        %key,
                        error = %format!("{err:#}"),
                        "this node's routing key is not on the list of keys yet"
                    );
                    still_unsaid.push(key);
                }
            }
            unsaid = still_unsaid;
            if let Err(err) = self.look(&mut covered).await {
                tracing::error!(
                    error = %format!("{err:#}"),
                    "the routing keys without a node were not looked for"
                );
            }
            *shown.lock().expect("covered keys poisoned") = covered
                .iter()
                .map(|(key, subscriptions)| CoveredKey {
                    key: key.clone(),
                    subscriptions: subscriptions.iter().map(|(name, _)| name.clone()).collect(),
                })
                .collect();
        }
        for (_, subscriptions) in std::mem::take(&mut covered) {
            stop(subscriptions).await;
        }
        shown.lock().expect("covered keys poisoned").clear();
    }

    /// Takes on the keys that fall to this node and hands back the others
    /// it had.
    async fn look(&self, covered: &mut Covered) -> anyhow::Result<()> {
        let metrics = self.cluster.node().metrics_handle();
        // A subscription that stopped on its own (a handler failed) leaves
        // its key half covered: the key is let go, and taken on again below
        // if it still falls to this node.
        let failed: Vec<String> = covered
            .iter()
            .filter(|(_, subscriptions)| subscriptions.iter().any(|(_, sub)| sub.is_finished()))
            .map(|(key, _)| key.clone())
            .collect();
        for key in failed {
            let subscriptions = covered.remove(&key).unwrap_or_default();
            for (name, subscription) in &subscriptions {
                if let Some(reason) = subscription.stop_reason() {
                    tracing::error!(%key, subscription = %name, %reason, "a subscription run for a routing key without a node stopped on its own");
                }
            }
            stop(subscriptions).await;
            metrics.record_cover_hand_back();
        }

        let known = keys::known(&self.executor).await?;
        let live = self.cluster.live(self.after);
        let Some(wanted) = to_cover(self.cluster.id(), &live, &known) else {
            return Ok(());
        };
        let handed_back: Vec<String> = covered
            .keys()
            .filter(|key| !wanted.contains(key))
            .cloned()
            .collect();
        for key in handed_back {
            stop(covered.remove(&key).unwrap_or_default()).await;
            metrics.record_cover_hand_back();
            tracing::info!(%key, "a node serves this routing key again, or another took it on: its subscriptions are handed back");
        }
        for key in wanted {
            if covered.contains_key(&key) {
                continue;
            }
            match self.take_on(&key).await {
                Ok(subscriptions) => {
                    metrics.record_cover_take();
                    tracing::warn!(%key, "no node heard from serves this routing key: this node takes its subscriptions on");
                    covered.insert(key, subscriptions);
                }
                Err(err) => tracing::error!(
                    %key,
                    error = %format!("{err:#}"),
                    "the subscriptions of a routing key without a node were not taken on; tried again at the next look"
                ),
            }
        }
        Ok(())
    }

    /// Starts every subscription on `key`'s events. All of them or none: a
    /// start that failed stops those before it.
    async fn take_on(&self, key: &str) -> anyhow::Result<Vec<(String, Subscription)>> {
        let mut started = Vec::with_capacity(self.subscriptions.len());
        for (name, make) in &self.subscriptions {
            match make(key).routing_key(key).start(&self.executor).await {
                Ok(subscription) => started.push((name.clone(), subscription)),
                Err(err) => {
                    stop(started).await;
                    return Err(err.context(format!("cannot start {name} for {key}")));
                }
            }
        }
        Ok(started)
    }
}

/// Stops `subscriptions` and waits for each to be done with the event it
/// was handling.
async fn stop(subscriptions: Vec<(String, Subscription)>) {
    for (_, subscription) in &subscriptions {
        subscription.stop();
    }
    for (_, subscription) in subscriptions {
        let _ = subscription.shutdown().await;
    }
}

/// The cover, running: what [`Cover::start`] returns. Dropped, it stops as
/// with [`shutdown`](Self::shutdown), without being waited for.
pub struct Covering {
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
    covered: Arc<Mutex<Vec<CoveredKey>>>,
}

impl Covering {
    /// The keys this node covers for now, as of its last look.
    pub fn covered(&self) -> Vec<CoveredKey> {
        self.covered.lock().expect("covered keys poisoned").clone()
    }

    /// Stops looking, and the subscriptions run for the keys covered, each
    /// once it is done with the event it was handling. Before
    /// [`Cluster::shutdown`], which the subscriptions need the node for.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(nodes: &[(u64, Option<&[Option<&str>]>)]) -> Vec<LiveNode> {
        nodes
            .iter()
            .map(|(id, keys)| LiveNode {
                id: NodeId(*id),
                label: None,
                keys: keys.map(|keys| keys.iter().map(|key| key.map(str::to_owned)).collect()),
            })
            .collect()
    }

    fn known(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|key| key.to_string()).collect()
    }

    /// The nodes of `live` that take `key` on.
    fn takers(live: &[LiveNode], keys: &BTreeSet<String>, key: &str) -> Vec<u64> {
        live.iter()
            .filter(|node| {
                to_cover(node.id, live, keys)
                    .unwrap_or_default()
                    .iter()
                    .any(|k| k == key)
            })
            .map(|node| node.id.0)
            .collect()
    }

    #[test]
    fn nothing_is_taken_on_while_every_key_has_a_node() {
        let live = live(&[
            (0, Some(&[Some("a")])),
            (1, Some(&[Some("b")])),
            (2, Some(&[Some("c")])),
        ]);
        let keys = known(&["a", "b", "c"]);
        for node in 0..3 {
            assert_eq!(to_cover(NodeId(node), &live, &keys), Some(Vec::new()));
        }
    }

    /// One node, and one only, of those left takes on the key of the node
    /// that is away; a key a node left for good is one more such.
    #[test]
    fn one_of_the_nodes_left_takes_a_key_without_a_node() {
        let live = live(&[(0, Some(&[Some("a")])), (2, Some(&[Some("c")]))]);
        let keys = known(&["a", "b", "c", "gone"]);
        assert_eq!(takers(&live, &keys, "b").len(), 1);
        assert_eq!(takers(&live, &keys, "gone").len(), 1);
        assert!(takers(&live, &keys, "a").is_empty());
        assert!(takers(&live, &keys, "c").is_empty());
    }

    /// A key another node serves besides its own has a node while that one
    /// is up.
    #[test]
    fn a_key_also_served_by_a_node_up_is_left_alone() {
        let live = live(&[(0, Some(&[Some("a")])), (2, Some(&[Some("c"), Some("b")]))]);
        assert!(takers(&live, &known(&["a", "b", "c"]), "b").is_empty());
    }

    /// A node heard from that has not said its keys yet may be the one
    /// serving a key: nothing can be told until it has.
    #[test]
    fn nothing_is_told_before_every_node_heard_from_said_its_keys() {
        let live = live(&[(0, Some(&[Some("a")])), (1, None)]);
        assert_eq!(to_cover(NodeId(0), &live, &known(&["a", "b", "c"])), None);
    }

    /// A node that serves no key takes none on, and is not counted among
    /// those that do: a key is not left waiting for it.
    #[test]
    fn a_node_without_a_key_takes_none_on() {
        let live = live(&[
            (0, Some(&[None])),
            (1, Some(&[Some("b")])),
            (2, Some(&[Some("c")])),
        ]);
        let keys = known(&["a", "b", "c", "d", "e", "f"]);
        assert_eq!(to_cover(NodeId(0), &live, &keys), Some(Vec::new()));
        for key in ["a", "d", "e", "f"] {
            assert_eq!(takers(&live, &keys, key).len(), 1, "{key}");
        }
    }

    /// A node alone over a store other nodes wrote, as after a restore from
    /// one node's copy, takes every other key on.
    #[test]
    fn a_node_alone_takes_every_other_key_on() {
        let live = live(&[(0, Some(&[Some("a")]))]);
        assert_eq!(
            to_cover(NodeId(0), &live, &known(&["a", "b", "c"])),
            Some(vec!["b".to_owned(), "c".to_owned()])
        );
    }
}
