//! A node takes on the routing key of a node that is away (#562), one node
//! at a time (#565).
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
//! nodes up that serve a key ([`to_cover`]), and the node a key falls to
//! takes its **lease** ([`lease`]): a write through the cluster, which one
//! node wins. The holder starts a copy of every subscription it was given,
//! filtered on that key, and stops them once a node that serves the key is
//! heard from again, or another node took the lease.
//!
//! ```rust,ignore
//! let covering = cluster
//!     .cover(executor.local_reads())
//!     .after(Duration::from_secs(60))
//!     .every(Duration::from_secs(10))
//!     .lease(Duration::from_secs(30))
//!     .own_keys()
//!     .subscription("payments", |_key| payments())
//!     .subscription("mails", |_key| mails())
//!     .start();
//! // `covering.covered()` is what a status page shows;
//! // `covering.shutdown().await` before `cluster.shutdown()`.
//! ```
//!
//! # One node at a time
//!
//! Who is up is what each node heard, not something the cluster agreed on:
//! two nodes that disagree for a moment both decide a key is theirs. The
//! lease is what they agree on. A node runs a key's subscriptions only
//! while it holds the key's lease:
//!
//! - every [`every`](Cover::every) the holder asks the cluster whether the
//!   lease is still its own, a question only a majority answers
//!   ([`lease::confirmed`]). Told yes, it is in its right for
//!   [`lease`](Cover::lease) from the moment it asked; told another node
//!   took the key, it stops; not answered by then (it is cut off), it stops
//!   until it is answered again;
//! - a node that takes the lease from another waits
//!   [`lease`](Cover::lease) before it starts: the time the node before it
//!   may still think itself in its right.
//!
//! A lease stays with its holder while that node is up and may run the key:
//! the share only says who takes a lease that is free, so a key does not
//! move each time the nodes up change.
//!
//! With [`own_keys`](Cover::own_keys) the keys a node serves go through the
//! same lease: the node runs the subscriptions on its own keys from here
//! too, and a node that returns takes its key back and waits for the node
//! that covered for it to be done. Without it a node's own subscriptions
//! are started by the application, as soon as the node is up, and overlap
//! with its cover until that one hears the node again.
//!
//! # What the handlers must bear
//!
//! **The event in hand when the lease is lost.** A subscription is stopped
//! between two events: the handler running when its node's time is up runs
//! to its end, however long it takes. What it writes through the cluster
//! then is not fenced by the lease, and what it does outside the store (a
//! mail sent, a call made) cannot be. The lease also counts on the nodes'
//! clocks running at the same pace, as any lease does.
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
//!   node the share gave it to, until that node is away too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_core::subscription::{Subscription, SubscriptionBuilder};
use evento_core::Executor;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::clock::{system_micros, NodeId};
use crate::host::{Cluster, LiveNode};
use crate::keys;
use crate::lease;
use crate::share::Share;

/// Builds one subscription for the key it is given.
type Make<E> = Arc<dyn Fn(&str) -> SubscriptionBuilder<E> + Send + Sync>;

/// The keys whose subscriptions run now, as [`Covering::covered`] shows
/// them.
type Shown = Arc<Mutex<BTreeMap<String, CoveredKey>>>;

/// The nodes of `live` (the nodes heard from, by id) that may run `key`'s
/// subscriptions. While a node of `live` serves the key: the nodes that
/// serve it when they run their own keys from the cover (`own`,
/// [`Cover::own_keys`]), else none, the key being its node's to follow.
/// While none does: the nodes that serve a key.
pub fn entitled(live: &[LiveNode], key: &str, own: bool) -> Vec<NodeId> {
    let serving: Vec<NodeId> = live
        .iter()
        .filter(|node| node.serves(Some(key)))
        .map(|node| node.id)
        .collect();
    if !serving.is_empty() {
        return if own { serving } else { Vec::new() };
    }
    live.iter()
        .filter(|node| {
            node.keys
                .as_deref()
                .is_some_and(|keys| keys.iter().any(Option::is_some))
        })
        .map(|node| node.id)
        .collect()
}

/// The one of `entitled` that takes `key` while its lease is free: the
/// same on every node that heard from the same nodes.
pub fn preferred(entitled: &[NodeId], key: &str) -> Option<NodeId> {
    let of = entitled.len() as u64;
    entitled
        .iter()
        .enumerate()
        .find(|(index, _)| {
            Share {
                index: *index as u64,
                of,
            }
            .owns(key)
        })
        .map(|(_, id)| *id)
}

/// The keys of `known` whose subscriptions fall to `me`: those no node of
/// `live` (the nodes heard from, `me` included) serves, shared out among the
/// nodes of `live` that serve a key so each is taken on by one. `None` while
/// a node heard from has not said what it serves yet: what is without a node
/// cannot be told.
pub fn to_cover(me: NodeId, live: &[LiveNode], known: &BTreeSet<String>) -> Option<Vec<String>> {
    if live.iter().any(|node| node.keys.is_none()) {
        return None;
    }
    Some(
        known
            .iter()
            .filter(|key| preferred(&entitled(live, key, false), key) == Some(me))
            .cloned()
            .collect(),
    )
}

/// A key whose subscriptions this node runs, as a status page shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoveredKey {
    /// The routing key.
    pub key: String,
    /// Whether this node serves the key ([`Cover::own_keys`]); `false` for a
    /// key it covers for.
    pub own: bool,
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
    lease: Duration,
    own: bool,
    subscriptions: Vec<(String, Make<E>)>,
}

impl Cluster {
    /// Has this node take on the subscriptions of the routing keys no node
    /// up serves, and hand them back: see the [module](crate::cover) for
    /// what that asks of the handlers.
    ///
    /// `executor` is what the subscriptions run on and where the list of
    /// keys and the leases are read: this node's
    /// [`ClusterExecutor`](crate::ClusterExecutor), or a wrapper around it;
    /// its [`local_reads`](crate::ClusterExecutor::local_reads) handle, as
    /// for the node's own subscriptions. This node's keys
    /// ([`Builder::serves`](crate::host::Builder::serves)) and the leases it
    /// takes are written through it.
    pub fn cover<E: Executor + Clone>(self: &Arc<Self>, executor: E) -> Cover<E> {
        Cover {
            cluster: Arc::clone(self),
            executor,
            after: Duration::from_secs(60),
            every: Duration::from_secs(10),
            lease: Duration::from_secs(30),
            own: false,
            subscriptions: Vec::new(),
        }
    }
}

impl<E: Executor + Clone> Cover<E> {
    /// How long a key goes without a node heard from before another takes
    /// it on (60 s unless set). Longer than a node takes to restart: the
    /// first time a node takes a key on, each subscription goes through
    /// that key's history. And above the silences between two nodes that
    /// are both up, which reach seconds on a disk under load (a node
    /// reports once its journal is flushed): under them a key is taken from
    /// a node that is up, which then stops and waits for it back.
    pub fn after(mut self, after: Duration) -> Self {
        self.after = after;
        self
    }

    /// How often the node looks for keys to take on or hand back, and asks
    /// whether the leases it holds are still its own (10 s unless set).
    pub fn every(mut self, every: Duration) -> Self {
        self.every = every;
        self
    }

    /// How long a holder is in its right after the cluster told it the
    /// lease is its own, and how long a node that takes a lease from
    /// another waits before it starts (30 s unless set). A few
    /// [`every`](Self::every)s: a holder that misses one answer is not out
    /// of time yet. The same on every node.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Runs the subscriptions on the keys this node serves too, each under
    /// its lease like a key covered for: the application starts none on
    /// them itself. A node that returns then takes its key back and starts
    /// once the node that covered for it is done; of several nodes serving
    /// one key, one runs its subscriptions at a time. Set on every node, or
    /// on none.
    pub fn own_keys(mut self) -> Self {
        self.own = true;
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
        let covered = Shown::default();
        let task = tokio::spawn(self.run(shutdown_rx, Arc::clone(&covered)));
        Covering {
            shutdown_tx,
            task,
            covered,
        }
    }

    async fn run(self, mut shutdown: watch::Receiver<bool>, shown: Shown) {
        let Cover {
            cluster,
            executor,
            after,
            every,
            lease,
            own,
            subscriptions,
        } = self;
        let served: Vec<String> = cluster
            .node()
            .serves()
            .unwrap_or_default()
            .iter()
            .flatten()
            .cloned()
            .collect();
        // This node's keys not on the list yet.
        let mut unsaid = served.clone();
        let keeping = Arc::new(Keeping {
            cluster,
            executor,
            every,
            lease,
            subscriptions,
            shown,
        });
        let looking = Looking { after, own, served };
        let mut keepers = BTreeMap::new();
        let mut tick = tokio::time::interval(every);
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
                if let Err(err) = keys::record(&keeping.executor, &key).await {
                    tracing::warn!(
                        %key,
                        error = %format!("{err:#}"),
                        "this node's routing key is not on the list of keys yet"
                    );
                    still_unsaid.push(key);
                }
            }
            unsaid = still_unsaid;
            if let Err(err) = keeping.look(&looking, &mut keepers).await {
                tracing::error!(
                    error = %format!("{err:#}"),
                    "the routing keys without a node were not looked for"
                );
            }
        }
        for (_, keeper) in keepers {
            keeper.release().await;
        }
    }
}

/// What a look goes by, besides what [`Keeping`] holds.
struct Looking {
    after: Duration,
    own: bool,
    /// The keys this node serves.
    served: Vec<String>,
}

/// A key this node holds the lease of, or waits to: the task that keeps the
/// lease confirmed and runs the key's subscriptions under it.
struct Keeper {
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Keeper {
    /// Stops the key's subscriptions, each once it is done with the event
    /// it was handling. The lease stays in this node's name until another
    /// takes it.
    async fn release(self) {
        let _ = self.release.send(true);
        let _ = self.task.await;
    }
}

/// What the look and the keepers share.
struct Keeping<E: Executor + Clone> {
    cluster: Arc<Cluster>,
    executor: E,
    every: Duration,
    lease: Duration,
    subscriptions: Vec<(String, Make<E>)>,
    shown: Shown,
}

impl<E: Executor + Clone> Keeping<E> {
    /// Takes the leases that fall to this node and lets go of the keys it
    /// may no longer run.
    async fn look(
        self: &Arc<Self>,
        looking: &Looking,
        keepers: &mut BTreeMap<String, Keeper>,
    ) -> anyhow::Result<()> {
        // A keeper ends on its own when another node took the lease, or a
        // subscription stopped on its own: the key is looked at afresh.
        keepers.retain(|_, keeper| !keeper.task.is_finished());

        let mut keys = keys::known(&self.executor).await?;
        let live = self.cluster.live(looking.after);
        if live.iter().any(|node| node.keys.is_none()) {
            return Ok(());
        }
        if looking.own {
            keys.extend(looking.served.iter().cloned());
        }
        keys.extend(keepers.keys().cloned());
        let me = self.cluster.id();
        for key in keys {
            let entitled = entitled(&live, &key, looking.own);
            if !entitled.contains(&me) {
                if let Some(keeper) = keepers.remove(&key) {
                    keeper.release().await;
                    tracing::info!(%key, "a node serves this routing key again: its subscriptions are handed back");
                }
                continue;
            }
            if keepers.contains_key(&key) {
                continue;
            }
            let own = looking.served.contains(&key);
            match self.claim(&key, own, &entitled).await {
                Ok(Some(keeper)) => {
                    keepers.insert(key, keeper);
                }
                Ok(None) => {}
                Err(err) => tracing::error!(
                    %key,
                    error = %format!("{err:#}"),
                    "the lease of a routing key was not taken; tried again at the next look"
                ),
            }
        }
        Ok(())
    }

    /// Has this node keep `key` if its lease is this node's, or free and
    /// this node's to take. `entitled` are the nodes that may run the key,
    /// this one among them.
    async fn claim(
        self: &Arc<Self>,
        key: &str,
        own: bool,
        entitled: &[NodeId],
    ) -> anyhow::Result<Option<Keeper>> {
        let me = self.cluster.id();
        let held = lease::held(&self.executor, key).await?;
        let now = Instant::now();
        let (term, start_at) = match held {
            // Taken by this node before: by this process, which let the key
            // go since, or by one before a restart, which may have been
            // waiting for the holder before it when it stopped. A term
            // taken lately is waited for in full: its stamp is when it was
            // asked for, and the holder before it was in its right until
            // the term was agreed, some time later.
            Some(lease) if lease.holder == me => {
                let age = Duration::from_micros(system_micros().saturating_sub(lease.taken_at));
                let wait = match lease.term {
                    1 => Duration::ZERO,
                    _ if age >= self.lease * 2 => Duration::ZERO,
                    _ => self.lease,
                };
                (lease.term, now + wait)
            }
            // Its holder is up and may run it: the key stays there.
            Some(lease) if entitled.contains(&lease.holder) => return Ok(None),
            // Free: one node takes it.
            _ if preferred(entitled, key) != Some(me) => return Ok(None),
            _ => match lease::take(&self.executor, key, me, held).await? {
                Some(taken) => {
                    tracing::info!(%key, term = taken.term, "this node took the lease of a routing key");
                    // The holder before may think itself in its right for
                    // as long as a lease lasts.
                    let wait = held.map_or(Duration::ZERO, |_| self.lease);
                    (taken.term, Instant::now() + wait)
                }
                // Another node took it meanwhile.
                None => return Ok(None),
            },
        };
        let (release, released) = watch::channel(false);
        let task =
            tokio::spawn(Arc::clone(self).keep(key.to_owned(), own, term, start_at, released));
        Ok(Some(Keeper { release, task }))
    }

    /// Keeps `key` under the term `term` of its lease, this node's: asks
    /// every [`every`](Cover::every) whether it still is, and runs the
    /// key's subscriptions from `start_at` on while the last yes is not
    /// older than [`lease`](Cover::lease). Ends when another node holds the
    /// lease, a subscription stopped on its own, or the key is released.
    async fn keep(
        self: Arc<Self>,
        key: String,
        own: bool,
        term: u16,
        start_at: Instant,
        mut released: watch::Receiver<bool>,
    ) {
        let me = self.cluster.id();
        let node = self.cluster.node();
        let metrics = node.metrics_handle();
        let mut running: Vec<(String, Subscription)> = Vec::new();
        // Until when this node is in its right.
        let mut valid_until: Option<Instant> = None;
        loop {
            // A subscription that stopped on its own (a handler failed)
            // leaves its key half run: the key is let go, and kept again at
            // the next look.
            if let Some((name, reason)) = running
                .iter()
                .find_map(|(name, sub)| Some((name, sub.stop_reason()?)))
            {
                tracing::error!(%key, subscription = %name, %reason, "a subscription run under the lease of a routing key stopped on its own");
                break;
            }

            let asked = Instant::now();
            let out_of_time = async {
                match valid_until.filter(|_| !running.is_empty()) {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let answer = tokio::select! {
                answer = lease::confirmed(node, &self.executor, &key) => Some(answer),
                _ = out_of_time => None,
                _ = released.changed() => break,
            };
            match answer {
                Some(Ok(Some(lease))) if lease.holder == me && lease.term == term => {
                    valid_until = Some(asked + self.lease);
                    if running.is_empty() && Instant::now() >= start_at {
                        match self.take_on(&key).await {
                            Ok(subscriptions) => {
                                metrics.record_cover_take();
                                if own {
                                    tracing::info!(%key, term, "this node holds the lease of its routing key: its subscriptions run");
                                } else {
                                    tracing::warn!(%key, term, "no node heard from serves this routing key: this node runs its subscriptions");
                                }
                                self.show(&key, own, &subscriptions);
                                running = subscriptions;
                            }
                            Err(err) => tracing::error!(
                                %key,
                                error = %format!("{err:#}"),
                                "the subscriptions of a routing key were not started; tried again in a moment"
                            ),
                        }
                    }
                }
                Some(Ok(lease)) => {
                    tracing::info!(%key, term, holder = ?lease.map(|lease| lease.holder), "another node took the lease of this routing key");
                    break;
                }
                // Not answered: cut off, or the cluster is slow. This node
                // goes on while its time lasts.
                Some(Err(err)) => tracing::debug!(
                    %key,
                    error = %format!("{err:#}"),
                    "the lease of a routing key was not confirmed"
                ),
                None => {}
            }
            if !running.is_empty() && valid_until.is_none_or(|at| Instant::now() >= at) {
                tracing::warn!(%key, term, "the lease of this routing key was not confirmed in time: its subscriptions stop until it is");
                metrics.record_cover_lapse();
                self.let_go(&key, std::mem::take(&mut running)).await;
                continue;
            }

            let mut wake = Instant::now() + self.every;
            if running.is_empty() {
                if start_at > Instant::now() {
                    wake = wake.min(start_at);
                }
            } else if let Some(at) = valid_until {
                wake = wake.min(at);
            }
            tokio::select! {
                _ = tokio::time::sleep_until(wake) => {}
                _ = released.changed() => break,
            }
        }
        self.let_go(&key, running).await;
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

    fn show(&self, key: &str, own: bool, subscriptions: &[(String, Subscription)]) {
        self.shown.lock().expect("covered keys poisoned").insert(
            key.to_owned(),
            CoveredKey {
                key: key.to_owned(),
                own,
                subscriptions: subscriptions.iter().map(|(name, _)| name.clone()).collect(),
            },
        );
    }

    /// Stops the subscriptions run on `key`, if any.
    async fn let_go(&self, key: &str, running: Vec<(String, Subscription)>) {
        if running.is_empty() {
            return;
        }
        stop(running).await;
        self.shown
            .lock()
            .expect("covered keys poisoned")
            .remove(key);
        self.cluster
            .node()
            .metrics_handle()
            .record_cover_hand_back();
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
    covered: Shown,
}

impl Covering {
    /// The keys whose subscriptions this node runs now: those it covers
    /// for, and its own with [`Cover::own_keys`].
    pub fn covered(&self) -> Vec<CoveredKey> {
        self.covered
            .lock()
            .expect("covered keys poisoned")
            .values()
            .cloned()
            .collect()
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

    fn ids(nodes: &[u64]) -> Vec<NodeId> {
        nodes.iter().copied().map(NodeId).collect()
    }

    /// A key whose node is up is that node's: no other may run it, and the
    /// node itself only when it runs its own keys from the cover.
    #[test]
    fn a_key_with_a_node_up_is_run_by_the_nodes_that_serve_it() {
        let live = live(&[
            (0, Some(&[Some("a")])),
            (1, Some(&[Some("b"), Some("a")])),
            (2, Some(&[Some("c")])),
        ]);
        assert!(entitled(&live, "a", false).is_empty());
        assert_eq!(entitled(&live, "a", true), ids(&[0, 1]));
        assert_eq!(entitled(&live, "c", true), ids(&[2]));
    }

    /// A key without a node may be run by any node that serves a key,
    /// whether or not the nodes run their own keys from the cover.
    #[test]
    fn a_key_without_a_node_is_run_by_a_node_that_serves_a_key() {
        let live = live(&[
            (0, Some(&[Some("a")])),
            (1, Some(&[None])),
            (2, Some(&[Some("c")])),
        ]);
        for own in [false, true] {
            assert_eq!(entitled(&live, "b", own), ids(&[0, 2]));
        }
    }

    /// One node takes a lease that is free, the same whoever asks.
    #[test]
    fn a_free_lease_goes_to_one_of_the_nodes_that_may_run_the_key() {
        let nodes = ids(&[0, 2, 5]);
        for key in ["a", "b", "c", "d", "e"] {
            let to = preferred(&nodes, key).unwrap();
            assert!(nodes.contains(&to), "{key}");
        }
        assert_eq!(preferred(&[], "a"), None);
        assert_eq!(preferred(&ids(&[7]), "a"), Some(NodeId(7)));
    }
}
