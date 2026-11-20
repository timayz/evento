//! A node takes on the routing key of a node that is away (#562): three
//! nodes over real TCP, each serving a key of its own and covering for the
//! others. One stops with an event of its key not handled yet; exactly one
//! of the two left handles it once the node has been silent long enough,
//! and stops following the key when the node is back. The key goes to the
//! node that holds its lease (#565): a holder stops when another node took
//! the lease or the cluster no longer confirms it, and a node that runs its
//! own key from the cover takes it back when it returns.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_cluster::host::Settings;
use evento_cluster::{keys, lease, Cluster, ClusterExecutor, Covering, NodeConfig};
use evento_core::subscription::{Context, Handler, SubscriptionBuilder};
use evento_core::{Event, Executor, WriteError};
use evento_fjall::Fjall;

const AGG: &str = "test/Order";

/// How long a key goes without a node before it is covered for. Above the
/// silences between two nodes up, which reach seconds here while the tests
/// share a disk: under them both would take the key on.
const AFTER: Duration = Duration::from_secs(8);

/// How often a node looks.
const EVERY: Duration = Duration::from_millis(200);

/// How long a holder is in its right after the cluster confirmed its lease.
/// Above the time one question may go unanswered (`collect_timeout`, 5 s),
/// so a holder is not out of time for one answer lost while the tests share
/// a disk.
const LEASE: Duration = Duration::from_secs(6);

/// An event of the stream `id`, created under `key`.
fn event(id: &str, key: &str) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: AGG.into(),
        aggregate_id: id.into(),
        version: 1,
        name: "Placed".into(),
        routing_key: Some(key.into()),
        ..Default::default()
    }
}

/// What each node handled, as `(node, stream)`, across the cluster.
type Handled = Arc<Mutex<Vec<(u64, String)>>>;

/// Records every event the subscription handles, with the node it ran on.
struct Recorder {
    node: u64,
    handled: Handled,
}

impl<E: Executor> Handler<E> for Recorder {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.handled
                .lock()
                .unwrap()
                .push((self.node, event.aggregate_id.clone()));
            Ok(())
        })
    }

    fn aggregate_type(&self) -> &'static str {
        AGG
    }

    fn event_name(&self) -> &'static str {
        "Placed"
    }
}

/// A tracing subscriber over `RUST_LOG`, to debug a test; nothing without it.
fn init_tracing() {
    if std::env::var_os("RUST_LOG").is_none() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

fn temp_root(test: &str) -> PathBuf {
    init_tracing();
    let dir = std::env::temp_dir().join(format!("evento-cover-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn free_addrs() -> Vec<SocketAddr> {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// One node: it serves `key-<id>` and covers for the keys without a node.
/// Unless it runs its own key from the cover (`own`), it runs no
/// subscription on it, so what is handled was handled by a node covering.
struct Member {
    cluster: Arc<Cluster>,
    executor: ClusterExecutor<Fjall>,
    covering: Covering,
}

impl Member {
    async fn open(root: &Path, id: u64, peers: &[SocketAddr], handled: &Handled) -> Self {
        Self::open_with(root, id, peers, handled, false).await
    }

    async fn open_with(
        root: &Path,
        id: u64,
        peers: &[SocketAddr],
        handled: &Handled,
        own: bool,
    ) -> Self {
        let dir = root.join(format!("node{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Fjall::open(dir.join("store")).unwrap();
        let (cluster, executor) = Cluster::builder(store)
            .settings(Some(Settings::new(id, peers.to_vec())))
            .state_dir(&dir)
            // Quick to call an event stable, so a subscription sees it soon.
            .node_config(|config| NodeConfig {
                compaction_margin: Duration::from_millis(300),
                ..config
            })
            .serves([format!("key-{id}")])
            .open()
            .await
            .unwrap();
        let handled = handled.clone();
        let mut cover = cluster
            .cover(executor.local_reads())
            .after(AFTER)
            .every(EVERY)
            .lease(LEASE)
            .subscription("orders", move |_key| {
                SubscriptionBuilder::new("orders").handler(Recorder {
                    node: id,
                    handled: handled.clone(),
                })
            });
        if own {
            cover = cover.own_keys();
        }
        let covering = cover.start();
        Self {
            cluster,
            executor,
            covering,
        }
    }

    fn covered(&self) -> Vec<String> {
        self.covering
            .covered()
            .into_iter()
            .map(|covered| covered.key)
            .collect()
    }

    /// Writes the first event of the stream `id` under `key`. A write may
    /// fail for a moment after a node stopped (its answer is waited for and
    /// missed): it is made again, and one whose outcome was not known finds
    /// its event there.
    async fn write(&self, id: &str, key: &str) {
        eventually("the write goes through", || async {
            matches!(
                self.executor.write(vec![event(id, key)]).await,
                Ok(()) | Err(WriteError::InvalidOriginalVersion)
            )
        })
        .await;
    }

    /// How many times this node handled the stream `id`.
    fn handled(&self, handled: &Handled, id: &str) -> usize {
        let node = self.cluster.id().0;
        handled
            .lock()
            .unwrap()
            .iter()
            .filter(|(by, stream)| *by == node && stream == id)
            .count()
    }

    async fn shutdown(self) {
        self.covering.shutdown().await;
        self.cluster.shutdown().await;
    }
}

/// Three nodes, confirmed, each having read every key on the list.
async fn three(root: &Path, addrs: &[SocketAddr], handled: &Handled, own: bool) -> Vec<Member> {
    let (n0, n1, n2) = tokio::join!(
        Member::open_with(root, 0, addrs, handled, own),
        Member::open_with(root, 1, addrs, handled, own),
        Member::open_with(root, 2, addrs, handled, own),
    );
    let nodes = vec![n0, n1, n2];
    let all: BTreeSet<String> = ["key-0", "key-1", "key-2"].map(str::to_owned).into();
    for member in &nodes {
        member
            .cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
        eventually("every node reads every key", || async {
            keys::known(&member.executor.local_reads()).await.unwrap() == all
        })
        .await;
    }
    nodes
}

async fn eventually<Fut: Future<Output = bool>>(what: &str, mut ok: impl FnMut() -> Fut) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while !ok().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_key_of_a_node_away_is_taken_on_by_one_node_and_handed_back() {
    let root = temp_root("away");
    let addrs = free_addrs().await;
    let handled: Handled = Arc::default();
    let (n0, n1, n2) = tokio::join!(
        Member::open(&root, 0, &addrs, &handled),
        Member::open(&root, 1, &addrs, &handled),
        Member::open(&root, 2, &addrs, &handled),
    );
    for member in [&n0, &n1, &n2] {
        member
            .cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
    }

    // Each node says its key on the list in the store.
    let all: BTreeSet<String> = ["key-0", "key-1", "key-2"].map(str::to_owned).into();
    for member in [&n0, &n1, &n2] {
        eventually("every node reads every key", || async {
            keys::known(&member.executor.local_reads()).await.unwrap() == all
        })
        .await;
    }

    // Node 1 writes under its key and stops before anything handled it.
    n1.write("before", "key-1").await;
    // Every key has a node: nothing is covered for, whatever the looks.
    tokio::time::sleep(EVERY * 5).await;
    for member in [&n0, &n1, &n2] {
        assert!(member.covered().is_empty());
    }
    assert!(handled.lock().unwrap().is_empty());
    n1.shutdown().await;

    // Once node 1 has been silent for `AFTER`, one of the two others takes
    // its key on and handles what waited.
    eventually("the event of the node away is handled", || async {
        !handled.lock().unwrap().is_empty()
    })
    .await;
    eventually("a node shows the key it covers for", || async {
        n0.covered().len() + n2.covered().len() == 1
    })
    .await;
    let (by, other) = if n0.covered().is_empty() {
        (&n2, &n0)
    } else {
        (&n0, &n2)
    };
    assert_eq!(by.covered(), ["key-1"]);
    assert_eq!(by.covering.covered()[0].subscriptions, ["orders"]);
    assert!(other.covered().is_empty());

    // What is written under the key while its node is away goes to the
    // same node, and to that one alone.
    other.write("during", "key-1").await;
    eventually("an event written meanwhile is handled", || async {
        handled.lock().unwrap().len() == 2
    })
    .await;
    tokio::time::sleep(EVERY * 5).await;
    let by_id = by.cluster.id().0;
    assert_eq!(
        *handled.lock().unwrap(),
        [(by_id, "before".to_owned()), (by_id, "during".to_owned())]
    );
    // A lease not confirmed in time, on a slow disk, is a hand-back and a
    // take more: what counts is that the key runs on this node now.
    let metrics = by.cluster.node().metrics();
    assert_eq!(metrics.cover_takes - metrics.cover_hand_backs, 1);
    let metrics = other.cluster.node().metrics();
    assert_eq!((metrics.cover_takes, metrics.cover_hand_backs), (0, 0));

    // Node 1 is back: the key is handed back.
    let n1 = Member::open(&root, 1, &addrs, &handled).await;
    n1.cluster
        .until_confirmed(Duration::from_secs(30))
        .await
        .unwrap();
    eventually("the key is handed back", || async {
        by.covered().is_empty()
    })
    .await;
    let metrics = by.cluster.node().metrics();
    assert_eq!(metrics.cover_takes, metrics.cover_hand_backs);
    // And stays so: an event of the key is no longer handled by the node
    // that covered for it.
    n1.write("after", "key-1").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(handled.lock().unwrap().len(), 2);
    assert!(n0.covered().is_empty() && n1.covered().is_empty() && n2.covered().is_empty());

    for member in [n0, n1, n2] {
        member.shutdown().await;
    }
}

/// The node that covers for a key stops when another node took the key's
/// lease, and the one that took it starts once the first is out of time. A
/// holder the cluster no longer answers stops too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_stops_once_the_lease_is_another_node_s_or_is_not_confirmed() {
    let root = temp_root("lease");
    let addrs = free_addrs().await;
    let handled: Handled = Arc::default();
    let mut nodes = three(&root, &addrs, &handled, false).await;
    let n1 = nodes.remove(1);
    n1.write("before", "key-1").await;
    n1.shutdown().await;

    eventually("a node covers for the key", || async {
        nodes.iter().any(|node| node.covered() == ["key-1"])
    })
    .await;
    let (by, other) = match nodes[0].covered().is_empty() {
        true => (1, 0),
        false => (0, 1),
    };
    let first = lease::held(&nodes[by].executor.local_reads(), "key-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((first.holder, first.term), (nodes[by].cluster.id(), 1));

    // The other node takes the lease, as it would had it not heard from the
    // first for a while: the first stops at its next question, and the
    // other starts once the first is out of time.
    let taken = lease::take(
        &nodes[other].executor,
        "key-1",
        nodes[other].cluster.id(),
        Some(first),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(taken.term, 2);
    let took = std::time::Instant::now();
    eventually("the first holder stops", || async {
        nodes[by].covered().is_empty()
    })
    .await;
    assert!(nodes[other].covered().is_empty());
    eventually("the node that took the lease starts", || async {
        nodes[other].covered() == ["key-1"]
    })
    .await;
    assert!(took.elapsed() >= LEASE);
    // A term taken over a term that is gone is refused.
    assert!(lease::take(
        &nodes[by].executor,
        "key-1",
        nodes[by].cluster.id(),
        Some(first)
    )
    .await
    .unwrap()
    .is_none());

    // What is written now goes to the new holder alone.
    nodes[by].write("during", "key-1").await;
    eventually("the new holder handles the key", || async {
        nodes[other].handled(&handled, "during") == 1
    })
    .await;
    assert_eq!(nodes[by].handled(&handled, "during"), 0);

    // Alone of three, the holder is not answered: it stops when its time
    // is up.
    let stopped = nodes.remove(by);
    stopped.shutdown().await;
    let last = nodes.remove(0);
    eventually("a holder that is cut off stops", || async {
        last.covered().is_empty()
    })
    .await;
    assert!(last.cluster.node().metrics().cover_lapses >= 1);
    last.shutdown().await;
}

/// Nodes that run their own keys from the cover: a node that returns takes
/// its key back from the node that covered for it, and what is written then
/// is handled by it alone. Each goes on from where the other stopped (#566):
/// no event of the key is handled twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_returns_takes_its_own_key_back() {
    let root = temp_root("own");
    let addrs = free_addrs().await;
    let handled: Handled = Arc::default();
    let mut nodes = three(&root, &addrs, &handled, true).await;

    // Each node runs its own key, and no other.
    for (id, node) in nodes.iter().enumerate() {
        eventually("a node runs its own key", || async {
            node.covered() == [format!("key-{id}")]
        })
        .await;
        assert!(node.covering.covered()[0].own);
    }
    nodes[1].write("before", "key-1").await;
    eventually("a node handles what is written under its key", || async {
        nodes[1].handled(&handled, "before") == 1
    })
    .await;

    // Node 1 away: one of the others takes its key on, besides its own.
    let n1 = nodes.remove(1);
    n1.shutdown().await;
    nodes[0].write("during", "key-1").await;
    eventually("the key of the node away is covered for", || async {
        nodes
            .iter()
            .map(|node| node.handled(&handled, "during"))
            .sum::<usize>()
            == 1
    })
    .await;
    let by = match nodes[0].handled(&handled, "during") {
        1 => 0,
        _ => 1,
    };
    assert_eq!(nodes[by].covered().len(), 2);
    // It went on from where node 1 stopped, not from the key's first event.
    for node in &nodes {
        assert_eq!(node.handled(&handled, "before"), 0);
    }
    assert_eq!(nodes[by].cluster.node().metrics().cover_cursors_moved, 1);
    assert!(nodes[by]
        .covering
        .covered()
        .iter()
        .any(|covered| covered.key == "key-1" && !covered.own));

    // Node 1 is back: it takes the lease of its key, and the node that
    // covered for it stops.
    let n1 = Member::open_with(&root, 1, &addrs, &handled, true).await;
    n1.cluster
        .until_confirmed(Duration::from_secs(30))
        .await
        .unwrap();
    eventually("the node back runs its key", || async {
        n1.covered() == ["key-1"]
    })
    .await;
    assert_eq!(nodes[by].covered().len(), 1);
    let held = lease::held(&n1.executor.local_reads(), "key-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((held.holder, held.term), (n1.cluster.id(), 3));
    n1.write("after", "key-1").await;
    eventually("the node back handles its key", || async {
        n1.handled(&handled, "after") == 1
    })
    .await;
    tokio::time::sleep(EVERY * 5).await;
    for node in &nodes {
        assert_eq!(node.handled(&handled, "after"), 0);
    }
    // And node 1 went on from where its cover stopped: every event of the
    // key was handled once, whichever node ran it then.
    assert_eq!(n1.handled(&handled, "during"), 0);
    assert_eq!(n1.cluster.node().metrics().cover_cursors_moved, 1);
    let mut all: Vec<String> = handled
        .lock()
        .unwrap()
        .iter()
        .map(|(_, stream)| stream.clone())
        .collect();
    all.sort();
    assert_eq!(all, ["after", "before", "during"]);

    n1.shutdown().await;
    for node in nodes {
        node.shutdown().await;
    }
}

/// A node alone over a store that holds the key of a node that is gone, as
/// after a restore from one node's copy, takes that key on at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_alone_takes_on_a_key_left_on_the_list() {
    let root = temp_root("alone");
    let store = Fjall::open(root.join("store")).unwrap();
    let (cluster, executor) = Cluster::builder(store)
        .serves(["here"])
        .open()
        .await
        .unwrap();
    assert!(keys::record(&executor, "gone").await.unwrap());
    executor.write(vec![event("left", "gone")]).await.unwrap();
    executor.write(vec![event("own", "here")]).await.unwrap();

    let handled: Handled = Arc::default();
    let recorded = handled.clone();
    let covering = cluster
        .cover(executor.clone())
        .every(EVERY)
        .subscription("orders", move |_key| {
            SubscriptionBuilder::new("orders").handler(Recorder {
                node: 0,
                handled: recorded.clone(),
            })
        })
        .start();
    eventually("the key left behind is handled", || async {
        !handled.lock().unwrap().is_empty()
    })
    .await;
    tokio::time::sleep(EVERY * 3).await;
    // Its own key is its own subscriptions' to follow, not the cover's.
    assert_eq!(*handled.lock().unwrap(), [(0, "left".to_owned())]);
    assert_eq!(
        covering
            .covered()
            .into_iter()
            .map(|covered| covered.key)
            .collect::<Vec<_>>(),
        ["gone"]
    );
    assert_eq!(
        keys::known(&executor).await.unwrap(),
        ["gone", "here"].map(str::to_owned).into()
    );

    covering.shutdown().await;
    cluster.shutdown().await;
}
