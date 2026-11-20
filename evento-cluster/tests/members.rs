//! The members of a running cluster changed through the host: three nodes
//! over real TCP, each over a Fjall store, founded from [`Settings`]. A
//! node added by one call on a member copies the store at its first open
//! and counts, whether it starts before the call or after it, and whoever
//! is restarted meanwhile; it founds nothing.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::host::{RejoinMark, Settings};
use evento_cluster::{Cluster, ClusterExecutor, NodeConfig, NodeId, PeerAddr};
use evento_core::{cursor::Args, Event, EventFilter, Executor, TenantFilter};
use evento_fjall::Fjall;

const AGG: &str = "test/Account";

/// The nodes the test clusters are founded with.
const FOUNDERS: usize = 3;

fn event(id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: AGG.into(),
        aggregate_id: id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

/// A fresh directory for one test's nodes.
fn temp_root(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("evento-members-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `count` free localhost addresses.
async fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let mut listeners = Vec::new();
    for _ in 0..count {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

fn ids(nodes: &[u64]) -> Vec<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

/// One node of the test cluster, as an application holds it.
struct Member {
    cluster: Arc<Cluster>,
    executor: ClusterExecutor<Fjall>,
}

impl Member {
    fn dir(root: &Path, id: u64) -> PathBuf {
        root.join(format!("node{id}"))
    }

    /// Opens node `id` over its own directory, through the host, with the
    /// `peers` its settings name: a founder's name the founders alone, as
    /// they did when the cluster began.
    async fn open(root: &Path, id: u64, peers: &[SocketAddr]) -> Self {
        Self::try_open(root, id, peers).await.unwrap()
    }

    async fn try_open(root: &Path, id: u64, peers: &[SocketAddr]) -> anyhow::Result<Self> {
        let dir = Self::dir(root, id);
        std::fs::create_dir_all(&dir)?;
        let store = Fjall::open(dir.join("store"))?;
        let settings = Settings {
            founders: Some(FOUNDERS as u64),
            absent_after_secs: 0,
            ..Settings::new(id, peers.to_vec())
        };
        let (cluster, executor) = Cluster::builder(store)
            .settings(Some(settings))
            .state_dir(&dir)
            .node_config(|config| NodeConfig {
                compaction_margin: Duration::from_millis(300),
                ..config
            })
            .open()
            .await?;
        Ok(Self { cluster, executor })
    }

    async fn write(&self, id: &str, version: u16) {
        // A write right after the members changed, or a node stopped, can
        // fail once: the node says so, and the caller writes again.
        let mut last = None;
        for _ in 0..40 {
            match self.executor.write(vec![event(id, version)]).await {
                Ok(()) => return,
                Err(err) => last = Some(err),
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!(
            "write of {id} v{version} on node {}: {last:?}",
            self.cluster.id().0
        );
    }

    async fn count(&self, id: &str) -> usize {
        self.executor
            .local_reads()
            .read(
                Some([EventFilter::by_id_raw(AGG, id)].into()),
                None,
                TenantFilter::All,
                Args::forward(100, None),
                None,
            )
            .await
            .unwrap()
            .edges
            .len()
    }

    async fn await_count(&self, id: &str, expected: usize, what: &str) {
        eventually(what, || async { self.count(id).await == expected }).await;
    }

    async fn await_members(&self, nodes: &[u64], what: &str) {
        eventually(what, || async {
            let mut members = self.cluster.members().nodes;
            members.sort();
            members == ids(nodes)
        })
        .await;
    }
}

async fn eventually<Fut: std::future::Future<Output = bool>>(
    what: &str,
    mut ok: impl FnMut() -> Fut,
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while !ok().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// The founders, each started with the founders' addresses alone.
async fn found(root: &Path, addrs: &[SocketAddr]) -> Vec<Member> {
    let peers = &addrs[..FOUNDERS];
    let (a, b, c) = tokio::join!(
        Member::open(root, 0, peers),
        Member::open(root, 1, peers),
        Member::open(root, 2, peers),
    );
    for member in [&a, &b, &c] {
        member
            .cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
    }
    vec![a, b, c]
}

/// A node started before it is added waits, founds nothing, and copies the
/// store once a member adds it; from then on it counts, and the members,
/// none of them restarted, reach it at the address the call gave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_started_first_waits_to_be_added_then_copies_the_store() {
    let root = temp_root("started-first");
    let addrs = free_addrs(4).await;
    let mut nodes = found(&root, &addrs).await;
    for version in 1..=3 {
        nodes[0].write("acc", version).await;
    }

    let opening = {
        let (root, addrs) = (root.clone(), addrs.clone());
        tokio::spawn(async move { Member::open(&root, 3, &addrs).await })
    };
    // Marked for a copy before anything else: a restart meanwhile copies
    // too.
    eventually("the new node marks its store", || async {
        RejoinMark::read(&Member::dir(&root, 3)).unwrap() == Some(RejoinMark::KeepJournal)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!opening.is_finished(), "no member added it yet");
    assert_eq!(nodes[1].cluster.members().nodes, ids(&[0, 1, 2]));
    nodes[1].write("acc", 4).await;

    let members = nodes[1]
        .cluster
        .add_node(NodeId(3), addrs[3])
        .await
        .unwrap();
    assert_eq!(members.nodes, ids(&[0, 1, 2, 3]));
    assert_eq!(members.epoch, 1);

    let added = tokio::time::timeout(Duration::from_secs(60), opening)
        .await
        .expect("the added node opens")
        .unwrap();
    assert!(added.cluster.health().joined_at_start);
    assert!(added.cluster.health().serving());
    assert_eq!(added.count("acc").await, 4, "the store was copied");
    assert_eq!(RejoinMark::read(&Member::dir(&root, 3)).unwrap(), None);
    for node in nodes.iter().chain([&added]) {
        node.await_members(&[0, 1, 2, 3], "every node holds the four members")
            .await;
    }
    for node in &nodes {
        assert_eq!(
            node.cluster.address(NodeId(3)),
            Some(PeerAddr::from(addrs[3]))
        );
    }

    // It counts: with one founder stopped, three of the four still decide,
    // the new node among them.
    added.write("acc", 5).await;
    nodes[0]
        .await_count("acc", 5, "a founder reads the new node's write")
        .await;
    let stopped = nodes.remove(2);
    stopped.cluster.shutdown().await;
    drop(stopped);
    nodes[0].write("acc", 6).await;
    added
        .await_count(
            "acc",
            6,
            "the new node reads a write made with a founder away",
        )
        .await;

    for node in nodes.iter().chain([&added]) {
        node.cluster.shutdown().await;
    }
}

/// A node added before it starts, by two calls on two members, is added
/// once. A member restarted meanwhile, onto the settings it always had,
/// holds the four members from its journal and reaches the new node where
/// the cluster says. The node then starts, and copies the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_added_first_and_twice_joins_when_it_starts() {
    let root = temp_root("added-first");
    let addrs = free_addrs(4).await;
    let mut nodes = found(&root, &addrs).await;
    nodes[0].write("acc", 1).await;

    let first = nodes[0]
        .cluster
        .add_node(NodeId(3), addrs[3])
        .await
        .unwrap();
    let again = nodes[2]
        .cluster
        .add_node(NodeId(3), addrs[3])
        .await
        .unwrap();
    assert_eq!(first.nodes, ids(&[0, 1, 2, 3]));
    assert_eq!(again.nodes, first.nodes);
    assert_eq!(again.epoch, 1, "added once");

    // Three of four decide while the fourth has yet to start, and a read
    // that waits for what a majority committed is answered.
    nodes[1].write("acc", 2).await;
    eventually("a linearizable read is answered one node short", || async {
        nodes[2]
            .executor
            .read(
                Some([EventFilter::by_id_raw(AGG, "acc")].into()),
                None,
                TenantFilter::All,
                Args::forward(100, None),
                None,
            )
            .await
            .is_ok_and(|page| page.edges.len() == 2)
    })
    .await;

    let restarted = nodes.remove(1);
    restarted.cluster.shutdown().await;
    drop(restarted);
    let restarted = Member::open(&root, 1, &addrs[..FOUNDERS]).await;
    restarted
        .await_members(&[0, 1, 2, 3], "the journal holds the members")
        .await;
    eventually("the restarted member reaches the new node", || async {
        restarted.cluster.address(NodeId(3)) == Some(PeerAddr::from(addrs[3]))
    })
    .await;
    nodes.insert(1, restarted);

    let added = Member::open(&root, 3, &addrs).await;
    assert!(added.cluster.health().joined_at_start);
    assert_eq!(added.count("acc").await, 2);
    added.write("acc", 3).await;
    for node in &nodes {
        node.await_count("acc", 3, "every founder reads the new node's write")
            .await;
    }

    // Restarted in its turn, it opens as any member does: no copy.
    added.cluster.shutdown().await;
    drop(added);
    let added = Member::open(&root, 3, &addrs).await;
    assert!(!added.cluster.health().joined_at_start);
    assert_eq!(added.cluster.members().nodes, ids(&[0, 1, 2, 3]));
    assert_eq!(added.count("acc").await, 3);

    for node in nodes.iter().chain([&added]) {
        node.cluster.shutdown().await;
    }
}

/// A new node whose open is given up while it waits — its process stopped —
/// waits again at its next start, and copies the store once added.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_restarted_while_it_waits_is_added_all_the_same() {
    let root = temp_root("restarted-waiting");
    let addrs = free_addrs(4).await;
    let nodes = found(&root, &addrs).await;
    nodes[0].write("acc", 1).await;

    let opening = {
        let (root, addrs) = (root.clone(), addrs.clone());
        tokio::spawn(async move { Member::open(&root, 3, &addrs).await })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    opening.abort();
    let _ = opening.await;

    // The files are let go a moment after the open is.
    let mut again = None;
    for _ in 0..100 {
        match Fjall::open(Member::dir(&root, 3).join("probe")) {
            Ok(probe) => drop(probe),
            Err(err) => panic!("{err}"),
        }
        let (root, addrs) = (root.clone(), addrs.clone());
        let opening = tokio::spawn(async move { Member::try_open(&root, 3, &addrs).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        if !opening.is_finished() {
            again = Some(opening);
            break;
        }
        // Still bound, or the store still held: tried again.
        let _ = opening.await;
    }
    let opening = again.expect("the node opens again and waits");

    nodes[2]
        .cluster
        .add_node(NodeId(3), addrs[3])
        .await
        .unwrap();
    let added = tokio::time::timeout(Duration::from_secs(60), opening)
        .await
        .expect("the added node opens")
        .unwrap()
        .unwrap();
    assert_eq!(added.count("acc").await, 1);
    added.write("acc", 2).await;
    nodes[0]
        .await_count("acc", 2, "a founder reads the new node's write")
        .await;

    for node in nodes.iter().chain([&added]) {
        node.cluster.shutdown().await;
    }
}

/// A node that is no member, over files that hold something, does not
/// open: it is not emptied on the word of a `node_id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_is_no_member_over_a_store_that_holds_events_does_not_open() {
    let root = temp_root("not-empty");
    let addrs = free_addrs(4).await;
    let dir = Member::dir(&root, 3);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let store = Fjall::open(dir.join("store")).unwrap();
        store.write(vec![event("acc", 1)]).await.unwrap();
    }
    let err = Member::try_open(&root, 3, &addrs)
        .await
        .err()
        .expect("refused");
    assert!(format!("{err:#}").contains("is no member"), "{err:#}");
    assert_eq!(RejoinMark::read(&dir).unwrap(), None);
    let store = Fjall::open(dir.join("store")).unwrap();
    let kept = store
        .read(None, None, TenantFilter::All, Args::forward(10, None), None)
        .await
        .unwrap();
    assert_eq!(kept.edges.len(), 1, "nothing was emptied");
}

#[test]
fn settings_say_how_many_nodes_found_the_cluster() {
    let addrs: Vec<SocketAddr> = (0..4)
        .map(|i| format!("10.0.0.{}:7000", i + 1).parse().unwrap())
        .collect();
    let all = Settings::new(1, addrs.clone());
    assert_eq!(all.founder_ids(), ids(&[0, 1, 2, 3]));
    all.validate().unwrap();

    let grown: Settings = serde_json::from_str(
        r#"{"node_id": 3, "founders": 3,
            "peers": ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000", "10.0.0.4:7000"]}"#,
    )
    .unwrap();
    grown.validate().unwrap();
    assert_eq!(grown.founder_ids(), ids(&[0, 1, 2]));
    assert_eq!(grown.ids(), ids(&[0, 1, 2, 3]));

    for founders in [0, 5] {
        let settings = Settings {
            founders: Some(founders),
            ..Settings::new(0, addrs.clone())
        };
        assert!(settings.validate().is_err(), "founders = {founders}");
    }
}
