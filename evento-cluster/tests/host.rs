//! The host a deployer runs: a cluster of three nodes over real TCP opened
//! from [`Settings`] alone, each over a Fjall store with its journal inside.
//! A write on one node is read on another; a node away longer than its
//! peers wait for it is told so, marks its store, and copies it back at its
//! next open; a node alone runs without settings; peers named by host are
//! resolved when dialled; the settings and the mark round-trip.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::host::{RejoinMark, Settings};
use evento_cluster::keys;
use evento_cluster::{Cluster, ClusterExecutor, LiveNode, NodeConfig, NodeId, PeerAddr, Share};
use evento_core::{cursor::Args, Event, EventFilter, Executor, TenantFilter};
use evento_fjall::Fjall;

const AGG: &str = "test/Account";

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
    let dir = std::env::temp_dir().join(format!("evento-host-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Three free localhost addresses.
async fn free_addrs() -> Vec<SocketAddr> {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
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

    /// Opens node `id` over its own directory, through the host.
    async fn open(root: &Path, id: u64, peers: &[SocketAddr], absent_after_secs: u64) -> Self {
        Self::try_open(root, id, peers, absent_after_secs)
            .await
            .unwrap()
    }

    async fn try_open(
        root: &Path,
        id: u64,
        peers: &[SocketAddr],
        absent_after_secs: u64,
    ) -> anyhow::Result<Self> {
        let dir = Self::dir(root, id);
        std::fs::create_dir_all(&dir)?;
        let store = Fjall::open(dir.join("store"))?;
        let settings = Settings {
            absent_after_secs,
            ..Settings::new(id, peers.to_vec())
        };
        let (cluster, executor) = Cluster::builder(store)
            .settings(Some(settings))
            .state_dir(&dir)
            // Quick to tidy up, so a test of a node given up on stays short.
            .node_config(|config| NodeConfig {
                compaction_margin: Duration::from_millis(300),
                ..config
            })
            // What each node says about itself.
            .label(format!("member-{id}"))
            // Each follows a key of its own; node 2 node 1's too.
            .serves(match id {
                2 => vec!["key-2".to_owned(), "key-1".to_owned()],
                id => vec![format!("key-{id}")],
            })
            .open()
            .await?;
        Ok(Self { cluster, executor })
    }

    async fn count(&self, id: &str) -> usize {
        self.executor
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
}

async fn eventually<Fut: std::future::Future<Output = bool>>(
    what: &str,
    mut ok: impl FnMut() -> Fut,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !ok().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

async fn open_all(root: &Path, addrs: &[SocketAddr], absent_after_secs: u64) -> [Member; 3] {
    let (a, b, c) = tokio::join!(
        Member::open(root, 0, addrs, absent_after_secs),
        Member::open(root, 1, addrs, absent_after_secs),
        Member::open(root, 2, addrs, absent_after_secs),
    );
    for member in [&a, &b, &c] {
        member
            .cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
    }
    [a, b, c]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_on_one_node_is_read_on_another() {
    let root = temp_root("replicate");
    let addrs = free_addrs().await;
    let nodes = open_all(&root, &addrs, 0).await;

    nodes[0]
        .executor
        .write(vec![event("acc", 1)])
        .await
        .unwrap();
    nodes[2]
        .await_count("acc", 1, "node 2 reads node 0's write")
        .await;
    nodes[1]
        .executor
        .write(vec![event("acc", 2)])
        .await
        .unwrap();
    nodes[0]
        .await_count("acc", 2, "node 0 reads node 1's write")
        .await;

    let health = nodes[0].cluster.health();
    assert_eq!(health.id, NodeId(0));
    assert_eq!(health.nodes, 3);
    assert_eq!(health.peers.len(), 2);
    assert!(health.serving(), "{health:?}");
    assert!(!health.joined_at_start);
    assert!(health.peers.iter().all(|peer| !peer.given_up));
    eventually("every node is heard from", || async {
        nodes[1].cluster.live_nodes(Duration::from_secs(5)) == (1, 3)
    })
    .await;
    // Each node's label reaches the others with its watermark reports: a
    // peer is heard from (any message) before it has reported.
    let labels = |live: Vec<LiveNode>| -> Vec<(u64, Option<String>)> {
        live.into_iter()
            .map(|node| (node.id.0, node.label))
            .collect()
    };
    let all_labelled = [
        (0, Some("member-0".to_owned())),
        (1, Some("member-1".to_owned())),
        (2, Some("member-2".to_owned())),
    ];
    eventually("every node's label is heard", || async {
        labels(nodes[1].cluster.live(Duration::from_secs(5))) == all_labelled
    })
    .await;
    let health = nodes[1].cluster.health();
    assert_eq!(
        health
            .peers
            .iter()
            .map(|peer| (peer.id.0, peer.label.clone()))
            .collect::<Vec<_>>(),
        [
            (0, Some("member-0".to_owned())),
            (2, Some("member-2".to_owned()))
        ]
    );

    // And the keys each follows, which work shared out by key goes by.
    // Within a minute: two nodes up may hear nothing from each other for
    // seconds while the tests share a disk.
    let within = Duration::from_secs(60);
    eventually("every node's keys are heard by every node", || async {
        nodes.iter().all(|node| {
            let live = node.cluster.live(within);
            live.len() == 3 && live.iter().all(|node| node.keys.is_some())
        })
    })
    .await;
    let live = nodes[1].cluster.live(within);
    assert!(live[0].serves(Some("key-0")) && !live[0].serves(Some("key-1")));
    assert!(live[2].serves(Some("key-2")) && live[2].serves(Some("key-1")));
    assert!(!live[2].serves(None));
    assert_eq!(
        nodes[1].cluster.share_serving(Some("key-1"), within),
        Share { index: 0, of: 2 }
    );
    assert_eq!(
        nodes[2].cluster.share_serving(Some("key-1"), within),
        Share { index: 1, of: 2 }
    );
    // A node counts itself among the nodes of a key it does not follow.
    assert_eq!(
        nodes[1].cluster.share_serving(Some("key-0"), within),
        Share { index: 1, of: 2 }
    );
    assert_eq!(nodes[1].cluster.share(within), Share { index: 1, of: 3 });
    assert_eq!(
        nodes[1].cluster.health().peers[1].keys,
        Some(vec![Some("key-2".to_owned()), Some("key-1".to_owned())])
    );

    for node in nodes {
        node.cluster.shutdown().await;
    }
}

/// Each node writes the keys it serves into the store, so a node that never
/// heard another say its key still finds it there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_keys_the_nodes_serve_are_kept_in_the_store() {
    let root = temp_root("keys");
    let addrs = free_addrs().await;
    let nodes = open_all(&root, &addrs, 0).await;

    // Side by side, as nodes that start together say theirs: one write of
    // a key lands, whichever, and the other finds it there.
    let (first, second, other) = tokio::join!(
        keys::record(&nodes[0].executor, "eu"),
        keys::record(&nodes[1].executor, "eu"),
        keys::record(&nodes[1].executor, "us"),
    );
    assert!(first.unwrap() ^ second.unwrap(), "one of the two said eu");
    assert!(other.unwrap());
    assert!(!keys::record(&nodes[2].executor, "us").await.unwrap());

    let all: BTreeSet<String> = ["eu", "us"].map(str::to_owned).into();
    for node in &nodes {
        // From the node's own copy, whatever handle reads it.
        eventually("every node reads the keys", || async {
            keys::known(&node.executor.local_reads()).await.unwrap() == all
        })
        .await;
        assert_eq!(keys::known(&node.executor).await.unwrap(), all);
    }
    // The list is of its own type: the streams of the application are not
    // on it, and it is not among theirs.
    nodes[0]
        .executor
        .write(vec![event("acc", 1)])
        .await
        .unwrap();
    nodes[0].await_count("acc", 1, "the write is read").await;
    assert_eq!(keys::known(&nodes[0].executor).await.unwrap(), all);
    assert_eq!(nodes[0].count("eu").await, 0);

    for node in nodes {
        node.cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_away_too_long_copies_the_store_back_at_its_next_open() {
    let root = temp_root("rejoin");
    let addrs = free_addrs().await;
    let [n0, n1, n2] = open_all(&root, &addrs, 2).await;
    n0.executor.write(vec![event("acc", 1)]).await.unwrap();
    n2.await_count("acc", 1, "node 2 holds the first write")
        .await;

    // Node 2 goes down (its store handle with it: fjall locks the directory);
    // the others write past its absence and tidy up.
    n2.cluster.shutdown().await;
    drop(n2);
    tokio::time::sleep(Duration::from_secs(3)).await;
    for version in 2..=6 {
        n0.executor
            .write(vec![event("acc", version)])
            .await
            .unwrap();
    }
    eventually("the two nodes tidy up without node 2", || async {
        n0.cluster.health().peers.iter().any(|peer| peer.given_up)
            && n0.cluster.node().command_count() == 0
            && n1.cluster.node().command_count() == 0
    })
    .await;

    // Back with its old store: told to rejoin, the host marks it for a copy.
    let n2 = Member::open(&root, 2, &addrs, 2).await;
    eventually("node 2 is told to rejoin and marks its store", || async {
        n2.cluster.copy_pending()
    })
    .await;
    let health = n2.cluster.health();
    assert!(health.rejoin_required, "{health:?}");
    assert!(!health.serving());
    assert_eq!(
        RejoinMark::read(&Member::dir(&root, 2)).unwrap(),
        Some(RejoinMark::KeepJournal)
    );
    n2.cluster.shutdown().await;
    drop(n2);

    // The next open copies the store from a peer and serves again.
    let n2 = Member::open(&root, 2, &addrs, 2).await;
    assert_eq!(RejoinMark::read(&Member::dir(&root, 2)).unwrap(), None);
    assert!(n2.cluster.health().joined_at_start);
    n2.await_count("acc", 6, "node 2 holds the copied store")
        .await;
    n2.cluster
        .until_confirmed(Duration::from_secs(30))
        .await
        .unwrap();
    n2.executor.write(vec![event("acc", 7)]).await.unwrap();
    n0.await_count("acc", 7, "node 0 reads the rejoined node's write")
        .await;

    for node in [n0, n1, n2] {
        node.cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_store_is_copied_back_once_marked() {
    let root = temp_root("lost-store");
    let addrs = free_addrs().await;
    let [n0, n1, n2] = open_all(&root, &addrs, 0).await;
    n0.executor.write(vec![event("acc", 1)]).await.unwrap();
    n2.await_count("acc", 1, "node 2 holds the write").await;
    n2.cluster.shutdown().await;
    drop(n2);

    // The operator loses node 2's store, marks it and opens it again.
    std::fs::remove_dir_all(Member::dir(&root, 2).join("store")).unwrap();
    Cluster::mark_for_rejoin(Member::dir(&root, 2)).unwrap();
    let n2 = Member::open(&root, 2, &addrs, 0).await;
    assert!(n2.cluster.health().joined_at_start);
    n2.await_count("acc", 1, "node 2 holds the copied store")
        .await;

    for node in [n0, n1, n2] {
        node.cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_journal_marks_the_store_and_the_next_open_copies_it() {
    use evento_cluster::ClusterBackend;

    let root = temp_root("lost-journal");
    let addrs = free_addrs().await;
    let [n0, n1, n2] = open_all(&root, &addrs, 0).await;
    n0.executor.write(vec![event("acc", 1)]).await.unwrap();
    n2.await_count("acc", 1, "node 2 holds the write").await;
    n2.cluster.shutdown().await;
    drop(n2);

    // Node 2's journal is gone while its store holds events: the open is
    // refused, and leaves the mark itself.
    let store = Fjall::open(Member::dir(&root, 2).join("store")).unwrap();
    store.discard_journal().await.unwrap();
    drop(store);
    let err = Member::try_open(&root, 2, &addrs, 0)
        .await
        .err()
        .expect("a store whose journal is gone does not open as it is");
    assert!(err.to_string().contains("marked for a copy"), "{err:#}");
    assert_eq!(
        RejoinMark::read(&Member::dir(&root, 2)).unwrap(),
        Some(RejoinMark::KeepJournal)
    );
    let n2 = Member::open(&root, 2, &addrs, 0).await;
    assert!(n2.cluster.health().joined_at_start);
    n2.await_count("acc", 1, "node 2 holds the copied store")
        .await;

    for node in [n0, n1, n2] {
        node.cluster.shutdown().await;
    }
}

/// Copies the directory `from` to `to`, replacing it.
fn copy_dir(from: &Path, to: &Path) {
    let _ = std::fs::remove_dir_all(to);
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// The group is lost but for a copy of one node's store, its journal
/// inside. Put under every node and marked, it founds a new group (#541):
/// the nodes discard the copy's journal, confirm one another over the same
/// events and serve them, and the next open is plain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_comes_back_from_one_nodes_store_once_marked() {
    let root = temp_root("found");
    let addrs = free_addrs().await;
    let nodes = open_all(&root, &addrs, 0).await;
    nodes[0]
        .executor
        .write(vec![event("acc", 1), event("acc", 2)])
        .await
        .unwrap();
    nodes[1]
        .await_count("acc", 2, "node 1 holds the writes")
        .await;
    for node in &nodes {
        node.cluster.shutdown().await;
    }
    drop(nodes);

    // One node's store is all that is kept, and goes under every node.
    let backup = root.join("backup");
    copy_dir(&Member::dir(&root, 1).join("store"), &backup);
    for id in 0..3 {
        copy_dir(&backup, &Member::dir(&root, id).join("store"));
    }
    // Unmarked, it is three nodes under one node's journal: nothing a group
    // may run on. Marked, a new group begins with it.
    let marks: Vec<PathBuf> = (0..3)
        .map(|id| Cluster::mark_for_founding(Member::dir(&root, id)).unwrap())
        .collect();
    let nodes = open_all(&root, &addrs, 0).await;
    for (node, mark) in nodes.iter().zip(&marks) {
        let health = node.cluster.health();
        assert!(health.founded_at_start && health.serving(), "{health:?}");
        assert!(!mark.exists(), "the mark outlived the founding");
        assert_eq!(node.count("acc").await, 2, "the copied events are served");
    }
    nodes[2]
        .executor
        .write(vec![event("acc", 3)])
        .await
        .expect("the founded group writes after the copied events");
    nodes[0]
        .await_count("acc", 3, "node 0 reads the founded group's write")
        .await;
    for node in &nodes {
        node.cluster.shutdown().await;
    }
    drop(nodes);

    let nodes = open_all(&root, &addrs, 0).await;
    for node in &nodes {
        let health = node.cluster.health();
        assert!(!health.founded_at_start && health.serving(), "{health:?}");
        assert_eq!(node.count("acc").await, 3);
    }
    for node in &nodes {
        node.cluster.shutdown().await;
    }
}

/// A store cannot be both copied from a peer and what a group begins with.
#[tokio::test]
async fn a_store_marked_for_a_copy_and_for_founding_does_not_open() {
    let root = temp_root("both-marks");
    let addrs = free_addrs().await;
    Cluster::mark_for_rejoin(Member::dir(&root, 0)).unwrap();
    Cluster::mark_for_founding(Member::dir(&root, 0)).unwrap();
    let err = Member::try_open(&root, 0, &addrs, 0)
        .await
        .err()
        .expect("two marks that contradict each other");
    assert!(err.to_string().contains("remove one of them"), "{err:#}");
}

#[tokio::test]
async fn a_node_alone_runs_without_settings() {
    let root = temp_root("alone");
    let store = Fjall::open(root.join("store")).unwrap();
    let (cluster, executor) = Cluster::builder(store.clone())
        .state_dir(&root)
        .open()
        .await
        .unwrap();
    executor.write(vec![event("acc", 1)]).await.unwrap();
    let health = cluster.health();
    assert_eq!(health.nodes, 1);
    assert!(health.peers.is_empty());
    assert!(health.serving(), "{health:?}");
    assert_eq!(cluster.live_nodes(Duration::from_secs(1)), (0, 1));
    assert_eq!(
        cluster.live(Duration::from_secs(1)),
        [LiveNode {
            id: NodeId(0),
            label: None,
            // A store without a default routing key: the node follows the
            // events written without one.
            keys: Some(vec![None]),
        }]
    );
    assert_eq!(
        cluster.share_serving(None, Duration::from_secs(1)),
        Share::ALONE
    );
    assert!(
        !cluster.node().linearizable_reads(),
        "alone, no read barrier"
    );
    cluster.shutdown().await;

    // A mark on a node alone is a mistake the open names.
    Cluster::mark_for_rejoin(&root).unwrap();
    let err = Cluster::builder(store)
        .state_dir(&root)
        .open()
        .await
        .err()
        .expect("a single node has no peer to copy from");
    assert!(err.to_string().contains("no peer"), "{err}");
}

#[tokio::test]
async fn a_node_with_peers_needs_a_state_dir() {
    let addrs = free_addrs().await;
    let err = Cluster::builder(Fjall::temporary().unwrap())
        .settings(Some(Settings::new(0, addrs)))
        .open()
        .await
        .err()
        .expect("no state dir");
    assert!(err.to_string().contains("state directory"), "{err}");
}

/// Peers named by host, as on a platform where a node keeps its name and not
/// its address: a name is resolved when the peer is dialled, and one that
/// does not resolve (the third node's, which is not up) is a peer away, not
/// a start-up failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_named_by_host_are_resolved_when_dialled() {
    let root = temp_root("named");
    let addrs = free_addrs().await;
    let peers: Vec<PeerAddr> = vec![
        format!("localhost:{}", addrs[0].port()).parse().unwrap(),
        format!("localhost:{}", addrs[1].port()).parse().unwrap(),
        "node-2.invalid:7000".parse().unwrap(),
    ];
    let open = |id: u64| {
        let dir = Member::dir(&root, id);
        let settings = Settings {
            listen: Some(addrs[id as usize]),
            absent_after_secs: 0,
            ..Settings::new(id, peers.clone())
        };
        async move {
            std::fs::create_dir_all(&dir).unwrap();
            let (cluster, executor) = Cluster::builder(Fjall::open(dir.join("store")).unwrap())
                .settings(Some(settings))
                .state_dir(&dir)
                .open()
                .await
                .unwrap();
            Member { cluster, executor }
        }
    };
    let (a, b) = tokio::join!(open(0), open(1));
    for member in [&a, &b] {
        member
            .cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
    }

    a.executor.write(vec![event("acc", 1)]).await.unwrap();
    b.await_count("acc", 1, "node 1 reads node 0's write").await;
    b.executor.write(vec![event("acc", 2)]).await.unwrap();
    a.await_count("acc", 2, "node 0 reads node 1's write").await;

    for member in [a, b] {
        member.cluster.shutdown().await;
    }
}

#[test]
fn settings_name_a_peer_by_host_and_then_need_listen() {
    let named = r#"{"node_id": 1, "peers": ["app-0.app:7000", "app-1.app:7000", "10.0.0.3:7000"]}"#;
    let settings: Settings = serde_json::from_str(named).unwrap();
    assert_eq!(
        settings.peers[1],
        PeerAddr::Host {
            host: "app-1.app".into(),
            port: 7000
        }
    );
    assert_eq!(
        serde_json::to_value(&settings).unwrap()["peers"],
        serde_json::json!(["app-0.app:7000", "app-1.app:7000", "10.0.0.3:7000"])
    );

    // Its own entry is a name: nothing to bind.
    let err = settings.validate().unwrap_err().to_string();
    assert!(err.contains("cluster.listen must be set"), "{err}");
    assert!(err.contains("app-1.app:7000"), "{err}");

    let mut listening = settings.clone();
    listening.listen = Some("0.0.0.0:7000".parse().unwrap());
    listening.validate().unwrap();
    assert_eq!(
        listening.listen_addr().unwrap(),
        "0.0.0.0:7000".parse().unwrap()
    );

    // The node whose entry is an address still binds it.
    let mut by_address = settings;
    by_address.node_id = 2;
    by_address.validate().unwrap();

    let err = serde_json::from_str::<Settings>(r#"{"node_id": 0, "peers": ["app-0.app"]}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("host:port"), "{err}");
}

#[test]
fn settings_deserialize_with_defaults_and_validate() {
    let settings: Settings = serde_json::from_str(
        r#"{"node_id": 1, "peers": ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]}"#,
    )
    .unwrap();
    assert_eq!(settings.node_id, 1);
    assert_eq!(settings.listen, None);
    assert_eq!(
        settings.listen_addr().unwrap(),
        "10.0.0.2:7000".parse().unwrap()
    );
    assert_eq!(settings.absent_after(), Some(Duration::from_secs(3600)));
    assert_eq!(settings.write_backlog, 100_000);
    assert_eq!(settings.ids(), vec![NodeId(0), NodeId(1), NodeId(2)]);
    settings.validate().unwrap();

    let config = settings.node_config();
    assert!(
        config.linearizable_reads,
        "with peers, reads wait for the quorum"
    );
    assert_eq!(config.peer_absence_limit, Some(Duration::from_secs(3600)));
    assert_eq!(config.max_commands, 100_000);

    let alone = Settings::new(0, ["10.0.0.1:7000".parse::<SocketAddr>().unwrap()]);
    let config = alone.node_config();
    assert!(
        !config.linearizable_reads,
        "alone, a node applies its own writes first"
    );
    assert_eq!(config.peer_absence_limit, None);

    let mut explicit = settings.clone();
    explicit.linearizable_reads = Some(false);
    explicit.absent_after_secs = 0;
    assert!(!explicit.node_config().linearizable_reads);
    assert_eq!(explicit.node_config().peer_absence_limit, None);

    let mut bad = settings.clone();
    bad.node_id = 3;
    assert!(bad
        .validate()
        .unwrap_err()
        .to_string()
        .contains("out of range"));
    let mut bad = settings.clone();
    bad.write_backlog = 0;
    assert!(bad
        .validate()
        .unwrap_err()
        .to_string()
        .contains("write_backlog"));
    let mut bad = settings;
    bad.peers.clear();
    assert!(bad
        .validate()
        .unwrap_err()
        .to_string()
        .contains("at least one"));
}

#[test]
fn the_rejoin_mark_round_trips_and_never_weakens() {
    let dir = temp_root("mark");
    assert_eq!(RejoinMark::read(&dir).unwrap(), None);
    RejoinMark::KeepJournal.write(&dir).unwrap();
    assert_eq!(
        RejoinMark::read(&dir).unwrap(),
        Some(RejoinMark::KeepJournal)
    );
    RejoinMark::DiscardJournal.write(&dir).unwrap();
    assert_eq!(
        RejoinMark::read(&dir).unwrap(),
        Some(RejoinMark::DiscardJournal)
    );
    RejoinMark::KeepJournal.write(&dir).unwrap();
    assert_eq!(
        RejoinMark::read(&dir).unwrap(),
        Some(RejoinMark::DiscardJournal),
        "a discard is never weakened to a keep"
    );
    RejoinMark::clear(&dir).unwrap();
    assert_eq!(RejoinMark::read(&dir).unwrap(), None);
    RejoinMark::clear(&dir).unwrap();
    // An empty file, left by hand, keeps the journal.
    std::fs::write(RejoinMark::path(&dir), b"").unwrap();
    assert_eq!(
        RejoinMark::read(&dir).unwrap(),
        Some(RejoinMark::KeepJournal)
    );
    std::fs::write(RejoinMark::path(&dir), b"what").unwrap();
    assert!(RejoinMark::read(&dir).is_err());
}
