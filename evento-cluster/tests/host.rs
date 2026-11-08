//! The host a deployer runs: a cluster of three nodes over real TCP opened
//! from [`Settings`] alone, each over a Fjall store with its journal inside.
//! A write on one node is read on another; a node away longer than its
//! peers wait for it is told so, marks its store, and copies it back at its
//! next open; a node alone runs without settings; the settings and the mark
//! round-trip.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::host::{RejoinMark, Settings};
use evento_cluster::{Cluster, ClusterExecutor, LiveNode, NodeConfig, NodeId};
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
            label: None
        }]
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

#[test]
fn settings_deserialize_with_defaults_and_validate() {
    let settings: Settings = serde_json::from_str(
        r#"{"node_id": 1, "peers": ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]}"#,
    )
    .unwrap();
    assert_eq!(settings.node_id, 1);
    assert_eq!(settings.listen, None);
    assert_eq!(settings.listen_addr(), "10.0.0.2:7000".parse().unwrap());
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

    let alone = Settings::new(0, vec!["10.0.0.1:7000".parse().unwrap()]);
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
