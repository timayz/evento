//! Mutual TLS between the nodes, opened from [`Settings`] alone: each node
//! over a certificate of its own, issued by the test's authority for the
//! loopback address the node is dialled at (`127.0.0.<id + 1>`). A write on
//! one node is read on another; a node added while the cluster runs is let
//! in by its certificate, nothing of it handed to the members; a
//! certificate is replaced on one node while the others run; and a link a
//! certificate keeps down is told from a peer that is away, on both sides.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::host::Settings;
use evento_cluster::tls::tokio_rustls::rustls::pki_types::ServerName;
use evento_cluster::{
    Cluster, ClusterExecutor, LinkRefusal, NodeConfig, NodeHealth, NodeId, NodeTls, PeerAddr,
    TlsSettings,
};
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
    let dir = std::env::temp_dir().join(format!("evento-host-tls-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The loopback address node `id` is dialled at, and named by in its
/// certificate: one per node, so the nodes are told apart.
fn ip(id: u64) -> String {
    format!("127.0.0.{}", id + 1)
}

/// A free port on each of `count` nodes' addresses.
async fn free_addrs(count: u64) -> Vec<SocketAddr> {
    let mut listeners = Vec::new();
    for id in 0..count {
        listeners.push(
            tokio::net::TcpListener::bind(format!("{}:0", ip(id)))
                .await
                .unwrap(),
        );
    }
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// An authority that issues the nodes' certificates.
struct Authority {
    params: rcgen::CertificateParams,
    key: rcgen::KeyPair,
    pem: String,
}

impl Authority {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let pem = params.self_signed(&key).unwrap().pem();
        Self { params, key, pem }
    }

    /// A certificate for `name` (a host, or an address) and its key, in PEM.
    fn issue(&self, name: &str) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        let issuer = rcgen::Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// Where node `id` keeps its certificate.
fn tls_files(root: &Path, id: u64) -> TlsSettings {
    let dir = Member::dir(root, id).join("tls");
    TlsSettings {
        ca: dir.join("ca.crt"),
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
    }
}

/// Puts a certificate for `name`, issued by `issuer`, under node `id`, with
/// `trusted` as the authority the node accepts its peers by.
fn install(root: &Path, id: u64, trusted: &Authority, issuer: &Authority, name: &str) {
    let files = tls_files(root, id);
    std::fs::create_dir_all(files.ca.parent().unwrap()).unwrap();
    let (cert, key) = issuer.issue(name);
    std::fs::write(&files.ca, &trusted.pem).unwrap();
    std::fs::write(&files.cert, cert).unwrap();
    std::fs::write(&files.key, key).unwrap();
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

    /// Opens node `id` through the host, over the certificate under its
    /// directory, with the `peers` its settings name.
    async fn open(root: &Path, id: u64, peers: &[SocketAddr]) -> Self {
        Self::try_open(root, id, peers)
            .await
            .unwrap_or_else(|err| panic!("node {id} does not open: {err:#}"))
    }

    async fn try_open(root: &Path, id: u64, peers: &[SocketAddr]) -> anyhow::Result<Self> {
        let dir = Self::dir(root, id);
        std::fs::create_dir_all(&dir)?;
        let store = Fjall::open(dir.join("store"))?;
        let settings = Settings {
            founders: Some(FOUNDERS as u64),
            absent_after_secs: 0,
            tls: Some(tls_files(root, id)),
            ..Settings::new(id, peers.to_vec())
        };
        let (cluster, executor) = Cluster::builder(store)
            .settings(Some(settings))
            .state_dir(&dir)
            .node_config(|config| NodeConfig {
                collect_timeout: Duration::from_secs(2),
                ..config
            })
            .open()
            .await?;
        Ok(Self { cluster, executor })
    }

    async fn write(&self, id: &str, version: u16) {
        // A write right after the members changed, or a link came up, can
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

    /// Why this node's link with `peer` is refused, if it is.
    fn refused(&self, peer: u64) -> Option<LinkRefusal> {
        refused(&self.cluster.health(), peer)
    }

    /// Whether this node holds a connection to `peer` and hears it.
    fn linked(&self, peer: u64) -> bool {
        self.cluster.health().peers.iter().any(|state| {
            state.id == NodeId(peer)
                && state.reached
                && state.refused.is_none()
                && state.silent_for < Duration::from_secs(1)
        })
    }
}

fn refused(health: &NodeHealth, peer: u64) -> Option<LinkRefusal> {
    health
        .peers
        .iter()
        .find(|state| state.id == NodeId(peer))
        .and_then(|state| state.refused.clone())
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

/// Opens the founders, each over the certificate already under it.
async fn found(root: &Path, addrs: &[SocketAddr]) -> Vec<Member> {
    let founders = &addrs[..FOUNDERS];
    let (a, b, c) = tokio::join!(
        Member::open(root, 0, founders),
        Member::open(root, 1, founders),
        Member::open(root, 2, founders),
    );
    vec![a, b, c]
}

/// The consensus traffic of a cluster opened from its settings goes over
/// mutual TLS, and nothing else changes: a write on one node is read on
/// another, and no link is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_nodes_talk_over_tls_from_their_settings() {
    let root = temp_root("talk");
    let addrs = free_addrs(3).await;
    let authority = Authority::new();
    for id in 0..3 {
        install(&root, id, &authority, &authority, &ip(id));
    }
    let nodes = found(&root, &addrs).await;
    for node in &nodes {
        node.cluster
            .until_confirmed(Duration::from_secs(30))
            .await
            .unwrap();
    }

    nodes[0].write("acc", 1).await;
    nodes[1]
        .await_count("acc", 1, "node 1 reads node 0's write")
        .await;
    nodes[2].write("acc", 2).await;
    nodes[0]
        .await_count("acc", 2, "node 0 reads node 2's write")
        .await;

    for node in &nodes {
        let me = node.cluster.id().0;
        for peer in (0..3).filter(|&peer| peer != me) {
            eventually("every node reaches every other", || async {
                node.linked(peer)
            })
            .await;
        }
    }
    for node in nodes {
        node.cluster.shutdown().await;
    }
}

/// A node added while the cluster runs is let in by its certificate: the
/// members are handed its address, as without TLS, and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_added_while_the_cluster_runs_is_let_in_by_its_certificate() {
    let root = temp_root("added");
    let addrs = free_addrs(4).await;
    let authority = Authority::new();
    for id in 0..4 {
        install(&root, id, &authority, &authority, &ip(id));
    }
    let nodes = found(&root, &addrs).await;
    nodes[0].write("acc", 1).await;

    // The new node's settings name the four; the members' never did.
    let opening = tokio::spawn({
        let (root, addrs) = (root.clone(), addrs.clone());
        async move { Member::try_open(&root, 3, &addrs).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!opening.is_finished(), "no member added it yet");

    nodes[1]
        .cluster
        .add_node(NodeId(3), addrs[3])
        .await
        .unwrap();
    let added = tokio::time::timeout(Duration::from_secs(60), opening)
        .await
        .expect("the added node opens")
        .unwrap()
        .unwrap();
    assert!(added.cluster.health().serving());
    assert_eq!(added.count("acc").await, 1, "the store was copied");

    added.write("acc", 2).await;
    for node in &nodes {
        node.await_count("acc", 2, "the members read the new node's write")
            .await;
        eventually("every member reaches the new node", || async {
            node.linked(3)
        })
        .await;
    }
    for node in nodes.into_iter().chain([added]) {
        node.cluster.shutdown().await;
    }
}

/// A node whose certificate names another node is refused by its peers,
/// which say so of it, and says itself that its own certificate is the one
/// refused: neither side reads it as a node that is away. Once the right
/// certificate is put in its files, the node presents it and is let in, and
/// no node was restarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_certificate_is_said_on_both_sides_and_replaced_in_place() {
    let root = temp_root("replaced");
    let addrs = free_addrs(3).await;
    let authority = Authority::new();
    for id in 0..2 {
        install(&root, id, &authority, &authority, &ip(id));
    }
    // Node 2 holds a certificate of the authority's, for another address.
    install(&root, 2, &authority, &authority, "127.0.0.9");
    let nodes = found(&root, &addrs).await;

    // The two others are a majority, and say what keeps the third out.
    nodes[0].write("acc", 1).await;
    nodes[1]
        .await_count("acc", 1, "node 1 reads node 0's write")
        .await;
    for node in &nodes[..2] {
        eventually("a member says node 2's certificate is refused", || async {
            matches!(node.refused(2), Some(LinkRefusal::PeerCertificate(_)))
        })
        .await;
        assert_eq!(node.refused(1 - node.cluster.id().0), None);
    }
    for peer in 0..2 {
        eventually("node 2 says its own certificate is refused", || async {
            matches!(nodes[2].refused(peer), Some(LinkRefusal::OwnCertificate(_)))
        })
        .await;
    }
    assert_eq!(nodes[2].count("acc").await, 0, "nothing reached node 2");

    // The files are replaced, as a renewal does: read again within seconds.
    install(&root, 2, &authority, &authority, &ip(2));
    for (node, peer) in [(0, 2), (1, 2), (2, 0), (2, 1)] {
        eventually("the links come up", || async { nodes[node].linked(peer) }).await;
    }
    nodes[2].await_count("acc", 1, "node 2 catches up").await;
    nodes[2].write("acc", 2).await;
    nodes[0]
        .await_count("acc", 2, "node 0 reads node 2's write")
        .await;

    for node in nodes {
        node.cluster.shutdown().await;
    }
}

/// A certificate issued by another authority than the cluster's: the peers
/// refuse it in the handshake, and the node is told so by their alert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_certificate_of_another_authority_is_refused() {
    let root = temp_root("authority");
    let addrs = free_addrs(3).await;
    let authority = Authority::new();
    let other = Authority::new();
    for id in 0..2 {
        install(&root, id, &authority, &authority, &ip(id));
    }
    // Node 2 trusts the cluster's authority and holds another's certificate.
    install(&root, 2, &authority, &other, &ip(2));
    let nodes = found(&root, &addrs).await;

    nodes[0].write("acc", 1).await;
    for node in &nodes[..2] {
        eventually("a member says node 2's certificate is refused", || async {
            matches!(node.refused(2), Some(LinkRefusal::PeerCertificate(_)))
        })
        .await;
    }
    for peer in 0..2 {
        eventually("node 2 says its own certificate is refused", || async {
            matches!(nodes[2].refused(peer), Some(LinkRefusal::OwnCertificate(_)))
        })
        .await;
    }
    for node in nodes {
        node.cluster.shutdown().await;
    }
}

/// Files that are no certificate end the open, before anything is bound.
#[tokio::test]
async fn a_certificate_that_cannot_be_used_ends_the_open() {
    let root = temp_root("unusable");
    let addrs = free_addrs(3).await;
    let authority = Authority::new();

    let missing = Member::try_open(&root, 0, &addrs).await.err().unwrap();
    assert!(
        format!("{missing:#}").contains("cluster.tls"),
        "{missing:#}"
    );

    // Another certificate's key.
    install(&root, 0, &authority, &authority, &ip(0));
    let (_, key) = authority.issue(&ip(0));
    std::fs::write(tls_files(&root, 0).key, key).unwrap();
    let mismatched = Member::try_open(&root, 0, &addrs).await.err().unwrap();
    assert!(
        format!("{mismatched:#}").contains("do not go together"),
        "{mismatched:#}"
    );

    // The same files, right: the node opens, and a certificate held in
    // memory is renewed by the application.
    install(&root, 0, &authority, &authority, &ip(0));
    let files = tls_files(&root, 0);
    let read = |path: &Path| std::fs::read(path).unwrap();
    let tls = NodeTls::from_pem(&read(&files.ca), &read(&files.cert), &read(&files.key)).unwrap();
    assert!(!tls.reload().unwrap(), "nothing to read again");
    let (cert, key) = authority.issue(&ip(0));
    assert!(tls
        .replace(&read(&files.ca), cert.as_bytes(), key.as_bytes())
        .unwrap());
    assert!(!tls
        .replace(&read(&files.ca), cert.as_bytes(), key.as_bytes())
        .unwrap());
    let from_files = NodeTls::from_files(&files).unwrap();
    assert!(!from_files.reload().unwrap());
    std::fs::write(&files.cert, cert).unwrap();
    assert!(from_files.reload().is_err(), "the key is the old one's");
    std::fs::write(&files.key, key).unwrap();
    assert!(from_files.reload().unwrap());
}

/// An application's own channel between the nodes goes over the nodes'
/// certificate: what a [`NodeTls`] accepts and dials with is the
/// application's to use, a caller is checked against the node it says it is,
/// and a renewal reaches every clone as another configuration.
#[tokio::test]
async fn an_application_channel_goes_over_the_nodes_certificate() {
    let root = temp_root("application");
    let authority = Authority::new();
    for id in 0..2 {
        install(&root, id, &authority, &authority, &ip(id));
    }
    let caller = NodeTls::from_files(&tls_files(&root, 0)).unwrap();
    let served = NodeTls::from_files(&tls_files(&root, 1)).unwrap();
    // The clone an application keeps of what it handed the host.
    let kept = served.clone();

    let listener = tokio::net::TcpListener::bind(format!("{}:0", ip(1)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let name = ServerName::try_from(ip(1)).unwrap();
    // One connection: the certificate the caller presented, as the server
    // saw it, or how the handshake ended.
    let call = |connector: evento_cluster::tls::tokio_rustls::TlsConnector| {
        let (listener, acceptor, name) = (&listener, kept.acceptor(), name.clone());
        async move {
            let dial = async {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                connector.connect(name, stream).await
            };
            let accept = async {
                let (stream, _) = listener.accept().await?;
                acceptor.accept(stream).await
            };
            let (dialled, accepted) = tokio::join!(dial, accept);
            let accepted = accepted?;
            dialled?;
            let presented = accepted.get_ref().1.peer_certificates().map(<[_]>::to_vec);
            std::io::Result::Ok(presented)
        }
    };

    let presented = call(caller.connector()).await.unwrap();
    let at = |id: u64| -> PeerAddr { format!("{}:7000", ip(id)).parse().unwrap() };
    evento_cluster::tls::names(presented.as_deref(), &at(0)).unwrap();
    let other = evento_cluster::tls::names(presented.as_deref(), &at(2)).unwrap_err();
    assert!(other.contains("does not name"), "{other}");
    assert!(evento_cluster::tls::names(None, &at(0)).is_err());

    // A certificate of another authority is not let in.
    let stranger = Authority::new();
    install(&root, 2, &authority, &stranger, &ip(2));
    let outsider = NodeTls::from_files(&tls_files(&root, 2)).unwrap();
    assert!(call(outsider.connector()).await.is_err());

    // Renewed in place: the clone holds another configuration, the one the
    // next connections are made with, and nothing changes until then.
    let before = kept.acceptor().config().clone();
    let dialled_with = kept.connector().config().clone();
    assert!(Arc::ptr_eq(&before, kept.acceptor().config()));
    install(&root, 1, &authority, &authority, &ip(1));
    assert!(served.reload().unwrap());
    assert!(!Arc::ptr_eq(&before, kept.acceptor().config()));
    assert!(!Arc::ptr_eq(&dialled_with, kept.connector().config()));
    call(caller.connector()).await.unwrap();
}

#[test]
fn the_settings_name_the_certificate_files() {
    let text = r#"{
        "node_id": 0,
        "peers": ["app-0.app:7000", "app-1.app:7000"],
        "listen": "0.0.0.0:7000",
        "tls": {"ca": "/tls/ca.crt", "cert": "/tls/tls.crt", "key": "/tls/tls.key"}
    }"#;
    let settings: Settings = serde_json::from_str(text).unwrap();
    settings.validate().unwrap();
    assert_eq!(
        settings.tls,
        Some(TlsSettings {
            ca: "/tls/ca.crt".into(),
            cert: "/tls/tls.crt".into(),
            key: "/tls/tls.key".into(),
        })
    );
    let clear: Settings =
        serde_json::from_str(r#"{"node_id": 0, "peers": ["a:1"], "listen": "0.0.0.0:1"}"#).unwrap();
    assert_eq!(clear.tls, None);
}
