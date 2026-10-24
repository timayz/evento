//! End-to-end test of the framed-TCP transport **over mutual TLS with per-node
//! certificate identities**: a 3-node Accord cluster wired across real localhost
//! sockets, every connection wrapped in rustls with each node presenting its **own**
//! leaf certificate. Identity is enforced by **pinning** (`serve_tls_verified` +
//! `TlsClient::with_peer_certs`): a connection is authenticated to the `NodeId`
//! whose pinned certificate it presents, and that id — not the self-declared wire
//! `from` — is what the receiver trusts. Proves (a) the consensus path is identical
//! over authenticated, encrypted, per-node-pinned transport, and (b) a CA-valid but
//! un-pinned outsider is refused.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    serve_tls_verified, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, Journal,
    Message, MessageSink, Node, NodeId, PeerCerts, StaticTopology, TcpTransport, Timestamp,
    TlsClient, TxnId,
};
use evento_core::Event;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Per-node TLS material: a server acceptor (verifying client certs against the CA)
/// and a client connector, each backed by that node's own leaf certificate.
#[derive(Clone)]
struct NodeTls {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
}

/// The cluster's TLS material: per-node acceptors/connectors, the pin map
/// (`NodeId → leaf cert`), a CA-valid but **un-pinned** outsider connector, and the
/// shared server name (every leaf carries the `localhost` SAN so name verification
/// passes; identity is the pinned DER, not the name).
struct ClusterTls {
    nodes: Vec<NodeTls>,
    outsider: TlsConnector,
    pins: PeerCerts,
    server_name: ServerName<'static>,
}

/// Mints a fresh leaf certificate (with the shared `localhost` SAN) signed by the
/// CA, returning its chain, private key, and leaf DER (for pinning).
fn make_leaf(
    issuer: &rcgen::Issuer<'_, impl rcgen::SigningKey>,
) -> (
    Vec<CertificateDer<'static>>,
    PrivateKeyDer<'static>,
    CertificateDer<'static>,
) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let cert = params.signed_by(&key, issuer).unwrap();
    let der = cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
    (vec![der.clone()], key_der, der)
}

/// Builds the server + client rustls configs for one node's leaf, both anchored to
/// `roots` (mutual TLS: the server verifies client certs too).
fn configs(
    roots: Arc<RootCertStore>,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> (ServerConfig, ClientConfig) {
    let verifier = rustls::server::WebPkiClientVerifier::builder(roots.clone())
        .build()
        .unwrap();
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain.clone(), key.clone_key())
        .unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .unwrap();
    (server, client)
}

/// Builds a CA and `n` distinct per-node leaf certificates (plus one un-pinned
/// outsider leaf), and the rustls configs for per-node-pinned mutual TLS.
fn cluster_tls(n: u64) -> ClusterTls {
    // rustls 0.23 needs a process-wide crypto provider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert.der().clone()).unwrap();
    let roots = Arc::new(roots);

    let mut nodes = Vec::new();
    let mut pins: PeerCerts = HashMap::new();
    for i in 0..n {
        let (chain, key, der) = make_leaf(&issuer);
        pins.insert(NodeId(i), der);
        let (server, client) = configs(roots.clone(), chain, key);
        nodes.push(NodeTls {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
        });
    }

    // A CA-valid leaf that is NOT in the pin map — a would-be impostor.
    let (chain, key, _der) = make_leaf(&issuer);
    let (_s, outsider_client) = configs(roots, chain, key);

    ClusterTls {
        nodes,
        outsider: TlsConnector::from(Arc::new(outsider_client)),
        pins,
        server_name: ServerName::try_from("localhost").unwrap(),
    }
}

/// A cluster whose nodes communicate over per-node-pinned mutual-TLS TCP.
struct TlsCluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TlsCluster {
    async fn start(n: u64) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let tls = cluster_tls(n);

        let mut listeners = Vec::new();
        let mut peers: HashMap<NodeId, std::net::SocketAddr> = HashMap::new();
        for &id in &ids {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap());
            listeners.push((id, listener));
        }

        let mut nodes = Vec::new();
        let mut stores = Vec::new();
        let mut tasks = Vec::new();

        for (id, listener) in listeners {
            let node_tls = tls.nodes[id.0 as usize].clone();
            let (inbox_tx, inbox_rx) = mpsc::channel(1024);
            // Authenticate inbound peers by their pinned certificate.
            tasks.push(serve_tls_verified(
                listener,
                inbox_tx,
                node_tls.acceptor,
                tls.pins.clone(),
            ));

            let clock = Arc::new(HybridLogicalClock::new(id));
            // Pin every peer's certificate on the outbound side too.
            let client = TlsClient::new(node_tls.connector, tls.server_name.clone())
                .with_peer_certs(tls.pins.clone());
            let sink: Arc<dyn MessageSink> =
                Arc::new(TcpTransport::with_tls(id, peers.clone(), client));
            let store = Arc::new(InMemoryDataStore::new());
            let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
            let topology = Arc::new(StaticTopology::new(id, ids.clone()));
            let node = Node::new(
                id,
                topology,
                clock,
                sink,
                Arc::clone(&store) as Arc<dyn DataStore>,
                journal,
            );

            tasks.push(node.start(inbox_rx));
            nodes.push(node);
            stores.push(store);
        }

        TlsCluster {
            nodes,
            stores,
            _tasks: tasks,
        }
    }

    async fn await_applied(&self, expected_len: usize) {
        for _ in 0..400 {
            if self
                .stores
                .iter()
                .all(|s| s.applied_log().len() >= expected_len)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("replicas did not converge to {expected_len} applied entries over mTLS");
    }

    fn assert_identical_order(&self, expected_len: usize) -> Vec<(TxnId, bool)> {
        let order: Vec<(TxnId, bool)> = self.stores[0]
            .applied_log()
            .iter()
            .map(|e| (e.txn, e.conflict))
            .collect();
        assert_eq!(order.len(), expected_len);
        for (i, store) in self.stores.iter().enumerate() {
            let this: Vec<(TxnId, bool)> = store
                .applied_log()
                .iter()
                .map(|e| (e.txn, e.conflict))
                .collect();
            assert_eq!(this, order, "replica {i} diverged");
        }
        order
    }
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_over_per_node_mutual_tls() {
    let cluster = TlsCluster::start(3).await;

    for i in 0..4u32 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            cluster.nodes[(i % 3) as usize].write(vec![event(&format!("acc-{i}"), 1, "Opened")]),
        )
        .await
        .expect("write timed out")
        .expect("write failed over mTLS");
        assert!(!outcome.conflict);
    }

    cluster.await_applied(4).await;
    let order = cluster.assert_identical_order(4);
    assert!(order.iter().all(|(_, conflict)| !conflict));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolves_a_conflict_over_per_node_mutual_tls() {
    let cluster = TlsCluster::start(3).await;

    let n0 = cluster.nodes[0].clone();
    let n1 = cluster.nodes[1].clone();
    let w0 = tokio::spawn(async move { n0.write(vec![event("acc", 1, "A")]).await.unwrap() });
    let w1 = tokio::spawn(async move { n1.write(vec![event("acc", 1, "B")]).await.unwrap() });

    let o0 = tokio::time::timeout(Duration::from_secs(10), w0)
        .await
        .expect("w0 timed out")
        .unwrap();
    let o1 = tokio::time::timeout(Duration::from_secs(10), w1)
        .await
        .expect("w1 timed out")
        .unwrap();

    assert!(
        o0.conflict ^ o1.conflict,
        "exactly one write must conflict over mTLS, got {o0:?} {o1:?}"
    );

    cluster.await_applied(2).await;
    let order = cluster.assert_identical_order(2);
    assert_eq!(order.iter().filter(|(_, c)| !c).count(), 1);
}

fn applied_msg(n: u64) -> Message {
    Message::Applied {
        txn: TxnId(Timestamp {
            micros: n,
            logical: 0,
            node: NodeId(n),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpinned_outsider_is_refused_while_a_pinned_peer_is_served() {
    // A single verified node. A CA-valid but **un-pinned** outsider cannot get its
    // frames handled (its connection is dropped at the identity check), whereas a
    // pinned peer's frames are handled normally.
    let tls = cluster_tls(2); // pins for NodeId(0) and NodeId(1)
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    let _serve = serve_tls_verified(
        listener,
        inbox_tx,
        tls.nodes[0].acceptor.clone(),
        tls.pins.clone(),
    );

    let node = Node::new(
        NodeId(0),
        Arc::new(StaticTopology::new(NodeId(0), vec![NodeId(0)])),
        Arc::new(HybridLogicalClock::new(NodeId(0))),
        Arc::new(evento_accord::InMemoryNetwork::new().sink(NodeId(0))) as Arc<dyn MessageSink>,
        Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    let _loop = node.start(inbox_rx);

    let peers: HashMap<NodeId, std::net::SocketAddr> = HashMap::from([(NodeId(0), addr)]);

    // Outsider: a CA-valid leaf that is not in the pin map. Even framing itself as a
    // known id, its connection is refused before any frame is read.
    let outsider = TcpTransport::with_tls(
        NodeId(1),
        peers.clone(),
        TlsClient::new(tls.outsider.clone(), tls.server_name.clone())
            .with_peer_certs(tls.pins.clone()),
    );
    for n in 0..5 {
        outsider.send(NodeId(0), applied_msg(n)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        node.metrics().messages_handled,
        0,
        "the un-pinned outsider's frames must never be handled"
    );

    // A pinned peer (NodeId(1) with its real leaf) is served normally.
    let legit = TcpTransport::with_tls(
        NodeId(1),
        peers,
        TlsClient::new(tls.nodes[1].connector.clone(), tls.server_name.clone())
            .with_peer_certs(tls.pins.clone()),
    );
    legit.send(NodeId(0), applied_msg(1)).await.unwrap();
    for _ in 0..200 {
        if node.metrics().messages_handled > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        node.metrics().messages_handled > 0,
        "a pinned peer's frames must be handled"
    );
}
