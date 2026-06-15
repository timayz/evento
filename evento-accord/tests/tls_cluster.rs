//! End-to-end test of the framed-TCP transport **over mutual TLS**: a 3-node
//! Accord cluster wired across real localhost sockets, every connection wrapped
//! in rustls with each side presenting a certificate the other verifies against a
//! shared CA. Proves the consensus path is identical over an authenticated,
//! encrypted transport — replication into one serial order, and one-winner
//! conflict resolution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    serve_tls, DataStore, HybridLogicalClock, InMemoryDataStore, InMemoryJournal, Journal,
    MessageSink, Node, NodeId, StaticTopology, TcpTransport, TlsClient, TxnId,
};
use evento_core::Event;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// The shared TLS material for the cluster: an acceptor (server side, verifying
/// client certs) and a connector (client side, with its own cert).
#[derive(Clone)]
struct ClusterTls {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

/// Builds a CA, a single leaf certificate (identity `localhost`) signed by it, and
/// the rustls configs for mutual TLS — every node shares the leaf as both its
/// server and client identity, all chaining to the CA.
fn cluster_tls() -> ClusterTls {
    // rustls 0.23 needs a process-wide crypto provider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let ca_der = ca_cert.der().clone();
    let leaf_chain = vec![leaf_cert.der().clone()];
    let leaf_key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));

    let mut roots = RootCertStore::empty();
    roots.add(ca_der).unwrap();
    let roots = Arc::new(roots);

    let verifier = rustls::server::WebPkiClientVerifier::builder(roots.clone())
        .build()
        .unwrap();
    let server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(leaf_chain.clone(), leaf_key_der.clone_key())
        .unwrap();

    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(leaf_chain, leaf_key_der)
        .unwrap();

    ClusterTls {
        acceptor: TlsAcceptor::from(Arc::new(server_config)),
        connector: TlsConnector::from(Arc::new(client_config)),
        server_name: ServerName::try_from("localhost").unwrap(),
    }
}

/// A cluster whose nodes communicate over mutual-TLS TCP.
struct TlsCluster {
    nodes: Vec<Node>,
    stores: Vec<Arc<InMemoryDataStore>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TlsCluster {
    async fn start(n: u64) -> Self {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let tls = cluster_tls();

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
            let (inbox_tx, inbox_rx) = mpsc::channel(1024);
            tasks.push(serve_tls(listener, inbox_tx, tls.acceptor.clone()));

            let clock = Arc::new(HybridLogicalClock::new(id));
            let client = TlsClient::new(tls.connector.clone(), tls.server_name.clone());
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
async fn replicates_over_mutual_tls() {
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
async fn resolves_a_conflict_over_mutual_tls() {
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
