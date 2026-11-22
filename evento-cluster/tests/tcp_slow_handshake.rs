//! A peer that takes the connection and is slow to finish the TLS handshake
//! is waited for (#610). On a machine short of CPU a handshake takes
//! seconds; under the connect's two-second bound it was given up midway and
//! begun again at every dial, and the link never came up. The handshake has
//! its own bound now ([`HANDSHAKE_TIMEOUT`]).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use evento_cluster::protocol::{Message, Timestamp, TxnId};
use evento_cluster::tcp::{CONNECT_TIMEOUT, HANDSHAKE_TIMEOUT};
use evento_cluster::{serve_tls_named, MessageSink, NodeId, NodeTls, TcpTransport};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// How long the peer leaves a connection alone before its handshake begins:
/// past the connect's bound, within the handshake's.
const SLOW: Duration = Duration::from_secs(3);

fn applied(n: u64) -> Message {
    Message::Applied {
        txn: TxnId(Timestamp {
            micros: n,
            logical: 0,
            node: NodeId(0),
        }),
    }
}

/// The loopback address node `id` is dialled at, and named by in its
/// certificate.
fn ip(id: u64) -> String {
    format!("127.0.0.{}", id + 1)
}

/// A certificate for each of two nodes, issued by one authority.
fn certificates() -> Vec<NodeTls> {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_pem = ca.self_signed(&ca_key).unwrap().pem();
    (0..2)
        .map(|id| {
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec![ip(id)]).unwrap();
            let issuer = rcgen::Issuer::from_params(&ca, &ca_key);
            let cert = params.signed_by(&key, &issuer).unwrap();
            NodeTls::from_pem(
                ca_pem.as_bytes(),
                cert.pem().as_bytes(),
                key.serialize_pem().as_bytes(),
            )
            .unwrap()
        })
        .collect()
}

/// Takes the connections to `listener` at once and hands each to `to` only
/// after [`SLOW`]: a peer whose process is there and gets no CPU for a while.
async fn slow_front(listener: TcpListener, to: SocketAddr) {
    loop {
        let Ok((mut inbound, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            tokio::time::sleep(SLOW).await;
            let Ok(mut outbound) = TcpStream::connect(to).await else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_slow_to_finish_the_handshake_is_reached() {
    assert!(CONNECT_TIMEOUT < SLOW && SLOW < HANDSHAKE_TIMEOUT);
    let tls = certificates();

    // Node 1, behind a front that holds each connection for a while.
    let listener = TcpListener::bind(format!("{}:0", ip(1))).await.unwrap();
    let front = TcpListener::bind(format!("{}:0", ip(1))).await.unwrap();
    let (addr, front_addr) = (listener.local_addr().unwrap(), front.local_addr().unwrap());
    let own: SocketAddr = format!("{}:1", ip(0)).parse().unwrap();
    let (inbox_tx, mut inbox) = mpsc::channel(16);
    let peer = Arc::new(TcpTransport::with_tls_named(
        NodeId(1),
        HashMap::from([(NodeId(0), own), (NodeId(1), addr)]),
        tls[1].clone(),
    ));
    let serving = serve_tls_named(listener, inbox_tx, tls[1].clone(), peer);
    let fronting = tokio::spawn(slow_front(front, addr));

    let transport = TcpTransport::with_tls_named(
        NodeId(0),
        HashMap::from([(NodeId(0), own), (NodeId(1), front_addr)]),
        tls[0].clone(),
    );
    // The frame that prompts the dial waited too long by the time the link
    // is up, and is dropped; the link stays.
    transport.send(NodeId(1), applied(1)).await.unwrap();
    tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        while !transport.reaches(NodeId(1)) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the peer is reached once its handshake is done");
    assert_eq!(transport.refusal(NodeId(1)), None);

    transport.send(NodeId(1), applied(2)).await.unwrap();
    let envelope = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
        .await
        .expect("a frame sent over the link arrives")
        .unwrap();
    assert_eq!(envelope.from, NodeId(0));
    assert!(matches!(envelope.message, Message::Applied { txn } if txn.0.micros == 2));

    serving.abort();
    fronting.abort();
}
