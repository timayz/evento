//! The TLS variants: a client with the right root talks to a `serve_tls`
//! server through the whole conformance path, a plaintext client cannot.

use std::sync::Arc;
use std::time::Duration;

use evento_remote::{serve_tls, Client};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// A self-signed `localhost` certificate: the server's acceptor and a client
/// connector trusting exactly that certificate.
fn tls() -> (TlsAcceptor, TlsConnector) {
    // rustls 0.23 needs a process-wide crypto provider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let cert = params.self_signed(&key).unwrap();
    let der = cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));

    let mut roots = RootCertStore::empty();
    roots.add(der.clone()).unwrap();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![der], key)
        .unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (
        TlsAcceptor::from(Arc::new(server)),
        TlsConnector::from(Arc::new(client)),
    )
}

#[tokio::test]
async fn subscriptions_run_over_tls() -> anyhow::Result<()> {
    let (acceptor, connector) = tls();
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = serve_tls(listener, acceptor, fjall);

    let client = Client::builder(addr)
        .tls(connector, ServerName::try_from("localhost")?)
        .connect()
        .await?;
    evento_test::subscribe_low_latency(&client).await
}

#[tokio::test]
async fn a_plaintext_client_cannot_reach_a_tls_server() -> anyhow::Result<()> {
    let (acceptor, _) = tls();
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = serve_tls(listener, acceptor, fjall);

    let Err(err) = Client::builder(addr)
        .request_timeout(Duration::from_secs(2))
        .connect()
        .await
    else {
        panic!("a plaintext hello is not a TLS handshake");
    };
    // The exact failure depends on how fast the server tears the link down
    // (a reset, a closed stream, a garbage reply); what matters is that no
    // session exists.
    assert!(!format!("{err:#}").is_empty());
    Ok(())
}
