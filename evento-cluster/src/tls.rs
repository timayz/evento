//! A node's certificate, and the authority its peers' are issued by: mutual
//! TLS between the nodes of a cluster where a node is **who its certificate
//! names**, not which certificate it holds.
//!
//! Every node holds a certificate issued by one authority (a CA of the
//! deployment's own), naming the node as its peers dial it: the host of its
//! entry in [`Settings::peers`](crate::host::Settings::peers) as a DNS name,
//! or its address as an IP one. A node dialling a peer accepts the peer's
//! certificate for the name it dialled, as any TLS client does; a node dialled
//! by a peer accepts the peer's certificate for the name it would dial that
//! peer at. Nothing about one node is handed to the others:
//!
//! - a node **added** while the cluster runs
//!   ([`Cluster::add_node`](crate::Cluster::add_node)) is accepted once its
//!   address reached the members, which is all they need of it;
//! - a certificate is **renewed** on one node by replacing its files: the node
//!   reads them again ([`NodeTls::reload`], which the host calls every few
//!   seconds) and presents the new one on its next connections, and its peers,
//!   which trust the authority, are not restarted. The authority's own file
//!   may list several certificates, so the authority is changed the same way,
//!   the old and the new one listed together meanwhile.
//!
//! One certificate valid for several nodes' names (a wildcard, a list) lets
//! any of them speak as another: the nodes are told apart as far as their
//! certificates tell them apart.
//!
//! A link a certificate keeps down is not a peer that is away, and is said
//! apart ([`LinkRefusal`]): in the log when it begins, and in
//! [`PeerState::refused`](crate::PeerState::refused).
//!
//! # An application's own channel
//!
//! What the nodes of one application say to each other besides the store's
//! traffic (over HTTP, say) can go over the same certificate: a clone of the
//! [`NodeTls`] handed to the host ([`Builder::tls`](crate::host::Builder::tls))
//! gives what the node accepts and dials with, as last read
//! ([`NodeTls::acceptor`], [`NodeTls::connector`]), and [`names`] lets in
//! only the nodes of the cluster, as the store's channel does. One authority,
//! one certificate per node, renewed in place for both channels.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio_rustls::rustls::client::verify_server_name;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use tokio_rustls::rustls::{self, AlertDescription, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::tcp::PeerAddr;

/// The TLS library the acceptor and the connector of a [`NodeTls`] are of,
/// for an application that uses them: the same version as this crate's.
pub use tokio_rustls;

/// The `[cluster.tls]` section of a deployment's configuration: where this
/// node's certificate is, and the authority every node's is issued by. The
/// three files of a `kubernetes.io/tls` secret as cert-manager fills it.
///
/// ```toml
/// [cluster.tls]
/// ca = "/etc/cluster/tls/ca.crt"
/// cert = "/etc/cluster/tls/tls.crt"
/// key = "/etc/cluster/tls/tls.key"
/// ```
///
/// The same section on every node, each over its own files. The files are
/// read again while the node runs: see [`NodeTls`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSettings {
    /// The certificates of the authority the nodes' are issued by, in PEM:
    /// one, or several while the authority is changed.
    pub ca: PathBuf,
    /// This node's certificate, in PEM, followed by the intermediates up to
    /// the authority if any. It names the node as its peers dial it.
    pub cert: PathBuf,
    /// The private key of `cert`, in PEM.
    pub key: PathBuf,
}

/// Why the link with a peer is down while the peer answers: what a node says
/// in place of a peer that is away ([`PeerState::refused`](crate::PeerState::refused)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkRefusal {
    /// This node does not accept the certificate the peer presents: issued
    /// by another authority, expired, or not naming the peer as this node
    /// knows it. With what was wrong with it.
    PeerCertificate(String),
    /// The peer does not accept this node's certificate, with what it said.
    /// When every peer says so, the wrong file is this node's.
    OwnCertificate(String),
    /// The peer does not know this node: none of its settings name it and
    /// the cluster keeps no address for it. A node not added yet
    /// ([`Cluster::add_node`](crate::Cluster::add_node)), or one whose
    /// address has not reached that peer.
    NotKnown,
    /// The TLS handshake failed otherwise, with how: a peer that does not
    /// speak TLS, say, while this node does.
    Handshake(String),
}

impl std::fmt::Display for LinkRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PeerCertificate(why) => {
                write!(f, "the peer's certificate is not accepted: {why}")
            }
            Self::OwnCertificate(why) => {
                write!(f, "the peer does not accept this node's certificate: {why}")
            }
            Self::NotKnown => f.write_str("the peer does not know this node"),
            Self::Handshake(why) => write!(f, "the TLS handshake failed: {why}"),
        }
    }
}

impl LinkRefusal {
    /// What a failed handshake, or a read cut short by the peer, says of the
    /// link; `None` for a failure of the network under it.
    pub(crate) fn of(err: &std::io::Error) -> Option<Self> {
        let tls = err.get_ref()?.downcast_ref::<rustls::Error>()?;
        Some(match tls {
            rustls::Error::InvalidCertificate(why) => Self::PeerCertificate(format!("{why:?}")),
            rustls::Error::AlertReceived(alert) if about_certificate(*alert) => {
                Self::OwnCertificate(format!("it answered {alert:?}"))
            }
            other => Self::Handshake(other.to_string()),
        })
    }
}

/// Whether a peer's alert is its verdict on the certificate it was shown.
fn about_certificate(alert: AlertDescription) -> bool {
    matches!(
        alert,
        AlertDescription::BadCertificate
            | AlertDescription::UnsupportedCertificate
            | AlertDescription::CertificateRevoked
            | AlertDescription::CertificateExpired
            | AlertDescription::CertificateUnknown
            | AlertDescription::CertificateRequired
            | AlertDescription::UnknownCA
            | AlertDescription::AccessDenied
            | AlertDescription::DecryptError
    )
}

/// The name a node at `addr` is to hold a certificate for: its host, or its
/// IP address.
pub(crate) fn name_of(addr: &PeerAddr) -> Result<ServerName<'static>, String> {
    match addr {
        PeerAddr::Socket(addr) => Ok(ServerName::IpAddress(addr.ip().into())),
        PeerAddr::Host { host, .. } => ServerName::try_from(host.clone())
            .map_err(|_| format!("{host:?} is not a name a certificate can hold")),
    }
}

/// Whether the certificate a peer presented (`presented`, its leaf first)
/// names the node reached at `addr`; otherwise what is wrong with it. The
/// handshake already checked it against the authority.
///
/// For an application's own channel between the nodes: the certificate a
/// caller presented (`peer_certificates()` of the accepted connection)
/// against the address the cluster knows that node at, so a certificate of
/// the authority's lets a machine in as the nodes it names and no other.
pub fn names(presented: Option<&[CertificateDer<'_>]>, addr: &PeerAddr) -> Result<(), String> {
    let leaf = presented
        .and_then(|chain| chain.first())
        .ok_or("no certificate was presented")?;
    let name = name_of(addr)?;
    let parsed = ParsedCertificate::try_from(leaf).map_err(|err| err.to_string())?;
    verify_server_name(&parsed, &name)
        .map_err(|_| format!("it does not name {addr}'s host, the node it says it is"))
}

/// What the files held when they were last read.
#[derive(PartialEq, Eq)]
struct Pem {
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl Pem {
    fn read(files: &TlsSettings) -> anyhow::Result<Self> {
        let read = |path: &Path| {
            std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))
        };
        Ok(Self {
            ca: read(&files.ca)?,
            cert: read(&files.cert)?,
            key: read(&files.key)?,
        })
    }

    /// The two sides of a node these make: it accepts the peers the
    /// authority issued a certificate to, and presents its own to them.
    fn sides(&self) -> anyhow::Result<(TlsAcceptor, TlsConnector)> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(&self.ca) {
            roots
                .add(cert.context("the authority's certificates are not PEM")?)
                .context("the authority's certificate is not one")?;
        }
        anyhow::ensure!(
            !roots.is_empty(),
            "the authority's file holds no certificate"
        );
        let roots = Arc::new(roots);
        let chain = CertificateDer::pem_slice_iter(&self.cert)
            .collect::<Result<Vec<_>, _>>()
            .context("the node's certificate is not PEM")?;
        anyhow::ensure!(!chain.is_empty(), "the node's file holds no certificate");
        let key = PrivateKeyDer::from_pem_slice(&self.key)
            .context("the node's key is not a PEM private key")?;

        // The process's provider when the application installed one.
        let provider = CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .context("cannot verify peers against the authority")?;
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .context("the crypto provider supports no TLS version")?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .context("the node's certificate and key do not go together")?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .context("the crypto provider supports no TLS version")?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .context("the node's certificate and key do not go together")?;
        Ok((
            TlsAcceptor::from(Arc::new(server)),
            TlsConnector::from(Arc::new(client)),
        ))
    }
}

/// What a node's connections are made with, as last read.
struct Current {
    pem: Pem,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
}

impl Current {
    fn of(pem: Pem) -> anyhow::Result<Self> {
        let (acceptor, connector) = pem.sides()?;
        Ok(Self {
            pem,
            acceptor,
            connector,
        })
    }
}

struct Inner {
    /// Where the certificate is read from again; `None` when it was given
    /// as bytes.
    files: Option<TlsSettings>,
    current: RwLock<Current>,
}

/// One node's side of the cluster's mutual TLS: its certificate and key, and
/// the authority its peers' certificates are issued by. Given to the host
/// ([`Builder::tls`](crate::host::Builder::tls), or
/// [`Settings::tls`](crate::host::Settings::tls) for the files), or to a
/// transport built by hand ([`TcpTransport::with_tls_named`](crate::TcpTransport::with_tls_named)
/// with [`serve_tls_named`](crate::serve_tls_named)). See the
/// [module](crate::tls) for who is accepted.
///
/// Cheap to clone: the clones are one certificate, replaced for all of them
/// by [`reload`](Self::reload) or [`replace`](Self::replace). A connection
/// made before keeps the certificate it was made with.
#[derive(Clone)]
pub struct NodeTls {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NodeTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeTls")
            .field("files", &self.inner.files)
            .finish_non_exhaustive()
    }
}

impl NodeTls {
    /// Reads the node's certificate, its key and the authority from the
    /// files `settings` name. An error when one cannot be read, is not PEM,
    /// or when the key is not the certificate's.
    pub fn from_files(settings: &TlsSettings) -> anyhow::Result<Self> {
        let current = Current::of(Pem::read(settings)?).with_context(|| {
            format!(
                "cannot use the certificate in {} with the key in {} and the authority in {}",
                settings.cert.display(),
                settings.key.display(),
                settings.ca.display()
            )
        })?;
        Ok(Self {
            inner: Arc::new(Inner {
                files: Some(settings.clone()),
                current: RwLock::new(current),
            }),
        })
    }

    /// The same from PEM held in memory: the authority's certificates, the
    /// node's certificate (and intermediates), its key. Renewed by
    /// [`replace`](Self::replace).
    pub fn from_pem(ca: &[u8], cert: &[u8], key: &[u8]) -> anyhow::Result<Self> {
        let pem = Pem {
            ca: ca.to_vec(),
            cert: cert.to_vec(),
            key: key.to_vec(),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                files: None,
                current: RwLock::new(Current::of(pem)?),
            }),
        })
    }

    /// Reads the files again, and makes the node's next connections with
    /// what they hold when it changed: `true` then. For a certificate
    /// renewed in place; the host does it every few seconds. On an error
    /// the node keeps what it had: files caught half replaced (the
    /// certificate without its key yet) are read again at the next call.
    /// Always `false` for a certificate given as bytes.
    pub fn reload(&self) -> anyhow::Result<bool> {
        let Some(files) = &self.inner.files else {
            return Ok(false);
        };
        let pem = Pem::read(files)?;
        if self.inner.current.read().expect("tls poisoned").pem == pem {
            return Ok(false);
        }
        self.install(pem)
            .with_context(|| format!("cannot use the certificate in {}", files.cert.display()))?;
        Ok(true)
    }

    /// Makes the node's next connections with this PEM instead; `true` when
    /// it differs from what the node had.
    pub fn replace(&self, ca: &[u8], cert: &[u8], key: &[u8]) -> anyhow::Result<bool> {
        let pem = Pem {
            ca: ca.to_vec(),
            cert: cert.to_vec(),
            key: key.to_vec(),
        };
        if self.inner.current.read().expect("tls poisoned").pem == pem {
            return Ok(false);
        }
        self.install(pem)?;
        Ok(true)
    }

    fn install(&self, pem: Pem) -> anyhow::Result<()> {
        let current = Current::of(pem)?;
        *self.inner.current.write().expect("tls poisoned") = current;
        Ok(())
    }

    /// What an inbound connection is accepted with, as last read: the node's
    /// certificate presented, and a client let in only with a certificate
    /// of the authority's. Which node the client is, is for the caller to
    /// check ([`names`]).
    ///
    /// For a channel of the application's own between the nodes. The
    /// configuration under it is [`TlsAcceptor::config`]; it is another
    /// `Arc` once the certificate was renewed, so what is built from it once
    /// (a server's configuration) is built again when
    /// [`Arc::ptr_eq`] says it changed. Taken at each connection instead,
    /// nothing is to follow.
    pub fn acceptor(&self) -> TlsAcceptor {
        self.inner
            .current
            .read()
            .expect("tls poisoned")
            .acceptor
            .clone()
    }

    /// What a peer is dialled with, as last read: the peer's certificate
    /// accepted when the authority issued it for the name dialled, and the
    /// node's own presented.
    ///
    /// For a channel of the application's own between the nodes. The
    /// configuration under it is [`TlsConnector::config`], which an HTTP
    /// client takes; it is another `Arc` once the certificate was renewed,
    /// so a client built from it is built again when [`Arc::ptr_eq`] says
    /// it changed.
    pub fn connector(&self) -> TlsConnector {
        self.inner
            .current
            .read()
            .expect("tls poisoned")
            .connector
            .clone()
    }
}
