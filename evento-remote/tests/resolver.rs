//! The per-connection resolver: a token picks the executor a connection is
//! served with, a bad one is refused, a session's guard lives as long as the
//! connection, and the server can end a connection on its own.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evento_core::cursor::Args;
use evento_core::{Event, Executor};
use evento_fjall::Fjall;
use evento_remote::{serve_with, Client, Hello, Rejection, Resolver, Session};
use futures_util::SinkExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A store, the guard its sessions keep, and the signal that ends them.
type Entry = (Fjall, Arc<()>, watch::Sender<bool>);

/// One Fjall store per token; a guard counter and a close signal per token
/// so the tests can watch the connection lifecycle from the host's side.
#[derive(Default)]
struct ByToken {
    stores: Mutex<HashMap<String, Entry>>,
    hellos: Mutex<Vec<Hello>>,
}

impl ByToken {
    fn add(&self, token: &str) -> (Arc<()>, watch::Sender<bool>) {
        let guard = Arc::new(());
        let (close_tx, _) = watch::channel(false);
        self.stores.lock().unwrap().insert(
            token.to_owned(),
            (Fjall::temporary().unwrap(), guard.clone(), close_tx.clone()),
        );
        (guard, close_tx)
    }

    fn remove(&self, token: &str) {
        self.stores.lock().unwrap().remove(token);
    }
}

/// The resolver handed to the server, sharing the registry with the test.
struct Shared(Arc<ByToken>);

#[async_trait::async_trait]
impl Resolver for Shared {
    type Executor = Fjall;

    async fn resolve(&self, hello: &Hello) -> Result<Session<Fjall>, Rejection> {
        self.0.hellos.lock().unwrap().push(hello.clone());
        let token = hello.token.as_deref().ok_or(Rejection::Unauthorized)?;
        let stores = self.0.stores.lock().unwrap();
        let (store, guard, close_tx) = stores.get(token).ok_or(Rejection::Unauthorized)?;
        Ok(Session::new(store.clone())
            .keep(Arc::clone(guard))
            .close_when(close_tx.subscribe()))
    }
}

async fn start(resolver: Arc<ByToken>) -> (std::net::SocketAddr, evento_remote::ServerHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, serve_with(listener, Shared(resolver)))
}

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_id: aggregate_id.to_owned(),
        aggregate_type: "resolver/Probe".to_owned(),
        version,
        name: "Probed".to_owned(),
        ..Default::default()
    }
}

async fn count(client: &Client) -> usize {
    client
        .read(
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(100, None),
            None,
        )
        .await
        .unwrap()
        .edges
        .len()
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_token_picks_its_own_store() {
    let resolver = Arc::new(ByToken::default());
    resolver.add("alpha");
    resolver.add("beta");
    let (addr, server) = start(resolver.clone()).await;

    let alpha = Client::builder(addr)
        .token("alpha")
        .connect()
        .await
        .unwrap();
    let beta = Client::builder(addr).token("beta").connect().await.unwrap();

    alpha.write(vec![event("a", 1)]).await.unwrap();
    assert_eq!(count(&alpha).await, 1);
    assert_eq!(
        count(&beta).await,
        0,
        "a token must not see another's store"
    );

    let hellos = resolver.hellos.lock().unwrap().clone();
    assert_eq!(hellos.len(), 2);
    assert!(hellos.iter().all(|h| h.peer.ip().is_loopback()));
    assert_eq!(hellos[0].token.as_deref(), Some("alpha"));
    assert_eq!(hellos[1].token.as_deref(), Some("beta"));

    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_or_missing_token_is_refused() {
    let resolver = Arc::new(ByToken::default());
    resolver.add("alpha");
    let (addr, server) = start(resolver).await;

    let Err(err) = Client::builder(addr).token("nope").connect().await else {
        panic!("an unknown token must be refused");
    };
    assert!(format!("{err:#}").contains("unauthorized"), "{err:#}");
    assert!(err.downcast_ref::<Rejection>().is_some(), "{err:#}");

    let Err(err) = Client::connect(addr).await else {
        panic!("a missing token must be refused");
    };
    assert!(format!("{err:#}").contains("unauthorized"), "{err:#}");

    server.shutdown().await;
}

#[tokio::test]
async fn the_guard_lives_as_long_as_the_connection() {
    let resolver = Arc::new(ByToken::default());
    let (guard, _) = resolver.add("alpha");
    let (addr, server) = start(resolver.clone()).await;

    // The resolver's map holds one reference; the session the other.
    let client = Client::builder(addr)
        .token("alpha")
        .connect()
        .await
        .unwrap();
    eventually("session holds the guard", || Arc::strong_count(&guard) == 3).await;

    drop(client);
    eventually("guard released on disconnect", || {
        Arc::strong_count(&guard) == 2
    })
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn the_server_can_end_a_connection() {
    let resolver = Arc::new(ByToken::default());
    let (_, close_tx) = resolver.add("alpha");
    let (addr, server) = start(resolver.clone()).await;

    let client = Client::builder(addr)
        .token("alpha")
        .request_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    assert_eq!(count(&client).await, 0);

    // Revoke: the connection is ended, and the resolver refuses the reconnect.
    resolver.remove("alpha");
    close_tx.send(true).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match client
            .read(
                None,
                None,
                evento_core::TenantFilter::All,
                Args::forward(1, None),
                None,
            )
            .await
        {
            Ok(_) => assert!(tokio::time::Instant::now() < deadline, "still served"),
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("connection lost") || msg.contains("timed out"),
                    "unexpected error: {msg}"
                );
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Only one more hello: the refused reconnect (then backoff).
    eventually("a refused reconnect", || {
        resolver.hellos.lock().unwrap().len() >= 2
    })
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn the_first_frame_must_be_hello() {
    let resolver = Arc::new(ByToken::default());
    resolver.add("alpha");
    let (addr, server) = start(resolver.clone()).await;

    // Speak the protocol by hand: a `Read` before any `Hello`.
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    // A hand-built frame: the header (`Er`, version 4, client kind) and an
    // arbitrary payload — anything but a hello closes the link, so the
    // payload's shape does not matter beyond not being one.
    let mut frame = b"Er\x04\x01".to_vec();
    frame.extend_from_slice(&[0xff; 8]);
    framed.send(bytes::Bytes::from(frame)).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        use futures_util::StreamExt;
        framed.next().await.is_none()
    })
    .await
    .unwrap();
    assert!(closed, "the connection must be closed");
    assert!(resolver.hellos.lock().unwrap().is_empty());

    server.shutdown().await;
}
