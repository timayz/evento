//! A client built before its server answers (issue #600): it exists at once,
//! says how it stands, fails its calls meanwhile, learns its routing key at
//! the first `Hello`, and a supervised subscription started on it handles its
//! first event when the server comes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bank::aggregator::AccountOpened;
use bank::{AccountType, OpenAccount};
use evento::cursor::Args;
use evento::metadata::Event;
use evento::subscription::{Context, State, SubscriptionBuilder};
use evento::{Evento, Executor, Sidecar, TenantFilter};
use evento_fjall::Fjall;
use evento_remote::{
    serve, serve_with, Client, ClientState, Hello, NotConnected, Rejection, Resolver, Session,
};
use tokio::net::TcpListener;

/// An address nothing listens on yet.
async fn reserve() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// A client of `addr` that dials again within a few milliseconds.
fn lazy(addr: std::net::SocketAddr) -> Client {
    Client::builder(addr)
        .token("key")
        .reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .connect_lazy()
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_all<E: Executor>(executor: &E) -> anyhow::Result<Vec<evento::Event>> {
    Ok(executor
        .read(
            None,
            None,
            TenantFilter::All,
            Args::forward(100, None),
            None,
        )
        .await?
        .edges
        .into_iter()
        .map(|edge| edge.node)
        .collect())
}

async fn open_account<E: Executor + Clone>(executor: &E, owner: &str) -> anyhow::Result<String> {
    Ok(bank::Command(executor.clone())
        .open_account(OpenAccount {
            owner_id: owner.to_owned(),
            owner_name: owner.to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?)
}

#[tokio::test]
async fn a_lazy_client_exists_before_the_server_answers() -> anyhow::Result<()> {
    let addr = reserve().await;
    let client = lazy(addr);
    // Wrapped before the key is known: the wrapper asks again later.
    let wrapped = Evento::new(client.clone());

    eventually("an attempt failed", || {
        matches!(
            client.state(),
            ClientState::Connecting { error: Some(_), since } if since > 0
        )
    })
    .await;
    assert!(!client.state().is_connected());

    // Calls fail at once, with nothing sent.
    let err = read_all(&client).await.expect_err("no server");
    assert!(err.downcast_ref::<NotConnected>().is_some(), "{err:#}");
    assert_eq!(client.default_routing_key(), None);
    let err = client.resolve_default_routing_key().await.unwrap_err();
    assert!(err.downcast_ref::<NotConnected>().is_some(), "{err:#}");
    let err = wrapped.resolve_default_routing_key().await.unwrap_err();
    assert!(err.downcast_ref::<NotConnected>().is_some(), "{err:#}");
    // A new aggregate is not written under no key meanwhile.
    open_account(&wrapped, "early")
        .await
        .expect_err("no server");

    let store = Fjall::temporary()?;
    let _server = serve(
        TcpListener::bind(addr).await?,
        Evento::new(store.clone()).default_routing_key("eu"),
    );
    client.connected().await?;
    assert!(client.state().is_connected());
    assert_eq!(client.default_routing_key(), Some("eu"));
    assert_eq!(Executor::default_routing_key(&wrapped), Some("eu"));
    assert_eq!(
        wrapped.resolve_default_routing_key().await?.as_deref(),
        Some("eu")
    );

    let account = open_account(&wrapped, "late").await?;
    let events = read_all(&store).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_id, account);
    assert_eq!(events[0].routing_key.as_deref(), Some("eu"));
    Ok(())
}

/// Refuses everyone as told: for good, or for now.
struct Gate {
    store: Fjall,
    unauthorized: bool,
    unavailable: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Resolver for Gate {
    type Executor = Fjall;

    async fn resolve(&self, _hello: &Hello) -> Result<Session<Fjall>, Rejection> {
        if self.unauthorized {
            return Err(Rejection::Unauthorized);
        }
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(Rejection::Unavailable("the shop is suspended".to_owned()));
        }
        Ok(Session::new(self.store.clone()))
    }
}

#[tokio::test]
async fn a_refused_credential_is_final() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = serve_with(
        listener,
        Gate {
            store: Fjall::temporary()?,
            unauthorized: true,
            unavailable: Arc::default(),
        },
    );

    let client = lazy(addr);
    let err = client.connected().await.expect_err("refused");
    assert_eq!(
        err.downcast_ref::<Rejection>(),
        Some(&Rejection::Unauthorized)
    );
    assert!(matches!(client.state(), ClientState::Refused));
    assert_eq!(client.state().to_string(), "refused");

    let err = read_all(&client).await.expect_err("refused");
    assert_eq!(
        err.downcast_ref::<Rejection>(),
        Some(&Rejection::Unauthorized)
    );
    let err = client.resolve_default_routing_key().await.unwrap_err();
    assert_eq!(
        err.downcast_ref::<Rejection>(),
        Some(&Rejection::Unauthorized)
    );
    Ok(())
}

#[tokio::test]
async fn an_unavailable_store_is_tried_again() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let unavailable = Arc::new(AtomicBool::new(true));
    let _server = serve_with(
        listener,
        Gate {
            store: Fjall::temporary()?,
            unauthorized: false,
            unavailable: unavailable.clone(),
        },
    );

    let client = Client::builder(addr)
        .connections(2)
        .reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .connect_lazy();
    eventually("the refusal shows", || {
        matches!(
            client.state(),
            ClientState::Connecting { error: Some(error), .. }
                if matches!(error.downcast_ref::<Rejection>(), Some(Rejection::Unavailable(_)))
        )
    })
    .await;
    let err = read_all(&client).await.expect_err("unavailable");
    let Some(NotConnected { error: Some(error) }) = err.downcast_ref::<NotConnected>() else {
        panic!("{err:#}")
    };
    assert!(error.downcast_ref::<Rejection>().is_some(), "{err:#}");

    unavailable.store(false, Ordering::SeqCst);
    client.connected().await?;
    assert!(read_all(&client).await?.is_empty());
    assert_eq!(client.default_routing_key(), None);
    Ok(())
}

#[derive(Clone, Default)]
struct Opened(Arc<Mutex<Vec<String>>>);

#[evento::subscription]
async fn on_account_opened<E: Executor>(
    context: &Context<'_, E>,
    event: Event<AccountOpened>,
) -> anyhow::Result<()> {
    let opened = context.extract::<Opened>();
    opened.0.lock().unwrap().push(event.aggregate_id.clone());
    Ok(())
}

fn subscription<E: Executor>(opened: &Opened) -> SubscriptionBuilder<E> {
    SubscriptionBuilder::new("catalog")
        .data(opened.clone())
        .handler(on_account_opened())
        .no_retry()
}

/// The storefront: the events on the served store, the cursor in a local
/// one, started while the served store is away.
#[tokio::test]
async fn a_supervised_subscription_starts_before_the_server_answers() -> anyhow::Result<()> {
    let addr = reserve().await;
    let local = Fjall::temporary()?;
    let executor = Sidecar::new(lazy(addr), local.clone());
    let opened = Opened::default();

    let err = subscription(&opened)
        .start(&executor)
        .await
        .expect_err("an unsupervised start needs the store");
    assert!(err.downcast_ref::<NotConnected>().is_some(), "{err:#}");

    let worker = subscription(&opened)
        .restart_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .start(&executor)
        .await?;
    let mut held_up = None;
    eventually("the worker says it is held up", || {
        held_up = match worker.state() {
            State::Restarting { error, .. } => Some(error),
            _ => None,
        };
        held_up.is_some()
    })
    .await;
    let error = held_up.unwrap();
    assert!(error.downcast_ref::<NotConnected>().is_some(), "{error:#}");

    let store = Fjall::temporary()?;
    let _server = serve(
        TcpListener::bind(addr).await?,
        Evento::new(store.clone()).default_routing_key("eu"),
    );
    executor.events().connected().await?;
    let account = open_account(&executor, "ada").await?;

    eventually("the account is seen", || {
        *opened.0.lock().unwrap() == [account.clone()]
    })
    .await;
    eventually("the worker runs", || worker.state().is_running()).await;
    // The cursor is kept under the key the server brought, in the local
    // store.
    assert!(local
        .get_subscriber_cursor("eu.catalog".to_owned())
        .await?
        .is_some());
    assert!(worker.lag().await?.is_caught_up());
    worker.shutdown().await?;
    Ok(())
}
