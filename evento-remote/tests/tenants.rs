//! Tenants over one shared store: the host application's resolver maps each
//! token to a tenant and serves the connection in it (`Session::tenant`), and
//! a client — whatever it sends — reaches nothing of another tenant's events
//! or snapshots, while its own code stays tenant-agnostic: it reads its events
//! in the default tenant, as from a store of its own.

use std::sync::{Arc, Mutex};

use bank::aggregator::AccountOpened;
use bank::{AccountType, OpenAccount};
use evento::metadata::Event as Typed;
use evento::subscription::{Context, SubscriptionBuilder};
use evento::Sidecar;
use evento_core::cursor::{Args, Value};
use evento_core::{Event, EventFilter, Executor, TenantFilter, WriteError};
use evento_fjall::Fjall;
use evento_remote::{serve_with, Client, Hello, Rejection, Resolver, Session};
use tokio::net::TcpListener;
use ulid::Ulid;

/// Serves the shared store in the tenant a `tenant:<id>` token names.
struct ByTenant(Fjall);

#[async_trait::async_trait]
impl Resolver for ByTenant {
    type Executor = Fjall;

    async fn resolve(&self, hello: &Hello) -> Result<Session<Fjall>, Rejection> {
        let tenant = hello
            .token
            .as_deref()
            .and_then(|token| token.strip_prefix("tenant:"))
            .ok_or(Rejection::Unauthorized)?;
        Session::new(self.0.clone())
            .tenant(tenant)
            .map_err(|_| Rejection::Unauthorized)
    }
}

const AGGREGATE: &str = "tenants/Probe";

fn event(aggregate_id: &str, version: u16, tenant: &str) -> Event {
    Event {
        id: Ulid::generate(),
        aggregate_id: aggregate_id.to_owned(),
        aggregate_type: AGGREGATE.to_owned(),
        version,
        name: "Probed".to_owned(),
        tenant: tenant.to_owned(),
        ..Default::default()
    }
}

async fn connect(addr: std::net::SocketAddr, tenant: &str) -> Client {
    Client::builder(addr)
        .token(format!("tenant:{tenant}"))
        .connect()
        .await
        .unwrap()
}

/// What `executor` reads of aggregate `id` asking for `tenant`, as
/// `(tenant, version)` pairs.
async fn read(executor: &impl Executor, id: &str, tenant: TenantFilter) -> Vec<(String, u16)> {
    executor
        .read(
            Some([EventFilter::by_id_raw(AGGREGATE, id)].into()),
            None,
            tenant,
            Args::forward(10, None),
            None,
        )
        .await
        .unwrap()
        .edges
        .into_iter()
        .map(|edge| (edge.node.tenant, edge.node.version))
        .collect()
}

#[tokio::test]
async fn a_client_reaches_only_its_own_tenant() {
    let store = Fjall::temporary().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = serve_with(listener, ByTenant(store.clone()));
    let a = connect(addr, "a").await;
    let b = connect(addr, "b").await;

    // Tenant-agnostic writes land in each client's tenant: one aggregate id,
    // two streams.
    a.write(vec![event("acc", 1, "")]).await.unwrap();
    b.write(vec![event("acc", 1, "")]).await.unwrap();
    b.write(vec![event("acc", 2, "")]).await.unwrap();
    assert_eq!(
        read(&store, "acc", TenantFilter::All).await.len(),
        3,
        "the store holds both tenants' streams"
    );

    // Whatever a asks for, it reads its own stream, in the default tenant.
    for asked in [
        TenantFilter::All,
        TenantFilter::Only("b".to_owned()),
        TenantFilter::Only(String::new()),
    ] {
        assert_eq!(read(&a, "acc", asked).await, [(String::new(), 1)]);
    }
    assert_eq!(
        a.latest_version("b".to_owned(), AGGREGATE.to_owned(), "acc".to_owned())
            .await
            .unwrap(),
        1
    );

    // An event naming b's tenant lands in a's, and replicate is refused.
    a.write(vec![event("acc", 2, "b")]).await.unwrap();
    let Err(WriteError::Unknown(refused)) = a.replicate(vec![event("other", 1, "")]).await else {
        panic!("replicate was not refused");
    };
    assert!(refused.to_string().contains("refused"), "{refused:#}");
    assert_eq!(
        read(&store, "acc", TenantFilter::Only("a".to_owned())).await,
        [("a".to_owned(), 1), ("a".to_owned(), 2)]
    );
    assert_eq!(
        read(&store, "acc", TenantFilter::Only("b".to_owned())).await,
        [("b".to_owned(), 1), ("b".to_owned(), 2)]
    );
    assert!(read(&store, "other", TenantFilter::All).await.is_empty());

    // Snapshots land in a's tenant, whatever it names.
    a.save_snapshot(
        "b".to_owned(),
        AGGREGATE.to_owned(),
        "view".to_owned(),
        "0".to_owned(),
        "acc".to_owned(),
        vec![1],
        Value(String::new()),
    )
    .await
    .unwrap();
    for (tenant, expected) in [("a", Some(vec![1u8])), ("b", None)] {
        let snapshot = store
            .get_snapshot(
                tenant.to_owned(),
                AGGREGATE.to_owned(),
                "view".to_owned(),
                "0".to_owned(),
                "acc".to_owned(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.map(|s| s.0), expected);
    }
}

/// A subscriber row belongs to no tenant, so a connection serving one refuses
/// every subscriber call: a subscription run straight on the client fails
/// before it reads anything, and the store keeps no row of it.
#[tokio::test]
async fn a_tenant_connection_refuses_the_subscriber_calls() {
    let store = Fjall::temporary().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = serve_with(listener, ByTenant(store.clone()));
    let a = connect(addr, "a").await;
    let key = || "projector".to_owned();
    let worker = Ulid::generate();

    let refused = |result: anyhow::Result<()>| {
        let err = result.expect_err("the subscriber call was served");
        assert!(err.to_string().contains("not served"), "{err:#}");
    };
    refused(a.upsert_subscriber(key(), worker).await);
    refused(a.get_subscriber_cursor(key()).await.map(drop));
    refused(a.is_subscriber_running(key(), worker).await.map(drop));
    refused(a.subscriber_status(key(), worker).await.map(drop));
    refused(
        a.acknowledge(key(), worker, Value(String::new()), 0)
            .await
            .map(drop),
    );
    refused(
        SubscriptionBuilder::new("projector")
            .handler(on_account_opened())
            .no_retry()
            .run_once(&a)
            .await,
    );

    assert!(store.get_subscriber_cursor(key()).await.unwrap().is_none());
    assert!(!store.is_subscriber_running(key(), worker).await.unwrap());
}

#[tokio::test]
async fn a_bad_tenant_token_is_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = serve_with(listener, ByTenant(Fjall::temporary().unwrap()));
    for token in ["no-prefix", "tenant:"] {
        let Err(err) = Client::builder(addr).token(token).connect().await else {
            panic!("{token}: the connection was accepted");
        };
        assert!(
            err.downcast_ref::<Rejection>()
                .is_some_and(|rejection| *rejection == Rejection::Unauthorized),
            "{token}: {err:#}"
        );
    }
}

/// The accounts a subscription saw opened, in order.
#[derive(Clone, Default)]
struct Opened(Arc<Mutex<Vec<String>>>);

#[evento::subscription]
async fn on_account_opened<E: Executor>(
    context: &Context<'_, E>,
    event: Typed<AccountOpened>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        event.tenant.is_empty(),
        "a tenant session reads its events in the default tenant, not {:?}",
        event.tenant
    );
    let opened = context.extract::<Opened>();
    opened.0.lock().unwrap().push(event.aggregate_id.clone());
    Ok(())
}

async fn open_account<E: Executor>(executor: E, owner: &str) -> String {
    bank::Command(executor)
        .open_account(OpenAccount {
            owner_id: owner.to_owned(),
            owner_name: owner.to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await
        .unwrap()
}

/// A tenant application follows the shared store through a `Sidecar` — the
/// remote client as its events source, its own store for everything else —
/// over a session the server serves in its tenant. Its code names no tenant:
/// it reads, loads and subscribes to its tenant's events only — in the
/// default tenant — its writes land in its tenant, and its cursors and
/// snapshots stay in its own store.
#[tokio::test]
async fn a_sidecar_follows_only_its_tenant() {
    let store = Fjall::temporary().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = serve_with(listener, ByTenant(store.clone()));

    let theirs = open_account(connect(addr, "b").await, "owner-b").await;
    let local = Fjall::temporary().unwrap();
    let app = Sidecar::from((connect(addr, "a").await, local.clone()));
    let ours = open_account(app.clone(), "owner-a").await;

    let opened = Opened::default();
    SubscriptionBuilder::new("accounts")
        .data(opened.clone())
        .handler(on_account_opened())
        .run_once(&app)
        .await
        .unwrap();
    assert_eq!(*opened.0.lock().unwrap(), std::slice::from_ref(&ours));

    let command = bank::Command(app.clone());
    assert!(command.load(&ours).await.unwrap().is_some());
    assert!(
        command.load(&theirs).await.unwrap().is_none(),
        "another tenant's account is out of reach"
    );

    // On the server, the app's account landed in its tenant.
    let filter = Some([EventFilter::by_id_raw("bank/BankAccount", ours.clone())].into());
    for (tenant, expected) in [("a", 1), ("", 0), ("b", 0)] {
        let read = store
            .read(
                filter.clone(),
                None,
                TenantFilter::Only(tenant.to_owned()),
                Args::forward(10, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(read.edges.len(), expected, "tenant {tenant:?}");
    }

    // The bookkeeping is local, as before tenants.
    assert!(local
        .get_subscriber_cursor("accounts".to_owned())
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get_subscriber_cursor("accounts".to_owned())
        .await
        .unwrap()
        .is_none());
}
