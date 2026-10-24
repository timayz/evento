//! Tenants over one shared store: the resolver serves each token the store
//! scoped to its tenant, and a client — whatever it sends — reaches nothing
//! of another tenant's events, snapshots or subscriptions.

use evento_core::cursor::{Args, Value};
use evento_core::{Event, EventFilter, Evento, Executor, TenantFilter, WriteError};
use evento_fjall::Fjall;
use evento_remote::{serve_with, Client, Hello, Rejection, Resolver, Session};
use tokio::net::TcpListener;
use ulid::Ulid;

/// Serves the shared store scoped to the tenant a `tenant:<id>` token names.
struct ByTenant(Fjall);

#[async_trait::async_trait]
impl Resolver for ByTenant {
    type Executor = Evento;

    async fn resolve(&self, hello: &Hello) -> Result<Session<Evento>, Rejection> {
        let tenant = hello
            .token
            .as_deref()
            .and_then(|token| token.strip_prefix("tenant:"))
            .ok_or(Rejection::Unauthorized)?;
        let scoped = Evento::new(self.0.clone())
            .tenant(tenant)
            .map_err(|_| Rejection::Unauthorized)?;
        Ok(Session::new(scoped))
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
    assert_eq!(a.tenant(), Some("a"), "Hello names the session's tenant");

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

    // Whatever a asks for, it reads its own stream.
    for asked in [
        TenantFilter::All,
        TenantFilter::Only("b".to_owned()),
        TenantFilter::Only(String::new()),
    ] {
        assert_eq!(read(&a, "acc", asked).await, [("a".to_owned(), 1)]);
    }
    assert_eq!(
        a.latest_version("b".to_owned(), AGGREGATE.to_owned(), "acc".to_owned())
            .await
            .unwrap(),
        1
    );

    // It writes nothing into b's tenant, and cannot replicate.
    assert!(matches!(
        a.write(vec![event("acc", 3, "b")]).await,
        Err(WriteError::ForeignTenant { .. })
    ));
    assert!(matches!(
        a.replicate(vec![event("other", 1, "")]).await,
        Err(WriteError::ReplicateRefused)
    ));
    assert_eq!(
        read(&store, "acc", TenantFilter::Only("b".to_owned())).await,
        [("b".to_owned(), 1), ("b".to_owned(), 2)]
    );

    // Snapshots and subscriber rows land in a's tenant, whatever it names.
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
    let worker = Ulid::generate();
    a.upsert_subscriber("b".to_owned(), "projector".to_owned(), worker)
        .await
        .unwrap();
    assert!(store
        .is_subscriber_running("a".to_owned(), "projector".to_owned(), worker)
        .await
        .unwrap());
    assert!(!store
        .is_subscriber_running("b".to_owned(), "projector".to_owned(), worker)
        .await
        .unwrap());
    assert!(!b
        .is_subscriber_running(String::new(), "projector".to_owned(), worker)
        .await
        .unwrap());
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
