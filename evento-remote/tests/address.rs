//! Addressing a server by name, and serving a client whose cached watermark
//! came from another node: a name is looked up on every connection attempt,
//! and a server never reads past its own stability watermark.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use evento_core::cursor::{Args, ReadResult, Value};
use evento_core::{Event, EventFilter, Executor, RoutingKey, TenantFilter, WriteError};
use evento_remote::{serve, Address, Client};
use ulid::Ulid;

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        id: Ulid::generate(),
        aggregate_id: aggregate_id.to_owned(),
        aggregate_type: "remote/Address".to_owned(),
        version,
        name: "Bumped".to_owned(),
        ..Default::default()
    }
}

fn filter() -> Option<Arc<[EventFilter]>> {
    Some([EventFilter::by_type_raw("remote/Address")].into())
}

/// Retries a read until the client has reconnected (or the deadline passes).
async fn read_eventually(client: &Client) -> anyhow::Result<ReadResult<Event>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match client
            .read(
                filter(),
                None,
                evento_core::TenantFilter::All,
                Args::forward(10, None),
                None,
            )
            .await
        {
            Ok(page) => return Ok(page),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

#[test]
fn address_conversions() {
    let socket = std::net::SocketAddr::from(([127, 0, 0, 1], 4321));
    assert_eq!(Address::from(socket).to_string(), "127.0.0.1:4321");
    assert_eq!(
        Address::from("store.example.com:4321").to_string(),
        "store.example.com:4321"
    );
    assert_eq!(
        Address::from(String::from("store.example.com:4321")),
        Address::from("store.example.com:4321")
    );
}

/// A client built with a host name connects, and after the server is replaced
/// (a different store behind the same name) its reconnect reaches the new one.
#[tokio::test]
async fn host_name_is_looked_up_on_every_connection() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let first = evento_fjall::Fjall::temporary()?;
    first.write(vec![event("first", 1)]).await?;
    let handle = serve(listener, first);

    let client = Client::builder(format!("localhost:{port}"))
        .request_timeout(Duration::from_secs(2))
        .connect()
        .await?;
    let page = client
        .read(
            filter(),
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await?;
    assert_eq!(page.edges.len(), 1);
    assert_eq!(page.edges[0].node.aggregate_id, "first");

    handle.shutdown().await;
    let second = evento_fjall::Fjall::temporary()?;
    second
        .write(vec![event("second", 1), event("second", 2)])
        .await?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let _handle = serve(listener, second);

    let page = read_eventually(&client).await?;
    assert_eq!(page.edges.len(), 2);
    assert!(page.edges.iter().all(|e| e.node.aggregate_id == "second"));
    Ok(())
}

#[tokio::test]
async fn unresolvable_name_fails_to_connect() {
    let Err(err) = Client::connect("does-not-exist.invalid:4321").await else {
        panic!("connecting to an unresolvable name must fail");
    };
    assert!(
        format!("{err:#}").contains("does-not-exist.invalid:4321"),
        "the error names the address: {err:#}"
    );
}

/// A store whose stability watermark is set by the test, standing in for a
/// replica that has applied less than the node the client last talked to.
#[derive(Clone)]
struct Lagging {
    inner: evento_fjall::Fjall,
    stable: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Executor for Lagging {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.inner.write(events).await
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        Ok(Some(self.stable.load(Ordering::Acquire)))
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.inner.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.inner.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.inner.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.inner.acknowledge(key, worker_id, cursor, lag).await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.inner
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.inner
            .latest_timestamp(aggregators, routing_key, tenant)
            .await
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.inner
            .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.inner
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregate_revision,
                id,
                data,
                cursor,
            )
            .await
    }

    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_snapshot(tenant, aggregate_type, projection, id)
            .await
    }
}

/// A bound above the server's own watermark (as a client carries after
/// reconnecting from a node further ahead) is lowered to it: the events this
/// node does not vouch for yet stay unread instead of being skipped later.
#[tokio::test]
async fn bounded_read_stops_at_the_servers_own_watermark() -> anyhow::Result<()> {
    let store = Lagging {
        inner: evento_fjall::Fjall::temporary()?,
        stable: Arc::new(AtomicU64::new(1)),
    };
    store
        .write(vec![event("lagging", 1), event("lagging", 2)])
        .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _handle = serve(listener, store.clone());
    let client = Client::connect(addr).await?;

    let bounded = client
        .read(
            filter(),
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            Some(u64::MAX),
        )
        .await?;
    assert!(
        bounded.edges.is_empty(),
        "nothing is below this node's watermark"
    );

    let unbounded = client
        .read(
            filter(),
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            None,
        )
        .await?;
    assert_eq!(unbounded.edges.len(), 2, "an unbounded read is left alone");

    store.stable.store(u64::MAX, Ordering::Release);
    let caught_up = client
        .read(
            filter(),
            None,
            evento_core::TenantFilter::All,
            Args::forward(10, None),
            Some(u64::MAX),
        )
        .await?;
    assert_eq!(caught_up.edges.len(), 2, "the node caught up");
    Ok(())
}
