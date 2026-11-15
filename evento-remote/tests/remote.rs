//! The shared executor conformance suite, run over a TCP round trip to a
//! served Fjall backend, plus the pooled-connection and shutdown cases.

use evento_remote::{serve, Client, ServerHandle};

/// Serves an ephemeral Fjall executor on an ephemeral port and connects a client
/// to it. The handle is kept to hold the server alive for the test's duration;
/// the store's temporary directory is owned by the executor the server holds.
struct TestServer {
    client: Client,
    _handle: ServerHandle,
}

async fn setup() -> anyhow::Result<TestServer> {
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall);
    let client = Client::connect(addr).await?;
    Ok(TestServer {
        client,
        _handle: handle,
    })
}

#[tokio::test]
async fn remote_routing_key() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::routing_key(&server.client).await
}

#[tokio::test]
async fn remote_load() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::load(&server.client).await
}

#[tokio::test]
async fn remote_load_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::load_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_load_with_snapshot() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::load_with_snapshot(&server.client).await
}

#[tokio::test]
async fn remote_invalid_original_version() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::invalid_original_version(&server.client).await
}

#[tokio::test]
async fn remote_subscriber_running() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscriber_running(&server.client).await
}

#[tokio::test]
async fn remote_subscribe() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_low_latency() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_low_latency(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_routing_key() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_routing_key(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_default(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_co_keyed_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::load_co_keyed_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_routing_key_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_default_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_routing_key() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_default_routing_key(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_routing_key_all_isolation() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscribe_default_routing_key_all_isolation(&server.client).await
}

#[tokio::test]
async fn remote_all_commands() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::all_commands(&server.client).await
}

#[tokio::test]
async fn remote_read_order_timestamp() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_order_timestamp(&server.client).await
}

#[tokio::test]
async fn remote_exact_filter() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::exact_filter(&server.client).await
}

#[tokio::test]
async fn remote_concurrent_append() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::concurrent_append(&server.client).await
}

#[tokio::test]
async fn remote_strict_unhandled() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::strict_unhandled(&server.client).await
}

#[tokio::test]
async fn remote_tombstone() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tombstone(&server.client).await
}

#[tokio::test]
async fn remote_subscription_data() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_data(&server.client).await
}

#[tokio::test]
async fn remote_subscription_stop_reason() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_stop_reason(&server.client).await
}

#[tokio::test]
async fn remote_subscription_shutdown_acknowledges_in_flight() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_shutdown_acknowledges_in_flight(&server.client).await
}

#[tokio::test]
async fn remote_subscription_all_counts() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_all_counts(&server.client).await
}

#[tokio::test]
async fn remote_snapshot_revision_scope() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::snapshot_revision_scope(&server.client).await
}

#[tokio::test]
async fn remote_snapshot_projection_scope() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::snapshot_projection_scope(&server.client).await
}

#[tokio::test]
async fn remote_upcast_load() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_load(&server.client).await
}

#[tokio::test]
async fn remote_upcast_nearest_target() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_nearest_target(&server.client).await
}

#[tokio::test]
async fn remote_upcast_explicit_wins() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_explicit_wins(&server.client).await
}

#[tokio::test]
async fn remote_upcast_skip() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_skip(&server.client).await
}

#[tokio::test]
async fn remote_upcast_strict_unhandled_target() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_strict_unhandled_target(&server.client).await
}

#[tokio::test]
async fn remote_upcast_subscription() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_subscription(&server.client).await
}

#[tokio::test]
async fn remote_upcast_projection_subscription() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_projection_subscription(&server.client).await
}

#[tokio::test]
async fn remote_upcast_tombstone() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_tombstone(&server.client).await
}

#[tokio::test]
async fn remote_upcast_has_event() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::upcast_has_event(&server.client).await
}

#[tokio::test]
async fn remote_read_stream() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_stream(&server.client).await
}

#[tokio::test]
async fn remote_read_drains_pages() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_drains_pages(&server.client).await
}

#[tokio::test]
async fn remote_read_limit() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_limit(&server.client).await
}

#[tokio::test]
async fn remote_read_page_cursor() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_page_cursor(&server.client).await
}

#[tokio::test]
async fn remote_read_decode() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_decode(&server.client).await
}

#[tokio::test]
async fn remote_read_routing_key() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::read_routing_key(&server.client).await
}

/// After shutdown the server no longer answers; a request fails cleanly (with
/// the client's request timeout as the upper bound) instead of hanging.
#[tokio::test]
async fn remote_requests_fail_after_shutdown() -> anyhow::Result<()> {
    use evento_core::{cursor::Args, Executor};

    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall);
    let client = Client::builder(addr)
        .request_timeout(std::time::Duration::from_secs(2))
        .connect()
        .await?;

    client
        .read(
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(1, None),
            None,
        )
        .await?;

    handle.shutdown().await;

    let err = client
        .read(
            None,
            None,
            evento_core::TenantFilter::All,
            Args::forward(1, None),
            None,
        )
        .await
        .expect_err("request against a stopped server must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("connection lost") || msg.contains("timed out"),
        "unexpected error: {msg}"
    );
    Ok(())
}

use evento_core::{cursor::Args as PoolArgs, Executor as _};

/// A pooled client (4 connections) drives the same conformance scenarios as
/// the single-connection one — round-robined dispatch is transparent.
async fn setup_pooled(connections: usize) -> anyhow::Result<TestServer> {
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall);
    let client = Client::builder(addr)
        .connections(connections)
        .connect()
        .await?;
    Ok(TestServer {
        client,
        _handle: handle,
    })
}

#[tokio::test]
async fn pooled_routing_key() -> anyhow::Result<()> {
    let server = setup_pooled(4).await?;
    evento_test::routing_key(&server.client).await
}

#[tokio::test]
async fn pooled_subscribe() -> anyhow::Result<()> {
    let server = setup_pooled(4).await?;
    evento_test::subscribe(&server.client).await
}

#[tokio::test]
async fn pooled_read_order_timestamp() -> anyhow::Result<()> {
    let server = setup_pooled(4).await?;
    evento_test::read_order_timestamp(&server.client).await
}

/// Concurrent writers on one pooled client: every write lands, and a read
/// afterwards observes all of them.
#[tokio::test]
async fn pooled_concurrent_requests() -> anyhow::Result<()> {
    let server = setup_pooled(4).await?;

    let mut handles = Vec::new();
    for task in 0..8u16 {
        let client = server.client.clone();
        handles.push(tokio::spawn(async move {
            for version in 1..=10u16 {
                let event = evento_core::Event {
                    id: ulid::Ulid::generate(),
                    aggregate_id: format!("conc-{task}"),
                    aggregate_type: "remote/Concurrent".to_string(),
                    version,
                    name: "Bumped".to_string(),
                    ..Default::default()
                };
                client.write(vec![event]).await?;
            }
            anyhow::Ok(())
        }));
    }
    for handle in handles {
        handle.await??;
    }

    let mut total = 0usize;
    let mut after = None;
    loop {
        let page = server
            .client
            .read(
                Some([evento_core::EventFilter::by_type_raw("remote/Concurrent")].into()),
                None,
                evento_core::TenantFilter::All,
                PoolArgs::forward(50, after),
                None,
            )
            .await?;
        total += page.edges.len();
        if !page.page_info.has_next_page {
            break;
        }
        after = page.page_info.end_cursor.clone();
    }
    assert_eq!(total, 80, "every concurrent write is observable");
    Ok(())
}

/// Killing the server fails in-flight requests on every pooled connection
/// (nothing hangs), and a restarted listener on the same address lets all
/// connections recover.
#[tokio::test]
async fn pooled_reconnect_recovers_all_connections() -> anyhow::Result<()> {
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall.clone());
    let client = Client::builder(addr)
        .connections(3)
        .request_timeout(std::time::Duration::from_secs(2))
        .connect()
        .await?;

    client
        .read(
            None,
            None,
            evento_core::TenantFilter::All,
            PoolArgs::forward(1, None),
            None,
        )
        .await?;
    handle.shutdown().await;

    // More requests than connections: each lane sees at least one failure,
    // none of them hangs past the timeout.
    for _ in 0..6 {
        let err = client
            .read(
                None,
                None,
                evento_core::TenantFilter::All,
                PoolArgs::forward(1, None),
                None,
            )
            .await
            .expect_err("request against a stopped server must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("connection lost") || msg.contains("timed out"),
            "unexpected error: {msg}"
        );
    }

    // Restart on the same address; every lane reconnects and serves again.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let _handle = serve(listener, fjall);
    for _ in 0..6 {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match client
                .read(
                    None,
                    None,
                    evento_core::TenantFilter::All,
                    PoolArgs::forward(1, None),
                    None,
                )
                .await
            {
                Ok(_) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn remote_subscription_start_from_latest() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_start_from_latest(&server.client).await
}

#[tokio::test]
async fn remote_subscription_ephemeral() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_ephemeral(&server.client).await
}

#[tokio::test]
async fn remote_subscription_ephemeral_start_from_latest() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_ephemeral_start_from_latest(&server.client).await
}

#[tokio::test]
async fn remote_subscription_ephemeral_concurrent_same_key() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_ephemeral_concurrent_same_key(&server.client).await
}

#[tokio::test]
async fn remote_subscription_context_stop() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_context_stop(&server.client).await
}

#[tokio::test]
async fn remote_subscription_live() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_live(&server.client).await
}

#[tokio::test]
async fn remote_subscription_ephemeral_run_once() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::subscription_ephemeral_run_once(&server.client).await
}

#[tokio::test]
async fn remote_tenant_streams_are_isolated() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_streams_are_isolated(&server.client).await
}

#[tokio::test]
async fn remote_tenant_subscriptions() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_subscriptions(&server.client).await
}

#[tokio::test]
async fn remote_tenant_filtered_subscription() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_filtered_subscription(&server.client).await
}

#[tokio::test]
async fn remote_undecodable_event_is_skipped() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::undecodable_event_is_skipped(&server.client).await
}

/// Dead letters do not cross the wire: a subscription that asks for them
/// over a bare client is refused before it processes anything, instead of
/// stopping on its first poisoned event. Behind a `Sidecar` they are kept in
/// the local store, with the cursor (see `tests/sidecar.rs`).
#[tokio::test]
async fn remote_dead_lettering_is_refused_up_front() -> anyhow::Result<()> {
    let server = setup().await?;
    let err = evento::subscription::SubscriptionBuilder::<Client>::new("remote-dead-letters")
        .dead_letter()
        .run_once(&server.client)
        .await
        .expect_err("a remote client keeps no dead letters");
    assert!(
        format!("{err:#}").contains("dead letters are not supported"),
        "{err:#}"
    );
    Ok(())
}

#[tokio::test]
async fn remote_tenant_projection_subscription() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_projection_subscription(&server.client).await
}

#[tokio::test]
async fn remote_tenant_projection_write_gateway() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_projection_write_gateway(&server.client).await
}

#[tokio::test]
async fn remote_tenant_snapshots_are_isolated() -> anyhow::Result<()> {
    let server = setup().await?;
    evento_test::tenant_snapshots_are_isolated(&server.client).await
}

/// An event of the stream `id` of `aggregate_type`, first of its stream.
fn watched_event(aggregate_type: &str, id: &str, routing_key: Option<&str>) -> evento_core::Event {
    evento_core::Event {
        id: ulid::Ulid::generate(),
        aggregate_type: aggregate_type.to_owned(),
        aggregate_id: id.to_owned(),
        version: 1,
        name: "Probed".to_owned(),
        routing_key: routing_key.map(str::to_owned),
        ..Default::default()
    }
}

/// What a subscription that follows `aggregate_type`, written without a
/// routing key, watches with.
fn interest_in(aggregate_type: &str) -> evento_core::WriteInterest {
    evento_core::WriteInterest {
        filters: Some([evento_core::EventFilter::by_type_raw(aggregate_type)].into()),
        routing_key: Some(evento_core::RoutingKey::Value(None)),
        tenant: evento_core::TenantFilter::All,
    }
}

/// Whether `watch` is woken within `within`.
async fn woken(watch: &mut tokio::sync::watch::Receiver<u64>, within: std::time::Duration) -> bool {
    let woken = tokio::time::timeout(within, watch.changed()).await.is_ok();
    watch.borrow_and_update();
    woken
}

const SOON: std::time::Duration = std::time::Duration::from_secs(5);
const QUIET: std::time::Duration = std::time::Duration::from_millis(300);

/// Issue #535: a client's watch is woken by the writes its filter reads, not
/// by every write of the store — whether the write came through this client,
/// another one, or the served executor itself.
#[tokio::test]
async fn a_watch_is_woken_by_the_writes_it_follows_only() -> anyhow::Result<()> {
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _handle = serve(listener, fjall.clone());
    let client = Client::connect(addr).await?;
    let other = Client::connect(addr).await?;

    let mut accounts = client
        .write_watch_for(&interest_in("test/Account"))
        .unwrap();
    let mut cards = client.write_watch_for(&interest_in("test/Card")).unwrap();
    // Each watch is woken once as the server registers it.
    assert!(woken(&mut accounts, SOON).await);
    assert!(woken(&mut cards, SOON).await);

    client
        .write(vec![watched_event("test/Order", "o1", None)])
        .await?;
    other
        .write(vec![watched_event("test/Card", "c1", None)])
        .await?;
    // An account, but under a routing key the watch does not read.
    fjall
        .write(vec![watched_event("test/Account", "a1", Some("eu"))])
        .await?;
    assert!(woken(&mut cards, SOON).await);
    assert!(
        !woken(&mut accounts, QUIET).await,
        "no write held an event the accounts watch reads"
    );

    fjall
        .write(vec![watched_event("test/Account", "a2", None)])
        .await?;
    assert!(woken(&mut accounts, SOON).await);
    assert!(!woken(&mut cards, QUIET).await);

    // The unfiltered watch still sees every write.
    let mut all = client.write_watch().unwrap();
    assert!(woken(&mut all, SOON).await);
    other
        .write(vec![watched_event("test/Order", "o2", None)])
        .await?;
    assert!(woken(&mut all, SOON).await);
    assert!(!woken(&mut accounts, QUIET).await);
    Ok(())
}

/// The server knows the watches of a connection: after a reconnect the client
/// registers them again, and each is woken once for what was written while
/// there was no connection.
#[tokio::test]
async fn a_watch_survives_a_reconnect() -> anyhow::Result<()> {
    let fjall = evento_fjall::Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall.clone());
    let client = Client::builder(addr)
        .reconnect_backoff(
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(50),
        )
        .connect()
        .await?;

    let mut accounts = client
        .write_watch_for(&interest_in("test/Account"))
        .unwrap();
    assert!(woken(&mut accounts, SOON).await);

    handle.shutdown().await;
    // Written while the client has no connection.
    fjall
        .write(vec![watched_event("test/Account", "a1", None)])
        .await?;
    // Registered while the client has no connection.
    let mut cards = client.write_watch_for(&interest_in("test/Card")).unwrap();
    assert!(!woken(&mut accounts, QUIET).await);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let _handle = serve(listener, fjall.clone());
    assert!(
        woken(&mut accounts, SOON).await,
        "a watch is woken once the connection is back"
    );
    assert!(woken(&mut cards, SOON).await);

    // And they follow the writes again.
    fjall
        .write(vec![watched_event("test/Account", "a2", None)])
        .await?;
    assert!(woken(&mut accounts, SOON).await);
    assert!(!woken(&mut cards, QUIET).await);
    fjall
        .write(vec![watched_event("test/Card", "c1", None)])
        .await?;
    assert!(woken(&mut cards, SOON).await);
    Ok(())
}
