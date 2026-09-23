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
        .read(None, None, Args::forward(1, None), None)
        .await?;

    handle.shutdown().await;

    let err = client
        .read(None, None, Args::forward(1, None), None)
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
        .read(None, None, PoolArgs::forward(1, None), None)
        .await?;
    handle.shutdown().await;

    // More requests than connections: each lane sees at least one failure,
    // none of them hangs past the timeout.
    for _ in 0..6 {
        let err = client
            .read(None, None, PoolArgs::forward(1, None), None)
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
                .read(None, None, PoolArgs::forward(1, None), None)
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
