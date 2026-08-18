use evento_remote::{serve, Client, ServerHandle};
use tempfile::TempDir;

/// Serves a Fjall executor (in a temporary directory) on an ephemeral port and
/// connects a client to it. The handle and TempDir are returned to keep the
/// server and its directory alive for the test's duration.
struct TestServer {
    client: Client,
    _handle: ServerHandle,
    _temp_dir: TempDir,
}

async fn setup(name: &str) -> anyhow::Result<TestServer> {
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("evento_remote_test_{name}"))
        .tempdir()?;
    let fjall = evento_fjall::Fjall::open(temp_dir.path())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, fjall);
    let client = Client::connect(addr).await?;
    Ok(TestServer {
        client,
        _handle: handle,
        _temp_dir: temp_dir,
    })
}

#[tokio::test]
async fn remote_routing_key() -> anyhow::Result<()> {
    let server = setup("routing_key").await?;
    evento_test::routing_key(&server.client).await
}

#[tokio::test]
async fn remote_load() -> anyhow::Result<()> {
    let server = setup("load").await?;
    evento_test::load(&server.client).await
}

#[tokio::test]
async fn remote_load_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup("load_multiple_aggregator").await?;
    evento_test::load_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_load_with_snapshot() -> anyhow::Result<()> {
    let server = setup("load_with_snapshot").await?;
    evento_test::load_with_snapshot(&server.client).await
}

#[tokio::test]
async fn remote_invalid_original_version() -> anyhow::Result<()> {
    let server = setup("invalid_original_version").await?;
    evento_test::invalid_original_version(&server.client).await
}

#[tokio::test]
async fn remote_subscriber_running() -> anyhow::Result<()> {
    let server = setup("subscriber_running").await?;
    evento_test::subscriber_running(&server.client).await
}

#[tokio::test]
async fn remote_subscribe() -> anyhow::Result<()> {
    let server = setup("subscribe").await?;
    evento_test::subscribe(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_low_latency() -> anyhow::Result<()> {
    let server = setup("subscribe_low_latency").await?;
    evento_test::subscribe_low_latency(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_routing_key() -> anyhow::Result<()> {
    let server = setup("subscribe_routing_key").await?;
    evento_test::subscribe_routing_key(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default() -> anyhow::Result<()> {
    let server = setup("subscribe_default").await?;
    evento_test::subscribe_default(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup("subscribe_multiple_aggregator").await?;
    evento_test::subscribe_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let server = setup("subscribe_co_keyed_aggregator").await?;
    evento_test::subscribe_co_keyed_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let server = setup("load_co_keyed_aggregator").await?;
    evento_test::load_co_keyed_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup("subscribe_routing_key_multiple_aggregator").await?;
    evento_test::subscribe_routing_key_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let server = setup("subscribe_default_multiple_aggregator").await?;
    evento_test::subscribe_default_multiple_aggregator(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_routing_key() -> anyhow::Result<()> {
    let server = setup("subscribe_default_routing_key").await?;
    evento_test::subscribe_default_routing_key(&server.client).await
}

#[tokio::test]
async fn remote_subscribe_default_routing_key_all_isolation() -> anyhow::Result<()> {
    let server = setup("subscribe_default_routing_key_all_isolation").await?;
    evento_test::subscribe_default_routing_key_all_isolation(&server.client).await
}

#[tokio::test]
async fn remote_all_commands() -> anyhow::Result<()> {
    let server = setup("all_commands").await?;
    evento_test::all_commands(&server.client).await
}

#[tokio::test]
async fn remote_read_order_timestamp() -> anyhow::Result<()> {
    let server = setup("read_order_timestamp").await?;
    evento_test::read_order_timestamp(&server.client).await
}

#[tokio::test]
async fn remote_exact_filter() -> anyhow::Result<()> {
    let server = setup("exact_filter").await?;
    evento_test::exact_filter(&server.client).await
}

#[tokio::test]
async fn remote_concurrent_append() -> anyhow::Result<()> {
    let server = setup("concurrent_append").await?;
    evento_test::concurrent_append(&server.client).await
}

#[tokio::test]
async fn remote_strict_unhandled() -> anyhow::Result<()> {
    let server = setup("strict_unhandled").await?;
    evento_test::strict_unhandled(&server.client).await
}

#[tokio::test]
async fn remote_tombstone() -> anyhow::Result<()> {
    let server = setup("tombstone").await?;
    evento_test::tombstone(&server.client).await
}

#[tokio::test]
async fn remote_subscription_all_counts() -> anyhow::Result<()> {
    let server = setup("subscription_all_counts").await?;
    evento_test::subscription_all_counts(&server.client).await
}

#[tokio::test]
async fn remote_snapshot_revision_scope() -> anyhow::Result<()> {
    let server = setup("snapshot_revision_scope").await?;
    evento_test::snapshot_revision_scope(&server.client).await
}

/// After shutdown the server no longer answers; a request fails cleanly (with
/// the client's request timeout as the upper bound) instead of hanging.
#[tokio::test]
async fn remote_requests_fail_after_shutdown() -> anyhow::Result<()> {
    use evento_core::{cursor::Args, Executor};

    let temp_dir = tempfile::Builder::new()
        .prefix("evento_remote_test_shutdown")
        .tempdir()?;
    let fjall = evento_fjall::Fjall::open(temp_dir.path())?;
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
async fn setup_pooled(name: &str, connections: usize) -> anyhow::Result<TestServer> {
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("evento_remote_test_{name}"))
        .tempdir()?;
    let fjall = evento_fjall::Fjall::open(temp_dir.path())?;
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
        _temp_dir: temp_dir,
    })
}

#[tokio::test]
async fn pooled_routing_key() -> anyhow::Result<()> {
    let server = setup_pooled("pooled_routing_key", 4).await?;
    evento_test::routing_key(&server.client).await
}

#[tokio::test]
async fn pooled_subscribe() -> anyhow::Result<()> {
    let server = setup_pooled("pooled_subscribe", 4).await?;
    evento_test::subscribe(&server.client).await
}

#[tokio::test]
async fn pooled_read_order_timestamp() -> anyhow::Result<()> {
    let server = setup_pooled("pooled_read_order", 4).await?;
    evento_test::read_order_timestamp(&server.client).await
}

/// Concurrent writers on one pooled client: every write lands, and a read
/// afterwards observes all of them.
#[tokio::test]
async fn pooled_concurrent_requests() -> anyhow::Result<()> {
    let server = setup_pooled("pooled_concurrent", 4).await?;

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
                Some([evento_core::EventFilter::by_type("remote/Concurrent")].into()),
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
    let temp_dir = tempfile::Builder::new()
        .prefix("evento_remote_test_pooled_reconnect")
        .tempdir()?;
    let fjall = evento_fjall::Fjall::open(temp_dir.path())?;
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
