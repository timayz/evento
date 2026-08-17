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
