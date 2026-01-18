use evento_fjall::Fjall;
use tempfile::TempDir;

/// Creates a Fjall executor with a temporary directory.
/// Returns both the executor and the TempDir to keep the directory alive.
fn create_fjall_executor(name: &str) -> anyhow::Result<(Fjall, TempDir)> {
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("evento_fjall_test_{}", name))
        .tempdir()?;
    let fjall = Fjall::open(temp_dir.path())?;
    Ok((fjall, temp_dir))
}

#[tokio::test]
async fn fjall_routing_key() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("routing_key")?;
    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn fjall_load() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("load")?;
    evento_test::load(&executor).await
}

#[tokio::test]
async fn fjall_load_multiple_aggregator() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("load_multiple_aggregator")?;
    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_load_with_snapshot() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("load_with_snapshot")?;
    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn fjall_invalid_original_version() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("invalid_original_version")?;
    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn fjall_subscriber_running() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscriber_running")?;
    evento_test::subscriber_running(&executor).await
}

#[tokio::test]
async fn fjall_subscribe() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe")?;
    evento_test::subscribe(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_routing_key() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe_routing_key")?;
    evento_test::subscribe_routing_key(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe_default")?;
    evento_test::subscribe_default(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe_multiple_aggregator")?;
    evento_test::subscribe_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe_routing_key_multiple_aggregator")?;
    evento_test::subscribe_routing_key_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("subscribe_default_multiple_aggregator")?;
    evento_test::subscribe_default_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_all_commands() -> anyhow::Result<()> {
    let (executor, _temp_dir) = create_fjall_executor("all_commands")?;
    evento_test::all_commands(&executor).await
}
