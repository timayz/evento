use evento_fjall::Fjall;

#[tokio::test]
async fn fjall_routing_key() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn fjall_load() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::load(&executor).await
}

#[tokio::test]
async fn fjall_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_load_with_snapshot() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn fjall_invalid_original_version() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn fjall_subscriber_running() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscriber_running(&executor).await
}

#[tokio::test]
async fn fjall_subscribe() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_low_latency() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_low_latency(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_routing_key() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_routing_key(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_default(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_co_keyed_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::load_co_keyed_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_routing_key_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_default_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default_routing_key() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_default_routing_key(&executor).await
}

#[tokio::test]
async fn fjall_subscribe_default_routing_key_all_isolation() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscribe_default_routing_key_all_isolation(&executor).await
}

#[tokio::test]
async fn fjall_all_commands() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::all_commands(&executor).await
}

#[tokio::test]
async fn fjall_read_order_timestamp() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_order_timestamp(&executor).await
}

#[tokio::test]
async fn fjall_exact_filter() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::exact_filter(&executor).await
}

#[tokio::test]
async fn fjall_concurrent_append() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::concurrent_append(&executor).await
}

#[tokio::test]
async fn fjall_strict_unhandled() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::strict_unhandled(&executor).await
}

#[tokio::test]
async fn fjall_tombstone() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::tombstone(&executor).await
}

#[tokio::test]
async fn fjall_subscription_data() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_data(&executor).await
}

#[tokio::test]
async fn fjall_subscription_stop_reason() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_stop_reason(&executor).await
}

#[tokio::test]
async fn fjall_subscription_all_counts() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_all_counts(&executor).await
}

#[tokio::test]
async fn fjall_snapshot_revision_scope() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::snapshot_revision_scope(&executor).await
}

#[tokio::test]
async fn fjall_snapshot_projection_scope() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::snapshot_projection_scope(&executor).await
}

#[tokio::test]
async fn fjall_upcast_load() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_load(&executor).await
}

#[tokio::test]
async fn fjall_upcast_nearest_target() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_nearest_target(&executor).await
}

#[tokio::test]
async fn fjall_upcast_explicit_wins() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_explicit_wins(&executor).await
}

#[tokio::test]
async fn fjall_upcast_skip() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_skip(&executor).await
}

#[tokio::test]
async fn fjall_upcast_strict_unhandled_target() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_strict_unhandled_target(&executor).await
}

#[tokio::test]
async fn fjall_upcast_subscription() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_subscription(&executor).await
}

#[tokio::test]
async fn fjall_upcast_projection_subscription() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_projection_subscription(&executor).await
}

#[tokio::test]
async fn fjall_upcast_tombstone() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_tombstone(&executor).await
}

#[tokio::test]
async fn fjall_upcast_has_event() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::upcast_has_event(&executor).await
}

#[tokio::test]
async fn fjall_read_stream() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_stream(&executor).await
}

#[tokio::test]
async fn fjall_read_drains_pages() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_drains_pages(&executor).await
}

#[tokio::test]
async fn fjall_read_limit() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_limit(&executor).await
}

#[tokio::test]
async fn fjall_read_page_cursor() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_page_cursor(&executor).await
}

#[tokio::test]
async fn fjall_read_decode() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_decode(&executor).await
}

#[tokio::test]
async fn fjall_read_routing_key() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::read_routing_key(&executor).await
}

#[tokio::test]
async fn fjall_subscription_start_from_latest() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_start_from_latest(&executor).await
}

#[tokio::test]
async fn fjall_subscription_ephemeral() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_ephemeral(&executor).await
}

#[tokio::test]
async fn fjall_subscription_ephemeral_start_from_latest() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_ephemeral_start_from_latest(&executor).await
}

#[tokio::test]
async fn fjall_subscription_ephemeral_concurrent_same_key() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_ephemeral_concurrent_same_key(&executor).await
}

#[tokio::test]
async fn fjall_subscription_context_stop() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_context_stop(&executor).await
}

#[tokio::test]
async fn fjall_subscription_live() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_live(&executor).await
}

#[tokio::test]
async fn fjall_subscription_ephemeral_run_once() -> anyhow::Result<()> {
    let executor = Fjall::temporary()?;
    evento_test::subscription_ephemeral_run_once(&executor).await
}
