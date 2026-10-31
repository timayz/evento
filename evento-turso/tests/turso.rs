//! The shared conformance suite (`evento-test`) over a fresh in-memory
//! [`Turso`] per scenario, as `evento-fjall/tests/fjall.rs` runs it; plus the
//! `rw_*` variants over an `Rw<Turso, Turso>` pair and a few through the
//! type-erased `evento::Evento`, as the SQLite suite does.

use evento::turso::RwTurso;
use evento_turso::Turso;

async fn rw() -> anyhow::Result<RwTurso> {
    let turso = Turso::in_memory().await?;
    Ok((turso.clone(), turso).into())
}

#[tokio::test]
async fn turso_routing_key() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn turso_load() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::load(&executor).await
}

#[tokio::test]
async fn turso_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn turso_load_with_snapshot() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn turso_invalid_original_version() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn turso_subscriber_running() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscriber_running(&executor).await
}

#[tokio::test]
async fn turso_subscribe() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe(&executor).await
}

#[tokio::test]
async fn turso_subscribe_low_latency() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_low_latency(&executor).await
}

#[tokio::test]
async fn turso_subscribe_routing_key() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_routing_key(&executor).await
}

#[tokio::test]
async fn turso_subscribe_default() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_default(&executor).await
}

#[tokio::test]
async fn turso_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn turso_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_co_keyed_aggregator(&executor).await
}

#[tokio::test]
async fn turso_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::load_co_keyed_aggregator(&executor).await
}

#[tokio::test]
async fn turso_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_routing_key_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn turso_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_default_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn turso_subscribe_default_routing_key() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_default_routing_key(&executor).await
}

#[tokio::test]
async fn turso_subscribe_default_routing_key_all_isolation() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscribe_default_routing_key_all_isolation(&executor).await
}

#[tokio::test]
async fn turso_all_commands() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::all_commands(&executor).await
}

#[tokio::test]
async fn turso_read_order_timestamp() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_order_timestamp(&executor).await
}

#[tokio::test]
async fn turso_write_restamps_client_clock() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::write_restamps_client_clock(&executor).await
}

#[tokio::test]
async fn turso_exact_filter() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::exact_filter(&executor).await
}

#[tokio::test]
async fn turso_concurrent_append() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::concurrent_append(&executor).await
}

#[tokio::test]
async fn turso_strict_unhandled() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::strict_unhandled(&executor).await
}

#[tokio::test]
async fn turso_tombstone() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tombstone(&executor).await
}

#[tokio::test]
async fn turso_subscription_data() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_data(&executor).await
}

#[tokio::test]
async fn turso_subscription_stop_reason() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_stop_reason(&executor).await
}

#[tokio::test]
async fn turso_subscription_shutdown_acknowledges_in_flight() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_shutdown_acknowledges_in_flight(&executor).await
}

#[tokio::test]
async fn turso_subscription_all_counts() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_all_counts(&executor).await
}

#[tokio::test]
async fn turso_snapshot_revision_scope() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::snapshot_revision_scope(&executor).await
}

#[tokio::test]
async fn turso_snapshot_projection_scope() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::snapshot_projection_scope(&executor).await
}

#[tokio::test]
async fn turso_upcast_load() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_load(&executor).await
}

#[tokio::test]
async fn turso_upcast_nearest_target() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_nearest_target(&executor).await
}

#[tokio::test]
async fn turso_upcast_explicit_wins() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_explicit_wins(&executor).await
}

#[tokio::test]
async fn turso_upcast_skip() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_skip(&executor).await
}

#[tokio::test]
async fn turso_upcast_strict_unhandled_target() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_strict_unhandled_target(&executor).await
}

#[tokio::test]
async fn turso_upcast_subscription() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_subscription(&executor).await
}

#[tokio::test]
async fn turso_upcast_projection_subscription() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_projection_subscription(&executor).await
}

#[tokio::test]
async fn turso_upcast_tombstone() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_tombstone(&executor).await
}

#[tokio::test]
async fn turso_upcast_has_event() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::upcast_has_event(&executor).await
}

#[tokio::test]
async fn turso_read_stream() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_stream(&executor).await
}

#[tokio::test]
async fn turso_read_drains_pages() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_drains_pages(&executor).await
}

#[tokio::test]
async fn turso_write_validates_every_event() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::write_validates_every_event(&executor).await
}

#[tokio::test]
async fn turso_read_limit() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_limit(&executor).await
}

#[tokio::test]
async fn turso_read_page_cursor() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_page_cursor(&executor).await
}

#[tokio::test]
async fn turso_read_decode() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_decode(&executor).await
}

#[tokio::test]
async fn turso_read_routing_key() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::read_routing_key(&executor).await
}

#[tokio::test]
async fn turso_subscription_start_from_latest() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_start_from_latest(&executor).await
}

#[tokio::test]
async fn turso_subscription_ephemeral() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_ephemeral(&executor).await
}

#[tokio::test]
async fn turso_subscription_ephemeral_start_from_latest() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_ephemeral_start_from_latest(&executor).await
}

#[tokio::test]
async fn turso_subscription_ephemeral_concurrent_same_key() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_ephemeral_concurrent_same_key(&executor).await
}

#[tokio::test]
async fn turso_subscription_context_stop() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_context_stop(&executor).await
}

#[tokio::test]
async fn turso_subscription_live() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_live(&executor).await
}

#[tokio::test]
async fn turso_subscription_ephemeral_run_once() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::subscription_ephemeral_run_once(&executor).await
}

#[tokio::test]
async fn turso_replicate_large_batch() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::replicate_large_batch(&executor).await
}

#[tokio::test]
async fn turso_tenant_streams_are_isolated() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tenant_streams_are_isolated(&executor).await
}

#[tokio::test]
async fn turso_tenant_subscriptions() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tenant_subscriptions(&executor).await
}

#[tokio::test]
async fn turso_tenant_projection_subscription() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tenant_projection_subscription(&executor).await
}

#[tokio::test]
async fn turso_tenant_projection_write_gateway() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tenant_projection_write_gateway(&executor).await
}

#[tokio::test]
async fn turso_tenant_snapshots_are_isolated() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::tenant_snapshots_are_isolated(&executor).await
}

#[tokio::test]
async fn turso_delete_stream() -> anyhow::Result<()> {
    let executor = Turso::in_memory().await?;
    evento_test::delete_stream(&executor).await
}

#[tokio::test]
async fn rw_turso_routing_key() -> anyhow::Result<()> {
    evento_test::routing_key(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_load() -> anyhow::Result<()> {
    evento_test::load(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_load_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::load_multiple_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_load_with_snapshot() -> anyhow::Result<()> {
    evento_test::load_with_snapshot(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_invalid_original_version() -> anyhow::Result<()> {
    evento_test::invalid_original_version(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscriber_running() -> anyhow::Result<()> {
    evento_test::subscriber_running(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe() -> anyhow::Result<()> {
    evento_test::subscribe(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_routing_key() -> anyhow::Result<()> {
    evento_test::subscribe_routing_key(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_default() -> anyhow::Result<()> {
    evento_test::subscribe_default(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_multiple_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_co_keyed_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_load_co_keyed_aggregator() -> anyhow::Result<()> {
    evento_test::load_co_keyed_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_routing_key_multiple_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_default_multiple_aggregator(&rw().await?).await
}

#[tokio::test]
async fn rw_turso_all_commands() -> anyhow::Result<()> {
    evento_test::all_commands(&rw().await?).await
}

#[tokio::test]
async fn evento_turso_load() -> anyhow::Result<()> {
    let executor: evento::Evento = Turso::in_memory().await?.into();
    evento_test::load(&executor).await
}

#[tokio::test]
async fn evento_turso_all_commands() -> anyhow::Result<()> {
    let executor: evento::Evento = Turso::in_memory().await?.into();
    evento_test::all_commands(&executor).await
}

#[tokio::test]
async fn evento_turso_subscribe() -> anyhow::Result<()> {
    let executor: evento::Evento = Turso::in_memory().await?.into();
    evento_test::subscribe(&executor).await
}

#[tokio::test]
async fn evento_turso_tenant_streams_are_isolated() -> anyhow::Result<()> {
    let executor: evento::Evento = Turso::in_memory().await?.into();
    evento_test::tenant_streams_are_isolated(&executor).await
}
