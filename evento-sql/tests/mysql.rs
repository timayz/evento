use evento::sql::Sql;
use sqlx::MySqlPool;

mod pool;

async fn create_mysql_pool(key: impl Into<String>) -> anyhow::Result<MySqlPool> {
    let key = key.into();
    let url = format!("mysql://root:root@localhost:3306/{key}");

    pool::create_pool(url).await
}

async fn create_mysql_executor(key: impl Into<String>) -> anyhow::Result<Sql<sqlx::MySql>> {
    Ok(create_mysql_pool(key).await?.into())
}

#[tokio::test]
async fn mysql_routing_key() -> anyhow::Result<()> {
    let executor = create_mysql_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn mysql_load() -> anyhow::Result<()> {
    let executor = create_mysql_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn mysql_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = create_mysql_executor("load_multiple_aggregator").await?;

    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn mysql_load_with_snapshot() -> anyhow::Result<()> {
    let executor = create_mysql_executor("load_with_snapshot").await?;

    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn mysql_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_mysql_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn mysql_subscriber_running() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe").await?;

    evento_test::subscribe::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_routing_key").await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_default() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_default").await?;

    evento_test::subscribe_default::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_multiple_aggregator").await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_co_keyed_aggregator").await?;

    evento_test::subscribe_co_keyed_aggregator::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("load_co_keyed_aggregator").await?;

    evento_test::load_co_keyed_aggregator::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_routing_key_multiple_aggregator").await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_default_multiple_aggregator").await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_all_commands() -> anyhow::Result<()> {
    let pool = create_mysql_pool("all_commands").await?;

    evento_test::all_commands::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_forward_asc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("forward_asc").await?;

    pool::forward_asc(pool).await
}

#[tokio::test]
async fn mysql_forward_desc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("forward_desc").await?;

    pool::forward_desc(pool).await
}

#[tokio::test]
async fn mysql_backward_asc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("backward_asc").await?;

    pool::backward_asc(pool).await
}

#[tokio::test]
async fn mysql_backward_desc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("backward_desc").await?;

    pool::backward_desc(pool).await
}

#[tokio::test]
async fn mysql_read_order_timestamp() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_order_timestamp").await?;
    evento_test::read_order_timestamp(&executor).await
}

#[tokio::test]
async fn mysql_exact_filter() -> anyhow::Result<()> {
    let executor = create_mysql_executor("exact_filter").await?;
    evento_test::exact_filter(&executor).await
}

#[tokio::test]
async fn mysql_write_restamps_client_clock() -> anyhow::Result<()> {
    let executor = create_mysql_executor("write_restamps_client_clock").await?;
    evento_test::write_restamps_client_clock(&executor).await
}

#[tokio::test]
async fn mysql_concurrent_append() -> anyhow::Result<()> {
    let executor = create_mysql_executor("concurrent_append").await?;
    evento_test::concurrent_append(&executor).await
}

#[tokio::test]
async fn mysql_strict_unhandled() -> anyhow::Result<()> {
    let executor = create_mysql_executor("strict_unhandled").await?;
    evento_test::strict_unhandled(&executor).await
}

#[tokio::test]
async fn mysql_tombstone() -> anyhow::Result<()> {
    let executor = create_mysql_executor("tombstone").await?;
    evento_test::tombstone(&executor).await
}

#[tokio::test]
async fn mysql_subscription_data() -> anyhow::Result<()> {
    let executor = create_mysql_executor("subscription_data").await?;
    evento_test::subscription_data(&executor).await
}

#[tokio::test]
async fn mysql_subscription_stop_reason() -> anyhow::Result<()> {
    let executor = create_mysql_executor("subscription_stop_reason").await?;
    evento_test::subscription_stop_reason(&executor).await
}

#[tokio::test]
async fn mysql_subscription_start_from_latest() -> anyhow::Result<()> {
    let executor = create_mysql_executor("subscription_start_from_latest").await?;
    evento_test::subscription_start_from_latest(&executor).await
}

#[tokio::test]
async fn mysql_subscription_all_counts() -> anyhow::Result<()> {
    let executor = create_mysql_executor("subscription_all_counts").await?;
    evento_test::subscription_all_counts(&executor).await
}

#[tokio::test]
async fn mysql_snapshot_revision_scope() -> anyhow::Result<()> {
    let executor = create_mysql_executor("snapshot_revision_scope").await?;
    evento_test::snapshot_revision_scope(&executor).await
}

#[tokio::test]
async fn mysql_snapshot_projection_scope() -> anyhow::Result<()> {
    let executor = create_mysql_executor("snapshot_projection_scope").await?;
    evento_test::snapshot_projection_scope(&executor).await
}

#[tokio::test]
async fn mysql_upcast_load() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_load").await?;
    evento_test::upcast_load(&executor).await
}

#[tokio::test]
async fn mysql_upcast_nearest_target() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_nearest_target").await?;
    evento_test::upcast_nearest_target(&executor).await
}

#[tokio::test]
async fn mysql_upcast_explicit_wins() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_explicit_wins").await?;
    evento_test::upcast_explicit_wins(&executor).await
}

#[tokio::test]
async fn mysql_upcast_skip() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_skip").await?;
    evento_test::upcast_skip(&executor).await
}

#[tokio::test]
async fn mysql_upcast_strict_unhandled_target() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_strict_unhandled_target").await?;
    evento_test::upcast_strict_unhandled_target(&executor).await
}

#[tokio::test]
async fn mysql_upcast_subscription() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_subscription").await?;
    evento_test::upcast_subscription(&executor).await
}

#[tokio::test]
async fn mysql_upcast_projection_subscription() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_projection_subscription").await?;
    evento_test::upcast_projection_subscription(&executor).await
}

#[tokio::test]
async fn mysql_upcast_tombstone() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_tombstone").await?;
    evento_test::upcast_tombstone(&executor).await
}

#[tokio::test]
async fn mysql_upcast_has_event() -> anyhow::Result<()> {
    let executor = create_mysql_executor("upcast_has_event").await?;
    evento_test::upcast_has_event(&executor).await
}

#[tokio::test]
async fn mysql_read_stream() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_stream").await?;
    evento_test::read_stream(&executor).await
}

#[tokio::test]
async fn mysql_read_drains_pages() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_drains_pages").await?;
    evento_test::read_drains_pages(&executor).await
}

#[tokio::test]
async fn mysql_read_limit() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_limit").await?;
    evento_test::read_limit(&executor).await
}

#[tokio::test]
async fn mysql_read_page_cursor() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_page_cursor").await?;
    evento_test::read_page_cursor(&executor).await
}

#[tokio::test]
async fn mysql_read_decode() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_decode").await?;
    evento_test::read_decode(&executor).await
}

#[tokio::test]
async fn mysql_read_routing_key() -> anyhow::Result<()> {
    let executor = create_mysql_executor("read_routing_key").await?;
    evento_test::read_routing_key(&executor).await
}
