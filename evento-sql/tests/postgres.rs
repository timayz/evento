use evento::sql::Sql;
use sqlx::PgPool;

mod pool;

#[tokio::test]
async fn postgres_routing_key() -> anyhow::Result<()> {
    let executor = create_postgres_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn postgres_load() -> anyhow::Result<()> {
    let executor = create_postgres_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn postgres_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = create_postgres_executor("load_multiple_aggregator").await?;

    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn postgres_load_with_snapshot() -> anyhow::Result<()> {
    let executor = create_postgres_executor("load_with_snapshot").await?;

    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn postgres_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_postgres_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn postgres_subscriber_running() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe").await?;

    evento_test::subscribe::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_routing_key").await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe_default() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_default").await?;

    evento_test::subscribe_default::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_multiple_aggregator").await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_co_keyed_aggregator").await?;

    evento_test::subscribe_co_keyed_aggregator::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_load_co_keyed_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("load_co_keyed_aggregator").await?;

    evento_test::load_co_keyed_aggregator::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_routing_key_multiple_aggregator").await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::Postgres>>(&pool.into())
        .await
}

#[tokio::test]
async fn postgres_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_default_multiple_aggregator").await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_all_commands() -> anyhow::Result<()> {
    let pool = create_postgres_pool("all_commands").await?;

    evento_test::all_commands::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_forward_asc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("forward_asc").await?;

    pool::forward_asc(pool).await
}

#[tokio::test]
async fn postgres_forward_desc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("forward_desc").await?;

    pool::forward_desc(pool).await
}

#[tokio::test]
async fn postgres_backward_asc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("backward_asc").await?;

    pool::backward_asc(pool).await
}

#[tokio::test]
async fn postgres_backward_desc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("backward_desc").await?;

    pool::backward_desc(pool).await
}

async fn create_postgres_executor(key: impl Into<String>) -> anyhow::Result<Sql<sqlx::Postgres>> {
    Ok(create_postgres_pool(key).await?.into())
}

async fn create_postgres_pool(key: impl Into<String>) -> anyhow::Result<PgPool> {
    let key = key.into();
    let url = format!("postgres://postgres:postgres@localhost:5432/{key}");

    pool::create_pool(url).await
}

#[tokio::test]
async fn postgres_read_order_timestamp() -> anyhow::Result<()> {
    let executor = create_postgres_executor("read_order_timestamp").await?;
    evento_test::read_order_timestamp(&executor).await
}

#[tokio::test]
async fn postgres_exact_filter() -> anyhow::Result<()> {
    let executor = create_postgres_executor("exact_filter").await?;
    evento_test::exact_filter(&executor).await
}

#[tokio::test]
async fn postgres_concurrent_append() -> anyhow::Result<()> {
    let executor = create_postgres_executor("concurrent_append").await?;
    evento_test::concurrent_append(&executor).await
}

#[tokio::test]
async fn postgres_strict_unhandled() -> anyhow::Result<()> {
    let executor = create_postgres_executor("strict_unhandled").await?;
    evento_test::strict_unhandled(&executor).await
}

#[tokio::test]
async fn postgres_tombstone() -> anyhow::Result<()> {
    let executor = create_postgres_executor("tombstone").await?;
    evento_test::tombstone(&executor).await
}

#[tokio::test]
async fn postgres_subscription_all_counts() -> anyhow::Result<()> {
    let executor = create_postgres_executor("subscription_all_counts").await?;
    evento_test::subscription_all_counts(&executor).await
}

#[tokio::test]
async fn postgres_snapshot_revision_scope() -> anyhow::Result<()> {
    let executor = create_postgres_executor("snapshot_revision_scope").await?;
    evento_test::snapshot_revision_scope(&executor).await
}
