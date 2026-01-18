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
