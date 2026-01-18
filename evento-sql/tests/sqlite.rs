use evento::sql::{RwSqlite, Sql};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::str::FromStr;

mod pool;

fn rw_from_pools(pools: (SqlitePool, SqlitePool)) -> RwSqlite {
    let (r, w) = pools;
    (evento::Sqlite::from(r), evento::Sqlite::from(w)).into()
}

#[tokio::test]
async fn sqlite_routing_key() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn sqlite_load() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn sqlite_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("load_multiple_aggregator").await?;

    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn sqlite_load_with_snapshot() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("load_with_snapshot").await?;

    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn sqlite_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn sqlite_subscriber_running() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe").await?;

    evento_test::subscribe::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_routing_key").await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe_default() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_default").await?;

    evento_test::subscribe_default::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_multiple_aggregator").await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_routing_key_multiple_aggregator").await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_default_multiple_aggregator").await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_all_commands() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("all_commands").await?;

    evento_test::all_commands::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn rw_sqlite_routing_key() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn rw_sqlite_load() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn rw_sqlite_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("load_multiple_aggregator").await?;

    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn rw_sqlite_load_with_snapshot() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("load_with_snapshot").await?;

    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn rw_sqlite_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn rw_sqlite_subscriber_running() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscriber_running").await?;

    evento_test::subscriber_running::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe").await?;

    evento_test::subscribe::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_routing_key").await?;

    evento_test::subscribe_routing_key::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_default() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_default").await?;

    evento_test::subscribe_default::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_multiple_aggregator").await?;

    evento_test::subscribe_multiple_aggregator::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_routing_key_multiple_aggregator").await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_default_multiple_aggregator").await?;

    evento_test::subscribe_default_multiple_aggregator::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn rw_sqlite_all_commands() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("all_commands").await?;

    evento_test::all_commands::<RwSqlite>(&rw_from_pools(pool)).await
}

#[tokio::test]
async fn sqlite_forward_asc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("forward_asc").await?;

    pool::forward_asc(pool).await
}

#[tokio::test]
async fn sqlite_forward_desc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("forward_desc").await?;

    pool::forward_desc(pool).await
}

#[tokio::test]
async fn sqlite_backward_asc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("backward_asc").await?;

    pool::backward_asc(pool).await
}

#[tokio::test]
async fn sqlite_backward_desc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("backward_desc").await?;

    pool::backward_desc(pool).await
}

async fn create_rw_sqlite_executor(
    key: impl Into<String>,
) -> anyhow::Result<evento::Rw<evento::Sqlite, evento::Sqlite>> {
    let (r, w) = create_rw_sqlite_pool(key).await?;
    Ok((evento::Sqlite::from(r), evento::Sqlite::from(w)).into())
}

async fn create_sqlite_executor(key: impl Into<String>) -> anyhow::Result<evento::Evento> {
    let executor: evento::Sqlite = create_sqlite_pool(key).await?.into();

    Ok(executor.into())
}

async fn create_rw_sqlite_pool(key: impl Into<String>) -> anyhow::Result<(SqlitePool, SqlitePool)> {
    let key = key.into();
    let key = format!("rw_{key}");
    let url = format!("sqlite:../target/tmp/test_sql_{key}.db");

    let w = create_sqlite_pool(key).await?;
    sqlx::query("PRAGMA journal_mode = WAL").execute(&w).await?;
    sqlx::query("PRAGMA busy_timeout = 5000")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA cache_size = -20000")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA foreign_keys = true")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA temp_store = memory")
        .execute(&w)
        .await?;

    let options = SqliteConnectOptions::from_str(&url)?.read_only(true);

    let r = SqlitePoolOptions::new().connect_with(options).await?;
    sqlx::query("PRAGMA journal_mode = WAL").execute(&r).await?;
    sqlx::query("PRAGMA busy_timeout = 5000")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA cache_size = -20000")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA foreign_keys = true")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA temp_store = memory")
        .execute(&r)
        .await?;

    Ok((r, w))
}

async fn create_sqlite_pool(key: impl Into<String>) -> anyhow::Result<SqlitePool> {
    let key = key.into();
    let url = format!("sqlite:../target/tmp/test_sql_{key}.db");

    pool::create_pool(url).await
}
