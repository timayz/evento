#[path = "evento.rs"]
mod evento_test;

use evento::sql_migrator::{InitMigration, M0002, M0003, M0004};
use evento_accord::{AccordConfig, AccordExecutor, NodeAddr, ShutdownHandle};
use sqlx::{
    any::install_default_drivers, migrate::MigrateDatabase, Any, Database, Pool, SqlitePool,
};
use sqlx_migrator::{Migrate, Plan};
use std::sync::atomic::{AtomicU16, Ordering};

/// Global port counter to avoid conflicts between tests
static PORT_COUNTER: AtomicU16 = AtomicU16::new(19000);

// ============================================================
// AccordExecutor single-node tests using SQLite
// ============================================================

#[tokio::test]
async fn accord_routing_key() -> anyhow::Result<()> {
    let executor = create_executor("accord_routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn accord_load() -> anyhow::Result<()> {
    let executor = create_executor("accord_load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn accord_load_multiple_aggregator() -> anyhow::Result<()> {
    let executor = create_executor("accord_load_multiple_aggregator").await?;

    evento_test::load_multiple_aggregator(&executor).await
}

#[tokio::test]
async fn accord_load_with_snapshot() -> anyhow::Result<()> {
    let executor = create_executor("accord_load_with_snapshot").await?;

    evento_test::load_with_snapshot(&executor).await
}

#[tokio::test]
async fn accord_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_executor("accord_invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

// ============================================================
// AccordExecutor 3-node cluster tests
// ============================================================

#[tokio::test]
async fn accord_cluster_load() -> anyhow::Result<()> {
    let (executors, handles) = create_cluster("cluster_load", 3).await?;

    // Run test against first node (coordinator)
    let result = evento_test::load(&executors[0]).await;

    // Shutdown all nodes
    shutdown_cluster(handles).await;

    result
}

#[tokio::test]
async fn accord_cluster_routing_key() -> anyhow::Result<()> {
    let (executors, handles) = create_cluster("cluster_routing_key", 3).await?;

    let result = evento_test::routing_key(&executors[0]).await;

    shutdown_cluster(handles).await;

    result
}

#[tokio::test]
async fn accord_cluster_invalid_original_version() -> anyhow::Result<()> {
    let (executors, handles) = create_cluster("cluster_invalid_version", 3).await?;

    let result = evento_test::invalid_original_version(&executors[0]).await;

    shutdown_cluster(handles).await;

    result
}

// ============================================================
// Helper functions - Single node
// ============================================================

async fn create_executor(
    name: &str,
) -> anyhow::Result<AccordExecutor<evento::sql::Sql<sqlx::Sqlite>>> {
    let inner = create_sqlite_executor(name).await?;
    Ok(AccordExecutor::single_node(inner))
}

async fn create_sqlite_executor(
    key: impl Into<String>,
) -> anyhow::Result<evento::sql::Sql<sqlx::Sqlite>> {
    Ok(create_sqlite_pool(key).await?.into())
}

async fn create_sqlite_pool(key: impl Into<String>) -> anyhow::Result<SqlitePool> {
    let key = key.into();
    let url = format!("sqlite:../target/tmp/test_accord_{key}.db");

    create_pool(url).await
}

async fn create_pool<DB: Database>(url: impl Into<String>) -> anyhow::Result<Pool<DB>>
where
    for<'q> DB::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    sqlx_migrator::Migrator<DB>: sqlx_migrator::migrator::DatabaseOperation<DB>,
{
    install_default_drivers();

    let url = url.into();

    let _ = Any::drop_database(&url).await;
    Any::create_database(&url).await?;

    let pool = Pool::<DB>::connect(&url).await?;
    let mut conn = pool.acquire().await?;
    let migrator = evento::sql_migrator::new::<DB>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;

    Ok(pool)
}

// ============================================================
// Helper functions - Cluster
// ============================================================

type ClusterExecutor = AccordExecutor<evento::sql::Sql<sqlx::Sqlite>>;

async fn create_cluster(
    name: &str,
    node_count: usize,
) -> anyhow::Result<(Vec<ClusterExecutor>, Vec<ShutdownHandle>)> {
    // Get unique ports for this cluster
    let base_port = PORT_COUNTER.fetch_add(node_count as u16, Ordering::SeqCst);

    // Build node addresses
    let nodes: Vec<NodeAddr> = (0..node_count)
        .map(|i| NodeAddr {
            id: i as u16,
            host: "127.0.0.1".to_string(),
            port: base_port + i as u16,
        })
        .collect();

    let mut executors = Vec::with_capacity(node_count);
    let mut handles = Vec::with_capacity(node_count);

    // Create each node with its own database (simulating distributed nodes)
    for (i, local) in nodes.iter().enumerate() {
        let db_name = format!("{}_node{}", name, i);
        let inner = create_sqlite_executor(&db_name).await?;

        let config = AccordConfig::cluster(local.clone(), nodes.clone());
        let (executor, handle) = AccordExecutor::cluster(inner, config).await?;

        executors.push(executor);
        handles.push(handle);
    }

    // Wait for connections to establish
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok((executors, handles))
}

async fn shutdown_cluster(handles: Vec<ShutdownHandle>) {
    for handle in handles {
        handle
            .shutdown_with_timeout(std::time::Duration::from_secs(5))
            .await;
    }
}
