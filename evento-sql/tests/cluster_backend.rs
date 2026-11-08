//! [`Sql`] as a `ClusterBackend`, on each dialect: the shared scenario over a
//! freshly migrated store whose journal shares its pool. Gated by the
//! `cluster` feature; Postgres and MySQL need `make up`.

use evento::sql::Sql;

mod pool;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_cluster_backend() -> anyhow::Result<()> {
    let pool =
        pool::create_pool::<sqlx::Sqlite>("sqlite:../target/tmp/test_sql_cluster_backend.db")
            .await?;
    evento_test::cluster_backend(&Sql::from(pool)).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_cluster_backend() -> anyhow::Result<()> {
    let pool = pool::create_pool::<sqlx::Postgres>(
        "postgres://postgres:postgres@localhost:5432/cluster_backend",
    )
    .await?;
    evento_test::cluster_backend(&Sql::from(pool)).await
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_cluster_backend() -> anyhow::Result<()> {
    let pool = pool::create_pool::<sqlx::MySql>("mysql://root:root@localhost:3306/cluster_backend")
        .await?;
    evento_test::cluster_backend(&Sql::from(pool)).await
}
