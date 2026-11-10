//! The cluster consensus-journal migration applies and reverts on **MySQL**.
//! Gated by the `cluster` + `mysql` features; needs a server at the convention URL.

#![cfg(all(feature = "cluster", feature = "mysql"))]

use sqlx::migrate::MigrateDatabase;
use sqlx::{MySql, MySqlPool};
use sqlx_migrator::Plan;

const DB: &str = "cluster_migrator_my";
const URL: &str = "mysql://root:root@localhost:3306/cluster_migrator_my";

const TABLES: [&str; 4] = [
    "cluster_commands",
    "cluster_meta",
    "cluster_metadata_log",
    "cluster_acceptors",
];

async fn count_tables(pool: &MySqlPool) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = ? AND table_name IN (?, ?, ?, ?)",
    )
    .bind(DB)
    .bind(TABLES[0])
    .bind(TABLES[1])
    .bind(TABLES[2])
    .bind(TABLES[3])
    .fetch_one(pool)
    .await
    .unwrap();
    n
}

#[tokio::test]
async fn cluster_journal_migration_applies_and_reverts() {
    let _ = MySql::drop_database(URL).await;
    MySql::create_database(URL).await.unwrap();
    let pool = MySqlPool::connect(URL).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let migrator = evento_sql_migrator::new::<MySql>().unwrap();
    migrator.run(&mut *conn, &Plan::apply_all()).await.unwrap();
    assert_eq!(count_tables(&pool).await, 4, "all cluster tables created");

    migrator.run(&mut *conn, &Plan::revert_all()).await.unwrap();
    assert_eq!(
        count_tables(&pool).await,
        0,
        "all cluster tables dropped on revert"
    );
}
