//! The accord consensus-journal migration applies and reverts on **MySQL**.
//! Gated by the `accord` + `mysql` features; needs a server at the convention URL.

#![cfg(all(feature = "accord", feature = "mysql"))]

use sqlx::migrate::MigrateDatabase;
use sqlx::{MySql, MySqlPool};
use sqlx_migrator::{Migrate, Plan};

const DB: &str = "accord_migrator_my";
const URL: &str = "mysql://root:root@localhost:3306/accord_migrator_my";

const TABLES: [&str; 4] = [
    "accord_commands",
    "accord_meta",
    "accord_metadata_log",
    "accord_acceptors",
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
async fn accord_journal_migration_applies_and_reverts() {
    let _ = MySql::drop_database(URL).await;
    MySql::create_database(URL).await.unwrap();
    let pool = MySqlPool::connect(URL).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let migrator = evento_sql_migrator::new::<MySql>().unwrap();
    migrator.run(&mut *conn, &Plan::apply_all()).await.unwrap();
    assert_eq!(count_tables(&pool).await, 4, "all accord tables created");

    migrator.run(&mut *conn, &Plan::revert_all()).await.unwrap();
    assert_eq!(count_tables(&pool).await, 0, "all accord tables dropped on revert");
}
