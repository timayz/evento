//! The accord consensus-journal migration applies and reverts on **PostgreSQL**.
//! Gated by the `accord` + `postgres` features; needs a server at the convention URL.

#![cfg(all(feature = "accord", feature = "postgres"))]

use sqlx::migrate::MigrateDatabase;
use sqlx::{PgPool, Postgres};
use sqlx_migrator::{Migrate, Plan};

const URL: &str = "postgres://postgres:postgres@localhost:5432/accord_migrator_pg";

const TABLES: [&str; 4] = [
    "accord_commands",
    "accord_meta",
    "accord_metadata_log",
    "accord_acceptors",
];

async fn count_tables(pool: &PgPool) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = ANY($1)",
    )
    .bind(&TABLES[..])
    .fetch_one(pool)
    .await
    .unwrap();
    n
}

#[tokio::test]
async fn accord_journal_migration_applies_and_reverts() {
    let _ = Postgres::drop_database(URL).await;
    Postgres::create_database(URL).await.unwrap();
    let pool = PgPool::connect(URL).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let migrator = evento_sql_migrator::new::<Postgres>().unwrap();
    migrator.run(&mut *conn, &Plan::apply_all()).await.unwrap();
    assert_eq!(count_tables(&pool).await, 4, "all accord tables created");

    migrator.run(&mut *conn, &Plan::revert_all()).await.unwrap();
    assert_eq!(count_tables(&pool).await, 0, "all accord tables dropped on revert");
}
