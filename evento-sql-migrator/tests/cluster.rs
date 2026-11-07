//! The optional cluster consensus-journal migration applies and reverts cleanly on
//! SQLite, creating exactly the four tables `SqlJournal` expects.

#![cfg(all(feature = "cluster", feature = "sqlite"))]

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Sqlite;
use sqlx_migrator::{Migrate, Plan};

const TABLES: [&str; 4] = [
    "cluster_commands",
    "cluster_meta",
    "cluster_metadata_log",
    "cluster_acceptors",
];

async fn table_exists(conn: &mut sqlx::SqliteConnection, name: &str) -> bool {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(name)
            .fetch_optional(&mut *conn)
            .await
            .unwrap();
    row.is_some()
}

#[tokio::test]
async fn cluster_journal_migration_applies_and_reverts() {
    // One connection so the in-memory database is shared across the whole test.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let migrator = evento_sql_migrator::new::<Sqlite>().unwrap();

    // Apply everything (event schema + the cluster journal migration).
    migrator.run(&mut *conn, &Plan::apply_all()).await.unwrap();
    for table in TABLES {
        assert!(
            table_exists(&mut conn, table).await,
            "{table} created by the cluster migration"
        );
    }

    // Revert everything — the cluster tables are dropped.
    migrator.run(&mut *conn, &Plan::revert_all()).await.unwrap();
    for table in TABLES {
        assert!(
            !table_exists(&mut conn, table).await,
            "{table} dropped on revert"
        );
    }
}
