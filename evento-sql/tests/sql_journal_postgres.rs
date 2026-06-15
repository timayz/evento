//! `SqlJournal` integration tests on **PostgreSQL** (a live server at the repo's
//! convention URL, `postgres://postgres:postgres@localhost:5432`). Each scenario runs
//! in its own database for isolation. Gated by the `postgres` feature.

use evento_accord::Journal;
use evento_sql::SqlJournal;
use sqlx::migrate::MigrateDatabase;
use sqlx::{PgPool, Postgres};

mod sql_support;
use sql_support::{command, timestamp};

const BASE: &str = "postgres://postgres:postgres@localhost:5432";

/// A freshly-created, migrated journal in its own database `name`.
async fn fresh(name: &str) -> SqlJournal<Postgres> {
    let url = format!("{BASE}/{name}");
    let _ = Postgres::drop_database(&url).await;
    Postgres::create_database(&url).await.unwrap();
    let journal = SqlJournal::new(PgPool::connect(&url).await.unwrap());
    journal.migrate().await.unwrap();
    journal
}

#[tokio::test]
async fn round_trips_commands_metadata_and_acceptors() {
    sql_support::round_trip(&fresh("accord_pg_round_trip").await).await;
}

#[tokio::test]
async fn group_commit_batches_staged_writes() {
    sql_support::group_commit(&fresh("accord_pg_group_commit").await).await;
}

#[tokio::test]
async fn truncate_drops_below_the_watermark() {
    sql_support::truncate(&fresh("accord_pg_truncate").await).await;
}

#[tokio::test]
async fn survives_reconnect() {
    let name = "accord_pg_reconnect";
    let b = command(200, 2);
    {
        let journal = fresh(name).await;
        journal.record(&command(100, 1)).await.unwrap();
        journal.record(&b).await.unwrap();
        journal.truncate(timestamp(150, 0)).await.unwrap(); // drops the 100-record
        journal.close().await;
    }
    // Reconnect a brand-new pool to the same database (tables already migrated).
    let journal = SqlJournal::new(PgPool::connect(&format!("{BASE}/{name}")).await.unwrap());
    let all = journal.load_all().await.unwrap();
    assert_eq!(all.len(), 1, "consensus state is durable across a reconnect");
    assert_eq!(all[0].txn, b.txn);
    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(150, 0))
    );
}
