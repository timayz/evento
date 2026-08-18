//! `SqlJournal` integration tests on **MySQL** (a live server at the repo's
//! convention URL, `mysql://root:root@localhost:3306`). Each scenario runs in its own
//! database for isolation. Gated by the `mysql` feature.

use evento_accord::Journal;
use evento_sql::SqlJournal;
use sqlx::migrate::MigrateDatabase;
use sqlx::{MySql, MySqlPool};

mod sql_support;
use sql_support::{command, timestamp};

const BASE: &str = "mysql://root:root@localhost:3306";

/// A freshly-created, migrated journal in its own database `name`.
async fn fresh(name: &str) -> SqlJournal<MySql> {
    let url = format!("{BASE}/{name}");
    let _ = MySql::drop_database(&url).await;
    MySql::create_database(&url).await.unwrap();
    let journal = SqlJournal::new(MySqlPool::connect(&url).await.unwrap());
    journal.migrate().await.unwrap();
    journal
}

#[tokio::test]
async fn round_trips_commands_metadata_and_acceptors() {
    sql_support::round_trip(&fresh("accord_my_round_trip").await).await;
}

#[tokio::test]
async fn group_commit_batches_staged_writes() {
    sql_support::group_commit(&fresh("accord_my_group_commit").await).await;
}

#[tokio::test]
async fn truncate_drops_below_the_watermark() {
    sql_support::truncate(&fresh("accord_my_truncate").await).await;
}

#[tokio::test]
async fn flush_dedupes_restaged_txn() {
    sql_support::flush_dedupes_restaged_txn(&fresh("accord_my_flush_dedupe").await).await;
}

#[tokio::test]
async fn metadata_batch_append() {
    sql_support::metadata_batch_append(&fresh("accord_my_meta_batch").await).await;
}

#[tokio::test]
async fn survives_reconnect() {
    let name = "accord_my_reconnect";
    let b = command(200, 2);
    {
        let journal = fresh(name).await;
        journal.record(&command(100, 1)).await.unwrap();
        journal.record(&b).await.unwrap();
        journal.truncate(timestamp(150, 0)).await.unwrap();
        journal.close().await;
    }
    let journal = SqlJournal::new(MySqlPool::connect(&format!("{BASE}/{name}")).await.unwrap());
    let all = journal.load_all().await.unwrap();
    assert_eq!(
        all.len(),
        1,
        "consensus state is durable across a reconnect"
    );
    assert_eq!(all[0].txn, b.txn);
    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(150, 0))
    );
}
