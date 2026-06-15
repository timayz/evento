//! Phase B parity — the SQL-backed `Journal` (`SqlJournal<DB>`) on **SQLite**:
//! round-trips every record type, truncates by watermark, batches a group commit
//! (shared `sql_support` scenarios), and survives a full close/reopen.

use std::path::Path;

use evento_accord::Journal;
use evento_sql::SqlJournal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Sqlite;

mod sql_support;
use sql_support::{command, layout, timestamp};

/// A fresh in-memory journal (single connection so the in-memory DB is shared).
async fn memory() -> SqlJournal<Sqlite> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let journal = SqlJournal::new(pool);
    journal.migrate().await.unwrap();
    journal
}

/// A fresh journal at `path` (for the close/reopen durability test).
async fn at(path: &Path) -> SqlJournal<Sqlite> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
    let journal = SqlJournal::new(pool);
    journal.migrate().await.unwrap();
    journal
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trips_commands_metadata_and_acceptors() {
    sql_support::round_trip(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_commit_batches_staged_writes() {
    sql_support::group_commit(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_drops_below_the_watermark() {
    sql_support::truncate(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn survives_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("accord_sql_reopen")
        .tempfile()
        .unwrap();

    let a = command(100, 1);
    let b = command(200, 2);

    {
        let journal = at(temp.path()).await;
        journal.record(&a).await.unwrap();
        journal.record(&b).await.unwrap();
        journal.append_metadata(1, &layout(&[0, 1, 2])).await.unwrap();
        journal.truncate(timestamp(150, 0)).await.unwrap(); // drops `a`, keeps `b`
        journal.close().await;
    }

    let journal = at(temp.path()).await;
    let all = journal.load_all().await.unwrap();
    assert_eq!(all.len(), 1, "durable across restart, minus the truncated prefix");
    assert_eq!(all[0].txn, b.txn);
    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(150, 0)),
        "the watermark survives the restart"
    );
    assert_eq!(journal.load_metadata().await.unwrap().len(), 1);
}
