//! Phase B parity — the SQL-backed `Journal` (`SqlJournal<DB>`) on **SQLite**:
//! round-trips every record type, truncates by watermark, batches a group commit
//! (shared `sql_support` scenarios), and survives a full close/reopen.

use std::path::Path;

use evento_accord::{Ballot, Journal, Key, NodeId, TxnId};
use evento_sql::SqlJournal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool};

mod sql_support;
use sql_support::{command, layout, timestamp};

/// A fresh in-memory journal (single connection so the in-memory DB is shared).
async fn memory() -> SqlJournal<Sqlite> {
    memory_with_pool().await.0
}

/// [`memory`], plus a handle on the journal's (one-connection) pool.
async fn memory_with_pool() -> (SqlJournal<Sqlite>, SqlitePool) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let journal = SqlJournal::new(pool.clone());
    journal.migrate().await.unwrap();
    (journal, pool)
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
async fn flush_dedupes_restaged_txn() {
    sql_support::flush_dedupes_restaged_txn(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_chunks_large_batches() {
    sql_support::flush_chunks_large_batches(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_batch_append() {
    sql_support::metadata_batch_append(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_and_promises() {
    sql_support::routes_and_promises(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_truncate_keeps_staged() {
    let (journal, pool) = memory_with_pool().await;
    sql_support::failed_truncate_keeps_staged(&journal, async {
        sqlx::query("DROP TABLE accord_meta")
            .execute(&pool)
            .await
            .unwrap();
    })
    .await;
}

/// On a file-backed journal with a multi-connection pool, so the flush and
/// the truncation can really overlap on the database side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_races_flush() {
    let temp = tempfile::tempdir().unwrap();
    let journal = at(&temp.path().join("journal.db")).await;
    sql_support::truncate_races_flush(&journal).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clock_bound() {
    sql_support::clock_bound(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amnesia_fence() {
    sql_support::amnesia_fence(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fences() {
    sql_support::fences(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_fence() {
    sql_support::vote_fence(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_state_and_slot_gc() {
    sql_support::copy_state_and_slot_gc(&memory().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_marks() {
    sql_support::peer_marks(&memory().await).await;
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
        journal
            .append_metadata(1, &layout(&[0, 1, 2]))
            .await
            .unwrap();
        journal.truncate(timestamp(150, 0)).await.unwrap(); // drops `a`, keeps `b`
        journal
            .save_route(b.txn, &[Key("acc".into()), Key("other".into())])
            .await
            .unwrap();
        journal
            .save_promise(TxnId(timestamp(400, 1)), Ballot(timestamp(500, 2)))
            .await
            .unwrap();
        journal.flush().await.unwrap();
        journal.close().await;
    }

    let journal = at(temp.path()).await;
    let all = journal.load_all().await.unwrap();
    assert_eq!(
        all.len(),
        1,
        "durable across restart, minus the truncated prefix"
    );
    assert_eq!(all[0].txn, b.txn);
    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(150, 0)),
        "the watermark survives the restart"
    );
    assert_eq!(journal.load_metadata().await.unwrap().len(), 1);
    assert_eq!(
        journal.load_routes().await.unwrap(),
        vec![(b.txn, vec![Key("acc".into()), Key("other".into())])],
        "a route survives the restart"
    );
    assert_eq!(
        journal.load_promises().await.unwrap(),
        vec![(TxnId(timestamp(400, 1)), Ballot(timestamp(500, 2)))],
        "and so does a blind promise"
    );
}

#[tokio::test]
async fn rejoin_bounds_survive_close_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("journal.db");
    let bounds = vec![
        (NodeId(2), timestamp(300, 1)),
        (NodeId(4), timestamp(120, 0)),
    ];
    {
        let journal = at(&path).await;
        assert!(journal.load_rejoin_bounds().await.unwrap().is_empty());
        journal.save_rejoin_bounds(&bounds).await.unwrap();
        journal.close().await;
    }
    let journal = at(&path).await;
    assert_eq!(journal.load_rejoin_bounds().await.unwrap(), bounds);
    // Replaced whole, and an empty set clears the row.
    journal.save_rejoin_bounds(&bounds[..1]).await.unwrap();
    assert_eq!(journal.load_rejoin_bounds().await.unwrap(), bounds[..1]);
    journal.save_rejoin_bounds(&[]).await.unwrap();
    assert!(journal.load_rejoin_bounds().await.unwrap().is_empty());
}

#[tokio::test]
async fn clock_bound_survives_close_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("journal.db");
    {
        let journal = at(&path).await;
        journal.save_clock_bound(1_234).await.unwrap();
        journal.close().await;
    }
    let journal = at(&path).await;
    assert_eq!(journal.load_clock_bound().await.unwrap(), Some(1_234));
}

#[tokio::test]
async fn amnesia_fence_survives_close_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("journal.db");
    let fence = evento_accord::AmnesiaFence {
        fence_micros: 1_234,
        joined: true,
        votes_restored: true,
    };
    {
        let journal = at(&path).await;
        journal.save_amnesia_fence(&fence).await.unwrap();
        journal.close().await;
    }
    let journal = at(&path).await;
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(fence));
}
