//! Phase B parity — the SQL-backed `Journal` (`SqlJournal<DB>`) over SQLite:
//! round-trips every record type, truncates by watermark, batches a group commit,
//! and survives a full close/reopen (a real process restart), just like
//! `FjallJournal`.

use std::path::Path;

use evento_accord::{
    AcceptorRecord, Ballot, CommandState, Journal, Key, NodeId, SqlJournal, Status, Timestamp,
    TxnId,
};
use evento_core::Event;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Sqlite;

/// Opens (creating if absent) a SQLite-backed journal at `path` and migrates it.
async fn journal(path: &Path) -> SqlJournal<Sqlite> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
    let journal = SqlJournal::new(pool);
    journal.migrate().await.unwrap();
    journal
}

fn timestamp(micros: u64, node: u64) -> Timestamp {
    Timestamp {
        micros,
        logical: 0,
        node: NodeId(node),
    }
}

fn command(micros: u64, version: u16) -> CommandState {
    let txn = TxnId(timestamp(micros, 0));
    CommandState {
        txn,
        status: Status::Applied,
        promised: Ballot(txn.0),
        accepted: Ballot(txn.0),
        execute_at: txn.0,
        deps: vec![],
        keys: vec![Key("acc".into())],
        events: vec![Event {
            id: ulid::Ulid::new(),
            aggregate_type: "test/Account".into(),
            aggregate_id: "acc".into(),
            version,
            name: "Bumped".into(),
            ..Default::default()
        }],
        reply_to: NodeId(0),
        decision: Some(true),
        applied_conflict: Some(false),
    }
}

fn layout(nodes: &[u64]) -> Vec<Vec<NodeId>> {
    vec![nodes.iter().map(|&n| NodeId(n)).collect()]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trips_commands_metadata_and_acceptors() {
    let temp = tempfile::Builder::new()
        .prefix("accord_sql_rt")
        .tempfile()
        .unwrap();
    let journal = journal(temp.path()).await;

    let a = command(100, 1);
    let b = command(200, 2);
    journal.record(&a).await.unwrap();
    journal.record(&b).await.unwrap();

    let mut all = journal.load_all().await.unwrap();
    all.sort_by_key(|c| c.execute_at);
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].txn, a.txn);
    assert_eq!(all[1].events[0].version, 2);

    // Point lookup + re-record (upsert) of an advancing command.
    let loaded = journal.load(b.txn).await.unwrap().unwrap();
    assert_eq!(loaded.execute_at, b.execute_at);
    journal.record(&command(200, 2)).await.unwrap();
    assert_eq!(journal.load_all().await.unwrap().len(), 2, "re-record upserts");

    // Metadata log: out of order in, ascending out; idempotent re-append.
    journal.append_metadata(2, &layout(&[0, 1, 2, 3])).await.unwrap();
    journal.append_metadata(1, &layout(&[0, 1, 2])).await.unwrap();
    journal.append_metadata(1, &layout(&[9, 9, 9])).await.unwrap(); // ignored
    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], (1, layout(&[0, 1, 2])), "first layout wins, ascending");
    assert_eq!(entries[1].0, 2);

    // Acceptor state: upsert by epoch.
    journal
        .record_acceptor(
            2,
            &AcceptorRecord {
                promised: Ballot(timestamp(500, 1)),
                accepted: Some((Ballot(timestamp(500, 1)), layout(&[0, 1, 2, 3]))),
            },
        )
        .await
        .unwrap();
    let acceptors = journal.load_acceptors().await.unwrap();
    assert_eq!(acceptors.len(), 1);
    assert_eq!(acceptors[0].0, 2);
    assert_eq!(acceptors[0].1.promised, Ballot(timestamp(500, 1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_commit_batches_staged_writes() {
    let temp = tempfile::Builder::new()
        .prefix("accord_sql_gc")
        .tempfile()
        .unwrap();
    let journal = journal(temp.path()).await;

    // Stage several without flushing — they are durable no later than the flush.
    for i in 1..=5 {
        journal.stage(&command(i * 100, i as u16)).await.unwrap();
    }
    assert!(
        journal.load_all().await.unwrap().is_empty(),
        "staged rows land at flush"
    );

    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_all().await.unwrap().len(),
        5,
        "one flush commits the whole batch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_drops_below_the_watermark() {
    let temp = tempfile::Builder::new()
        .prefix("accord_sql_trunc")
        .tempfile()
        .unwrap();
    let journal = journal(temp.path()).await;

    let old = command(100, 1);
    let mid = command(200, 2);
    let new = command(300, 3);
    journal.record(&old).await.unwrap();
    journal.record(&mid).await.unwrap();
    journal.record(&new).await.unwrap();
    assert!(journal.load_watermark().await.unwrap().is_none());

    journal.truncate(timestamp(250, 0)).await.unwrap();

    let all = journal.load_all().await.unwrap();
    assert_eq!(all.len(), 1, "only the record above the watermark survives");
    assert_eq!(all[0].txn, new.txn);
    assert!(journal.load(old.txn).await.unwrap().is_none());
    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(250, 0))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn survives_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("accord_sql_reopen")
        .tempfile()
        .unwrap();

    let a = command(100, 1);
    let b = command(200, 2);

    // Open, write, then drop (close every connection — a real shutdown).
    {
        let journal = journal(temp.path()).await;
        journal.record(&a).await.unwrap();
        journal.record(&b).await.unwrap();
        journal.append_metadata(1, &layout(&[0, 1, 2])).await.unwrap();
        journal.truncate(timestamp(150, 0)).await.unwrap(); // drops `a`, keeps `b`
        journal.close().await;
    }

    // Reopen at the same path — a fresh process's view of the disk.
    let journal = journal(temp.path()).await;
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
