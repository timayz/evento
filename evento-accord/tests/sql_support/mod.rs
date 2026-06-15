//! Shared, DB-generic journal scenarios run by the sqlite / postgres / mysql
//! integration tests. Each scenario assumes a freshly-migrated, **empty** journal.
//! (A subdir module, so cargo does not compile it as its own test binary.)
#![allow(dead_code)]

use evento_accord::{
    AcceptorRecord, Ballot, CommandState, Journal, Key, NodeId, SqlJournal, Status, Timestamp,
    TxnId,
};
use evento_core::Event;
use sqlx::Database;

pub fn timestamp(micros: u64, node: u64) -> Timestamp {
    Timestamp {
        micros,
        logical: 0,
        node: NodeId(node),
    }
}

pub fn command(micros: u64, version: u16) -> CommandState {
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

pub fn layout(nodes: &[u64]) -> Vec<Vec<NodeId>> {
    vec![nodes.iter().map(|&n| NodeId(n)).collect()]
}

/// Round-trips commands (incl. upsert), the metadata log (idempotent, ascending),
/// and acceptor state.
pub async fn round_trip<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let a = command(100, 1);
    let b = command(200, 2);
    journal.record(&a).await.unwrap();
    journal.record(&b).await.unwrap();

    let mut all = journal.load_all().await.unwrap();
    all.sort_by_key(|c| c.execute_at);
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].txn, a.txn);
    assert_eq!(all[1].events[0].version, 2);

    let loaded = journal.load(b.txn).await.unwrap().unwrap();
    assert_eq!(loaded.execute_at, b.execute_at);
    journal.record(&command(200, 2)).await.unwrap();
    assert_eq!(journal.load_all().await.unwrap().len(), 2, "re-record upserts");

    journal.append_metadata(2, &layout(&[0, 1, 2, 3])).await.unwrap();
    journal.append_metadata(1, &layout(&[0, 1, 2])).await.unwrap();
    journal.append_metadata(1, &layout(&[9, 9, 9])).await.unwrap(); // ignored
    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], (1, layout(&[0, 1, 2])), "first layout wins, ascending");
    assert_eq!(entries[1].0, 2);

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

/// Staged commands land in one `flush` (group commit).
pub async fn group_commit<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
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

/// `truncate` drops commands below the watermark (single range delete) and persists
/// the watermark.
pub async fn truncate<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
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
