//! Shared, DB-generic journal scenarios run by the sqlite / postgres / mysql
//! integration tests. Each scenario assumes a freshly-migrated, **empty** journal.
//! (A subdir module, so cargo does not compile it as its own test binary.)
#![allow(dead_code)]

use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, ConfigEntry, CopyPhase, CopyState,
    DecisionRecord, DurableMark, FenceRecord, Journal, Key, NodeId, ShardMap, Status, Timestamp,
    TxnId,
};
use evento_core::Event;
use evento_sql::SqlJournal;
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
        epoch: 0,
        txn,
        status: Status::Applied,
        promised: Ballot(txn.0),
        accepted: Ballot(txn.0),
        execute_at: txn.0,
        deps: vec![],
        keys: vec![Key("acc".into())],
        events: vec![Event {
            id: ulid::Ulid::generate(),
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

pub fn layout(nodes: &[u64]) -> ConfigEntry {
    ConfigEntry::active(vec![nodes.iter().map(|&n| NodeId(n)).collect()])
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
    assert_eq!(
        journal.load_all().await.unwrap().len(),
        2,
        "re-record upserts"
    );

    journal
        .append_metadata(2, &layout(&[0, 1, 2, 3]))
        .await
        .unwrap();
    journal
        .append_metadata(1, &layout(&[0, 1, 2]))
        .await
        .unwrap();
    journal
        .append_metadata(1, &layout(&[9, 9, 9]))
        .await
        .unwrap(); // ignored
    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0],
        (1, layout(&[0, 1, 2])),
        "first layout wins, ascending"
    );
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

/// Re-staging the same txn within one batch flushes the LAST staged value
/// through the multi-row upsert (Postgres would reject a statement updating
/// one row twice) and still drains the whole staged buffer.
pub async fn flush_dedupes_restaged_txn<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let mut state = command(100, 1);
    journal.stage(&state).await.unwrap();
    state.applied_conflict = Some(true);
    journal.stage(&state).await.unwrap();
    journal.stage(&command(200, 2)).await.unwrap();

    journal.flush().await.unwrap();

    let loaded = journal.load(state.txn).await.unwrap().unwrap();
    assert_eq!(
        loaded.applied_conflict,
        Some(true),
        "the last staged value for a re-staged txn wins"
    );
    assert_eq!(journal.load_all().await.unwrap().len(), 2);
    // The buffer drained fully (all three staged entries, not just the two
    // deduped rows): a second flush has nothing to write.
    journal.flush().await.unwrap();
    assert_eq!(journal.load_all().await.unwrap().len(), 2);
}

/// A staged batch larger than one multi-row statement chunk (8000 rows) lands
/// in a single flush.
pub async fn flush_chunks_large_batches<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    const TOTAL: u64 = 8_500;
    for i in 0..TOTAL {
        journal.stage(&command(1_000 + i, 1)).await.unwrap();
    }
    journal.flush().await.unwrap();
    assert_eq!(journal.load_all().await.unwrap().len(), TOTAL as usize);
}

/// A batched metadata append is idempotent per entry: an epoch that is already
/// durable keeps its first decided layout.
pub async fn metadata_batch_append<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    journal.append_metadata(2, &layout(&[0, 1])).await.unwrap();
    journal
        .append_metadata_batch(&[
            (1, layout(&[0])),
            (2, layout(&[9, 9, 9])),
            (3, layout(&[0, 1, 2])),
        ])
        .await
        .unwrap();

    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (1, layout(&[0])));
    assert_eq!(
        entries[1],
        (2, layout(&[0, 1])),
        "the first decided layout for an epoch wins over a batched re-append"
    );
    assert_eq!(entries[2], (3, layout(&[0, 1, 2])));
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

/// A truncation that fails (here: `accord_meta` is gone, so the watermark
/// upsert errors and the transaction rolls back) leaves the staged commands
/// below its bound for the next flush — nothing was deleted, so nothing may
/// be forgotten.
pub async fn failed_truncate_keeps_staged<DB>(
    journal: &SqlJournal<DB>,
    drop_meta_table: impl std::future::Future<Output = ()>,
) where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let old = command(100, 1);
    let new = command(300, 2);
    journal.stage(&old).await.unwrap();
    journal.stage(&new).await.unwrap();

    drop_meta_table.await;
    journal
        .truncate(timestamp(200, 0))
        .await
        .expect_err("the watermark upsert has no table to go to");

    // Commands only, so the flush does not need `accord_meta`.
    journal.flush().await.unwrap();
    let mut all = journal.load_all().await.unwrap();
    all.sort_by_key(|state| state.txn);
    assert_eq!(
        all.iter().map(|state| state.txn).collect::<Vec<_>>(),
        vec![old.txn, new.txn],
        "a failed truncation must not have dropped the staged command below its bound"
    );
}

/// A flush and a truncation racing: whichever order they commit in, no
/// command below the persisted watermark survives (the flush must not
/// re-insert what the truncation deleted).
pub async fn truncate_races_flush<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    for round in 1..=100u64 {
        let below = command(round * 10, 1);
        let bound = timestamp(round * 10 + 5, 0);
        journal.stage(&below).await.unwrap();
        let (flushed, truncated) = tokio::join!(journal.flush(), journal.truncate(bound));
        flushed.unwrap();
        truncated.unwrap();

        let survivors: Vec<_> = journal
            .load_all()
            .await
            .unwrap()
            .into_iter()
            .map(|state| state.txn)
            .filter(|txn| txn.0 < bound)
            .collect();
        assert!(
            survivors.is_empty(),
            "round {round}: {survivors:?} below the watermark {bound:?} after a flush raced a truncate"
        );
    }
}

/// Routes, blind promises and decision records are staged (invisible until
/// the flush), round-trip after it, and keep the last value written per
/// transaction. Routes and promises are truncated with the commands; decision
/// records — flushed or still staged — outlive them until dropped.
pub async fn routes_and_promises<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let old = TxnId(timestamp(100, 1));
    // Exactly the truncation bound below: `txn < before` keeps it.
    let at = TxnId(timestamp(200, 0));
    let new = TxnId(timestamp(300, 2));
    let route = vec![Key("a".into()), Key("b".into())];
    let low = Ballot(timestamp(400, 3));
    let high = Ballot(timestamp(500, 4));

    journal.save_route(old, &route).await.unwrap();
    journal.save_route(at, &route).await.unwrap();
    journal.save_route(new, &route).await.unwrap();
    journal.save_promise(old, low).await.unwrap();
    journal.save_promise(at, low).await.unwrap();
    journal.save_promise(new, low).await.unwrap();
    journal.save_promise(new, high).await.unwrap();
    let decision = |txn: TxnId, conflict: bool| DecisionRecord {
        epoch: 0,
        txn,
        execute_at: timestamp(txn.0.micros + 1, 0),
        conflict,
        route: route.clone(),
    };
    journal.save_decision(&decision(old, false)).await.unwrap();
    journal.save_decision(&decision(old, true)).await.unwrap();
    journal.save_decision(&decision(new, false)).await.unwrap();
    assert!(
        journal.load_routes().await.unwrap().is_empty(),
        "staged only"
    );
    assert!(
        journal.load_decisions().await.unwrap().is_empty(),
        "staged only"
    );
    journal.flush().await.unwrap();

    let mut routes = journal.load_routes().await.unwrap();
    routes.sort_by_key(|(txn, _)| *txn);
    assert_eq!(
        routes,
        vec![
            (old, route.clone()),
            (at, route.clone()),
            (new, route.clone())
        ]
    );
    let mut promises = journal.load_promises().await.unwrap();
    promises.sort_by_key(|(txn, _)| *txn);
    assert_eq!(
        promises,
        vec![(old, low), (at, low), (new, high)],
        "last write wins"
    );

    let decisions = || async {
        let mut records = journal.load_decisions().await.unwrap();
        records.sort_by_key(|record| record.txn);
        records
    };
    assert_eq!(
        decisions().await,
        vec![decision(old, true), decision(new, false)],
        "last write wins"
    );

    // A record staged below the bound survives the truncate too.
    let staged = TxnId(timestamp(150, 5));
    journal
        .save_decision(&decision(staged, false))
        .await
        .unwrap();
    journal.truncate(at.0).await.unwrap();
    journal.flush().await.unwrap();
    let mut routes = journal.load_routes().await.unwrap();
    routes.sort_by_key(|(txn, _)| *txn);
    assert_eq!(
        routes,
        vec![(at, route.clone()), (new, route.clone())],
        "below the bound goes, at and above the bound stay"
    );
    let mut promises = journal.load_promises().await.unwrap();
    promises.sort_by_key(|(txn, _)| *txn);
    assert_eq!(promises, vec![(at, low), (new, high)]);
    assert_eq!(
        decisions().await,
        vec![
            decision(old, true),
            decision(staged, false),
            decision(new, false)
        ]
    );

    journal.drop_decisions(&[old, staged]).await.unwrap();
    assert_eq!(decisions().await, vec![decision(new, false)]);
}

/// The clock bound round-trips, keeps the last value written, and is not
/// touched by truncation (it covers every id the node ever minted).
pub async fn clock_bound<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    assert_eq!(journal.load_clock_bound().await.unwrap(), None);
    journal.save_clock_bound(400).await.unwrap();
    journal.save_clock_bound(900).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    assert_eq!(journal.load_clock_bound().await.unwrap(), Some(900));
}

/// The amnesia fence round-trips, keeps the last value written (a join marks
/// it joined), and is not touched by truncation.
pub async fn amnesia_fence<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), None);
    let pending = AmnesiaFence {
        fence_micros: 500,
        joined: false,
        votes_restored: false,
    };
    journal.save_amnesia_fence(&pending).await.unwrap();
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(pending));
    let joined = AmnesiaFence {
        joined: true,
        ..pending
    };
    journal.save_amnesia_fence(&joined).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(joined));
}

/// The transition fences round-trip, a save replaces the set, and a
/// truncate keeps them.
pub async fn fences<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    assert!(journal.load_fences().await.unwrap().is_empty());
    let first = FenceRecord {
        since: 1,
        cap: timestamp(500, 0),
    };
    journal.save_fences(&[first]).await.unwrap();
    assert_eq!(journal.load_fences().await.unwrap(), vec![first]);
    let second = FenceRecord {
        since: 3,
        cap: timestamp(900, 1),
    };
    journal.save_fences(&[first, second]).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    assert_eq!(journal.load_fences().await.unwrap(), vec![first, second]);
}

/// The vote fence is staged like a command: invisible until a flush, then
/// it round-trips, keeps the last value staged, and a truncate — between a
/// stage and its flush too — never drops it (#337).
pub async fn vote_fence<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    assert_eq!(journal.load_vote_fence().await.unwrap(), None);
    journal.save_vote_fence(timestamp(400, 0)).await.unwrap();
    assert_eq!(
        journal.load_vote_fence().await.unwrap(),
        None,
        "staged, not durable before a flush"
    );
    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_vote_fence().await.unwrap(),
        Some(timestamp(400, 0))
    );
    journal.save_vote_fence(timestamp(900, 1)).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_vote_fence().await.unwrap(),
        Some(timestamp(900, 1)),
        "a truncate between a stage and its flush keeps the fence"
    );
}

/// The copy state and the slot GC epoch are staged like a command: invisible
/// until a flush, then they round-trip, the copy state replaced by the last
/// one staged, and a truncate — between a stage and its flush too — never
/// drops them (#372).
pub async fn copy_state_and_slot_gc<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let map = ShardMap::even(vec![vec![NodeId(0)], vec![NodeId(1)]]);
    let dropped = CopyState {
        since: 3,
        map,
        phase: CopyPhase::Dropped,
    };
    let copied = CopyState {
        phase: CopyPhase::Copied,
        ..dropped.clone()
    };
    assert_eq!(journal.load_copy_state().await.unwrap(), None);
    assert_eq!(journal.load_slot_gc().await.unwrap(), None);
    journal.save_copy_state(&dropped).await.unwrap();
    journal.save_slot_gc(4).await.unwrap();
    assert_eq!(
        journal.load_copy_state().await.unwrap(),
        None,
        "staged, not durable before a flush"
    );
    journal.flush().await.unwrap();
    assert_eq!(journal.load_copy_state().await.unwrap(), Some(dropped));
    assert_eq!(journal.load_slot_gc().await.unwrap(), Some(4));
    journal.save_copy_state(&copied).await.unwrap();
    journal.save_slot_gc(6).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_copy_state().await.unwrap(),
        Some(copied),
        "a truncate between a stage and its flush keeps the copy state"
    );
    assert_eq!(journal.load_slot_gc().await.unwrap(), Some(6));
}

/// The marks a replica saw from its peers are staged like a command:
/// invisible until a flush, then they round-trip, the last set staged
/// replaces the one before, and a truncate — between a stage and its flush
/// too — never drops them (#339). Staged with the vote fence, each keeps its
/// own row.
pub async fn peer_marks<DB>(journal: &SqlJournal<DB>)
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let mark = |life: u64, clock_bound: u64| DurableMark {
        life,
        clock_bound,
        vote_fence: timestamp(clock_bound / 2, 0),
    };
    assert!(journal.load_peer_marks().await.unwrap().is_empty());
    journal
        .save_peer_marks(&[(NodeId(1), mark(0, 400))])
        .await
        .unwrap();
    journal.save_vote_fence(timestamp(200, 0)).await.unwrap();
    assert!(
        journal.load_peer_marks().await.unwrap().is_empty(),
        "staged, not durable before a flush"
    );
    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_peer_marks().await.unwrap(),
        vec![(NodeId(1), mark(0, 400))]
    );
    assert_eq!(
        journal.load_vote_fence().await.unwrap(),
        Some(timestamp(200, 0))
    );
    let kept = vec![(NodeId(1), mark(0, 900)), (NodeId(2), mark(7, 300))];
    journal.save_peer_marks(&kept).await.unwrap();
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    journal.flush().await.unwrap();
    assert_eq!(
        journal.load_peer_marks().await.unwrap(),
        kept,
        "a truncate between a stage and its flush keeps the marks"
    );
    assert_eq!(
        journal.load_vote_fence().await.unwrap(),
        Some(timestamp(200, 0))
    );
}
