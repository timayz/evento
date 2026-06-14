//! Phase B — genuine durability: the disk-backed `FjallJournal` survives a full
//! close/reopen (a real process restart), not just an in-process rebuild.

use evento_accord::{
    Ballot, CommandState, FjallJournal, Journal, Key, NodeId, Status, Timestamp, TxnId,
};
use evento_core::Event;

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
            aggregator_type: "test/Account".into(),
            aggregator_id: "acc".into(),
            version,
            name: "Bumped".into(),
            ..Default::default()
        }],
        reply_to: NodeId(0),
        decision: Some(true),
        applied_conflict: Some(false),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_survives_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal")
        .tempdir()
        .unwrap();

    let a = command(100, 1);
    let b = command(200, 2);

    // Open, record, then drop (close the database on disk).
    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        journal.record(&a).await.unwrap();
        journal.record(&b).await.unwrap();
    }

    // Reopen at the same path — this is a fresh process's view of the disk.
    let journal = FjallJournal::open(temp.path()).unwrap();

    let mut all = journal.load_all().await.unwrap();
    all.sort_by_key(|c| c.execute_at);
    assert_eq!(all.len(), 2, "both records survived the restart");
    assert_eq!(all[0].txn, a.txn);
    assert_eq!(all[0].events[0].version, 1);
    assert_eq!(all[1].events[0].version, 2);
    assert_eq!(all[1].status, Status::Applied);

    // Point lookup also works after reopening.
    let loaded = journal.load(b.txn).await.unwrap().unwrap();
    assert_eq!(loaded.execute_at, b.execute_at);
    assert_eq!(loaded.applied_conflict, Some(false));
}
