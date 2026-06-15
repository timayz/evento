//! Phase B (evento-accord) — genuine durability: the disk-backed `FjallJournal`
//! survives a full close/reopen (a real process restart), not just an in-process
//! rebuild. Gated by the `accord` feature.

#![cfg(feature = "accord")]

use evento_accord::{AcceptorRecord, Ballot, CommandState, Journal, Key, NodeId, Status, Timestamp, TxnId};
use evento_core::Event;
use evento_fjall::FjallJournal;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_survives_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal")
        .tempdir()
        .unwrap();

    let a = command(100, 1);
    let b = command(200, 2);

    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        journal.record(&a).await.unwrap();
        journal.record(&b).await.unwrap();
    }

    let journal = FjallJournal::open(temp.path()).unwrap();

    let mut all = journal.load_all().await.unwrap();
    all.sort_by_key(|c| c.execute_at);
    assert_eq!(all.len(), 2, "both records survived the restart");
    assert_eq!(all[0].txn, a.txn);
    assert_eq!(all[0].events[0].version, 1);
    assert_eq!(all[1].events[0].version, 2);
    assert_eq!(all[1].status, Status::Applied);

    let loaded = journal.load(b.txn).await.unwrap().unwrap();
    assert_eq!(loaded.execute_at, b.execute_at);
    assert_eq!(loaded.applied_conflict, Some(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncation_drops_old_records_and_persists_the_watermark() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_truncate")
        .tempdir()
        .unwrap();

    let old = command(100, 1);
    let mid = command(200, 2);
    let new = command(300, 3);

    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        journal.record(&old).await.unwrap();
        journal.record(&mid).await.unwrap();
        journal.record(&new).await.unwrap();
        assert!(journal.load_watermark().await.unwrap().is_none());

        journal.truncate(timestamp(250, 0)).await.unwrap();
    }

    let journal = FjallJournal::open(temp.path()).unwrap();

    let all = journal.load_all().await.unwrap();
    assert_eq!(all.len(), 1, "only the record above the watermark survives");
    assert_eq!(all[0].txn, new.txn);
    assert!(journal.load(old.txn).await.unwrap().is_none());
    assert!(journal.load(mid.txn).await.unwrap().is_none());

    assert_eq!(
        journal.load_watermark().await.unwrap(),
        Some(timestamp(250, 0)),
        "the truncation watermark survives a restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_log_and_acceptor_state_survive_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_meta")
        .tempdir()
        .unwrap();

    let layout = |nodes: &[u64]| vec![nodes.iter().map(|&n| NodeId(n)).collect::<Vec<_>>()];

    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        journal.append_metadata(2, &layout(&[0, 1, 2, 3])).await.unwrap();
        journal.append_metadata(1, &layout(&[0, 1, 2])).await.unwrap();
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
    }

    let journal = FjallJournal::open(temp.path()).unwrap();

    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 2, "both metadata entries survived the restart");
    assert_eq!(entries[0].0, 1, "entries come back ascending by epoch");
    assert_eq!(entries[1].0, 2);
    assert_eq!(entries[1].1, layout(&[0, 1, 2, 3]));

    let acceptors = journal.load_acceptors().await.unwrap();
    assert_eq!(acceptors.len(), 1);
    assert_eq!(acceptors[0].0, 2);
    assert_eq!(acceptors[0].1.promised, Ballot(timestamp(500, 1)));
    assert!(acceptors[0].1.accepted.is_some());

    assert!(journal.load_watermark().await.unwrap().is_none());
}
