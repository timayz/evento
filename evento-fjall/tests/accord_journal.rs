//! Phase B (evento-accord) — genuine durability: the disk-backed `FjallJournal`
//! survives a full close/reopen (a real process restart), not just an in-process
//! rebuild. Gated by the `accord` feature.

#![cfg(feature = "accord")]

use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, DecisionRecord, Journal, Key, NodeId,
    Status, Timestamp, TxnId,
};
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
        journal
            .append_metadata(2, &layout(&[0, 1, 2, 3]))
            .await
            .unwrap();
        journal
            .append_metadata(1, &layout(&[0, 1, 2]))
            .await
            .unwrap();
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
    assert_eq!(
        entries.len(),
        2,
        "both metadata entries survived the restart"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_batch_append_keeps_first_decided_layout() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_meta_batch")
        .tempdir()
        .unwrap();

    let layout = |nodes: &[u64]| vec![nodes.iter().map(|&n| NodeId(n)).collect::<Vec<_>>()];

    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        journal.append_metadata(2, &layout(&[0, 1])).await.unwrap();
        // A batch overlapping an already-durable epoch must not overwrite its
        // decided layout, and must still append the new epochs.
        journal
            .append_metadata_batch(&[
                (1, layout(&[0])),
                (2, layout(&[9, 9, 9])),
                (3, layout(&[0, 1, 2])),
            ])
            .await
            .unwrap();
    }

    let journal = FjallJournal::open(temp.path()).unwrap();

    let entries = journal.load_metadata().await.unwrap();
    assert_eq!(entries.len(), 3, "all epochs survived the restart");
    assert_eq!(entries[0], (1, layout(&[0])));
    assert_eq!(
        entries[1],
        (2, layout(&[0, 1])),
        "the first decided layout for an epoch wins over a batched re-append"
    );
    assert_eq!(entries[2], (3, layout(&[0, 1, 2])));
}

#[tokio::test]
async fn rejoin_bounds_survive_close_and_reopen() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_bounds")
        .tempdir()
        .unwrap();
    let bounds = vec![
        (evento_accord::NodeId(2), timestamp(300, 1)),
        (evento_accord::NodeId(4), timestamp(120, 0)),
    ];
    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        assert!(journal.load_rejoin_bounds().await.unwrap().is_empty());
        journal.save_rejoin_bounds(&bounds).await.unwrap();
    }
    let journal = FjallJournal::open(temp.path()).unwrap();
    assert_eq!(journal.load_rejoin_bounds().await.unwrap(), bounds);
    journal.save_rejoin_bounds(&[]).await.unwrap();
    assert!(journal.load_rejoin_bounds().await.unwrap().is_empty());
}

#[tokio::test]
async fn routes_and_promises_survive_close_and_reopen_and_truncate() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_routes")
        .tempdir()
        .unwrap();
    let old = TxnId(timestamp(100, 1));
    let new = TxnId(timestamp(300, 2));
    let route = vec![Key("a".into()), Key("b".into())];
    let ballot = Ballot(timestamp(400, 3));
    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        assert!(journal.load_routes().await.unwrap().is_empty());
        assert!(journal.load_promises().await.unwrap().is_empty());
        // Staged, then made durable by the group-commit flush.
        journal.save_route(old, &route).await.unwrap();
        journal.save_route(new, &route).await.unwrap();
        journal.save_promise(old, ballot).await.unwrap();
        journal.save_promise(new, ballot).await.unwrap();
        journal.save_decision(&decision(old, &route)).await.unwrap();
        journal.save_decision(&decision(new, &route)).await.unwrap();
        journal.flush().await.unwrap();
    }
    let journal = FjallJournal::open(temp.path()).unwrap();
    let mut routes = journal.load_routes().await.unwrap();
    routes.sort_by_key(|(txn, _)| *txn);
    assert_eq!(routes, vec![(old, route.clone()), (new, route.clone())]);
    let mut promises = journal.load_promises().await.unwrap();
    promises.sort_by_key(|(txn, _)| *txn);
    assert_eq!(promises, vec![(old, ballot), (new, ballot)]);

    let decisions = || async { sorted(journal.load_decisions().await.unwrap()) };
    assert_eq!(
        decisions().await,
        vec![decision(old, &route), decision(new, &route)]
    );

    // Truncated with the commands — but decision records outlive them,
    // until dropped.
    journal.truncate(timestamp(200, 0)).await.unwrap();
    assert_eq!(
        journal.load_routes().await.unwrap(),
        vec![(new, route.clone())]
    );
    assert_eq!(journal.load_promises().await.unwrap(), vec![(new, ballot)]);
    assert_eq!(
        decisions().await,
        vec![decision(old, &route), decision(new, &route)]
    );
    journal.drop_decisions(&[old]).await.unwrap();
    journal.flush().await.unwrap();
    drop(journal);
    let journal = FjallJournal::open(temp.path()).unwrap();
    assert_eq!(
        journal.load_decisions().await.unwrap(),
        vec![decision(new, &route)]
    );
}

fn decision(txn: TxnId, route: &[Key]) -> DecisionRecord {
    DecisionRecord {
        epoch: 0,
        txn,
        execute_at: timestamp(txn.0.micros + 1, 0),
        conflict: txn.0.micros % 200 == 100,
        route: route.to_vec(),
    }
}

fn sorted(mut records: Vec<DecisionRecord>) -> Vec<DecisionRecord> {
    records.sort_by_key(|record| record.txn);
    records
}

#[tokio::test]
async fn clock_bound_survives_close_reopen_and_truncate() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_clock")
        .tempdir()
        .unwrap();
    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        assert_eq!(journal.load_clock_bound().await.unwrap(), None);
        journal.save_clock_bound(400).await.unwrap();
        journal.save_clock_bound(900).await.unwrap();
    }
    let journal = FjallJournal::open(temp.path()).unwrap();
    assert_eq!(journal.load_clock_bound().await.unwrap(), Some(900));
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    assert_eq!(journal.load_clock_bound().await.unwrap(), Some(900));
}

#[tokio::test]
async fn amnesia_fence_survives_close_reopen_and_truncate() {
    let temp = tempfile::Builder::new()
        .prefix("evento_accord_journal_amnesia")
        .tempdir()
        .unwrap();
    let pending = AmnesiaFence {
        fence_micros: 500,
        joined: false,
    };
    let joined = AmnesiaFence {
        joined: true,
        ..pending
    };
    {
        let journal = FjallJournal::open(temp.path()).unwrap();
        assert_eq!(journal.load_amnesia_fence().await.unwrap(), None);
        journal.save_amnesia_fence(&pending).await.unwrap();
        assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(pending));
        journal.save_amnesia_fence(&joined).await.unwrap();
    }
    let journal = FjallJournal::open(temp.path()).unwrap();
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(joined));
    journal.truncate(timestamp(1_000, 0)).await.unwrap();
    assert_eq!(journal.load_amnesia_fence().await.unwrap(), Some(joined));
}
