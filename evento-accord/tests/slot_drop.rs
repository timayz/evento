//! Dropping slots from the production data store (`ExecutorDataStore`),
//! over the simulator's in-memory evento executor: the store has no index by
//! slot, so a drop scans it once and deletes each stream it finds in the
//! slots (#372).

mod common;

use common::executor::SimExecutor;
use evento_accord::{slot_of, DataStore, ExecutorDataStore, Key, NodeId, Timestamp, TxnId};
use evento_core::Event;

fn event(id: &str, version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: id.into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

/// `drop_slots` deletes every stream whose key is in the slots, leaves the
/// others, and lets a dropped stream start over from its first version.
#[tokio::test]
async fn an_executor_store_drops_the_streams_in_the_slots() {
    let executor = SimExecutor::default();
    let store = ExecutorDataStore::new(executor.clone());
    let txn = TxnId(Timestamp {
        micros: 1,
        logical: 0,
        node: NodeId(0),
    });
    let a = "a".to_string();
    let b = (0..10_000)
        .map(|i| format!("b{i}"))
        .find(|b| slot_of(&Key(b.clone())) != slot_of(&Key(a.clone())))
        .unwrap();
    // A second stream in `a`'s slot: a drop takes every stream of the slot.
    let c = (0..100_000)
        .map(|i| format!("c{i}"))
        .find(|c| slot_of(&Key(c.clone())) == slot_of(&Key(a.clone())))
        .unwrap();
    store
        .apply(
            txn,
            txn.0,
            vec![event(&a, 1), event(&a, 2), event(&b, 1), event(&c, 1)],
            true,
        )
        .await
        .unwrap();

    let slot = slot_of(&Key(a.clone())) as u16;
    assert_eq!(store.drop_slots(&[slot]).await.unwrap(), 2);
    assert_eq!(executor.version_of("test/Account", &a), 0);
    assert_eq!(executor.version_of("test/Account", &c), 0);
    assert_eq!(executor.version_of("test/Account", &b), 1);
    assert_eq!(store.drop_slots(&[slot]).await.unwrap(), 0, "idempotent");

    store
        .apply(txn, txn.0, vec![event(&a, 1)], true)
        .await
        .expect("the stream starts over");
    assert_eq!(executor.version_of("test/Account", &a), 1);
}
