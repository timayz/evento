//! In-memory [`DataStore`] and [`Journal`] for tests and the simulation harness.
//!
//! The data store mirrors evento's optimistic-concurrency rule — a unique
//! constraint on `(tenant, aggregate_type, aggregate_id, version)` — by
//! rejecting an append whose version is already taken for that stream. Because the node
//! applies committed transactions strictly in execution-timestamp order, two
//! racing same-version appends deterministically resolve to one winner.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use evento_core::Event;

use crate::api::{
    same_event, slot_of, AcceptorRecord, AmnesiaFence, ConfigEntry, CopyState, DataStore,
    DecisionRecord, DurableMark, FenceRecord, Journal, SLOTS,
};
use crate::clock::{Ballot, NodeId, Timestamp, TxnId};
use crate::message::{CommandState, Key};

/// One applied transaction, recorded so tests can assert that every replica
/// produced the same global serial order and the same conflict outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEntry {
    /// The transaction that executed.
    pub txn: TxnId,
    /// Its execution timestamp (the position in the global order).
    pub execute_at: Timestamp,
    /// Whether the optimistic-version condition rejected the append.
    pub conflict: bool,
}

#[derive(Default)]
struct StoreState {
    /// Highest version appended per `(tenant, aggregate_type, aggregate_id)`.
    versions: HashMap<(String, String, String), u16>,
    /// The event that committed each `(tenant, type, id, version)` — for the
    /// simulation's split-brain oracle (no two distinct events at one version)
    /// and to serve the bootstrap [`snapshot`](DataStore::snapshot).
    committed: HashMap<(String, String, String, u16), Event>,
    /// Applied transactions, in the order this replica executed them.
    log: Vec<AppliedEntry>,
}

/// In-memory applied event store for one replica.
#[derive(Default)]
pub struct InMemoryDataStore {
    state: Mutex<StoreState>,
}

impl InMemoryDataStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The transactions this replica has applied, in execution order. Used by
    /// tests to compare the global serial order across replicas.
    pub fn applied_log(&self) -> Vec<AppliedEntry> {
        self.state.lock().expect("store poisoned").log.clone()
    }

    /// Every `(tenant, type, id, version)` committed here and the id of the
    /// event that did it.
    pub fn committed_events(&self) -> Vec<((String, String, String, u16), ulid::Ulid)> {
        self.state
            .lock()
            .expect("store poisoned")
            .committed
            .iter()
            .map(|(k, v)| (k.clone(), v.id))
            .collect()
    }
}

#[async_trait]
impl DataStore for InMemoryDataStore {
    async fn version(
        &self,
        tenant: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<u16> {
        let state = self.state.lock().expect("store poisoned");
        Ok(state
            .versions
            .get(&(
                tenant.to_owned(),
                aggregate_type.to_owned(),
                aggregate_id.to_owned(),
            ))
            .copied()
            .unwrap_or(0))
    }

    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("store poisoned");

        // The commit decision was already made by the coordinator (across all
        // shards) during the Read phase; here we only enact it. On commit we
        // append, advancing each aggregate's version; on abort we record the
        // no-op so the global order is still observable.
        if commit {
            // As a store keyed by version would: an event already in place
            // (a restart replaying its journal over a store that kept it) is
            // a no-op, a version never goes back down, and another event at
            // a taken version is refused — before anything is applied.
            for event in &events {
                let at = (
                    event.tenant.clone(),
                    event.aggregate_type.clone(),
                    event.aggregate_id.clone(),
                    event.version,
                );
                if let Some(existing) = state.committed.get(&at) {
                    anyhow::ensure!(
                        existing.id == event.id,
                        "version {} of {}/{} already holds event {}, not {}",
                        event.version,
                        event.aggregate_type,
                        event.aggregate_id,
                        existing.id,
                        event.id
                    );
                }
            }
            for event in &events {
                let key = (
                    event.tenant.clone(),
                    event.aggregate_type.clone(),
                    event.aggregate_id.clone(),
                );
                let version = state.versions.entry(key.clone()).or_default();
                *version = (*version).max(event.version);
                state
                    .committed
                    .insert((key.0, key.1, key.2, event.version), event.clone());
            }
        }

        state.log.push(AppliedEntry {
            txn,
            execute_at,
            conflict: !commit,
        });

        Ok(())
    }

    async fn holds(&self, events: &[Event]) -> anyhow::Result<bool> {
        let state = self.state.lock().expect("store poisoned");
        Ok(events.iter().all(|event| {
            state
                .committed
                .get(&(
                    event.tenant.clone(),
                    event.aggregate_type.clone(),
                    event.aggregate_id.clone(),
                    event.version,
                ))
                .is_some_and(|stored| same_event(stored, event))
        }))
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        // Every committed event, ordered by (tenant, type, id, version) so a
        // consumer that re-applies them sees each stream's versions ascending.
        let state = self.state.lock().expect("store poisoned");
        let mut events: Vec<Event> = state.committed.values().cloned().collect();
        events.sort_by(|a, b| {
            (&a.tenant, &a.aggregate_type, &a.aggregate_id, a.version).cmp(&(
                &b.tenant,
                &b.aggregate_type,
                &b.aggregate_id,
                b.version,
            ))
        });
        Ok(events)
    }

    async fn drop_slots(&self, slots: &[u16]) -> anyhow::Result<usize> {
        let mut wanted = vec![false; SLOTS];
        for &slot in slots {
            if let Some(wanted) = wanted.get_mut(usize::from(slot)) {
                *wanted = true;
            }
        }
        let mut state = self.state.lock().expect("store poisoned");
        let mut dropped: HashSet<(String, String, String)> = HashSet::new();
        state
            .committed
            .retain(|(tenant, aggregate_type, aggregate_id, _), event| {
                if wanted[slot_of(&Key::of(event))] {
                    dropped.insert((tenant.clone(), aggregate_type.clone(), aggregate_id.clone()));
                    false
                } else {
                    true
                }
            });
        state.versions.retain(|stream, _| !dropped.contains(stream));
        Ok(dropped.len())
    }
}

/// In-memory [`Journal`]. Records each command's durable state as it advances;
/// process-restart replay from the journal is future work, so for now it is
/// exercised but not yet relied upon for recovery (which uses live replica state).
#[derive(Default)]
pub struct InMemoryJournal {
    entries: Mutex<HashMap<TxnId, CommandState>>,
    /// The persisted truncation watermark (redundancy floor).
    watermark: Mutex<Option<Timestamp>>,
    /// The persisted rejoin bounds (see [`Journal::save_rejoin_bounds`]).
    rejoin_bounds: Mutex<Vec<(NodeId, Timestamp)>>,
    /// Decided metadata-log entries (epoch → entry). `BTreeMap` so iteration
    /// is ascending and deterministic.
    metadata: Mutex<BTreeMap<u64, ConfigEntry>>,
    /// Config-Paxos acceptor state per epoch.
    acceptors: Mutex<HashMap<u64, AcceptorRecord>>,
    /// Multi-shard routes (see [`Journal::save_route`]).
    routes: Mutex<HashMap<TxnId, Vec<Key>>>,
    /// Blind recovery promises (see [`Journal::save_promise`]).
    promises: Mutex<HashMap<TxnId, Ballot>>,
    /// Decision records of compacted multi-shard transactions (see
    /// [`Journal::save_decision`]) — kept across `truncate`.
    decisions: Mutex<BTreeMap<TxnId, DecisionRecord>>,
    /// The persisted clock bound (see [`Journal::save_clock_bound`]).
    clock_bound: Mutex<Option<u64>>,
    /// The persisted vote fence (see [`Journal::save_vote_fence`]) — kept
    /// across `truncate`.
    vote_fence: Mutex<Option<Timestamp>>,
    /// The persisted amnesia fence (see [`Journal::save_amnesia_fence`]) —
    /// kept across `truncate`.
    amnesia_fence: Mutex<Option<AmnesiaFence>>,
    /// The persisted transition fences (see [`Journal::save_fences`]) — kept
    /// across `truncate`.
    fences: Mutex<Vec<FenceRecord>>,
    /// The persisted peer marks (see [`Journal::save_peer_marks`]) — kept
    /// across `truncate`.
    peer_marks: Mutex<Vec<(NodeId, DurableMark)>>,
    /// The persisted copy state (see [`Journal::save_copy_state`]) — kept
    /// across `truncate`.
    copy_state: Mutex<Option<CopyState>>,
    /// The persisted slot GC epoch (see [`Journal::save_slot_gc`]) — kept
    /// across `truncate`.
    slot_gc: Mutex<Option<u64>>,
}

impl InMemoryJournal {
    /// Creates an empty journal.
    pub fn new() -> Self {
        Self::default()
    }

    /// A copy of everything this journal holds, as a backup taken now would
    /// hold it: reopening a node over it later is reopening it over an older
    /// copy of its journal (#339).
    pub fn snapshot(&self) -> Self {
        fn copy<T: Clone>(kept: &Mutex<T>) -> Mutex<T> {
            Mutex::new(kept.lock().expect("journal poisoned").clone())
        }
        Self {
            entries: copy(&self.entries),
            watermark: copy(&self.watermark),
            rejoin_bounds: copy(&self.rejoin_bounds),
            metadata: copy(&self.metadata),
            acceptors: copy(&self.acceptors),
            routes: copy(&self.routes),
            promises: copy(&self.promises),
            decisions: copy(&self.decisions),
            clock_bound: copy(&self.clock_bound),
            vote_fence: copy(&self.vote_fence),
            amnesia_fence: copy(&self.amnesia_fence),
            fences: copy(&self.fences),
            peer_marks: copy(&self.peer_marks),
            copy_state: copy(&self.copy_state),
            slot_gc: copy(&self.slot_gc),
        }
    }
}

#[async_trait]
impl Journal for InMemoryJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.entries
            .lock()
            .expect("journal poisoned")
            .insert(state.txn, state.clone());
        Ok(())
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        Ok(self
            .entries
            .lock()
            .expect("journal poisoned")
            .get(&txn)
            .cloned())
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        Ok(self
            .entries
            .lock()
            .expect("journal poisoned")
            .values()
            .cloned()
            .collect())
    }

    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        self.entries
            .lock()
            .expect("journal poisoned")
            .retain(|txn, _| txn.0 >= before);
        self.routes
            .lock()
            .expect("journal poisoned")
            .retain(|txn, _| txn.0 >= before);
        self.promises
            .lock()
            .expect("journal poisoned")
            .retain(|txn, _| txn.0 >= before);
        *self.watermark.lock().expect("journal poisoned") = Some(before);
        Ok(())
    }

    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        Ok(*self.watermark.lock().expect("journal poisoned"))
    }

    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        *self.rejoin_bounds.lock().expect("journal poisoned") = bounds.to_vec();
        Ok(())
    }

    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        Ok(self.rejoin_bounds.lock().expect("journal poisoned").clone())
    }

    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        *self.clock_bound.lock().expect("journal poisoned") = Some(micros);
        Ok(())
    }

    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        Ok(*self.clock_bound.lock().expect("journal poisoned"))
    }

    async fn save_vote_fence(&self, fence: Timestamp) -> anyhow::Result<()> {
        let mut kept = self.vote_fence.lock().expect("journal poisoned");
        *kept = Some(kept.map_or(fence, |kept| kept.max(fence)));
        Ok(())
    }

    async fn load_vote_fence(&self) -> anyhow::Result<Option<Timestamp>> {
        Ok(*self.vote_fence.lock().expect("journal poisoned"))
    }

    async fn save_peer_marks(&self, marks: &[(NodeId, DurableMark)]) -> anyhow::Result<()> {
        *self.peer_marks.lock().expect("journal poisoned") = marks.to_vec();
        Ok(())
    }

    async fn load_peer_marks(&self) -> anyhow::Result<Vec<(NodeId, DurableMark)>> {
        Ok(self.peer_marks.lock().expect("journal poisoned").clone())
    }

    async fn save_copy_state(&self, state: &CopyState) -> anyhow::Result<()> {
        *self.copy_state.lock().expect("journal poisoned") = Some(state.clone());
        Ok(())
    }

    async fn load_copy_state(&self) -> anyhow::Result<Option<CopyState>> {
        Ok(self.copy_state.lock().expect("journal poisoned").clone())
    }

    async fn save_slot_gc(&self, epoch: u64) -> anyhow::Result<()> {
        let mut kept = self.slot_gc.lock().expect("journal poisoned");
        *kept = Some(kept.map_or(epoch, |kept| kept.max(epoch)));
        Ok(())
    }

    async fn load_slot_gc(&self) -> anyhow::Result<Option<u64>> {
        Ok(*self.slot_gc.lock().expect("journal poisoned"))
    }

    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        *self.amnesia_fence.lock().expect("journal poisoned") = Some(*fence);
        Ok(())
    }

    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
        Ok(*self.amnesia_fence.lock().expect("journal poisoned"))
    }

    async fn save_route(&self, txn: TxnId, route: &[Key]) -> anyhow::Result<()> {
        self.routes
            .lock()
            .expect("journal poisoned")
            .insert(txn, route.to_vec());
        Ok(())
    }

    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        Ok(self
            .routes
            .lock()
            .expect("journal poisoned")
            .iter()
            .map(|(&txn, route)| (txn, route.clone()))
            .collect())
    }

    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        let mut promises = self.promises.lock().expect("journal poisoned");
        let promised = promises.entry(txn).or_insert(ballot);
        *promised = (*promised).max(ballot);
        Ok(())
    }

    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        Ok(self
            .promises
            .lock()
            .expect("journal poisoned")
            .iter()
            .map(|(&txn, &ballot)| (txn, ballot))
            .collect())
    }

    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        self.decisions
            .lock()
            .expect("journal poisoned")
            .insert(record.txn, record.clone());
        Ok(())
    }

    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        Ok(self
            .decisions
            .lock()
            .expect("journal poisoned")
            .values()
            .cloned()
            .collect())
    }

    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        let mut decisions = self.decisions.lock().expect("journal poisoned");
        for txn in txns {
            decisions.remove(txn);
        }
        Ok(())
    }

    async fn append_metadata(&self, epoch: u64, entry: &ConfigEntry) -> anyhow::Result<()> {
        // Idempotent: the first decided entry for an epoch wins (Paxos guarantees it
        // is the chosen value); a re-commit never overwrites it.
        self.metadata
            .lock()
            .expect("journal poisoned")
            .entry(epoch)
            .or_insert_with(|| entry.clone());
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
        Ok(self
            .metadata
            .lock()
            .expect("journal poisoned")
            .iter()
            .map(|(&epoch, entry)| (epoch, entry.clone()))
            .collect())
    }

    async fn save_fences(&self, fences: &[FenceRecord]) -> anyhow::Result<()> {
        *self.fences.lock().expect("journal poisoned") = fences.to_vec();
        Ok(())
    }

    async fn load_fences(&self) -> anyhow::Result<Vec<FenceRecord>> {
        Ok(self.fences.lock().expect("journal poisoned").clone())
    }

    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        self.acceptors
            .lock()
            .expect("journal poisoned")
            .insert(epoch, state.clone());
        Ok(())
    }

    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        Ok(self
            .acceptors
            .lock()
            .expect("journal poisoned")
            .iter()
            .map(|(&epoch, state)| (epoch, state.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Ballot, NodeId};
    use crate::message::Status;

    fn command(micros: u64) -> CommandState {
        let txn = TxnId(Timestamp {
            micros,
            logical: 0,
            node: NodeId(0),
        });
        CommandState {
            epoch: 0,
            txn,
            status: Status::Applied,
            promised: Ballot(txn.0),
            accepted: Ballot(txn.0),
            execute_at: txn.0,
            deps: vec![],
            keys: vec![],
            events: vec![],
            reply_to: NodeId(0),
            decision: Some(true),
            applied_conflict: Some(false),
        }
    }

    /// `drop_slots` removes the streams in the named slots, their versions
    /// with them, and nothing else; dropping again removes nothing.
    #[tokio::test]
    async fn drop_slots_removes_only_the_named_slots_streams() {
        use crate::api::slot_of;
        use evento_core::Event;

        let store = InMemoryDataStore::new();
        let event = |id: &str, version: u16| Event {
            id: ulid::Ulid::generate(),
            aggregate_type: "test/Account".into(),
            aggregate_id: id.into(),
            version,
            name: "Bumped".into(),
            ..Default::default()
        };
        // Two aggregates in different slots.
        let a = "a".to_string();
        let b = (0..10_000)
            .map(|i| format!("b{i}"))
            .find(|b| slot_of(&Key(b.clone())) != slot_of(&Key(a.clone())))
            .unwrap();
        let txn = TxnId(Timestamp {
            micros: 1,
            logical: 0,
            node: NodeId(0),
        });
        store
            .apply(
                txn,
                txn.0,
                vec![event(&a, 1), event(&a, 2), event(&b, 1)],
                true,
            )
            .await
            .unwrap();
        assert_eq!(store.version("", "test/Account", &a).await.unwrap(), 2);

        let slot = slot_of(&Key(a.clone())) as u16;
        assert_eq!(store.drop_slots(&[slot]).await.unwrap(), 1);
        assert_eq!(store.version("", "test/Account", &a).await.unwrap(), 0);
        assert_eq!(store.version("", "test/Account", &b).await.unwrap(), 1);
        assert_eq!(store.snapshot().await.unwrap().len(), 1);
        assert_eq!(store.drop_slots(&[slot]).await.unwrap(), 0);
        // The stream starts over from its first version.
        store
            .apply(txn, txn.0, vec![event(&a, 1)], true)
            .await
            .unwrap();
        assert_eq!(store.version("", "test/Account", &a).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn in_memory_truncate_drops_below_and_records_the_watermark() {
        let journal = InMemoryJournal::new();
        for micros in [100, 200, 300] {
            journal.record(&command(micros)).await.unwrap();
        }
        assert!(journal.load_watermark().await.unwrap().is_none());

        let before = Timestamp {
            micros: 250,
            logical: 0,
            node: NodeId(0),
        };
        journal.truncate(before).await.unwrap();

        let remaining = journal.load_all().await.unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "only the record above the watermark stays"
        );
        assert_eq!(remaining[0].txn.0.micros, 300);
        assert_eq!(journal.load_watermark().await.unwrap(), Some(before));
    }

    #[tokio::test]
    async fn in_memory_clock_bound_round_trips_and_survives_truncate() {
        let journal = InMemoryJournal::new();
        assert_eq!(journal.load_clock_bound().await.unwrap(), None);
        journal.save_clock_bound(400).await.unwrap();
        journal.save_clock_bound(900).await.unwrap();
        journal
            .truncate(Timestamp {
                micros: 1_000,
                logical: 0,
                node: NodeId(0),
            })
            .await
            .unwrap();
        assert_eq!(journal.load_clock_bound().await.unwrap(), Some(900));
    }

    /// The peer marks round-trip, the last set saved replaces the one
    /// before, and a truncate keeps them; a snapshot holds what the journal
    /// held when taken, and nothing saved after (#339).
    #[tokio::test]
    async fn in_memory_peer_marks_and_snapshots() {
        let journal = InMemoryJournal::new();
        assert!(journal.load_peer_marks().await.unwrap().is_empty());
        let mark = |clock_bound| DurableMark {
            life: 0,
            clock_bound,
            vote_fence: Timestamp::MIN,
            born: 0,
        };
        journal
            .save_peer_marks(&[(NodeId(1), mark(400))])
            .await
            .unwrap();
        journal.save_clock_bound(400).await.unwrap();
        journal.record(&command(10)).await.unwrap();
        let backup = journal.snapshot();

        let kept = vec![(NodeId(1), mark(900)), (NodeId(2), mark(300))];
        journal.save_peer_marks(&kept).await.unwrap();
        journal.save_clock_bound(900).await.unwrap();
        journal.record(&command(20)).await.unwrap();
        journal
            .truncate(Timestamp {
                micros: 1,
                logical: 0,
                node: NodeId(0),
            })
            .await
            .unwrap();
        assert_eq!(journal.load_peer_marks().await.unwrap(), kept);

        assert_eq!(
            backup.load_peer_marks().await.unwrap(),
            vec![(NodeId(1), mark(400))]
        );
        assert_eq!(backup.load_clock_bound().await.unwrap(), Some(400));
        assert_eq!(backup.load_all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn in_memory_metadata_and_acceptors_round_trip_ascending() {
        let journal = InMemoryJournal::new();
        let layout =
            |n: &[u64]| ConfigEntry::active(vec![n.iter().map(|&i| NodeId(i)).collect::<Vec<_>>()]);

        // Append out of order; load_metadata must return ascending by epoch.
        journal
            .append_metadata(3, &layout(&[1, 2, 3]))
            .await
            .unwrap();
        journal
            .append_metadata(1, &layout(&[0, 1, 2]))
            .await
            .unwrap();
        let entries = journal.load_metadata().await.unwrap();
        assert_eq!(
            entries.iter().map(|(e, _)| *e).collect::<Vec<_>>(),
            vec![1, 3],
            "metadata entries come back ascending"
        );

        let rec = AcceptorRecord {
            promised: Ballot(Timestamp {
                micros: 7,
                logical: 0,
                node: NodeId(1),
            }),
            accepted: None,
        };
        journal.record_acceptor(1, &rec).await.unwrap();
        let accs = journal.load_acceptors().await.unwrap();
        assert_eq!(accs, vec![(1, rec)]);
    }
}
