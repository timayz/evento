//! In-memory [`DataStore`] and [`Journal`] for tests and the simulation harness.
//!
//! The data store mirrors evento's optimistic-concurrency rule — a unique
//! constraint on `(aggregate_type, aggregate_id, version)` — by rejecting an
//! append whose version is already taken for that aggregate. Because the node
//! applies committed transactions strictly in execution-timestamp order, two
//! racing same-version appends deterministically resolve to one winner.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use evento_core::Event;

use crate::api::{AcceptorRecord, DataStore, Journal};
use crate::clock::{NodeId, Timestamp, TxnId};
use crate::message::CommandState;

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
    /// Highest version appended per `(aggregate_type, aggregate_id)`.
    versions: HashMap<(String, String), u16>,
    /// The event that committed each `(type, id, version)` — for the simulation's
    /// split-brain oracle (no two distinct events at one version) and to serve the
    /// bootstrap [`snapshot`](DataStore::snapshot).
    committed: HashMap<(String, String, u16), Event>,
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

    /// Every `(type, id, version)` committed here and the id of the event that did
    /// it.
    pub fn committed_events(&self) -> Vec<((String, String, u16), ulid::Ulid)> {
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
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        let state = self.state.lock().expect("store poisoned");
        Ok(state
            .versions
            .get(&(aggregate_type.to_owned(), aggregate_id.to_owned()))
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
            for event in &events {
                let key = (event.aggregate_type.clone(), event.aggregate_id.clone());
                state.versions.insert(key.clone(), event.version);
                state
                    .committed
                    .insert((key.0, key.1, event.version), event.clone());
            }
        }

        state.log.push(AppliedEntry {
            txn,
            execute_at,
            conflict: !commit,
        });

        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        // Every committed event, ordered by (type, id, version) so a consumer that
        // re-applies them sees each aggregate's versions ascending.
        let state = self.state.lock().expect("store poisoned");
        let mut events: Vec<Event> = state.committed.values().cloned().collect();
        events.sort_by(|a, b| {
            (&a.aggregate_type, &a.aggregate_id, a.version).cmp(&(
                &b.aggregate_type,
                &b.aggregate_id,
                b.version,
            ))
        });
        Ok(events)
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
    /// Decided metadata-log entries (epoch → layout). `BTreeMap` so iteration is
    /// ascending and deterministic.
    metadata: Mutex<BTreeMap<u64, Vec<Vec<NodeId>>>>,
    /// Config-Paxos acceptor state per epoch.
    acceptors: Mutex<HashMap<u64, AcceptorRecord>>,
}

impl InMemoryJournal {
    /// Creates an empty journal.
    pub fn new() -> Self {
        Self::default()
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
        *self.watermark.lock().expect("journal poisoned") = Some(before);
        Ok(())
    }

    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        Ok(*self.watermark.lock().expect("journal poisoned"))
    }

    async fn append_metadata(&self, epoch: u64, layout: &[Vec<NodeId>]) -> anyhow::Result<()> {
        // Idempotent: the first decided layout for an epoch wins (Paxos guarantees it
        // is the chosen value); a re-commit never overwrites it.
        self.metadata
            .lock()
            .expect("journal poisoned")
            .entry(epoch)
            .or_insert_with(|| layout.to_vec());
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, Vec<Vec<NodeId>>)>> {
        Ok(self
            .metadata
            .lock()
            .expect("journal poisoned")
            .iter()
            .map(|(&epoch, layout)| (epoch, layout.clone()))
            .collect())
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
    async fn in_memory_metadata_and_acceptors_round_trip_ascending() {
        let journal = InMemoryJournal::new();
        let layout = |n: &[u64]| vec![n.iter().map(|&i| NodeId(i)).collect::<Vec<_>>()];

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
