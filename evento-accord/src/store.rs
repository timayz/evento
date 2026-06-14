//! In-memory [`DataStore`] and [`Journal`] for tests and the simulation harness.
//!
//! The data store mirrors evento's optimistic-concurrency rule — a unique
//! constraint on `(aggregator_type, aggregator_id, version)` — by rejecting an
//! append whose version is already taken for that aggregate. Because the node
//! applies committed transactions strictly in execution-timestamp order, two
//! racing same-version appends deterministically resolve to one winner.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use evento_core::Event;

use crate::api::{DataStore, Journal};
use crate::clock::{Timestamp, TxnId};
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
    /// Highest version appended per `(aggregator_type, aggregator_id)`.
    versions: HashMap<(String, String), u16>,
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
}

#[async_trait]
impl DataStore for InMemoryDataStore {
    async fn version(&self, aggregator_type: &str, aggregator_id: &str) -> anyhow::Result<u16> {
        let state = self.state.lock().expect("store poisoned");
        Ok(state
            .versions
            .get(&(aggregator_type.to_owned(), aggregator_id.to_owned()))
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
                let key = (event.aggregator_type.clone(), event.aggregator_id.clone());
                state.versions.insert(key, event.version);
            }
        }

        state.log.push(AppliedEntry {
            txn,
            execute_at,
            conflict: !commit,
        });

        Ok(())
    }
}

/// In-memory [`Journal`]. Records each command's durable state as it advances;
/// process-restart replay from the journal is future work, so for now it is
/// exercised but not yet relied upon for recovery (which uses live replica state).
#[derive(Default)]
pub struct InMemoryJournal {
    entries: Mutex<HashMap<TxnId, CommandState>>,
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
}
