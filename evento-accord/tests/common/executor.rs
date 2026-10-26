//! A deterministic in-memory evento executor, so the simulation runs the real
//! [`ExecutorDataStore`](evento_accord::ExecutorDataStore) — the store every
//! production node uses — rather than only the test-only `InMemoryDataStore`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use evento_core::cursor::{Args, ReadResult, Reader, Value};
use evento_core::{Event, EventFilter, Executor, RoutingKey, TenantFilter, WriteError};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ulid::Ulid;

/// The events of a [`SimExecutor`], and how many of them are persisted.
#[derive(Default)]
struct Log {
    /// Every event written, in write order.
    events: Vec<Event>,
    /// How many of `events` are persisted: a [crash](SimExecutor::crash)
    /// keeps only these.
    persisted: usize,
    /// Each stream's latest version, by `(tenant, type, id)`.
    versions: HashMap<(String, String, String), u16>,
    /// When a write persists everything so far: a seeded generator and the
    /// probability per write. `None` persists every write at once.
    persist: Option<(StdRng, f64)>,
}

/// The stream `event` belongs to: `(tenant, type, id)`.
fn stream_of(event: &Event) -> (String, String, String) {
    (
        event.tenant.clone(),
        event.aggregate_type.clone(),
        event.aggregate_id.clone(),
    )
}

impl Log {
    fn rebuild_versions(&mut self) {
        self.versions.clear();
        for event in &self.events {
            let key = stream_of(event);
            let version = self.versions.entry(key).or_default();
            *version = (*version).max(event.version);
        }
    }
}

/// An in-memory [`Executor`] holding one log of events, with the version rule
/// of the fjall backend: an event must be its aggregate's version + 1, or the
/// whole write fails with [`WriteError::InvalidOriginalVersion`] (so it
/// reports a duplicate *and* a gap alike).
///
/// Writes are durable only once **persisted**, at seeded points (see
/// [`set_persistence`](Self::set_persistence)); a [crash](Self::crash) drops
/// the rest, leaving the store **behind** its journal — what a backend that
/// buffers its writes can do on a hard kill, and the case #309 suspects. Reads
/// see every write, as a page cache would.
///
/// Clones share one log: the node writes through one (inside an
/// `ExecutorDataStore`) while the simulation's oracles read another.
#[derive(Clone, Default)]
pub struct SimExecutor {
    log: Arc<Mutex<Log>>,
}

impl SimExecutor {
    /// From now on a write persists everything written so far with
    /// probability `p`, drawn from `seed` (the rest waits for a later write).
    pub fn set_persistence(&self, seed: u64, p: f64) {
        self.log.lock().unwrap().persist = Some((StdRng::seed_from_u64(seed), p));
    }

    /// A crash: every write not yet persisted is gone.
    pub fn crash(&self) {
        let mut log = self.log.lock().unwrap();
        let persisted = log.persisted;
        log.events.truncate(persisted);
        log.rebuild_versions();
    }

    /// Every stored event's `(tenant, type, id, version)` and id — what the
    /// simulation's oracles compare across replicas.
    pub fn committed_events(&self) -> Vec<((String, String, String, u16), Ulid)> {
        self.log
            .lock()
            .unwrap()
            .events
            .iter()
            .map(|e| {
                (
                    (
                        e.tenant.clone(),
                        e.aggregate_type.clone(),
                        e.aggregate_id.clone(),
                        e.version,
                    ),
                    e.id,
                )
            })
            .collect()
    }

    /// `aggregate`'s latest version in the default tenant, 0 if it has none.
    pub fn version_of(&self, aggregate_type: &str, aggregate_id: &str) -> u16 {
        self.version_in("", aggregate_type, aggregate_id)
    }

    /// The latest version of the stream `(tenant, type, id)`, 0 if it has
    /// none.
    pub fn version_in(&self, tenant: &str, aggregate_type: &str, aggregate_id: &str) -> u16 {
        self.log
            .lock()
            .unwrap()
            .versions
            .get(&(
                tenant.to_owned(),
                aggregate_type.to_owned(),
                aggregate_id.to_owned(),
            ))
            .copied()
            .unwrap_or(0)
    }
}

/// Whether `event` passes the read's filters.
fn matches(
    event: &Event,
    aggregators: &Option<Arc<[EventFilter]>>,
    routing_key: &Option<RoutingKey>,
    to_micros: Option<u64>,
) -> bool {
    let aggregate = aggregators.as_deref().is_none_or(|filters| {
        filters.iter().any(|f| {
            f.aggregate_type == event.aggregate_type
                && f.aggregate_id
                    .as_ref()
                    .is_none_or(|id| *id == event.aggregate_id)
                && f.name.as_ref().is_none_or(|name| *name == event.name)
        })
    });
    let routing = match routing_key {
        Some(RoutingKey::Value(Some(key))) => event.routing_key.as_ref() == Some(key),
        Some(RoutingKey::Value(None)) => event.routing_key.is_none(),
        Some(RoutingKey::All) | None => true,
    };
    let below = to_micros.is_none_or(|bound| event.timestamp_micros() < bound);
    aggregate && routing && below
}

#[async_trait]
impl Executor for SimExecutor {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.replicate(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        let mut log = self.log.lock().unwrap();
        // All or nothing: every event checked before any is kept.
        let mut next: HashMap<(String, String, String), u16> = HashMap::new();
        for event in &events {
            let key = stream_of(event);
            let current = next
                .get(&key)
                .or_else(|| log.versions.get(&key))
                .copied()
                .unwrap_or(0);
            if event.version != current + 1 {
                return Err(WriteError::InvalidOriginalVersion);
            }
            next.insert(key, event.version);
        }
        log.events.extend(events);
        log.versions.extend(next);
        let persist = match log.persist.as_mut() {
            Some((rng, p)) => rng.random_bool(p.clamp(0.0, 1.0)),
            None => true,
        };
        if persist {
            log.persisted = log.events.len();
        }
        Ok(())
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        Ok(self.version_in(&tenant, &aggregate_type, &aggregate_id))
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        let events: Vec<Event> = self
            .log
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| {
                tenant.matches(&e.tenant) && matches(e, &aggregators, &routing_key, to_micros)
            })
            .cloned()
            .collect();
        Ok(Reader::new(events).args(args).execute()?)
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        Ok(self
            .log
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| tenant.matches(&e.tenant) && matches(e, &aggregators, &routing_key, None))
            .map(Event::timestamp_micros)
            .max()
            .unwrap_or(0))
    }

    // The simulation never subscribes nor snapshots projections.

    async fn get_subscriber_cursor(&self, _key: String) -> anyhow::Result<Option<Value>> {
        Ok(None)
    }

    async fn is_subscriber_running(&self, _key: String, _worker_id: Ulid) -> anyhow::Result<bool> {
        Ok(false)
    }

    async fn upsert_subscriber(&self, _key: String, _worker_id: Ulid) -> anyhow::Result<()> {
        Ok(())
    }

    async fn acknowledge(
        &self,
        _key: String,
        _worker_id: Ulid,
        _cursor: Value,
        _lag: u64,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }

    async fn get_snapshot(
        &self,
        _tenant: String,
        _aggregate_type: String,
        _projection: String,
        _aggregate_revision: String,
        _id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        Ok(None)
    }

    async fn save_snapshot(
        &self,
        _tenant: String,
        _aggregate_type: String,
        _projection: String,
        _aggregate_revision: String,
        _id: String,
        _data: Vec<u8>,
        _cursor: Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_snapshot(
        &self,
        _tenant: String,
        _aggregate_type: String,
        _projection: String,
        _id: String,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        let mut log = self.log.lock().unwrap();
        let stream = (tenant, aggregate_type, aggregate_id);
        // `persisted` is a prefix length: every removed event below it
        // shortens the prefix by one, so a crash still keeps exactly the
        // events that were persisted.
        let removed_persisted = log.events[..log.persisted]
            .iter()
            .filter(|e| stream_of(e) == stream)
            .count();
        log.events.retain(|e| stream_of(e) != stream);
        log.persisted -= removed_persisted;
        log.rebuild_versions();
        Ok(())
    }
}
