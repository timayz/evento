//! Wiring Accord into evento's [`Executor`] trait.
//!
//! [`AccordExecutor`] is a drop-in `evento_core::Executor` whose **writes** are
//! coordinated through an Accord cluster — replicated, strictly serializable, and
//! atomic across aggregates — while **reads, subscriptions, and snapshots** are
//! served from a local evento backend (`Fjall`/`Sql`) that each replica keeps up
//! to date by applying committed transactions.
//!
//! [`ExecutorDataStore`] is the bridge in the other direction: the Accord
//! [`DataStore`] that a replica uses to read an
//! aggregate's version (for the commit condition) and to append committed events
//! — both delegated to that same local backend. Share one backend instance
//! (it is `Clone`/`Arc`-backed) between the two so a node's writes are visible to
//! its reads.
//!
//! ## Tenants
//!
//! A stream is `(tenant, type, id)`, and a key lives in its tenant
//! ([`Key::in_tenant`]): the same aggregate id in two tenants is two keys, so
//! their writes never conflict. The local backend applies and serves every
//! tenant's events.
//!
//! ## Scope
//!
//! This targets a **single-shard** cluster (every node replicates everything),
//! so a node's local backend holds the whole log and serves complete reads.
//! Routing reads to owning shards in a multi-shard cluster is a later layer.

use std::sync::Arc;

use async_trait::async_trait;
use evento_core::{
    cursor::{Args, ReadResult, Value},
    Event, EventFilter, Executor, RoutingKey, TenantFilter, WriteError,
};
use ulid::Ulid;

use crate::api::{same_event, snapshot_event_size, DataStore, SnapshotPage};
use crate::clock::{Timestamp, TxnId};
use crate::message::Key;
use crate::node::{BacklogAtCap, Node, NodeClosed, RejoinRequired};

/// Most events one backend read returns for the scans behind
/// [`ExecutorDataStore::snapshot_page`] and [`ExecutorDataStore::snapshot`].
/// Both walk the backend page by page until exhausted, so a store of any size
/// is handled. Sized to balance round-trips against per-page memory.
const SNAPSHOT_PAGE_SIZE: u16 = 4096;

/// Events one backend read returns while [`ExecutorDataStore::holds`] scans
/// an aggregate back from its newest event.
const HOLDS_PAGE_SIZE: u16 = 64;

/// The single key a read targets, if it can be pinned to one shard: an explicit
/// routing key, or a query for one aggregate by id — however many filters spell
/// it out (a projection load issues one filter per handled event type, all for
/// the same id) — in the one tenant it reads ([`Key::in_tenant`]). Broad scans
/// (several aggregate ids, by event type alone, or every tenant) return `None`
/// and are served locally.
///
/// **Caveat:** the write side derives the shard key from `routing_key` falling
/// back to the aggregate id ([`Key::of`]). A by-id pin is therefore only
/// correct for aggregates written **without** a routing key; a read of a
/// *routed* aggregate must pass its routing key too, or the pin lands on the
/// wrong key — a forwarded read goes to the wrong shard, and a read barrier
/// fences the wrong conflict set.
fn target_key(
    aggregators: &Option<Arc<[EventFilter]>>,
    routing_key: &Option<RoutingKey>,
    tenant: &TenantFilter,
) -> Option<Key> {
    // A key belongs to one tenant: a read of every tenant spans many.
    let TenantFilter::Only(tenant) = tenant else {
        return None;
    };
    if let Some(RoutingKey::Value(Some(key))) = routing_key {
        return Some(Key::in_tenant(tenant, key));
    }
    let filters = aggregators.as_deref()?;
    let (first, rest) = filters.split_first()?;
    let id = first.aggregate_id.as_deref()?;
    rest.iter()
        .all(|f| f.aggregate_id.as_deref() == Some(id))
        .then(|| Key::in_tenant(tenant, id))
}

#[cfg(test)]
mod target_key_tests {
    use super::*;

    fn default_tenant() -> TenantFilter {
        TenantFilter::Only(String::new())
    }

    fn filter(id: Option<&str>, name: Option<&str>) -> EventFilter {
        EventFilter {
            aggregate_type: "t/A".into(),
            aggregate_id: id.map(str::to_owned),
            name: name.map(str::to_owned),
        }
    }

    #[test]
    fn pins_one_aggregate_however_many_filters_name_it() {
        let one: Arc<[EventFilter]> = Arc::from(vec![filter(Some("a"), None)]);
        assert_eq!(
            target_key(&Some(one), &None, &default_tenant()),
            Some(Key("a".into()))
        );
        // A projection load: one filter per handled event, same id.
        let many: Arc<[EventFilter]> = Arc::from(vec![
            filter(Some("a"), Some("Opened")),
            filter(Some("a"), Some("Deposited")),
            filter(Some("a"), None),
        ]);
        assert_eq!(
            target_key(&Some(many), &None, &default_tenant()),
            Some(Key("a".into()))
        );
    }

    #[test]
    fn broad_scans_are_not_pinned() {
        let by_type: Arc<[EventFilter]> = Arc::from(vec![filter(None, Some("Opened"))]);
        assert_eq!(target_key(&Some(by_type), &None, &default_tenant()), None);
        let two_ids: Arc<[EventFilter]> =
            Arc::from(vec![filter(Some("a"), None), filter(Some("b"), None)]);
        assert_eq!(target_key(&Some(two_ids), &None, &default_tenant()), None);
        let mixed: Arc<[EventFilter]> =
            Arc::from(vec![filter(Some("a"), None), filter(None, Some("Opened"))]);
        assert_eq!(target_key(&Some(mixed), &None, &default_tenant()), None);
        let empty: Arc<[EventFilter]> = Arc::from(Vec::<EventFilter>::new());
        assert_eq!(target_key(&Some(empty), &None, &default_tenant()), None);
        assert_eq!(target_key(&None, &None, &default_tenant()), None);
    }

    #[test]
    fn an_explicit_routing_key_wins() {
        let two_ids: Arc<[EventFilter]> =
            Arc::from(vec![filter(Some("a"), None), filter(Some("b"), None)]);
        assert_eq!(
            target_key(
                &Some(two_ids),
                &Some(RoutingKey::Value(Some("region".into()))),
                &default_tenant(),
            ),
            Some(Key("region".into()))
        );
    }

    /// A read of one tenant pins the key in that tenant, as the write side
    /// derives it; a read of every tenant spans many keys and is not pinned.
    #[test]
    fn a_tenant_read_pins_the_key_in_its_tenant() {
        let one: Arc<[EventFilter]> = Arc::from(vec![filter(Some("a"), None)]);
        let acme = TenantFilter::Only("acme".to_owned());
        let pinned = target_key(&Some(one.clone()), &None, &acme);
        let written = Key::of(&Event {
            aggregate_id: "a".into(),
            tenant: "acme".into(),
            ..Default::default()
        });
        assert_eq!(pinned, Some(written.clone()));
        assert_ne!(
            Some(written),
            target_key(&Some(one.clone()), &None, &default_tenant())
        );
        assert_eq!(target_key(&Some(one), &None, &TenantFilter::All), None);
        assert_eq!(
            target_key(&None, &Some(RoutingKey::Value(Some("r".into()))), &acme),
            Some(Key::in_tenant("acme", "r"))
        );
    }
}

/// An Accord [`DataStore`] backed by a local evento [`Executor`].
///
/// `version` reads the aggregate's current version from the backend (the
/// optimistic-concurrency input to Accord's Read phase); `apply` appends the
/// committed events (or does nothing on abort).
pub struct ExecutorDataStore<E: Executor> {
    local: E,
}

impl<E: Executor> ExecutorDataStore<E> {
    /// Wraps a local evento backend as an Accord data store.
    pub fn new(local: E) -> Self {
        Self { local }
    }
}

#[async_trait]
impl<E: Executor> DataStore for ExecutorDataStore<E> {
    async fn version(
        &self,
        tenant: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<u16> {
        // The aggregate's current version is the highest among its events —
        // an indexed lookup on every backend (`MAX(version)` on SQL, a
        // reverse index seek on fjall) instead of paging the whole stream.
        // The default trait impl still pages for backends without an
        // override, so correctness never depends on the fast path.
        self.local
            .latest_version(
                tenant.to_owned(),
                aggregate_type.to_owned(),
                aggregate_id.to_owned(),
            )
            .await
    }

    async fn apply(
        &self,
        _txn: TxnId,
        _execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if !commit || events.is_empty() {
            return Ok(());
        }
        // `replicate`, not `write`: the node has already stamped the events
        // from the transaction's agreed `execute_at` — the same on every
        // replica, rising with every key's serial order — so they must be
        // persisted verbatim (a backend re-stamping with its own clock would
        // desync this replica's cursors from its peers', and from the
        // stability watermark computed from those stamps).
        match self.local.replicate(events.clone()).await {
            Ok(()) => Ok(()),
            // Accord already validated the condition at this serial point,
            // so a version conflict is an idempotent re-apply — if these very
            // events are in place. Otherwise the store is behind (a gap) or
            // holds other events there: taking that for "already present"
            // would mark the transaction applied with nothing stored, and
            // every later event on the aggregate would fail the same way — a
            // store frozen for good (#309). Fail instead: the replica keeps
            // the transaction un-applied and retries it.
            Err(WriteError::InvalidOriginalVersion) => {
                anyhow::ensure!(
                    self.holds(&events).await?,
                    "the store's versions do not admit this apply: it is behind, or holds other events"
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn holds(&self, events: &[Event]) -> anyhow::Result<bool> {
        for event in events {
            let filter: Arc<[EventFilter]> = Arc::from([EventFilter::by_id_raw(
                event.aggregate_type.clone(),
                event.aggregate_id.clone(),
            )]);
            // Newest first: a transaction's own events are usually among its
            // aggregates' latest, and versions only rise along a stream, so
            // the scan stops at the first version below this one.
            let mut before = None;
            let held = 'scan: loop {
                let page = self
                    .local
                    .read(
                        Some(Arc::clone(&filter)),
                        None,
                        TenantFilter::Only(event.tenant.clone()),
                        Args::backward(HOLDS_PAGE_SIZE, before),
                        None,
                    )
                    .await?;
                for edge in page.edges.iter().rev() {
                    if edge.node.version == event.version {
                        break 'scan same_event(&edge.node, event);
                    }
                    if edge.node.version < event.version {
                        break 'scan false;
                    }
                }
                if !page.page_info.has_previous_page {
                    break false;
                }
                match page.page_info.start_cursor {
                    Some(cursor) => before = Some(cursor),
                    None => break false,
                }
            };
            if !held {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.local
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        // The materialised state is the full event log, in memory at once: only
        // the legacy inline bootstrap answer uses it. A joiner on the current
        // protocol pulls `snapshot_page`s, so a store of any size crosses the
        // wire in frame-sized pieces.
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .snapshot_page(after, SNAPSHOT_PAGE_SIZE, usize::MAX)
                .await?;
            out.extend(page.events);
            match page.next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(out)
    }

    async fn snapshot_page(
        &self,
        after: Option<Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<SnapshotPage> {
        // One backend read in the store's own cursor order, trimmed to the byte
        // budget: the cursor of the last event kept is where the next page
        // starts, so a trimmed remainder is simply read again next time.
        let result = self
            .local
            .read(
                None,
                None,
                // The whole log, every tenant's events: a joiner copies it all.
                TenantFilter::All,
                Args::forward(max_events.clamp(1, SNAPSHOT_PAGE_SIZE), after),
                None,
            )
            .await?;
        let has_next = result.page_info.has_next_page;
        let end_cursor = result.page_info.end_cursor;
        let mut events = Vec::with_capacity(result.edges.len());
        let mut last_cursor = None;
        let mut bytes = 0;
        let mut trimmed = false;
        for edge in result.edges {
            let size = snapshot_event_size(&edge.node);
            if !events.is_empty() && bytes + size > max_bytes {
                trimmed = true;
                break;
            }
            bytes += size;
            last_cursor = Some(edge.cursor);
            events.push(edge.node);
        }
        let next = if trimmed {
            last_cursor
        } else if has_next {
            end_cursor
        } else {
            None
        };
        Ok(SnapshotPage { events, next })
    }
}

/// An evento [`Executor`] whose writes are coordinated through an Accord cluster
/// and whose reads/subscriptions/snapshots are served from a local backend.
#[derive(Clone)]
pub struct AccordExecutor<E: Executor + Clone> {
    node: Node,
    local: E,
}

impl<E: Executor + Clone> AccordExecutor<E> {
    /// Builds an executor over `node` (for coordinating writes) and `local`
    /// (the backend serving reads — the same instance the node's
    /// [`ExecutorDataStore`] applies to).
    pub fn new(node: Node, local: E) -> Self {
        Self { node, local }
    }

    /// The underlying node, e.g. to run recovery.
    pub fn node(&self) -> &Node {
        &self.node
    }
}

#[async_trait]
impl<E: Executor + Clone> Executor for AccordExecutor<E> {
    fn default_routing_key(&self) -> Option<&str> {
        self.local.default_routing_key()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Err(WriteError::MissingData);
        }
        // A refusal that clears on its own (the backlog cap, a rejoin in
        // progress, a node retired until its host reopens it) is `Unavailable`,
        // so a caller can tell a transient condition from a failure and keep
        // the typed cause.
        let outcome = self.node.write(events).await.map_err(|err| {
            if err.is::<BacklogAtCap>() || err.is::<RejoinRequired>() || err.is::<NodeClosed>() {
                WriteError::Unavailable(err)
            } else {
                WriteError::Unknown(err)
            }
        })?;
        if outcome.conflict {
            Err(WriteError::InvalidOriginalVersion)
        } else {
            Ok(())
        }
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        // Subscriptions read from the local backend, and committed writes are
        // applied to it via the node's `ExecutorDataStore`. That apply calls the
        // local executor's `write`, which bumps the local signal — so forwarding
        // here delivers a wakeup once a write is locally visible.
        self.local.write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // Gate subscriptions by the node's stability watermark. Events are
        // stamped from their transaction's `execute_at`, but a replica learns
        // of non-conflicting transactions in no particular order — one with
        // a lower `execute_at` (a lower cursor) can arrive after a higher one
        // was applied — so without this gate the forward read would skip it.
        // See `Node::stable_micros`.
        Ok(Some(self.node.stable_micros()))
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        // A single-shard read for a key this node does not own is forwarded to an
        // owner. Everything else (owned keys, and broad scans that can't be pinned
        // to one shard) is served from the local backend.
        if let Some(key) = target_key(&aggregators, &routing_key, &tenant) {
            if self.node.owns_key(&key) {
                // Linearizable reads: fence the local backend with a read barrier
                // (a read-only consensus round) so it reflects every write that
                // committed before this read began, then serve locally. Off by
                // default — reads stay local-only (serializable, not linearizable).
                if self.node.linearizable_reads() {
                    self.node.read_barrier(key).await?;
                }
            } else if let Some(owner) = self.node.an_owner_of(&key) {
                // NOTE: a forwarded (non-owned, multi-shard) read is not yet
                // linearized — the owner would need to barrier before serving.
                return self
                    .node
                    .forward_read(owner, aggregators, routing_key, tenant, args, to_micros)
                    .await;
            }
        }
        self.local
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.local
            .latest_timestamp(aggregators, routing_key, tenant)
            .await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.local.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.local.is_subscriber_running(key, worker_id).await
    }

    // Explicitly forwarded (not left to the trait defaults) so the local
    // backend's single-round-trip overrides are reached through the wrapper.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<evento_core::SubscriberStatus> {
        self.local.subscriber_status(key, worker_id).await
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.local
            .latest_version(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.local
            .stream_routing_key(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.local.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.local.acknowledge(key, worker_id, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregator_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.local
            .get_snapshot(tenant, aggregate_type, projection, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.local
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregator_revision,
                id,
                data,
                cursor,
            )
            .await
    }

    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()> {
        self.local
            .delete_snapshot(tenant, aggregate_type, projection, id)
            .await
    }
}
