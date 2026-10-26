//! Event storage and retrieval abstraction.
//!
//! This module defines the [`Executor`] trait, the core abstraction for event
//! persistence. Implementations handle storing events, querying, and managing
//! subscriptions.
//!
//! # Types
//!
//! - [`Executor`] - Core trait for event storage backends
//! - [`Evento`] - Type-erased wrapper around any executor
//! - [`EventoGroup`] - Multi-executor aggregation (feature: `group`)
//! - [`Rw`] - Read-write split executor (feature: `rw`)
//! - [`EventFilter`] - Query filter for reading events
//! - [`TenantFilter`] - Which tenants a scan covers

use std::{hash::Hash, sync::Arc};
use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    Aggregate, AggregateEvent, Event, RoutingKey, WriteError,
};

/// Longest tenant id a store keeps: SQL backends hold it in a `VARCHAR(255)`.
pub const MAX_TENANT_LEN: usize = 255;

/// Which tenants a scan ([`Executor::read`], [`Executor::latest_timestamp`])
/// covers.
///
/// A stream belongs to one tenant: its identity is `(tenant, aggregate_type,
/// aggregate_id)`, and [`Event::tenant`] is `""` for the **default tenant**,
/// where every single-tenant application lives. A read of one aggregate
/// always names its tenant; a scan across aggregates — what a subscription
/// reads — covers every tenant.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TenantFilter {
    /// Every tenant.
    All,
    /// One tenant; `""` is the default tenant.
    Only(String),
}

impl TenantFilter {
    /// Whether an event of `tenant` passes this filter.
    pub fn matches(&self, tenant: &str) -> bool {
        match self {
            TenantFilter::All => true,
            TenantFilter::Only(only) => only == tenant,
        }
    }
}

/// A tenant id that cannot name a tenant (see [`TenantError::check`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TenantError {
    /// The empty tenant is the default one, where callers that name no tenant
    /// act.
    #[error("a tenant id must not be empty: the empty tenant is the default one")]
    Empty,
    /// Longer than [`MAX_TENANT_LEN`] bytes.
    #[error("a tenant id of {0} bytes is longer than the {MAX_TENANT_LEN} a store keeps")]
    TooLong(usize),
}

impl TenantError {
    /// Checks that `tenant` names a tenant other than the default one, and
    /// that a store can keep it.
    pub fn check(tenant: &str) -> Result<(), TenantError> {
        if tenant.is_empty() {
            return Err(TenantError::Empty);
        }
        Self::check_len(tenant)
    }

    /// Checks only that a store can keep `tenant`: the empty tenant passes,
    /// since a write that names no tenant lands in the default one.
    ///
    /// The one definition of the length bound, shared with
    /// [`check`](Self::check), so a write and a session can never disagree
    /// on [`MAX_TENANT_LEN`].
    pub fn check_len(tenant: &str) -> Result<(), TenantError> {
        if tenant.len() > MAX_TENANT_LEN {
            return Err(TenantError::TooLong(tenant.len()));
        }
        Ok(())
    }
}

/// Filter for querying events by aggregate.
///
/// Each constructor comes in two forms: a typed one that takes the aggregate (or
/// event) as a type parameter and reads the stored type name off it, and a
/// `*_raw` one that takes the name as a string for callers holding a type only
/// at runtime. Prefer the typed form — it cannot drift from the aggregate.
///
/// # Example
///
/// ```rust
/// # use evento::EventFilter;
/// #[evento::aggregate(name = "myapp/User")]
/// pub enum User {
///     UserCreated { name: String },
/// }
///
/// // All events for an aggregate type
/// let filter = EventFilter::by_type::<User>();
///
/// // Events for a specific aggregate instance
/// let filter = EventFilter::by_id::<User>("user-123");
///
/// // Events of a specific type
/// let filter = EventFilter::by_event::<UserCreated>();
///
/// // ...and one specific event of one instance
/// let filter = EventFilter::exact::<UserCreated>("user-123");
///
/// // When the aggregate type is only known as a string:
/// let filter = EventFilter::by_id_raw("myapp/User", "user-123");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EventFilter {
    /// Aggregate type (e.g., "myapp/User")
    pub aggregate_type: String,
    /// Optional specific aggregate ID
    pub aggregate_id: Option<String>,
    /// Optional event name filter
    pub name: Option<String>,
}

impl EventFilter {
    /// Creates a filter for one specific event of one aggregate instance.
    ///
    /// The aggregate type and event name both come from `EV`; see
    /// [`exact_raw`](Self::exact_raw) for the string form.
    pub fn exact<EV: AggregateEvent>(id: impl Into<String>) -> Self {
        Self::exact_raw(EV::aggregate_type(), id, EV::event_name())
    }

    /// Creates a filter with all fields specified, from raw strings.
    ///
    /// Filters events by aggregate type, specific aggregate ID, and event name.
    /// Prefer [`exact`](Self::exact) when the event type is known at compile time.
    pub fn exact_raw(
        aggregate_type: impl Into<String>,
        id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: Some(id.into()),
            name: Some(name.into()),
        }
    }

    /// Creates a filter for all events of an aggregate type.
    ///
    /// Returns all events regardless of aggregate ID or event name. See
    /// [`by_type_raw`](Self::by_type_raw) for the string form.
    pub fn by_type<A: Aggregate>() -> Self {
        Self::by_type_raw(A::aggregate_type())
    }

    /// Creates a filter for all events of an aggregate type, from a raw string.
    ///
    /// Prefer [`by_type`](Self::by_type) when the aggregate is known at compile time.
    pub fn by_type_raw(value: impl Into<String>) -> Self {
        Self {
            aggregate_type: value.into(),
            aggregate_id: None,
            name: None,
        }
    }

    /// Creates a filter for a specific aggregate instance.
    ///
    /// Returns all events for the given ID, with the aggregate type taken from
    /// `A`. See [`by_id_raw`](Self::by_id_raw) for the string form.
    pub fn by_id<A: Aggregate>(id: impl Into<String>) -> Self {
        Self::by_id_raw(A::aggregate_type(), id)
    }

    /// Creates a filter for a specific aggregate instance, from a raw type string.
    ///
    /// Prefer [`by_id`](Self::by_id) when the aggregate is known at compile time.
    pub fn by_id_raw(aggregate_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: Some(id.into()),
            name: None,
        }
    }

    /// Creates a filter for one event type, across every aggregate instance.
    ///
    /// Both the aggregate type and the event name come from `EV`; see
    /// [`by_event_raw`](Self::by_event_raw) for the string form.
    pub fn by_event<EV: AggregateEvent>() -> Self {
        Self::by_event_raw(EV::aggregate_type(), EV::event_name())
    }

    /// Creates a filter for a specific event type, from raw strings.
    ///
    /// Prefer [`by_event`](Self::by_event) when the event is known at compile time.
    pub fn by_event_raw(aggregate_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: None,
            name: Some(name.into()),
        }
    }
}

impl Hash for EventFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.aggregate_type.hash(state);
        self.aggregate_id.hash(state);
        self.name.hash(state);
    }
}

/// Combined subscriber fencing + cursor state, fetched in one backend round
/// trip by [`Executor::subscriber_status`].
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SubscriberStatus {
    /// Whether the queried `worker_id` is still the registered owner of the
    /// subscription (same contract as [`Executor::is_subscriber_running`]).
    pub running: bool,
    /// The subscription's current cursor position.
    pub cursor: Option<Value>,
}

/// Core trait for event storage backends.
///
/// Implementations handle persisting events (`write`/`replicate`), querying
/// (`read`, `latest_timestamp`), and managing subscription state
/// (`upsert_subscriber`, `acknowledge`, `subscriber_status`). Backends include
/// `evento_sql::Sql` (SQLite/MySQL/PostgreSQL), `evento_fjall::Fjall`
/// (embedded), `evento_remote::Client` (TCP), and `evento_accord`
/// (consensus-replicated).
#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    /// Default routing key applied to new aggregates and inherited by
    /// subscriptions.
    ///
    /// Most backends return `None`; [`Evento`] overrides this to expose its
    /// configured default. Used by `WriteBuilder::commit` to fill in the
    /// routing key for a brand-new aggregate (an existing stream keeps its
    /// original key), and by `SubscriptionBuilder::start` /
    /// `ProjectionSubscription::start` to inherit a default when the user
    /// has not called `.routing_key()` or `.any_routing_key()`.
    fn default_routing_key(&self) -> Option<&str> {
        None
    }

    /// Persists events atomically.
    ///
    /// Backends that own event ordering (SQL, Fjall) re-stamp
    /// `timestamp`/`timestamp_subsec` with their commit clock here, so cursor
    /// order matches commit order. Callers must not rely on the timestamps
    /// they supplied surviving a `write`; use [`replicate`](Self::replicate)
    /// to persist pre-stamped events verbatim.
    ///
    /// Returns `WriteError::InvalidOriginalVersion` if version conflicts occur.
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;

    /// Persists pre-stamped events verbatim, without re-stamping timestamps.
    ///
    /// Replication layers that own ordering themselves (e.g. Accord applying
    /// consensus-ordered events to a local store) use this to preserve the
    /// timestamps agreed on by the replication protocol. The default delegates
    /// to [`write`](Self::write); backends that re-stamp in `write` must
    /// override this to skip the re-stamp.
    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.write(events).await
    }

    /// Returns a receiver notified after each successful in-process `write`,
    /// for low-latency subscription wakeup.
    ///
    /// The channel carries a monotonically increasing write generation. A
    /// running subscription selects on this alongside its poll interval so it
    /// wakes the instant an event is committed through the same executor
    /// instance (or a clone), instead of waiting for the next poll tick.
    ///
    /// Default `None` → pure polling. Cross-process writers are not observed by
    /// this signal; the poll interval remains the fallback for those.
    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }

    /// An exclusive upper bound, in microseconds since the Unix epoch, on the
    /// event timestamps a subscription may safely process — or `None` for no
    /// bound (the default).
    ///
    /// Backends that serialize writes in-process and stamp them with a
    /// monotonic commit clock (Fjall) return `None`: their cursor order always
    /// matches commit order. Backends where independent writers can commit out
    /// of cursor order return a stability watermark: a SQL store shared by
    /// several processes returns DB-server time minus a small margin covering
    /// in-flight statement duration, and a replicated backend (multi-node
    /// Accord) returns its own stability bound. The subscription processes only
    /// events whose timestamp is strictly below the watermark, so it never
    /// advances past a position where a lower-cursor event could still become
    /// visible later.
    ///
    /// Async so implementations can consult an authoritative clock (e.g. the
    /// DB server) instead of the local wall clock.
    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        Ok(None)
    }

    /// Gets the current cursor position for a subscription.
    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>>;

    /// Checks if a subscription is running with the given worker ID.
    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool>;

    /// Fetches subscriber fencing state and cursor together.
    ///
    /// The subscription loop calls this once per pass; backends should
    /// override it to answer from a single query instead of the default's two
    /// round trips. The cursor is only meaningful while `running` is true, so
    /// the default skips fetching it for a fenced-out worker.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        if !self.is_subscriber_running(key.clone(), worker_id).await? {
            return Ok(SubscriberStatus {
                running: false,
                cursor: None,
            });
        }
        let cursor = self.get_subscriber_cursor(key).await?;
        Ok(SubscriberStatus {
            running: true,
            cursor,
        })
    }

    /// Returns the highest committed version of an aggregate stream, or 0 when
    /// the stream does not exist.
    ///
    /// Must be exact under both [`write`](Self::write) and
    /// [`replicate`](Self::replicate): replicated events can carry timestamps
    /// out of version order, so backends must derive this from versions (e.g.
    /// `MAX(version)` or a version index), never from cursor position. The
    /// default pages through the stream and takes the running max — correct
    /// but O(stream length); backends should override with an indexed lookup.
    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        const PAGE_SIZE: u16 = 4096;
        let filters: Arc<[EventFilter]> =
            Arc::from([EventFilter::by_id_raw(aggregate_type, aggregate_id)]);
        let tenant = TenantFilter::Only(tenant);
        let mut max = 0u16;
        let mut after = None;
        loop {
            let result = self
                .read(
                    Some(filters.clone()),
                    None,
                    tenant.clone(),
                    Args::forward(PAGE_SIZE, after),
                    None,
                )
                .await?;
            if let Some(page_max) = result.edges.iter().map(|e| e.node.version).max() {
                max = max.max(page_max);
            }
            if !result.page_info.has_next_page {
                break;
            }
            // Defensive: a "more pages" claim with no cursor can't advance —
            // stop rather than loop forever.
            match result.page_info.end_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(max)
    }

    /// Returns the routing key of an existing aggregate stream.
    ///
    /// `None` means the stream does not exist; `Some(key)` is the routing key
    /// its first event was committed with (which may itself be `None`). Used
    /// by `WriteBuilder::commit` so an append inherits the stream's original
    /// key without fetching a full event row — backends should override with
    /// a narrow lookup of the version-1 event.
    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        let result = self
            .read(
                Some(Arc::from([EventFilter::by_id_raw(
                    aggregate_type,
                    aggregate_id,
                )])),
                None,
                TenantFilter::Only(tenant),
                Args::forward(1, None),
                None,
            )
            .await?;
        Ok(result.edges.first().map(|e| e.node.routing_key.clone()))
    }

    /// Creates or updates a subscription record.
    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()>;

    /// Updates the subscription cursor after processing an event, fenced by
    /// `worker_id`.
    ///
    /// Implementations must only apply the update while `worker_id` is still
    /// the registered worker for `key`, and return `Ok(false)` (without
    /// updating) when ownership has been taken over by another worker — this
    /// prevents a superseded worker from rewinding the shared cursor
    /// mid-chunk. Returns `Ok(true)` when the cursor was updated.
    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool>;

    /// Queries events with filtering and pagination.
    ///
    /// `tenant` narrows the read to one tenant's events, or covers every
    /// tenant ([`TenantFilter::All`]).
    ///
    /// `to_micros` is an optional **exclusive** upper bound on event stamps,
    /// in microseconds since the Unix epoch: events at or above it are
    /// excluded, ideally by the backend itself (SQL adds a sargable predicate;
    /// fjall filters before pagination) so a watermark-gated subscription
    /// never fetches rows it would discard. Implementations may ignore it —
    /// the subscription loop keeps a per-event check as a backstop — but
    /// should honor it for performance.
    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>>;

    /// Returns the timestamp of the most recent event matching the filter, in
    /// whole **seconds** since the Unix epoch (unlike
    /// [`stable_timestamp`](Self::stable_timestamp), which is microseconds).
    ///
    /// Returns 0 when no matching event exists. Used by the subscription loop
    /// to compute lag without fetching the full event row (data/metadata blobs).
    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64>;

    /// Retrieves the snapshot a projection stored for an aggregate.
    ///
    /// Snapshots are keyed by `(tenant, aggregate_type, projection, id)`: `projection`
    /// is the name of the view the bytes belong to (see
    /// [`ProjectionCursor::projection_name`](crate::projection::ProjectionCursor::projection_name)),
    /// so several snapshotted projections of one aggregate never share a slot.
    ///
    /// Returns the serialized snapshot data and cursor position, or `None`
    /// if no snapshot exists under that key at `aggregate_revision`.
    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>>;

    /// Stores a projection's snapshot for an aggregate.
    ///
    /// Snapshots cache aggregate state to avoid replaying all events.
    /// The `cursor` indicates the event position up to which the snapshot is valid.
    // One argument per part of the snapshot key — tenant, aggregate type,
    // projection, id — plus the revision and the payload.
    #[allow(clippy::too_many_arguments)]
    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()>;

    /// Deletes a projection's stored snapshot for an aggregate.
    ///
    /// Idempotent: deleting a snapshot that does not exist is not an error.
    /// Revision is intentionally omitted — `save_snapshot` upserts on
    /// `(tenant, type, projection, id)` so there is only ever one row per
    /// projection of an aggregate.
    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()>;

    /// Removes one stream — the events of `(tenant, aggregate_type,
    /// aggregate_id)` and every index entry that leads to them — so the
    /// stream can be written again from its first version.
    ///
    /// A maintenance primitive for replication layers, not an application
    /// API: a replica that stops owning a key range drops its streams, and
    /// one that gains a range back copies them in afresh. Projection
    /// snapshots and subscriber cursors stay — events are immutable, so a
    /// snapshot at version N is still a valid prefix of a stream copied back
    /// in, and a cursor is a position, not an offset.
    ///
    /// Idempotent: deleting a stream that does not exist is not an error.
    /// The default refuses; backends that own their storage (SQL, Fjall)
    /// implement it.
    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        let _ = (tenant, aggregate_type, aggregate_id);
        anyhow::bail!("delete_stream is not supported by this executor")
    }
}

/// Type-erased wrapper around any [`Executor`] implementation.
///
/// `Evento` wraps an executor in `Arc<Box<dyn Executor>>` for dynamic dispatch.
/// This allows storing different executor implementations in the same collection.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{Event, Evento, Executor};
/// # async fn run<E: Executor>(executor: E, events: Vec<Event>) -> anyhow::Result<()> {
/// let evento = Evento::new(executor);
///
/// // Use like any executor
/// evento.write(events).await?;
/// # Ok(())
/// # }
/// ```
pub struct Evento {
    inner: Arc<Box<dyn Executor>>,
    default_routing_key: Option<String>,
}

impl Clone for Evento {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            default_routing_key: self.default_routing_key.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Executor for Evento {
    fn default_routing_key(&self) -> Option<&str> {
        self.default_routing_key.as_deref()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        // The default routing key is applied by `WriteBuilder::commit` (which
        // consults `default_routing_key()` only for a brand-new aggregate, so
        // an existing stream keeps its original key even when that key is
        // `None`). Filling it in here would split such a stream across two
        // routing keys.
        self.inner.write(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.inner.replicate(events).await
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.inner.write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        self.inner.stable_timestamp().await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.inner
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.inner
            .latest_timestamp(aggregators, routing_key, tenant)
            .await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.inner.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.inner.is_subscriber_running(key, worker_id).await
    }

    // Explicitly forwarded (not left to the trait default) so the inner
    // backend's single-round-trip overrides are reached through the wrapper.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.inner.subscriber_status(key, worker_id).await
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.inner
            .latest_version(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.inner
            .stream_routing_key(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.inner.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.inner.acknowledge(key, worker_id, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.inner
            .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.inner
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregate_revision,
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
        self.inner
            .delete_snapshot(tenant, aggregate_type, projection, id)
            .await
    }

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_stream(tenant, aggregate_type, aggregate_id)
            .await
    }
}

impl Evento {
    /// Creates a new type-erased executor wrapper.
    pub fn new<E: Executor>(executor: E) -> Self {
        Self {
            inner: Arc::new(Box::new(executor)),
            default_routing_key: None,
        }
    }

    /// Sets a default routing key applied to writes and inherited by
    /// subscriptions built from this executor.
    ///
    /// Per-event/per-aggregate routing keys still take precedence on writes.
    /// Subscriptions inherit this key only when the user has not called
    /// `.routing_key()` or `.any_routing_key()`.
    pub fn default_routing_key(mut self, key: impl Into<String>) -> Self {
        self.default_routing_key = Some(key.into());
        self
    }
}

/// Multi-executor aggregation (requires `group` feature).
///
/// `EventoGroup` combines multiple executors into one. Reads query all executors
/// and merge results; writes go only to the first executor. [`delete_stream`]
/// is the one mutation that reaches every executor, so a deleted stream is
/// gone from the merged read as well.
///
/// [`delete_stream`]: Executor::delete_stream
///
/// Useful for aggregating events from multiple sources.
#[cfg(feature = "group")]
#[derive(Clone, Default)]
pub struct EventoGroup {
    executors: Vec<Evento>,
}

#[cfg(feature = "group")]
impl EventoGroup {
    /// Adds an executor to the group.
    ///
    /// Returns `self` for method chaining.
    pub fn executor(mut self, executor: impl Into<Evento>) -> Self {
        self.executors.push(executor.into());

        self
    }

    /// Returns a reference to the first executor in the group.
    ///
    /// # Panics
    ///
    /// Panics if the group has no executors.
    pub fn first(&self) -> &Evento {
        self.executors
            .first()
            .expect("EventoGroup must have at least one executor")
    }
}

#[cfg(feature = "group")]
#[async_trait::async_trait]
impl Executor for EventoGroup {
    fn default_routing_key(&self) -> Option<&str> {
        self.first().default_routing_key()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.first().write(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.first().replicate(events).await
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.first().write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        self.first().stable_timestamp().await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        use crate::cursor;
        let futures = self.executors.iter().map(|e| {
            e.read(
                aggregators.to_owned(),
                routing_key.to_owned(),
                tenant.clone(),
                args.clone(),
                to_micros,
            )
        });

        let results = futures_util::future::join_all(futures).await;
        let mut events = vec![];
        // A child executor that filled its own limit may hold further pages the
        // merged in-memory reader cannot see (it never over-fetches children),
        // so its `has_next_page`/`has_previous_page` must carry over — otherwise
        // callers stop paginating early and silently miss that child's events.
        let mut child_has_next = false;
        let mut child_has_previous = false;
        for res in results {
            let res = res?;
            child_has_next |= res.page_info.has_next_page;
            child_has_previous |= res.page_info.has_previous_page;
            for edge in res.edges {
                events.push(edge.node);
            }
        }

        let mut merged = cursor::Reader::new(events).args(args).execute()?;
        merged.page_info.has_next_page |= child_has_next;
        merged.page_info.has_previous_page |= child_has_previous;
        Ok(merged)
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        let futures = self.executors.iter().map(|e| {
            e.latest_timestamp(
                aggregators.to_owned(),
                routing_key.to_owned(),
                tenant.clone(),
            )
        });

        let results = futures_util::future::join_all(futures).await;
        let mut max = 0u64;
        for res in results {
            let ts = res?;
            if ts > max {
                max = ts;
            }
        }

        Ok(max)
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.first().get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.first().is_subscriber_running(key, worker_id).await
    }

    // Subscriber state lives on the first executor. `latest_version` and
    // `stream_routing_key` intentionally keep the trait defaults: those go
    // through `self.read`, which merges all children — forwarding to
    // `first()` would miss events held by the other executors.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.first().subscriber_status(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.first().upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.first().acknowledge(key, worker_id, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.first()
            .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.first()
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregate_revision,
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
        self.first()
            .delete_snapshot(tenant, aggregate_type, projection, id)
            .await
    }

    // Fanned out like `read`: the group's reads merge every child, so a
    // delete that only reached the first one would leave the stream visible
    // through the group — and a replication layer that drops then recopies
    // would end up with the stream twice. Every child is asked even when an
    // earlier one failed, so one refusing child (a backend without
    // `delete_stream`) does not leave the others holding the stream.
    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        let futures = self
            .executors
            .iter()
            .map(|e| e.delete_stream(tenant.clone(), aggregate_type.clone(), aggregate_id.clone()));

        futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect()
    }
}

#[cfg(all(test, feature = "group"))]
mod group_tests {
    use super::*;
    use crate::cursor;
    use crate::TenantFilter;
    use std::sync::Mutex;

    /// An in-memory store that answers `read` and `delete_stream` and nothing
    /// else, so a group can be exercised without a backend.
    #[derive(Clone, Default)]
    struct MemoryStore(Arc<Mutex<Vec<Event>>>);

    impl MemoryStore {
        fn holding(events: Vec<Event>) -> Self {
            Self(Arc::new(Mutex::new(events)))
        }

        fn ids(&self) -> Vec<Ulid> {
            self.0.lock().unwrap().iter().map(|e| e.id).collect()
        }
    }

    #[async_trait::async_trait]
    impl Executor for MemoryStore {
        async fn write(&self, _events: Vec<Event>) -> Result<(), WriteError> {
            unreachable!()
        }
        async fn get_subscriber_cursor(&self, _key: String) -> anyhow::Result<Option<Value>> {
            unreachable!()
        }
        async fn is_subscriber_running(
            &self,
            _key: String,
            _worker_id: Ulid,
        ) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn upsert_subscriber(&self, _key: String, _worker_id: Ulid) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn acknowledge(
            &self,
            _key: String,
            _worker_id: Ulid,
            _cursor: Value,
            _lag: u64,
        ) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn read(
            &self,
            _aggregators: Option<Arc<[EventFilter]>>,
            _routing_key: Option<RoutingKey>,
            _tenant: TenantFilter,
            args: Args,
            _to_micros: Option<u64>,
        ) -> anyhow::Result<ReadResult<Event>> {
            let events = self.0.lock().unwrap().clone();
            Ok(cursor::Reader::new(events).args(args).execute()?)
        }
        async fn latest_timestamp(
            &self,
            _aggregators: Option<Arc<[EventFilter]>>,
            _routing_key: Option<RoutingKey>,
            _tenant: TenantFilter,
        ) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn get_snapshot(
            &self,
            _tenant: String,
            _aggregate_type: String,
            _projection: String,
            _aggregate_revision: String,
            _id: String,
        ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
            unreachable!()
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
            unreachable!()
        }
        async fn delete_snapshot(
            &self,
            _tenant: String,
            _aggregate_type: String,
            _projection: String,
            _id: String,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn delete_stream(
            &self,
            tenant: String,
            aggregate_type: String,
            aggregate_id: String,
        ) -> anyhow::Result<()> {
            self.0.lock().unwrap().retain(|e| {
                (
                    e.tenant.as_str(),
                    e.aggregate_type.as_str(),
                    e.aggregate_id.as_str(),
                ) != (
                    tenant.as_str(),
                    aggregate_type.as_str(),
                    aggregate_id.as_str(),
                )
            });
            Ok(())
        }
    }

    /// A store that refuses `delete_stream`, as the trait default does.
    #[derive(Clone)]
    struct Refusing(MemoryStore);

    #[async_trait::async_trait]
    impl Executor for Refusing {
        async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
            self.0.write(events).await
        }
        async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
            self.0.get_subscriber_cursor(key).await
        }
        async fn is_subscriber_running(
            &self,
            key: String,
            worker_id: Ulid,
        ) -> anyhow::Result<bool> {
            self.0.is_subscriber_running(key, worker_id).await
        }
        async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
            self.0.upsert_subscriber(key, worker_id).await
        }
        async fn acknowledge(
            &self,
            key: String,
            worker_id: Ulid,
            cursor: Value,
            lag: u64,
        ) -> anyhow::Result<bool> {
            self.0.acknowledge(key, worker_id, cursor, lag).await
        }
        async fn read(
            &self,
            aggregators: Option<Arc<[EventFilter]>>,
            routing_key: Option<RoutingKey>,
            tenant: TenantFilter,
            args: Args,
            to_micros: Option<u64>,
        ) -> anyhow::Result<ReadResult<Event>> {
            self.0
                .read(aggregators, routing_key, tenant, args, to_micros)
                .await
        }
        async fn latest_timestamp(
            &self,
            aggregators: Option<Arc<[EventFilter]>>,
            routing_key: Option<RoutingKey>,
            tenant: TenantFilter,
        ) -> anyhow::Result<u64> {
            self.0
                .latest_timestamp(aggregators, routing_key, tenant)
                .await
        }
        async fn get_snapshot(
            &self,
            tenant: String,
            aggregate_type: String,
            projection: String,
            aggregate_revision: String,
            id: String,
        ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
            self.0
                .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
                .await
        }
        async fn save_snapshot(
            &self,
            tenant: String,
            aggregate_type: String,
            projection: String,
            aggregate_revision: String,
            id: String,
            data: Vec<u8>,
            cursor: Value,
        ) -> anyhow::Result<()> {
            self.0
                .save_snapshot(
                    tenant,
                    aggregate_type,
                    projection,
                    aggregate_revision,
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
            self.0
                .delete_snapshot(tenant, aggregate_type, projection, id)
                .await
        }
    }

    fn event(id: &str, version: u16) -> Event {
        Event {
            id: Ulid::generate(),
            aggregate_id: id.to_owned(),
            aggregate_type: "test/Group".to_owned(),
            version,
            name: "Opened".to_owned(),
            routing_key: None,
            tenant: String::new(),
            data: vec![],
            metadata: Default::default(),
            timestamp: 1_000 + u64::from(version),
            timestamp_subsec: 0,
        }
    }

    async fn group_ids(group: &EventoGroup) -> Vec<Ulid> {
        group
            .read(None, None, TenantFilter::All, Args::forward(10, None), None)
            .await
            .unwrap()
            .edges
            .into_iter()
            .map(|e| e.node.id)
            .collect()
    }

    /// The group's reads merge every child, so a delete through the group
    /// must reach every child: a stream held only by the second one is gone
    /// from the merged read afterwards, and other streams are untouched.
    #[tokio::test]
    async fn delete_stream_reaches_every_child() {
        let first = MemoryStore::holding(vec![event("kept", 1)]);
        let second =
            MemoryStore::holding(vec![event("gone", 1), event("gone", 2), event("kept", 2)]);
        let group = EventoGroup::default()
            .executor(Evento::new(first.clone()))
            .executor(Evento::new(second.clone()));
        assert_eq!(group_ids(&group).await.len(), 4);

        group
            .delete_stream(String::new(), "test/Group".to_owned(), "gone".to_owned())
            .await
            .unwrap();

        let kept: Vec<Ulid> = first.ids().into_iter().chain(second.ids()).collect();
        assert_eq!(kept.len(), 2, "only the `kept` stream survives");
        let mut seen = group_ids(&group).await;
        seen.sort();
        let mut kept = kept;
        kept.sort();
        assert_eq!(seen, kept, "the merged read no longer sees `gone`");
    }

    /// One child refusing the delete does not leave the others holding the
    /// stream: every child is asked, and the refusal is still reported.
    #[tokio::test]
    async fn a_refusing_child_does_not_shield_the_others() {
        let refusing = MemoryStore::holding(vec![event("gone", 1)]);
        let willing = MemoryStore::holding(vec![event("gone", 1)]);
        let group = EventoGroup::default()
            .executor(Evento::new(Refusing(refusing.clone())))
            .executor(Evento::new(willing.clone()));

        let err = group
            .delete_stream(String::new(), "test/Group".to_owned(), "gone".to_owned())
            .await
            .expect_err("the refusing child's error is reported");
        assert!(err.to_string().contains("not supported"), "{err}");

        assert_eq!(refusing.ids().len(), 1, "the refusing child keeps its copy");
        assert!(willing.ids().is_empty(), "the willing child still deleted");
    }
}

/// Read-write split executor (requires `rw` feature).
///
/// Separates read and write operations to different executors.
/// Useful for CQRS patterns where read and write databases differ.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{Executor, Rw};
/// # fn wire<R: Executor, W: Executor>(read_executor: R, write_executor: W) {
/// let rw: Rw<R, W> = (read_executor, write_executor).into();
/// # let _ = rw;
/// # }
/// ```
#[cfg(feature = "rw")]
pub struct Rw<R: Executor, W: Executor> {
    r: R,
    w: W,
}

#[cfg(feature = "rw")]
impl<R: Executor + Clone, W: Executor + Clone> Clone for Rw<R, W> {
    fn clone(&self) -> Self {
        Self {
            r: self.r.clone(),
            w: self.w.clone(),
        }
    }
}

#[cfg(feature = "rw")]
#[async_trait::async_trait]
impl<R: Executor, W: Executor> Executor for Rw<R, W> {
    fn default_routing_key(&self) -> Option<&str> {
        self.w.default_routing_key()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.w.write(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.w.replicate(events).await
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        // Writes go through `w`, so its channel is the one that fires; `r`
        // never observes writes and its watch would leave subscriptions on
        // pure polling.
        self.w.write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // The watermark gates what a subscription reads, and reads come from
        // `r`. With a genuine read replica the backend's margin must also
        // cover replication lag — see `Sql::stable_margin`.
        self.r.stable_timestamp().await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.r
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.r
            .latest_timestamp(aggregators, routing_key, tenant)
            .await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.r.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.r.is_subscriber_running(key, worker_id).await
    }

    // Forwarded to the same sides the constituent calls already use (reads
    // from `r`), so the backends' single-round-trip overrides are reached.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.r.subscriber_status(key, worker_id).await
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.r
            .latest_version(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.r
            .stream_routing_key(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.w.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.w.acknowledge(key, worker_id, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.r
            .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.w
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregate_revision,
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
        self.w
            .delete_snapshot(tenant, aggregate_type, projection, id)
            .await
    }

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        self.w
            .delete_stream(tenant, aggregate_type, aggregate_id)
            .await
    }
}

#[cfg(feature = "rw")]
impl<R: Executor, W: Executor> From<(R, W)> for Rw<R, W> {
    fn from((r, w): (R, W)) -> Self {
        Self { r, w }
    }
}

/// Keeps events on one executor and a subscription's bookkeeping on another.
///
/// Events are written to and read from `events` — typically a remote store
/// shared by many processes — while subscriber cursors, worker fences and
/// projection snapshots live in `local`, a store this process owns (an
/// embedded Fjall directory, a SQLite file). A replica of a client application
/// can then follow a shared store with its own cursor: several replicas never
/// fence each other out of one subscription key, nothing accumulates on the
/// shared store, and a restart resumes from the local cursor.
///
/// `local` must belong to exactly one process: two processes sharing it would
/// share cursors again. Cursor keys are still prefixed by the routing key of
/// `events` (`{routing_key}.{name}`), so one local store may follow several
/// event stores with distinct routing keys.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{Executor, Sidecar};
/// # fn wire<E: Executor, L: Executor>(events: E, local: L) {
/// let sidecar: Sidecar<E, L> = (events, local).into();
/// # let _ = sidecar;
/// # }
/// ```
#[cfg(feature = "rw")]
pub struct Sidecar<E: Executor, L: Executor> {
    events: E,
    local: L,
}

#[cfg(feature = "rw")]
impl<E: Executor, L: Executor> Sidecar<E, L> {
    /// Pairs the store of the events with the store of the bookkeeping.
    pub fn new(events: E, local: L) -> Self {
        Self { events, local }
    }

    /// The executor holding the events.
    pub fn events(&self) -> &E {
        &self.events
    }

    /// The executor holding the subscriber cursors and the snapshots.
    pub fn local(&self) -> &L {
        &self.local
    }
}

#[cfg(feature = "rw")]
impl<E: Executor + Clone, L: Executor + Clone> Clone for Sidecar<E, L> {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            local: self.local.clone(),
        }
    }
}

#[cfg(feature = "rw")]
#[async_trait::async_trait]
impl<E: Executor, L: Executor> Executor for Sidecar<E, L> {
    fn default_routing_key(&self) -> Option<&str> {
        // The events' key: it scopes new aggregates and prefixes the cursor
        // keys, so a local store following two event stores keeps them apart.
        self.events.default_routing_key()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.events.write(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.events.replicate(events).await
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        // Writes land on `events`; a remote client forwards the server's
        // notifications through this channel.
        self.events.write_watch()
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // The watermark gates what a subscription reads, and reads come from
        // `events`.
        self.events.stable_timestamp().await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.events
            .read(aggregators, routing_key, tenant, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.events
            .latest_timestamp(aggregators, routing_key, tenant)
            .await
    }

    // Forwarded explicitly so the backends' indexed overrides are reached
    // instead of the trait defaults paging through `read`.
    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.events
            .latest_version(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.events
            .stream_routing_key(tenant, aggregate_type, aggregate_id)
            .await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.local.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.local.is_subscriber_running(key, worker_id).await
    }

    // One local round trip instead of the default's two.
    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.local.subscriber_status(key, worker_id).await
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
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.local
            .get_snapshot(tenant, aggregate_type, projection, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.local
            .save_snapshot(
                tenant,
                aggregate_type,
                projection,
                aggregate_revision,
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

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        self.events
            .delete_stream(tenant, aggregate_type, aggregate_id)
            .await
    }
}

#[cfg(feature = "rw")]
impl<E: Executor, L: Executor> From<(E, L)> for Sidecar<E, L> {
    fn from((events, local): (E, L)) -> Self {
        Self { events, local }
    }
}
