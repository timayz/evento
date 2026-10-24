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
/// always names its tenant; only a scan across aggregates — a subscription
/// that opted into [`all_tenants`](crate::subscription::SubscriptionBuilder::all_tenants)
/// — reads every tenant.
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

/// A tenant id an executor cannot be scoped to (see [`Evento::tenant`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TenantError {
    /// The empty tenant is the default one, which unscoped callers act in.
    #[error("a tenant id must not be empty: the empty tenant is the default one")]
    Empty,
    /// Longer than [`MAX_TENANT_LEN`] bytes.
    #[error("a tenant id of {0} bytes is longer than the {MAX_TENANT_LEN} a store keeps")]
    TooLong(usize),
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

    /// The tenant this executor is scoped to, if any.
    ///
    /// Most backends return `None`: unscoped, they write, load and checkpoint
    /// in whatever tenant a call names — the default tenant (`""`) unless a
    /// builder says otherwise — and a subscription opting into
    /// [`all_tenants`](crate::subscription::SubscriptionBuilder::all_tenants)
    /// reads every tenant. [`Evento::tenant`] returns a scoped executor, which
    /// confines every call to its tenant.
    fn tenant(&self) -> Option<&str> {
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
    ///
    /// A subscriber row is keyed by `(tenant, key)`: the same key in two
    /// tenants names two subscriptions.
    async fn get_subscriber_cursor(
        &self,
        tenant: String,
        key: String,
    ) -> anyhow::Result<Option<Value>>;

    /// Checks if a subscription is running with the given worker ID.
    async fn is_subscriber_running(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<bool>;

    /// Fetches subscriber fencing state and cursor together.
    ///
    /// The subscription loop calls this once per pass; backends should
    /// override it to answer from a single query instead of the default's two
    /// round trips. The cursor is only meaningful while `running` is true, so
    /// the default skips fetching it for a fenced-out worker.
    async fn subscriber_status(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        if !self
            .is_subscriber_running(tenant.clone(), key.clone(), worker_id)
            .await?
        {
            return Ok(SubscriberStatus {
                running: false,
                cursor: None,
            });
        }
        let cursor = self.get_subscriber_cursor(tenant, key).await?;
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
    async fn upsert_subscriber(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<()>;

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
        tenant: String,
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
///
/// # Tenants
///
/// [`tenant`](Evento::tenant) scopes the wrapper to one tenant. Every call
/// through it then acts in that tenant, whatever its arguments say, so a
/// scoped `Evento` can be handed to an untrusted caller — an `evento-remote`
/// session, for instance — that must reach only its own tenant's events,
/// snapshots and subscriptions.
pub struct Evento {
    inner: Arc<Box<dyn Executor>>,
    default_routing_key: Option<String>,
    tenant: Option<String>,
}

impl Clone for Evento {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            default_routing_key: self.default_routing_key.clone(),
            tenant: self.tenant.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Executor for Evento {
    fn default_routing_key(&self) -> Option<&str> {
        self.default_routing_key.as_deref()
    }

    fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        // The default routing key is applied by `WriteBuilder::commit` (which
        // consults `default_routing_key()` only for a brand-new aggregate, so
        // an existing stream keeps its original key even when that key is
        // `None`). Filling it in here would split such a stream across two
        // routing keys.
        let events = self.scope_events(events)?;
        self.inner.write(events).await
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        // `replicate` keeps the caller's timestamps and skips the version
        // contiguity check: a scoped caller could backdate events behind a
        // subscription's cursor. Only a trusted replication layer, unscoped,
        // may replicate.
        if self.tenant.is_some() {
            return Err(WriteError::ReplicateRefused);
        }
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
            .read(
                aggregators,
                routing_key,
                self.scope_filter(tenant),
                args,
                to_micros,
            )
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        self.inner
            .latest_timestamp(aggregators, routing_key, self.scope_filter(tenant))
            .await
    }

    async fn get_subscriber_cursor(
        &self,
        tenant: String,
        key: String,
    ) -> anyhow::Result<Option<Value>> {
        self.inner
            .get_subscriber_cursor(self.scope(tenant), key)
            .await
    }

    async fn is_subscriber_running(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<bool> {
        self.inner
            .is_subscriber_running(self.scope(tenant), key, worker_id)
            .await
    }

    // Explicitly forwarded (not left to the trait default) so the inner
    // backend's single-round-trip overrides are reached through the wrapper.
    async fn subscriber_status(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.inner
            .subscriber_status(self.scope(tenant), key, worker_id)
            .await
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.inner
            .latest_version(self.scope(tenant), aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.inner
            .stream_routing_key(self.scope(tenant), aggregate_type, aggregate_id)
            .await
    }

    async fn upsert_subscriber(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<()> {
        self.inner
            .upsert_subscriber(self.scope(tenant), key, worker_id)
            .await
    }

    async fn acknowledge(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.inner
            .acknowledge(self.scope(tenant), key, worker_id, cursor, lag)
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
        self.inner
            .get_snapshot(
                self.scope(tenant),
                aggregate_type,
                projection,
                aggregate_revision,
                id,
            )
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
                self.scope(tenant),
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
            .delete_snapshot(self.scope(tenant), aggregate_type, projection, id)
            .await
    }
}

impl Evento {
    /// Creates a new type-erased executor wrapper.
    pub fn new<E: Executor>(executor: E) -> Self {
        Self {
            inner: Arc::new(Box::new(executor)),
            default_routing_key: None,
            tenant: None,
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

    /// Scopes this executor to `tenant`.
    ///
    /// Every call through the scoped executor acts in `tenant`, whatever its
    /// arguments say: reads see only `tenant`'s events (a read of every tenant
    /// is narrowed to it), loads, snapshots and subscription cursors resolve in
    /// it, and a write stamps `tenant` on events that name none and refuses
    /// one that names another ([`WriteError::ForeignTenant`]). `replicate` is
    /// refused ([`WriteError::ReplicateRefused`]): it trusts the caller's
    /// timestamps and versions. A scoped `Evento` is therefore what to hand an
    /// untrusted caller confined to one tenant — an `evento-remote` session,
    /// for instance — while the store stays shared by every tenant.
    ///
    /// Scoping an already-scoped executor rescopes it.
    ///
    /// # Errors
    ///
    /// [`TenantError::Empty`] for `""` — the default tenant, which unscoped
    /// callers act in — and [`TenantError::TooLong`] past
    /// [`MAX_TENANT_LEN`] bytes.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Result<Self, TenantError> {
        let tenant = tenant.into();
        if tenant.is_empty() {
            return Err(TenantError::Empty);
        }
        if tenant.len() > MAX_TENANT_LEN {
            return Err(TenantError::TooLong(tenant.len()));
        }
        self.tenant = Some(tenant);
        Ok(self)
    }

    /// The tenant a call acts in: this executor's when scoped, else the
    /// caller's.
    fn scope(&self, tenant: String) -> String {
        match &self.tenant {
            Some(scoped) => scoped.clone(),
            None => tenant,
        }
    }

    /// The tenants a scan covers: this executor's alone when scoped.
    fn scope_filter(&self, tenant: TenantFilter) -> TenantFilter {
        match &self.tenant {
            Some(scoped) => TenantFilter::Only(scoped.clone()),
            None => tenant,
        }
    }

    /// Stamps this executor's tenant on events that name none, and refuses
    /// any that names another.
    fn scope_events(&self, mut events: Vec<Event>) -> Result<Vec<Event>, WriteError> {
        let Some(scoped) = &self.tenant else {
            return Ok(events);
        };
        for event in &mut events {
            if event.tenant.is_empty() {
                event.tenant = scoped.clone();
            } else if &event.tenant != scoped {
                return Err(WriteError::ForeignTenant {
                    expected: scoped.clone(),
                    found: event.tenant.clone(),
                });
            }
        }
        Ok(events)
    }
}

/// Multi-executor aggregation (requires `group` feature).
///
/// `EventoGroup` combines multiple executors into one. Reads query all executors
/// and merge results; writes go only to the first executor.
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

    fn tenant(&self) -> Option<&str> {
        self.first().tenant()
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

    async fn get_subscriber_cursor(
        &self,
        tenant: String,
        key: String,
    ) -> anyhow::Result<Option<Value>> {
        self.first().get_subscriber_cursor(tenant, key).await
    }

    async fn is_subscriber_running(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<bool> {
        self.first()
            .is_subscriber_running(tenant, key, worker_id)
            .await
    }

    // Subscriber state lives on the first executor. `latest_version` and
    // `stream_routing_key` intentionally keep the trait defaults: those go
    // through `self.read`, which merges all children — forwarding to
    // `first()` would miss events held by the other executors.
    async fn subscriber_status(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.first().subscriber_status(tenant, key, worker_id).await
    }

    async fn upsert_subscriber(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<()> {
        self.first().upsert_subscriber(tenant, key, worker_id).await
    }

    async fn acknowledge(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.first()
            .acknowledge(tenant, key, worker_id, cursor, lag)
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

    fn tenant(&self) -> Option<&str> {
        self.w.tenant()
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

    async fn get_subscriber_cursor(
        &self,
        tenant: String,
        key: String,
    ) -> anyhow::Result<Option<Value>> {
        self.r.get_subscriber_cursor(tenant, key).await
    }

    async fn is_subscriber_running(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<bool> {
        self.r.is_subscriber_running(tenant, key, worker_id).await
    }

    // Forwarded to the same sides the constituent calls already use (reads
    // from `r`), so the backends' single-round-trip overrides are reached.
    async fn subscriber_status(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.r.subscriber_status(tenant, key, worker_id).await
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

    async fn upsert_subscriber(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<()> {
        self.w.upsert_subscriber(tenant, key, worker_id).await
    }

    async fn acknowledge(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.w
            .acknowledge(tenant, key, worker_id, cursor, lag)
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
/// `events` (`{routing_key}.{name}`), and cursors and snapshots are kept under
/// the tenant of `events`, so one local store may follow several event stores
/// with distinct routing keys or tenants.
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

    fn tenant(&self) -> Option<&str> {
        // The events' tenant: it scopes what is read and written, and the
        // subscriber rows and snapshots kept in `local` are keyed by it.
        self.events.tenant()
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

    async fn get_subscriber_cursor(
        &self,
        tenant: String,
        key: String,
    ) -> anyhow::Result<Option<Value>> {
        self.local.get_subscriber_cursor(tenant, key).await
    }

    async fn is_subscriber_running(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<bool> {
        self.local
            .is_subscriber_running(tenant, key, worker_id)
            .await
    }

    // One local round trip instead of the default's two.
    async fn subscriber_status(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        self.local.subscriber_status(tenant, key, worker_id).await
    }

    async fn upsert_subscriber(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<()> {
        self.local.upsert_subscriber(tenant, key, worker_id).await
    }

    async fn acknowledge(
        &self,
        tenant: String,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        self.local
            .acknowledge(tenant, key, worker_id, cursor, lag)
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
}

#[cfg(feature = "rw")]
impl<E: Executor, L: Executor> From<(E, L)> for Sidecar<E, L> {
    fn from((events, local): (E, L)) -> Self {
        Self { events, local }
    }
}
