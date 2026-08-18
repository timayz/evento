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

use std::{hash::Hash, sync::Arc};
use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    Event, RoutingKey, WriteError,
};

/// Filter for querying events by aggregate.
///
/// Use the constructor methods to create filters:
///
/// # Example
///
/// ```rust,ignore
/// // All events for an aggregate type
/// let filter = EventFilter::by_type("myapp/User");
///
/// // Events for a specific aggregate instance
/// let filter = EventFilter::by_id("myapp/User", "user-123");
///
/// // Events of a specific type
/// let filter = EventFilter::by_event("myapp/User", "UserCreated");
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
    /// Creates a filter with all fields specified.
    ///
    /// Filters events by aggregate type, specific aggregate ID, and event name.
    pub fn exact(
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
    /// Returns all events regardless of aggregate ID or event name.
    pub fn by_type(value: impl Into<String>) -> Self {
        Self {
            aggregate_type: value.into(),
            aggregate_id: None,
            name: None,
        }
    }

    /// Creates a filter for a specific aggregate instance.
    ///
    /// Returns all events for the given aggregate type and ID.
    pub fn by_id(aggregate_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: Some(id.into()),
            name: None,
        }
    }

    /// Creates a filter for a specific event type.
    ///
    /// Returns all events of the given name for an aggregate type.
    pub fn by_event(aggregate_type: impl Into<String>, name: impl Into<String>) -> Self {
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
/// Implementations handle persisting events, querying, and managing subscriptions.
/// The main implementation is [`evento_sql::Sql`](../evento_sql/struct.Sql.html).
///
/// # Methods
///
/// - `write` - Persist events atomically
/// - `read` - Query events with filtering and pagination
/// - `latest_timestamp` - Get the timestamp of the most recent matching event
/// - `get_subscriber_cursor` - Get subscription position
/// - `is_subscriber_running` - Check if subscription is active
/// - `upsert_subscriber` - Create/update subscription
/// - `acknowledge` - Update subscription cursor
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
    /// has not called `.routing_key()` or `.all()`.
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
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        const PAGE_SIZE: u16 = 4096;
        let filters: Arc<[EventFilter]> =
            Arc::from([EventFilter::by_id(aggregate_type, aggregate_id)]);
        let mut max = 0u16;
        let mut after = None;
        loop {
            let result = self
                .read(
                    Some(filters.clone()),
                    None,
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
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        let result = self
            .read(
                Some(Arc::from([EventFilter::by_id(
                    aggregate_type,
                    aggregate_id,
                )])),
                None,
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
    ) -> anyhow::Result<u64>;

    /// Retrieves a stored snapshot for an aggregate.
    ///
    /// Returns the serialized snapshot data and cursor position, or `None`
    /// if no snapshot exists for the given aggregate.
    async fn get_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>>;

    /// Stores a snapshot for an aggregate.
    ///
    /// Snapshots cache aggregate state to avoid replaying all events.
    /// The `cursor` indicates the event position up to which the snapshot is valid.
    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()>;

    /// Deletes a stored snapshot for an aggregate.
    ///
    /// Idempotent: deleting a snapshot that does not exist is not an error.
    /// Revision is intentionally omitted — `save_snapshot` upserts on
    /// `(type, id)` so there is only ever one row per aggregate.
    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()>;
}

/// Type-erased wrapper around any [`Executor`] implementation.
///
/// `Evento` wraps an executor in `Arc<Box<dyn Executor>>` for dynamic dispatch.
/// This allows storing different executor implementations in the same collection.
///
/// # Example
///
/// ```rust,ignore
/// let sql_executor: Sql<sqlx::Sqlite> = pool.into();
/// let evento = Evento::new(sql_executor);
///
/// // Use like any executor
/// evento.write(events).await?;
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
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.inner
            .read(aggregators, routing_key, args, to_micros)
            .await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        self.inner.latest_timestamp(aggregators, routing_key).await
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
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.inner
            .latest_version(aggregate_type, aggregate_id)
            .await
    }

    async fn stream_routing_key(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.inner
            .stream_routing_key(aggregate_type, aggregate_id)
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
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.inner
            .get_snapshot(aggregate_type, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.inner
            .save_snapshot(aggregate_type, aggregate_revision, id, data, cursor)
            .await
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        self.inner.delete_snapshot(aggregate_type, id).await
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
    /// `.routing_key()` or `.all()`.
    pub fn default_routing_key(mut self, key: impl Into<String>) -> Self {
        self.default_routing_key = Some(key.into());
        self
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
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        use crate::cursor;
        let futures = self.executors.iter().map(|e| {
            e.read(
                aggregators.to_owned(),
                routing_key.to_owned(),
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
    ) -> anyhow::Result<u64> {
        let futures = self
            .executors
            .iter()
            .map(|e| e.latest_timestamp(aggregators.to_owned(), routing_key.to_owned()));

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
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.first()
            .get_snapshot(aggregate_type, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.first()
            .save_snapshot(aggregate_type, aggregate_revision, id, data, cursor)
            .await
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        self.first().delete_snapshot(aggregate_type, id).await
    }
}

/// Read-write split executor (requires `rw` feature).
///
/// Separates read and write operations to different executors.
/// Useful for CQRS patterns where read and write databases differ.
///
/// # Example
///
/// ```rust,ignore
/// let rw: Rw<ReadReplica, Primary> = (read_executor, write_executor).into();
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
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.r.read(aggregators, routing_key, args, to_micros).await
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        self.r.latest_timestamp(aggregators, routing_key).await
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
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        self.r.latest_version(aggregate_type, aggregate_id).await
    }

    async fn stream_routing_key(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.r
            .stream_routing_key(aggregate_type, aggregate_id)
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
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.r
            .get_snapshot(aggregate_type, aggregate_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.w
            .save_snapshot(aggregate_type, aggregate_revision, id, data, cursor)
            .await
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        self.w.delete_snapshot(aggregate_type, id).await
    }
}

#[cfg(feature = "rw")]
impl<R: Executor, W: Executor> From<(R, W)> for Rw<R, W> {
    fn from((r, w): (R, W)) -> Self {
        Self { r, w }
    }
}
