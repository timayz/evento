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
//! - [`ReadAggregator`] - Query filter for reading events

use std::{hash::Hash, sync::Arc};
use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    Event, RoutingKey, WriteError,
};

/// Filter for querying events by aggregator.
///
/// Use the constructor methods to create filters:
///
/// # Example
///
/// ```rust,ignore
/// // All events for an aggregator type
/// let filter = ReadAggregator::aggregator("myapp/User");
///
/// // Events for a specific aggregate instance
/// let filter = ReadAggregator::id("myapp/User", "user-123");
///
/// // Events of a specific type
/// let filter = ReadAggregator::event("myapp/User", "UserCreated");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ReadAggregator {
    /// Aggregator type (e.g., "myapp/User")
    pub aggregator_type: String,
    /// Optional specific aggregate ID
    pub aggregator_id: Option<String>,
    /// Optional event name filter
    pub name: Option<String>,
}

impl ReadAggregator {
    /// Creates a filter with all fields specified.
    ///
    /// Filters events by aggregator type, specific aggregate ID, and event name.
    pub fn new(
        aggregator_type: impl Into<String>,
        id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: Some(id.into()),
            name: Some(name.into()),
        }
    }

    /// Creates a filter for all events of an aggregator type.
    ///
    /// Returns all events regardless of aggregate ID or event name.
    pub fn aggregator(value: impl Into<String>) -> Self {
        Self {
            aggregator_type: value.into(),
            aggregator_id: None,
            name: None,
        }
    }

    /// Creates a filter for a specific aggregate instance.
    ///
    /// Returns all events for the given aggregator type and ID.
    pub fn id(aggregator_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: Some(id.into()),
            name: None,
        }
    }

    /// Creates a filter for a specific event type.
    ///
    /// Returns all events of the given name for an aggregator type.
    pub fn event(aggregator_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: None,
            name: Some(name.into()),
        }
    }
}

impl Hash for ReadAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.aggregator_type.hash(state);
        self.aggregator_id.hash(state);
        self.name.hash(state);
    }
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
/// - `get_subscriber_cursor` - Get subscription position
/// - `is_subscriber_running` - Check if subscription is active
/// - `upsert_subscriber` - Create/update subscription
/// - `acknowledge` - Update subscription cursor
#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    /// Persists events atomically.
    ///
    /// Returns `WriteError::InvalidOriginalVersion` if version conflicts occur.
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;

    /// Gets the current cursor position for a subscription.
    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>>;

    /// Checks if a subscription is running with the given worker ID.
    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool>;

    /// Creates or updates a subscription record.
    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()>;

    /// Updates subscription cursor after processing events.
    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()>;

    /// Queries events with filtering and pagination.
    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>>;

    /// Retrieves a stored snapshot for an aggregate.
    ///
    /// Returns the serialized snapshot data and cursor position, or `None`
    /// if no snapshot exists for the given aggregate.
    async fn get_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>>;

    /// Stores a snapshot for an aggregate.
    ///
    /// Snapshots cache aggregate state to avoid replaying all events.
    /// The `cursor` indicates the event position up to which the snapshot is valid.
    async fn save_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()>;
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
pub struct Evento(Arc<Box<dyn Executor>>);

impl Clone for Evento {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait::async_trait]
impl Executor for Evento {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.0.write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.0.read(aggregators, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.0.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.0.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.0.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.0.acknowledge(key, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.0.get_snapshot(revision, id).await
    }

    async fn save_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.0.save_snapshot(revision, id, data, cursor).await
    }
}

impl Evento {
    /// Creates a new type-erased executor wrapper.
    pub fn new<E: Executor>(executor: E) -> Self {
        Self(Arc::new(Box::new(executor)))
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
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.first().write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        use crate::cursor;
        let futures = self
            .executors
            .iter()
            .map(|e| e.read(aggregators.to_owned(), routing_key.to_owned(), args.clone()));

        let results = futures_util::future::join_all(futures).await;
        let mut events = vec![];
        for res in results {
            for edge in res?.edges {
                events.push(edge.node);
            }
        }

        Ok(cursor::Reader::new(events).args(args).execute()?)
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.first().get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.first().is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.first().upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.first().acknowledge(key, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.first().get_snapshot(revision, id).await
    }

    async fn save_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.first().save_snapshot(revision, id, data, cursor).await
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
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.w.write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.r.read(aggregators, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.r.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.r.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.w.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.w.acknowledge(key, cursor, lag).await
    }

    async fn get_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        self.r.get_snapshot(revision, id).await
    }

    async fn save_snapshot(
        &self,
        revision: String,
        id: Vec<u8>,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        self.w.save_snapshot(revision, id, data, cursor).await
    }
}

#[cfg(feature = "rw")]
impl<R: Executor, W: Executor> From<(R, W)> for Rw<R, W> {
    fn from((r, w): (R, W)) -> Self {
        Self { r, w }
    }
}
