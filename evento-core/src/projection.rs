//! Projections and event subscriptions.
//!
//! This module provides the core building blocks for event sourcing:
//! - Projections that build read models from events
//! - Subscriptions that continuously process events
//! - Loading aggregate state from event streams
//!
//! # Key Types
//!
//! - [`Projection`] - Defines handlers for building projections
//! - [`LoadBuilder`] - Loads aggregate state from events
//! - [`ProjectionSubscription`] - Starts a subscription that keeps a projection up to date
//! - [`SubscriptionBuilder`](crate::subscription::SubscriptionBuilder) - Builds continuous event subscriptions
//! - [`Subscription`](crate::subscription::Subscription) - Handle to a running subscription
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::projection::Projection;
//!
//! // Define a projection with event handlers (no id at construction)
//! let projection = Projection::<_, AccountView>::new::<Account>()
//!     .handler(account_opened())
//!     .handler(money_deposited());
//!
//! // Load aggregate state for one id
//! let result = projection
//!     .load("account-123")
//!     .execute(&executor)
//!     .await?;
//!
//! // Or keep the projection auto-updated via a subscription
//! let subscription = Projection::<_, AccountView>::new::<Account>()
//!     .handler(account_opened())
//!     .handler(money_deposited())
//!     .subscription("account-view")
//!     .start(&executor)
//!     .await?;
//! ```

use std::{
    collections::HashMap, future::Future, marker::PhantomData, ops::Deref, pin::Pin, sync::Arc,
    time::Duration,
};

use crate::{
    context,
    cursor::{self, Args, Cursor},
    subscription::{self, RoutingKey, Subscription, SubscriptionBuilder},
    Aggregator, AggregatorBuilder, AggregatorEvent, Executor, ReadAggregator,
};

/// Handler context providing access to executor and shared data.
///
/// `Context` wraps an [`RwContext`](crate::context::RwContext) for type-safe
/// data storage and provides access to the executor for database operations.
#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    context: context::RwContext,
    /// Reference to the executor for database operations
    pub executor: &'a E,
    pub id: String,
    revision: u16,
    aggregator_type: String,
    aggregators: &'a HashMap<String, String>,
}

impl<'a, E: Executor> Context<'a, E> {
    /// Retrieves a stored snapshot for the given ID.
    ///
    /// Returns `None` if no snapshot exists.
    pub async fn get_snapshot<D: bitcode::DecodeOwned + ProjectionCursor>(
        &self,
    ) -> anyhow::Result<Option<D>> {
        let Some((data, cursor)) = self
            .executor
            .get_snapshot(
                self.aggregator_type.to_owned(),
                self.revision.to_string(),
                self.id.to_owned(),
            )
            .await?
        else {
            return Ok(None);
        };

        let mut data: D = bitcode::decode(&data)?;
        data.set_cursor(&cursor);

        Ok(Some(data))
    }

    /// Stores a snapshot for the given ID.
    ///
    /// The snapshot cursor is extracted from the data to track the event position.
    pub async fn take_snapshot<D: bitcode::Encode + ProjectionCursor>(
        &self,
        data: &D,
    ) -> anyhow::Result<()> {
        let cursor = data.get_cursor();
        let data = bitcode::encode(data);

        self.executor
            .save_snapshot(
                self.aggregator_type.to_owned(),
                self.revision.to_string(),
                self.id.to_owned(),
                data,
                cursor,
            )
            .await
    }

    /// Deletes the stored snapshot for the given ID.
    ///
    /// Idempotent: no error if no snapshot exists.
    pub async fn drop_snapshot(&self) -> anyhow::Result<()> {
        self.executor
            .delete_snapshot(self.aggregator_type.to_owned(), self.id.to_owned())
            .await
    }

    /// Returns the aggregate ID for a registered aggregator type.
    ///
    /// # Panics
    ///
    /// Panics if the aggregator type was not registered via [`LoadBuilder::aggregator`].
    pub async fn aggregator<A: Aggregator>(&self) -> String {
        tracing::debug!(
            "Failed to get `Aggregator id <{}>` For the Aggregator id extractor to work \
        correctly, register the related aggregator with `.aggregator::<MyAggregator>(id)` on \
        the load builder. Ensure that types align in both the set and retrieve calls.",
            A::aggregator_type()
        );

        self.aggregators
            .get(A::aggregator_type())
            .expect("Projection Aggregator not configured correctly. View/enable debug logs for more details.")
            .to_owned()
    }
}

impl<'a, E: Executor> Deref for Context<'a, E> {
    type Target = context::RwContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

/// Trait for event handlers.
///
/// Handlers process events in two modes:
/// - `handle`: For subscriptions that perform side effects (send emails, update read models)
/// - `apply`: For loading aggregate state by replaying events
///
/// This trait is typically implemented via the `#[evento::handler]` macro.
pub trait Handler<P: 'static>: Sync + Send {
    /// Applies an event to build projection state.
    ///
    /// This is called when loading aggregate state by replaying events.
    /// It should be a pure function that modifies the projection without side effects.
    fn handle<'a>(
        &'a self,
        projection: &'a mut P,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Returns the aggregator type this handler processes.
    fn aggregator_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

/// Trait for types that track their cursor position in the event stream.
///
/// This trait is typically derived using the `#[evento::projection]` macro.
pub trait ProjectionCursor {
    /// Returns the current cursor position.
    fn get_cursor(&self) -> cursor::Value;
    /// Sets the cursor position.
    fn set_cursor(&mut self, v: &cursor::Value);
}

/// Trait for projections that can create an [`AggregatorBuilder`].
///
/// Extends [`ProjectionCursor`] to provide aggregate identity and versioning,
/// enabling projections to emit new events.
pub trait ProjectionAggregator: ProjectionCursor {
    /// Returns the aggregate ID for this projection.
    ///
    /// # Panics
    ///
    /// Default implementation panics; must be overridden.
    fn aggregator_id(&self) -> String {
        todo!("ProjectionCursor.aggregator_id must be implemented for ProjectionCursor.aggregator")
    }

    /// Returns the current aggregate version from the cursor.
    ///
    /// Returns `0` if no cursor is set.
    fn aggregator_version(&self) -> anyhow::Result<u16> {
        let value = self.get_cursor();
        if value == Default::default() {
            return Ok(0);
        }

        let cursor = crate::Event::deserialize_cursor(&value)?;

        Ok(cursor.v)
    }

    /// Creates an [`AggregatorBuilder`] pre-configured with ID and version.
    ///
    /// Use this to emit new events from a projection.
    fn aggregator(&self) -> anyhow::Result<AggregatorBuilder> {
        Ok(AggregatorBuilder::new(self.aggregator_id())
            .original_version(self.aggregator_version()?)
            .to_owned())
    }
}

/// Trait for types that can be restored from snapshots.
///
/// Snapshots provide a performance optimization by storing pre-computed
/// state, avoiding the need to replay all events from the beginning.
///
/// This trait is typically implemented via the `#[evento::snapshot]` macro.
pub trait Snapshot<E: Executor>: ProjectionCursor + Sized {
    /// Restores state from a snapshot if available.
    ///
    /// Returns `None` if no snapshot exists for the given ID.
    fn restore(
        _context: &Context<'_, E>,
    ) -> impl Future<Output = anyhow::Result<Option<Self>>> + Send {
        Box::pin(async { Ok(None) })
    }

    /// Stores the current state as a snapshot.
    ///
    /// Default implementation does nothing.
    fn take_snapshot(
        &self,
        _context: &Context<'_, E>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        Box::pin(async { Ok(()) })
    }

    /// Drops any stored snapshot for the given aggregate.
    ///
    /// Called by a [`ProjectionSubscription`] when the configured tombstone
    /// event is observed (see [`Projection::tombstone`]). Override this for
    /// projections backed by a custom table to delete the row; the default
    /// is a no-op.
    fn drop_snapshot(_context: &Context<'_, E>) -> impl Future<Output = anyhow::Result<()>> + Send {
        Box::pin(async { Ok(()) })
    }
}

impl<T: bitcode::Encode + bitcode::DecodeOwned + ProjectionCursor + Send + Sync, E: Executor>
    Snapshot<E> for T
{
    async fn restore(context: &Context<'_, E>) -> anyhow::Result<Option<Self>> {
        context.get_snapshot().await
    }

    async fn take_snapshot(&self, context: &Context<'_, E>) -> anyhow::Result<()> {
        context.take_snapshot(self).await
    }

    async fn drop_snapshot(context: &Context<'_, E>) -> anyhow::Result<()> {
        context.drop_snapshot().await
    }
}

/// Projection definition: a set of handlers for a primary aggregator type.
///
/// A `Projection` is constructed without an aggregate id. Terminal operations:
/// - [`Projection::load`] returns a [`LoadBuilder`] bound to a specific aggregate id.
/// - [`Projection::subscription`] returns a [`ProjectionSubscription`] that auto-updates
///   the projection as new events arrive.
///
/// # Example
///
/// ```rust,ignore
/// // Load a single aggregate
/// let result = Projection::<_, AccountView>::new::<Account>()
///     .handler(account_opened())
///     .handler(money_deposited())
///     .data(app_config)
///     .load("account-123")
///     .execute(&executor)
///     .await?;
///
/// // Start a subscription that keeps every aggregate up to date
/// let subscription = Projection::<_, AccountView>::new::<Account>()
///     .handler(account_opened())
///     .handler(money_deposited())
///     .data(app_config)
///     .subscription("account-view")
///     .start(&executor)
///     .await?;
/// ```
pub struct Projection<E: Executor, P: Default + 'static> {
    aggregator_type: &'static str,
    revision: u16,
    handlers: HashMap<String, Box<dyn Handler<P>>>,
    context: context::RwContext,
    safety_disabled: bool,
    tombstone: Option<(&'static str, &'static str)>,
    executor: PhantomData<E>,
}

impl<E: Executor, P: Snapshot<E> + Default + 'static> Projection<E, P> {
    /// Creates a new projection definition for the given primary aggregator type.
    pub fn new<A: Aggregator>() -> Projection<E, P> {
        Projection {
            aggregator_type: A::aggregator_type(),
            context: Default::default(),
            handlers: HashMap::new(),
            safety_disabled: true,
            tombstone: None,
            executor: PhantomData,
            revision: 0,
        }
    }

    /// Sets the snapshot revision.
    ///
    /// Changing the revision invalidates existing snapshots, forcing a full rebuild.
    pub fn revision(mut self, value: u16) -> Self {
        self.revision = value;

        self
    }

    /// Enables safety checks for unhandled events.
    ///
    /// When enabled, execution fails if an event is encountered without a handler.
    pub fn safety_check(mut self) -> Self {
        self.safety_disabled = false;

        self
    }

    /// Declares which event marks an aggregate as deleted (tombstoned).
    ///
    /// When set:
    /// - [`LoadBuilder::execute`] short-circuits and returns `Ok(None)` as soon
    ///   as it sees a committed tombstone event for the requested id, avoiding
    ///   any snapshot read or event replay.
    /// - [`ProjectionSubscription`] routes tombstone events to
    ///   [`Snapshot::drop_snapshot`] so the user can delete their snapshot row.
    pub fn tombstone<EV: AggregatorEvent + Send + Sync + 'static>(mut self) -> Self {
        self.tombstone = Some((EV::aggregator_type(), EV::event_name()));
        self
    }

    /// Registers an event handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<P> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregator_type(), h.event_name());
        if self.handlers.insert(key.to_owned(), Box::new(h)).is_some() {
            panic!("Cannot register event handler: key {} already exists", key);
        }
        self
    }

    /// Registers a skip handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn skip<EV: AggregatorEvent + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<EV>(PhantomData))
    }

    /// Adds shared data to the handler context.
    ///
    /// Data added here is accessible in handlers via the context. Data lives
    /// on the projection definition and is reused for both [`Projection::load`]
    /// and [`Projection::subscription`].
    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        self.context.insert(v);

        self
    }

    /// Returns a [`LoadBuilder`] for loading the aggregate with the given id.
    pub fn load(self, id: impl Into<String>) -> LoadBuilder<E, P> {
        let id = id.into();
        let mut aggregators = HashMap::new();
        aggregators.insert(self.aggregator_type.to_string(), id.to_owned());

        LoadBuilder {
            projection: self,
            id,
            aggregators,
        }
    }

    /// Returns a [`LoadBuilder`] for loading state keyed on multiple aggregate ids.
    ///
    /// The ids are hashed via [`crate::hash_ids`] to produce a stable snapshot id.
    pub fn load_ids(self, ids: Vec<impl Into<String>>) -> LoadBuilder<E, P> {
        self.load(crate::hash_ids(ids))
    }

    /// Returns a builder for a subscription that keeps this projection up to date.
    pub fn subscription(self, key: impl Into<String>) -> ProjectionSubscription<E, P> {
        ProjectionSubscription {
            projection: self,
            key: key.into(),
            routing_key: None,
            chunk_size: 300,
            retry: Some(30),
            delay: None,
            is_accept_failure: false,
        }
    }

    async fn load_aggregator(
        &self,
        executor: &E,
        id: &str,
        extra_aggregators: &HashMap<String, String>,
    ) -> anyhow::Result<Option<P>> {
        if let Some((tombstone_type, tombstone_event)) = self.tombstone {
            let res = executor
                .read(
                    Some(vec![ReadAggregator::new(
                        tombstone_type,
                        id.to_owned(),
                        tombstone_event,
                    )]),
                    None,
                    Args::backward(1, None),
                )
                .await?;
            if !res.edges.is_empty() {
                return Ok(None);
            }
        }

        let mut aggregators = HashMap::with_capacity(extra_aggregators.len() + 1);
        aggregators.insert(self.aggregator_type.to_string(), id.to_owned());
        for (k, v) in extra_aggregators {
            aggregators.insert(k.to_owned(), v.to_owned());
        }

        let context = Context {
            context: self.context.clone(),
            executor,
            id: id.to_owned(),
            aggregator_type: self.aggregator_type.to_string(),
            aggregators: &aggregators,
            revision: self.revision,
        };
        let snapshot = P::restore(&context).await?;
        let cursor = snapshot.as_ref().map(|s| s.get_cursor());

        let read_aggregators = self
            .handlers
            .values()
            .map(|h| match aggregators.get(h.aggregator_type()) {
                Some(id) => ReadAggregator {
                    aggregator_type: h.aggregator_type().to_owned(),
                    aggregator_id: Some(id.to_owned()),
                    name: if self.safety_disabled {
                        Some(h.event_name().to_owned())
                    } else {
                        None
                    },
                },
                _ => {
                    if self.safety_disabled {
                        ReadAggregator::event(h.aggregator_type(), h.event_name())
                    } else {
                        ReadAggregator::aggregator(h.aggregator_type())
                    }
                }
            })
            .collect::<Vec<_>>();

        let events = executor
            .read(
                Some(read_aggregators.to_vec()),
                None,
                Args::forward(100, cursor.clone()),
            )
            .await?;

        if events.edges.is_empty() && snapshot.is_none() {
            return Ok(None);
        }

        let mut snapshot = snapshot.unwrap_or_default();

        for event in events.edges.iter() {
            let key = format!("{}_{}", event.node.aggregator_type, event.node.name);

            let Some(handler) = self.handlers.get(&key) else {
                if !self.safety_disabled {
                    anyhow::bail!("no handler k={key}");
                }

                continue;
            };

            handler.handle(&mut snapshot, &event.node).await?;
        }

        if let Some(event) = events.edges.last() {
            snapshot.set_cursor(&event.cursor);
            snapshot.take_snapshot(&context).await?;
        }

        if events.page_info.has_next_page {
            anyhow::bail!("Too busy");
        }

        Ok(Some(snapshot))
    }
}

/// Builder for loading the projection state of a specific aggregate id.
///
/// Created via [`Projection::load`]. Allows registering related aggregators
/// whose events also feed this projection, then [`LoadBuilder::execute`] runs
/// the load.
pub struct LoadBuilder<E: Executor, P: Default + 'static> {
    projection: Projection<E, P>,
    id: String,
    aggregators: HashMap<String, String>,
}

impl<E: Executor, P: Snapshot<E> + Default + 'static> LoadBuilder<E, P> {
    /// Adds a related aggregate to load events from.
    pub fn aggregator<A: Aggregator>(self, id: impl Into<String>) -> Self {
        self.aggregator_raw(A::aggregator_type().to_owned(), id)
    }

    /// Adds a related aggregate to load events from (raw aggregator type).
    pub fn aggregator_raw(
        mut self,
        aggregator_type: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        self.aggregators.insert(aggregator_type.into(), id.into());

        self
    }

    /// Executes the load, returning the rebuilt state.
    ///
    /// Returns `None` if no events exist for the aggregate, or if a tombstone
    /// was registered via [`Projection::tombstone`] and the corresponding
    /// event has been committed for this id.
    /// Returns `Err` if there are too many events to process in one batch.
    pub async fn execute(&self, executor: &E) -> anyhow::Result<Option<P>> {
        self.projection
            .load_aggregator(executor, &self.id, &self.aggregators)
            .await
    }
}

/// Builder for a subscription that keeps a [`Projection`] auto-updated.
///
/// Created via [`Projection::subscription`]. On each incoming event, the
/// subscription loads the affected aggregate id through the projection,
/// which in turn re-runs handlers and persists the snapshot.
pub struct ProjectionSubscription<E: Executor, P: Default + 'static> {
    projection: Projection<E, P>,
    key: String,
    routing_key: Option<RoutingKey>,
    chunk_size: u16,
    retry: Option<u8>,
    delay: Option<Duration>,
    is_accept_failure: bool,
}

impl<E, P> ProjectionSubscription<E, P>
where
    E: Executor + Clone + 'static,
    P: Snapshot<E> + Default + Send + Sync + 'static,
{
    /// Filters events by routing key.
    ///
    /// Overrides any executor-level default.
    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = Some(RoutingKey::Value(Some(v.into())));
        self
    }

    /// Processes all events regardless of routing key.
    ///
    /// Overrides any executor-level default.
    pub fn all(mut self) -> Self {
        self.routing_key = Some(RoutingKey::All);
        self
    }

    /// Sets the number of events to process per batch (default 300).
    pub fn chunk_size(mut self, v: u16) -> Self {
        self.chunk_size = v;
        self
    }

    /// Sets the maximum number of retries on failure (default 30).
    pub fn retry(mut self, v: u8) -> Self {
        self.retry = Some(v);
        self
    }

    /// Disables retries.
    pub fn no_retry(mut self) -> Self {
        self.retry = None;
        self
    }

    /// Sets a delay before starting the subscription.
    pub fn delay(mut self, v: Duration) -> Self {
        self.delay = Some(v);
        self
    }

    /// Allows the subscription to continue after handler failures.
    pub fn accept_failure(mut self) -> Self {
        self.is_accept_failure = true;
        self
    }

    /// Starts the subscription.
    ///
    /// Returns a [`Subscription`] handle that can be used for graceful shutdown.
    pub async fn start(self, executor: &E) -> anyhow::Result<Subscription> {
        let ProjectionSubscription {
            projection,
            key,
            routing_key,
            chunk_size,
            retry,
            delay,
            is_accept_failure,
        } = self;

        let aggregator_type: &'static str = projection.aggregator_type;
        let tombstone = projection.tombstone;
        // Capture the event names registered for the primary aggregator type.
        // Each event_name is &'static str because Handler::event_name returns
        // &'static str (always derived from the `#[evento::handler]` macro).
        // Drop any handler that collides with the tombstone — that event is
        // routed to ProjectionTombstoneHandler instead.
        let event_names: Vec<&'static str> = projection
            .handlers
            .values()
            .filter(|h| h.aggregator_type() == aggregator_type)
            .filter(|h| {
                tombstone
                    .map(|(t, e)| !(h.aggregator_type() == t && h.event_name() == e))
                    .unwrap_or(true)
            })
            .map(|h| h.event_name())
            .collect();

        let projection = Arc::new(projection);

        let mut builder: SubscriptionBuilder<E> = SubscriptionBuilder::new(key);
        builder = match routing_key {
            Some(RoutingKey::All) => builder.all(),
            Some(RoutingKey::Value(Some(v))) => builder.routing_key(v),
            Some(RoutingKey::Value(None)) | None => builder,
        };
        builder = builder.chunk_size(chunk_size);
        builder = match retry {
            Some(n) => builder.retry(n),
            None => builder,
        };
        if let Some(d) = delay {
            builder = builder.delay(d);
        }
        if is_accept_failure {
            builder = builder.accept_failure();
        }

        for event_name in event_names {
            builder = builder.handler(ProjectionAutoHandler::<E, P> {
                projection: projection.clone(),
                aggregator_type,
                event_name,
                _marker: PhantomData,
            });
        }

        if let Some((tombstone_type, tombstone_event)) = tombstone {
            builder = builder.handler(ProjectionTombstoneHandler::<E, P> {
                projection: projection.clone(),
                aggregator_type: tombstone_type,
                event_name: tombstone_event,
                _marker: PhantomData,
            });
        }

        if retry.is_none() {
            builder.unretry_start(executor).await
        } else {
            builder.start(executor).await
        }
    }
}

struct ProjectionAutoHandler<E: Executor, P: Default + 'static> {
    projection: Arc<Projection<E, P>>,
    aggregator_type: &'static str,
    event_name: &'static str,
    _marker: PhantomData<E>,
}

impl<E, P> subscription::Handler<E> for ProjectionAutoHandler<E, P>
where
    E: Executor,
    P: Snapshot<E> + Default + Send + Sync + 'static,
{
    fn handle<'a>(
        &'a self,
        context: &'a subscription::Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.projection
                .load_aggregator(context.executor, &event.aggregator_id, &HashMap::new())
                .await?;
            Ok(())
        })
    }

    fn aggregator_type(&self) -> &'static str {
        self.aggregator_type
    }

    fn event_name(&self) -> &'static str {
        self.event_name
    }
}

struct ProjectionTombstoneHandler<E: Executor, P: Default + 'static> {
    projection: Arc<Projection<E, P>>,
    aggregator_type: &'static str,
    event_name: &'static str,
    _marker: PhantomData<E>,
}

impl<E, P> subscription::Handler<E> for ProjectionTombstoneHandler<E, P>
where
    E: Executor,
    P: Snapshot<E> + Default + Send + Sync + 'static,
{
    fn handle<'a>(
        &'a self,
        context: &'a subscription::Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let aggregators: HashMap<String, String> = HashMap::new();
            let ctx = Context {
                context: self.projection.context.clone(),
                executor: context.executor,
                id: event.aggregator_id.clone(),
                aggregator_type: self.projection.aggregator_type.to_string(),
                aggregators: &aggregators,
                revision: self.projection.revision,
            };
            P::drop_snapshot(&ctx).await?;
            Ok(())
        })
    }

    fn aggregator_type(&self) -> &'static str {
        self.aggregator_type
    }

    fn event_name(&self) -> &'static str {
        self.event_name
    }
}

pub(crate) struct SkipHandler<E: AggregatorEvent>(PhantomData<E>);

impl<P: 'static, EV: AggregatorEvent + Send + Sync> Handler<P> for SkipHandler<EV> {
    fn handle<'a>(
        &'a self,
        _projection: &'a mut P,
        _event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn aggregator_type(&self) -> &'static str {
        EV::aggregator_type()
    }

    fn event_name(&self) -> &'static str {
        EV::event_name()
    }
}
