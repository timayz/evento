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
//! - [`SubscriptionBuilder`] - Builds continuous event subscriptions
//! - [`Subscription`] - Handle to a running subscription
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
    cursor::{self, Args},
    subscription::{self, RoutingKey, Subscription, SubscriptionBuilder},
    Aggregate, AggregateEvent, EventFilter, Executor, WriteBuilder,
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
    aggregate_type: String,
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
                self.aggregate_type.to_owned(),
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
                self.aggregate_type.to_owned(),
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
            .delete_snapshot(self.aggregate_type.to_owned(), self.id.to_owned())
            .await
    }

    /// Returns the aggregate ID for a registered aggregate type.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate type was not registered via [`LoadBuilder::aggregate`].
    pub async fn aggregate<A: Aggregate>(&self) -> String {
        match self.aggregators.get(A::aggregate_type()) {
            Some(id) => id.to_owned(),
            None => {
                tracing::error!(
                    "Failed to get `Aggregate id <{}>` For the Aggregate id extractor to work \
                correctly, register the related aggregator with `.aggregate::<MyAggregator>(id)` on \
                the load builder. Ensure that types align in both the set and retrieve calls.",
                    A::aggregate_type()
                );
                panic!("Projection Aggregate not configured correctly. See error logs for details.")
            }
        }
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

    /// Returns the aggregate type this handler processes.
    fn aggregate_type(&self) -> &'static str;
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

    /// Returns the version of the primary aggregate as of the last applied
    /// primary-aggregate event (0 when none has been applied yet).
    ///
    /// Maintained by `Projection::load_aggregator`; unlike the stream cursor,
    /// this only ever advances on events of the primary aggregate itself, so
    /// it stays correct for multi-aggregate and name-filtered projections.
    fn get_aggregate_version(&self) -> u16;

    /// Records the version of the primary aggregate.
    fn set_aggregate_version(&mut self, v: u16);
}

/// Trait for projections that can create a [`WriteBuilder`].
///
/// Extends [`ProjectionCursor`] to provide aggregate identity and versioning,
/// enabling projections to emit new events.
pub trait ProjectionAggregate: ProjectionCursor {
    /// Returns the aggregate ID for this projection.
    ///
    /// Implementors must return the stable identity of the aggregate this
    /// projection represents (typically a field populated from the first event).
    fn aggregate_id(&self) -> String;

    /// Returns the current version of the primary aggregate.
    ///
    /// Returns `0` if no primary-aggregate event has been applied yet. This is
    /// tracked separately from the stream cursor: the cursor's last event may
    /// belong to a secondary aggregate or a name-filtered subset, so its
    /// version field is not a reliable source for optimistic concurrency.
    fn aggregate_version(&self) -> anyhow::Result<u16> {
        Ok(self.get_aggregate_version())
    }

    /// Creates a [`WriteBuilder`] pre-configured with this projection's ID and version.
    ///
    /// Use this to emit new events from a projection (the write gateway).
    fn write(&self) -> anyhow::Result<WriteBuilder> {
        Ok(WriteBuilder::new(self.aggregate_id())
            .original_version(self.aggregate_version()?)
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

/// Projection definition: a set of handlers for a primary aggregate type.
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
    aggregate_type: &'static str,
    revision: u16,
    handlers: HashMap<String, Box<dyn Handler<P>>>,
    context: context::RwContext,
    safety_disabled: bool,
    tombstone: Option<(&'static str, &'static str)>,
    executor: PhantomData<E>,
}

impl<E: Executor, P: Snapshot<E> + Default + 'static> Projection<E, P> {
    /// Creates a new projection definition for the given primary aggregate type.
    pub fn new<A: Aggregate>() -> Projection<E, P> {
        Projection {
            aggregate_type: A::aggregate_type(),
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
    pub fn strict(mut self) -> Self {
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
    pub fn tombstone<EV: AggregateEvent + Send + Sync + 'static>(mut self) -> Self {
        self.tombstone = Some((EV::aggregate_type(), EV::event_name()));
        self
    }

    /// Registers an event handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<P> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregate_type(), h.event_name());
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
    pub fn skip<EV: AggregateEvent + Send + Sync + 'static>(self) -> Self {
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
        aggregators.insert(self.aggregate_type.to_string(), id.to_owned());

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
            continue_on_error: false,
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
                    Some(vec![EventFilter::exact(
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
        aggregators.insert(self.aggregate_type.to_string(), id.to_owned());
        for (k, v) in extra_aggregators {
            aggregators.insert(k.to_owned(), v.to_owned());
        }

        let context = Context {
            context: self.context.clone(),
            executor,
            id: id.to_owned(),
            aggregate_type: self.aggregate_type.to_string(),
            aggregators: &aggregators,
            revision: self.revision,
        };
        let snapshot = P::restore(&context).await?;
        let cursor = snapshot.as_ref().map(|s| s.get_cursor());

        let read_aggregators = self
            .handlers
            .values()
            .map(|h| {
                // Scope each handler to one id: the id registered via
                // `.aggregate::<S>(id)` when present, otherwise auto-key to the
                // primary id (co-keyed by default). In subscription mode the
                // primary id is the event's aggregate id, so co-keyed secondary
                // aggregates are scoped correctly with no explicit registration.
                let aggregate_id = aggregators
                    .get(h.aggregate_type())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| id.to_owned());

                EventFilter {
                    aggregate_type: h.aggregate_type().to_owned(),
                    aggregate_id: Some(aggregate_id),
                    name: if self.safety_disabled {
                        Some(h.event_name().to_owned())
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<_>>();

        // On a backend with a stability watermark, events at/above it may still
        // be reordered by late commits, so the persisted snapshot cursor must
        // never advance past the watermark. Such events are still folded into
        // the returned in-memory state (read-your-writes), just not persisted.
        let stable = executor.stable_timestamp().await?;

        let mut snapshot_state: Option<P> = snapshot;
        let mut page_cursor = cursor;
        // Applied events pending persistence; only ever true below the watermark.
        let mut dirty = false;
        // Once set, the watermark was reached: keep applying in memory, stop persisting.
        let mut gated = false;
        let mut any_events = false;

        loop {
            let events = executor
                .read(
                    Some(read_aggregators.to_vec()),
                    None,
                    Args::forward(100, page_cursor.clone()),
                )
                .await?;

            if events.edges.is_empty() {
                break;
            }
            any_events = true;
            let state = snapshot_state.get_or_insert_with(Default::default);

            for event in events.edges.iter() {
                if !gated {
                    if let Some(w) = stable {
                        let event_micros = (event.node.timestamp)
                            .saturating_mul(1_000_000)
                            .saturating_add(event.node.timestamp_subsec as u64 * 1_000);
                        if event_micros >= w {
                            // Persist the stable prefix before folding events
                            // that could still be reordered.
                            if dirty {
                                state.take_snapshot(&context).await?;
                                dirty = false;
                            }
                            gated = true;
                        }
                    }
                }

                let key = format!("{}_{}", event.node.aggregate_type, event.node.name);
                match self.handlers.get(&key) {
                    Some(handler) => handler.handle(state, &event.node).await?,
                    None if !self.safety_disabled => anyhow::bail!("no handler k={key}"),
                    None => {}
                }

                state.set_cursor(&event.cursor);
                if event.node.aggregate_type == self.aggregate_type
                    && event.node.aggregate_id == id
                {
                    state.set_aggregate_version(event.node.version);
                }
                if !gated {
                    dirty = true;
                }
            }

            page_cursor = events.edges.last().map(|e| e.cursor.to_owned());
            if !events.page_info.has_next_page {
                break;
            }
        }

        if !any_events && snapshot_state.is_none() {
            return Ok(None);
        }

        let snapshot = snapshot_state.unwrap_or_default();
        if dirty {
            snapshot.take_snapshot(&context).await?;
        }

        Ok(Some(snapshot))
    }
}

/// Builder for loading the projection state of a specific aggregate id.
///
/// Created via [`Projection::load`]. Allows registering related aggregates
/// whose events also feed this projection, then [`LoadBuilder::execute`] runs
/// the load.
///
/// A handler's aggregate type defaults to **co-keyed** with the loaded id: its
/// events are read scoped to that same id. Register a secondary aggregate with
/// [`LoadBuilder::aggregate`] (or [`LoadBuilder::aggregate_raw`]) only when it
/// uses a different id.
pub struct LoadBuilder<E: Executor, P: Default + 'static> {
    projection: Projection<E, P>,
    id: String,
    aggregators: HashMap<String, String>,
}

impl<E: Executor, P: Snapshot<E> + Default + 'static> LoadBuilder<E, P> {
    /// Adds a related aggregate to load events from.
    pub fn aggregate<A: Aggregate>(self, id: impl Into<String>) -> Self {
        self.aggregate_raw(A::aggregate_type().to_owned(), id)
    }

    /// Adds a related aggregate to load events from (raw aggregate type).
    pub fn aggregate_raw(
        mut self,
        aggregate_type: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        self.aggregators.insert(aggregate_type.into(), id.into());

        self
    }

    /// Executes the load, returning the rebuilt state.
    ///
    /// Replays events in pages of 100, restoring from a snapshot when one is
    /// available. Returns `None` if no events exist for the aggregate, or if a
    /// tombstone was registered via [`Projection::tombstone`] and the
    /// corresponding event has been committed for this id.
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
///
/// # Multi-aggregate projections (co-keying)
///
/// Unlike [`LoadBuilder`], a subscription has no per-call related-aggregate
/// ids to work with — it only knows the id of the event that woke it. So every
/// **secondary** aggregate a handler targets (any aggregate type other than the
/// primary passed to [`Projection::new`]) is treated as **co-keyed**: assumed
/// to share the primary aggregate's id. Concretely:
///
/// - The worker also wakes on secondary-aggregate events (not just primary
///   ones), so a change to a co-keyed aggregate re-projects the row.
/// - When reloading, each secondary aggregate is scoped to the woken
///   `event.aggregate_id`, exactly as if you had called
///   `.aggregate::<Secondary>(event.aggregate_id)` in load mode.
///
/// **Limitation:** if a secondary aggregate does *not* share the primary's id,
/// its events won't be found under that id and won't be applied by the
/// subscription. For that case, rebuild on demand with
/// [`LoadBuilder::aggregate`] (which takes the real secondary id) or drive a
/// manual [`SubscriptionBuilder`] that resolves the mapping itself.
pub struct ProjectionSubscription<E: Executor, P: Default + 'static> {
    projection: Projection<E, P>,
    key: String,
    routing_key: Option<RoutingKey>,
    chunk_size: u16,
    retry: Option<u8>,
    delay: Option<Duration>,
    continue_on_error: bool,
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
    pub fn continue_on_error(mut self) -> Self {
        self.continue_on_error = true;
        self
    }

    /// Starts the subscription.
    ///
    /// Returns a [`Subscription`] handle that can be used for graceful shutdown.
    pub async fn start(self, executor: &E) -> anyhow::Result<Subscription> {
        self.into_builder().start(executor).await
    }

    /// Processes every currently-available event once, then returns.
    ///
    /// Runs a single drain pass without spawning a background worker. Useful
    /// for tests and one-shot rebuilds; [`start`](Self::start) is the normal
    /// entry point for a long-running subscription.
    pub async fn run_once(self, executor: &E) -> anyhow::Result<()> {
        let mut builder = self.into_builder();
        builder.run_once(executor).await
    }

    /// Assembles the underlying [`SubscriptionBuilder`] shared by
    /// [`start`](Self::start) and [`run_once`](Self::run_once).
    fn into_builder(self) -> SubscriptionBuilder<E> {
        let ProjectionSubscription {
            projection,
            key,
            routing_key,
            chunk_size,
            retry,
            delay,
            continue_on_error,
        } = self;

        let tombstone = projection.tombstone;

        // One auto-handler per registered event, across the primary *and* any
        // co-keyed secondary aggregate, so the worker wakes on secondary events
        // too (not just the primary's). Each event_name is &'static str
        // (Handler::event_name always derives from the `#[evento::handler]`
        // macro). Drop any handler that collides with the tombstone — that event
        // routes to ProjectionTombstoneHandler. Secondary aggregates are scoped
        // by `load_aggregator`, which auto-keys them to the event's id.
        let specs: Vec<(&'static str, &'static str)> = projection
            .handlers
            .values()
            .filter(|h| {
                tombstone
                    .map(|(t, e)| !(h.aggregate_type() == t && h.event_name() == e))
                    .unwrap_or(true)
            })
            .map(|h| (h.aggregate_type(), h.event_name()))
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
            None => builder.no_retry(),
        };
        if let Some(d) = delay {
            builder = builder.delay(d);
        }
        if continue_on_error {
            builder = builder.continue_on_error();
        }

        for (spec_type, event_name) in specs {
            builder = builder.handler(ProjectionAutoHandler::<E, P> {
                projection: projection.clone(),
                aggregate_type: spec_type,
                event_name,
                _marker: PhantomData,
            });
        }

        if let Some((tombstone_type, tombstone_event)) = tombstone {
            builder = builder.handler(ProjectionTombstoneHandler::<E, P> {
                projection: projection.clone(),
                aggregate_type: tombstone_type,
                event_name: tombstone_event,
                _marker: PhantomData,
            });
        }

        builder
    }
}

struct ProjectionAutoHandler<E: Executor, P: Default + 'static> {
    projection: Arc<Projection<E, P>>,
    aggregate_type: &'static str,
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
            // `load_aggregator` auto-keys every co-keyed secondary aggregate to
            // this event's id, so no extra aggregators need to be supplied here.
            self.projection
                .load_aggregator(context.executor, &event.aggregate_id, &HashMap::new())
                .await?;
            Ok(())
        })
    }

    fn aggregate_type(&self) -> &'static str {
        self.aggregate_type
    }

    fn event_name(&self) -> &'static str {
        self.event_name
    }
}

struct ProjectionTombstoneHandler<E: Executor, P: Default + 'static> {
    projection: Arc<Projection<E, P>>,
    aggregate_type: &'static str,
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
                id: event.aggregate_id.clone(),
                aggregate_type: self.projection.aggregate_type.to_string(),
                aggregators: &aggregators,
                revision: self.projection.revision,
            };
            P::drop_snapshot(&ctx).await?;
            Ok(())
        })
    }

    fn aggregate_type(&self) -> &'static str {
        self.aggregate_type
    }

    fn event_name(&self) -> &'static str {
        self.event_name
    }
}

pub(crate) struct SkipHandler<E: AggregateEvent>(PhantomData<E>);

impl<P: 'static, EV: AggregateEvent + Send + Sync> Handler<P> for SkipHandler<EV> {
    fn handle<'a>(
        &'a self,
        _projection: &'a mut P,
        _event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn aggregate_type(&self) -> &'static str {
        EV::aggregate_type()
    }

    fn event_name(&self) -> &'static str {
        EV::event_name()
    }
}
