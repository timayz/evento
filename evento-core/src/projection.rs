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
//! - [`SubscriptionBuilder`] - Builds continuous event subscriptions
//! - [`Subscription`] - Handle to a running subscription
//! - [`EventData`] - Typed event with deserialized data and metadata
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::projection::Projection;
//!
//! // Define a projection with event handlers
//! let projection = Projection::<AccountView, _>::new("accounts")
//!     .handler(account_opened)
//!     .handler(money_deposited);
//!
//! // Load aggregate state
//! let result = projection
//!     .load::<Account>("account-123")
//!     .execute(&executor)
//!     .await?;
//!
//! // Or start a subscription
//! let subscription = projection
//!     .subscription()
//!     .routing_key("accounts")
//!     .start(&executor)
//!     .await?;
//! ```

use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    time::Duration,
};
use tokio::{
    sync::{oneshot::Receiver, Mutex},
    time::{interval_at, Instant},
};
use tracing::field::Empty;
use ulid::Ulid;

use crate::{
    context,
    cursor::{Args, Cursor},
    Executor, ReadAggregator,
};

/// Filter for events by routing key.
///
/// Routing keys allow partitioning events for parallel processing
/// or filtering subscriptions to specific event streams.
#[derive(Clone)]
pub enum RoutingKey {
    /// Match all events regardless of routing key
    All,
    /// Match events with a specific routing key (or no key if `None`)
    Value(Option<String>),
}

/// Handler context providing access to executor and shared data.
///
/// `Context` wraps an [`RwContext`](crate::context::RwContext) for type-safe
/// data storage and provides access to the executor for database operations.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::handler]
/// async fn my_handler<E: Executor>(
///     event: Event<MyEventData>,
///     action: Action<'_, MyView, E>,
/// ) -> anyhow::Result<()> {
///     if let Action::Handle(ctx) = action {
///         // Access shared data
///         let config: Data<AppConfig> = ctx.extract();
///
///         // Use executor for queries
///         let events = ctx.executor.read(...).await?;
///     }
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    context: context::RwContext,
    /// Reference to the executor for database operations
    pub executor: &'a E,
}

impl<'a, E: Executor> Deref for Context<'a, E> {
    type Target = context::RwContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

/// Trait for aggregate types.
///
/// Aggregates are the root entities in event sourcing. Each aggregate
/// type has a unique identifier string used for event storage and routing.
///
/// This trait is typically derived using the `#[evento::aggregator]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator("myapp/Account")]
/// #[derive(Default)]
/// pub struct Account {
///     pub balance: i64,
///     pub owner: String,
/// }
/// ```
pub trait Aggregator: Default {
    /// Returns the unique type identifier for this aggregate (e.g., "myapp/Account")
    fn aggregator_type() -> &'static str;
}

/// Trait for event types.
///
/// Events represent state changes that have occurred. Each event type
/// has a name and belongs to an aggregator type.
///
/// This trait is typically derived using the `#[evento::aggregator]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator("myapp/Account")]
/// #[derive(bitcode::Encode, bitcode::Decode)]
/// pub struct AccountOpened {
///     pub owner: String,
/// }
/// ```
pub trait Event: Aggregator {
    /// Returns the event name (e.g., "AccountOpened")
    fn event_name() -> &'static str;
}

/// Trait for event handlers.
///
/// Handlers process events in two modes:
/// - `handle`: For subscriptions that perform side effects (send emails, update read models)
/// - `apply`: For loading aggregate state by replaying events
///
/// This trait is typically implemented via the `#[evento::handler]` macro.
pub trait Handler<P: 'static, E: Executor>: Sync + Send {
    /// Handles an event during subscription processing.
    ///
    /// This is called when processing events in a subscription context,
    /// where side effects like database updates or API calls are appropriate.
    fn handle<'a>(
        &'a self,
        context: &'a Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Applies an event to build projection state.
    ///
    /// This is called when loading aggregate state by replaying events.
    /// It should be a pure function that modifies the projection without side effects.
    fn apply<'a>(
        &'a self,
        projection: &'a mut P,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Returns the aggregator type this handler processes.
    fn aggregator_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

/// Action passed to event handlers.
///
/// Determines whether the handler should apply state changes or
/// handle the event with side effects.
pub enum Action<'a, P: 'static, E: Executor> {
    /// Apply event to projection state (for loading)
    Apply(&'a mut P),
    /// Handle event with context (for subscriptions)
    Handle(&'a Context<'a, E>),
}

/// Typed event with deserialized data and metadata.
///
/// `EventData` wraps a raw [`Event`](crate::Event) and provides typed access
/// to the deserialized event data and metadata. It implements `Deref` to
/// provide access to the underlying event fields (id, timestamp, version, etc.).
///
/// # Type Parameters
///
/// - `D`: The event data type (e.g., `AccountOpened`)
/// - `M`: The metadata type (defaults to `bool` for no metadata)
///
/// # Example
///
/// ```rust,ignore
/// use evento::metadata::Event;
///
/// #[evento::handler]
/// async fn handle_deposit<E: Executor>(
///     event: Event<MoneyDeposited>,
///     action: Action<'_, AccountView, E>,
/// ) -> anyhow::Result<()> {
///     // Access typed data
///     println!("Amount: {}", event.data.amount);
///
///     // Access metadata
///     if let Ok(user) = event.metadata.user() {
///         println!("By user: {}", user);
///     }
///
///     // Access underlying event fields via Deref
///     println!("Event ID: {}", event.id);
///     println!("Version: {}", event.version);
///
///     Ok(())
/// }
/// ```
pub struct EventData<D, M = bool> {
    event: crate::Event,
    /// The typed event data
    pub data: D,
    /// The typed event metadata
    pub metadata: M,
}

impl<D, M> Deref for EventData<D, M> {
    type Target = crate::Event;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<D, M> TryFrom<&crate::Event> for EventData<D, M>
where
    D: bitcode::DecodeOwned,
    M: bitcode::DecodeOwned,
{
    type Error = bitcode::Error;

    fn try_from(value: &crate::Event) -> Result<Self, Self::Error> {
        let data = bitcode::decode::<D>(&value.data)?;
        let metadata = bitcode::decode::<M>(&value.metadata)?;
        Ok(EventData {
            data,
            metadata,
            event: value.clone(),
        })
    }
}

/// Container for event handlers that build a projection.
///
/// A `Projection` groups related event handlers together and provides
/// methods to load aggregate state or create subscriptions.
///
/// # Type Parameters
///
/// - `P`: The projection/view type being built
/// - `E`: The executor type for database operations
///
/// # Example
///
/// ```rust,ignore
/// let projection = Projection::<AccountView, _>::new("accounts")
///     .handler(account_opened)
///     .handler(money_deposited)
///     .handler(money_withdrawn);
///
/// // Use for loading state
/// let state = projection.clone()
///     .load::<Account>("account-123")
///     .execute(&executor)
///     .await?;
///
/// // Or create a subscription
/// let sub = projection
///     .subscription()
///     .start(&executor)
///     .await?;
/// ```
pub struct Projection<P: 'static, E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<P, E>>>,
    safety_disabled: bool,
}

impl<P: 'static, E: Executor> Projection<P, E> {
    /// Creates a new projection with the given key.
    ///
    /// The key is used as the subscription identifier for cursor tracking.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            handlers: HashMap::new(),
            safety_disabled: false,
        }
    }

    pub fn no_safety_check(mut self) -> Self {
        self.safety_disabled = true;

        self
    }

    /// Registers an event handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<P, E> + 'static>(mut self, h: H) -> Self {
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
    pub fn skip<EV: Event + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<EV>(PhantomData))
    }

    /// Creates a builder for loading aggregate state.
    ///
    /// This consumes the projection and returns a [`LoadBuilder`] configured
    /// to load the state for the specified aggregate.
    ///
    /// # Type Parameters
    ///
    /// - `A`: The aggregate type to load
    pub fn load<A: Aggregator>(self, id: impl Into<String>) -> LoadBuilder<P, E>
    where
        P: Snapshot + Default,
    {
        let id = id.into();
        let mut aggregators = HashMap::new();
        aggregators.insert(A::aggregator_type().to_owned(), id.to_owned());

        LoadBuilder {
            key: self.key.to_owned(),
            id,
            aggregator_type: A::aggregator_type().to_owned(),
            aggregators,
            handlers: self.handlers,
            context: Default::default(),
            filter_events_by_name: true,
            safety_disabled: self.safety_disabled,
        }
    }

    /// Creates a builder for a continuous event subscription.
    ///
    /// This consumes the projection and returns a [`SubscriptionBuilder`]
    /// that can be configured and started.
    pub fn subscription(self) -> SubscriptionBuilder<P, E> {
        SubscriptionBuilder {
            key: self.key.to_owned(),
            context: Default::default(),
            handlers: self.handlers,
            delay: None,
            retry: Some(30),
            chunk_size: 300,
            is_accept_failure: false,
            routing_key: RoutingKey::Value(None),
            aggregators: Default::default(),
            safety_disabled: self.safety_disabled,
            shutdown_rx: None,
        }
    }
}

/// Result of loading an aggregate's state.
///
/// Contains the rebuilt projection state along with the current version
/// and routing key. Implements `Deref` and `DerefMut` for transparent
/// access to the inner item.
///
/// # Example
///
/// ```rust,ignore
/// let result: LoadResult<AccountView> = projection
///     .load::<Account>("account-123")
///     .execute(&executor)
///     .await?
///     .expect("Account not found");
///
/// // Access inner item via Deref
/// println!("Balance: {}", result.balance);
///
/// // Access metadata
/// println!("Version: {}", result.version);
/// println!("Routing key: {:?}", result.routing_key);
/// ```
#[derive(Debug, Clone, Default)]
pub struct LoadResult<A> {
    /// The loaded projection/view state
    pub item: A,
    /// Current version of the aggregate
    pub version: u16,
    /// Routing key for the aggregate (if set)
    pub routing_key: Option<String>,
}

impl<A> Deref for LoadResult<A> {
    type Target = A;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<A> DerefMut for LoadResult<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}

/// Trait for types that can be restored from snapshots.
///
/// Snapshots provide a performance optimization by storing pre-computed
/// state, avoiding the need to replay all events from the beginning.
///
/// This trait is typically implemented via the `#[evento::snapshot]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::snapshot]
/// #[derive(Default)]
/// pub struct AccountView {
///     pub balance: i64,
///     pub owner: String,
/// }
///
/// // The macro generates the restore implementation that loads
/// // from a snapshot table if available
/// ```
pub trait Snapshot: Sized {
    /// Restores state from a snapshot if available.
    ///
    /// Returns `None` if no snapshot exists for the given ID.
    fn restore<'a>(
        _context: &'a context::RwContext,
        _id: String,
        _aggregators: &'a HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<Self>>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
}

/// Builder for loading aggregate state from events.
///
/// Created via [`Projection::load`], this builder configures how to
/// load an aggregate's state by replaying events.
///
/// # Example
///
/// ```rust,ignore
/// let result = projection
///     .load::<Account>("account-123")
///     .data(app_config)  // Add shared data
///     .aggregator::<User>("user-456")  // Add related aggregate
///     .execute(&executor)
///     .await?;
/// ```
pub struct LoadBuilder<P: Snapshot + Default + 'static, E: Executor> {
    key: String,
    id: String,
    aggregator_type: String,
    aggregators: HashMap<String, String>,
    handlers: HashMap<String, Box<dyn Handler<P, E>>>,
    context: context::RwContext,
    filter_events_by_name: bool,
    safety_disabled: bool,
}

impl<P: Snapshot + Default + 'static, E: Executor> LoadBuilder<P, E> {
    /// Adds shared data to the load context.
    ///
    /// Data added here is accessible in handlers via the context.
    pub fn data<D: Send + Sync + 'static>(&mut self, v: D) -> &mut Self {
        self.context.insert(v);

        self
    }

    /// Adds a related aggregate to load events from.
    ///
    /// Use this when the projection needs events from multiple aggregates.
    pub fn aggregator<A: Aggregator>(&mut self, id: impl Into<String>) -> &mut Self {
        self.aggregator_raw(A::aggregator_type().to_owned(), id)
    }

    /// Adds a related aggregate to load events from.
    ///
    /// Use this when the projection needs events from multiple aggregates.
    pub fn aggregator_raw(
        &mut self,
        aggregator_type: impl Into<String>,
        id: impl Into<String>,
    ) -> &mut Self {
        self.aggregators.insert(aggregator_type.into(), id.into());

        self
    }

    /// Executes the load operation, returning the rebuilt state with all events of aggregator.
    ///
    /// Returns `None` if no events exist for the aggregate.
    /// Returns `Err` if there are too many events to process in one batch.
    pub async fn execute_all(&mut self, executor: &E) -> anyhow::Result<Option<LoadResult<P>>> {
        self.filter_events_by_name = false;

        self.execute(executor).await
    }

    /// Executes the load operation, returning the rebuilt state.
    ///
    /// Returns `None` if no events exist for the aggregate.
    /// Returns `Err` if there are too many events to process in one batch.
    pub async fn execute(&self, executor: &E) -> anyhow::Result<Option<LoadResult<P>>> {
        let context = Context {
            context: self.context.clone(),
            executor,
        };

        let mut cursor = executor.get_subscriber_cursor(self.key.to_owned()).await?;
        let (mut version, mut routing_key) = match cursor {
            Some(ref cursor) => {
                let cursor = crate::Event::deserialize_cursor(cursor)?;

                (cursor.v, cursor.r)
            }
            _ => (0, None),
        };
        let loaded = P::restore(&context, self.id.to_owned(), &self.aggregators).await?;
        if loaded.is_none() {
            cursor = None;
        }

        let read_aggregators = self
            .handlers
            .values()
            .map(|h| match self.aggregators.get(h.aggregator_type()) {
                Some(id) => ReadAggregator {
                    aggregator_type: h.aggregator_type().to_owned(),
                    aggregator_id: Some(id.to_owned()),
                    name: if self.filter_events_by_name && self.safety_disabled {
                        Some(h.event_name().to_owned())
                    } else {
                        None
                    },
                },
                _ => {
                    if self.filter_events_by_name && self.safety_disabled {
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

        if events.edges.is_empty() && loaded.is_none() {
            return Ok(None);
        }

        let mut snapshot = loaded.unwrap_or_default();

        for event in events.edges.iter() {
            let key = format!("{}_{}", event.node.aggregator_type, event.node.name);
            let Some(handler) = self.handlers.get(&key) else {
                if !self.safety_disabled {
                    anyhow::bail!("no handler s={} k={key}", self.key);
                }

                continue;
            };

            handler.apply(&mut snapshot, &event.node).await?;

            if event.node.aggregator_type == self.aggregator_type {
                version = event.node.version;
                routing_key = event.node.routing_key.to_owned();
            }
        }

        if events.page_info.has_next_page && !self.filter_events_by_name {
            anyhow::bail!("Too busy");
        }

        Ok(Some(LoadResult {
            item: snapshot,
            version,
            routing_key,
        }))
    }
}

/// Builder for creating event subscriptions.
///
/// Created via [`Projection::subscription`], this builder configures
/// a continuous event processing subscription with retry logic,
/// routing key filtering, and graceful shutdown support.
///
/// # Example
///
/// ```rust,ignore
/// let subscription = projection
///     .subscription()
///     .routing_key("accounts")
///     .chunk_size(100)
///     .retry(5)
///     .delay(Duration::from_secs(10))
///     .start(&executor)
///     .await?;
///
/// // Later, gracefully shutdown
/// subscription.shutdown().await?;
/// ```
pub struct SubscriptionBuilder<P: 'static, E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<P, E>>>,
    context: context::RwContext,
    routing_key: RoutingKey,
    delay: Option<Duration>,
    chunk_size: u16,
    is_accept_failure: bool,
    retry: Option<u8>,
    aggregators: HashMap<String, String>,
    safety_disabled: bool,
    shutdown_rx: Option<Mutex<Receiver<()>>>,
}

impl<P, E: Executor + 'static> SubscriptionBuilder<P, E> {
    /// Adds shared data to the load context.
    ///
    /// Data added here is accessible in handlers via the context.
    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        self.context.insert(v);

        self
    }

    /// Allows the subscription to continue after handler failures.
    ///
    /// By default, subscriptions stop on the first error. With this flag,
    /// errors are logged but processing continues.
    pub fn accept_failure(mut self) -> Self {
        self.is_accept_failure = true;

        self
    }

    /// Sets the number of events to process per batch.
    ///
    /// Default is 300.
    pub fn chunk_size(mut self, v: u16) -> Self {
        self.chunk_size = v;

        self
    }

    /// Sets a delay before starting the subscription.
    ///
    /// Useful for staggering subscription starts in multi-node deployments.
    pub fn delay(mut self, v: Duration) -> Self {
        self.delay = Some(v);

        self
    }

    /// Filters events by routing key.
    ///
    /// Only events with the matching routing key will be processed.
    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = RoutingKey::Value(Some(v.into()));

        self
    }

    /// Sets the maximum number of retries on failure.
    ///
    /// Uses exponential backoff. Default is 30.
    pub fn retry(mut self, v: u8) -> Self {
        self.retry = Some(v);

        self
    }

    /// Processes all events regardless of routing key.
    pub fn all(mut self) -> Self {
        self.routing_key = RoutingKey::All;

        self
    }

    /// Adds a related aggregate to process events from.
    pub fn aggregator<A: Aggregator>(mut self, id: impl Into<String>) -> Self {
        self.aggregators
            .insert(A::aggregator_type().to_owned(), id.into());

        self
    }

    fn read_aggregators(&self) -> Vec<ReadAggregator> {
        self.handlers
            .values()
            .map(|h| match self.aggregators.get(h.aggregator_type()) {
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
            .collect()
    }

    fn key(&self) -> String {
        if let RoutingKey::Value(Some(ref key)) = self.routing_key {
            return format!("{key}.{}", self.key);
        }

        self.key.to_owned()
    }

    #[tracing::instrument(
        skip_all,
        fields(
            subscription = Empty,
            aggregator_type = Empty,
            aggregator_id = Empty,
            event = Empty,
        )
    )]
    async fn process(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &[ReadAggregator],
    ) -> anyhow::Result<bool> {
        let mut interval = interval_at(
            Instant::now() - Duration::from_millis(400),
            Duration::from_millis(300),
        );

        tracing::Span::current().record("subscription", self.key());

        loop {
            interval.tick().await;

            if !executor.is_subscriber_running(self.key(), *id).await? {
                return Ok(false);
            }

            let cursor = executor.get_subscriber_cursor(self.key()).await?;

            let timestamp = executor
                .read(
                    Some(aggregators.to_vec()),
                    Some(self.routing_key.to_owned()),
                    Args::backward(1, None),
                )
                .await?
                .edges
                .last()
                .map(|e| e.node.timestamp)
                .unwrap_or_default();

            let res = executor
                .read(
                    Some(aggregators.to_vec()),
                    Some(self.routing_key.to_owned()),
                    Args::forward(self.chunk_size, cursor),
                )
                .await?;

            if res.edges.is_empty() {
                return Ok(false);
            }

            let context = Context {
                context: self.context.clone(),
                executor,
            };

            for event in res.edges {
                if let Some(ref rx) = self.shutdown_rx {
                    let mut rx = rx.lock().await;
                    if rx.try_recv().is_ok() {
                        tracing::info!(
                            key = self.key(),
                            "Subscription received shutdown signal, stopping gracefull"
                        );

                        return Ok(true);
                    }
                    drop(rx);
                }

                tracing::Span::current().record("aggregator_type", &event.node.aggregator_type);
                tracing::Span::current().record("aggregator_id", &event.node.aggregator_id);
                tracing::Span::current().record("event", &event.node.name);

                let key = format!("{}_{}", event.node.aggregator_type, event.node.name);
                let Some(handler) = self.handlers.get(&key) else {
                    if !self.safety_disabled {
                        anyhow::bail!("no handler s={} k={key}", self.key());
                    }

                    continue;
                };

                if let Err(err) = handler.handle(&context, &event.node).await {
                    tracing::error!("failed");

                    return Err(err);
                }

                tracing::debug!("completed");

                executor
                    .acknowledge(
                        self.key(),
                        event.cursor.to_owned(),
                        timestamp - event.node.timestamp,
                    )
                    .await?;
            }
        }
    }

    /// Starts the subscription without retry logic.
    ///
    /// Equivalent to calling `start()` with retries disabled.
    pub async fn unretry_start(mut self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        self.retry = None;
        self.start(executor).await
    }

    /// Starts a continuous background subscription.
    ///
    /// Returns a [`Subscription`] handle that can be used for graceful shutdown.
    /// The subscription runs in a spawned tokio task and polls for new events.
    #[tracing::instrument(skip_all, fields(
        subscription = self.key(),
        aggregator_type = tracing::field::Empty,
        aggregator_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn start(mut self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        let executor = executor.clone();
        let id = Ulid::new();
        let subscription_id = id;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_rx = Some(Mutex::new(shutdown_rx));

        executor
            .upsert_subscriber(self.key(), id.to_owned())
            .await?;

        let task_handle = tokio::spawn(async move {
            let read_aggregators = self.read_aggregators();
            let start = self
                .delay
                .map(|d| Instant::now() + d)
                .unwrap_or_else(Instant::now);

            let mut interval = interval_at(
                start - Duration::from_millis(1200),
                Duration::from_millis(1000),
            );

            loop {
                interval.tick().await;

                if let Some(ref rx) = self.shutdown_rx {
                    let mut rx = rx.lock().await;
                    if rx.try_recv().is_ok() {
                        tracing::info!(
                            key = self.key(),
                            "Subscription received shutdown signal, stopping gracefull"
                        );

                        break;
                    }
                    drop(rx);
                }

                let result = match self.retry {
                    Some(retry) => {
                        (|| async { self.process(&executor, &id, &read_aggregators).await })
                            .retry(ExponentialBuilder::default().with_max_times(retry.into()))
                            .sleep(tokio::time::sleep)
                            .notify(|err, dur| {
                                tracing::error!(
                                    error = %err,
                                    duration = ?dur,
                                    "Failed to process event"
                                );
                            })
                            .await
                    }
                    _ => self.process(&executor, &id, &read_aggregators).await,
                };

                match result {
                    Ok(shutdown) => {
                        if shutdown {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to process event");

                        if !self.is_accept_failure {
                            break;
                        }
                    }
                };
            }
        });

        Ok(Subscription {
            id: subscription_id,
            task_handle,
            shutdown_tx,
        })
    }

    /// Executes the subscription once without retry logic.
    ///
    /// Processes all pending events and returns. Does not poll for new events.
    pub async fn unretry_execute(mut self, executor: &E) -> anyhow::Result<()> {
        self.retry = None;
        self.execute(executor).await
    }

    /// Executes the subscription once, processing all pending events.
    ///
    /// Unlike `start()`, this does not run continuously. It processes
    /// all currently pending events and returns.
    #[tracing::instrument(skip_all, fields(
        subscription = self.key(),
        aggregator_type = tracing::field::Empty,
        aggregator_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn execute(&self, executor: &E) -> anyhow::Result<()> {
        let id = Ulid::new();

        executor
            .upsert_subscriber(self.key(), id.to_owned())
            .await?;

        let read_aggregators = self.read_aggregators();

        match self.retry {
            Some(retry) => {
                (|| async { self.process(executor, &id, &read_aggregators).await })
                    .retry(ExponentialBuilder::default().with_max_times(retry.into()))
                    .sleep(tokio::time::sleep)
                    .notify(|err, dur| {
                        tracing::error!(
                            error = %err,
                            duration = ?dur,
                            "Failed to process event"
                        );
                    })
                    .await
            }
            _ => self.process(executor, &id, &read_aggregators).await,
        }?;

        Ok(())
    }
}

/// Handle to a running event subscription.
///
/// Returned by [`SubscriptionBuilder::start`], this handle provides
/// the subscription ID and a method for graceful shutdown.
///
/// # Example
///
/// ```rust,ignore
/// let subscription = projection
///     .subscription()
///     .start(&executor)
///     .await?;
///
/// println!("Started subscription: {}", subscription.id);
///
/// // On application shutdown
/// subscription.shutdown().await?;
/// ```
#[derive(Debug)]
pub struct Subscription {
    /// Unique ID for this subscription instance
    pub id: Ulid,
    task_handle: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl Subscription {
    /// Gracefully shuts down the subscription.
    ///
    /// Signals the subscription to stop and waits for it to finish
    /// processing the current event before returning.
    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown_tx.send(());

        self.task_handle.await
    }
}

struct SkipHandler<E: Event>(PhantomData<E>);

impl<P: 'static, E: Executor, EV: Event + Send + Sync> Handler<P, E> for SkipHandler<EV> {
    fn apply<'a>(
        &'a self,
        _projection: &'a mut P,
        _event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
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
