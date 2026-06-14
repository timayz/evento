//! Continuous event subscriptions.
//!
//! This module provides infrastructure for processing events continuously
//! in the background with retry logic, routing key filtering, and graceful
//! shutdown support.
//!
//! # Key Types
//!
//! - [`SubscriptionBuilder`] - Builds and configures event subscriptions
//! - [`Subscription`] - Handle to a running subscription
//! - [`Handler`] - Trait for event handlers
//! - [`Context`] - Handler context with executor access
//! - [`RoutingKey`] - Filter for event routing
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::subscription::SubscriptionBuilder;
//!
//! // Build a subscription with handlers
//! let subscription = SubscriptionBuilder::new("my-subscription")
//!     .handler(account_opened_handler)
//!     .handler(money_deposited_handler)
//!     .routing_key("accounts")
//!     .chunk_size(100)
//!     .retry(5)
//!     .start(&executor)
//!     .await?;
//!
//! // Later, gracefully shutdown
//! subscription.shutdown().await?;
//! ```

use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::HashMap, future::Future, marker::PhantomData, ops::Deref, pin::Pin, time::Duration,
};
use tokio::{
    sync::{oneshot::Receiver, Mutex},
    time::{interval_at, Instant},
};
use tracing::field::Empty;
use ulid::Ulid;

use crate::{context, cursor::Args, Aggregate, AggregateEvent, EventFilter, Executor};

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
/// #[evento::subscription]
/// async fn my_handler<E: Executor>(
///     context: &Context<'_, E>,
///     event: Event<MyEventData>,
/// ) -> anyhow::Result<()> {
///     // Access shared data
///     let config: Data<AppConfig> = context.extract();
///
///     // Use executor for queries
///     let events = context.executor.read(...).await?;
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

/// Trait for event handlers.
///
/// Handlers process events in two modes:
/// - `handle`: For subscriptions that perform side effects (send emails, update read models)
/// - `apply`: For loading aggregate state by replaying events
///
/// This trait is typically implemented via the `#[evento::handler]` macro.
pub trait Handler<E: Executor>: Sync + Send {
    /// Handles an event during subscription processing.
    ///
    /// This is called when processing events in a subscription context,
    /// where side effects like database updates or API calls are appropriate.
    fn handle<'a>(
        &'a self,
        context: &'a Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Returns the aggregate type this handler processes.
    fn aggregate_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

/// Builder for creating event subscriptions.
///
/// Created via [`Projection::subscription`](crate::projection::Projection::subscription), this builder configures
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
pub struct SubscriptionBuilder<E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<E>>>,
    context: context::RwContext,
    routing_key: Option<RoutingKey>,
    prefix_key: Option<String>,
    delay: Option<Duration>,
    chunk_size: u16,
    continue_on_error: bool,
    retry: Option<u8>,
    aggregators: HashMap<String, String>,
    safety_disabled: bool,
    shutdown_rx: Option<Mutex<Receiver<()>>>,
}

impl<E: Executor + 'static> SubscriptionBuilder<E> {
    /// Creates a new projection with the given key.
    ///
    /// The key is used as the subscription identifier for cursor tracking.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            handlers: HashMap::new(),
            safety_disabled: true,
            context: Default::default(),
            delay: None,
            retry: Some(30),
            chunk_size: 300,
            continue_on_error: false,
            routing_key: None,
            prefix_key: None,
            aggregators: Default::default(),
            shutdown_rx: None,
        }
    }

    /// Enables safety checks for unhandled events.
    ///
    /// When enabled, processing fails if an event is encountered without a handler.
    pub fn strict(mut self) -> Self {
        self.safety_disabled = false;

        self
    }

    /// Registers an event handler with this subscription.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<E> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregate_type(), h.event_name());
        if self.handlers.insert(key.to_owned(), Box::new(h)).is_some() {
            panic!("Cannot register event handler: key {} already exists", key);
        }
        self
    }

    /// Registers a skip handler for an event type.
    ///
    /// Events of this type will be acknowledged but not processed.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn skip<EV: AggregateEvent + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<EV>(PhantomData))
    }

    /// Adds shared data to the subscription context.
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
    pub fn continue_on_error(mut self) -> Self {
        self.continue_on_error = true;

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
    /// Overrides any executor-level default.
    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = Some(RoutingKey::Value(Some(v.into())));

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
    ///
    /// Overrides any executor-level default.
    pub fn all(mut self) -> Self {
        self.routing_key = Some(RoutingKey::All);

        self
    }

    /// Adds a related aggregate to process events from.
    pub fn aggregate<A: Aggregate>(mut self, id: impl Into<String>) -> Self {
        self.aggregators
            .insert(A::aggregate_type().to_owned(), id.into());

        self
    }

    fn read_aggregators(&self) -> Vec<EventFilter> {
        self.handlers
            .values()
            .map(|h| {
                // `#[subscription_all]` handlers report the sentinel name "all" and
                // match every event of the type, so they must read by type rather
                // than by a literal event name (which would match nothing).
                let by_name = self.safety_disabled && h.event_name() != "all";
                match self.aggregators.get(h.aggregate_type()) {
                    Some(id) => EventFilter {
                        aggregate_type: h.aggregate_type().to_owned(),
                        aggregate_id: Some(id.to_owned()),
                        name: by_name.then(|| h.event_name().to_owned()),
                    },
                    _ => {
                        if by_name {
                            EventFilter::by_event(h.aggregate_type(), h.event_name())
                        } else {
                            EventFilter::by_type(h.aggregate_type())
                        }
                    }
                }
            })
            .collect()
    }

    fn key(&self) -> String {
        let prefix = match &self.routing_key {
            Some(RoutingKey::Value(Some(k))) => Some(k.as_str()),
            Some(RoutingKey::All) => self.prefix_key.as_deref(),
            _ => None,
        };
        match prefix {
            Some(p) => format!("{p}.{}", self.key),
            None => self.key.to_owned(),
        }
    }

    /// Resolves an unset routing key from the executor's default and captures
    /// the default as a storage-key prefix.
    ///
    /// Called once at the top of `start()` / `run_once()` before any other
    /// method reads `self.routing_key`. After this, `routing_key` is always
    /// `Some(_)`.
    ///
    /// `prefix_key` is captured from `executor.default_routing_key()` even
    /// when the user has already called `.all()`, so the storage key remains
    /// scoped to the executor's tenant — otherwise two executors with
    /// different defaults would share one row in the subscriber table.
    fn resolve_routing_key(&mut self, executor: &E) {
        if self.prefix_key.is_none() {
            self.prefix_key = executor.default_routing_key().map(|s| s.to_owned());
        }
        if self.routing_key.is_some() {
            return;
        }
        self.routing_key = Some(match executor.default_routing_key() {
            Some(k) => RoutingKey::Value(Some(k.to_owned())),
            None => RoutingKey::Value(None),
        });
    }

    fn effective_routing_key(&self) -> RoutingKey {
        self.routing_key.clone().unwrap_or(RoutingKey::Value(None))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            subscription = Empty,
            aggregate_type = Empty,
            aggregate_id = Empty,
            event = Empty,
        )
    )]
    async fn process(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &[EventFilter],
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

            let res = executor
                .read(
                    Some(aggregators.to_vec()),
                    Some(self.effective_routing_key()),
                    Args::forward(self.chunk_size, cursor),
                )
                .await?;

            if res.edges.is_empty() {
                return Ok(false);
            }

            let timestamp = executor
                .latest_timestamp(
                    Some(aggregators.to_vec()),
                    Some(self.effective_routing_key()),
                )
                .await?;

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
                            "Subscription received shutdown signal, stopping gracefully"
                        );

                        return Ok(true);
                    }
                    drop(rx);
                }

                tracing::Span::current().record("aggregate_type", &event.node.aggregate_type);
                tracing::Span::current().record("aggregate_id", &event.node.aggregate_id);
                tracing::Span::current().record("event", &event.node.name);

                let all_key = format!("{}_all", event.node.aggregate_type);
                let key = format!("{}_{}", event.node.aggregate_type, event.node.name);
                let Some(handler) = self.handlers.get(&all_key).or(self.handlers.get(&key)) else {
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
                        timestamp.saturating_sub(event.node.timestamp),
                    )
                    .await?;
            }
        }
    }

    /// Disables retry-on-failure for this subscription.
    ///
    /// By default failed batches are retried with exponential backoff (see
    /// [`retry`](Self::retry)). Combine this with [`start`](Self::start) or
    /// [`run_once`](Self::run_once) to process without retries.
    pub fn no_retry(mut self) -> Self {
        self.retry = None;

        self
    }

    /// Starts a continuous background subscription.
    ///
    /// Returns a [`Subscription`] handle that can be used for graceful shutdown.
    /// The subscription runs in a spawned tokio task and polls for new events.
    #[tracing::instrument(skip_all, fields(
        subscription = self.key(),
        aggregate_type = tracing::field::Empty,
        aggregate_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn start(mut self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        self.resolve_routing_key(executor);
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
                            "Subscription received shutdown signal, stopping gracefully"
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

                        if !self.continue_on_error {
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

    /// Processes all currently pending events once, then returns.
    ///
    /// Unlike [`start`](Self::start), this does not run continuously or spawn a
    /// background task — it drains the events available now and returns. Pair with
    /// [`no_retry`](Self::no_retry) to run a single pass without retries.
    #[tracing::instrument(skip_all, fields(
        subscription = self.key(),
        aggregate_type = tracing::field::Empty,
        aggregate_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn run_once(&mut self, executor: &E) -> anyhow::Result<()> {
        self.resolve_routing_key(executor);
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

struct SkipHandler<E: AggregateEvent>(PhantomData<E>);

impl<E: Executor, EV: AggregateEvent + Send + Sync> Handler<E> for SkipHandler<EV> {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
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
