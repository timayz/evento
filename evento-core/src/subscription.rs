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

use crate::{context, cursor::Args, Aggregator, AggregatorEvent, Executor, ReadAggregator};

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

    /// Returns the aggregator type this handler processes.
    fn aggregator_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
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
pub struct SubscriptionBuilder<E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<E>>>,
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
            is_accept_failure: false,
            routing_key: RoutingKey::Value(None),
            aggregators: Default::default(),
            shutdown_rx: None,
        }
    }

    /// Enables safety checks for unhandled events.
    ///
    /// When enabled, processing fails if an event is encountered without a handler.
    pub fn safety_check(mut self) -> Self {
        self.safety_disabled = false;

        self
    }

    /// Registers an event handler with this subscription.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<E> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregator_type(), h.event_name());
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
    pub fn skip<EV: AggregatorEvent + Send + Sync + 'static>(self) -> Self {
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

                let all_key = format!("{}_all", event.node.aggregator_type);
                let key = format!("{}_{}", event.node.aggregator_type, event.node.name);
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

struct SkipHandler<E: AggregatorEvent>(PhantomData<E>);

impl<E: Executor, EV: AggregatorEvent + Send + Sync> Handler<E> for SkipHandler<EV> {
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
