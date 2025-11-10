#[cfg(feature = "handler")]
use backon::{ExponentialBuilder, Retryable};

#[cfg(feature = "stream")]
use futures_util::stream::{self, Stream};

#[cfg(any(feature = "stream", feature = "handler"))]
use tokio::time::{interval_at, Instant};
use ulid::Ulid;

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;

use crate::{
    context,
    cursor::{Args, Value},
    Aggregator, AggregatorName, Event, Executor,
};

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("duplicate handler {0:?}")]
    DuplicateHandler(HashSet<String>),

    #[error("read >> {0}")]
    ReadError(#[from] super::ReadError),

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ulid.decode >> {0}")]
    UlidDecode(#[from] ulid::DecodeError),

    #[error("ulid.decode >> {0}")]
    Acknowledge(#[from] AcknowledgeError),
}

#[derive(Debug, Error)]
pub enum AcknowledgeError {
    #[error("{0}")]
    Unknown(#[from] anyhow::Error),
}

#[derive(Clone)]
pub enum RoutingKey {
    All,
    Value(Option<String>),
}

/// Handle for managing a running subscription
///
/// This handle allows you to gracefully shutdown the subscription and wait for it to complete.
/// Useful for implementing graceful shutdown in web servers and other applications.
#[cfg(feature = "handler")]
#[derive(Debug)]
pub struct SubscriptionHandle {
    /// Handle to the spawned subscription task
    task_handle: tokio::task::JoinHandle<()>,
    /// Shutdown signal sender
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

#[cfg(feature = "handler")]
impl SubscriptionHandle {
    /// Signal the subscription to shutdown gracefully
    ///
    /// This sends a shutdown signal to the subscription. The subscription will finish
    /// processing the current event and then stop.
    pub fn shutdown(self) -> Result<tokio::task::JoinHandle<()>, String> {
        // Send shutdown signal (ignore error if receiver is already dropped)
        let _ = self.shutdown_tx.send(());
        Ok(self.task_handle)
    }

    /// Wait for the subscription to complete
    ///
    /// This waits for the subscription task to finish execution.
    pub async fn wait(self) -> Result<(), tokio::task::JoinError> {
        self.task_handle.await
    }

    /// Signal shutdown and wait for completion
    ///
    /// This is a convenience method that calls shutdown() and then wait().
    pub async fn shutdown_and_wait(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handle = self
            .shutdown()
            .map_err(|_| "Failed to send shutdown signal")?;
        handle
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    inner: Arc<Mutex<context::Context>>,
    key: String,
    cursor: Value,
    lag: u32,
    pub event: Event,
    pub executor: &'a E,
}

impl<'a, E: Executor> Context<'a, E> {
    pub fn extract<T: Clone + 'static>(&self) -> T {
        let context = self.inner.lock().expect("Unable to lock Context.inner");
        context.extract::<T>().clone()
    }

    pub async fn acknowledge(&self) -> Result<(), AcknowledgeError> {
        self.executor
            .acknowledge(self.key.to_owned(), self.cursor.to_owned(), self.lag)
            .await
    }
}

pub trait SubscribeHandler<E: Executor>: Send + Sync {
    fn handle<'async_trait>(
        &'async_trait self,
        context: &'async_trait Context<'_, E>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait;
    fn aggregator_type(&self) -> &'static str;
    fn event_name(&self) -> &'static str;
}

pub struct SubscribeBuilder<E: Executor> {
    id: Ulid,
    key: String,
    routing_key: RoutingKey,
    #[allow(dead_code)]
    delay: Option<Duration>,
    #[allow(dead_code)]
    handlers: HashMap<String, Box<dyn SubscribeHandler<E>>>,
    duplicate_handlers: HashSet<String>,
    aggregator_types: HashSet<String>,
    chunk_size: u16,
    backon: bool,
    #[cfg(feature = "handler")]
    enforce_handler: bool,
    context: Arc<Mutex<context::Context>>,
}

/// Create a new event subscription builder
///
/// Creates a builder for setting up continuous event processing. Subscriptions
/// listen to events from specified aggregates and process them with registered handlers.
///
/// # Parameters
///
/// - `key`: A unique identifier for this subscription (used for tracking progress)
///
/// # Examples
///
/// ```no_run
/// use evento::subscribe;
/// # use evento::*;
/// # use bincode::{Encode, Decode};
/// # #[derive(AggregatorName, Encode, Decode)]
/// # struct UserCreated { name: String }
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User;
/// # #[evento::aggregator]
/// # impl User {}
/// # #[evento::handler(User)]
/// # async fn on_user_created<E: Executor>(
/// #     context: &Context<'_, E>,
/// #     event: EventDetails<UserCreated>,
/// # ) -> anyhow::Result<()> { Ok(()) }
///
/// async fn setup_subscription(executor: evento::Sqlite) -> anyhow::Result<()> {
///     subscribe("user-handlers")
///         .aggregator::<User>()
///         .handler(on_user_created())
///         .run(&executor)
///         .await?;
///     
///     Ok(())
/// }
/// ```
pub fn subscribe<E: Executor>(key: impl Into<String>) -> SubscribeBuilder<E> {
    SubscribeBuilder {
        id: Ulid::new(),
        key: key.into(),
        delay: None,
        routing_key: RoutingKey::Value(None),
        handlers: HashMap::new(),
        duplicate_handlers: HashSet::new(),
        aggregator_types: HashSet::new(),
        chunk_size: 300,
        context: Arc::default(),
        backon: true,
        #[cfg(feature = "handler")]
        enforce_handler: true,
    }
}

impl<E: Executor + Clone> SubscribeBuilder<E> {
    pub fn chunk_size(mut self, v: u16) -> Self {
        self.chunk_size = v;

        self
    }

    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        let mut context = self
            .context
            .lock()
            .expect("Unable to lock SubscribeBuilder.context");

        context.insert(v);
        drop(context);

        self
    }

    #[cfg(feature = "handler")]
    pub fn delay(mut self, v: Duration) -> Self {
        self.delay = Some(v);

        self
    }

    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = RoutingKey::Value(Some(v.into()));

        self
    }

    fn backoff(mut self) -> Self {
        self.backon = false;

        self
    }

    #[cfg(feature = "handler")]
    pub fn handler_check_off(mut self) -> Self {
        self.enforce_handler = false;

        self
    }

    pub fn all(mut self) -> Self {
        self.routing_key = RoutingKey::All;

        self
    }

    /// Subscribe to events for a specific aggregator type
    ///
    /// This method allows subscribing to all events for a given aggregator without
    /// specifying individual handlers. Requires the `stream` feature to be enabled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use evento::{subscribe, EventDetails, AggregatorName};
    /// # use serde::{Serialize, Deserialize};
    /// # use bincode::{Encode, Decode};
    /// #
    /// # #[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
    /// # struct User {
    /// #     name: String,
    /// # }
    /// #
    /// # #[derive(AggregatorName, Encode, Decode)]
    /// # struct UserCreated {
    /// #     name: String,
    /// # }
    /// #
    /// # #[evento::aggregator]
    /// # impl User {
    /// #     async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
    /// #         self.name = event.data.name;
    /// #         Ok(())
    /// #     }
    /// # }
    /// #
    /// # async fn example(executor: &evento::Sqlite) -> anyhow::Result<()> {
    /// subscribe("user-stream")
    ///     .aggregator::<User>()
    ///     .run(executor)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "stream")]
    pub fn aggregator<A: Aggregator>(mut self) -> Self {
        self.aggregator_types.insert(A::name().to_owned());

        self
    }

    #[cfg(feature = "handler")]
    pub fn handler<H: SubscribeHandler<E> + 'static>(mut self, handler: H) -> Self {
        self.aggregator_types
            .insert(handler.aggregator_type().to_owned());

        let key = format!("{}-{}", handler.aggregator_type(), handler.event_name());
        if self
            .handlers
            .insert(key.to_owned(), Box::new(handler))
            .is_some()
        {
            self.duplicate_handlers.insert(key);
        };

        self
    }

    #[cfg(feature = "handler")]
    pub fn skip<A: Aggregator + 'static, N: AggregatorName + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<A, N>(PhantomData, PhantomData))
    }

    pub async fn init(&self, executor: &E) -> Result<(), SubscribeError> {
        if !self.duplicate_handlers.is_empty() {
            let values = self.duplicate_handlers.iter().cloned().collect();
            return Err(SubscribeError::DuplicateHandler(values));
        }

        executor
            .upsert_subscriber(self.key.to_owned(), self.id)
            .await?;

        Ok(())
    }

    pub async fn is_subscriber_running(&self, executor: &E) -> Result<bool, SubscribeError> {
        executor
            .is_subscriber_running(self.key.to_owned(), self.id)
            .await
    }

    pub async fn read<'a>(&self, executor: &'a E) -> Result<Vec<Context<'a, E>>, SubscribeError> {
        let cursor = executor.get_subscriber_cursor(self.key.to_owned()).await?;

        let timestamp = executor
            .read(
                self.aggregator_types.to_owned(),
                self.routing_key.clone(),
                Args::backward(1, None),
            )
            .await?
            .edges
            .last()
            .map(|e| e.node.timestamp)
            .unwrap_or_default();

        let res = executor
            .read(
                self.aggregator_types.to_owned(),
                self.routing_key.clone(),
                Args::forward(self.chunk_size, cursor),
            )
            .await?;

        Ok(res
            .edges
            .iter()
            .map(|edge| Context {
                inner: self.context.clone(),
                key: self.key.to_owned(),
                executor,
                lag: (timestamp - edge.node.timestamp) as u32,
                cursor: edge.cursor.to_owned(),
                event: edge.node.clone(),
            })
            .collect())
    }

    #[cfg(feature = "handler")]
    pub async fn run(self, executor: &E) -> Result<SubscriptionHandle, SubscribeError> {
        self.init(executor).await?;

        let executor = executor.clone();
        let start = self
            .delay
            .map(|d| Instant::now() + d)
            .unwrap_or_else(Instant::now);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        let task_handle = tokio::spawn(async move {
            let mut interval = interval_at(
                start - Duration::from_millis(400),
                Duration::from_millis(300),
            );
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    tracing::info!(
                        key = self.key,
                        "Subscription received shutdown signal, stopping gracefull"
                    );
                    break;
                }
                interval.tick().await;

                let data = (|| async { self.read(&executor).await })
                    .retry(ExponentialBuilder::default())
                    .when(|_| self.backon)
                    .sleep(tokio::time::sleep)
                    .notify(|err, dur| {
                        tracing::error!(
                            error_message = %err,
                            duration = ?dur,
                            "Failed to read events"
                        );
                    })
                    .await;

                let data = match data {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!(
                            error_message = %e,
                            "Failed to read events"
                        );
                        return;
                    }
                };

                for item in data {
                    let key = format!("{}-{}", item.event.aggregator_type, item.event.name);
                    let Some(handler) = self.handlers.get(&key) else {
                        if self.enforce_handler {
                            tracing::error!(
                                key = item.key,
                                aggregator = item.event.aggregator_type,
                                event = item.event.name,
                                "No event handler define",
                            );
                            return;
                        }

                        tracing::warn!(
                            key = item.key,
                            aggregator = item.event.aggregator_type,
                            event = item.event.name,
                            "No event handler define",
                        );

                        continue;
                    };

                    let running = (|| async { self.is_subscriber_running(&executor).await })
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .when(|_| self.backon)
                        .notify(|err, dur| {
                            tracing::error!(
                                error_message = %err,
                                duration = ?dur,
                                "Failed to check if subscriber is running"
                            )
                        })
                        .await;

                    let running = match running {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::error!(
                                error_message = %e,
                                "Failed to check if subscriber is running"
                            );
                            return;
                        }
                    };

                    if !running {
                        break;
                    }

                    if let Err(e) = (|| async { handler.handle(&item).await })
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .when(|_| self.backon)
                        .notify(|err, dur| {
                            tracing::error!(
                                key = item.key,
                                aggregator = item.event.aggregator_type,
                                event = item.event.name,
                                error_message = %err,
                                duration = ?dur,
                                "Failed to handle event"
                            );
                        })
                        .await
                    {
                        tracing::error!(error_message = %e, "Failed to handle event");
                        return;
                    }

                    if let Err(err) = (async || item.acknowledge().await)
                        .retry(ExponentialBuilder::default())
                        .when(|_| self.backon)
                        .sleep(tokio::time::sleep)
                        .notify(|err, dur| {
                            tracing::error!(error_message = %err, duration = ?dur, "Failed to acknowledge event");
                        })
                        .await
                    {
                        tracing::error!(error_message = %err, "Failed to acknowledge event");
                        break;
                    }

                    tracing::info!(
                        key = item.key,
                        aggregator = item.event.aggregator_type,
                        event = item.event.name,
                        "Event is handled"
                    );
                }
            }
        });

        Ok(SubscriptionHandle {
            task_handle,
            shutdown_tx,
        })
    }

    #[cfg(feature = "handler")]
    pub async fn unretry_run(self, executor: &E) -> Result<(), SubscribeError> {
        self.backoff().oneshot(executor).await
    }

    #[cfg(feature = "handler")]
    pub async fn unretry_oneshot(self, executor: &E) -> Result<(), SubscribeError> {
        self.backoff().oneshot(executor).await
    }

    #[cfg(feature = "handler")]
    #[deprecated(since = "1.4.0", note = "use oneshot instead")]
    pub async fn run_once(self, executor: &E) -> Result<(), SubscribeError> {
        self.oneshot(executor).await
    }

    #[cfg(feature = "handler")]
    pub async fn oneshot(self, executor: &E) -> Result<(), SubscribeError> {
        self.init(executor).await?;

        let executor = executor.clone();

        let mut interval = interval_at(
            Instant::now() - Duration::from_millis(400),
            Duration::from_millis(300),
        );
        loop {
            interval.tick().await;

            let data = (|| async { self.read(&executor).await })
                .retry(ExponentialBuilder::default())
                .sleep(tokio::time::sleep)
                .when(|_| self.backon)
                .notify(|err, dur| {
                    tracing::error!(
                        error_message = %err,
                        duration = ?dur,
                        "Failed to read events"
                    );
                })
                .await?;

            if data.is_empty() {
                break;
            }

            for item in data {
                let key = format!("{}-{}", item.event.aggregator_type, item.event.name);
                let Some(handler) = self.handlers.get(&key) else {
                    if !self.enforce_handler {
                        tracing::warn!(
                            key = item.key,
                            aggregator = item.event.aggregator_type,
                            event = item.event.name,
                            "No event handler define",
                        );
                        continue;
                    }

                    return Err(SubscribeError::Unknown(anyhow::anyhow!(
                        "No event handler define for {} {}",
                        item.event.aggregator_type,
                        item.event.name
                    )));
                };

                let running = (|| async { self.is_subscriber_running(&executor).await })
                    .retry(ExponentialBuilder::default())
                    .sleep(tokio::time::sleep)
                    .when(|_| self.backon)
                    .notify(|err, dur| {
                        tracing::error!(
                        error_message = %err,
                        duration = ?dur,
                        "Failed to check if subscriber running");
                    })
                    .await?;

                if !running {
                    break;
                }

                (|| async { handler.handle(&item).await })
                    .retry(ExponentialBuilder::default())
                    .sleep(tokio::time::sleep)
                    .when(|_| self.backon)
                    .notify(|err, dur| {
                        tracing::error!(
                            key = item.key,
                            aggregator = item.event.aggregator_type,
                            event = item.event.name,
                            error_message = %err,
                            duration = ?dur,
                            "Failed to handle event",
                        );
                    })
                    .await?;

                (async || item.acknowledge().await)
                    .retry(ExponentialBuilder::default())
                    .sleep(tokio::time::sleep)
                    .when(|_| self.backon)
                    .notify(|err, dur| {
                        tracing::error!(
                        error_message = %err,
                        duration = ?dur,
                        "Failed to acknowledge event");
                    })
                    .await?;

                tracing::info!(
                    key = item.key,
                    aggregator = item.event.aggregator_type,
                    event = item.event.name,
                    "Event is handled"
                );
            }
        }

        Ok(())
    }

    #[cfg(feature = "stream")]
    pub async fn stream<'a>(
        &self,
        executor: &'a E,
    ) -> Result<impl Stream<Item = Context<'a, E>> + use<'a, '_, E>, SubscribeError> {
        self.init(executor).await?;
        Ok(stream::unfold(
            (self, executor, Vec::<Context<'a, E>>::new().into_iter()),
            move |(sub, executor, mut data)| async move {
                let start = sub
                    .delay
                    .map(|d| Instant::now() + d)
                    .unwrap_or_else(Instant::now);

                let mut interval = interval_at(
                    start - Duration::from_millis(400),
                    Duration::from_millis(300),
                );

                loop {
                    if let Some(item) = data.next() {
                        return Some((item, (sub, executor, data)));
                    }

                    interval.tick().await;

                    let Ok(r_data) = (|| async { self.read(executor).await })
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .when(|_| self.backon)
                        .notify(|err, dur| {
                            tracing::error!(error_message = %err, duration = ?dur, "Failed to read events");
                        })
                        .await
                    else {
                        return None;
                    };

                    data = r_data.into_iter();
                }
            },
        ))
    }
}

pub struct SkipHandler<A: Aggregator, N: AggregatorName>(PhantomData<A>, PhantomData<N>);

impl<E: Executor, A: Aggregator, N: AggregatorName + Send + Sync> SubscribeHandler<E>
    for SkipHandler<A, N>
{
    fn handle<'async_trait>(
        &'async_trait self,
        _context: &'async_trait Context<'_, E>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait,
    {
        Box::pin(async { Ok(()) })
    }

    fn aggregator_type(&self) -> &'static str {
        A::name()
    }

    fn event_name(&self) -> &'static str {
        N::name()
    }
}
