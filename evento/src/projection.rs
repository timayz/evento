use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::HashMap,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    time::Duration,
};
use tokio::time::{interval_at, Instant};
use ulid::Ulid;

use crate::{context, cursor::Args, Executor, ReadAggregator};

#[derive(Clone)]
pub enum RoutingKey {
    All,
    Value(Option<String>),
}

#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    context: context::RwContext,
    pub executor: &'a E,
}

impl<'a, E: Executor> Deref for Context<'a, E> {
    type Target = context::RwContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

pub trait Aggregator: Default {
    fn aggregator_type() -> &'static str;
}

pub trait Event: Aggregator {
    fn event_name() -> &'static str;
}

pub trait Handler<P: 'static, E: Executor>: Sync + Send {
    fn handle<'a>(
        &'a self,
        context: &'a Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    fn apply<'a>(
        &'a self,
        projection: &'a mut P,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    fn aggregator_type(&self) -> &'static str;
    fn event_name(&self) -> &'static str;
}

pub enum Action<'a, P: 'static, E: Executor> {
    Apply(&'a mut P),
    Handle(&'a Context<'a, E>),
}

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

impl<D: bincode::Decode<()>, M: bincode::Decode<()>> TryFrom<&crate::Event> for EventData<D, M> {
    type Error = bincode::error::DecodeError;

    fn try_from(value: &crate::Event) -> Result<Self, Self::Error> {
        let config = bincode::config::standard();

        let (data, _) = bincode::decode_from_slice(&value.data[..], config)?;
        let (metadata, _) = bincode::decode_from_slice(&value.metadata[..], config)?;
        Ok(EventData {
            data,
            metadata,
            event: value.clone(),
        })
    }
}

pub struct Projection<P: 'static, E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<P, E>>>,
}

impl<P: 'static, E: Executor> Projection<P, E> {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            handlers: HashMap::new(),
        }
    }

    pub fn handler<H: Handler<P, E> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregator_type(), h.event_name());
        if self.handlers.insert(key.to_owned(), Box::new(h)).is_some() {
            panic!("Cannot register event handler: key {} already exists", key);
        }
        self
    }

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
            aggregators,
            handlers: self.handlers,
            context: Default::default(),
        }
    }

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
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LoadResult<A> {
    pub item: A,
    pub version: i32,
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

pub type OptionLoadResult<A> = Option<LoadResult<A>>;

pub trait Snapshot: Sized {
    fn restore<'a>(
        context: &'a context::RwContext,
        id: String,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OptionLoadResult<Self>>> + Send + 'a>>;
}

pub struct LoadBuilder<P: Snapshot + Default + 'static, E: Executor> {
    key: String,
    id: String,
    aggregators: HashMap<String, String>,
    handlers: HashMap<String, Box<dyn Handler<P, E>>>,
    context: context::RwContext,
}

impl<P: Snapshot + Default + 'static, E: Executor> LoadBuilder<P, E> {
    pub fn data<D: Send + Sync + 'static>(&mut self, v: D) -> &mut Self {
        self.context.insert(v);

        self
    }

    pub fn aggregator<A: Aggregator>(&mut self, id: impl Into<String>) -> &mut Self {
        self.aggregators
            .insert(A::aggregator_type().to_owned(), id.into());

        self
    }

    pub async fn execute(&self, executor: &E) -> anyhow::Result<Option<LoadResult<P>>> {
        let context = Context {
            context: self.context.clone(),
            executor,
        };

        let cursor = executor.get_subscriber_cursor(self.key.to_owned()).await?;
        let loaded = P::restore(&context, self.id.to_owned()).await?;
        let has_loaded = loaded.is_some();
        let mut snapshot = loaded.unwrap_or_default();

        let read_aggregators = self
            .handlers
            .values()
            .map(|h| match self.aggregators.get(h.aggregator_type()) {
                Some(id) => ReadAggregator {
                    aggregator_type: h.aggregator_type().to_owned(),
                    aggregator_id: Some(id.to_owned()),
                    name: None,
                },
                _ => ReadAggregator::event(h.aggregator_type(), h.event_name()),
            })
            .collect::<Vec<_>>();

        let events = executor
            .read(
                Some(read_aggregators.to_vec()),
                None,
                Args::forward(100, cursor.clone()),
            )
            .await?;

        for event in events.edges.iter() {
            let key = format!("{}_{}", event.node.aggregator_type, event.node.name);
            let Some(handler) = self.handlers.get(&key) else {
                tracing::debug!("No handler found for {}/{key}", self.key);
                continue;
            };

            handler.apply(&mut snapshot, &event.node).await?;
        }

        if events.page_info.has_next_page {
            anyhow::bail!("Too busy");
        }

        if let Some(event) = events.edges.last() {
            snapshot.version = event.node.version;
            snapshot.routing_key = event.node.routing_key.to_owned();

            return Ok(Some(snapshot));
        }

        if !has_loaded {
            return Ok(None);
        }

        let events = executor
            .read(
                Some(read_aggregators.to_vec()),
                None,
                Args::backward(1, None),
            )
            .await?;

        if let Some(event) = events.edges.first() {
            snapshot.version = event.node.version;
            snapshot.routing_key = event.node.routing_key.to_owned();

            return Ok(Some(snapshot));
        }

        Ok(None)
    }
}

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
}

impl<P, E: Executor + 'static> SubscriptionBuilder<P, E> {
    pub fn accept_failure(mut self) -> Self {
        self.is_accept_failure = true;

        self
    }

    pub fn chunk_size(mut self, v: u16) -> Self {
        self.chunk_size = v;

        self
    }

    pub fn delay(mut self, v: Duration) -> Self {
        self.delay = Some(v);

        self
    }

    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = RoutingKey::Value(Some(v.into()));

        self
    }

    pub fn retry(mut self, v: u8) -> Self {
        self.retry = Some(v);

        self
    }

    fn without_retry(mut self) -> Self {
        self.retry = None;

        self
    }

    pub fn all(mut self) -> Self {
        self.routing_key = RoutingKey::All;

        self
    }

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
                    name: Some(h.event_name().to_owned()),
                },
                _ => ReadAggregator::event(h.aggregator_type(), h.event_name()),
            })
            .collect()
    }

    fn key(&self) -> String {
        if let RoutingKey::Value(Some(ref key)) = self.routing_key {
            return format!("{key}.{}", self.key);
        }

        self.key.to_owned()
    }

    async fn process(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &[ReadAggregator],
        mut rx: Option<&mut tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<()> {
        let mut interval = interval_at(
            Instant::now() - Duration::from_millis(400),
            Duration::from_millis(300),
        );

        loop {
            interval.tick().await;

            if !executor.is_subscriber_running(self.key(), *id).await? {
                return Ok(());
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
                return Ok(());
            }

            let context = Context {
                context: self.context.clone(),
                executor,
            };

            for event in res.edges {
                if let Some(rx) = rx.as_mut() {
                    if rx.try_recv().is_ok() {
                        tracing::info!(
                            key = self.key(),
                            "Subscription received shutdown signal, stopping gracefull"
                        );

                        return Ok(());
                    }
                }

                tracing::Span::current().record("aggregator_type", &event.node.aggregator_type);
                tracing::Span::current().record("aggregator_id", &event.node.aggregator_id);
                tracing::Span::current().record("event", &event.node.name);

                let key = format!("{}_{}", event.node.aggregator_type, event.node.name);
                let Some(handler) = self.handlers.get(&key) else {
                    panic!("No handler found for {}/{key}", self.key());
                };

                handler.handle(&context, &event.node).await?;

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

    pub async fn unretry_start(self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        self.without_retry().start(executor).await
    }

    pub async fn start(self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let executor = executor.clone();
        let id = Ulid::new();
        let subscription_id = id;

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
                if shutdown_rx.try_recv().is_ok() {
                    tracing::info!(
                        key = self.key(),
                        "Subscription received shutdown signal, stopping gracefull"
                    );

                    break;
                }

                interval.tick().await;

                let _ = tracing::error_span!(
                    "start",
                    key = self.key(),
                    aggregator_type = tracing::field::Empty,
                    aggregator_id = tracing::field::Empty,
                    event = tracing::field::Empty,
                )
                .entered();

                let result = match self.retry {
                    Some(retry) => {
                        (|| async { self.process(&executor, &id, &read_aggregators, None).await })
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
                    _ => self.process(&executor, &id, &read_aggregators, None).await,
                };

                let Err(err) = result else {
                    continue;
                };

                tracing::error!(error = %err, "Failed to process event");

                if !self.is_accept_failure {
                    break;
                }
            }
        });

        Ok(Subscription {
            id: subscription_id,
            task_handle,
            shutdown_tx,
        })
    }

    pub async fn unretry_execute(self, executor: &E) -> anyhow::Result<()> {
        self.without_retry().execute(executor).await
    }

    pub async fn execute(&self, executor: &E) -> anyhow::Result<()> {
        let id = Ulid::new();

        executor
            .upsert_subscriber(self.key(), id.to_owned())
            .await?;

        let read_aggregators = self.read_aggregators();

        let _ = tracing::error_span!(
            "execute",
            key = self.key(),
            aggregator_type = tracing::field::Empty,
            aggregator_id = tracing::field::Empty,
            event = tracing::field::Empty,
        )
        .entered();

        match self.retry {
            Some(retry) => {
                (|| async { self.process(executor, &id, &read_aggregators, None).await })
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
            _ => self.process(executor, &id, &read_aggregators, None).await,
        }
    }
}

#[derive(Debug)]
pub struct Subscription {
    pub id: Ulid,
    task_handle: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl Subscription {
    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown_tx.send(());

        self.task_handle.await
    }
}
