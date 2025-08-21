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
    #[error("read >> {0}")]
    ReadError(#[from] super::ReadError),

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ulid.decode >> {0}")]
    UlidDecode(#[from] ulid::DecodeError),
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

#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    inner: Arc<Mutex<context::Context>>,
    key: String,
    cursor: Value,
    lag: i64,
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
    aggregator_types: HashSet<String>,
    chunk_size: u16,
    context: Arc<Mutex<context::Context>>,
}

pub fn subscribe<E: Executor>(key: impl Into<String>) -> SubscribeBuilder<E> {
    SubscribeBuilder {
        id: Ulid::new(),
        key: key.into(),
        delay: None,
        routing_key: RoutingKey::Value(None),
        handlers: HashMap::new(),
        aggregator_types: HashSet::new(),
        chunk_size: 300,
        context: Arc::default(),
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

    pub fn all(mut self) -> Self {
        self.routing_key = RoutingKey::All;

        self
    }

    pub fn aggregator<A: Aggregator>(mut self) -> Self {
        self.aggregator_types.insert(A::name().to_owned());

        self
    }

    #[cfg(feature = "handler")]
    pub fn handler<H: SubscribeHandler<E> + 'static>(mut self, handler: H) -> Self {
        self.aggregator_types
            .insert(handler.aggregator_type().to_owned());

        let key = format!("{}-{}", handler.aggregator_type(), handler.event_name());
        self.handlers.insert(key, Box::new(handler));

        self
    }

    pub async fn init(&self, executor: &E) -> Result<(), SubscribeError> {
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
                lag: (timestamp - edge.node.timestamp),
                cursor: edge.cursor.to_owned(),
                event: edge.node.clone(),
            })
            .collect())
    }

    #[cfg(feature = "handler")]
    pub async fn run(self, executor: &E) -> Result<(), SubscribeError> {
        self.init(executor).await?;

        let executor = executor.clone();
        let start = self
            .delay
            .map(|d| Instant::now() + d)
            .unwrap_or_else(Instant::now);

        tokio::spawn(async move {
            let mut interval = interval_at(start, Duration::from_millis(300));
            loop {
                interval.tick().await;

                let Ok(data) = (|| async { self.read(&executor).await })
                    .retry(ExponentialBuilder::default())
                    .sleep(tokio::time::sleep)
                    .notify(|err, dur| {
                        tracing::error!(
                            "SubscribeBuilder.run().self.read() '{err}' sleeping='{dur:?}'"
                        );
                    })
                    .await
                else {
                    continue;
                };

                for item in data {
                    let key = format!("{}-{}", item.event.aggregator_type, item.event.name);
                    let Some(handler) = self.handlers.get(&key) else {
                        tracing::error!(
                            "subscriber='{}' id='{}' type='{}' event_name='{}' error='Not handled'",
                            item.key,
                            item.event.id,
                            item.event.aggregator_type,
                            item.event.name,
                        );

                        return;
                    };

                    let Ok(running) = (|| async {self.is_subscriber_running(&executor).await})
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .notify(|err, dur| {
                            tracing::error!(
                                "SubscribeBuilder.run().self.is_subscriber_running '{err}' sleeping='{dur:?}'"
                            );
                        }).await
                    else {
                        break;
                    };

                    if !running {
                        break;
                    }

                    let Ok(_) = (|| async { handler.handle(&item).await })
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .notify(|err, dur| {
                            tracing::error!(
                                "subscriber='{}' id='{}' type='{}' event_name='{}' error='{err}' sleeping='{dur:?}'",
                                item.key,
                                item.event.id,
                                item.event.aggregator_type,
                                item.event.name,
                            );
                        })
                        .await
                    else {
                        break;
                    };

                    if let Err(err) = item.acknowledge().await {
                        tracing::error!("acknowledge '{err}'");
                        break;
                    }

                    tracing::debug!(
                        "subscriber='{}' id='{}' type='{}' event_name='{}'",
                        item.key,
                        item.event.id,
                        item.event.aggregator_type,
                        item.event.name,
                    );
                }
            }
        });

        // @TODO: return struct to wait end of using mcsp that will send something when !running
        // can be use in gracefull shutdow axum fn for example

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

                let mut interval = interval_at(start, Duration::from_millis(300));

                loop {
                    if let Some(item) = data.next() {
                        return Some((item, (sub, executor, data)));
                    }

                    interval.tick().await;

                    let Ok(r_data) = (|| async { self.read(executor).await })
                        .retry(ExponentialBuilder::default())
                        .sleep(tokio::time::sleep)
                        .notify(|err, dur| {
                            tracing::error!(
                                "SubscribeBuilder.stream().self.read() '{err}' sleeping='{dur:?}'"
                            );
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

#[derive(Default)]
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
