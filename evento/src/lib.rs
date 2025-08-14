pub mod context;
pub mod cursor;
#[cfg(feature = "sqlite")]
pub mod sql;

use chrono::{DateTime, Utc};
#[cfg(feature = "macro")]
pub use evento_macro::*;

#[cfg(feature = "handler")]
use backon::{ExponentialBuilder, Retryable};
#[cfg(feature = "stream")]
use futures_util::stream::{self, Stream};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
#[cfg(any(feature = "stream", feature = "handler"))]
use tokio::time::{interval_at, Instant};
use ulid::Ulid;

use crate::cursor::{Args, Cursor, ReadResult, Value};

pub mod prelude {
    #[cfg(feature = "stream")]
    pub use tokio_stream::StreamExt;
}

pub struct EventData<D, M> {
    pub details: Event,
    pub data: D,
    pub metadata: M,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventCursor {
    pub i: Ulid,
    pub v: i32,
    pub t: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub id: Ulid,
    pub aggregator_id: String,
    pub aggregator_type: String,
    pub version: i32,
    pub name: String,
    pub routing_key: Option<String>,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl Event {
    pub fn to_data<D: AggregatorName + DeserializeOwned, M: DeserializeOwned>(
        &self,
    ) -> Result<Option<EventData<D, M>>, ciborium::de::Error<std::io::Error>> {
        if D::name() != self.name {
            return Ok(None);
        }

        let data = ciborium::from_reader(&self.data[..])?;
        let metadata = ciborium::from_reader(&self.metadata[..])?;

        Ok(Some(EventData {
            data,
            metadata,
            details: self.clone(),
        }))
    }
}

impl Cursor for Event {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        EventCursor {
            i: self.id,
            v: self.version,
            t: self.timestamp,
        }
    }
}

pub trait Aggregator:
    Default + Send + Sync + Serialize + DeserializeOwned + Clone + AggregatorName + Debug
{
    fn aggregate<'async_trait>(
        &'async_trait mut self,
        event: &'async_trait Event,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait;
    fn revision() -> &'static str;
}

pub trait AggregatorName {
    fn name() -> &'static str;
}

#[derive(Clone)]
pub enum RoutingKey {
    All,
    Value(Option<String>),
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;

    async fn get_event<A: Aggregator>(&self, cursor: Value) -> Result<Event, ReadError>;

    async fn read_by_aggregator<A: Aggregator>(
        &self,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError>;

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError>;

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError>;

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError>;

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError>;

    async fn get_snapshot<A: Aggregator>(
        &self,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError>;

    async fn save_snapshot<A: Aggregator>(
        &self,
        id: String,
        data: Vec<u8>,
        cursor: cursor::Value,
    ) -> Result<(), WriteError>;

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: i64,
    ) -> Result<(), AcknowledgeError>;
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("not found")]
    NotFound,

    #[error("too many events to aggregate")]
    TooManyEvents,

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ciborium.ser >> {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("ciborium.de >> {0}")]
    CiboriumDe(#[from] ciborium::de::Error<std::io::Error>),

    #[error("base64 decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("write: {0}")]
    Write(#[from] WriteError),
}

#[derive(Debug, Clone)]
pub struct LoadResult<A: Aggregator> {
    pub item: A,
    pub event: Event,
}

pub async fn load<A: Aggregator, E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> Result<LoadResult<A>, ReadError> {
    let id = id.into();
    let (mut aggregator, mut cursor) = match executor.get_snapshot::<A>(id.to_owned()).await? {
        Some((data, cursor)) => {
            let aggregator: A = ciborium::from_reader(&data[..])?;
            (aggregator, Some(cursor))
        }
        _ => (A::default(), None),
    };

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut loop_count = 0;

    loop {
        let events = executor
            .read_by_aggregator::<A>(id.to_owned(), Args::forward(1000, cursor.clone()))
            .await?;

        for event in events.edges.iter() {
            aggregator.aggregate(&event.node).await?;
        }

        if let (Some(event), Some(cursor)) = (events.edges.last().cloned(), cursor.clone()) {
            let mut data = vec![];
            ciborium::into_writer(&aggregator, &mut data)?;

            executor
                .save_snapshot::<A>(event.node.aggregator_id, data, cursor)
                .await?;
        }

        if !events.page_info.has_next_page {
            let event = match (cursor, events.edges.last()) {
                (_, Some(event)) => event.node.clone(),
                (Some(cursor), None) => executor.get_event::<A>(cursor).await?,
                _ => return Err(ReadError::NotFound),
            };

            return Ok(LoadResult {
                item: aggregator,
                event,
            });
        }

        cursor = events.page_info.end_cursor;

        interval.tick().await;

        loop_count += 1;
        if loop_count > 10 {
            return Err(ReadError::TooManyEvents);
        }
    }
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("invalid original version")]
    InvalidOriginalVersion,

    #[error("missing data")]
    MissingData,

    #[error("missing metadata")]
    MissingMetadata,

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ciborium.ser >> {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("systemtime >> {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

pub struct SaveBuilder<A: Aggregator> {
    aggregator_id: String,
    aggregator_type: String,
    aggregator: A,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: i32,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Option<Vec<u8>>,
}

impl<A: Aggregator> SaveBuilder<A> {
    pub fn new(aggregator: A, aggregator_id: impl Into<String>) -> SaveBuilder<A> {
        SaveBuilder {
            aggregator_id: aggregator_id.into(),
            aggregator,
            aggregator_type: A::name().to_owned(),
            routing_key: None,
            routing_key_locked: false,
            original_version: 0,
            data: Vec::default(),
            metadata: None,
        }
    }

    pub fn original_version(mut self, v: u16) -> Self {
        self.original_version = v as i32;

        self
    }

    pub fn routing_key(self, v: impl Into<String>) -> Self {
        self.routing_key_opt(Some(v.into()))
    }

    pub fn routing_key_opt(mut self, v: Option<String>) -> Self {
        if !self.routing_key_locked {
            self.routing_key = v;
            self.routing_key_locked = true;
        }

        self
    }

    pub fn metadata<M: Serialize>(
        mut self,
        v: &M,
    ) -> Result<Self, ciborium::ser::Error<std::io::Error>> {
        let mut metadata = Vec::new();
        ciborium::into_writer(v, &mut metadata)?;
        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn data<D: Serialize + AggregatorName>(
        mut self,
        v: &D,
    ) -> Result<Self, ciborium::ser::Error<std::io::Error>> {
        let mut data = Vec::new();
        ciborium::into_writer(v, &mut data)?;
        self.data.push((D::name(), data));

        Ok(self)
    }

    pub async fn commit<E: Executor>(&self, executor: &E) -> Result<String, WriteError> {
        let mut aggregator = self.aggregator.clone();
        let mut version = self.original_version;

        let Some(metadata) = &self.metadata else {
            return Err(WriteError::MissingMetadata);
        };

        let mut events = vec![];
        let timestamp = Utc::now();

        for (name, data) in &self.data {
            version += 1;

            let event = Event {
                id: Ulid::new(),
                name: name.to_string(),
                data: data.to_vec(),
                metadata: metadata.to_vec(),
                timestamp,
                aggregator_id: self.aggregator_id.to_owned(),
                aggregator_type: self.aggregator_type.to_owned(),
                version,
                routing_key: self.routing_key.to_owned(),
            };

            aggregator.aggregate(&event).await?;
            events.push(event);
        }

        let Some(last_event) = events.last().cloned() else {
            return Err(WriteError::MissingData);
        };

        executor.write(events).await?;

        let mut data = vec![];
        ciborium::into_writer(&aggregator, &mut data)?;
        let cursor = last_event.serialize_cursor()?;

        executor
            .save_snapshot::<A>(last_event.aggregator_id, data, cursor)
            .await?;

        Ok(self.aggregator_id.to_owned())
    }
}

pub fn create<A: Aggregator>() -> SaveBuilder<A> {
    SaveBuilder::new(A::default(), Ulid::new())
}

pub fn save<A: Aggregator>(aggregator: LoadResult<A>) -> SaveBuilder<A> {
    SaveBuilder::new(aggregator.item, aggregator.event.aggregator_id)
        .original_version(aggregator.event.version as u16)
        .routing_key_opt(aggregator.event.routing_key)
}

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("read >> {0}")]
    ReadError(#[from] ReadError),

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
                lag: (timestamp - edge.node.timestamp).num_seconds(),
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
                        continue;
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
