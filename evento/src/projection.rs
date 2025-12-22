use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{context, cursor::Args, Executor, ReadAggregator};

#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    context: Arc<Mutex<context::Context>>,
    pub executor: &'a E,
}

impl<'a, E: Executor> Context<'a, E> {
    pub fn extract<T: Clone + 'static>(&self) -> T {
        let context = self
            .context
            .lock()
            .expect("Unable to lock projection/Context");

        context.extract::<T>().clone()
    }
}

pub trait Aggregator {
    fn aggregator_type() -> &'static str;
}

pub trait Event: Aggregator {
    fn event_name() -> &'static str;
}

pub trait Handler<E: Executor>: Sync + Send {
    fn handle<'a>(
        &'a mut self,
        context: &'a Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;
    fn aggregator_type(&self) -> &'static str;
    fn event_name(&self) -> &'static str;
    fn key(&self) -> String;
}

pub struct Projection<E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<E>>>,
}

impl<E: Executor> Projection<E> {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            handlers: HashMap::new(),
        }
    }

    pub fn handler<H: Handler<E> + 'static>(&mut self, h: H) -> &mut Self {
        let key = h.key();
        if self.handlers.insert(h.key(), Box::new(h)).is_some() {
            panic!("Cannot register event handler: key {} already exists", key);
        }
        self
    }

    pub async fn load(&self, id: impl Into<String>) -> LoadBuilder {
        let read_aggregators = self
            .handlers
            .values()
            .map(|h| ReadAggregator::event(h.aggregator_type(), h.event_name()))
            .collect::<Vec<_>>();

        LoadBuilder {
            key: self.key.to_owned(),
            id: id.into(),
            aggregators: HashMap::new(),
            read_aggregators,
            context: Arc::default(),
        }
    }
}

pub trait FromProjection: Sized {
    fn from_projection<'a, E: Executor>(
        context: &'a Context<'a, E>,
        id: String,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<Self>>> + Send + 'a>>;
}

#[derive(Debug, Clone, Default)]
pub struct LoadResult<A> {
    pub item: A,
    pub version: i32,
    pub routing_key: Option<String>,
}

pub struct LoadBuilder {
    key: String,
    id: String,
    aggregators: HashMap<String, String>,
    read_aggregators: Vec<ReadAggregator>,
    context: Arc<Mutex<context::Context>>,
}

impl LoadBuilder {
    pub fn data<D: Send + Sync + 'static>(&mut self, v: D) -> &mut Self {
        let mut context = self
            .context
            .lock()
            .expect("Unable to lock SuBuilder.context");

        context.insert(v);
        drop(context);

        self
    }

    pub fn aggregator<A: Aggregator>(&mut self, id: impl Into<String>) -> &mut Self {
        self.aggregators
            .insert(A::aggregator_type().to_owned(), id.into());

        self
    }

    pub async fn execute<E: Executor, P: FromProjection + Default + Handler<E>>(
        &self,
        executor: &E,
    ) -> anyhow::Result<Option<LoadResult<P>>> {
        let context = Context {
            context: self.context.clone(),
            executor,
        };

        let cursor = executor.get_subscriber_cursor(self.key.to_owned()).await?;
        let loaded = P::from_projection(&context, self.id.to_owned()).await?;
        let has_loaded = loaded.is_some();
        let mut projection = loaded.unwrap_or_default();

        let read_aggregators = self
            .read_aggregators
            .clone()
            .into_iter()
            .map(|mut r| {
                if let Some(id) = self.aggregators.get(&r.aggregator_type) {
                    r.aggregator_id = Some(id.to_owned());
                }

                r
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
            projection.handle(&context, &event.node).await?;
        }

        if events.page_info.has_next_page {
            anyhow::bail!("Too busy");
        }

        if let Some(event) = events.edges.last() {
            return Ok(Some(LoadResult {
                item: projection,
                version: event.node.version,
                routing_key: event.node.routing_key.to_owned(),
            }));
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
            return Ok(Some(LoadResult {
                item: projection,
                version: event.node.version,
                routing_key: event.node.routing_key.to_owned(),
            }));
        }

        Ok(None)
    }
}
