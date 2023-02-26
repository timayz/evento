pub mod store;

mod context;
mod data;

pub use context::Context;
pub use data::Data;
use serde_json::Value;
pub use store::{Aggregate, Error as StoreError, Event, EventStore};

use parking_lot::RwLock;
// use sqlx::PgPool;
use chrono::{DateTime, Utc};
use futures_util::{future::join_all, FutureExt};
use pikav::topic::{TopicFilter, TopicName};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use store::{Engine as StoreEngine, EngineResult as StoreEngineResult};
use tokio::time::{interval_at, sleep, Instant};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub consumer_id: Uuid,
    pub key: String,
    pub enabled: bool,
    pub cursor: Option<Uuid>,
    pub cursor_updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

type SubscirberHandler =
    fn(e: Event, ctx: EventoContext) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

#[derive(Clone)]
pub struct Subscriber {
    key: String,
    filters: Vec<TopicFilter>,
    handlers: Vec<SubscirberHandler>,
}

impl Subscriber {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            filters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    pub fn filter<T: Into<String>>(mut self, topic: T) -> Self {
        match TopicFilter::new(topic) {
            Ok(filter) => self.filters.push(filter),
            Err(e) => panic!("{e}"),
        };

        self
    }

    pub fn handler(mut self, handler: SubscirberHandler) -> Self {
        self.handlers.push(handler);
        self
    }
}

pub struct Publisher<S: StoreEngine> {
    name: Option<String>,
    store: EventStore<S>,
}

impl<S: StoreEngine> Publisher<S> {
    pub fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>> + Send + '_>> {
        let mut updated_events = Vec::new();
        for event in events.iter() {
            let res = event
                .to_metadata::<HashMap<String, Value>>()
                .map(|metadata| metadata.unwrap_or_default());

            let mut metadata = match res {
                Ok(metadata) => metadata,
                Err(e) => return async move { Err(StoreError::SerdeJson(e.to_string())) }.boxed(),
            };

            if let Some(name) = &self.name {
                metadata.insert("_evento_name".to_owned(), Value::String(name.to_owned()));
            }

            metadata.insert(
                "_evento_topic".to_owned(),
                Value::String(A::aggregate_type().to_owned()),
            );

            let event = match event.clone().metadata(metadata) {
                Ok(metadata) => metadata,
                Err(e) => return async move { Err(StoreError::SerdeJson(e.to_string())) }.boxed(),
            };

            updated_events.push(event);
        }

        self.store
            .save::<A, _>(id, updated_events, original_version)
    }

    pub fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = StoreEngineResult<A>> + Send + '_>> {
        self.store.load::<A, _>(id)
    }
}

pub trait Engine: Clone {
    fn init<K: Into<String>>(
        &self,
        key: K,
        consumer_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>>;

    fn get_subscription<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<Subscription, StoreError>> + Send + '_>>;

    fn update_subscription(
        &self,
        subscription: Subscription,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>>;
}

#[derive(Clone)]
pub struct MemoryEngine(
    Arc<RwLock<HashMap<String, Vec<Event>>>>,
    Arc<RwLock<HashMap<String, Subscription>>>,
);

impl MemoryEngine {
    pub fn new<S: StoreEngine + Sync + Send + 'static>(store: EventStore<S>) -> Evento<Self, S> {
        Evento::new(
            Self(
                Arc::new(RwLock::new(HashMap::new())),
                Arc::new(RwLock::new(HashMap::new())),
            ),
            store,
        )
    }
}

impl Engine for MemoryEngine {
    fn init<K: Into<String>>(
        &self,
        key: K,
        consumer_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        let subscriptions = self.1.clone();
        let key = key.into();

        async move {
            let mut w_subs = subscriptions.write();
            let mut subscription = w_subs.entry(key.to_owned()).or_insert(Subscription {
                id: Uuid::new_v4(),
                consumer_id,
                key,
                enabled: true,
                cursor: None,
                cursor_updated_at: None,
                created_at: Utc::now(),
            });

            subscription.consumer_id = consumer_id;

            Ok(())
        }
        .boxed()
    }

    fn get_subscription<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<Subscription, StoreError>> + Send + '_>> {
        let key = key.into();
        let res = match self.1.read().get(&key) {
            Some(subscription) => Ok(subscription.clone()),
            _ => Err(StoreError::Unknown(format!("subscription {key} not found"))),
        };

        async move { res }.boxed()
    }

    fn update_subscription(
        &self,
        subscription: Subscription,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        self.1
            .write()
            .insert(subscription.key.to_lowercase(), subscription);

        async move { Ok(()) }.boxed()
    }
}

// pub struct PgEngine(PgPool);

// impl PgEngine {
//     pub fn new<S: StoreEngine>(pool: PgPool, store: EventStore<S>) -> Evento<Self, S> {
//         Evento::new(Self(pool), store)
//     }
// }

// impl Engine for PgEngine {

// }

struct EventoContextName(Option<String>);

#[derive(Clone, Default)]
pub struct EventoContext(pub Arc<RwLock<Context>>);

impl EventoContext {
    pub fn name(&self) -> Option<String> {
        self.0.read().extract::<EventoContextName>().0.to_owned()
    }
}

pub struct Evento<E: Engine + Sync + Send, S: StoreEngine + Sync + Send> {
    id: Uuid,
    name: Option<String>,
    context: EventoContext,
    engine: E,
    store: EventStore<S>,
    subscribers: HashMap<String, Subscriber>,
}

impl<E: Engine + Sync + Send + 'static, S: StoreEngine + Sync + Send + 'static> Evento<E, S> {
    pub fn new(engine: E, store: EventStore<S>) -> Self {
        Self {
            engine,
            store,
            subscribers: HashMap::new(),
            context: EventoContext::default(),
            name: None,
            id: Uuid::new_v4(),
        }
    }

    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self.context
            .0
            .write()
            .insert(EventoContextName(self.name.to_owned()));
        self
    }

    pub fn data<V: Send + Sync + 'static>(self, v: V) -> Self {
        self.context.0.write().insert(v);
        self
    }

    pub fn subscribe(mut self, s: Subscriber) -> Self {
        self.subscribers.insert(s.key.to_owned(), s);
        self
    }

    pub async fn run(&self) -> Result<Publisher<S>, StoreError> {
        self.run_with_delay(Duration::from_secs(30)).await
    }

    pub async fn run_with_delay(&self, delay: Duration) -> Result<Publisher<S>, StoreError> {
        let futures = self
            .subscribers
            .iter()
            .map(|(key, _)| self.engine.init(key, self.id));

        let fut_err = join_all(futures)
            .await
            .into_iter()
            .find_map(|res| res.err());

        if let Some(err) = fut_err {
            return Err(err);
        }

        let futures = self
            .subscribers
            .iter()
            .map(|(_, sub)| self.spawn(sub.clone(), delay));

        join_all(futures).await;

        Ok(Publisher {
            name: self.name.to_owned(),
            store: self.store.clone(),
        })
    }

    async fn spawn(&self, sub: Subscriber, delay: Duration) {
        let engine = self.engine.clone();
        let store = self.store.clone();
        let consumer_id = self.id.clone();
        let ctx = self.context.clone();
        let name = self.name.to_owned();

        tokio::spawn(async move {
            sleep(delay).await;

            let mut interval = interval_at(Instant::now(), Duration::from_secs(1));

            loop {
                interval.tick().await;

                let mut subscription = match engine.get_subscription(sub.key.to_owned()).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("{e}");
                        continue;
                    }
                };

                if subscription.consumer_id != consumer_id {
                    tracing::info!(
                        "consumer {consumer_id} lost ownership of {} over consumer {}",
                        sub.key,
                        subscription.consumer_id
                    );
                    break;
                }

                let filters = sub
                    .filters
                    .iter()
                    .filter_map(|filter| {
                        let mut map = HashMap::new();

                        if let Some((topic, _)) = filter.split_once("/") {
                            map.insert("_evento_topic".to_owned(), topic.to_owned());
                        }

                        if let Some(name) = name.as_ref() {
                            map.insert("_evento_name".to_owned(), name.to_owned());
                        }

                        if map.is_empty() {
                            None
                        } else {
                            Some(map)
                        }
                    })
                    .collect();

                let events = match store
                    .read_all(100, subscription.cursor, Some(filters))
                    .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        tracing::error!("{e}");
                        continue;
                    }
                };

                for event in events.iter() {
                    let (aggregate_type, aggregate_id) = match event.aggregate_details() {
                        Some(details) => details,
                        _ => {
                            tracing::error!(
                                "faield to aggregate_details of {}",
                                event.aggregate_id
                            );
                            continue;
                        }
                    };

                    let topic_name = match TopicName::new(format!(
                        "{}/{}/{}",
                        aggregate_type, aggregate_id, event.name
                    )) {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!("{e}");
                            continue;
                        }
                    };

                    if !sub
                        .filters
                        .iter()
                        .any(|filter| filter.get_matcher().is_match(&topic_name))
                    {
                        continue;
                    }

                    let futures = sub.handlers.iter().map(|h| h(event.clone(), ctx.clone()));

                    join_all(futures).await;
                }

                let last_event = match events.last().map(|e| e.id) {
                    Some(cursor) => cursor,
                    None => {
                        tracing::debug!("No events found after {:?}", subscription.cursor);
                        continue;
                    }
                };

                subscription.cursor = Some(last_event);
                subscription.cursor_updated_at = Some(Utc::now());

                if let Err(e) = engine.update_subscription(subscription).await {
                    tracing::error!("{e}");
                }
            }
        });
    }
}
