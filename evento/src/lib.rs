pub mod store;

mod context;
mod data;

pub use context::Context;
pub use data::Data;
pub use store::{Aggregate, Error as StoreError, Event, EventStore};

use parking_lot::RwLock;
// use sqlx::PgPool;
use chrono::{DateTime, Utc};
use futures_util::{future::join_all, FutureExt};
use pikav::topic::TopicFilter;
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
    fn(e: Event, ctx: &Context) -> Pin<Box<dyn Future<Output = Result<(), String>>>>;

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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>>>> {
        self.store.save::<A, _>(id, events, original_version)
    }

    pub fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = StoreEngineResult<A>>>> {
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
    pub fn new<S: StoreEngine>(store: EventStore<S>) -> Evento<Self, S> {
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

pub struct Evento<E: Engine + Sync + Send, S: StoreEngine> {
    id: Uuid,
    name: Option<String>,
    engine: E,
    store: EventStore<S>,
    subscribers: HashMap<String, Subscriber>,
}

impl<E: Engine + Sync + Send + 'static, S: StoreEngine> Evento<E, S> {
    pub fn new(engine: E, store: EventStore<S>) -> Self {
        Self {
            engine,
            store,
            subscribers: HashMap::new(),
            name: None,
            id: Uuid::new_v4(),
        }
    }

    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn subscribe(mut self, s: Subscriber) -> Self {
        self.subscribers.insert(s.key.to_owned(), s);
        self
    }

    pub async fn run(
        &self,
        create_ctx: &impl Fn(&mut Context),
    ) -> Result<Publisher<S>, StoreError> {
        self.run_with_delay(Duration::from_secs(30), create_ctx)
            .await
    }

    pub async fn run_with_delay(
        &self,
        delay: Duration,
        create_ctx: &impl Fn(&mut Context),
    ) -> Result<Publisher<S>, StoreError> {
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
            .map(|(_, sub)| self.spawn(sub.clone(), delay, create_ctx));

        join_all(futures).await;

        Ok(Publisher {
            name: self.name.to_owned(),
            store: self.store.clone(),
        })
    }

    async fn spawn(&self, sub: Subscriber, delay: Duration, create_ctx: impl Fn(&mut Context)) {
        let engine = self.engine.clone();
        let consumer_id = self.id.clone();
        let mut ctx = Context::new();
        create_ctx(&mut ctx);

        tokio::spawn(async move {
            sleep(delay).await;

            let mut interval = interval_at(Instant::now(), Duration::from_secs(1));

            loop {
                interval.tick().await;

                println!("{}", sub.key);
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
            }
        });
    }
}
