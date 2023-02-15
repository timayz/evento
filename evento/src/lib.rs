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
use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};
use store::{Engine as StoreEngine, EngineResult as StoreEngineResult};
use tokio::time::{interval_at, sleep, Instant};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Consumer {
    pub id: Uuid,
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

pub trait Engine {
    fn create_if_not_exists<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>>>>;

    fn get_cursor<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, StoreError>>>>;

    fn update_cursor<K: Into<String>>(
        &self,
        key: K,
        cursor: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>>>>;
}

pub struct MemoryEngine(
    RwLock<HashMap<String, Vec<Event>>>,
    RwLock<HashMap<String, Consumer>>,
);

impl MemoryEngine {
    pub fn new<S: StoreEngine>(store: EventStore<S>) -> Evento<Self, S> {
        Evento::new(
            Self(RwLock::new(HashMap::new()), RwLock::new(HashMap::new())),
            store,
        )
    }
}

impl Engine for MemoryEngine {
    fn create_if_not_exists<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>>>> {
        let mut consumers = self.1.write().clone();
        let key = key.into();

        async move {
            consumers.entry(key.to_owned()).or_insert(Consumer {
                id: Uuid::new_v4(),
                key,
                enabled: true,
                cursor: None,
                cursor_updated_at: None,
                created_at: Utc::now(),
            });

            Ok(())
        }
        .boxed()
    }

    fn get_cursor<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, StoreError>>>> {
        let key = key.into();
        let res = match self.1.read().get(&key) {
            Some(c) => Ok(c.cursor.to_owned()),
            _ => Err(StoreError::Unknown(format!("consumer {key} not found"))),
        };

        async move { res }.boxed()
    }

    fn update_cursor<K: Into<String>>(
        &self,
        key: K,
        cursor: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>>>> {
        let key = key.into();
        let res = match self.1.write().get_mut(&key) {
            Some(consumer) => {
                consumer.cursor = Some(cursor);
                consumer.cursor_updated_at = Some(Utc::now());

                Ok(())
            }
            _ => Err(StoreError::Unknown(format!("consumer {key} not found"))),
        };

        async move { res }.boxed()
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

pub struct Evento<E: Engine, S: StoreEngine> {
    name: Option<String>,
    engine: E,
    store: EventStore<S>,
    ctx: Context,
    subscribers: HashMap<String, Subscriber>,
}

impl<E: Engine, S: StoreEngine> Evento<E, S> {
    pub fn new(engine: E, store: EventStore<S>) -> Self {
        Self {
            engine,
            store,
            ctx: Context::new(),
            subscribers: HashMap::new(),
            name: None,
        }
    }

    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn data<U: 'static>(mut self, val: U) -> Self {
        self.ctx.insert(val);
        self
    }

    pub fn subscribe(mut self, s: Subscriber) -> Self {
        self.subscribers.insert(s.key.to_owned(), s);
        self
    }

    pub async fn run(&self) -> Result<(), StoreError> {
        self.run_with_delay(Duration::from_secs(30)).await
    }

    pub async fn run_with_delay(&self, delay: Duration) -> Result<(), StoreError> {
        let futures = self
            .subscribers
            .iter()
            .map(|(key, _)| self.engine.create_if_not_exists(key));

        let fut_err = join_all(futures)
            .await
            .into_iter()
            .find_map(|res| res.err());

        if let Some(err) = fut_err {
            return Err(err);
        }

        for (_, subscriber) in &self.subscribers {
            let subscriber = subscriber.clone();

            tokio::spawn(async move {
                sleep(delay).await;

                let mut interval = interval_at(Instant::now(), Duration::from_secs(1));

                loop {
                    interval.tick().await;
                    println!("{}", subscriber.key);
                }
            });
        }

        Ok(())
    }

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
