#![forbid(unsafe_code)]

mod context;
mod data;

use anyhow::Error;
pub use context::Context;
pub use data::Data;
pub use evento_store::{Aggregate, Error as StoreError, Event, EventStore};

use chrono::{DateTime, Utc};
use evento_store::{Engine as StoreEngine, EngineResult as StoreEngineResult};
use futures_util::{future::join_all, FutureExt};
use glob_match::glob_match;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::time::{interval_at, sleep, Instant};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub consumer_id: Uuid,
    pub key: String,
    pub enabled: bool,
    pub cursor: Option<Uuid>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum SubscirberHandlerError {
    #[error("[{0}] {1}")]
    Reason(String, String),
}

impl SubscirberHandlerError {
    pub fn new<C: Into<String>, R: Into<String>>(code: C, reason: R) -> Self {
        Self::Reason(code.into(), reason.into())
    }
}

type SubscirberHandler = fn(
    e: Event,
    ctx: EventoContext,
) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<Event>>> + Send>>;

type SubscirberPostHandler =
    fn(e: Event, ctx: EventoContext) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

#[derive(Clone)]
pub struct Subscriber {
    key: String,
    filters: HashSet<String>,
    handlers: Vec<(String, SubscirberHandler)>,
    post_handlers: Vec<SubscirberPostHandler>,
    from_start: bool,
}

impl Subscriber {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            filters: HashSet::new(),
            handlers: Vec::new(),
            post_handlers: Vec::new(),
            from_start: true,
        }
    }

    pub fn handler(mut self, filter: impl Into<String>, handler: SubscirberHandler) -> Self {
        let filter = filter.into();
        self.filters.insert(filter.to_owned());
        self.handlers.push((filter, handler));

        self
    }

    pub fn post_handler(mut self, handler: SubscirberPostHandler) -> Self {
        self.post_handlers.push(handler);

        self
    }

    pub fn set_from_start(mut self, value: bool) -> Self {
        self.from_start = value;

        self
    }
}

pub type PgProducer = Producer<evento_store::PgEngine>;

#[derive(Clone)]
pub struct Producer<S: StoreEngine + Send + Sync> {
    name: Option<String>,
    store: EventStore<S>,
}

impl<S: StoreEngine + Send + Sync> Producer<S> {
    pub fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>> + Send + '_>> {
        let store = self.store.clone();
        let name = self.name.to_owned();
        let id = id.into();

        async move {
            let name = match store.get::<A, _>(&id).await? {
                Some(event) => event
                    .to_metadata::<HashMap<String, Value>>()?
                    .get("_evento_name")
                    .cloned(),
                _ => name.map(Value::String),
            };

            let mut updated_events = Vec::new();
            for event in events.iter() {
                let mut metadata = event.to_metadata::<HashMap<String, Value>>()?;

                if let Some(name) = &name {
                    metadata.insert("_evento_name".to_owned(), name.clone());
                }

                metadata.insert(
                    "_evento_topic".to_owned(),
                    Value::String(A::aggregate_type().to_owned()),
                );

                let event = event.clone().metadata(metadata)?;

                updated_events.push(event);
            }

            store
                .save::<A, _>(id, updated_events, original_version)
                .await
        }
        .boxed()
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

    fn add_deadletter(
        &self,
        events: Vec<Event>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>>;

    fn read_deadletters(
        &self,
        first: usize,
        after: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>> + Send + '_>>;
}

#[derive(Default, Clone)]
pub struct MemoryEngine(
    Arc<RwLock<Vec<Event>>>,
    Arc<RwLock<HashMap<String, Subscription>>>,
);

impl MemoryEngine {
    pub fn new<S: StoreEngine + Sync + Send + 'static>(store: EventStore<S>) -> Evento<Self, S> {
        Evento::new(Self::default(), store)
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
            let subscription = w_subs.entry(key.to_owned()).or_insert(Subscription {
                id: Uuid::new_v4(),
                consumer_id,
                key,
                enabled: true,
                cursor: None,
                updated_at: None,
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

    fn add_deadletter(
        &self,
        events: Vec<Event>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        self.0.write().extend(events);

        async move { Ok(()) }.boxed()
    }

    fn read_deadletters(
        &self,
        first: usize,
        after: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>> + Send + '_>> {
        let mut events = self.0.read().clone();

        events.sort_by(|a, b| {
            let cmp = a.created_at.partial_cmp(&b.created_at).unwrap();

            match cmp {
                Ordering::Equal => {}
                _ => return cmp,
            };

            let cmp = a.version.partial_cmp(&b.version).unwrap();

            match cmp {
                Ordering::Equal => a.id.partial_cmp(&b.id).unwrap(),
                _ => cmp,
            }
        });

        let start = (after
            .map(|id| events.iter().position(|event| event.id == id).unwrap() as i32)
            .unwrap_or(-1)
            + 1) as usize;

        async move {
            if events.is_empty() {
                return Ok(events);
            }

            let end = std::cmp::min(events.len(), first + 1);

            Ok(events[start..end].to_vec())
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct PgEngine(PgPool, Option<String>);

impl PgEngine {
    pub fn new(pool: PgPool) -> PgEvento {
        Evento::new(
            Self(pool.clone(), None),
            evento_store::PgEngine::new(pool.clone()),
        )
        .data(pool)
    }

    pub fn new_prefix(pool: PgPool, table_prefix: impl Into<String>) -> PgEvento {
        let table_prefix = table_prefix.into();

        Evento::new(
            Self(pool.clone(), Some(table_prefix.to_owned())),
            evento_store::PgEngine::new_prefix(pool.clone(), table_prefix),
        )
        .data(pool)
    }

    fn get_table_name(&self, name: impl Into<String>) -> String {
        match self.1.as_ref() {
            Some(prefix) => format!("evento_{prefix}_{}", name.into()),
            _ => format!("evento_{}", name.into()),
        }
    }
}

impl Engine for PgEngine {
    fn init<K: Into<String>>(
        &self,
        key: K,
        consumer_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        let pool = self.0.clone();
        let key = key.into();
        let table_subscriptions = self.get_table_name("subscriptions");

        async move {
            sqlx::query_as::<_, (Uuid,)>(
                format!(
                    r#"
                INSERT INTO {table_subscriptions} (id, consumer_id, key, enabled, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (key)
                DO
                    UPDATE SET consumer_id = $2
                RETURNING id
                "#
                )
                .as_str(),
            )
            .bind(Uuid::new_v4())
            .bind(consumer_id)
            .bind(&key)
            .bind(true)
            .bind(Utc::now())
            .fetch_one(&pool)
            .await?;
            Ok(())
        }
        .boxed()
    }

    fn get_subscription<K: Into<String>>(
        &self,
        key: K,
    ) -> Pin<Box<dyn Future<Output = Result<Subscription, StoreError>> + Send + '_>> {
        let pool = self.0.clone();
        let key = key.into();
        let table_subscriptions = self.get_table_name("subscriptions");
        async move {
            let sub = sqlx::query_as::<_, Subscription>(
                format!(
                    r#"
                SELECT * from {table_subscriptions} WHERE key = $1
                "#
                )
                .as_str(),
            )
            .bind(&key)
            .fetch_one(&pool)
            .await?;
            Ok(sub)
        }
        .boxed()
    }

    fn update_subscription(
        &self,
        subscription: Subscription,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        let pool = self.0.clone();
        let table_subscriptions = self.get_table_name("subscriptions");
        async move {
            sqlx::query_as::<_, (Uuid,)>(
                format!(
                    r#"
                UPDATE {table_subscriptions}
                SET cursor = $2, updated_at = $3
                WHERE key = $1
                RETURNING id
                "#
                )
                .as_str(),
            )
            .bind(&subscription.key)
            .bind(subscription.cursor)
            .bind(subscription.updated_at)
            .fetch_one(&pool)
            .await?;
            Ok(())
        }
        .boxed()
    }

    fn add_deadletter(
        &self,
        events: Vec<Event>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>> {
        let pool = self.0.clone();
        let table_deadletters = self.get_table_name("deadletters");

        async move {
            let mut tx = pool.begin().await?;

            for events in events.chunks(100).collect::<Vec<&[Event]>>() {
                let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                    format!("INSERT INTO {table_deadletters} (id, name, aggregate_id, version, data, metadata, created_at) ")
                );

                query_builder.push_values(events, |mut b, event| {
                    b.push_bind(event.id)
                        .push_bind(event.name.to_owned())
                        .push_bind(event.aggregate_id.to_owned())
                        .push_bind(event.version)
                        .push_bind(event.data.clone())
                        .push_bind(event.metadata.clone())
                        .push_bind(event.created_at);
                });

                query_builder.build().execute(&mut *tx).await?;
            }

            tx.commit().await?;

            Ok(())
        }.boxed()
    }

    fn read_deadletters(
        &self,
        first: usize,
        after: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>> + Send + '_>> {
        let pool = self.0.clone();
        let table_events = self.get_table_name("events");
        let table_deadletters = self.get_table_name("deadletters");

        async move {
            let limit = first as i16;
            let cursor = match after {
                Some(id) => Some(
                    sqlx::query_as::<_, Event>(
                        format!(r#"
                        SELECT * from {table_events}
                        WHERE id = $1
                        LIMIT 1
                        "#).as_str(),
                    )
                    .bind(id)
                    .fetch_one(&pool)
                    .await?,
                ),
                _ => None,
            };
            let events = match cursor {
                Some(cursor) => {
                    sqlx::query_as::<_, Event>(
                        format!(r#"
                        SELECT * from {table_deadletters}
                        WHERE created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))
                        ORDER BY created_at ASC, version ASC, id ASC
                        LIMIT $3
                        "#).as_str(),
                    )
                    .bind(cursor.created_at)
                    .bind(cursor.version)
                    .bind(cursor.id)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
                _ => {
                    sqlx::query_as::<_, Event>(
                        format!(r#"
                        SELECT * from {table_deadletters}
                        ORDER BY created_at ASC, version ASC, id ASC
                        LIMIT $1
                        "#).as_str(),
                    )
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
            };

            Ok(events)
        }
        .boxed()
    }
}

pub type PgEvento = Evento<PgEngine, evento_store::PgEngine>;

struct EventoContextName(Option<String>);

#[derive(Clone, Default)]
pub struct EventoContext(pub Arc<RwLock<Context>>);

impl EventoContext {
    pub fn name(&self) -> Option<String> {
        self.0.read().extract::<EventoContextName>().0.to_owned()
    }

    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.0.read().extract::<T>().clone()
    }
}

#[derive(Clone)]
pub struct Evento<E: Engine + Sync + Send, S: StoreEngine + Sync + Send> {
    id: Uuid,
    name: Option<String>,
    context: EventoContext,
    store: EventStore<S>,
    subscribers: HashMap<String, Subscriber>,
    pub engine: E,
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
        let name = self.name.to_owned().unwrap_or("_".to_owned());
        let mut sub = s.clone();
        sub.key = format!("{}.{}", name, s.key);

        self.subscribers.insert(sub.key.to_owned(), sub);
        self
    }

    pub fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = StoreEngineResult<A>> + Send + '_>> {
        self.store.load(id)
    }

    pub async fn run(&self, delay: u64) -> Result<Producer<S>, StoreError> {
        let futures = self
            .subscribers
            .keys()
            .map(|key| self.engine.init(key, self.id));

        let fut_err = join_all(futures)
            .await
            .into_iter()
            .find_map(|res| res.err());

        if let Some(err) = fut_err {
            return Err(err);
        }

        let futures = self
            .subscribers
            .values()
            .map(|sub| self.spawn(sub.clone(), delay));

        join_all(futures).await;

        Ok(Producer {
            name: self.name.to_owned(),
            store: self.store.clone(),
        })
    }

    async fn set_last_event(
        engine: &E,
        store: &EventStore<S>,
        key: String,
    ) -> Result<(), StoreError> {
        let Some(event) = store.get_last().await? else {
            return Ok(());
        };

        let mut subscription = engine.get_subscription(key).await?;
        subscription.cursor = Some(event.id);
        subscription.updated_at = Some(Utc::now());

        engine.update_subscription(subscription).await?;

        Ok(())
    }

    async fn spawn(&self, sub: Subscriber, delay: u64) {
        let engine = self.engine.clone();
        let store = self.store.clone();
        let consumer_id = self.id;
        let ctx = self.context.clone();
        let name = self.name.to_owned();

        if !sub.from_start {
            tracing::info!("{} set to last event.", sub.key);

            if let Err(e) = Self::set_last_event(&engine, &store, sub.key.to_owned()).await {
                tracing::error!("{e}");
                return;
            }
        }

        tokio::spawn(async move {
            tracing::info!("Starting consumer {} ...", sub.key);

            sleep(Duration::from_secs(delay)).await;

            tracing::info!("{} started.", sub.key);

            let mut interval = interval_at(Instant::now(), Duration::from_millis(100));

            loop {
                interval.tick().await;

                let mut subscription = match engine.get_subscription(sub.key.to_owned()).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("{e}");
                        continue;
                    }
                };

                if !subscription.enabled {
                    tracing::info!("subscription {} is disabled, skip", sub.key,);
                    break;
                }

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

                        if let Some((topic, _)) = filter.split_once('/') {
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

                let mut dead_events = Vec::new();

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

                    let topic_name = format!("{}/{}/{}", aggregate_type, aggregate_id, event.name);

                    let mut futures = Vec::new();
                    for (i, (filter, handler)) in sub.handlers.iter().enumerate() {
                        if !glob_match(filter.as_str(), &topic_name) {
                            continue;
                        }

                        let post_handler = sub.post_handlers.get(i);
                        let ctx = ctx.clone();

                        futures.push(async move {
                            let data = handler(event.clone(), ctx.clone()).await?;

                            if let (Some(event), Some(post_handler)) = (data, post_handler) {
                                return post_handler(event, ctx).await;
                            }

                            Ok::<_, Error>(())
                        });
                    }

                    if futures.is_empty() {
                        tracing::debug!(
                            "{:?} skiped event id={}, name={}, topic_name={}",
                            &sub.key,
                            &event.aggregate_id,
                            &event.name,
                            &topic_name.to_string()
                        );

                        continue;
                    }

                    let results = join_all(futures).await;
                    let mut event_errors: Vec<String> = vec![];

                    for res in results.iter() {
                        if let Err(e) = res {
                            tracing::error!(
                                "{:?} failed to handle event id={}, name={}, error={}",
                                &sub.key,
                                &event.aggregate_id,
                                &event.name,
                                e
                            );

                            event_errors.push(e.to_string());
                        }
                    }

                    if !event_errors.is_empty() {
                        let mut metadata = match event.to_metadata::<HashMap<String, Value>>() {
                            Ok(metadata) => metadata,
                            Err(e) => {
                                tracing::error!("{e}");

                                HashMap::default()
                            }
                        };

                        let event_errors = match serde_json::to_value(event_errors) {
                            Ok(errors) => errors,
                            Err(e) => {
                                tracing::error!("{e}");

                                Value::Null
                            }
                        };

                        metadata.insert(
                            "_evento_subscription_key".to_owned(),
                            Value::String(sub.key.to_owned()),
                        );
                        metadata.insert("_evento_errors".to_owned(), event_errors);

                        match event.clone().metadata(metadata) {
                            Ok(e) => dead_events.push(e),
                            Err(e) => {
                                tracing::error!("{e}");
                            }
                        };
                    }

                    tracing::debug!(
                        "{:?} succeeded to handle event id={}, name={}",
                        &sub.key,
                        &event.aggregate_id,
                        &event.name
                    );
                }

                if !dead_events.is_empty() {
                    if let Err(e) = engine.add_deadletter(dead_events).await {
                        tracing::error!("{e}");
                    }
                }

                let last_event_id = match events.last().map(|e| e.id) {
                    Some(cursor) => cursor,
                    _ => continue,
                };

                subscription.cursor = Some(last_event_id);
                subscription.updated_at = Some(Utc::now());

                if let Err(e) = engine.update_subscription(subscription).await {
                    tracing::error!("{e}");
                }
            }
        });
    }
}
