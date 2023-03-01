pub mod store;

#[cfg(feature = "actix-web")]
mod command;
mod context;
mod data;

#[cfg(feature = "actix-web")]
pub use command::{CommandError, CommandInfo, CommandResponse, CommandResult};
pub use context::Context;
pub use data::Data;
pub use store::{Aggregate, Error as StoreError, Event, EventStore};

use chrono::{DateTime, Utc};
use futures_util::{future::join_all, FutureExt};
use parking_lot::RwLock;
use pikav::topic::{TopicFilter, TopicName};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::{
    cmp::Ordering, collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration,
};
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
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Debug)]
pub struct SubscirberHandlerError {
    pub code: String,
    pub reason: String,
}

impl From<serde_json::Error> for SubscirberHandlerError {
    fn from(e: serde_json::Error) -> Self {
        SubscirberHandlerError {
            code: "serde_json".to_owned(),
            reason: e.to_string(),
        }
    }
}

impl From<StoreError> for SubscirberHandlerError {
    fn from(e: StoreError) -> Self {
        SubscirberHandlerError {
            code: "store".to_owned(),
            reason: e.to_string(),
        }
    }
}

impl From<sqlx::Error> for SubscirberHandlerError {
    fn from(e: sqlx::Error) -> Self {
        SubscirberHandlerError {
            code: "sqlx".to_owned(),
            reason: e.to_string(),
        }
    }
}

impl From<uuid::Error> for SubscirberHandlerError {
    fn from(e: uuid::Error) -> Self {
        SubscirberHandlerError {
            code: "uuid".to_owned(),
            reason: e.to_string(),
        }
    }
}

type SubscirberHandler =
    fn(
        e: Event,
        ctx: EventoContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), SubscirberHandlerError>> + Send>>;

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

#[derive(Clone)]
pub struct Publisher<S: StoreEngine + Send + Sync> {
    name: Option<String>,
    store: EventStore<S>,
}

impl<S: StoreEngine + Send + Sync> Publisher<S> {
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
                    .and_then(|metadata| metadata.get("_evento_name").cloned()),
                _ => name.map(Value::String),
            };

            let mut updated_events = Vec::new();
            for event in events.iter() {
                let mut metadata = event
                    .to_metadata::<HashMap<String, Value>>()
                    .map(|metadata| metadata.unwrap_or_default())?;

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
            let mut subscription = w_subs.entry(key.to_owned()).or_insert(Subscription {
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
pub struct PgEngine(PgPool);

impl PgEngine {
    pub fn new(pool: PgPool) -> Evento<Self, store::PgEngine> {
        Evento::new(Self(pool.clone()), store::PgEngine::new(pool))
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
        async move {
            sqlx::query_as::<_, (Uuid,)>(
                r#"
                INSERT INTO _evento_subscriptions (id, consumer_id, key, enabled, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (key)
                DO
                    UPDATE SET consumer_id = $2
                RETURNING id
                "#,
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
        async move {
            let sub = sqlx::query_as::<_, Subscription>(
                r#"
                SELECT * from _evento_subscriptions WHERE key = $1
                "#,
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
        async move {
            sqlx::query_as::<_, (Uuid,)>(
                r#"
                UPDATE _evento_subscriptions
                SET cursor = $2, updated_at = $3
                WHERE key = $1
                RETURNING id
                "#,
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

        async move {
            let mut tx = pool.begin().await?;

            for events in events.chunks(100).collect::<Vec<&[Event]>>() {
                let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                    "INSERT INTO _evento_deadletters (id, name, aggregate_id, version, data, metadata, created_at) "
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

        async move {
            let limit = first as i16;
            let cursor = match after {
                Some(id) => Some(
                    sqlx::query_as::<_, Event>(
                        r#"
                        SELECT * from _evento_events
                        WHERE id = $1
                        LIMIT 1
                        "#,
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
                        r#"
                    SELECT * from _evento_deadletters
                    WHERE created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))
                    ORDER BY created_at ASC, version ASC, id ASC
                    LIMIT $3
                    "#,
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
                        r#"
                    SELECT * from _evento_deadletters
                    ORDER BY created_at ASC, version ASC, id ASC
                    LIMIT $1
                    "#,
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

struct EventoContextName(Option<String>);

#[derive(Clone, Default)]
pub struct EventoContext(pub Arc<RwLock<Context>>);

impl EventoContext {
    pub fn name(&self) -> Option<String> {
        self.0.read().extract::<EventoContextName>().0.to_owned()
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

    pub async fn run(&self) -> Result<Publisher<S>, StoreError> {
        self.run_with_delay(Duration::from_secs(30)).await
    }

    pub async fn run_with_delay(&self, delay: Duration) -> Result<Publisher<S>, StoreError> {
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

        Ok(Publisher {
            name: self.name.to_owned(),
            store: self.store.clone(),
        })
    }

    async fn spawn(&self, sub: Subscriber, delay: Duration) {
        let engine = self.engine.clone();
        let store = self.store.clone();
        let consumer_id = self.id;
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
                    let results = join_all(futures).await;
                    let event_errors = results
                        .iter()
                        .filter_map(|res| match res {
                            Err(e) => Some(e),
                            _ => None,
                        })
                        .collect::<Vec<&SubscirberHandlerError>>();

                    if !event_errors.is_empty() {
                        let mut metadata = match event
                            .to_metadata::<HashMap<String, Value>>()
                            .map(|metadata| metadata.unwrap_or_default())
                        {
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

                        metadata.insert("_evento_subscription_key".to_owned(), Value::String(sub.key.to_owned()));
                        metadata.insert("_evento_errors".to_owned(), event_errors);

                        match event.clone().metadata(metadata) {
                            Ok(e) => dead_events.push(e),
                            Err(e) => {
                                tracing::error!("{e}");
                            }
                        };
                    }
                }

                if !dead_events.is_empty() {
                    if let Err(e) = engine.add_deadletter(dead_events).await {
                        tracing::error!("{e}");
                    }
                }

                let last_event_id = match events.last().map(|e| e.id) {
                    Some(cursor) => cursor,
                    None => {
                        tracing::debug!("No events found after {:?}", subscription.cursor);
                        continue;
                    }
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
