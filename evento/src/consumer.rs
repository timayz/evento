use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dyn_clone::DynClone;
use evento_store::{Aggregate, Cursor, Event, Result as StoreResult, Store, WriteEvent};
use futures_util::future::join_all;
use glob_match::glob_match;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::time::{interval_at, sleep, Duration, Instant};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{context::Context, engine::Engine, Producer};

#[derive(Clone)]
pub struct ConsumerContext {
    pub name: Option<String>,
    inner: Arc<RwLock<Context>>,
    producer: Producer,
}

impl ConsumerContext {
    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.inner.read().extract::<T>().clone()
    }

    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.inner.read().get::<T>().cloned()
    }

    pub async fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        event: WriteEvent,
        original_version: u16,
    ) -> StoreResult<Vec<Event>> {
        self.publish_all::<A, I>(id, vec![event], original_version)
            .await
    }

    pub async fn publish_all<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<WriteEvent>,
        original_version: u16,
    ) -> StoreResult<Vec<Event>> {
        self.producer
            .publish_all::<A, I>(id, events, original_version)
            .await
    }

    pub async fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> StoreResult<Option<(A, u16)>> {
        self.producer.load::<A, I>(id).await
    }
}

#[derive(Clone)]
pub struct Consumer<E: Engine + Clone> {
    pub(super) engine: E,
    pub(super) store: Store,
    pub deadletter: Store,
    rules: HashMap<String, Rule>,
    id: Uuid,
    name: Option<String>,
    context: ConsumerContext,
}

impl<E: Engine + Clone + 'static> Consumer<E> {
    pub(super) fn create(engine: E, store: Store, deadletter: Store) -> Self {
        let context = ConsumerContext {
            inner: Default::default(),
            name: None,
            producer: Producer {
                name: None,
                store: store.clone(),
            },
        };

        Self {
            engine,
            store,
            deadletter,
            rules: HashMap::new(),
            context,
            name: None,
            id: Uuid::new_v4(),
        }
    }

    pub fn name<N: Into<String>>(&self, name: N) -> Self {
        let name = name.into();

        let context = ConsumerContext {
            inner: Default::default(),
            name: Some(name.to_owned()),
            producer: self.context.producer.clone(),
        };

        Self {
            engine: self.engine.clone(),
            store: self.store.clone(),
            deadletter: self.deadletter.clone(),
            rules: self.rules.clone(),
            context,
            name: Some(name),
            id: Uuid::new_v4(),
        }
    }

    pub fn data<V: Send + Sync + 'static>(self, v: V) -> Self {
        self.context.inner.write().insert(v);
        self
    }

    pub fn rule(self, rule: Rule) -> Self {
        self.rules(vec![rule])
    }

    pub fn rules(mut self, rules: Vec<Rule>) -> Self {
        for rule in rules {
            let mut rule = rule.clone();

            if let Some(name) = self.name.as_ref() {
                rule.key = format!("{}.{}", name, rule.key);
            }

            if let Some(r) = self.rules.get_mut(&rule.key) {
                r.extend(rule);
            } else {
                self.rules.insert(rule.key.to_owned(), rule);
            }
        }

        self
    }

    pub async fn start(&self, delay: u64) -> crate::error::Result<Producer> {
        let futures = self
            .rules
            .keys()
            .map(|key| self.engine.upsert(key.to_owned(), self.id));

        let fut_err = join_all(futures)
            .await
            .into_iter()
            .find_map(|res| res.err());

        if let Some(err) = fut_err {
            return Err(err);
        }

        let futures = self
            .rules
            .values()
            .map(|rule| self.start_queue(rule, delay));

        join_all(futures).await;

        Ok(Producer {
            name: self.name.to_owned(),
            store: self.store.clone(),
        })
    }

    async fn start_queue(&self, rule: &Rule, delay: u64) {
        let rule = rule.clone();
        let engine = self.engine.clone();
        let store = rule.store.unwrap_or(self.store.clone());
        let deadletter = self.deadletter.clone();
        let consumer_id = self.id;
        let ctx = self.context.clone();
        let name = self.name.to_owned();

        tokio::spawn(async move {
            if delay > 0 {
                info!("wait {delay} seconds to start {}", rule.key);
                sleep(Duration::from_secs(delay)).await;
            }

            info!("{} started.", rule.key);

            if !rule.cdc {
                tracing::debug!("change data capture disabled for {}.", rule.key);

                if let Err(e) = Self::set_cursor_to_last(&engine, &store, rule.key.to_owned()).await
                {
                    tracing::error!("{e}");
                    return;
                }
            }

            let mut interval = interval_at(Instant::now(), Duration::from_millis(100));

            loop {
                interval.tick().await;

                let queue = match engine.get(rule.key.to_owned()).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };

                if !queue.enabled {
                    debug!("queue {} is disabled, skip", rule.key);
                    continue;
                }

                if queue.consumer_id != consumer_id {
                    info!(
                        "consumer {consumer_id} lost ownership of {} over consumer {}",
                        rule.key, queue.consumer_id
                    );
                    break;
                }

                let filters = rule
                    .filters
                    .iter()
                    .filter_map(|filter| {
                        let mut map = Map::new();

                        if let Some((topic, _)) = filter.split_once('/') {
                            map.insert("_evento_topic".to_owned(), Value::String(topic.to_owned()));
                        }

                        if let Some(name) = name.as_ref() {
                            map.insert("_evento_name".to_owned(), Value::String(name.to_owned()));
                        }

                        if map.is_empty() {
                            None
                        } else {
                            Some(Value::Object(map))
                        }
                    })
                    .collect::<Vec<_>>();

                let events = match store
                    .read(100, queue.cursor.map(|c| c.into()), Some(filters))
                    .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };

                let mut dead_events = Vec::new();

                for event in events.edges.iter() {
                    let (aggregate_type, aggregate_id) = match event.node.aggregate_details() {
                        Some(details) => details,
                        _ => {
                            error!("failed to aggregate_details of {}", event.node.aggregate_id);
                            continue;
                        }
                    };

                    let topic_name =
                        format!("{}/{}/{}", aggregate_type, aggregate_id, event.node.name);

                    let mut results = Vec::new();

                    for (filter, handler) in rule.handlers.iter() {
                        if !glob_match(filter.as_str(), &topic_name) {
                            continue;
                        }

                        results.push(handler.handle(event.node.clone(), ctx.clone()).await);
                    }

                    if results.is_empty() {
                        debug!(
                            "{:?} skiped event id={}, name={}, topic_name={}",
                            &rule.key,
                            &event.node.aggregate_id,
                            &event.node.name,
                            &topic_name.to_string()
                        );

                        continue;
                    }

                    let mut event_errors: Vec<String> = vec![];

                    for res in results.iter() {
                        if let Err(e) = res {
                            error!(
                                "{:?} failed to handle event id={}, name={}, error={}",
                                &rule.key, &event.node.aggregate_id, &event.node.name, e
                            );

                            event_errors.push(e.to_string());
                        }
                    }

                    if !event_errors.is_empty() {
                        let mut metadata = match event.node.to_metadata::<HashMap<String, Value>>()
                        {
                            Ok(metadata) => metadata.unwrap_or_default(),
                            Err(e) => {
                                error!("{e}");

                                HashMap::default()
                            }
                        };

                        let event_errors = match serde_json::to_value(event_errors) {
                            Ok(errors) => errors,
                            Err(e) => {
                                error!("{e}");

                                Value::Null
                            }
                        };

                        metadata.insert(
                            "_evento_rule_key".to_owned(),
                            Value::String(rule.key.to_owned()),
                        );

                        metadata.insert("_evento_errors".to_owned(), event_errors);

                        match event.node.clone().metadata(metadata) {
                            Ok(e) => dead_events.push(e),
                            Err(e) => {
                                error!("{e}");
                            }
                        };
                    }

                    debug!(
                        "{:?} succeeded to handle event id={}, name={}",
                        &rule.key, &event.node.aggregate_id, &event.node.name
                    );
                }

                if !dead_events.is_empty() {
                    if let Err(e) = deadletter.insert(dead_events).await {
                        error!("{e}");
                    }
                }

                let cursor = match events.edges.last().map(|e| e.cursor.to_owned()) {
                    Some(cursor) => cursor,
                    _ => continue,
                };

                if let Err(e) = engine.set_cursor(queue.rule, cursor).await {
                    error!("{e}");
                }
            }
        });
    }

    async fn set_cursor_to_last(engine: &E, store: &Store, key: String) -> Result<()> {
        let Some(event) = store.last().await? else {
            return Ok(());
        };

        engine.set_cursor(key, event.to_cursor()).await?;

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "pg", derive(sqlx::FromRow))]
pub struct Queue {
    pub id: Uuid,
    pub consumer_id: Uuid,
    pub rule: String,
    pub enabled: bool,
    pub cursor: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait RuleHandler: DynClone + Send + Sync {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()>;
}

dyn_clone::clone_trait_object!(RuleHandler);

#[derive(Clone)]
pub struct Rule {
    pub(crate) key: String,
    pub(crate) store: Option<Store>,
    pub(crate) handlers: Vec<(String, Box<dyn RuleHandler + 'static>)>,
    pub(crate) filters: HashSet<String>,
    pub(crate) cdc: bool,
}

impl Rule {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            store: None,
            filters: HashSet::new(),
            handlers: Vec::new(),
            cdc: true,
        }
    }

    pub fn handler<H: RuleHandler + 'static>(
        mut self,
        filter: impl Into<String>,
        handler: H,
    ) -> Self {
        let filter = filter.into();
        self.filters.insert(filter.to_owned());
        self.handlers.push((filter, Box::new(handler)));

        self
    }

    pub fn cdc(mut self, value: bool) -> Self {
        self.cdc = value;

        self
    }

    pub fn store(mut self, store: Store) -> Self {
        self.store = Some(store);

        self
    }

    pub fn extend(&mut self, rule: Rule) {
        self.filters.extend(rule.filters);
        self.handlers.extend(rule.handlers);
    }
}
