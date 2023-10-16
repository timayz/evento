use anyhow::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dyn_clone::DynClone;
use evento_store::{Cursor, Event, Store};
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

struct ConsumerName(Option<String>);

#[derive(Debug, Clone, Default)]
pub struct ConsumerContext(Arc<RwLock<Context>>);

impl ConsumerContext {
    pub fn name(&self) -> Option<String> {
        self.0.read().extract::<ConsumerName>().0.to_owned()
    }

    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.0.read().extract::<T>().clone()
    }

    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.0.read().get::<T>().cloned()
    }
}

#[derive(Clone)]
pub struct Consumer<E: Engine + Clone, S: evento_store::Engine> {
    pub(super) engine: E,
    pub(super) store: Store<S>,
    pub(super) deadletter_store: Store<S>,
    rules: HashMap<String, Rule<S>>,
    id: Uuid,
    name: Option<String>,
    context: ConsumerContext,
}

impl<E: Engine + Clone + 'static, S: evento_store::Engine + Clone + 'static> Consumer<E, S> {
    pub(super) fn create(engine: E, store: Store<S>, deadletter_store: Store<S>) -> Self {
        Self {
            engine,
            store,
            deadletter_store,
            rules: HashMap::new(),
            context: ConsumerContext::default(),
            name: None,
            id: Uuid::new_v4(),
        }
    }

    pub fn name<N: Into<String>>(&self, name: N) -> Self {
        let name: Option<_> = Some(name.into());
        let context = ConsumerContext::default();
        context.0.write().insert(ConsumerName(name.to_owned()));

        Self {
            engine: self.engine.clone(),
            store: self.store.clone(),
            deadletter_store: self.deadletter_store.clone(),
            rules: self.rules.clone(),
            context,
            name,
            id: Uuid::new_v4(),
        }
    }

    pub fn data<V: Send + Sync + 'static>(self, v: V) -> Self {
        self.context.0.write().insert(v);
        self
    }

    pub fn rule(mut self, rule: Rule<S>) -> Self {
        let mut rule = rule.clone();

        if let Some(name) = self.name.as_ref() {
            rule.key = format!("{}.{}", name, rule.key);
        }

        self.rules.insert(rule.key.to_owned(), rule);
        self
    }

    pub async fn start(&self, delay: u64) -> crate::error::Result<Producer<S>> {
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

    async fn start_queue(&self, rule: &Rule<S>, delay: u64) {
        let rule = rule.clone();
        let engine = self.engine.clone();
        let store = rule.store.unwrap_or(self.store.clone());
        let deadletter_store = self.deadletter_store.clone();
        let consumer_id = self.id;
        let ctx = self.context.clone();
        let name = self.name.to_owned();

        tokio::spawn(async move {
            info!("wait {delay} seconds to start {}", rule.key);
            sleep(Duration::from_secs(delay)).await;
            info!("{} started.", rule.key);

            if !rule.cdc {
                tracing::info!("change data capture disabled for {}.", rule.key);

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
                    info!("queue {} is disabled, skip", rule.key);
                    break;
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

                    let mut futures = Vec::new();
                    for (i, (filter, handler)) in rule.handlers.iter().enumerate() {
                        if !glob_match(filter.as_str(), &topic_name) {
                            continue;
                        }

                        let post_handler = rule.post_handlers.get(i);
                        let ctx = ctx.clone();

                        futures.push(async move {
                            let data = handler.handle(event.node.clone(), ctx.clone()).await?;

                            if let (Some(event), Some(post_handler)) = (data, post_handler) {
                                return post_handler.handle(event, ctx).await;
                            }

                            Ok::<_, Error>(())
                        });
                    }

                    if futures.is_empty() {
                        debug!(
                            "{:?} skiped event id={}, name={}, topic_name={}",
                            &rule.key,
                            &event.node.aggregate_id,
                            &event.node.name,
                            &topic_name.to_string()
                        );

                        continue;
                    }

                    let results = join_all(futures).await;
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
                    if let Err(e) = deadletter_store.insert(dead_events).await {
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

    async fn set_cursor_to_last(engine: &E, store: &Store<S>, key: String) -> Result<()> {
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
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>>;
}

dyn_clone::clone_trait_object!(RuleHandler);

#[async_trait]
pub trait RulePostHandler: DynClone + Send + Sync {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()>;
}

dyn_clone::clone_trait_object!(RulePostHandler);

#[derive(Clone)]
pub struct Rule<E: evento_store::Engine> {
    pub(crate) key: String,
    pub(crate) store: Option<Store<E>>,
    pub(crate) handlers: Vec<(String, Box<dyn RuleHandler + 'static>)>,
    pub(crate) post_handlers: Vec<Box<dyn RulePostHandler + 'static>>,
    pub(crate) filters: HashSet<String>,
    pub(crate) cdc: bool,
}

impl<E: evento_store::Engine> Rule<E> {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            store: None,
            filters: HashSet::new(),
            handlers: Vec::new(),
            post_handlers: Vec::new(),
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

    pub fn post_handler<H: RulePostHandler + 'static>(mut self, handler: H) -> Self {
        self.post_handlers.push(Box::new(handler));

        self
    }

    pub fn cdc(mut self, value: bool) -> Self {
        self.cdc = value;

        self
    }

    pub fn store(mut self, store: Store<E>) -> Self {
        self.store = Some(store);

        self
    }
}
