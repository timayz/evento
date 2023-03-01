use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, thiserror::Error, Clone)]
pub enum Error {
    #[error("Unexpected original version while saving event")]
    UnexpectedOriginalVersion,

    #[error("Sqlx error `{0}`")]
    Sqlx(String),

    #[error("serde_json error `{0}`")]
    SerdeJson(String),

    #[error("{0}")]
    Unknown(String),
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Sqlx(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerdeJson(e.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub version: i32,
    pub data: Value,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Event {
    pub fn new<N: Into<String>>(name: N) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn aggregate_id<V: Into<String>>(mut self, value: V) -> Self {
        self.aggregate_id = value.into();

        self
    }

    pub fn aggregate_details(&self) -> Option<(String, String)> {
        self.aggregate_id
            .split_once('_')
            .map(|(aggregate_type, id)| (aggregate_type.to_owned(), id.to_owned()))
    }

    pub fn version(mut self, value: i32) -> Self {
        self.version = value;

        self
    }

    pub fn data<D: Serialize>(mut self, value: D) -> Result<Self, serde_json::Error> {
        self.data = serde_json::to_value(&value)?;

        Ok(self)
    }

    pub fn metadata<M: Serialize>(
        mut self,
        value: HashMap<String, M>,
    ) -> Result<Self, serde_json::Error> {
        self.metadata = Some(serde_json::to_value(&value)?);

        Ok(self)
    }

    pub fn to_data<D: DeserializeOwned>(&self) -> Result<D, serde_json::Error> {
        serde_json::from_value(self.data.clone())
    }

    pub fn to_metadata<D: DeserializeOwned>(&self) -> Result<D, serde_json::Error> {
        serde_json::from_value(
            self.metadata
                .clone()
                .unwrap_or(Value::Object(Map::default())),
        )
    }
}

impl Default for Event {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: String::default(),
            aggregate_id: String::default(),
            version: i32::default(),
            data: Value::default(),
            metadata: None,
            created_at: Utc::now(),
        }
    }
}

pub trait Aggregate: Default {
    fn apply(&mut self, event: &'_ Event);
    fn aggregate_type<'a>() -> &'a str;

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("{}_{}", Self::aggregate_type(), id.into())
    }

    fn to_id<I: Into<String>>(aggregate_id: I) -> String {
        let id: String = aggregate_id.into();

        id.replacen(&format!("{}_", Self::aggregate_type()), "", 1)
    }
}

pub(crate) type EngineResult<A> = Result<Option<(A, Event)>, Error>;

pub trait Engine: Clone {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>>;

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>> + Send + '_>>;

    fn get<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Event>, Error>> + Send + '_>>;

    fn read_all<F: Serialize>(
        &self,
        first: usize,
        after: Option<Uuid>,
        filters: Option<Vec<F>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>>;
}

#[derive(Clone)]
pub struct MemoryEngine(Arc<RwLock<HashMap<String, Vec<Event>>>>);

impl MemoryEngine {
    pub fn new() -> EventStore<Self> {
        EventStore(Self(Arc::new(RwLock::new(HashMap::new()))))
    }
}

impl Engine for MemoryEngine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        let id: String = A::aggregate_id(id);
        let mut data = self.0.write();
        let data_events = data.entry(id.to_owned()).or_insert_with(Vec::new);

        let mut version = data_events.last().map(|e| e.version).unwrap_or(0);

        if version != original_version {
            drop(data);
            return async { Err(Error::UnexpectedOriginalVersion) }.boxed();
        }

        let mut events_with_info = Vec::new();

        for event in events {
            version += 1;
            let event_with_info = event.aggregate_id(id.to_owned()).version(version);
            data_events.push(event_with_info.clone());
            events_with_info.push(event_with_info);
        }

        drop(data);
        async move { Ok(events_with_info) }.boxed()
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>> + Send + '_>> {
        let id: String = A::aggregate_id(id);

        let events = {
            let data = self.0.read();

            data.get(&id).map_or(Vec::new(), |events| events.to_vec())
        };

        async move {
            if events.is_empty() {
                return Ok(None);
            }

            let mut aggregate = A::default();

            for event in events.iter() {
                aggregate.apply(event);
            }

            let last_event = match events.last() {
                Some(e) => e.clone(),
                _ => return Ok(None),
            };

            Ok(Some((aggregate, last_event)))
        }
        .boxed()
    }

    fn read_all<F: Serialize>(
        &self,
        first: usize,
        after: Option<Uuid>,
        filters: Option<Vec<F>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        let filters = filters.and_then(|filters| {
            if filters.is_empty() {
                None
            } else {
                Some(filters)
            }
        });

        let events = self
            .0
            .read()
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<Event>>();

        let mut filtered_events = Vec::new();

        for event in events.iter() {
            if let Some(filters) = &filters {
                let metadata = match event.to_metadata::<HashMap<String, Value>>() {
                    Ok(m) => m,
                    Err(e) => return async move { Err(Error::SerdeJson(e.to_string())) }.boxed(),
                };

                let mut matched = false;

                for filter in filters {
                    let filter = match serde_json::to_value(filter)
                        .and_then(serde_json::from_value::<HashMap<String, Value>>)
                    {
                        Ok(filter) => filter,
                        Err(e) => {
                            return async move { Err(Error::SerdeJson(e.to_string())) }.boxed()
                        }
                    };

                    matched = filter.iter().all(|(key, v)| metadata.get(key) == Some(v));

                    if matched {
                        break;
                    }
                }

                if !matched {
                    continue;
                }
            }

            filtered_events.push(event.clone());
        }

        filtered_events.sort_by(|a, b| {
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
            .map(|id| {
                filtered_events
                    .iter()
                    .position(|event| event.id == id)
                    .unwrap() as i32
            })
            .unwrap_or(-1)
            + 1) as usize;

        async move {
            if filtered_events.is_empty() {
                return Ok(filtered_events);
            }

            let end = std::cmp::min(filtered_events.len(), first + 1);

            Ok(filtered_events[start..end].to_vec())
        }
        .boxed()
    }

    fn get<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Event>, Error>> + Send + '_>> {
        let id: String = A::aggregate_id(id);

        let event = {
            self.0
                .read()
                .get(&id)
                .and_then(|events| events.first().cloned())
        };

        async move { Ok(event) }.boxed()
    }
}

#[derive(Clone)]
pub struct PgEngine(PgPool);

impl PgEngine {
    pub fn new(pool: PgPool) -> EventStore<Self> {
        EventStore(Self(pool))
    }
}

impl Engine for PgEngine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        let id: String = A::aggregate_id(id);
        let pool = self.0.clone();

        async move {
            let mut tx = pool.begin().await?;
            let mut version = original_version;
            let mut events_with_info = Vec::new();

            for events in events.chunks(100).collect::<Vec<&[Event]>>() {
                let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                    "INSERT INTO _evento_events (id, name, aggregate_id, version, data, metadata, created_at) "
                );

                query_builder.push_values(events, |mut b, event| {
                    version += 1;

                    let event = event.clone().aggregate_id(&id).version(version);

                    b.push_bind(event.id)
                        .push_bind(event.name.to_owned())
                        .push_bind(event.aggregate_id.to_owned())
                        .push_bind(event.version)
                        .push_bind(event.data.clone())
                        .push_bind(event.metadata.clone())
                        .push_bind(event.created_at);

                    events_with_info.push(event);
                });

                query_builder.build().execute(&mut *tx).await?;
            }

            let next_event_id = sqlx::query_as::<_, Event>(
                "SELECT * FROM _evento_events WHERE aggregate_id = $1 AND version = $2 LIMIT 1",
            )
            .bind(&id)
            .bind(original_version + 1)
            .fetch_optional(&mut *tx)
            .await?
            .map(|e| e.id)
            .unwrap_or(events[0].id);

            if next_event_id != events[0].id {
                tx.rollback().await?;

                return Err(Error::UnexpectedOriginalVersion);
            }

            tx.commit().await?;

            Ok(events_with_info)
        }.boxed()
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>> + Send + '_>> {
        let id: String = A::aggregate_id(id);
        let pool = self.0.clone();

        async move {
            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM _evento_events WHERE aggregate_id = $1 ORDER BY version",
            )
            .bind(&id)
            .fetch_all(&pool)
            .await?;

            if events.is_empty() {
                return Ok(None);
            }

            let mut aggregate = A::default();

            for event in events.iter() {
                aggregate.apply(event);
            }

            let last_event = match events.last() {
                Some(e) => e.clone(),
                _ => return Ok(None),
            };

            Ok(Some((aggregate, last_event)))
        }
        .boxed()
    }

    fn read_all<F: Serialize>(
        &self,
        first: usize,
        after: Option<Uuid>,
        filters: Option<Vec<F>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        let pool = self.0.clone();
        let mut json_filters = HashSet::new();

        if let Some(filters) = filters {
            for filter in filters {
                match serde_json::to_string(&filter) {
                    Ok(json_filter) => {
                        json_filters.insert(format!("metadata @> '{json_filter}'::jsonb"));
                    }
                    Err(e) => return async move { Err(Error::SerdeJson(e.to_string())) }.boxed(),
                };
            }
        }

        let filters = if json_filters.is_empty() {
            None
        } else {
            Some(
                json_filters
                    .into_iter()
                    .collect::<Vec<String>>()
                    .join(" OR "),
            )
        };

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
            let events = match (cursor, filters) {
                (None, None) => {
                    sqlx::query_as::<_, Event>(
                        r#"
                SELECT * from _evento_events
                ORDER BY created_at ASC, version ASC, id ASC
                LIMIT $1
                "#,
                    )
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
                (Some(cursor), None) => {
                    sqlx::query_as::<_, Event>(
                        r#"
                SELECT * from _evento_events
                WHERE created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))
                ORDER BY created_at ASC, version ASC, id ASC
                LIMIT $4
                "#,
                    )
                    .bind(cursor.created_at)
                    .bind(cursor.version)
                    .bind(cursor.id)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
                (None, Some(filters)) => {
                    sqlx::query_as::<_, Event>(&format!(
                        r#"
                    SELECT * from _evento_events
                    WHERE ({filters})
                    ORDER BY created_at ASC, version ASC, id ASC
                    LIMIT $1
                    "#
                    ))
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
                (Some(cursor), Some(filters)) => {
                    sqlx::query_as::<_, Event>(&format!(
                        r#"
                        SELECT * from _evento_events
                        WHERE ({filters}) AND created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))
                        ORDER BY created_at ASC, version ASC, id ASC
                        LIMIT $4
                        "#
                    ))
                    .bind(cursor.created_at)
                    .bind(cursor.version)
                    .bind(cursor.id)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await?
                }
            };

            Ok(events)
        }
        .boxed()
    }

    fn get<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Event>, Error>> + Send + '_>> {
        let pool = self.0.clone();
        let id: String = A::aggregate_id(id);
        async move {
            let event = sqlx::query_as::<_, Event>(
                r#"
                SELECT * from _evento_events
                WHERE aggregate_id = $1
                LIMIT 1
                "#,
            )
            .bind(&id)
            .fetch_optional(&pool)
            .await?;
            Ok(event)
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct EventStore<E: Engine>(E);

impl<E: Engine> Engine for EventStore<E> {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        self.0.save::<A, _>(id, events, original_version)
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>> + Send + '_>> {
        self.0.load(id)
    }

    fn read_all<F: Serialize>(
        &self,
        first: usize,
        after: Option<Uuid>,
        filters: Option<Vec<F>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>> + Send + '_>> {
        self.0.read_all(first, after, filters)
    }

    fn get<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Event>, Error>> + Send + '_>> {
        self.0.get::<A, _>(id)
    }
}
