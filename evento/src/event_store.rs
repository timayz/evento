use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::{collections::HashMap, future::Future, pin::Pin};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Unexpected original version while saving event")]
    UnexpectedOriginalVersion,
    #[error("Sqlx error `{0}`")]
    Sqlx(String),
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Sqlx(e.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
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

    pub fn version(mut self, value: i32) -> Self {
        self.version = value;

        self
    }

    pub fn data<D: Serialize>(mut self, value: D) -> Result<Self, serde_json::Error> {
        self.data = serde_json::to_value(&value)?;

        Ok(self)
    }

    pub fn metadata<M: Serialize>(mut self, value: M) -> Result<Self, serde_json::Error> {
        self.metadata = Some(serde_json::to_value(&value)?);

        Ok(self)
    }

    pub fn to_data<D: DeserializeOwned>(&self) -> Result<D, serde_json::Error> {
        serde_json::from_value(self.data.clone())
    }

    pub fn to_metadata<D: DeserializeOwned>(&self) -> Result<Option<D>, serde_json::Error> {
        match &self.metadata {
            Some(metadata) => serde_json::from_value(metadata.clone()),
            None => Ok(None),
        }
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

type EngineResult<A> = Result<Option<(A, Event)>, Error>;

pub trait Engine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>>>>;

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>>;
}

pub struct MemoryEngine(RwLock<HashMap<String, Vec<Event>>>);

impl MemoryEngine {
    pub fn new() -> EventStore<Self> {
        EventStore(Self(RwLock::new(HashMap::new())))
    }
}

impl Engine for MemoryEngine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>>>> {
        let id: String = A::aggregate_id(id);
        let mut data = self.0.write();
        let data_events = data.entry(id.to_owned()).or_insert_with(Vec::new);

        let mut version = data_events.last().map(|e| e.version).unwrap_or(0);

        if version != original_version {
            drop(data);
            return Box::pin(async { Err(Error::UnexpectedOriginalVersion) });
        }

        let mut events_with_info = Vec::new();

        for event in events {
            version += 1;
            let event_with_info = event.aggregate_id(id.to_owned()).version(version);
            data_events.push(event_with_info.clone());
            events_with_info.push(event_with_info);
        }

        drop(data);
        Box::pin(async move { Ok(events_with_info) })
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        let id: String = A::aggregate_id(id);

        let events = {
            let data = self.0.read();

            data.get(&id).map_or(Vec::new(), |events| events.to_vec())
        };

        Box::pin(async move {
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
        })
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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>>>> {
        let id: String = A::aggregate_id(id);
        let pool = self.0.clone();

        Box::pin(async move {
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

            let next_event_id = sqlx::query_as!(
                Event,
                "SELECT * FROM _evento_events WHERE aggregate_id = $1 AND version = $2 LIMIT 1",
                &id,
                original_version + 1,
            )
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
        })
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        let id: String = A::aggregate_id(id);
        let pool = self.0.clone();

        Box::pin(async move {
            let events = sqlx::query_as!(
                Event,
                "SELECT * FROM _evento_events WHERE aggregate_id = $1 ORDER BY version",
                &id,
            )
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
        })
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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>>>> {
        self.0.save::<A, _>(id, events, original_version)
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        self.0.load(id)
    }
}
