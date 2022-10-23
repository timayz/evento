use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
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
    fn aggregate_id<I: Into<String>>(id: I) -> String;
}

type EngineResult<A> = Result<Option<(A, Event)>, Error>;

pub trait Engine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<i32, Error>>>>;

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
    ) -> Pin<Box<dyn Future<Output = Result<i32, Error>>>> {
        let id: String = id.into();
        let mut data = self.0.write();
        let data_events = data.entry(id.to_owned()).or_insert_with(Vec::new);

        let mut version = data_events.last().map(|e| e.version).unwrap_or(0);

        if version != original_version {
            drop(data);
            return Box::pin(async { Err(Error::UnexpectedOriginalVersion) });
        }

        for event in events {
            version += 1;
            data_events.push(event.aggregate_id(id.to_owned()).version(version));
        }

        drop(data);
        Box::pin(async move { Ok(version) })
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        let id: String = id.into();

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
    ) -> Pin<Box<dyn Future<Output = Result<i32, Error>>>> {
        let id: String = id.into();
        let pool = self.0.clone();

        Box::pin(async move {
            let mut tx = pool.begin().await?;
            let mut version = original_version;

            for events in events.chunks(100).collect::<Vec<&[Event]>>() {
                let mut v1: Vec<Uuid> = Vec::with_capacity(events.len());
                let mut v2: Vec<&str> = Vec::with_capacity(events.len());
                let mut v3: Vec<&str> = Vec::with_capacity(events.len());
                let mut v4: Vec<i32> = Vec::with_capacity(events.len());
                let mut v5: Vec<Value> = Vec::with_capacity(events.len());
                let mut v6: Vec<Option<Value>> = Vec::with_capacity(events.len());
                let mut v7: Vec<DateTime<Utc>> = Vec::with_capacity(events.len());

                events.iter().for_each(|event| {
                    version += 1;

                    v1.push(event.id);
                    v2.push(&event.name);
                    v3.push(&id);
                    v4.push(version);
                    v5.push(event.data.clone());
                    v6.push(event.metadata.clone());
                    v7.push(event.created_at);
                });

                sqlx::query(r#"
                    INSERT INTO evento_events (id, name, aggregate_id, version, data, metadata, created_at)
                    SELECT * FROM UNNEST ($1,$2,$3,$4,$5,$6,$7)"#
                )
                .bind(v1)
                .bind(v2)
                .bind(v3)
                .bind(v4)
                .bind(v5)
                .bind(v6)
                .bind(v7)
                .execute(&mut *tx)
                .await?;
            }

            let next_event_id = sqlx::query_as!(
                Event,
                r#"
        SELECT *
        FROM evento_events
        WHERE aggregate_id = $1 AND version = $2
        LIMIT 1
                "#,
                &id,
                original_version + 1
            )
            .fetch_optional(&pool)
            .await?
            .map(|e| e.id)
            .unwrap_or(events[0].id);

            if next_event_id != events[0].id {
                tx.rollback().await?;

                return Err(Error::UnexpectedOriginalVersion);
            }

            tx.commit().await?;

            Ok(version)
        })
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        let id: String = id.into();
        let pool = self.0.clone();

        Box::pin(async move {
            let events = sqlx::query_as!(
                Event,
                r#"
        SELECT *
        FROM evento_events
        WHERE aggregate_id = $1
        ORDER BY version
                "#,
                &id
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
    ) -> Pin<Box<dyn Future<Output = Result<i32, Error>>>> {
        self.0.save::<A, _>(id, events, original_version)
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = EngineResult<A>>>> {
        self.0.load(id)
    }
}