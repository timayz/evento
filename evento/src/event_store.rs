use parking_lot::RwLock;
use rbatis::rbdc::datetime::FastDateTime;
use rbatis::{impl_insert, impl_select, Rbatis};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, pin::Pin};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    UnexpectedOriginalVersion,
    Rbatis(String),
}

impl From<rbatis::rbdc::Error> for Error {
    fn from(e: rbatis::rbdc::Error) -> Self {
        Error::Rbatis(e.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub version: i32,
    pub data: Option<String>,
    pub metadata: Option<String>,
    pub created_at: FastDateTime,
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
        self.data = Some(serde_json::to_string(&value)?);

        Ok(self)
    }

    pub fn metadata<M: Serialize>(mut self, value: M) -> Result<Self, serde_json::Error> {
        self.metadata = Some(serde_json::to_string(&value)?);

        Ok(self)
    }

    pub fn to_data<'de, D: Deserialize<'de>>(&'de self) -> Result<Option<D>, serde_json::Error> {
        match &self.data {
            Some(data) => Ok(serde_json::from_str(data.as_str())?),
            None => Ok(None),
        }
    }

    pub fn to_metadata<'de, D: Deserialize<'de>>(
        &'de self,
    ) -> Result<Option<D>, serde_json::Error> {
        match &self.metadata {
            Some(metadata) => Ok(serde_json::from_str(metadata.as_str())?),
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
            data: None,
            metadata: None,
            created_at: FastDateTime::utc(),
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

impl_insert!(Event {}, "evento_events");
impl_select!(Event{select_by_aggregate_id_version(table_name:String,aggregate_id:&str,version:i32) -> Option => "`where aggregate_id = #{aggregate_id} and version = #{version}` limit 1"});
impl_select!(Event{select_all_by_aggregate_id(table_name:String,aggregate_id:&str) => "`where aggregate_id = #{aggregate_id}` ORDER BY version"});

#[derive(Clone)]
pub struct RbatisEngine(Rbatis);

impl RbatisEngine {
    pub fn new(rb: Rbatis) -> EventStore<Self> {
        EventStore(Self(rb))
    }
}

impl Engine for RbatisEngine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<i32, Error>>>> {
        let id: String = id.into();
        let rb = self.0.clone();

        Box::pin(async move {
            let mut tx = rb.acquire_begin().await?;

            let mut version = original_version;
            let mut batch_size: u64 = 0;
            let mut tables = Vec::new();

            for event in events {
                batch_size += 1;
                version += 1;

                tables.push(event.aggregate_id(id.to_owned()).version(version));
            }

            Event::insert_batch(&mut tx, &tables, batch_size).await?;

            let next_id = Event::select_by_aggregate_id_version(
                &mut tx,
                "evento_events".to_owned(),
                &id,
                original_version + 1,
            )
            .await?
            .map(|e| e.id)
            .unwrap_or(tables[0].id);

            if next_id != tables[0].id {
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
        let rb = self.0.clone();

        Box::pin(async move {
            let mut rb = rb.acquire_begin().await?;

            let events =
                Event::select_all_by_aggregate_id(&mut rb, "evento_events".to_owned(), &id).await?;

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
