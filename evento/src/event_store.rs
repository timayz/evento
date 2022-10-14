use std::{collections::HashMap, future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Error {
    UnexpectedOriginalVersion,
}

#[derive(Default, Clone)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub version: u64,
    pub data: Option<Value>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Event {
    pub fn new<'a, N: Into<String>>(name: N) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            created_at: Utc::now(),
            ..Self::default()
        }
    }

    pub fn aggregate_id<V: Into<String>>(mut self, value: V) -> Self {
        self.aggregate_id = value.into();

        self
    }

    pub fn version(mut self, value: u64) -> Self {
        self.version = value;

        self
    }

    pub fn data<D: Serialize>(mut self, value: D) -> Result<Self, serde_json::Error> {
        self.data = Some(serde_json::to_value(value)?);

        Ok(self)
    }

    pub fn metadata<M: Serialize>(mut self, value: M) -> Result<Self, serde_json::Error> {
        self.metadata = Some(serde_json::to_value(value)?);

        Ok(self)
    }
}

pub trait Aggregate: Default {
    fn apply<'a>(&mut self, event: &'a Event);
    fn aggregate_id<I: Into<String>>(id: I) -> String;
}

pub trait Engine {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>>;

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(A, Event)>, Error>>>>;
}

pub struct MemoryStore(RwLock<HashMap<String, Vec<Event>>>);

impl MemoryStore {
    pub fn new() -> EventStore<Self> {
        EventStore(Self(RwLock::new(HashMap::new())))
    }
}

impl Engine for MemoryStore {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<(A, Event)>, Error>>>> {
        let id: String = id.into();

        let events = {
            let data = self.0.read();

            data.get(&id)
                .map_or(Vec::new(), |events| events.iter().cloned().collect())
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

pub struct PostgresStore;

impl PostgresStore {
    pub fn new<C: Into<String>>(_conn: C) -> EventStore<Self> {
        todo!()
    }
}

impl Engine for PostgresStore {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        _id: I,
        _events: Vec<Event>,
        _original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        _id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(A, Event)>, Error>>>> {
        todo!()
    }
}

pub struct EventStore<E: Engine>(E);

impl<E: Engine> Engine for EventStore<E> {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        self.0.save::<A, _>(id, events, original_version)
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(A, Event)>, Error>>>> {
        self.0.load(id)
    }
}
