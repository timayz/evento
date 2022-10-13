use std::{collections::HashMap, future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Error {
    UnexpectedOriginalVersion,
}

#[derive(Default)]
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
    fn apply(&mut self, event: Event);
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

pub struct MemoryStore(HashMap<String, Vec<Event>>);

impl MemoryStore {
    pub fn new() -> EventStore<Self> {
        EventStore(Self(HashMap::new()))
    }
}

impl Engine for MemoryStore {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(A, Event)>, Error>>>> {
        todo!()
    }
}

pub struct PostgresStore;

impl PostgresStore {
    pub fn new<C: Into<String>>(conn: C) -> EventStore<Self> {
        todo!()
    }
}

impl Engine for PostgresStore {
    fn save<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
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
