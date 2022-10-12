use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub enum Error {}

#[derive(Default)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub aggregate_version: u64,
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

    pub fn aggregate_type<V: Into<String>>(mut self, value: V) -> Self {
        self.aggregate_type = value.into();

        self
    }

    pub fn aggregate_version(mut self, value: u64) -> Self {
        self.aggregate_version = value;

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
}

pub trait Engine {
    fn save(&self, events: Vec<Event>, original_version: u64) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>>;
    fn load<A: Aggregate>(&self, id: String) -> Pin<Box<dyn Future<Output = Result<A, Error>>>>;
}

pub struct MemoryStore;

impl MemoryStore {
    pub fn new() -> EventStore<Self> {
        todo!()
    }
}

impl Engine for MemoryStore {
    fn save(&self, events: Vec<Event>, original_version: u64) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Pin<Box<dyn Future<Output = Result<A, Error>>>> {
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
    fn save(&self, events: Vec<Event>, original_version: u64) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Pin<Box<dyn Future<Output = Result<A, Error>>>> {
        todo!()
    }
}

pub struct EventStore<E: Engine>(E);

impl<E: Engine> Engine for EventStore<E> {
    fn save(&self, events: Vec<Event>, original_version: u64) -> Pin<Box<dyn Future<Output = Result<u64, Error>>>> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Pin<Box<dyn Future<Output = Result<A, Error>>>> {
        todo!()
    }
}
