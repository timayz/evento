use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

pub enum Error {}

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

pub trait Aggregate: Default {
    fn apply(&mut self, event: Event);
}

pub trait Engine {
    fn save(&self, events: Vec<Event>) -> Result<u64, Error>;
    fn load<A: Aggregate>(&self, id: String) -> Result<A, Error>;
}

pub struct MemoryEngine;

impl Engine for MemoryEngine {
    fn save(&self, events: Vec<Event>) -> Result<u64, Error> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Result<A, Error> {
        todo!()
    }
}

pub struct PostgresEngine;

impl Engine for PostgresEngine {
    fn save(&self, events: Vec<Event>) -> Result<u64, Error> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Result<A, Error> {
        todo!()
    }
}

pub struct EventStore<E: Engine> {
    engine: E,
}

impl<E: Engine> EventStore<E> {
    pub fn new<C: Into<String>>(conn: C) -> Self {
        todo!()
    }

    pub fn set_engine(mut self, e: E) -> Self {
        self.engine = e;

        self
    }
}

impl<E: Engine> Engine for EventStore<E> {
    fn save(&self, events: Vec<Event>) -> Result<u64, Error> {
        todo!()
    }

    fn load<A: Aggregate>(&self, id: String) -> Result<A, Error> {
        todo!()
    }
}
