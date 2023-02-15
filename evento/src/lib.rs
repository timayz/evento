pub mod store;

mod context;
mod data;

pub use context::Context;
pub use data::Data;
use parking_lot::RwLock;
use sqlx::PgPool;
pub use store::{Aggregate, Error as StoreError, Event, EventStore};

use pikav::topic::TopicFilter;
use std::{collections::HashMap, future::Future, pin::Pin};
use store::{Engine as StoreEngine, EngineResult as StoreEngineResult};

type SubscirberHandler =
    fn(e: Event, ctx: &Context) -> Pin<Box<dyn Future<Output = Result<(), String>>>>;

#[derive(Clone)]
pub struct Subscriber {
    key: String,
    filters: Vec<TopicFilter>,
    handlers: Vec<SubscirberHandler>,
}

impl Subscriber {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            filters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    pub fn filter<T: Into<String>>(mut self, topic: T) -> Self {
        match TopicFilter::new(topic) {
            Ok(filter) => self.filters.push(filter),
            Err(e) => panic!("{e}"),
        };

        self
    }

    pub fn handler(mut self, handler: SubscirberHandler) -> Self {
        self.handlers.push(handler);
        self
    }
}

pub trait Engine {
    fn save<I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>>>>;
}

pub struct MemoryEngine(RwLock<HashMap<String, Vec<Event>>>);

impl MemoryEngine {
    pub fn new<S: StoreEngine>(store: EventStore<S>) -> Evento<Self, S> {
        Evento::new(Self(RwLock::new(HashMap::new())), store)
    }
}

impl Engine for MemoryEngine {
    fn save<I: Into<String>>(
        &self,
        _id: I,
        _events: Vec<Event>,
        _original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>>>> {
        todo!()
    }
}

pub struct PgEngine(PgPool);

impl PgEngine {
    pub fn new<S: StoreEngine>(pool: PgPool, store: EventStore<S>) -> Evento<Self, S> {
        Evento::new(Self(pool), store)
    }
}

impl Engine for PgEngine {
    fn save<I: Into<String>>(
        &self,
        _id: I,
        _events: Vec<Event>,
        _original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>>>> {
        todo!()
    }
}

pub struct Evento<E: Engine, S: StoreEngine> {
    name: Option<String>,
    engine: E,
    store: EventStore<S>,
    ctx: Context,
    subscribers: HashMap<String, Subscriber>,
}

impl<E: Engine, S: StoreEngine> Evento<E, S> {
    pub fn new(engine: E, store: EventStore<S>) -> Self {
        Self {
            engine,
            store,
            ctx: Context::new(),
            subscribers: HashMap::new(),
            name: None,
        }
    }

    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn data<U: 'static>(mut self, val: U) -> Self {
        self.ctx.insert(val);
        self
    }

    pub fn subscribe(mut self, s: Subscriber) -> Self {
        self.subscribers.insert(s.key.to_owned(), s);
        self
    }

    pub async fn run(&self) {
        todo!()
    }

    pub fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, StoreError>>>> {
        self.store.save::<A, _>(id, events, original_version)
    }

    pub fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> Pin<Box<dyn Future<Output = StoreEngineResult<A>>>> {
        self.store.load::<A, _>(id)
    }
}
