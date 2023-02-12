use std::{collections::HashMap, future::Future, pin::Pin};

use pikav::topic::TopicFilter;

use crate::{Aggregate, Context, Engine, Error, Event, EventStore};

type SubscirberHandler = fn(e: Event, ctx: Context) -> Result<(), String>;

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

pub struct EventBus<E: Engine>(EventStore<E>, Context, HashMap<String, Subscriber>);

impl<E: Engine> EventBus<E> {
    pub fn new(store: EventStore<E>) -> Self {
        Self(store, Context::new(), HashMap::new())
    }

    pub fn data<U: 'static>(mut self, val: U) -> Self {
        self.1.insert(val);
        self
    }

    pub fn subscribe(mut self, s: Subscriber) -> Self {
        self.2.insert(s.key.to_owned(), s);
        self
    }

    pub fn run(&self) {}

    pub fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<Event>,
        original_version: i32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Event>, Error>>>> {
        self.0.save::<A, _>(id, events, original_version)
    }
}
