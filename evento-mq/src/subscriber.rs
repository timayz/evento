use std::collections::HashSet;

use anyhow::Result;
use async_trait::async_trait;
use dyn_clone::DynClone;
use evento_store::{Engine, Event, Store};

#[async_trait]
pub trait SubscribeHandler: DynClone + Send + Sync {
    async fn handle(&self, event: Event) -> Result<Option<Event>>;
}

dyn_clone::clone_trait_object!(SubscribeHandler);

#[async_trait]
pub trait PostSubscribeHandler: DynClone {
    async fn handle(&self, event: Event) -> Result<()>;
}

dyn_clone::clone_trait_object!(PostSubscribeHandler);

#[derive(Clone)]
pub struct Subscriber<E: Engine> {
    pub(crate) key: String,
    pub(crate) store: Option<Store<E>>,
    pub(crate) handlers: Vec<(String, Box<dyn SubscribeHandler + 'static>)>,
    pub(crate) post_handlers: Vec<Box<dyn PostSubscribeHandler + 'static>>,
    pub(crate) filters: HashSet<String>,
    pub(crate) with_cursor: bool,
}

impl<E: Engine> Subscriber<E> {
    pub fn new<K: Into<String>>(key: K) -> Self {
        Self {
            key: key.into(),
            store: None,
            filters: HashSet::new(),
            handlers: Vec::new(),
            post_handlers: Vec::new(),
            with_cursor: true,
        }
    }

    pub fn handler<H: SubscribeHandler + 'static>(
        mut self,
        filter: impl Into<String>,
        handler: H,
    ) -> Self {
        let filter = filter.into();
        self.filters.insert(filter.to_owned());
        self.handlers.push((filter, Box::new(handler)));

        self
    }

    pub fn post_handler<H: PostSubscribeHandler + 'static>(mut self, handler: H) -> Self {
        self.post_handlers.push(Box::new(handler));

        self
    }

    pub fn with_cursor(mut self, value: bool) -> Self {
        self.with_cursor = value;

        self
    }

    pub fn store(mut self, store: Store<E>) -> Self {
        self.store = Some(store);

        self
    }
}
