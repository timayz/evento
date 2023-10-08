use std::{collections::HashMap, sync::Arc};

use evento_store::Store;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{Context, Engine, Subscriber};

struct ConsumerName(Option<String>);

#[derive(Debug, Clone, Default)]
pub struct ConsumerContext(Arc<RwLock<Context>>);

impl ConsumerContext {
    pub fn name(&self) -> Option<String> {
        self.0.read().extract::<ConsumerName>().0.to_owned()
    }

    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.0.read().extract::<T>().clone()
    }

    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.0.read().get::<T>().cloned()
    }
}

#[derive(Clone)]
pub struct Consumer<E: Engine, S: evento_store::Engine> {
    pub(super) engine: E,
    pub(super) store: Store<S>,
    subscribers: HashMap<String, Subscriber<S>>,
    id: Uuid,
    name: Option<String>,
    context: ConsumerContext,
}

impl<E: Engine, S: evento_store::Engine + Clone> Consumer<E, S> {
    pub(super) fn create(engine: E, store: Store<S>) -> Self {
        Self {
            engine,
            store,
            subscribers: HashMap::new(),
            context: ConsumerContext::default(),
            name: None,
            id: Uuid::new_v4(),
        }
    }

    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self.context
            .0
            .write()
            .insert(ConsumerName(self.name.to_owned()));
        self
    }

    pub fn data<V: Send + Sync + 'static>(self, v: V) -> Self {
        self.context.0.write().insert(v);
        self
    }

    pub fn subscribe(mut self, sub: Subscriber<S>) -> Self {
        let mut sub = sub.clone();

        if let Some(name) = self.name.as_ref() {
            sub.key = format!("{}.{}", name, sub.key);
        }

        self.subscribers.insert(sub.key.to_owned(), sub);
        self
    }
}
