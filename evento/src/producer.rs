use std::collections::HashMap;

use evento_store::{Aggregate, Engine, Event, Result, Store, WriteEvent};
use serde_json::Value;

#[cfg(feature = "memory")]
pub type MemoryProducer = Producer<evento_store::Memory>;

#[cfg(feature = "pg")]
pub type PgProducer = Producer<evento_store::Pg>;

#[derive(Clone)]
pub struct Producer<S: Engine> {
    pub name: Option<String>,
    pub store: Store<S>,
}

impl<S: Engine + Send + Sync + Clone> Producer<S> {
    pub async fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        event: WriteEvent,
        original_version: u16,
    ) -> Result<Vec<Event>> {
        self.publish_all::<A, I>(id, vec![event], original_version)
            .await
    }

    pub async fn publish_all<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<WriteEvent>,
        original_version: u16,
    ) -> Result<Vec<Event>> {
        let id = id.into();

        let name = match self.store.first_of::<A>(&id).await? {
            Some(event) => event
                .to_metadata::<HashMap<String, Value>>()?
                .unwrap_or_default()
                .get("_evento_name")
                .cloned(),
            _ => self.name.to_owned().map(Value::String),
        };

        let mut updated_events = Vec::new();
        for event in events.iter() {
            let mut metadata = event
                .to_metadata::<HashMap<String, Value>>()?
                .unwrap_or_default();

            if let Some(name) = &name {
                metadata.insert("_evento_name".to_owned(), name.clone());
            }

            metadata.insert(
                "_evento_topic".to_owned(),
                Value::String(A::aggregate_type().to_owned()),
            );

            let event = event.clone().metadata(metadata)?;

            updated_events.push(event);
        }

        self.store
            .write_all::<A>(id, updated_events, original_version)
            .await
    }

    pub async fn load<A: Aggregate, I: Into<String>>(&self, id: I) -> Result<Option<(A, u16)>> {
        self.store.load::<A>(id).await
    }
}
