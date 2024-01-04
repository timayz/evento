use evento_store::{Aggregate, Applier, Event, Result, Store, WriteEvent};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Producer {
    pub name: Option<String>,
    pub store: Store,
}

impl Producer {
    pub async fn load<A: Aggregate + Applier, I: Into<String>>(
        &self,
        id: I,
    ) -> Result<Option<(A, u16)>> {
        self.store.load::<A>(id).await
    }

    pub fn write(&self, value: impl Into<String>) -> Publisher<'_> {
        let aggregate_id = value.into();

        Publisher {
            producer: self,
            aggregate_id,
            original_version: 0,
            metadata: None,
            events: Vec::new(),
        }
    }
}

pub trait PublisherEvent {
    type Output: Into<String>;

    fn event_name() -> Self::Output;
}

pub struct Publisher<'a> {
    producer: &'a Producer,
    aggregate_id: String,
    original_version: u16,
    events: Vec<WriteEvent>,
    metadata: Option<Value>,
}

impl<'a> Publisher<'a> {
    pub fn original_version(mut self, value: u16) -> Self {
        self.original_version = value;

        self
    }

    pub fn metadata(mut self, value: impl Serialize) -> Result<Self> {
        let metadata = serde_json::to_value(value)?;
        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn event<E: Serialize + PublisherEvent>(mut self, value: E) -> Result<Self> {
        self.events.push(
            WriteEvent::new(E::event_name())
                .data(value)?
                .raw_metadata(self.metadata.clone()),
        );

        Ok(self)
    }

    pub async fn commit<A: Aggregate>(&self) -> Result<Vec<Event>> {
        let name = match self
            .producer
            .store
            .first_of::<A>(&self.aggregate_id)
            .await?
        {
            Some(event) => event
                .to_metadata::<HashMap<String, Value>>()?
                .unwrap_or_default()
                .get("_evento_name")
                .cloned(),
            _ => self.producer.name.to_owned().map(Value::String),
        };

        let mut updated_events = Vec::new();
        for event in self.events.iter() {
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

        self.producer
            .store
            .write_all::<A>(&self.aggregate_id, updated_events, self.original_version)
            .await
    }
}
