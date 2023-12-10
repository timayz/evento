use evento_store::{Aggregate, Event, Result, Store, WriteEvent};
use serde::Serialize;
use serde_json::Value;
use std::{collections::HashMap, marker::PhantomData};

#[derive(Clone)]
pub struct Producer {
    pub name: Option<String>,
    pub store: Store,
}

impl Producer {
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

    pub fn aggregate<A: Aggregate>(&self, value: impl Into<String>) -> Publisher<'_, A> {
        let aggregate_id = value.into();

        Publisher {
            producer: self,
            aggregate_id,
            original_version: 0,
            metadata: None,
            events: Vec::new(),
            aggregate: PhantomData,
        }
    }
}

pub trait PublisherEvent {
    type Output: Into<String>;

    fn event_name() -> Self::Output;
}

pub struct Publisher<'a, A: Aggregate> {
    producer: &'a Producer,
    aggregate_id: String,
    original_version: u16,
    events: Vec<WriteEvent>,
    metadata: Option<Value>,
    aggregate: PhantomData<A>,
}

impl<'a, A: Aggregate> Publisher<'a, A> {
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

    pub async fn publish(&self) -> Result<Vec<Event>> {
        self.producer
            .publish_all::<A, _>(
                &self.aggregate_id,
                self.events.clone(),
                self.original_version,
            )
            .await
    }
}
