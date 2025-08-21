use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use thiserror::Error;
use ulid::Ulid;

use crate::{cursor::Cursor, Aggregator, AggregatorName, Event, Executor, LoadResult};

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("invalid original version")]
    InvalidOriginalVersion,

    #[error("missing data")]
    MissingData,

    #[error("missing metadata")]
    MissingMetadata,

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ciborium.ser >> {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("systemtime >> {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

pub struct SaveBuilder<A: Aggregator> {
    aggregator_id: String,
    aggregator_type: String,
    aggregator: Option<A>,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: i32,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Option<Vec<u8>>,
}

impl<A: Aggregator> SaveBuilder<A> {
    pub fn new(aggregator: Option<A>, aggregator_id: impl Into<String>) -> SaveBuilder<A> {
        SaveBuilder {
            aggregator_id: aggregator_id.into(),
            aggregator,
            aggregator_type: A::name().to_owned(),
            routing_key: None,
            routing_key_locked: false,
            original_version: 0,
            data: Vec::default(),
            metadata: None,
        }
    }

    pub fn original_version(mut self, v: u16) -> Self {
        self.original_version = v as i32;

        self
    }

    pub fn routing_key(self, v: impl Into<String>) -> Self {
        self.routing_key_opt(Some(v.into()))
    }

    pub fn routing_key_opt(mut self, v: Option<String>) -> Self {
        if !self.routing_key_locked {
            self.routing_key = v;
            self.routing_key_locked = true;
        }

        self
    }

    pub fn metadata<M: Serialize>(
        mut self,
        v: &M,
    ) -> Result<Self, ciborium::ser::Error<std::io::Error>> {
        let mut metadata = Vec::new();
        ciborium::into_writer(v, &mut metadata)?;
        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn data<D: Serialize + AggregatorName>(
        mut self,
        v: &D,
    ) -> Result<Self, ciborium::ser::Error<std::io::Error>> {
        let mut data = Vec::new();
        ciborium::into_writer(v, &mut data)?;
        self.data.push((D::name(), data));

        Ok(self)
    }

    pub async fn commit<E: Executor>(&self, executor: &E) -> Result<String, WriteError> {
        let (mut aggregator, mut version, routing_key) = match &self.aggregator {
            Some(aggregator) => (
                aggregator.clone(),
                self.original_version,
                self.routing_key.to_owned(),
            ),
            _ => {
                let aggregator = crate::load::<A, _>(executor, &self.aggregator_id)
                    .await
                    .map_err(|err| WriteError::Unknown(err.into()))?;

                (
                    aggregator.item,
                    aggregator.event.version,
                    aggregator.event.routing_key,
                )
            }
        };

        let Some(metadata) = &self.metadata else {
            return Err(WriteError::MissingMetadata);
        };

        let mut events = vec![];
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        for (name, data) in &self.data {
            version += 1;

            let event = Event {
                id: Ulid::new(),
                name: name.to_string(),
                data: data.to_vec(),
                metadata: metadata.to_vec(),
                timestamp,
                aggregator_id: self.aggregator_id.to_owned(),
                aggregator_type: self.aggregator_type.to_owned(),
                version,
                routing_key: routing_key.to_owned(),
            };

            aggregator.aggregate(&event).await?;
            events.push(event);
        }

        let Some(last_event) = events.last().cloned() else {
            return Err(WriteError::MissingData);
        };

        executor.write(events).await?;

        let mut data = vec![];
        ciborium::into_writer(&aggregator, &mut data)?;
        let cursor = last_event.serialize_cursor()?;

        executor
            .save_snapshot::<A>(last_event.aggregator_id, data, cursor)
            .await?;

        Ok(self.aggregator_id.to_owned())
    }
}

pub fn create<A: Aggregator>() -> SaveBuilder<A> {
    SaveBuilder::new(Some(A::default()), Ulid::new())
}

pub fn save<A: Aggregator>(id: impl Into<String>) -> SaveBuilder<A> {
    SaveBuilder::new(None, id)
}

pub fn save_with<A: Aggregator>(aggregator: LoadResult<A>) -> SaveBuilder<A> {
    SaveBuilder::new(Some(aggregator.item), aggregator.event.aggregator_id)
        .original_version(aggregator.event.version as u16)
        .routing_key_opt(aggregator.event.routing_key)
}
