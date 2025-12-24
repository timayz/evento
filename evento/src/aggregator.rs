use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use ulid::Ulid;

use crate::{cursor::Args, Event, Executor, ReadAggregator};

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("invalid original version")]
    InvalidOriginalVersion,

    #[error("trying to commit event without data")]
    MissingData,

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("bincode.encode >> {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    #[error("systemtime >> {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

pub struct AggregatorBuilder {
    aggregator_id: String,
    aggregator_type: String,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: i32,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Option<Vec<u8>>,
}

impl AggregatorBuilder {
    pub fn new(aggregator_id: impl Into<String>) -> AggregatorBuilder {
        AggregatorBuilder {
            aggregator_id: aggregator_id.into(),
            aggregator_type: "".to_owned(),
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

    pub fn metadata<M: bincode::Encode>(
        mut self,
        v: &M,
    ) -> Result<Self, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let metadata = bincode::encode_to_vec(v, config)?;
        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn event<D: bincode::Encode + crate::projection::Event>(
        mut self,
        v: &D,
    ) -> Result<Self, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let data = bincode::encode_to_vec(v, config)?;
        self.data.push((D::event_name(), data));
        self.aggregator_type = D::aggregator_type().to_owned();

        Ok(self)
    }

    pub async fn commit<E: Executor>(&self, executor: &E) -> Result<String, WriteError> {
        let (mut version, routing_key) = if self.original_version == 0 {
            let events = executor
                .read(
                    Some(vec![ReadAggregator::id(
                        &self.aggregator_type,
                        &self.aggregator_id,
                    )]),
                    None,
                    Args::backward(1, None),
                )
                .await
                .map_err(|e| WriteError::Unknown(e.into()))?;

            match events.edges.first() {
                Some(event) => (event.node.version, event.node.routing_key.to_owned()),
                _ => (self.original_version, self.routing_key.to_owned()),
            }
        } else {
            (self.original_version, self.routing_key.to_owned())
        };

        let metadata = self.metadata.to_owned().unwrap_or_else(|| {
            let config = bincode::config::standard();
            bincode::encode_to_vec(true, config).expect("Should never failed")
        });

        let mut events = vec![];
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        for (name, data) in &self.data {
            version += 1;

            let event = Event {
                id: Ulid::new(),
                name: name.to_string(),
                data: data.to_vec(),
                metadata: metadata.to_vec(),
                timestamp: now.as_secs(),
                timestamp_subsec: now.subsec_millis(),
                aggregator_id: self.aggregator_id.to_owned(),
                aggregator_type: self.aggregator_type.to_owned(),
                version,
                routing_key: routing_key.to_owned(),
            };

            events.push(event);
        }

        if events.is_empty() {
            return Err(WriteError::MissingData);
        }

        executor.write(events).await?;

        Ok(self.aggregator_id.to_owned())
    }
}

pub fn create() -> AggregatorBuilder {
    AggregatorBuilder::new(Ulid::new())
}

pub fn aggregator(id: impl Into<String>) -> AggregatorBuilder {
    AggregatorBuilder::new(id)
}
