//! Event creation and committing.
//!
//! This module provides the [`AggregatorBuilder`] for creating and persisting events.
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::{create, aggregator};
//!
//! // Create a new aggregate with auto-generated ID
//! let id = create()
//!     .event(&AccountOpened { owner: "John".into() })
//!     .metadata(&metadata)
//!     .routing_key("accounts")
//!     .commit(&executor)
//!     .await?;
//!
//! // Add events to existing aggregate
//! aggregator(&existing_id)
//!     .original_version(current_version)
//!     .event(&MoneyDeposited { amount: 100 })
//!     .commit(&executor)
//!     .await?;
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use ulid::Ulid;

use crate::{cursor::Args, Event, Executor, ReadAggregator};

/// Errors that can occur when writing events.
#[derive(Debug, Error)]
pub enum WriteError {
    /// Version conflict - another event was written concurrently
    #[error("invalid original version")]
    InvalidOriginalVersion,

    /// Attempted to commit without adding any events
    #[error("trying to commit event without data")]
    MissingData,

    /// Unknown error from the executor
    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    /// System time error
    #[error("systemtime >> {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

/// Builder for creating and committing events.
///
/// Use [`create()`] or [`aggregator()`] to create an instance, then chain
/// method calls to add events and metadata before committing.
///
/// # Optimistic Concurrency
///
/// If `original_version` is 0 (default for new aggregates), the builder
/// queries the current version before writing. Otherwise, it uses the
/// provided version for optimistic concurrency control.
///
/// # Example
///
/// ```rust,ignore
/// // New aggregate
/// let id = create()
///     .event(&MyEvent { ... })
///     .commit(&executor)
///     .await?;
///
/// // Existing aggregate with version check
/// aggregator(&id)
///     .original_version(5)
///     .event(&AnotherEvent { ... })
///     .commit(&executor)
///     .await?;
/// ```
pub struct AggregatorBuilder {
    aggregator_id: String,
    aggregator_type: String,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: u16,
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
        self.original_version = v;

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

    pub fn metadata<M>(mut self, v: &M) -> Self
    where
        M: bitcode::Encode,
    {
        self.metadata = Some(bitcode::encode(v));
        self
    }

    pub fn event<D>(mut self, v: &D) -> Self
    where
        D: crate::projection::Event + bitcode::Encode,
    {
        self.data.push((D::event_name(), bitcode::encode(v)));
        self.aggregator_type = D::aggregator_type().to_owned();
        self
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
                .map_err(WriteError::Unknown)?;

            match events.edges.first() {
                Some(event) => (event.node.version, event.node.routing_key.to_owned()),
                _ => (self.original_version, self.routing_key.to_owned()),
            }
        } else {
            (self.original_version, self.routing_key.to_owned())
        };

        let metadata = self
            .metadata
            .to_owned()
            .unwrap_or_else(|| bitcode::encode(&true));

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

/// Creates a new aggregate with an auto-generated ULID.
///
/// # Example
///
/// ```rust,ignore
/// let id = create()
///     .event(&AccountOpened { ... })
///     .commit(&executor)
///     .await?;
/// ```
pub fn create() -> AggregatorBuilder {
    AggregatorBuilder::new(Ulid::new())
}

/// Creates a builder for an existing aggregate.
///
/// # Example
///
/// ```rust,ignore
/// aggregator(&existing_id)
///     .original_version(current_version)
///     .event(&MoneyDeposited { amount: 100 })
///     .commit(&executor)
///     .await?;
/// ```
pub fn aggregator(id: impl Into<String>) -> AggregatorBuilder {
    AggregatorBuilder::new(id)
}
