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

use crate::{cursor::Args, metadata::Metadata, Event, Executor, ReadAggregator};

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

/// Trait for aggregate types.
///
/// Aggregates are the root entities in event sourcing. Each aggregate
/// type has a unique identifier string used for event storage and routing.
///
/// This trait is typically derived using the `#[evento::aggregator]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator("myapp/Account")]
/// #[derive(Default)]
/// pub struct Account {
///     pub balance: i64,
///     pub owner: String,
/// }
/// ```
pub trait Aggregator: Default {
    /// Returns the unique type identifier for this aggregate (e.g., "myapp/Account")
    fn aggregator_type() -> &'static str;
}

/// Trait for event types.
///
/// Events represent state changes that have occurred. Each event type
/// has a name and belongs to an aggregator type.
///
/// This trait is typically derived using the `#[evento::aggregator]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator("myapp/Account")]
/// #[derive(bitcode::Encode, bitcode::Decode)]
/// pub struct AccountOpened {
///     pub owner: String,
/// }
/// ```
pub trait AggregatorEvent: Aggregator {
    /// Returns the event name (e.g., "AccountOpened")
    fn event_name() -> &'static str;
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
#[derive(Clone)]
pub struct AggregatorBuilder {
    aggregator_id: String,
    aggregator_type: String,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: u16,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Metadata,
}

impl AggregatorBuilder {
    /// Creates a new builder for the given aggregate ID.
    pub fn new(aggregator_id: impl Into<String>) -> AggregatorBuilder {
        AggregatorBuilder {
            aggregator_id: aggregator_id.into(),
            aggregator_type: "".to_owned(),
            routing_key: None,
            routing_key_locked: false,
            original_version: 0,
            data: Vec::default(),
            metadata: Default::default(),
        }
    }

    /// Sets the expected version for optimistic concurrency control.
    ///
    /// If the aggregate's current version doesn't match, the commit will fail
    /// with [`WriteError::InvalidOriginalVersion`].
    pub fn original_version(&mut self, v: u16) -> &mut Self {
        self.original_version = v;

        self
    }

    /// Sets the routing key for event distribution.
    ///
    /// The routing key is used for partitioning events across consumers.
    /// Once set, subsequent calls are ignored (locked behavior).
    pub fn routing_key(&mut self, v: impl Into<String>) -> &mut Self {
        self.routing_key_opt(Some(v.into()))
    }

    /// Sets an optional routing key for event distribution.
    ///
    /// Pass `None` to explicitly clear the routing key.
    /// Once set, subsequent calls are ignored (locked behavior).
    pub fn routing_key_opt(&mut self, v: Option<String>) -> &mut Self {
        if !self.routing_key_locked {
            self.routing_key = v;
            self.routing_key_locked = true;
        }

        self
    }

    /// Sets the metadata to attach to all events.
    ///
    /// Metadata is serialized using bitcode and stored alongside each event.
    pub fn metadata<M>(&mut self, key: impl Into<String>, value: &M) -> &mut Self
    where
        M: bitcode::Encode,
    {
        self.metadata.insert_enc(key, value);
        self
    }

    pub fn requested_by(&mut self, value: impl Into<String>) -> &mut Self {
        self.metadata.set_requested_by(value);
        self
    }

    pub fn requested_as(&mut self, value: impl Into<String>) -> &mut Self {
        self.metadata.set_requested_as(value);
        self
    }

    /// Sets the metadata to attach to all events.
    ///
    /// Metadata is serialized using bitcode and stored alongside each event.
    pub fn metadata_from(&mut self, value: impl Into<Metadata>) -> &mut Self {
        self.metadata = value.into();
        self
    }

    /// Adds an event to be committed.
    ///
    /// Multiple events can be added and will be committed atomically.
    /// The event data is serialized using bitcode.
    pub fn event<D>(&mut self, v: &D) -> &mut Self
    where
        D: AggregatorEvent + bitcode::Encode,
    {
        self.data.push((D::event_name(), bitcode::encode(v)));
        self.aggregator_type = D::aggregator_type().to_owned();
        self
    }

    /// Commits all added events to the executor.
    ///
    /// Returns the aggregate ID on success.
    ///
    /// # Errors
    ///
    /// - [`WriteError::MissingData`] - No events were added
    /// - [`WriteError::InvalidOriginalVersion`] - Version conflict occurred
    /// - [`WriteError::Unknown`] - Executor error
    pub async fn commit<E: Executor>(&self, executor: &E) -> Result<String, WriteError> {
        let first_event = executor
            .read(
                Some(vec![ReadAggregator::id(
                    &self.aggregator_type,
                    &self.aggregator_id,
                )]),
                None,
                Args::forward(1, None),
            )
            .await
            .map_err(WriteError::Unknown)?;

        let routing_key = match first_event.edges.first() {
            Some(event) => event.node.routing_key.to_owned(),
            _ => self.routing_key.to_owned(),
        };

        let mut version = self.original_version;
        let mut events = vec![];
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        for (name, data) in &self.data {
            version += 1;

            let event = Event {
                id: Ulid::new(),
                name: name.to_string(),
                data: data.to_vec(),
                metadata: self.metadata.clone(),
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

pub trait AggregatorExecutor<E: Executor> {
    fn has_event<A: AggregatorEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<bool>> + Send;
}

impl<E: Executor> AggregatorExecutor<E> for E {
    fn has_event<A: AggregatorEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<bool>> + Send {
        let id = id.into();
        Box::pin(async {
            let result = self
                .read(
                    Some(vec![ReadAggregator::new(
                        A::aggregator_type(),
                        id,
                        A::event_name(),
                    )]),
                    None,
                    Args::backward(1, None),
                )
                .await?;

            Ok(!result.edges.is_empty())
        })
    }
}
