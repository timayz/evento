//! Type-safe builder for creating and saving events
//!
//! This module provides a type-state pattern implementation for building events,
//! ensuring that invalid states are impossible to represent at compile time.

use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use crate::{Aggregator, AggregatorName, Event, Executor, LoadResult, WriteError, cursor::Cursor};

/// Type-state marker for builders without data
pub struct NoData;

/// Type-state marker for builders with data
pub struct WithData;

/// Type-state marker for builders without metadata
pub struct NoMetadata;

/// Type-state marker for builders with metadata
pub struct WithMetadata;

/// Type-safe event builder using the type-state pattern
/// 
/// This builder ensures that you cannot commit events without both data and metadata,
/// making invalid states unrepresentable at compile time.
/// 
/// # Type Parameters
/// 
/// - `A`: The aggregate type
/// - `D`: Data state (NoData or WithData)
/// - `M`: Metadata state (NoMetadata or WithMetadata)
pub struct TypedSaveBuilder<A: Aggregator, D, M> {
    aggregator_id: String,
    aggregator_type: String,
    aggregator: Option<A>,
    routing_key: Option<String>,
    original_version: i32,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Option<Vec<u8>>,
    _data_state: std::marker::PhantomData<D>,
    _metadata_state: std::marker::PhantomData<M>,
}

impl<A: Aggregator> TypedSaveBuilder<A, NoData, NoMetadata> {
    /// Create a new builder instance
    pub(crate) fn new(aggregator: Option<A>, aggregator_id: impl Into<String>) -> Self {
        Self {
            aggregator_id: aggregator_id.into(),
            aggregator_type: A::name().to_owned(),
            aggregator,
            routing_key: None,
            original_version: 0,
            data: Vec::new(),
            metadata: None,
            _data_state: std::marker::PhantomData,
            _metadata_state: std::marker::PhantomData,
        }
    }
}

impl<A: Aggregator, D, M> TypedSaveBuilder<A, D, M> {
    /// Set the original version for optimistic concurrency control
    pub fn original_version(mut self, version: u16) -> Self {
        self.original_version = version as i32;
        self
    }

    /// Set the routing key for event distribution
    pub fn routing_key(mut self, key: impl Into<String>) -> Self {
        self.routing_key = Some(key.into());
        self
    }

    /// Set an optional routing key for event distribution
    pub fn routing_key_opt(mut self, key: Option<String>) -> Self {
        self.routing_key = key;
        self
    }
}

impl<A: Aggregator, M> TypedSaveBuilder<A, NoData, M> {
    /// Add event data to the builder
    /// 
    /// This transitions the builder to the WithData state, allowing it to be committed
    /// (once metadata is also provided).
    pub fn data<T: bincode::Encode + AggregatorName>(
        mut self,
        event_data: &T,
    ) -> Result<TypedSaveBuilder<A, WithData, M>, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let data = bincode::encode_to_vec(event_data, config)?;
        self.data.push((T::name(), data));

        Ok(TypedSaveBuilder {
            aggregator_id: self.aggregator_id,
            aggregator_type: self.aggregator_type,
            aggregator: self.aggregator,
            routing_key: self.routing_key,
            original_version: self.original_version,
            data: self.data,
            metadata: self.metadata,
            _data_state: std::marker::PhantomData,
            _metadata_state: std::marker::PhantomData,
        })
    }
}

impl<A: Aggregator, D> TypedSaveBuilder<A, D, NoMetadata> {
    /// Add metadata to the builder
    /// 
    /// This transitions the builder to the WithMetadata state, allowing it to be committed
    /// (once data is also provided).
    pub fn metadata<T: bincode::Encode>(
        mut self,
        metadata: &T,
    ) -> Result<TypedSaveBuilder<A, D, WithMetadata>, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let encoded_metadata = bincode::encode_to_vec(metadata, config)?;
        self.metadata = Some(encoded_metadata);

        Ok(TypedSaveBuilder {
            aggregator_id: self.aggregator_id,
            aggregator_type: self.aggregator_type,
            aggregator: self.aggregator,
            routing_key: self.routing_key,
            original_version: self.original_version,
            data: self.data,
            metadata: self.metadata,
            _data_state: std::marker::PhantomData,
            _metadata_state: std::marker::PhantomData,
        })
    }
}

impl<A: Aggregator, D> TypedSaveBuilder<A, D, WithMetadata> {
    /// Add additional event data to the builder
    /// 
    /// This allows adding multiple events in a single transaction.
    pub fn add_data<T: bincode::Encode + AggregatorName>(
        mut self,
        event_data: &T,
    ) -> Result<TypedSaveBuilder<A, WithData, WithMetadata>, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let data = bincode::encode_to_vec(event_data, config)?;
        self.data.push((T::name(), data));

        Ok(TypedSaveBuilder {
            aggregator_id: self.aggregator_id,
            aggregator_type: self.aggregator_type,
            aggregator: self.aggregator,
            routing_key: self.routing_key,
            original_version: self.original_version,
            data: self.data,
            metadata: self.metadata,
            _data_state: std::marker::PhantomData,
            _metadata_state: std::marker::PhantomData,
        })
    }
}

// Only allow commit when both data and metadata are present
impl<A: Aggregator> TypedSaveBuilder<A, WithData, WithMetadata> {
    /// Commit the events to the event store
    /// 
    /// This method is only available when both data and metadata have been provided,
    /// ensuring that incomplete events cannot be saved.
    pub async fn commit<E: Executor>(&self, executor: &E) -> Result<String, WriteError> {
        let (mut aggregator, mut version, routing_key) = match &self.aggregator {
            Some(aggregator) => (
                aggregator.clone(),
                self.original_version,
                self.routing_key.clone(),
            ),
            None => {
                let loaded = crate::load::<A, _>(executor, &self.aggregator_id)
                    .await
                    .map_err(|err| WriteError::Unknown(err.into()))?;

                (
                    loaded.item,
                    loaded.event.version,
                    loaded.event.routing_key,
                )
            }
        };

        let metadata = self.metadata.as_ref().expect("Metadata must be present");
        let mut events = Vec::new();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        for (name, data) in &self.data {
            version += 1;

            let event = Event {
                id: Ulid::new(),
                name: name.to_string(),
                data: data.clone(),
                metadata: metadata.clone(),
                timestamp,
                aggregator_id: self.aggregator_id.clone(),
                aggregator_type: self.aggregator_type.clone(),
                version,
                routing_key: routing_key.clone(),
            };

            aggregator.aggregate(&event).await?;
            events.push(event);
        }

        let last_event = events.last().cloned().expect("Events must not be empty");

        executor.write(events).await?;

        // Save snapshot
        let config = bincode::config::standard();
        let snapshot_data = bincode::encode_to_vec(&aggregator, config)?;
        let cursor = last_event.serialize_cursor()?;

        executor
            .save_snapshot::<A>(last_event.aggregator_id, snapshot_data, cursor)
            .await?;

        Ok(self.aggregator_id.clone())
    }
}

/// Create a new aggregate with initial events using the type-safe builder
/// 
/// This function returns a builder that ensures you provide both data and metadata
/// before committing the events.
/// 
/// # Examples
/// 
/// ```no_run
/// use evento::typed_create;
/// # use evento::*;
/// # use bincode::{Encode, Decode};
/// # #[derive(AggregatorName, Encode, Decode)]
/// # struct UserCreated { name: String }
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User;
/// # #[evento::aggregator]
/// # impl User {}
/// 
/// async fn create_user(executor: &evento::Sqlite) -> anyhow::Result<String> {
///     let user_id = typed_create::<User>()
///         .data(&UserCreated { name: "John".to_string() })?
///         .metadata(&true)?
///         .commit(executor)
///         .await?;
///     Ok(user_id)
/// }
/// ```
pub fn typed_create<A: Aggregator>() -> TypedSaveBuilder<A, NoData, NoMetadata> {
    TypedSaveBuilder::new(Some(A::default()), Ulid::new())
}

/// Add events to an existing aggregate using the type-safe builder
/// 
/// This function returns a builder that ensures you provide both data and metadata
/// before committing the events.
pub fn typed_save<A: Aggregator>(id: impl Into<String>) -> TypedSaveBuilder<A, NoData, NoMetadata> {
    TypedSaveBuilder::new(None, id)
}

/// Save events using a previously loaded aggregate state with the type-safe builder
/// 
/// This is more efficient than `typed_save` because it avoids loading the aggregate again.
pub fn typed_save_with<A: Aggregator>(
    loaded: LoadResult<A>,
) -> TypedSaveBuilder<A, NoData, NoMetadata> {
    TypedSaveBuilder::new(Some(loaded.item), loaded.event.aggregator_id)
        .original_version(loaded.event.version as u16)
        .routing_key_opt(loaded.event.routing_key)
}