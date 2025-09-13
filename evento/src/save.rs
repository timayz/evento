use std::time::{SystemTime, UNIX_EPOCH};

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

    #[error("bincode.encode >> {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

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

    pub fn metadata<M: bincode::Encode>(
        mut self,
        v: &M,
    ) -> Result<Self, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let metadata = bincode::encode_to_vec(v, config)?;
        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn data<D: bincode::Encode + AggregatorName>(
        mut self,
        v: &D,
    ) -> Result<Self, bincode::error::EncodeError> {
        let config = bincode::config::standard();
        let data = bincode::encode_to_vec(v, config)?;
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

        let config = bincode::config::standard();
        let data = bincode::encode_to_vec(&aggregator, config)?;
        let cursor = last_event.serialize_cursor()?;

        executor
            .save_snapshot::<A>(last_event.aggregator_id, data, cursor)
            .await?;

        Ok(self.aggregator_id.to_owned())
    }
}

/// Create a new aggregate with initial events
///
/// Creates a builder for generating events that will create a new aggregate instance.
/// The aggregate starts in its default state and the generated ID is a new ULID.
///
/// # Examples
///
/// ```no_run
/// use evento::create;
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
///     let user_id = create::<User>()
///         .data(&UserCreated {
///             name: "John Doe".to_string(),
///         })?
///         .metadata(&true)?
///         .commit(executor)
///         .await?;
///     
///     println!("Created user with ID: {}", user_id);
///     Ok(user_id)
/// }
/// ```
pub fn create<A: Aggregator>() -> SaveBuilder<A> {
    SaveBuilder::new(Some(A::default()), Ulid::new())
}

/// Add events to an existing aggregate
///
/// Creates a builder for adding events to an aggregate with the specified ID.
/// The current state will be loaded from the event store before applying new events.
///
/// # Parameters
///
/// - `id`: The ID of the aggregate to modify
///
/// # Examples
///
/// ```no_run
/// use evento::save;
/// # use evento::*;
/// # use bincode::{Encode, Decode};
/// # #[derive(AggregatorName, Encode, Decode)]
/// # struct UserEmailChanged { email: String }
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User;
/// # #[evento::aggregator]
/// # impl User {}
///
/// async fn update_user_email(
///     executor: &evento::Sqlite,
///     user_id: &str,
///     new_email: &str,
/// ) -> anyhow::Result<String> {
///     let result_id = save::<User>(user_id)
///         .data(&UserEmailChanged {
///             email: new_email.to_string(),
///         })?
///         .metadata(&false)?
///         .commit(executor)
///         .await?;
///     
///     println!("Updated user {} with new email", result_id);
///     Ok(result_id)
/// }
/// ```
pub fn save<A: Aggregator>(id: impl Into<String>) -> SaveBuilder<A> {
    SaveBuilder::new(None, id)
}

/// Save events to an aggregate using a loaded aggregate state
///
/// Creates a builder for adding events to an aggregate using a previously loaded
/// [`LoadResult`]. This is more efficient than [`save`] because it avoids loading
/// the aggregate from the event store again.
///
/// The builder is pre-configured with the aggregate's current state, version, and routing key.
///
/// # Parameters
///
/// - `aggregator`: A [`LoadResult`] from [`load`] containing the aggregate state
///
/// # Examples
///
/// ```no_run
/// use evento::{load, save_with};
/// # use evento::*;
/// # use bincode::{Encode, Decode};
/// # #[derive(AggregatorName, Encode, Decode)]
/// # struct UserEmailChanged { email: String }
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User;
/// # #[evento::aggregator]
/// # impl User {}
///
/// async fn update_user_efficiently(
///     executor: &evento::Sqlite,
///     user_id: &str,
///     new_email: &str,
/// ) -> anyhow::Result<String> {
///     // Load the current state
///     let user = load::<User, _>(executor, user_id).await?;
///     
///     // Save using the loaded state (more efficient)
///     let result_id = save_with(user)
///         .data(&UserEmailChanged {
///             email: new_email.to_string(),
///         })?
///         .metadata(&false)?
///         .commit(executor)
///         .await?;
///     
///     Ok(result_id)
/// }
/// ```
pub fn save_with<A: Aggregator>(aggregator: LoadResult<A>) -> SaveBuilder<A> {
    SaveBuilder::new(Some(aggregator.item), aggregator.event.aggregator_id)
        .original_version(aggregator.event.version as u16)
        .routing_key_opt(aggregator.event.routing_key)
}
