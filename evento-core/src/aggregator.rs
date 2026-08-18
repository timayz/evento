//! Event creation and committing.
//!
//! This module provides the [`WriteBuilder`] for creating and persisting events.
//!
//! # Example
//!
//! ```rust,no_run
//! use evento::{append, create, Executor};
//!
//! # #[evento::aggregate]
//! # pub enum Account {
//! #     AccountOpened { owner: String },
//! #     MoneyDeposited { amount: i64 },
//! # }
//! # async fn run<E: Executor>(executor: &E, existing_id: &str) -> anyhow::Result<()> {
//! // Create a new aggregate with auto-generated ID
//! let id = create()
//!     .event(&AccountOpened { owner: "John".into() })
//!     .metadata("request_id", &"abc123".to_owned())
//!     .routing_key("accounts")
//!     .commit(executor)
//!     .await?;
//!
//! // Add events to existing aggregate
//! append(existing_id)
//!     .original_version(1)
//!     .event(&MoneyDeposited { amount: 100 })
//!     .commit(executor)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use sha3::{Digest, Sha3_256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use ulid::Ulid;

use crate::{cursor::Args, metadata::Metadata, Event, EventFilter, Executor};

/// Derives a stable aggregate ID from a set of IDs by hashing them.
///
/// Each ID is length-prefixed before hashing so distinct ID lists can never
/// collide (`["ab", "c"]` and `["a", "bc"]` produce different digests).
pub fn hash_ids(ids: Vec<impl Into<String>>) -> String {
    let mut hasher = Sha3_256::new();
    for id in ids {
        let id: String = id.into();
        hasher.update((id.len() as u64).to_be_bytes());
        hasher.update(id);
    }

    hex::encode(hasher.finalize())
}

/// Errors that can occur when writing events.
#[derive(Debug, Error)]
pub enum WriteError {
    /// Version conflict - another event was written concurrently
    #[error("invalid original version")]
    InvalidOriginalVersion,

    /// Attempted to commit without adding any events
    #[error("trying to commit event without data")]
    MissingData,

    /// Events from more than one aggregate type were added to a single builder
    #[error("all events in one commit must belong to aggregate type {expected}, got {got}")]
    MixedAggregateTypes {
        /// The aggregate type of the first event added to the builder.
        expected: &'static str,
        /// The differing aggregate type of a later event.
        got: &'static str,
    },

    /// The aggregate reached the maximum representable version (`u16::MAX`)
    #[error("aggregate version overflow")]
    VersionOverflow,

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
/// This trait is typically derived using the `#[evento::aggregate]` macro.
///
/// # Example
///
/// ```rust
/// use evento::Aggregate;
///
/// #[evento::aggregate(name = "myapp/Account")]
/// pub enum Account {
///     AccountOpened { owner: String },
/// }
///
/// assert_eq!(Account::aggregate_type(), "myapp/Account");
/// ```
pub trait Aggregate: Default {
    /// Returns the unique type identifier for this aggregate (e.g., "myapp/Account")
    fn aggregate_type() -> &'static str;
}

/// Trait for event types.
///
/// Events represent state changes that have occurred. Each event type
/// has a name and belongs to an aggregate type.
///
/// This trait is typically derived using the `#[evento::aggregate]` macro.
///
/// # Example
///
/// Each variant of an `#[evento::aggregate]` enum becomes an event struct
/// implementing this trait:
///
/// ```rust
/// use evento::AggregateEvent;
///
/// #[evento::aggregate]
/// pub enum Account {
///     AccountOpened { owner: String },
/// }
///
/// assert_eq!(AccountOpened::event_name(), "AccountOpened");
/// ```
pub trait AggregateEvent: Aggregate {
    /// Returns the event name (e.g., "AccountOpened")
    fn event_name() -> &'static str;
}

/// Builder for creating and committing events.
///
/// Use [`create()`] or [`append()`] to create an instance, then chain
/// method calls to add events and metadata before committing.
///
/// # Optimistic Concurrency
///
/// `original_version` (default 0, meaning a brand-new aggregate) is the
/// version the caller last observed. Events are written as
/// `original_version + 1..`; if another writer committed in between, the
/// store rejects the write with [`WriteError::InvalidOriginalVersion`].
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{append, create, Executor};
/// # #[evento::aggregate]
/// # pub enum Counter {
/// #     Incremented { by: i64 },
/// #     Reset,
/// # }
/// # async fn run<E: Executor>(executor: &E) -> anyhow::Result<()> {
/// // New aggregate
/// let id = create()
///     .event(&Incremented { by: 1 })
///     .commit(executor)
///     .await?;
///
/// // Existing aggregate with version check
/// append(&id)
///     .original_version(5)
///     .event(&Reset)
///     .commit(executor)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct WriteBuilder {
    aggregate_id: String,
    aggregate_type: &'static str,
    routing_key: Option<String>,
    routing_key_locked: bool,
    original_version: u16,
    data: Vec<(&'static str, Vec<u8>)>,
    metadata: Metadata,
    mixed_types: Option<(&'static str, &'static str)>,
}

impl WriteBuilder {
    /// Creates a new builder for the given aggregate ID.
    pub fn new(aggregate_id: impl Into<String>) -> WriteBuilder {
        WriteBuilder {
            aggregate_id: aggregate_id.into(),
            aggregate_type: "",
            routing_key: None,
            routing_key_locked: false,
            original_version: 0,
            data: Vec::default(),
            metadata: Default::default(),
            mixed_types: None,
        }
    }

    /// Creates a new builder for the given aggregate IDs.
    pub fn ids(ids: Vec<impl Into<String>>) -> WriteBuilder {
        Self::new(hash_ids(ids))
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

    /// Inserts a single metadata entry attached to all events of this commit.
    ///
    /// Metadata is serialized using bitcode and stored alongside each event.
    /// Call multiple times to add several entries; note that a later
    /// [`metadata_from`](Self::metadata_from) call replaces the whole map.
    pub fn metadata<M>(&mut self, key: impl Into<String>, value: &M) -> &mut Self
    where
        M: bitcode::Encode,
    {
        self.metadata.insert_enc(key, value);
        self
    }

    /// Records who initiated this commit (e.g. a user id) in the metadata of
    /// all its events.
    pub fn requested_by(&mut self, value: impl Into<String>) -> &mut Self {
        self.metadata.set_requested_by(value);
        self
    }

    /// Records the role or identity the initiator acted as (e.g. when
    /// impersonating) in the metadata of all events of this commit.
    pub fn requested_as(&mut self, value: impl Into<String>) -> &mut Self {
        self.metadata.set_requested_as(value);
        self
    }

    /// Replaces the entire metadata map attached to all events of this commit.
    ///
    /// Any entries added earlier via [`metadata`](Self::metadata),
    /// [`requested_by`](Self::requested_by) or
    /// [`requested_as`](Self::requested_as) are discarded — call this first
    /// when combining it with per-entry setters.
    pub fn metadata_from(&mut self, value: impl Into<Metadata>) -> &mut Self {
        self.metadata = value.into();
        self
    }

    /// Adds an event to be committed.
    ///
    /// Multiple events can be added and will be committed atomically. All
    /// events in one builder must belong to the same aggregate type;
    /// [`commit`](Self::commit) fails with [`WriteError::MixedAggregateTypes`]
    /// otherwise. The event data is serialized using bitcode.
    pub fn event<D>(&mut self, v: &D) -> &mut Self
    where
        D: AggregateEvent + bitcode::Encode,
    {
        if self.aggregate_type.is_empty() {
            self.aggregate_type = D::aggregate_type();
        } else if self.aggregate_type != D::aggregate_type() && self.mixed_types.is_none() {
            self.mixed_types = Some((self.aggregate_type, D::aggregate_type()));
        }
        self.data.push((D::event_name(), bitcode::encode(v)));
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
        if self.data.is_empty() {
            return Err(WriteError::MissingData);
        }
        if let Some((expected, got)) = self.mixed_types {
            return Err(WriteError::MixedAggregateTypes { expected, got });
        }

        // An existing stream keeps the routing key of its first event — even
        // when that key is `None` — so one aggregate never spans two keys. Only
        // a brand-new aggregate consults the builder value or the executor's
        // configured default. `original_version == 0` declares a brand-new
        // stream, so the lookup is skipped entirely: if the stream secretly
        // exists, `write` fails with `InvalidOriginalVersion` before anything
        // is persisted, so a wrong key can never reach storage.
        let existing_key = if self.original_version == 0 {
            None
        } else {
            executor
                .stream_routing_key(self.aggregate_type.to_owned(), self.aggregate_id.to_owned())
                .await
                .map_err(WriteError::Unknown)?
        };
        let routing_key = match existing_key {
            Some(key) => key,
            None => self
                .routing_key
                .to_owned()
                .or_else(|| executor.default_routing_key().map(str::to_owned)),
        };

        let mut events = vec![];
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        for (offset, (name, data)) in self.data.iter().enumerate() {
            let version = u16::try_from(offset)
                .ok()
                .and_then(|o| self.original_version.checked_add(1)?.checked_add(o))
                .ok_or(WriteError::VersionOverflow)?;

            let event = Event {
                id: Ulid::generate(),
                name: name.to_string(),
                data: data.to_vec(),
                metadata: self.metadata.clone(),
                // Provisional stamp: backends that own ordering (SQL, Fjall)
                // replace it with their commit clock inside `write`.
                timestamp: now.as_secs(),
                timestamp_subsec: now.subsec_millis(),
                aggregate_id: self.aggregate_id.to_owned(),
                aggregate_type: self.aggregate_type.to_owned(),
                version,
                routing_key: routing_key.to_owned(),
            };

            events.push(event);
        }

        executor.write(events).await?;

        Ok(self.aggregate_id.to_owned())
    }
}

/// Creates a new aggregate with an auto-generated ULID.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{create, Executor};
/// # #[evento::aggregate]
/// # pub enum Account {
/// #     AccountOpened { owner: String },
/// # }
/// # async fn run<E: Executor>(executor: &E) -> anyhow::Result<()> {
/// let id = create()
///     .event(&AccountOpened { owner: "Alice".into() })
///     .commit(executor)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn create() -> WriteBuilder {
    WriteBuilder::new(Ulid::generate())
}

/// Creates a builder for an existing aggregate.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::{append, Executor};
/// # #[evento::aggregate]
/// # pub enum Account {
/// #     MoneyDeposited { amount: i64 },
/// # }
/// # async fn run<E: Executor>(executor: &E, existing_id: &str) -> anyhow::Result<()> {
/// append(existing_id)
///     .original_version(1)
///     .event(&MoneyDeposited { amount: 100 })
///     .commit(executor)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn append(id: impl Into<String>) -> WriteBuilder {
    WriteBuilder::new(id)
}

/// Convenience queries about an aggregate's event stream, implemented for
/// every [`Executor`].
pub trait AggregateExt<E: Executor> {
    /// Returns `true` if the aggregate `id` has at least one event of type `A`.
    fn has_event<A: AggregateEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<bool>> + Send;

    /// Returns the current version of the aggregate `id`'s stream (the version
    /// of its most recent event), or `None` when the stream is empty. Useful
    /// as input to `WriteBuilder::original_version` for optimistic concurrency.
    fn original_version<A: AggregateEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<Option<u16>>> + Send;
}

impl<E: Executor> AggregateExt<E> for E {
    fn has_event<A: AggregateEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<bool>> + Send {
        let id = id.into();
        Box::pin(async {
            let result = self
                .read(
                    Some(Arc::from([EventFilter::exact(
                        A::aggregate_type(),
                        id,
                        A::event_name(),
                    )])),
                    None,
                    Args::backward(1, None),
                    None,
                )
                .await?;

            Ok(!result.edges.is_empty())
        })
    }

    fn original_version<A: AggregateEvent>(
        &self,
        id: impl Into<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<Option<u16>>> + Send {
        let id = id.into();
        Box::pin(async {
            let result = self
                .read(
                    Some(Arc::from([EventFilter::by_id(A::aggregate_type(), id)])),
                    None,
                    Args::backward(1, None),
                    None,
                )
                .await?;

            Ok(result.edges.first().map(|e| e.node.version))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::{Args, ReadResult, Value};
    use crate::{EventFilter, RoutingKey};

    #[test]
    fn hash_ids_is_collision_free_across_boundaries() {
        // Without length prefixes these two lists concatenate identically.
        assert_ne!(
            hash_ids(vec!["ab", "c"]),
            hash_ids(vec!["a", "bc"]),
            "id-boundary shifts must produce different digests"
        );
        assert_eq!(hash_ids(vec!["a", "b"]), hash_ids(vec!["a", "b"]));
    }

    /// An executor stub for paths that must fail before any storage call.
    struct UnreachableExecutor;

    #[async_trait::async_trait]
    impl Executor for UnreachableExecutor {
        async fn write(&self, _events: Vec<Event>) -> Result<(), WriteError> {
            unreachable!()
        }
        async fn get_subscriber_cursor(&self, _key: String) -> anyhow::Result<Option<Value>> {
            unreachable!()
        }
        async fn is_subscriber_running(
            &self,
            _key: String,
            _worker_id: Ulid,
        ) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn upsert_subscriber(&self, _key: String, _worker_id: Ulid) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn acknowledge(
            &self,
            _key: String,
            _worker_id: Ulid,
            _cursor: Value,
            _lag: u64,
        ) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn read(
            &self,
            _aggregators: Option<Arc<[EventFilter]>>,
            _routing_key: Option<RoutingKey>,
            _args: Args,
            _to_micros: Option<u64>,
        ) -> anyhow::Result<ReadResult<Event>> {
            unreachable!()
        }
        async fn latest_timestamp(
            &self,
            _aggregators: Option<Arc<[EventFilter]>>,
            _routing_key: Option<RoutingKey>,
        ) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn get_snapshot(
            &self,
            _aggregate_type: String,
            _aggregate_revision: String,
            _id: String,
        ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
            unreachable!()
        }
        async fn save_snapshot(
            &self,
            _aggregate_type: String,
            _aggregate_revision: String,
            _id: String,
            _data: Vec<u8>,
            _cursor: Value,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn delete_snapshot(
            &self,
            _aggregate_type: String,
            _id: String,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    #[derive(bitcode::Encode, bitcode::Decode, Default)]
    struct AlphaOpened;
    impl Aggregate for AlphaOpened {
        fn aggregate_type() -> &'static str {
            "test/Alpha"
        }
    }
    impl AggregateEvent for AlphaOpened {
        fn event_name() -> &'static str {
            "AlphaOpened"
        }
    }

    #[derive(bitcode::Encode, bitcode::Decode, Default)]
    struct BetaOpened;
    impl Aggregate for BetaOpened {
        fn aggregate_type() -> &'static str {
            "test/Beta"
        }
    }
    impl AggregateEvent for BetaOpened {
        fn event_name() -> &'static str {
            "BetaOpened"
        }
    }

    /// Mixing events of two aggregate types in one builder must fail the
    /// commit instead of silently writing both streams under the last type.
    #[tokio::test]
    async fn commit_rejects_mixed_aggregate_types() {
        let result = create()
            .event(&AlphaOpened)
            .event(&BetaOpened)
            .commit(&UnreachableExecutor)
            .await;

        assert!(matches!(
            result,
            Err(WriteError::MixedAggregateTypes {
                expected: "test/Alpha",
                got: "test/Beta",
            })
        ));
    }

    /// An empty builder fails with `MissingData` before touching the executor.
    #[tokio::test]
    async fn commit_rejects_empty_builder() {
        let result = create().commit(&UnreachableExecutor).await;
        assert!(matches!(result, Err(WriteError::MissingData)));
    }
}
