//! Standard event metadata types.
//!
//! This module provides standard metadata types for events, including
//! user identification and unique metadata IDs.
//!
//! # Types
//!
//! - [`Metadata`] - Standard metadata with ID and extensible key-value storage
//! - [`Event`] - Typed event wrapper with deserialized data
//! - [`RawEvent`] - Raw event without deserialization (for batch processing)
//!
//! # Example
//!
//! ```rust,no_run
//! use evento::metadata::Metadata;
//! # use evento::{create, Executor};
//! # #[evento::aggregate]
//! # pub enum Account {
//! #     AccountOpened { owner: String },
//! # }
//!
//! // Create default metadata (anonymous)
//! let mut metadata = Metadata::default();
//!
//! // Set who is making the request
//! metadata.set_requested_by("user-123");
//!
//! // Set who the request is on behalf of (for impersonation)
//! metadata.set_requested_as("impersonated-user-789");
//!
//! # async fn run<E: Executor>(executor: &E, metadata: Metadata) -> anyhow::Result<()> {
//! // Use with event creation
//! create()
//!     .event(&AccountOpened { owner: "Alice".into() })
//!     .metadata_from(metadata)
//!     .commit(executor)
//!     .await?;
//! # Ok(())
//! # }
//!
//! # fn read(event: &evento::Event) {
//! // Access metadata from events
//! if let Ok(user_id) = event.metadata.requested_by() {
//!     println!("Requested by: {}", user_id);
//! }
//! # }
//! ```

use std::{collections::HashMap, marker::PhantomData, ops::Deref};
use thiserror::Error;
use ulid::Ulid;

const REQUESTED_BY: &str = "EVENTO_REQUESTED_BY";
const REQUESTED_AS: &str = "EVENTO_REQUESTED_AS";

/// Errors when accessing metadata fields.
#[derive(Debug, Error)]
pub enum MetadataError {
    /// No entry exists under the requested key.
    #[error("not found")]
    NotFound,

    /// The stored bytes could not be decoded as the requested type.
    #[error("decode: {0}")]
    Decode(#[from] bitcode::Error),
}

/// Standard event metadata.
///
/// Contains a unique ID and user identification. Default creates
/// anonymous metadata with an auto-generated ULID.
#[derive(Clone, PartialEq, Debug, bitcode::Encode, bitcode::Decode)]
pub struct Metadata {
    /// Unique metadata ID (ULID)
    pub id: String,
    meta: HashMap<String, Vec<u8>>,
}

impl Metadata {
    pub(crate) fn insert_enc<V: bitcode::Encode>(
        &mut self,
        key: impl Into<String>,
        value: &V,
    ) -> &mut Self {
        self.meta.insert(key.into(), bitcode::encode(value));

        self
    }

    /// Decodes the entry stored under `key`.
    pub fn try_get<D: bitcode::DecodeOwned>(&self, key: &str) -> Result<D, MetadataError> {
        let Some(value) = self.meta.get(key) else {
            return Err(MetadataError::NotFound);
        };

        Ok(bitcode::decode(value)?)
    }

    /// Sets the role or identity the initiator acted as.
    pub fn set_requested_as(&mut self, value: impl Into<String>) -> &mut Self {
        let value = value.into();
        self.insert_enc(REQUESTED_AS, &value);

        self
    }

    /// Returns the role or identity the initiator acted as.
    pub fn requested_as(&self) -> Result<String, MetadataError> {
        self.try_get(REQUESTED_AS)
    }

    /// Sets who initiated the commit (e.g. a user id).
    pub fn set_requested_by(&mut self, value: impl Into<String>) -> &mut Self {
        let value = value.into();
        self.insert_enc(REQUESTED_BY, &value);

        self
    }

    /// Returns who initiated the commit.
    pub fn requested_by(&self) -> Result<String, MetadataError> {
        self.try_get(REQUESTED_BY)
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            id: Ulid::generate().to_string(),
            meta: Default::default(),
        }
    }
}

impl Deref for Metadata {
    type Target = HashMap<String, Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl From<&Metadata> for Metadata {
    fn from(value: &Metadata) -> Self {
        value.clone()
    }
}

/// Typed event with deserialized data.
///
/// `Event` wraps a raw [`crate::Event`] and provides typed access
/// to the deserialized event data. It implements `Deref` to
/// provide access to the underlying event fields (id, timestamp, version, metadata, etc.).
///
/// # Type Parameters
///
/// - `D`: The event data type (e.g., `AccountOpened`)
///
/// # Example
///
/// ```rust,no_run
/// use evento::metadata::Event;
///
/// # #[evento::aggregate]
/// # pub enum Account {
/// #     MoneyDeposited { amount: i64 },
/// # }
/// # #[evento::projection(bitcode::Encode, bitcode::Decode)]
/// # pub struct AccountView {
/// #     pub balance: i64,
/// # }
/// #[evento::handler]
/// async fn handle_deposit(
///     event: Event<MoneyDeposited>,
///     view: &mut AccountView,
/// ) -> anyhow::Result<()> {
///     // Access typed data
///     println!("Amount: {}", event.data.amount);
///
///     // Access metadata via Deref
///     if let Ok(user) = event.metadata.requested_by() {
///         println!("By user: {}", user);
///     }
///
///     // Access underlying event fields via Deref
///     println!("Event ID: {}", event.id);
///     println!("Version: {}", event.version);
///
///     Ok(())
/// }
/// ```
pub struct Event<'a, D> {
    /// The raw event, borrowed for the duration of the handler call — a typed
    /// dispatch decodes `data` without deep-copying the event's strings and
    /// blobs. To keep an event past the handler, clone the raw event through
    /// `Deref`: `let owned: evento::Event = (*event).clone();`.
    event: &'a crate::Event,
    /// The typed event data
    pub data: D,
}

impl<D> Deref for Event<'_, D> {
    type Target = crate::Event;

    fn deref(&self) -> &Self::Target {
        self.event
    }
}

impl<'a, D> TryFrom<&'a crate::Event> for Event<'a, D>
where
    D: bitcode::DecodeOwned,
{
    type Error = bitcode::Error;

    fn try_from(value: &'a crate::Event) -> Result<Self, Self::Error> {
        let data = bitcode::decode::<D>(&value.data)?;
        Ok(Event { data, event: value })
    }
}

/// The untyped counterpart of [`Event`] handed to `#[subscription_all]`
/// handlers: the raw event borrowed for the handler call, tagged with the
/// aggregate's event enum. Clone through `Deref` to keep it past the call.
pub struct RawEvent<'a, D>(pub &'a crate::Event, pub PhantomData<D>);

impl<D> Deref for RawEvent<'_, D> {
    type Target = crate::Event;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
