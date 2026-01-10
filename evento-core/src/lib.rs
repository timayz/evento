//! Core types and traits for the Evento event sourcing library.
//!
//! This crate provides the foundational abstractions for building event-sourced applications
//! with Evento. It defines the core traits, types, and builders used throughout the framework.
//!
//! # Features
//!
//! - **`macro`** (default) - Procedural macros from `evento-macro`
//! - **`group`** - Multi-executor support via `EventoGroup`
//! - **`rw`** - Read-write split executor pattern via `Rw`
//! - **`sqlite`**, **`mysql`**, **`postgres`** - Database support via sqlx
//! - **`fjall`** - Embedded key-value storage with Fjall
//!
//! # Core Concepts
//!
//! ## Events
//!
//! Events are immutable facts that represent something that happened in your domain.
//! The [`Event`] struct stores serialized event data with metadata:
//!
//! ```rust,ignore
//! // Define events using the aggregator macro
//! #[evento::aggregator]
//! pub enum BankAccount {
//!     AccountOpened { owner_id: String, initial_balance: i64 },
//!     MoneyDeposited { amount: i64 },
//! }
//! ```
//!
//! ## Executor
//!
//! The [`Executor`] trait abstracts event storage and retrieval. Implementations
//! handle persisting events, querying, and managing subscriptions.
//!
//! ## Aggregator Builder
//!
//! Use [`create()`] or [`aggregator()`] to build and commit events:
//!
//! ```rust,ignore
//! use evento::metadata::Metadata;
//!
//! let id = evento::create()
//!     .event(&AccountOpened { owner_id: "user1".into(), initial_balance: 1000 })
//!     .metadata(&Metadata::default())
//!     .commit(&executor)
//!     .await?;
//! ```
//!
//! ## Projections
//!
//! Build read models by subscribing to events. See the [`projection`] module.
//!
//! ## Cursor-based Pagination
//!
//! GraphQL-style pagination for querying events. See the [`cursor`] module.
//!
//! # Modules
//!
//! - [`context`] - Type-safe request context for storing arbitrary data
//! - [`cursor`] - Cursor-based pagination types and traits
//! - [`metadata`] - Standard event metadata types
//! - [`projection`] - Projections, subscriptions, and event handlers
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::{Executor, metadata::Metadata, cursor::Args, ReadAggregator};
//!
//! // Create and persist an event
//! let id = evento::create()
//!     .event(&AccountOpened { owner_id: "user1".into(), initial_balance: 1000 })
//!     .metadata(&Metadata::default())
//!     .commit(&executor)
//!     .await?;
//!
//! // Query events with pagination
//! let events = executor.read(
//!     Some(vec![ReadAggregator::id("myapp/Account", &id)]),
//!     None,
//!     Args::forward(10, None),
//! ).await?;
//! ```

mod aggregator;
pub mod context;
pub mod cursor;
mod executor;
pub mod metadata;
pub mod projection;
pub mod subscription;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use aggregator::*;
pub use executor::*;
pub use subscription::RoutingKey;

use std::{fmt::Debug, marker::PhantomData, ops::Deref};
use ulid::Ulid;

use crate::cursor::Cursor;

/// Cursor data for event pagination.
///
/// Used internally for base64-encoded cursor values in paginated queries.
/// Contains the essential fields needed to uniquely identify an event's position.
#[derive(Debug, bitcode::Encode, bitcode::Decode)]
pub struct EventCursor {
    /// Event ID (ULID string)
    pub i: String,
    /// Event version
    pub v: u16,
    /// Event timestamp (Unix timestamp in seconds)
    pub t: u64,
    /// Sub-second precision (milliseconds)
    pub s: u32,
}

/// A stored event in the event store.
///
/// Events are immutable records of facts that occurred in your domain.
/// They contain serialized data and metadata, along with positioning
/// information for the aggregate they belong to.
///
/// # Fields
///
/// - `id` - Unique event identifier (ULID format for time-ordering)
/// - `aggregator_id` - The aggregate instance this event belongs to
/// - `aggregator_type` - Type name like `"myapp/BankAccount"`
/// - `version` - Sequence number within the aggregate (for optimistic concurrency)
/// - `name` - Event type name like `"AccountOpened"`
/// - `routing_key` - Optional key for event distribution/partitioning
/// - `data` - Serialized event payload (bitcode format)
/// - `metadata` - Serialized metadata (bitcode format)
/// - `timestamp` - When the event occurred (Unix seconds)
/// - `timestamp_subsec` - Sub-second precision (milliseconds)
///
/// # Serialization
///
/// Event data and metadata are serialized using [bitcode](https://crates.io/crates/bitcode)
/// for compact binary representation. Use [`projection::EventData`] to deserialize.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Event {
    /// Unique event identifier (ULID)
    pub id: Ulid,
    /// ID of the aggregate this event belongs to
    pub aggregator_id: String,
    /// Type name of the aggregate (e.g., "myapp/User")
    pub aggregator_type: String,
    /// Version number of the aggregate after this event
    pub version: u16,
    /// Event type name
    pub name: String,
    /// Optional routing key for event distribution
    pub routing_key: Option<String>,
    /// Serialized event data (bitcode format)
    pub data: Vec<u8>,
    /// Serialized event metadata (bitcode format)
    pub metadata: Vec<u8>,
    /// Unix timestamp when the event occurred (seconds)
    pub timestamp: u64,
    /// Sub-second precision (milliseconds)
    pub timestamp_subsec: u32,
}

impl Cursor for Event {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        EventCursor {
            i: self.id.to_string(),
            v: self.version,
            t: self.timestamp,
            s: self.timestamp_subsec,
        }
    }
}

impl cursor::Bind for Event {
    type T = Self;

    fn sort_by(data: &mut Vec<Self::T>, is_order_desc: bool) {
        if !is_order_desc {
            data.sort_by(|a, b| {
                if a.timestamp_subsec != b.timestamp_subsec {
                    return a.timestamp_subsec.cmp(&b.timestamp_subsec);
                }

                if a.timestamp != b.timestamp {
                    return a.timestamp.cmp(&b.timestamp);
                }

                if a.version != b.version {
                    return a.version.cmp(&b.version);
                }

                a.id.cmp(&b.id)
            });
        } else {
            data.sort_by(|a, b| {
                if a.timestamp_subsec != b.timestamp_subsec {
                    return b.timestamp_subsec.cmp(&a.timestamp_subsec);
                }

                if a.timestamp != b.timestamp {
                    return b.timestamp.cmp(&a.timestamp);
                }

                if a.version != b.version {
                    return b.version.cmp(&a.version);
                }

                b.id.cmp(&a.id)
            });
        }
    }

    fn retain(
        data: &mut Vec<Self::T>,
        cursor: <<Self as cursor::Bind>::T as Cursor>::T,
        is_order_desc: bool,
    ) {
        data.retain(|event| {
            if is_order_desc {
                event.timestamp < cursor.t
                    || (event.timestamp == cursor.t
                        && (event.timestamp_subsec < cursor.s
                            || (event.timestamp_subsec == cursor.s
                                && (event.version < cursor.v
                                    || (event.version == cursor.v
                                        && event.id.to_string() < cursor.i)))))
            } else {
                event.timestamp > cursor.t
                    || (event.timestamp == cursor.t
                        && (event.timestamp_subsec > cursor.s
                            || (event.timestamp_subsec == cursor.s
                                && (event.version > cursor.v
                                    || (event.version == cursor.v
                                        && event.id.to_string() > cursor.i)))))
            }
        });
    }
}

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
impl<R: sqlx::Row> sqlx::FromRow<'_, R> for Event
where
    i32: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    Vec<u8>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::Type<R::Database> + sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
{
    fn from_row(row: &R) -> Result<Self, sqlx::Error> {
        let timestamp: i64 = sqlx::Row::try_get(row, "timestamp")?;
        let timestamp_subsec: i64 = sqlx::Row::try_get(row, "timestamp_subsec")?;
        let version: i32 = sqlx::Row::try_get(row, "version")?;

        Ok(Event {
            id: Ulid::from_string(sqlx::Row::try_get(row, "id")?)
                .map_err(|err| sqlx::Error::InvalidArgument(err.to_string()))?,
            aggregator_id: sqlx::Row::try_get(row, "aggregator_id")?,
            aggregator_type: sqlx::Row::try_get(row, "aggregator_type")?,
            version: version as u16,
            name: sqlx::Row::try_get(row, "name")?,
            routing_key: sqlx::Row::try_get(row, "routing_key")?,
            data: sqlx::Row::try_get(row, "data")?,
            metadata: sqlx::Row::try_get(row, "metadata")?,
            timestamp: timestamp as u64,
            timestamp_subsec: timestamp_subsec as u32,
        })
    }
}

/// Typed event with deserialized data and metadata.
///
/// `EventData` wraps a raw [`Event`](crate::Event) and provides typed access
/// to the deserialized event data and metadata. It implements `Deref` to
/// provide access to the underlying event fields (id, timestamp, version, etc.).
///
/// # Type Parameters
///
/// - `D`: The event data type (e.g., `AccountOpened`)
/// - `M`: The metadata type (defaults to `bool` for no metadata)
///
/// # Example
///
/// ```rust,ignore
/// use evento::metadata::Event;
///
/// #[evento::handler]
/// async fn handle_deposit<E: Executor>(
///     event: Event<MoneyDeposited>,
///     action: Action<'_, AccountView, E>,
/// ) -> anyhow::Result<()> {
///     // Access typed data
///     println!("Amount: {}", event.data.amount);
///
///     // Access metadata
///     if let Ok(user) = event.metadata.user() {
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
pub struct EventData<D, M = bool> {
    event: Event,
    /// The typed event data
    pub data: D,
    /// The typed event metadata
    pub metadata: M,
}

impl<D, M> Deref for EventData<D, M> {
    type Target = Event;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<D, M> TryFrom<&Event> for EventData<D, M>
where
    D: bitcode::DecodeOwned,
    M: bitcode::DecodeOwned,
{
    type Error = bitcode::Error;

    fn try_from(value: &Event) -> Result<Self, Self::Error> {
        let data = bitcode::decode::<D>(&value.data)?;
        let metadata = bitcode::decode::<M>(&value.metadata)?;
        Ok(EventData {
            data,
            metadata,
            event: value.clone(),
        })
    }
}

pub struct SkipEventData<D>(pub Event, pub PhantomData<D>);

impl<D> Deref for SkipEventData<D> {
    type Target = Event;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
