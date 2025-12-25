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
//! let id = evento::create()
//!     .event(&AccountOpened { owner_id: "user1".into(), initial_balance: 1000 })?
//!     .metadata(&metadata)?
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
//! use evento::{create, Executor};
//!
//! // Create and persist an event
//! let id = create()
//!     .event(&MyEvent { ... })?
//!     .commit(&executor)
//!     .await?;
//!
//! // Query events with pagination
//! let events = executor.read(
//!     Some(vec![ReadAggregator::id("myapp/User", &user_id)]),
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

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use aggregator::*;
pub use executor::*;
pub use projection::RoutingKey;

use std::fmt::Debug;
use ulid::Ulid;

use crate::cursor::Cursor;

/// Cursor data for event pagination.
///
/// Used internally for base64-encoded cursor values in paginated queries.
/// Contains the essential fields needed to uniquely identify an event's position.
#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
/// - `data` - Serialized event payload (rkyv format)
/// - `metadata` - Serialized metadata (rkyv format)
/// - `timestamp` - When the event occurred (Unix seconds)
/// - `timestamp_subsec` - Sub-second precision (milliseconds)
///
/// # Serialization
///
/// Event data and metadata are serialized using [rkyv](https://rkyv.org/) for
/// zero-copy deserialization. Use [`projection::EventData`] to deserialize.
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
    /// Serialized event data (rkyv format)
    pub data: Vec<u8>,
    /// Serialized event metadata (rkyv format)
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

    fn serialize_cursor(&self) -> Result<cursor::Value, cursor::CursorError> {
        use base64::{alphabet, engine::general_purpose, engine::GeneralPurpose, Engine};

        let cursor = self.serialize();
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&cursor)
            .map_err(|e| cursor::CursorError::Rkyv(e.to_string()))?;

        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        Ok(cursor::Value(engine.encode(&encoded)))
    }

    fn deserialize_cursor(value: &cursor::Value) -> Result<Self::T, cursor::CursorError> {
        use base64::{alphabet, engine::general_purpose, engine::GeneralPurpose, Engine};

        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(value)?;

        let result = rkyv::from_bytes::<Self::T, rkyv::rancor::Error>(&decoded)
            .map_err(|e| cursor::CursorError::Rkyv(e.to_string()))?;

        Ok(result)
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
