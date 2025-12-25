mod aggregator;
pub mod context;
pub mod cursor;
mod executor;
pub mod metadata;
pub mod projection;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql_migrator;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql_types;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use aggregator::*;
pub use executor::*;
pub use projection::*;

use std::fmt::Debug;
use ulid::Ulid;

use crate::cursor::Cursor;

/// Stream utilities for working with event streams
///
/// This module provides stream processing capabilities when the `stream` feature is enabled.
///
/// ```no_run
/// use evento::stream::StreamExt;
/// ```
#[cfg(feature = "stream")]
pub mod stream {
    pub use tokio_stream::StreamExt;
}

/// Database migration utilities
///
/// This module provides migration support for SQL databases. Migrations are automatically
/// included when using any SQL database feature (sqlite, postgres, mysql).
///
/// ```no_run
/// use evento::migrator::{Migrate, Plan};
/// ```
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
pub mod migrator {
    pub use sqlx_migrator::{Migrate, Plan};
}

#[cfg(feature = "mysql")]
pub use sql::MySql;

#[cfg(feature = "postgres")]
pub use sql::Postgres;

#[cfg(feature = "sqlite")]
pub use sql::Sqlite;

/// Cursor for event pagination and positioning
///
/// `EventCursor` represents a position in the event stream. It contains the event ID,
/// version, and timestamp to enable efficient pagination and resuming from specific points.
///
/// This is primarily used internally for event stream pagination and subscription tracking.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct EventCursor {
    /// Event ID (ULID string)
    pub i: String,
    /// Event version
    pub v: i32,
    /// Event timestamp (Unix timestamp in seconds)
    pub t: u64,
    pub s: u32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Event {
    /// Unique event identifier (ULID)
    pub id: Ulid,
    /// ID of the aggregate this event belongs to
    pub aggregator_id: String,
    /// Type name of the aggregate (e.g., "myapp/User")
    pub aggregator_type: String,
    /// Version number of the aggregate after this event
    pub version: i32,
    /// Event type name
    pub name: String,
    /// Optional routing key for event distribution
    pub routing_key: Option<String>,
    /// Serialized event data (bincode format)
    pub data: Vec<u8>,
    /// Serialized event metadata (bincode format)
    pub metadata: Vec<u8>,
    /// Unix timestamp when the event occurred (seconds)
    pub timestamp: u64,
    /// Unix timestamp when the event occurred (seconds)
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
