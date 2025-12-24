//! # Evento - Event Sourcing and CQRS Framework
//!
//! Evento is a comprehensive library for building event-sourced applications using Domain-Driven Design (DDD)
//! and Command Query Responsibility Segregation (CQRS) patterns in Rust.
//!
//! ## Overview
//!
//! Event sourcing is a pattern where state changes are stored as a sequence of events. Instead of persisting
//! just the current state, you persist all the events that led to the current state. This provides:
//!
//! - **Complete audit trail**: Every change is recorded as an event
//! - **Time travel**: Replay events to see state at any point in time  
//! - **Event-driven architecture**: React to events with handlers
//! - **CQRS support**: Separate read and write models
//!
//! ## Core Concepts
//!
//! - **Events**: Immutable facts representing something that happened
//! - **Aggregates**: Domain objects that process events and maintain state
//! - **Event Handlers**: Functions that react to events and trigger side effects
//! - **Event Store**: Persistent storage for events (SQL databases supported)
//! - **Snapshots**: Periodic state captures to optimize loading
//!
//! ## Quick Start
//!
//! ```no_run
//! use evento::{EventDetails, AggregatorName};
//! use serde::{Deserialize, Serialize};
//! use bincode::{Decode, Encode};
//!
//! // Define events
//! #[derive(AggregatorName, Encode, Decode)]
//! struct UserCreated {
//!     name: String,
//!     email: String,
//! }
//!
//! // Define aggregate
//! #[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
//! struct User {
//!     name: String,
//!     email: String,
//! }
//!
//! // Implement event handlers on the aggregate
//! #[evento::aggregator]
//! impl User {
//!     async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
//!         self.name = event.data.name;
//!         self.email = event.data.email;
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Setup SQLite executor
//!     let pool = sqlx::SqlitePool::connect("sqlite:events.db").await?;
//!     let executor: evento::Sqlite = pool.into();
//!
//!     // Create and save events
//!     let user_id = evento::create::<User>()
//!         .data(&UserCreated {
//!             name: "John Doe".to_string(),
//!             email: "john@example.com".to_string(),
//!         })?
//!         .metadata(&true)?
//!         .commit(&executor)
//!         .await?;
//!
//!     // Load aggregate from events
//!     let user = evento::load::<User, _>(&executor, &user_id).await?;
//!     println!("User: {:?}", user.item);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! - **SQL Database Support**: SQLite, PostgreSQL, MySQL
//! - **Event Handlers**: Async event processing with retries
//! - **Event Subscriptions**: Continuous event processing
//! - **Streaming**: Real-time event streams (with `stream` feature)
//! - **Migrations**: Database schema management
//! - **Macros**: Procedural macros for cleaner code
//!
//! ## Feature Flags
//!
//! - `macro` - Enable procedural macros (default)
//! - `handler` - Enable event handlers (default)  
//! - `stream` - Enable streaming support
//! - `sql` - Enable all SQL database backends
//! - `sqlite` - SQLite support
//! - `postgres` - PostgreSQL support
//! - `mysql` - MySQL support

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

/// Raw event stored in the event store
///
/// `Event` represents a single immutable event in the event stream. Events are the
/// fundamental building blocks of event sourcing - they represent facts that have
/// occurred in the domain.
///
/// Events are typically not used directly in application code. Instead, use
/// [`EventDetails`] which provides typed access to the event data.
///
/// # Fields
///
/// - `id`: Unique identifier for the event (ULID)
/// - `aggregator_id`: ID of the aggregate this event belongs to
/// - `aggregator_type`: Type name of the aggregate
/// - `version`: Version number of the aggregate after this event
/// - `name`: Event type name
/// - `routing_key`: Optional routing key for event distribution
/// - `data`: Serialized event data (bincode)
/// - `metadata`: Serialized event metadata (bincode)
/// - `timestamp`: Unix timestamp when the event occurred
///
/// # Examples
///
/// Events are usually created through the [`create`] and [`save`] functions:
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
///         .data(&UserCreated { name: "John".to_string() })?
///         .metadata(&true)?
///         .commit(executor)
///         .await?;
///     Ok(user_id)
/// }
/// ```
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
