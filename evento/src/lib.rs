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

pub mod context;
pub mod cursor;
mod executor;
mod load;
mod save;
mod subscribe;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql_migrator;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use executor::*;
pub use load::*;
pub use save::*;
pub use subscribe::*;

use std::{fmt::Debug, ops::Deref};
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

/// Event with typed data and metadata
///
/// `EventDetails` wraps a raw [`Event`] with typed data and metadata. This provides
/// type-safe access to event payloads in event handlers and aggregators.
///
/// # Type Parameters
///
/// - `D`: The type of the event data (must implement [`AggregatorName`] and be decodable)
/// - `M`: The type of the metadata (defaults to `bool`, must be decodable)
///
/// # Examples
///
/// ```no_run
/// use evento::{EventDetails, AggregatorName};
/// use bincode::{Encode, Decode};
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserCreated {
///     name: String,
///     email: String,
/// }
///
/// // In an event handler
/// async fn handle_user_created(event: EventDetails<UserCreated>) -> anyhow::Result<()> {
///     println!("User created: {} ({})", event.data.name, event.data.email);
///     println!("Event ID: {}", event.id);
///     Ok(())
/// }
/// ```
pub struct EventDetails<D, M = bool> {
    inner: Event,
    /// The typed event data
    pub data: D,
    /// The typed event metadata
    pub metadata: M,
}

impl<D, M> Deref for EventDetails<D, M> {
    type Target = Event;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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
#[derive(Debug, Clone, PartialEq)]
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

impl Event {
    /// Convert this raw event to typed [`EventDetails`]
    ///
    /// This method attempts to deserialize the event data and metadata into the specified types.
    /// Returns `None` if the event name doesn't match the expected type `D`.
    ///
    /// # Type Parameters
    ///
    /// - `D`: The expected event data type (must implement [`AggregatorName`])
    /// - `M`: The expected metadata type
    ///
    /// # Errors
    ///
    /// Returns a [`bincode::error::DecodeError`] if deserialization fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use evento::{Event, EventDetails};
    /// # use evento::*;
    /// # use bincode::{Encode, Decode};
    /// # #[derive(AggregatorName, Encode, Decode)]
    /// # struct UserCreated { name: String }
    ///
    /// fn handle_event(event: &Event) -> anyhow::Result<()> {
    ///     if let Some(details) = event.to_details::<UserCreated, bool>()? {
    ///         println!("User created: {}", details.data.name);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn to_details<D: AggregatorName + bincode::Decode<()>, M: bincode::Decode<()>>(
        &self,
    ) -> Result<Option<EventDetails<D, M>>, bincode::error::DecodeError> {
        if D::name() != self.name {
            return Ok(None);
        }

        let config = bincode::config::standard();

        let (data, _) = bincode::decode_from_slice(&self.data[..], config)?;
        let (metadata, _) = bincode::decode_from_slice(&self.metadata[..], config)?;

        Ok(Some(EventDetails {
            data,
            metadata,
            inner: self.clone(),
        }))
    }
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

/// Trait for domain aggregates that process events
///
/// `Aggregator` defines the contract for objects that maintain state by processing events.
/// Aggregates are the core building blocks in event sourcing - they represent domain entities
/// that rebuild their state by replaying events from the event store.
///
/// # Implementation
///
/// Instead of implementing this trait manually, use the `#[evento::aggregator]` attribute macro
/// which generates the implementation automatically based on your event handler methods.
///
/// # Requirements
///
/// Aggregators must:
/// - Implement `Default` (initial empty state)
/// - Be `Send + Sync` for async processing
/// - Be serializable with `bincode::Encode + bincode::Decode`
/// - Be `Clone`able for snapshots
/// - Implement [`AggregatorName`] for type identification
/// - Be `Debug`gable for diagnostics
///
/// # Examples
///
/// ```no_run
/// use evento::{Aggregator, AggregatorName, EventDetails};
/// use serde::{Deserialize, Serialize};
/// use bincode::{Encode, Decode};
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserCreated {
///     name: String,
///     email: String,
/// }
///
/// #[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
/// struct User {
///     name: String,
///     email: String,
///     is_active: bool,
/// }
///
/// #[evento::aggregator]
/// impl User {
///     async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
///         self.name = event.data.name;
///         self.email = event.data.email;
///         self.is_active = true;
///         Ok(())
///     }
/// }
/// ```
pub trait Aggregator:
    Default + Send + Sync + bincode::Encode + bincode::Decode<()> + Clone + AggregatorName + Debug
{
    /// Process an event and update the aggregate's state
    ///
    /// This method is called for each event when rebuilding the aggregate from the event store.
    /// The implementation should update the aggregate's state based on the event.
    ///
    /// Typically, this method is generated automatically by the `#[evento::aggregator]` macro
    /// and dispatches to specific handler methods based on the event type.
    fn aggregate<'async_trait>(
        &'async_trait mut self,
        event: &'async_trait Event,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait;

    /// Get the revision hash for this aggregate implementation
    ///
    /// The revision changes when the aggregate's event handling logic changes.
    /// This is used for versioning and ensuring compatibility.
    fn revision() -> &'static str;
}

/// Trait for types that have a name identifier
///
/// `AggregatorName` provides a way to get the name of a type at runtime.
/// This is used to identify event types and aggregate types in the event store.
///
/// # Implementation
///
/// For events, derive this trait using `#[derive(AggregatorName)]`.
/// For aggregates, this is automatically implemented by the `#[evento::aggregator]` macro.
///
/// # Examples
///
/// ```no_run
/// use evento::AggregatorName;
/// use bincode::{Encode, Decode};
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserCreated {
///     name: String,
/// }
///
/// assert_eq!(UserCreated::name(), "UserCreated");
/// ```
pub trait AggregatorName {
    /// Get the name of this type
    fn name() -> &'static str;
}
