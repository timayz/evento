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
//!
//! Storage backends (SQL via sqlx, Fjall) live in the `evento-sql` and
//! `evento-fjall` crates and are re-exported through the `evento` facade
//! crate's feature flags.
//!
//! # Core Concepts
//!
//! ## Events
//!
//! Events are immutable facts that represent something that happened in your domain.
//! The [`Event`] struct stores serialized event data with metadata:
//!
//! ```rust,ignore
//! // Define events using the aggregate macro
//! #[evento::aggregate]
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
//! ## Aggregate Builder
//!
//! Use [`create()`] or [`append()`] to build and commit events:
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
//! Build read models by replaying events. Use the [`projection`](mod@projection) module for loading
//! aggregate state:
//!
//! ```rust,ignore
//! use evento::projection::Projection;
//!
//! #[evento::projection]
//! #[derive(Debug)]
//! pub struct AccountView {
//!     pub balance: i64,
//! }
//!
//! #[evento::handler]
//! async fn on_deposited(
//!     event: Event<MoneyDeposited>,
//!     projection: &mut AccountView,
//! ) -> anyhow::Result<()> {
//!     projection.balance += event.data.amount;
//!     Ok(())
//! }
//!
//! let result = Projection::<_, AccountView>::new::<BankAccount>()
//!     .handler(on_deposited())
//!     .load("account-123")
//!     .execute(&executor)
//!     .await?;
//! ```
//!
//! ## Subscriptions
//!
//! Process events continuously in real-time. See the [`subscription`](mod@subscription) module:
//!
//! ```rust,ignore
//! use evento::subscription::SubscriptionBuilder;
//!
//! #[evento::subscription]
//! async fn on_deposited<E: Executor>(
//!     context: &Context<'_, E>,
//!     event: Event<MoneyDeposited>,
//! ) -> anyhow::Result<()> {
//!     // Perform side effects
//!     Ok(())
//! }
//!
//! let subscription = SubscriptionBuilder::<Sqlite>::new("deposit-processor")
//!     .handler(on_deposited())
//!     .routing_key("accounts")
//!     .start(&executor)
//!     .await?;
//! ```
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
//! - [`projection`](mod@projection) - Projections for loading aggregate state
//! - [`subscription`](mod@subscription) - Continuous event processing with subscriptions
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::{Executor, metadata::Metadata, cursor::Args, EventFilter};
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
//!     Some(vec![EventFilter::by_id("myapp/Account", &id)]),
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

use std::fmt::Debug;
use ulid::Ulid;

use crate::{cursor::Cursor, metadata::Metadata};

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
/// - `aggregate_id` - The aggregate instance this event belongs to
/// - `aggregate_type` - Type name like `"myapp/BankAccount"`
/// - `version` - Sequence number within the aggregate (for optimistic concurrency)
/// - `name` - Event type name like `"AccountOpened"`
/// - `routing_key` - Optional key for event distribution/partitioning
/// - `data` - Serialized event payload (bitcode format)
/// - `metadata` - Event metadata (see [`metadata::Metadata`])
/// - `timestamp` - When the event occurred (Unix seconds)
/// - `timestamp_subsec` - Sub-second precision (milliseconds)
///
/// # Serialization
///
/// Event data is serialized using [bitcode](https://crates.io/crates/bitcode)
/// for compact binary representation. Use [`metadata::Event`] to deserialize typed events.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Event {
    /// Unique event identifier (ULID)
    pub id: Ulid,
    /// ID of the aggregate this event belongs to
    pub aggregate_id: String,
    /// Type name of the aggregate (e.g., "myapp/User")
    pub aggregate_type: String,
    /// Version number of the aggregate after this event
    pub version: u16,
    /// Event type name
    pub name: String,
    /// Optional routing key for event distribution
    pub routing_key: Option<String>,
    /// Serialized event data (bitcode format)
    pub data: Vec<u8>,
    /// Event metadata
    pub metadata: Metadata,
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
                if a.timestamp != b.timestamp {
                    return a.timestamp.cmp(&b.timestamp);
                }

                if a.timestamp_subsec != b.timestamp_subsec {
                    return a.timestamp_subsec.cmp(&b.timestamp_subsec);
                }

                if a.version != b.version {
                    return a.version.cmp(&b.version);
                }

                a.id.cmp(&b.id)
            });
        } else {
            data.sort_by(|a, b| {
                if a.timestamp != b.timestamp {
                    return b.timestamp.cmp(&a.timestamp);
                }

                if a.timestamp_subsec != b.timestamp_subsec {
                    return b.timestamp_subsec.cmp(&a.timestamp_subsec);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::Bind;

    fn event_at(timestamp: u64, timestamp_subsec: u32) -> Event {
        Event {
            id: Ulid::generate(),
            timestamp,
            timestamp_subsec,
            ..Default::default()
        }
    }

    /// Cursor strings persisted by earlier releases (hand-built base64
    /// engine, URL_SAFE + PAD) must keep round-tripping through the prebuilt
    /// `URL_SAFE` engine constant — subscriber cursors live in the store.
    #[test]
    fn cursor_encoding_is_stable_across_releases() {
        use crate::cursor::{Cursor, Value};

        let event = Event {
            id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            version: 7,
            timestamp: 1_700_000_000,
            timestamp_subsec: 123,
            ..Default::default()
        };
        // Fixture produced by the pre-change encoder.
        let fixture = Value("GjAxQVJaM05ERUtUU1Y0UlJGRlE2OUc1RkFWBwACAPFTZQR7".to_string());

        assert_eq!(event.serialize_cursor().unwrap(), fixture);
        let decoded = Event::deserialize_cursor(&fixture).unwrap();
        assert_eq!(decoded.i, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(decoded.v, 7);
        assert_eq!(decoded.t, 1_700_000_000);
        assert_eq!(decoded.s, 123);
    }

    /// Events must be ordered by whole seconds first, then sub-seconds — matching
    /// the SQL `ORDER BY timestamp, timestamp_subsec, version, id`. A regression for
    /// the bug where `timestamp_subsec` was (incorrectly) the major sort key, which
    /// reordered events whose larger second carried a smaller sub-second.
    #[test]
    fn sort_orders_by_timestamp_before_subsec() {
        // (t=1000, s=500) must come before (t=1001, s=100) ascending.
        let earlier = event_at(1000, 500);
        let later = event_at(1001, 100);

        let mut asc = vec![later.clone(), earlier.clone()];
        Event::sort_by(&mut asc, false);
        assert_eq!(
            (asc[0].timestamp, asc[1].timestamp),
            (1000, 1001),
            "ascending order must place the smaller whole-second first"
        );

        let mut desc = vec![earlier, later];
        Event::sort_by(&mut desc, true);
        assert_eq!(
            (desc[0].timestamp, desc[1].timestamp),
            (1001, 1000),
            "descending order must place the larger whole-second first"
        );
    }

    /// `retain` (cursor keyset filter) must agree with `sort_by`: forward pagination
    /// from a cursor at (t=1000, s=500) keeps the strictly-later (t=1001, s=100).
    #[test]
    fn retain_agrees_with_sort_order() {
        let cursor = EventCursor {
            i: Ulid::nil().to_string(),
            v: 0,
            t: 1000,
            s: 500,
        };

        let mut forward = vec![event_at(1001, 100), event_at(1000, 400)];
        Event::retain(&mut forward, cursor, false);
        assert_eq!(forward.len(), 1);
        assert_eq!(forward[0].timestamp, 1001);
    }
}
