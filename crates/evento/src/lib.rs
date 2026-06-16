//! Event sourcing and CQRS toolkit with SQL persistence, projections, and subscriptions.
//!
//! Evento provides a complete toolkit for implementing event sourcing patterns in Rust,
//! with support for SQLite, PostgreSQL, MySQL databases, and embedded storage via Fjall.
//!
//! # Features
//!
//! - **Event Sourcing** - Store state changes as immutable events with complete audit trail
//! - **CQRS Pattern** - Separate read and write models for scalable architectures
//! - **SQL Database Support** - Built-in support for SQLite, PostgreSQL, and MySQL
//! - **Embedded Storage** - Fjall key-value store for embedded applications
//! - **Projections** - Build read models by replaying events
//! - **Subscriptions** - Continuous event stream processing with cursor tracking
//! - **Database Migrations** - Automated schema management
//! - **Compact Serialization** - Fast binary serialization with bitcode
//!
//! # Feature Flags
//!
//! - `macro` (default) - Procedural macros for cleaner code
//! - `group` - Multi-executor support via `EventoGroup`
//! - `rw` - Read-write split executor pattern
//! - `sql` - Enable all SQL database backends
//! - `sqlite` - SQLite support via sqlx
//! - `mysql` - MySQL support via sqlx
//! - `postgres` - PostgreSQL support via sqlx
//! - `fjall` - Embedded key-value storage with Fjall
//!
//! # Quick Start
//!
//! The example below is generic over any [`Executor`], so it type-checks the whole
//! surface — macros, write path, projection, and subscriptions — without a database.
//! See `examples/bank` for a runnable backend setup (SQLite/Fjall + migrations).
//!
//! ```rust,no_run
//! use evento::{
//!     metadata::{Event, RawEvent},
//!     projection::Projection,
//!     subscription::{Context, SubscriptionBuilder},
//!     Executor,
//! };
//!
//! // Define events with an enum; each variant becomes an event struct.
//! #[evento::aggregate]
//! pub enum Account {
//!     AccountOpened { owner: String, initial_balance: i64 },
//!     MoneyDeposited { amount: i64 },
//! }
//!
//! // A read model. Add bitcode derives so it can be snapshotted.
//! #[evento::projection(bitcode::Encode, bitcode::Decode)]
//! pub struct AccountView {
//!     pub owner: String,
//!     pub balance: i64,
//! }
//!
//! // Projection handlers are pure: (event, &mut view).
//! #[evento::handler]
//! async fn on_opened(event: Event<AccountOpened>, view: &mut AccountView) -> anyhow::Result<()> {
//!     view.owner = event.data.owner.clone();
//!     view.balance = event.data.initial_balance;
//!     Ok(())
//! }
//! #[evento::handler]
//! async fn on_deposited(event: Event<MoneyDeposited>, view: &mut AccountView) -> anyhow::Result<()> {
//!     view.balance += event.data.amount;
//!     Ok(())
//! }
//!
//! // Subscription handlers take the context first and may do side effects.
//! #[evento::subscription]
//! async fn notify<E: Executor>(_ctx: &Context<'_, E>, event: Event<MoneyDeposited>) -> anyhow::Result<()> {
//!     println!("deposited {}", event.data.amount);
//!     Ok(())
//! }
//!
//! // `subscription_all` sees every event without deserializing the payload.
//! #[evento::subscription_all]
//! async fn audit<E: Executor>(_ctx: &Context<'_, E>, event: RawEvent<Account>) -> anyhow::Result<()> {
//!     println!("event {} on {}", event.name, event.aggregate_id);
//!     Ok(())
//! }
//!
//! async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
//!     // Start a new aggregate, then append to it (with optimistic concurrency).
//!     let id = evento::create()
//!         .event(&AccountOpened { owner: "Alice".into(), initial_balance: 1000 })
//!         .routing_key("accounts")
//!         .commit(executor)
//!         .await?;
//!     evento::append(&id)
//!         .original_version(1)
//!         .event(&MoneyDeposited { amount: 100 })
//!         .commit(executor)
//!         .await?;
//!
//!     // Load the read model by replaying events.
//!     let _view: Option<AccountView> = Projection::<_, AccountView>::new::<Account>()
//!         .handler(on_opened())
//!         .handler(on_deposited())
//!         .load(&id)
//!         .execute(executor)
//!         .await?;
//!
//!     // Run a background subscription, then shut it down.
//!     let subscription = SubscriptionBuilder::new("deposit-notifier")
//!         .handler(notify())
//!         .routing_key("accounts")
//!         .start(executor)
//!         .await?;
//!     subscription.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Re-exports
//!
//! This crate re-exports types from [`evento_core`] and conditionally from
//! `evento_sql` and `evento_sql_migrator` when database features are enabled.

// Re-export everything from evento-core
pub use evento_core::*;

/// Projection types re-exported at root level for convenience.
///
/// These are the most commonly used types for building read models:
/// - [`Handler`] - Trait for projection event handlers
/// - [`Projection`] - Builder for loading aggregate state
/// - [`ProjectionAggregate`] - Trait for projections that emit events
/// - [`ProjectionCursor`] - Trait for cursor position tracking
/// - [`Snapshot`] - Trait for snapshot restoration
pub use evento_core::projection::{
    Handler, LoadBuilder, Projection, ProjectionAggregate, ProjectionCursor,
    ProjectionSubscription, Snapshot,
};

// Re-export SQL types when SQL features are enabled
/// SQL executor and types (requires `sqlite`, `mysql`, or `postgres` feature).
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql as sql;

/// SQL type wrappers for bitcode serialization.
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql::sql_types;

/// Database migration utilities for evento schema.
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql_migrator as sql_migrator;

/// Database migration plan and execution utilities.
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
pub mod migrator {
    pub use sqlx_migrator::{Migrate, Plan};
}

/// MySQL executor type alias.
#[cfg(feature = "mysql")]
pub use evento_sql::MySql;

/// PostgreSQL executor type alias.
#[cfg(feature = "postgres")]
pub use evento_sql::Postgres;

/// SQLite executor type alias.
#[cfg(feature = "sqlite")]
pub use evento_sql::Sqlite;

/// Fjall executor and types (requires `fjall` feature).
#[cfg(feature = "fjall")]
pub use evento_fjall as fjall;

/// Fjall executor type alias.
#[cfg(feature = "fjall")]
pub use evento_fjall::Fjall;
