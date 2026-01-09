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
//! - **Event Handlers** - Async event processing with automatic retries
//! - **Event Subscriptions** - Continuous event stream processing with cursor tracking
//! - **Projections** - Build read models by replaying events
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
//! ## Define Events Using an Enum
//!
//! ```rust,ignore
//! // Define events using an enum - the macro generates individual structs
//! #[evento::aggregator]
//! pub enum Account {
//!     AccountOpened {
//!         owner: String,
//!         initial_balance: i64,
//!     },
//!     MoneyDeposited {
//!         amount: i64,
//!     },
//!     MoneyWithdrawn {
//!         amount: i64,
//!     },
//! }
//! ```
//!
//! ## Create and Store Events
//!
//! ```rust,ignore
//! use evento::metadata::Metadata;
//!
//! // Setup database and run migrations
//! let pool = sqlx::SqlitePool::connect("sqlite:events.db").await?;
//! let mut conn = pool.acquire().await?;
//! evento::sql_migrator::new()?
//!     .run(&mut *conn, &evento::migrator::Plan::apply_all())
//!     .await?;
//!
//! let executor: evento::Sqlite = pool.into();
//!
//! // Create a new aggregate with an event
//! let account_id = evento::create()
//!     .event(&AccountOpened { owner: "Alice".into(), initial_balance: 1000 })
//!     .metadata(&Metadata::default())
//!     .routing_key("accounts")
//!     .commit(&executor)
//!     .await?;
//!
//! // Add more events to the aggregate
//! evento::aggregator(&account_id)
//!     .original_version(1)
//!     .event(&MoneyDeposited { amount: 100 })
//!     .metadata(&Metadata::default())
//!     .commit(&executor)
//!     .await?;
//! ```
//!
//! ## Build Projections and Handle Events
//!
//! ```rust,ignore
//! use evento::{Executor, metadata::Event, projection::{Action, Projection}};
//!
//! #[derive(Default)]
//! pub struct AccountView {
//!     pub balance: i64,
//!     pub owner: String,
//! }
//!
//! #[evento::handler]
//! async fn on_account_opened<E: Executor>(
//!     event: Event<AccountOpened>,
//!     action: Action<'_, AccountView, E>,
//! ) -> anyhow::Result<()> {
//!     match action {
//!         Action::Apply(view) => {
//!             view.owner = event.data.owner.clone();
//!             view.balance = event.data.initial_balance;
//!         }
//!         Action::Handle(_context) => {
//!             // Handle side effects here
//!         }
//!     };
//!     Ok(())
//! }
//!
//! #[evento::handler]
//! async fn on_money_deposited<E: Executor>(
//!     event: Event<MoneyDeposited>,
//!     action: Action<'_, AccountView, E>,
//! ) -> anyhow::Result<()> {
//!     if let Action::Apply(view) = action {
//!         view.balance += event.data.amount;
//!     }
//!     Ok(())
//! }
//!
//! // Load aggregate state via projection
//! let projection = Projection::<AccountView, _>::new("accounts")
//!     .handler(on_account_opened())
//!     .handler(on_money_deposited());
//!
//! let result = projection
//!     .load::<Account>(&account_id)
//!     .execute(&executor)
//!     .await?;
//! ```
//!
//! ## Run Continuous Subscriptions
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! let subscription = Projection::<AccountView, _>::new("account-processor")
//!     .handler(on_account_opened())
//!     .handler(on_money_deposited())
//!     .subscription()
//!     .routing_key("accounts")
//!     .chunk_size(100)
//!     .retry(5)
//!     .delay(Duration::from_secs(10))
//!     .start(&executor)
//!     .await?;
//!
//! // On shutdown
//! subscription.shutdown().await?;
//! ```
//!
//! # Re-exports
//!
//! This crate re-exports types from [`evento_core`] and conditionally from
//! `evento_sql` and `evento_sql_migrator` when database features are enabled.

// Re-export everything from evento-core
pub use evento_core::*;

// Re-export projection types at root level for convenience
pub use evento_core::projection::{Handler, Projection, ProjectionCursor, Snapshot};

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
#[cfg(all(
    feature = "macro",
    any(feature = "sqlite", feature = "postgres", feature = "mysql")
))]
mod sort_by;

/// MySQL executor type alias.
#[cfg(feature = "mysql")]
pub use evento_sql::MySql;

/// PostgreSQL executor type alias.
#[cfg(feature = "postgres")]
pub use evento_sql::Postgres;

/// SQLite executor type alias.
#[cfg(feature = "sqlite")]
pub use evento_sql::Sqlite;
