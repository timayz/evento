//! Procedural macros for the Evento event sourcing framework.
//!
//! This crate provides macros that eliminate boilerplate when building event-sourced
//! applications with Evento. It generates trait implementations, handler structs,
//! and serialization code automatically.
//!
//! # Macros
//!
//! | Macro | Type | Purpose |
//! |-------|------|---------|
//! | [`aggregator`] | Attribute | Transform enum into event structs with trait impls |
//! | [`handler`] | Attribute | Create event handler from async function |
//! | [`snapshot`] | Attribute | Implement snapshot restoration for projections |
//! | [`debug_handler`] | Attribute | Like `handler` but outputs generated code for debugging |
//! | [`debug_snapshot`] | Attribute | Like `snapshot` but outputs generated code for debugging |
//!
//! # Usage
//!
//! This crate is typically used through the main `evento` crate with the `macro` feature
//! enabled (on by default):
//!
//! ```toml
//! [dependencies]
//! evento = "1.8"
//! ```
//!
//! # Examples
//!
//! ## Defining Events with `#[evento::aggregator]`
//!
//! Transform an enum into individual event structs:
//!
//! ```rust,ignore
//! #[evento::aggregator]
//! pub enum BankAccount {
//!     /// Event raised when a new bank account is opened
//!     AccountOpened {
//!         owner_id: String,
//!         owner_name: String,
//!         initial_balance: i64,
//!     },
//!
//!     MoneyDeposited {
//!         amount: i64,
//!         transaction_id: String,
//!     },
//!
//!     MoneyWithdrawn {
//!         amount: i64,
//!         transaction_id: String,
//!     },
//! }
//! ```
//!
//! This generates:
//! - `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn` structs
//! - `Aggregator` and `Event` trait implementations for each
//! - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization
//!
//! ## Creating Handlers with `#[evento::handler]`
//!
//! ```rust,ignore
//! #[evento::handler]
//! async fn handle_money_deposited<E: Executor>(
//!     event: Event<MoneyDeposited>,
//!     action: Action<'_, AccountBalanceView, E>,
//! ) -> anyhow::Result<()> {
//!     match action {
//!         Action::Apply(row) => {
//!             row.balance += event.data.amount;
//!         }
//!         Action::Handle(_context) => {}
//!     };
//!     Ok(())
//! }
//!
//! // Use in a projection
//! let projection = Projection::new("account-balance")
//!     .handler(handle_money_deposited());
//! ```
//!
//! ## Snapshot Restoration with `#[evento::snapshot]`
//!
//! ```rust,ignore
//! #[evento::snapshot]
//! async fn restore(
//!     context: &evento::context::RwContext,
//!     id: String,
//! ) -> anyhow::Result<Option<LoadResult<AccountBalanceView>>> {
//!     // Load snapshot from database or return None
//!     Ok(None)
//! }
//! ```
//!
//! # Requirements
//!
//! When using these macros, your types must meet certain requirements:
//!
//! - **Events** (from `#[aggregator]`): Automatically derive required traits
//! - **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
//! - **Handler functions**: Must be `async` and return `anyhow::Result<()>`
//!
//! # Serialization
//!
//! Events are serialized using [bitcode](https://crates.io/crates/bitcode) for compact
//! binary representation. The `#[aggregator]` macro automatically adds the required
//! bitcode derives.

mod aggregator;
mod command;
mod handler;
mod snapshot;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

/// Transforms an enum into individual event structs with trait implementations.
///
/// This macro takes an enum where each variant represents an event type and generates:
/// - Individual public structs for each variant
/// - `Aggregator` trait implementation (provides `aggregator_type()`)
/// - `Event` trait implementation (provides `event_name()`)
/// - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization
///
/// # Aggregator Type Format
///
/// The aggregator type is formatted as `"{package_name}/{enum_name}"`, e.g., `"bank/BankAccount"`.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator]
/// pub enum BankAccount {
///     /// Event raised when account is opened
///     AccountOpened {
///         owner_id: String,
///         owner_name: String,
///         initial_balance: i64,
///     },
///
///     MoneyDeposited {
///         amount: i64,
///         transaction_id: String,
///     },
/// }
///
/// // Generated structs can be used directly:
/// let event = AccountOpened {
///     owner_id: "user123".into(),
///     owner_name: "John".into(),
///     initial_balance: 1000,
/// };
/// ```
///
/// # Additional Derives
///
/// Pass additional derives as arguments:
///
/// ```rust,ignore
/// #[evento::aggregator(serde::Serialize, serde::Deserialize)]
/// pub enum MyEvents {
///     // variants...
/// }
/// ```
///
/// # Variant Types
///
/// Supports all enum variant types:
/// - Named fields: `Variant { field: Type }`
/// - Tuple fields: `Variant(Type1, Type2)`
/// - Unit variants: `Variant`
#[proc_macro_attribute]
pub fn aggregator(attr: TokenStream, item: TokenStream) -> TokenStream {
    aggregator::aggregator(attr, item)
}

/// Creates an event handler from an async function.
///
/// This macro transforms an async function into a handler struct that implements
/// the `Handler<P, E>` trait for use with projections.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn handler_name<E: Executor>(
///     event: Event<EventType>,
///     action: Action<'_, ProjectionType, E>,
/// ) -> anyhow::Result<()>
/// ```
///
/// # Generated Code
///
/// For a function `handle_money_deposited`, the macro generates:
/// - `HandleMoneyDepositedHandler` struct
/// - `handle_money_deposited()` constructor function
/// - `Handler<ProjectionType, E>` trait implementation
///
/// # Example
///
/// ```rust,ignore
/// #[evento::handler]
/// async fn handle_money_deposited<E: Executor>(
///     event: Event<MoneyDeposited>,
///     action: Action<'_, AccountBalanceView, E>,
/// ) -> anyhow::Result<()> {
///     match action {
///         Action::Apply(row) => {
///             row.balance += event.data.amount;
///         }
///         Action::Handle(_context) => {
///             // Side effects, notifications, etc.
///         }
///     };
///     Ok(())
/// }
///
/// // Register with projection
/// let projection = Projection::new("account-balance")
///     .handler(handle_money_deposited());
/// ```
///
/// # Action Variants
///
/// - `Action::Apply(row)` - Mutate projection state (for rebuilding from events)
/// - `Action::Handle(context)` - Handle side effects during live processing
#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match handler::handler_next_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Debug variant of [`handler`] that writes generated code to a file.
///
/// The generated code is written to `target/evento_debug_handler_macro.rs`
/// for inspection. Useful for understanding what the macro produces.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::debug_handler]
/// async fn handle_event<E: Executor>(
///     event: Event<MyEvent>,
///     action: Action<'_, MyView, E>,
/// ) -> anyhow::Result<()> {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn debug_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match handler::handler_next_impl(&input, true) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Implements the `Snapshot` trait for projection state restoration.
///
/// This macro takes an async function that restores a projection from a snapshot
/// and generates the `Snapshot` trait implementation.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<ProjectionType>>>
/// ```
///
/// # Return Value
///
/// - `Ok(Some(LoadResult { ... }))` - Snapshot found, restore from this state
/// - `Ok(None)` - No snapshot, rebuild from events
/// - `Err(...)` - Error during restoration
///
/// # Example
///
/// ```rust,ignore
/// #[evento::snapshot]
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<AccountBalanceView>>> {
///     // Query snapshot from database
///     let snapshot = context.read()
///         .query_snapshot::<AccountBalanceView>(&id)
///         .await?;
///
///     Ok(snapshot)
/// }
/// ```
///
/// # Generated Code
///
/// The macro generates a `Snapshot` trait implementation for the projection type
/// extracted from the return type's `LoadResult<T>`.
#[proc_macro_attribute]
pub fn snapshot(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match snapshot::snapshot_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Debug variant of [`snapshot`] that writes generated code to a file.
///
/// The generated code is written to `target/evento_debug_snapshot_macro.rs`
/// for inspection. Useful for understanding what the macro produces.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::debug_snapshot]
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<MyView>>> {
///     Ok(None)
/// }
/// ```
#[proc_macro_attribute]
pub fn debug_snapshot(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match snapshot::snapshot_impl(&input, true) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// The main macro that transforms a struct into the Data + wrapper pattern
///
/// # Usage
/// ```rust
/// #[evento::my_macro]
/// pub struct Command {
///     pub id: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn command(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    match command::command_impl(input, false) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}

/// The main macro that transforms a struct into the Data + wrapper pattern
///
/// # Usage
/// ```rust
/// #[evento::my_macro]
/// pub struct Command {
///     pub id: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn debug_command(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    match command::command_impl(input, true) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}
