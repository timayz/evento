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
//! | [`handler`] | Attribute | Create projection handler from async function |
//! | [`subscription`] | Attribute | Create subscription handler for specific events |
//! | [`subscription_all`] | Attribute | Create subscription handler for all events of an aggregate |
//! | [`projection`] | Attribute | Add cursor field and implement `ProjectionCursor` |
//! | [`Cursor`] | Derive | Generate cursor struct and trait implementations |
//! | [`debug_handler`] | Attribute | Like `handler` but outputs generated code for debugging |
//!
//! # Usage
//!
//! This crate is typically used through the main `evento` crate with the `macro` feature
//! enabled (on by default):
//!
//! ```toml
//! [dependencies]
//! evento = "2"
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
//! ## Creating Projection Handlers with `#[evento::handler]`
//!
//! Projection handlers are used to build read models by replaying events:
//!
//! ```rust,ignore
//! use evento::metadata::Event;
//!
//! #[evento::handler]
//! async fn handle_money_deposited(
//!     event: Event<MoneyDeposited>,
//!     projection: &mut AccountBalanceView,
//! ) -> anyhow::Result<()> {
//!     projection.balance += event.data.amount;
//!     Ok(())
//! }
//!
//! // Use in a projection
//! let projection = Projection::<AccountBalanceView, _>::new::<BankAccount>("account-123")
//!     .handler(handle_money_deposited());
//! ```
//!
//! ## Creating Subscription Handlers with `#[evento::subscription]`
//!
//! Subscription handlers process events in real-time with side effects:
//!
//! ```rust,ignore
//! use evento::{Executor, metadata::Event, subscription::Context};
//!
//! #[evento::subscription]
//! async fn on_money_deposited<E: Executor>(
//!     context: &Context<'_, E>,
//!     event: Event<MoneyDeposited>,
//! ) -> anyhow::Result<()> {
//!     // Perform side effects: send notifications, update read models, etc.
//!     println!("Deposited: {}", event.data.amount);
//!     Ok(())
//! }
//!
//! // Use in a subscription
//! let subscription = SubscriptionBuilder::<Sqlite>::new("deposit-notifier")
//!     .handler(on_money_deposited())
//!     .routing_key("accounts")
//!     .start(&executor)
//!     .await?;
//! ```
//!
//! ## Handling All Events with `#[evento::subscription_all]`
//!
//! Handle all events from an aggregate type without deserializing:
//!
//! ```rust,ignore
//! use evento::{Executor, metadata::RawEvent, subscription::Context};
//!
//! #[evento::subscription_all]
//! async fn on_any_account_event<E: Executor>(
//!     context: &Context<'_, E>,
//!     event: RawEvent<BankAccount>,
//! ) -> anyhow::Result<()> {
//!     println!("Event {} on account {}", event.name, event.aggregator_id);
//!     Ok(())
//! }
//! ```
//!
//! ## Projection State with `#[evento::projection]`
//!
//! Automatically add cursor tracking to projection structs:
//!
//! ```rust,ignore
//! #[evento::projection]
//! #[derive(Debug)]
//! pub struct AccountBalanceView {
//!     pub balance: i64,
//!     pub owner: String,
//! }
//!
//! // Generates:
//! // - Adds `pub cursor: String` field
//! // - Implements `ProjectionCursor` trait
//! // - Adds `Default` and `Clone` derives
//! ```
//!
//! # Requirements
//!
//! When using these macros, your types must meet certain requirements:
//!
//! - **Events** (from `#[aggregator]`): Automatically derive required traits
//! - **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
//! - **Projection handlers**: Must be `async` and return `anyhow::Result<()>`
//! - **Subscription handlers**: Must be `async`, take `Context` first, and return `anyhow::Result<()>`
//!
//! # Serialization
//!
//! Events are serialized using [bitcode](https://crates.io/crates/bitcode) for compact
//! binary representation. The `#[aggregator]` macro automatically adds the required
//! bitcode derives.

mod aggregator;
mod cursor;
mod handler;
mod projection;
mod subscription;
mod subscription_all;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

/// Transforms an enum into individual event structs with trait implementations.
///
/// This macro takes an enum where each variant represents an event type and generates:
/// - Individual public structs for each variant
/// - `Aggregator` trait implementation (provides `aggregator_type()`)
/// - `AggregatorEvent` trait implementation (provides `event_name()`)
/// - A unit struct with the enum name implementing `Aggregator`
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

/// Creates a projection handler from an async function.
///
/// This macro transforms an async function into a handler struct that implements
/// the `projection::Handler<P>` trait for use with projections to build read models.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn handler_name(
///     event: Event<EventType>,
///     projection: &mut ProjectionType,
/// ) -> anyhow::Result<()>
/// ```
///
/// # Generated Code
///
/// For a function `handle_money_deposited`, the macro generates:
/// - `HandleMoneyDepositedHandler` struct
/// - `handle_money_deposited()` constructor function
/// - `projection::Handler<ProjectionType>` trait implementation
///
/// # Example
///
/// ```rust,ignore
/// use evento::metadata::Event;
///
/// #[evento::handler]
/// async fn handle_money_deposited(
///     event: Event<MoneyDeposited>,
///     projection: &mut AccountBalanceView,
/// ) -> anyhow::Result<()> {
///     projection.balance += event.data.amount;
///     Ok(())
/// }
///
/// // Register with projection
/// let projection = Projection::<AccountBalanceView, _>::new::<BankAccount>("account-123")
///     .handler(handle_money_deposited());
///
/// // Execute projection to get current state
/// let result = projection.execute(&executor).await?;
/// ```
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

/// Creates a subscription handler for specific events.
///
/// This macro transforms an async function into a handler struct that implements
/// the `subscription::Handler<E>` trait for processing events in real-time subscriptions.
///
/// Unlike projection handlers, subscription handlers receive a context with access
/// to the executor and can perform side effects like database updates, notifications,
/// or external API calls.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn handler_name<E: Executor>(
///     context: &Context<'_, E>,
///     event: Event<EventType>,
/// ) -> anyhow::Result<()>
/// ```
///
/// # Generated Code
///
/// For a function `on_money_deposited`, the macro generates:
/// - `OnMoneyDepositedHandler` struct
/// - `on_money_deposited()` constructor function
/// - `subscription::Handler<E>` trait implementation
///
/// # Example
///
/// ```rust,ignore
/// use evento::{Executor, metadata::Event, subscription::Context};
///
/// #[evento::subscription]
/// async fn on_money_deposited<E: Executor>(
///     context: &Context<'_, E>,
///     event: Event<MoneyDeposited>,
/// ) -> anyhow::Result<()> {
///     // Access shared data from context
///     let config: Data<AppConfig> = context.extract();
///
///     // Perform side effects
///     send_notification(&event.data).await?;
///
///     Ok(())
/// }
///
/// // Register with subscription
/// let subscription = SubscriptionBuilder::<Sqlite>::new("deposit-notifier")
///     .handler(on_money_deposited())
///     .routing_key("accounts")
///     .start(&executor)
///     .await?;
/// ```
#[proc_macro_attribute]
pub fn subscription(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match subscription::subscription_next_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Creates a subscription handler that processes all events of an aggregate type.
///
/// This macro is similar to [`subscription`] but handles all events from an aggregate
/// without requiring the event data to be deserialized. The event is wrapped in
/// [`RawEvent`](evento_core::metadata::RawEvent) which provides access to event metadata
/// (name, id, timestamp, etc.) without deserializing the payload.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn handler_name<E: Executor>(
///     context: &Context<'_, E>,
///     event: RawEvent<AggregateType>,
/// ) -> anyhow::Result<()>
/// ```
///
/// # Generated Code
///
/// For a function `on_any_account_event`, the macro generates:
/// - `OnAnyAccountEventHandler` struct
/// - `on_any_account_event()` constructor function
/// - `subscription::Handler<E>` trait implementation with `event_name()` returning `"all"`
///
/// # Example
///
/// ```rust,ignore
/// use evento::{Executor, metadata::RawEvent, subscription::Context};
///
/// #[evento::subscription_all]
/// async fn on_any_account_event<E: Executor>(
///     context: &Context<'_, E>,
///     event: RawEvent<BankAccount>,
/// ) -> anyhow::Result<()> {
///     // Access event metadata without deserializing
///     println!("Event: {} on {}", event.name, event.aggregator_id);
///     println!("Version: {}", event.version);
///     println!("Timestamp: {}", event.timestamp);
///
///     // Useful for logging, auditing, or forwarding events
///     Ok(())
/// }
///
/// // Register with subscription - handles all BankAccount events
/// let subscription = SubscriptionBuilder::<Sqlite>::new("account-auditor")
///     .handler(on_any_account_event())
///     .start(&executor)
///     .await?;
/// ```
#[proc_macro_attribute]
pub fn subscription_all(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match subscription_all::subscription_all_next_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro for generating cursor structs and trait implementations.
///
/// # Example
///
/// ```ignore
/// #[derive(Cursor)]
/// pub struct AdminView {
///     #[cursor(ContactAdmin::Id, 1)]
///     pub id: String,
///     #[cursor(ContactAdmin::CreatedAt, 2)]
///     pub created_at: u64,
/// }
/// ```
///
/// This generates:
/// - `AdminViewCursor` struct with shortened field names
/// - `impl evento::cursor::Cursor for AdminView`
/// - `impl evento::sql::Bind for AdminView`
#[proc_macro_derive(Cursor, attributes(cursor))]
pub fn derive_cursor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match cursor::cursor_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Adds a `cursor: String` field and implements `ProjectionCursor`.
///
/// This attribute macro transforms a struct to track its position in the event stream.
/// It automatically adds a `cursor` field and implements the `ProjectionCursor` trait.
///
/// # Generated Code
///
/// - Adds `pub cursor: String` field
/// - Adds `Default` and `Clone` derives (preserves existing derives)
/// - Implements `ProjectionCursor` trait
///
/// # Additional Derives
///
/// Pass additional derives as arguments:
///
/// ```ignore
/// #[evento::projection(serde::Serialize)]
/// pub struct MyView { ... }
/// ```
///
/// # Example
///
/// ```ignore
/// #[evento::projection]
/// #[derive(Debug)]
/// pub struct MyStruct {
///     pub id: String,
///     pub name: String,
/// }
///
/// // Generates:
/// // #[derive(Default, Clone, Debug)]
/// // pub struct MyStruct {
/// //     pub id: String,
/// //     pub name: String,
/// //     pub cursor: String,
/// // }
/// //
/// // impl evento::ProjectionCursor for MyStruct { ... }
/// ```
#[proc_macro_attribute]
pub fn projection(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match projection::projection_cursor_impl(attr, &input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}
