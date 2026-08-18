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
//! | [`aggregate`] | Attribute | Transform enum into event structs with trait impls |
//! | [`handler`] | Attribute | Create projection handler from async function |
//! | [`subscription`] | Attribute | Create subscription handler for specific events |
//! | [`subscription_all`] | Attribute | Create subscription handler for all events of an aggregate |
//! | [`projection`] | Attribute | Add cursor fields and implement `ProjectionCursor` |
//! | [`snapshot`] | Attribute | Implement the `Snapshot` trait (`none` or `memory` mode) |
//! | [`command`] | Attribute | Generate routing-key command variants from one method body |
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
//! ## Defining Events with `#[evento::aggregate]`
//!
//! Transform an enum into individual event structs:
//!
//! ```rust
//! #[evento::aggregate]
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
//! - `Aggregate` and `Event` trait implementations for each
//! - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization
//!
//! ## Creating Projection Handlers with `#[evento::handler]`
//!
//! Projection handlers are used to build read models by replaying events:
//!
//! ```rust,no_run
//! use evento::{metadata::Event, Executor, Projection};
//! # #[evento::aggregate]
//! # pub enum BankAccount {
//! #     MoneyDeposited { amount: i64 },
//! # }
//!
//! #[evento::projection(bitcode::Encode, bitcode::Decode)]
//! pub struct AccountBalanceView {
//!     pub balance: i64,
//! }
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
//! # async fn run<E: Executor>(executor: &E) -> anyhow::Result<()> {
//! // Use in a projection
//! let view: Option<AccountBalanceView> = Projection::<_, AccountBalanceView>::new::<BankAccount>()
//!     .handler(handle_money_deposited())
//!     .load("account-123")
//!     .execute(executor)
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! ## Creating Subscription Handlers with `#[evento::subscription]`
//!
//! Subscription handlers process events in real-time with side effects:
//!
//! ```rust,no_run
//! use evento::{
//!     metadata::Event,
//!     subscription::{Context, SubscriptionBuilder},
//!     Executor,
//! };
//! # #[evento::aggregate]
//! # pub enum BankAccount {
//! #     MoneyDeposited { amount: i64 },
//! # }
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
//! # async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
//! // Use in a subscription
//! let subscription = SubscriptionBuilder::new("deposit-notifier")
//!     .handler(on_money_deposited())
//!     .routing_key("accounts")
//!     .start(executor)
//!     .await?;
//! # subscription.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! ## Handling All Events with `#[evento::subscription_all]`
//!
//! Handle all events from an aggregate type without deserializing:
//!
//! ```rust
//! use evento::{metadata::RawEvent, subscription::Context, Executor};
//! # #[evento::aggregate]
//! # pub enum BankAccount {
//! #     MoneyDeposited { amount: i64 },
//! # }
//!
//! #[evento::subscription_all]
//! async fn on_any_account_event<E: Executor>(
//!     context: &Context<'_, E>,
//!     event: RawEvent<BankAccount>,
//! ) -> anyhow::Result<()> {
//!     println!("Event {} on account {}", event.name, event.aggregate_id);
//!     Ok(())
//! }
//! ```
//!
//! ## Projection State with `#[evento::projection]`
//!
//! Automatically add cursor tracking to projection structs:
//!
//! ```rust
//! #[evento::projection]
//! #[derive(Debug)]
//! pub struct AccountBalanceView {
//!     pub balance: i64,
//!     pub owner: String,
//! }
//! ```
//!
//! This adds a `pub cursor: String` field, implements `ProjectionCursor`, and adds
//! the `Default` and `Clone` derives alongside any you wrote yourself.
//!
//! # Requirements
//!
//! When using these macros, your types must meet certain requirements:
//!
//! - **Events** (from `#[aggregate]`): Automatically derive required traits
//! - **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
//! - **Projection handlers**: Must be `async` and return `anyhow::Result<()>`
//! - **Subscription handlers**: Must be `async`, take `Context` first, and return `anyhow::Result<()>`
//!
//! # Serialization
//!
//! Events are serialized using [bitcode](https://crates.io/crates/bitcode) for compact
//! binary representation. The `#[aggregate]` macro automatically adds the required
//! bitcode derives.

mod aggregator;
mod command;
mod cursor;
mod handler;
mod projection;
mod snapshot;
mod subscription;
mod subscription_all;
mod util;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

/// Transforms an enum into individual event structs with trait implementations.
///
/// This macro takes an enum where each variant represents an event type and generates:
/// - Individual public structs for each variant
/// - `Aggregate` trait implementation (provides `aggregate_type()`)
/// - `AggregateEvent` trait implementation (provides `event_name()`)
/// - A unit struct with the enum name implementing `Aggregate`
/// - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization
///
/// # Aggregate Type Format
///
/// The aggregate type defaults to `"{package_name}/{enum_name}"`, e.g., `"bank/BankAccount"`.
/// Because this identifies stored events on disk, renaming the crate or the enum
/// silently orphans previously written events. Pin the identity explicitly with
/// the `name` option to decouple it from code names:
///
/// ```rust
/// #[evento::aggregate(name = "bank/BankAccount")]
/// pub enum BankAccount {
///     /// Defaults to the variant name; override per variant if needed:
///     #[evento(name = "AccountOpened")]
///     AccountOpened { owner_id: String },
/// }
/// ```
///
/// # Example
///
/// ```rust
/// #[evento::aggregate]
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
/// assert_eq!(event.initial_balance, 1000);
/// ```
///
/// # Additional Derives
///
/// Pass additional derives as arguments:
///
/// ```rust
/// #[evento::aggregate(serde::Serialize, serde::Deserialize)]
/// pub enum MyEvents {
///     SomethingHappened { id: String },
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
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    match aggregator::aggregator(attr, item) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Generates routing-key command variants from a single method body.
///
/// Apply to an `impl` block. Every method whose **last** parameter is exactly
/// `routing_key: Option<String>` is expanded into three methods:
///
/// - `name_opt(..., routing_key: Option<String>)` — the original body (hidden
///   from docs)
/// - `name(...)` — forwards `None`
/// - `name_with_routing(..., routing_key: impl Into<String>)` — forwards
///   `Some(key)`
///
/// This removes the need to duplicate a command body just to add
/// `.routing_key(key)` — write the body once against
/// `WriteBuilder::routing_key_opt`:
///
/// ```rust
/// use evento::Executor;
/// # #[evento::aggregate]
/// # pub enum BankAccount {
/// #     MoneyTransferred { to: String, amount: i64 },
/// # }
///
/// pub struct Command<E: Executor>(pub E);
///
/// #[evento::command]
/// impl<E: Executor> Command<E> {
///     pub async fn transfer_money(
///         &self,
///         id: impl Into<String>,
///         to: String,
///         amount: i64,
///         routing_key: Option<String>,
///     ) -> anyhow::Result<()> {
///         // guards...
///         evento::append(id)
///             .routing_key_opt(routing_key)
///             .event(&MoneyTransferred { to, amount })
///             .commit(&self.0)
///             .await?;
///         Ok(())
///     }
/// }
///
/// # async fn call<E: Executor>(command: Command<E>) -> anyhow::Result<()> {
/// // Callers get the familiar pair:
/// command.transfer_money("account-1", "account-2".into(), 100).await?;
/// command
///     .transfer_money_with_routing("account-1", "account-2".into(), 100, "eu-west")
///     .await?;
/// # Ok(()) }
/// ```
///
/// Methods without a trailing `routing_key: Option<String>` parameter are
/// passed through untouched. Expanded methods must be `async`, take `&self`,
/// and use simple identifier parameters.
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::ItemImpl);
    match command::command_impl(attr, &input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
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
/// ```text
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
/// ```rust,no_run
/// use evento::{metadata::Event, Executor, Projection};
/// # #[evento::aggregate]
/// # pub enum BankAccount {
/// #     MoneyDeposited { amount: i64 },
/// # }
/// # #[evento::projection(bitcode::Encode, bitcode::Decode)]
/// # pub struct AccountBalanceView {
/// #     pub balance: i64,
/// # }
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
/// # async fn run<E: Executor>(executor: &E) -> anyhow::Result<()> {
/// // Register with projection
/// let projection = Projection::<_, AccountBalanceView>::new::<BankAccount>()
///     .handler(handle_money_deposited());
///
/// // Execute projection to get current state
/// let result = projection.load("account-123").execute(executor).await?;
/// # let _: Option<AccountBalanceView> = result;
/// # Ok(()) }
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
/// ```rust
/// use evento::metadata::Event;
/// # #[evento::aggregate]
/// # pub enum MyAggregate {
/// #     MyEvent { amount: i64 },
/// # }
/// # #[evento::projection(bitcode::Encode, bitcode::Decode)]
/// # pub struct MyView {
/// #     pub total: i64,
/// # }
///
/// #[evento::debug_handler]
/// async fn handle_event(
///     event: Event<MyEvent>,
///     projection: &mut MyView,
/// ) -> anyhow::Result<()> {
///     projection.total += event.data.amount;
///     Ok(())
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
/// ```text
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
/// ```rust,no_run
/// use evento::{
///     context::Data,
///     metadata::Event,
///     subscription::{Context, SubscriptionBuilder},
///     Executor,
/// };
/// # #[evento::aggregate]
/// # pub enum BankAccount {
/// #     MoneyDeposited { amount: i64 },
/// # }
/// # #[derive(Clone)]
/// # pub struct AppConfig {
/// #     pub webhook_url: String,
/// # }
/// # async fn send_notification(_url: &str, _amount: i64) -> anyhow::Result<()> {
/// #     Ok(())
/// # }
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
///     send_notification(&config.webhook_url, event.data.amount).await?;
///
///     Ok(())
/// }
///
/// # async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
/// // Register with subscription
/// let subscription = SubscriptionBuilder::new("deposit-notifier")
///     .handler(on_money_deposited())
///     .routing_key("accounts")
///     .start(executor)
///     .await?;
/// # subscription.shutdown().await?;
/// # Ok(()) }
/// ```
#[proc_macro_attribute]
pub fn subscription(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match subscription::subscription_next_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Creates a subscription handler that processes all events of an aggregate type.
///
/// This macro is similar to [`subscription`] but handles all events from an aggregate
/// without requiring the event data to be deserialized. The event is wrapped in
/// `RawEvent` which provides access to event metadata
/// (name, id, timestamp, etc.) without deserializing the payload.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```text
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
/// ```rust,no_run
/// use evento::{
///     metadata::RawEvent,
///     subscription::{Context, SubscriptionBuilder},
///     Executor,
/// };
/// # #[evento::aggregate]
/// # pub enum BankAccount {
/// #     MoneyDeposited { amount: i64 },
/// # }
///
/// #[evento::subscription_all]
/// async fn on_any_account_event<E: Executor>(
///     context: &Context<'_, E>,
///     event: RawEvent<BankAccount>,
/// ) -> anyhow::Result<()> {
///     // Access event metadata without deserializing
///     println!("Event: {} on {}", event.name, event.aggregate_id);
///     println!("Version: {}", event.version);
///     println!("Timestamp: {}", event.timestamp);
///
///     // Useful for logging, auditing, or forwarding events
///     Ok(())
/// }
///
/// # async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
/// // Register with subscription - handles all BankAccount events
/// let subscription = SubscriptionBuilder::new("account-auditor")
///     .handler(on_any_account_event())
///     .start(executor)
///     .await?;
/// # subscription.shutdown().await?;
/// # Ok(()) }
/// ```
#[proc_macro_attribute]
pub fn subscription_all(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match subscription_all::subscription_all_next_impl(&input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro for generating cursor structs and trait implementations.
///
/// Each `#[cursor(Column::Variant, order)]` field becomes part of a keyset cursor,
/// ordered by the given rank. The expansion binds against `sea_query` and
/// `evento::sql`, so this derive requires a SQL backend feature on `evento` plus a
/// direct `sea-query` dependency.
///
/// # Example
///
/// ```rust
/// use evento::Cursor;
/// use sea_query::Iden;
///
/// #[derive(Iden, Clone)]
/// pub enum ContactAdmin {
///     Id,
///     CreatedAt,
/// }
///
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

/// Implements the `Snapshot` trait for a projection struct.
///
/// Projections that derive `bitcode::Encode`/`bitcode::Decode` already get
/// executor-backed snapshots from a blanket impl. This macro covers the two
/// other common cases; the mode is required:
///
/// - `#[evento::snapshot(none)]` — opt out of snapshotting entirely (an empty
///   `Snapshot` impl). Use for views that are cheap to rebuild, or to prevent
///   a bitcode-encodable view from persisting snapshots.
/// - `#[evento::snapshot(memory)]` — an in-memory snapshot store. Generates a
///   `snapshot_rows()` associated function returning a
///   `&'static RwLock<HashMap<String, Self>>` keyed by aggregate id, and a
///   `Snapshot` impl whose `restore`/`take_snapshot`/`drop_snapshot` read,
///   insert, and remove entries in it.
///
/// Projections backed by a custom table (SQL, etc.) should keep implementing
/// `Snapshot` by hand.
///
/// Note: on a struct that is `bitcode::Encode + Decode`, either mode conflicts
/// with the blanket impl and rustc reports overlapping trait implementations —
/// drop the bitcode derives or the attribute.
///
/// # Example
///
/// ```rust
/// #[evento::projection]
/// #[evento::snapshot(memory)]
/// pub struct AccountDetailsView {
///     pub id: String,
///     pub balance: i64,
/// }
///
/// // Read the materialized rows elsewhere:
/// let rows = AccountDetailsView::snapshot_rows().read().unwrap();
/// assert!(rows.is_empty());
/// ```
#[proc_macro_attribute]
pub fn snapshot(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match snapshot::snapshot_impl(attr, &input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Adds cursor tracking fields and implements `ProjectionCursor`.
///
/// This attribute macro transforms a struct to track its position in the event stream.
/// It automatically adds cursor fields and implements the `ProjectionCursor` trait.
///
/// # Generated Code
///
/// - Adds `pub cursor: String` and `pub aggregate_version: u16` fields
/// - Adds `Default` and `Clone` derives (preserves existing derives)
/// - Implements `ProjectionCursor` trait
///
/// # Options
///
/// Arguments may combine named options and extra derive paths, in any order:
///
/// - `cursor = <Type>` — use a custom cursor field type instead of `String`.
///   The type must be `Clone + From<evento::cursor::Value> +
///   Into<evento::cursor::Value>` (`evento::cursor::Value` itself qualifies).
/// - `id = <field>` — additionally implement `ProjectionAggregate`, returning
///   the named field as the aggregate id. This enables `view.write()` for
///   emitting events from the projection.
/// - any path (e.g. `serde::Serialize`) — added to the derive list.
///
/// ```rust
/// #[evento::projection(cursor = evento::cursor::Value, id = id, serde::Serialize)]
/// pub struct MyView {
///     pub id: String,
/// }
/// ```
///
/// # Example
///
/// ```rust
/// #[evento::projection]
/// #[derive(Debug)]
/// pub struct MyStruct {
///     pub id: String,
///     pub name: String,
/// }
/// ```
///
/// expands to:
///
/// ```text
/// #[derive(Default, Clone, Debug)]
/// pub struct MyStruct {
///     pub id: String,
///     pub name: String,
///     pub cursor: String,
///     pub aggregate_version: u16,
/// }
///
/// impl evento::ProjectionCursor for MyStruct { ... }
/// ```
#[proc_macro_attribute]
pub fn projection(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match projection::projection_cursor_impl(attr, &input) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}
