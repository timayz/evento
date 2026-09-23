# evento-macro

Procedural macros for the [Evento](https://github.com/timayz/evento) event sourcing framework.

## Overview

This crate provides macros that eliminate boilerplate when building event-sourced applications. It generates trait implementations, handler structs, and serialization code automatically.

## Installation

This crate is typically used through the main `evento` crate with the `macro` feature enabled (on by default):

```toml
[dependencies]
evento = "2"
```

## Macros

| Macro | Type | Purpose |
|-------|------|---------|
| `#[evento::aggregate]` | Attribute | Transform enum into event structs |
| `#[evento::handler]` | Attribute | Create projection handler from async function |
| `#[evento::subscription]` | Attribute | Create subscription handler for specific events |
| `#[evento::subscription_all]` | Attribute | Create subscription handler for all events |
| `#[evento::projection]` | Attribute | Add cursor field and implement `ProjectionCursor` |
| `#[evento::snapshot]` | Attribute | Implement the `Snapshot` trait (`none` or `memory` mode) |
| `#[evento::command]` | Attribute | Generate routing-key command variants from one method body |
| `#[derive(Cursor)]` | Derive | Generate cursor struct and trait implementations |
| `#[evento::debug_handler]` | Attribute | Like `handler`, also dumps the expansion to a file |

## Usage

### Defining Events with `#[evento::aggregate]`

Transform an enum into individual event structs with all required trait implementations:

```rust
#[evento::aggregate]
pub enum BankAccount {
    /// Event raised when a new bank account is opened
    AccountOpened {
        owner_id: String,
        owner_name: String,
        initial_balance: i64,
    },

    MoneyDeposited {
        amount: i64,
        transaction_id: String,
    },
}
```

This generates:

- Individual structs: `AccountOpened`, `MoneyDeposited`
- `Aggregate` trait implementation (provides `aggregate_type()`)
- `AggregateEvent` trait implementation (provides `event_name()`)
- A unit struct named after the enum, for use as the aggregate marker
- A `{Enum}Event` enum newtyping those structs, with `TryFrom<&evento::Event>`
- Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization

The aggregate type defaults to `"{package_name}/{enum_name}"`, e.g. `"bank/BankAccount"`.

#### Pinning stored identities

The aggregate type and event names identify stored events on disk, so renaming
the crate, the enum, or a variant silently orphans previously written events.
Pin them explicitly to decouple the on-disk identity from code names:

```rust
#[evento::aggregate(name = "bank/BankAccount")]
pub enum BankAccount {
    #[evento(name = "AccountOpened")]
    AccountOpened { owner_id: String },
}
```

#### Evolving an event with `upcast_to`

A stored event's layout is frozen, so a new shape is a new variant. Point the old
variant at it and provide a `From` impl: older stored events are converted before
handlers see them, and only the newest handler has to exist.

```rust
#[evento::aggregate(name = "myapp/Payment")]
pub enum Payment {
    #[evento(upcast_to = PaymentRefundedV2)]
    PaymentRefunded { amount: i64 },
    PaymentRefundedV2 { amount: i64, reason: String },
}

impl From<PaymentRefunded> for PaymentRefundedV2 {
    fn from(old: PaymentRefunded) -> Self {
        Self { amount: old.amount, reason: "unknown".to_owned() }
    }
}
```

The target must be another variant of the same enum; self-references and cycles
are compile errors, and chains (`V1 -> V2 -> V3`) are folded into a single
conversion. The macro generates `AggregateEvent::upcasters()` on the newer
event; `#[evento::handler]`, `#[evento::subscription]` and `.skip::<E>()` pick
it up, so nothing is declared per projection. Keep the old variant in the enum —
it is the schema old events are decoded with.

#### Decoding a stored event back to the enum

The enum name is taken by the marker struct, so the events are also collected
into a sibling `{Enum}Event` that newtypes them. Anything forwarding events out
of the store — SSE, webhooks, outbox tables, audit logs — can then match
exhaustively instead of laddering over `event.name` with a `_` arm that silently
swallows variants added later:

```rust
match BankAccountEvent::try_from(&event)? {
    BankAccountEvent::AccountOpened(AccountOpened { owner_id, .. }) => { /* ... */ }
    BankAccountEvent::MoneyDeposited(deposit) => credit(deposit),
}
```

`event_name()` on the enum returns the stored name of the event it holds. The
conversion fails with `evento::FromEventError` when the event belongs to another
aggregate, names no known variant, or does not decode.

Decoding is verbatim — `#[evento(upcast_to = ...)]` is **not** applied, so an old
stored event decodes to its own variant. That matches what
`#[evento::subscription_all]` sees, and a `RawEvent<A>` decodes the same way:

```rust
#[evento::subscription_all]
async fn audit<E: Executor>(
    _ctx: &Context<'_, E>,
    event: RawEvent<BankAccount>,
) -> anyhow::Result<()> {
    match event.decode()? {
        BankAccountEvent::AccountOpened(_) => { /* ... */ }
        _ => {}
    }
    Ok(())
}
```

#### Additional Derives

Pass additional derives as arguments (they combine with `name = "..."` in any
order). They apply to the event structs *and* to the `{Enum}Event` enum, so
`serde::Serialize` is enough to serialize a whole decoded event:

```rust
#[evento::aggregate(serde::Serialize, serde::Deserialize)]
pub enum MyEvents {
    // variants...
}
```

### Projection State with `#[evento::projection]`

Automatically add cursor tracking to projection structs:

```rust
#[evento::projection]
#[derive(Debug)]
pub struct AccountBalanceView {
    pub balance: i64,
    pub owner: String,
}

// Generates:
// - Adds `pub cursor: String` and `pub aggregate_version: u16` fields
// - Implements `ProjectionCursor`
// - Adds `Default` and `Clone` derives
```

Options:

- `cursor = <Type>` — use a custom cursor field type instead of `String`. The
  type must be `Clone + From<evento::cursor::Value> + Into<evento::cursor::Value>`
  (`evento::cursor::Value` itself qualifies).
- `id = <field>` — additionally implement `ProjectionAggregate`, returning the
  named field as the aggregate id. This enables `view.write()` for emitting
  events from the projection.

```rust
#[evento::projection(cursor = evento::cursor::Value, id = id)]
pub struct AccountDetailsView {
    pub id: String,
    pub balance: i64,
}
```

### Snapshots with `#[evento::snapshot]`

Projections that derive `bitcode::Encode`/`bitcode::Decode` get executor-backed
snapshots from a blanket impl. Those snapshots are keyed by
`(aggregate type, projection name, aggregate id)`, so several snapshotted
projections of one aggregate never share a slot. The projection name defaults to
`"<module path>::<Struct>"`; pin it so that renaming or moving the struct keeps
its snapshots:

```rust
#[evento::projection(name = "myapp/BalanceView", bitcode::Encode, bitcode::Decode)]
pub struct BalanceView {
    pub balance: i64,
}
```

(`name` is rejected on generic structs, which keep a per-instantiation type
name.) For the other common cases, apply this attribute to the projection
struct (mode required):

```rust
// Opt out of snapshotting entirely:
#[evento::projection]
#[evento::snapshot(none)]
pub struct AccountStatusView { /* ... */ }

// In-memory snapshot store, keyed by aggregate id:
#[evento::projection]
#[evento::snapshot(memory)]
pub struct AccountDetailsView {
    pub id: String,
    pub balance: i64,
}

// `memory` also generates an accessor for reading the materialized rows:
let rows = AccountDetailsView::snapshot_rows().read().unwrap();
```

Projections backed by a custom table (SQL, etc.) should keep implementing
`Snapshot` by hand.

### Commands with `#[evento::command]`

Write a command body once with a trailing `routing_key: Option<String>`
parameter, and get the conventional `x` / `x_with_routing` pair generated:

```rust
#[evento::command]
impl<E: Executor> Command<E> {
    pub async fn transfer_money(
        &self,
        id: impl Into<String>,
        cmd: TransferMoney,
        routing_key: Option<String>,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await? else {
            return Err(BankAccountError::AccountNotFound);
        };
        // guards...
        account
            .write()?
            .routing_key_opt(routing_key)
            .event(&MoneyTransferred { /* ... */ })
            .commit(&self.0)
            .await?;
        Ok(())
    }
}

// Callers:
command.transfer_money("account-1", cmd).await?;
command.transfer_money_with_routing("account-1", cmd, "eu-west").await?;
```

Methods without a trailing `routing_key: Option<String>` parameter pass through
untouched.

### Creating Projection Handlers with `#[evento::handler]`

Projection handlers are used to build read models by replaying events:

```rust
use evento::metadata::Event;

#[evento::handler]
async fn handle_money_deposited(
    event: Event<MoneyDeposited>,
    projection: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    projection.balance += event.data.amount;
    Ok(())
}

// Register with a projection
let result = Projection::<_, AccountBalanceView>::new::<BankAccount>()
    .handler(handle_money_deposited())
    .load("account-123")
    .execute(&executor)
    .await?;
```

The macro generates:

- `HandleMoneyDepositedHandler` struct
- `handle_money_deposited()` constructor function
- `projection::Handler<AccountBalanceView>` trait implementation

The generated struct and constructor inherit the function's visibility, so a
`pub` handler can be registered from another module.

### Creating Subscription Handlers with `#[evento::subscription]`

Subscription handlers process events in real-time with side effects:

```rust
use evento::{Executor, metadata::Event, subscription::Context};

#[evento::subscription]
async fn on_money_deposited<E: Executor>(
    context: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    // Perform side effects: send notifications, update read models, etc.
    println!("Deposited: {}", event.data.amount);
    Ok(())
}

// Register with a subscription
let subscription = SubscriptionBuilder::<Sqlite>::new("deposit-notifier")
    .handler(on_money_deposited())
    .routing_key("accounts")
    .start(&executor)
    .await?;
```

### Handling All Events with `#[evento::subscription_all]`

Handle all events from an aggregate type without deserializing:

```rust
use evento::{Executor, metadata::RawEvent, subscription::Context};

#[evento::subscription_all]
async fn on_any_account_event<E: Executor>(
    context: &Context<'_, E>,
    event: RawEvent<BankAccount>,
) -> anyhow::Result<()> {
    println!("Event {} on account {}", event.name, event.aggregate_id);
    Ok(())
}
```

### Debug Macro

Use `#[evento::debug_handler]` to additionally write the generated code to a
file for inspection (resolved from `$CARGO_TARGET_DIR`, then
`<manifest>/target`, then the system temp dir — the write is best-effort and
never fails the build):

```rust
#[evento::debug_handler]
async fn handle_event(
    event: Event<MyEvent>,
    projection: &mut MyView,
) -> anyhow::Result<()> {
    // ...
}
// Generated code written to: target/evento_debug_handler_macro.rs
```

## Requirements

When using these macros, your types must meet certain requirements:

- **Events** (from `#[aggregate]`): Traits are automatically derived
- **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
- **Projection handlers**: Must be `async` and return `anyhow::Result<()>`
- **Subscription handlers**: Must be `async`, take `Context` first, and return `anyhow::Result<()>`

## Serialization

Events are serialized using [bitcode](https://crates.io/crates/bitcode) for compact binary representation. The `#[aggregate]` macro automatically adds the required bitcode derives:

- `bitcode::Encode`
- `bitcode::Decode`

## Minimum Supported Rust Version

Rust 1.75 or later.

## License

See the [LICENSE](../LICENSE) file in the repository root.
