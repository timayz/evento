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

Or use this crate directly:

```toml
[dependencies]
evento-macro = "2"
```

## Macros

| Macro | Type | Purpose |
|-------|------|---------|
| `#[evento::aggregator]` | Attribute | Transform enum into event structs |
| `#[evento::handler]` | Attribute | Create projection handler from async function |
| `#[evento::sub_handler]` | Attribute | Create subscription handler for specific events |
| `#[evento::sub_all_handler]` | Attribute | Create subscription handler for all events |
| `#[evento::projection]` | Attribute | Add cursor field and implement `ProjectionCursor` |
| `#[evento::snapshot]` | Attribute | Implement snapshot restoration |
| `#[derive(Cursor)]` | Derive | Generate cursor struct and trait implementations |
| `#[evento::debug_handler]` | Attribute | Like `handler` with debug output |
| `#[evento::debug_snapshot]` | Attribute | Like `snapshot` with debug output |

## Usage

### Defining Events with `#[evento::aggregator]`

Transform an enum into individual event structs with all required trait implementations:

```rust
#[evento::aggregator]
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

    MoneyWithdrawn {
        amount: i64,
        transaction_id: String,
    },
}
```

This generates:

- Individual structs: `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`
- `Aggregator` trait implementation (provides `aggregator_type()`)
- `Event` trait implementation (provides `event_name()`)
- Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and bitcode serialization

The aggregator type is formatted as `"{package_name}/{enum_name}"`, e.g., `"bank/BankAccount"`.

#### Additional Derives

Pass additional derives as arguments:

```rust
#[evento::aggregator(serde::Serialize, serde::Deserialize)]
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
// - Adds `pub cursor: String` field
// - Implements `ProjectionCursor` trait
// - Adds `Default` and `Clone` derives
```

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
let result = Projection::<_, AccountBalanceView>::new::<BankAccount>("account-123")
    .handler(handle_money_deposited())
    .execute(&executor)
    .await?;
```

The macro generates:

- `HandleMoneyDepositedHandler` struct
- `handle_money_deposited()` constructor function
- `projection::Handler<AccountBalanceView>` trait implementation

### Creating Subscription Handlers with `#[evento::sub_handler]`

Subscription handlers process events in real-time with side effects:

```rust
use evento::{Executor, metadata::Event, subscription::Context};

#[evento::sub_handler]
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

### Handling All Events with `#[evento::sub_all_handler]`

Handle all events from an aggregate type without deserializing:

```rust
use evento::{Executor, SkipEventData, subscription::Context};

#[evento::sub_all_handler]
async fn on_any_account_event<E: Executor>(
    context: &Context<'_, E>,
    event: SkipEventData<BankAccount>,
) -> anyhow::Result<()> {
    println!("Event {} on account {}", event.name, event.aggregator_id);
    Ok(())
}
```

### Snapshot Restoration with `#[evento::snapshot]`

Implement snapshot restoration for projections:

```rust
use evento::context::RwContext;
use std::collections::HashMap;

#[evento::snapshot]
async fn restore(
    context: &RwContext,
    id: String,
    aggregators: &HashMap<String, String>,
) -> anyhow::Result<Option<AccountBalanceView>> {
    // Query snapshot from your storage
    // Return None to rebuild from events
    Ok(None)
}
```

### Debug Macros

Use `#[evento::debug_handler]` or `#[evento::debug_snapshot]` to output the generated code to a file for inspection:

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

- **Events** (from `#[aggregator]`): Traits are automatically derived
- **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
- **Projection handlers**: Must be `async` and return `anyhow::Result<()>`
- **Subscription handlers**: Must be `async`, take `Context` first, and return `anyhow::Result<()>`

## Serialization

Events are serialized using [bitcode](https://crates.io/crates/bitcode) for compact binary representation. The `#[aggregator]` macro automatically adds the required bitcode derives:

- `bitcode::Encode`
- `bitcode::Decode`

## Minimum Supported Rust Version

Rust 1.75 or later.

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## License

See the [LICENSE](../LICENSE) file in the repository root.
