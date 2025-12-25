# evento-macro

Procedural macros for the [Evento](https://github.com/timayz/evento) event sourcing framework.

## Overview

This crate provides macros that eliminate boilerplate when building event-sourced applications. It generates trait implementations, handler structs, and serialization code automatically.

## Installation

This crate is typically used through the main `evento` crate with the `macro` feature enabled (on by default):

```toml
[dependencies]
evento = "1.8"
```

Or use this crate directly:

```toml
[dependencies]
evento-macro = "1.8"
```

## Macros

| Macro | Type | Purpose |
|-------|------|---------|
| `#[evento::aggregator]` | Attribute | Transform enum into event structs |
| `#[evento::handler]` | Attribute | Create event handler from async function |
| `#[evento::snapshot]` | Attribute | Implement snapshot restoration |
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
        description: String,
    },

    MoneyWithdrawn {
        amount: i64,
        transaction_id: String,
        description: String,
    },

    AccountClosed {
        reason: String,
        final_balance: i64,
    },
}
```

This generates:

- Individual structs: `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`, `AccountClosed`
- `Aggregator` trait implementation (provides `aggregator_type()`)
- `Event` trait implementation (provides `event_name()`)
- Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and rkyv serialization

The aggregator type is formatted as `"{package_name}/{enum_name}"`, e.g., `"bank/BankAccount"`.

#### Additional Derives

Pass additional derives as arguments:

```rust
#[evento::aggregator(serde::Serialize, serde::Deserialize)]
pub enum MyEvents {
    // variants...
}
```

### Creating Handlers with `#[evento::handler]`

Create event handlers for projections:

```rust
use evento::{Event, Executor};
use evento::projection::Action;

#[evento::handler]
async fn handle_money_deposited<E: Executor>(
    event: Event<MoneyDeposited>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            // Mutate projection state
            row.balance += event.data.amount;
        }
        Action::Handle(_context) => {
            // Handle side effects (notifications, external calls, etc.)
        }
    };
    Ok(())
}

// Register with a projection
let projection = Projection::new("account-balance")
    .handler(handle_money_deposited())
    .handler(handle_money_withdrawn())
    .handler(handle_account_opened());
```

The macro generates:

- `HandleMoneyDepositedHandler` struct
- `handle_money_deposited()` constructor function
- `Handler<AccountBalanceView, E>` trait implementation

### Snapshot Restoration with `#[evento::snapshot]`

Implement snapshot restoration for projections:

```rust
use evento::LoadResult;
use evento::context::RwContext;

#[evento::snapshot]
async fn restore(
    context: &RwContext,
    id: String,
) -> anyhow::Result<Option<LoadResult<AccountBalanceView>>> {
    // Query snapshot from your storage
    // Return None to rebuild from events
    Ok(None)
}
```

### Debug Macros

Use `#[evento::debug_handler]` or `#[evento::debug_snapshot]` to output the generated code to a file for inspection:

```rust
#[evento::debug_handler]
async fn handle_event<E: Executor>(
    event: Event<MyEvent>,
    action: Action<'_, MyView, E>,
) -> anyhow::Result<()> {
    // ...
}
// Generated code written to: target/evento_debug_handler_macro.rs
```

## Requirements

When using these macros, your types must meet certain requirements:

- **Events** (from `#[aggregator]`): Traits are automatically derived
- **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
- **Handler functions**: Must be `async` and return `anyhow::Result<()>`

## Serialization

Events are serialized using [rkyv](https://rkyv.org/) for zero-copy deserialization. The `#[aggregator]` macro automatically adds the required rkyv derives:

- `rkyv::Archive`
- `rkyv::Serialize`
- `rkyv::Deserialize`

## Minimum Supported Rust Version

Rust 1.75 or later.

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## License

See the [LICENSE](../LICENSE) file in the repository root.
