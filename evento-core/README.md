# evento-core

Core types and traits for the Evento event sourcing library.

## Overview

This crate provides the foundational types for building event-sourced applications:

- **Events** - Immutable records of state changes with ULID identifiers
- **Aggregates** - Domain entities that produce and consume events
- **Projections** - Read models built by replaying events
- **Subscriptions** - Continuous event processing with cursor tracking
- **Executors** - Storage abstraction for persisting and querying events

## Features

- `macro` (default) - Re-exports procedural macros from `evento-macro`
- `group` - Multi-executor aggregation for querying across databases
- `rw` - Read-write split executor for CQRS patterns
- `sqlite` - SQLite database support
- `mysql` - MySQL database support
- `postgres` - PostgreSQL database support
- `fjall` - Embedded key-value storage with Fjall

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-core = "2"
bitcode = "0.6"
```

## Usage

### Defining Aggregates and Events

```rust
// Define events using an enum
#[evento::aggregator]
pub enum Account {
    AccountOpened {
        owner: String,
        initial_balance: i64,
    },
    MoneyDeposited {
        amount: i64,
    },
    MoneyWithdrawn {
        amount: i64,
    },
}
```

### Creating Events

```rust
use evento::{create, aggregator, metadata::Metadata};

// Create a new aggregate with events
let account_id = evento::create()
    .event(&AccountOpened { owner: "Alice".into(), initial_balance: 100 })
    .metadata(&Metadata::default())
    .routing_key("accounts")
    .commit(&executor)
    .await?;

// Add events to existing aggregate
evento::aggregator(&account_id)
    .original_version(1)
    .event(&MoneyDeposited { amount: 100 })
    .metadata(&Metadata::default())
    .commit(&executor)
    .await?;
```

### Building Projections

Projections are used to load aggregate state by replaying events:

```rust
use evento::{Executor, metadata::Event, projection::Projection};

// Define projection state with cursor tracking
#[evento::projection]
#[derive(Debug)]
pub struct AccountView {
    pub balance: i64,
    pub owner: String,
}

// Projection handlers update state from events
#[evento::handler]
async fn on_account_opened(
    event: Event<AccountOpened>,
    view: &mut AccountView,
) -> anyhow::Result<()> {
    view.owner = event.data.owner.clone();
    view.balance = event.data.initial_balance;
    Ok(())
}

#[evento::handler]
async fn on_money_deposited(
    event: Event<MoneyDeposited>,
    view: &mut AccountView,
) -> anyhow::Result<()> {
    view.balance += event.data.amount;
    Ok(())
}

// Load aggregate state
let result = Projection::<AccountView, _>::new::<Account>("account-123")
    .handler(on_account_opened())
    .handler(on_money_deposited())
    .execute(&executor)
    .await?;
```

### Running Subscriptions

Subscriptions process events in real-time with side effects:

```rust
use std::time::Duration;
use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};

// Subscription handlers receive context and can perform side effects
#[evento::sub_handler]
async fn on_account_opened<E: Executor>(
    context: &Context<'_, E>,
    event: Event<AccountOpened>,
) -> anyhow::Result<()> {
    println!("Account opened for {}", event.data.owner);
    Ok(())
}

let subscription = SubscriptionBuilder::<Sqlite>::new("account-processor")
    .handler(on_account_opened())
    .routing_key("accounts")
    .chunk_size(100)
    .retry(5)
    .delay(Duration::from_secs(10))
    .start(&executor)
    .await?;

// On shutdown
subscription.shutdown().await?;
```

### Cursor-based Pagination

```rust
use evento::cursor::Args;

// Forward pagination
let args = Args::forward(20, None);
let result = executor.read(Some(aggregators), None, args).await?;

// Continue from cursor
let args = Args::forward(20, result.page_info.end_cursor);
let next_page = executor.read(Some(aggregators), None, args).await?;

// Backward pagination
let args = Args::backward(20, Some(cursor));
let result = executor.read(Some(aggregators), None, args).await?;
```

## Core Types

### Event

The raw event structure stored in the database:

```rust
pub struct Event {
    pub id: Ulid,
    pub name: String,
    pub aggregator_id: String,
    pub aggregator_type: String,
    pub version: u16,
    pub data: Vec<u8>,          // bitcode-serialized event data
    pub metadata: Vec<u8>,      // bitcode-serialized metadata
    pub timestamp: u64,
    pub timestamp_subsec: u32,
    pub routing_key: Option<String>,
}
```

### Executor Trait

The core storage abstraction:

```rust
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;
    async fn read(...) -> anyhow::Result<ReadResult<Event>>;
    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>>;
    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool>;
    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()>;
    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()>;
}
```

## License

See the [LICENSE](../LICENSE) file in the repository root.
