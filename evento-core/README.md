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

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-core = "1.8"
```

## Usage

### Defining Aggregates and Events

```rust
use evento::aggregator;

#[aggregator("myapp/Account")]
#[derive(Default)]
pub struct Account {
    pub balance: i64,
    pub owner: String,
}

#[aggregator("myapp/Account")]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AccountOpened {
    pub owner: String,
}

#[aggregator("myapp/Account")]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct MoneyDeposited {
    pub amount: i64,
}
```

### Creating Events

```rust
use evento::{create, aggregator, metadata::Metadata};

// Create a new aggregate with events
let account_id = create()
    .event(&AccountOpened { owner: "Alice".into() })?
    .metadata(&Metadata::new("user-123"))?
    .routing_key("accounts")
    .commit(&executor)
    .await?;

// Add events to existing aggregate
aggregator(&account_id)
    .original_version(1)
    .event(&MoneyDeposited { amount: 100 })?
    .metadata(&Metadata::new("user-123"))?
    .commit(&executor)
    .await?;
```

### Building Projections

```rust
use evento::{projection::Projection, handler, metadata::Event};

#[derive(Default)]
pub struct AccountView {
    pub balance: i64,
    pub owner: String,
}

#[handler]
async fn on_account_opened<E: Executor>(
    event: Event<AccountOpened>,
    action: Action<'_, AccountView, E>,
) -> anyhow::Result<()> {
    if let Action::Apply(view) = action {
        view.owner = event.data.owner.clone();
    }
    Ok(())
}

#[handler]
async fn on_money_deposited<E: Executor>(
    event: Event<MoneyDeposited>,
    action: Action<'_, AccountView, E>,
) -> anyhow::Result<()> {
    if let Action::Apply(view) = action {
        view.balance += event.data.amount;
    }
    Ok(())
}

// Load aggregate state
let projection = Projection::<AccountView, _>::new("accounts")
    .handler(on_account_opened)
    .handler(on_money_deposited);

let result = projection
    .load::<Account>("account-123")
    .execute(&executor)
    .await?;
```

### Running Subscriptions

```rust
use std::time::Duration;

let subscription = Projection::<AccountView, _>::new("account-processor")
    .handler(on_account_opened)
    .handler(on_money_deposited)
    .subscription()
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
    pub data: Vec<u8>,          // rkyv-serialized event data
    pub metadata: Vec<u8>,      // rkyv-serialized metadata
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
