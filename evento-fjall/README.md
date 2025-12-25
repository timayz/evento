# evento-fjall

Fjall embedded key-value store implementation for the evento event sourcing library.

## Overview

This crate provides an [`Executor`] implementation using [fjall](https://crates.io/crates/fjall),
an LSM-tree based embedded key-value storage engine. It's ideal for applications that need:

- **Embedded storage** - No external database server required
- **High write throughput** - LSM-tree optimized for write-heavy workloads
- **Simple deployment** - Single binary with no dependencies
- **Rust-native** - Pure Rust implementation

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-fjall = "1.8"
evento-core = "1.8"
rkyv = "0.8"
```

## Usage

### Basic Example

```rust
use evento_fjall::Fjall;
use evento::{Executor, metadata::Metadata, cursor::Args, ReadAggregator};

// Define events using an enum
#[evento::aggregator]
pub enum User {
    UserCreated { name: String },
    NameChanged { name: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Open the database
    let executor = Fjall::open("./my-events")?;

    // Create an event
    let id = evento::create()
        .event(&UserCreated { name: "test".into() })?
        .metadata(&Metadata::default())?
        .commit(&executor)
        .await?;

    // Query events
    let result = executor.read(
        Some(vec![ReadAggregator::id("user/User", &id)]),
        None,
        Args::forward(10, None),
    ).await?;

    println!("Found {} events", result.edges.len());
    Ok(())
}
```

### Custom Configuration

```rust
use evento_fjall::Fjall;
use fjall::Config;

// Configure fjall with custom options
let keyspace = Config::new("./events.db")
    .max_write_buffer_size(128 * 1024 * 1024)  // 128MB write buffer
    .open()?;

let executor = Fjall::from_keyspace(keyspace)?;
```

### With Projections

```rust
use evento_fjall::Fjall;
use evento::{Executor, metadata::Event, projection::{Action, Projection}};

// Define events
#[evento::aggregator]
pub enum User {
    UserCreated { name: String },
}

#[derive(Default)]
struct UserView {
    name: String,
}

#[evento::handler]
async fn on_user_created<E: Executor>(
    event: Event<UserCreated>,
    action: Action<'_, UserView, E>,
) -> anyhow::Result<()> {
    if let Action::Apply(view) = action {
        view.name = event.data.name.clone();
    }
    Ok(())
}

let executor = Fjall::open("./events")?;

let projection = Projection::<UserView, _>::new("users")
    .handler(on_user_created());

let result = projection
    .load::<User>(&user_id)
    .execute(&executor)
    .await?;
```

## Data Model

Events are stored across multiple partitions for efficient querying:

| Partition | Key Format | Value | Purpose |
|-----------|------------|-------|---------|
| `events` | `{ULID}` | `Event` | Primary event storage |
| `agg_index` | `{type}\0{id}\0{version}` | `ULID` | Query by aggregate |
| `routing_index` | `{routing_key}\0{ULID}` | `()` | Query by routing key |
| `type_index` | `{type}\0{name}\0{ULID}` | `()` | Query by event type |
| `subscribers` | `{key}` | `SubscriberState` | Subscription state |

## Performance Considerations

- **Write batching** - Events are written atomically in batches
- **Async I/O** - Blocking fjall operations are wrapped with `spawn_blocking`
- **Persistence** - Writes are synced to disk after each batch for durability
- **Compression** - Consider enabling fjall's compression for large datasets

## Comparison with SQL Executors

| Feature | evento-fjall | evento-sql |
|---------|--------------|------------|
| Deployment | Embedded | Requires DB server |
| Setup | Zero configuration | Needs migrations |
| Scalability | Single node | Distributed possible |
| Query flexibility | Limited | Full SQL |
| Write performance | Excellent | Good |
| Use case | Embedded apps, CLIs | Web services, microservices |

## License

See the [LICENSE](../LICENSE) file in the repository root.
