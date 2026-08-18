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
evento-fjall = "2"
evento-core = "2"
bitcode = "0.6"
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
        .event(&UserCreated { name: "test".into() })
        .metadata(&Metadata::default())
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
use evento::{metadata::Event, projection::Projection};

// Define events
#[evento::aggregator]
pub enum User {
    UserCreated { name: String },
}

#[evento::projection]
struct UserView {
    name: String,
}

#[evento::handler]
async fn on_user_created(
    event: Event<UserCreated>,
    view: &mut UserView,
) -> anyhow::Result<()> {
    view.name = event.data.name.clone();
    Ok(())
}

let executor = Fjall::open("./events")?;

let result = Projection::<_, UserView>::new::<User>(&user_id)
    .handler(on_user_created())
    .execute(&executor)
    .await?;
```

## Data Model

Events are stored across multiple partitions. Index keys are built from
length-prefixed components (`enc(...)` = `{u32 BE len}{bytes}` per component),
so caller-supplied strings can contain any byte without key collisions. Reads
are served by **cursor-ordered** indexes whose keys end with the 30-byte
`cursor_key` = `{timestamp BE u64}{subsec BE u32}{version BE u16}{ULID bytes}`
— lexicographic key order equals cursor order, so a page is a seek plus
`limit` steps:

| Partition | Key Format | Value | Purpose |
|-----------|------------|-------|---------|
| `events` | `{ULID}` | `Event` | Primary event storage |
| `agg_index` | `enc(type, id) + {version BE}` | `ULID` | Version lookup (optimistic concurrency) |
| `cursor_all` | `{cursor_key}` | `()` | Unfiltered reads |
| `cursor_agg` | `enc(type, id) + {cursor_key}` | `()` | Query by aggregate |
| `cursor_agg_name` | `enc(type, id, name) + {cursor_key}` | `()` | Query by aggregate + event name |
| `cursor_type` | `enc(type) + {cursor_key}` | `()` | Query by aggregate type |
| `cursor_type_name` | `enc(type, name) + {cursor_key}` | `()` | Query by event type |
| `cursor_routing` | `{0x01}enc(routing_key) + {cursor_key}` (`{0x00} + {cursor_key}` when unkeyed) | `()` | Query by routing key |
| `subscribers` | `{key}` | `SubscriberState` | Subscription state |
| `snapshots` | `enc(type, id)` | `StoredSnapshot` | Aggregate snapshots |
| `meta` | `last_stamp`, `index_version` | `u64 BE` | Commit clock, index layout version |

Opening a database written by an older layout rebuilds the cursor indexes from
`events` once (O(total events)), then stamps `index_version`.

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
