# evento-rocksdb

RocksDB embedded key-value store implementation for the evento event sourcing library.

## Overview

This crate provides an [`Executor`] implementation using [RocksDB](https://crates.io/crates/rocksdb),
Facebook's high-performance embedded key-value storage engine. It's ideal for applications that need:

- **Embedded storage** - No external database server required
- **High performance** - LSM-tree optimized for both reads and writes
- **Compression** - Built-in support for Snappy, LZ4, Zstd, Zlib, and Bzip2
- **Column families** - Efficient data organization
- **Battle-tested** - Used by Facebook, LinkedIn, and many others

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-rocksdb = "1.8"
evento-core = "1.8"
rkyv = "0.8"
```

## Usage

### Basic Example

```rust
use evento_rocksdb::Rocks;
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
    let executor = Rocks::open("./my-events")?;

    // Create an event
    let id = evento::create()
        .event(&UserCreated { name: "Alice".into() })?
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
use evento_rocksdb::Rocks;
use rocksdb::Options;

// Configure RocksDB with custom options
let mut opts = Options::default();
opts.set_max_write_buffer_number(4);
opts.set_write_buffer_size(128 * 1024 * 1024);  // 128MB write buffer
opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

let executor = Rocks::open_with_opts("./events.db", opts)?;
```

### With Projections

```rust
use evento_rocksdb::Rocks;
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

let executor = Rocks::open("./events")?;

let projection = Projection::<UserView, _>::new("users")
    .handler(on_user_created());

let result = projection
    .load::<User>(&user_id)
    .execute(&executor)
    .await?;
```

## Data Model

Events are stored across multiple column families for efficient querying:

| Column Family | Key Format | Value | Purpose |
|---------------|------------|-------|---------|
| `events` | `{ULID}` | `Event` | Primary event storage |
| `agg_index` | `{type}\0{id}\0{version}` | `ULID` | Query by aggregate |
| `routing_index` | `{routing_key}\0{ULID}` | `()` | Query by routing key |
| `type_index` | `{type}\0{name}\0{ULID}` | `()` | Query by event type |
| `subscribers` | `{key}` | `SubscriberState` | Subscription state |

## Performance Considerations

- **Write batching** - Events are written atomically using WriteBatch
- **Async I/O** - Blocking RocksDB operations are wrapped with `spawn_blocking`
- **Compression** - Enable LZ4 or Zstd for better storage efficiency
- **Column families** - Separate data by access pattern for better cache utilization

## Comparison with Other Executors

| Feature | evento-rocksdb | evento-fjall | evento-sql |
|---------|----------------|--------------|------------|
| Deployment | Embedded | Embedded | Requires DB server |
| Setup | Zero configuration | Zero configuration | Needs migrations |
| Language | C++ (via FFI) | Pure Rust | Varies |
| Maturity | Battle-tested | Newer | Mature |
| Compression | Multiple options | Optional | Database-dependent |
| Use case | High-performance embedded | Rust-native embedded | Web services |

## License

See the [LICENSE](../LICENSE) file in the repository root.
