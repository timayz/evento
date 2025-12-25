# evento-sql

SQL database implementations for the [Evento](https://github.com/timayz/evento) event sourcing library.

## Overview

This crate provides SQL-based persistence for events and subscriber state, implementing the `Executor` trait from `evento-core`. It supports SQLite, MySQL, and PostgreSQL through feature flags.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-sql = "2"
```

By default, all database backends are enabled. To use only specific databases:

```toml
[dependencies]
evento-sql = { version = "2", default-features = false, features = ["postgres"] }
```

## Features

- `sqlite` - SQLite database support
- `mysql` - MySQL database support
- `postgres` - PostgreSQL database support

## Usage

### Basic Setup

```rust
use evento_sql::Sql;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx_migrator::{Migrate, Plan};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a connection pool
    let pool = SqlitePoolOptions::new()
        .connect(":memory:")
        .await?;

    // Run migrations (requires evento-sql-migrator)
    let mut conn = pool.acquire().await?;
    let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    drop(conn);

    // Create the executor
    let executor: Sql<sqlx::Sqlite> = pool.into();

    // Use with Evento for event sourcing operations
    Ok(())
}
```

### Type Aliases

Convenience type aliases are provided for each database:

```rust
use evento_sql::{Sqlite, MySql, Postgres};

// These are equivalent:
let executor: Sqlite = pool.into();
let executor: Sql<sqlx::Sqlite> = pool.into();
```

### Read-Write Pairs (CQRS)

For CQRS patterns with separate read and write connections:

```rust
use evento_sql::{RwSqlite, RwMySql, RwPostgres};
```

## Core Types

### `Sql<DB>`

The main executor type that wraps a SQLx connection pool. Implements the `Executor` trait with:

- `read` - Query events with filtering and cursor-based pagination
- `write` - Persist events with optimistic concurrency control
- `get_subscriber_cursor` - Get current cursor position for a subscriber
- `is_subscriber_running` - Check if a subscriber is active
- `upsert_subscriber` - Create or update a subscriber record
- `acknowledge` - Update subscriber cursor after processing

### `Reader`

Query builder for cursor-based pagination:

```rust
use evento_sql::{Reader, Event};
use sea_query::Query;

let statement = Query::select()
    .columns([Event::Id, Event::Name, Event::Data])
    .from(Event::Table)
    .to_owned();

let result = Reader::new(statement)
    .forward(10, None)  // First 10 events
    .execute::<_, MyEvent, _>(&pool)
    .await?;

// Navigate pages
if result.page_info.has_next_page {
    let next = Reader::new(statement)
        .forward(10, result.page_info.end_cursor)
        .execute::<_, MyEvent, _>(&pool)
        .await?;
}
```

### Column Identifiers

Sea-query column identifiers for type-safe query building:

- `Event` - Event table columns
- `Subscriber` - Subscriber table columns
- `Snapshot` - Snapshot table columns (deprecated, dropped in M0003)

## Serialization

Event data is serialized using [bitcode](https://crates.io/crates/bitcode) for compact binary representation. The `sql_types::Bitcode<T>` wrapper provides SQLx integration:

```rust
use evento_sql::sql_types::Bitcode;

// Wrap data for storage
let data = Bitcode(my_event_data);

// Encode to bytes
let bytes = data.encode_to();

// Decode from bytes
let decoded = Bitcode::<MyData>::decode_from_bytes(&bytes)?;
```

## Database Schema

This crate expects the database schema created by `evento-sql-migrator`. See that crate for the full schema definition.

### Event Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(26) | Event ID (ULID) |
| `name` | VARCHAR(50) | Event type name |
| `aggregator_type` | VARCHAR(50) | Aggregate root type |
| `aggregator_id` | VARCHAR(26) | Aggregate root instance ID |
| `version` | INTEGER | Event sequence number |
| `data` | BLOB | Serialized event data (bitcode) |
| `metadata` | BLOB | Serialized metadata (bitcode) |
| `routing_key` | VARCHAR(50) | Optional routing key |
| `timestamp` | BIGINT | Timestamp (seconds) |
| `timestamp_subsec` | BIGINT | Sub-second precision |

### Subscriber Table

| Column | Type | Description |
|--------|------|-------------|
| `key` | VARCHAR(50) | Subscriber identifier (primary key) |
| `worker_id` | VARCHAR(26) | Current worker ID |
| `cursor` | TEXT | Current stream position |
| `lag` | INTEGER | Events behind |
| `enabled` | BOOLEAN | Active status |

## License

See the [LICENSE](../LICENSE) file in the repository root.
