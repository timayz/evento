# evento-sql-migrator

SQL database migrations for the [Evento](https://github.com/timayz/evento) event sourcing library.

## Overview

This crate provides database schema migrations required for storing events and subscriber state in SQL databases. It supports SQLite, MySQL, and PostgreSQL.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento-sql-migrator = "1.8"
```

By default, all database backends are enabled. To use only specific databases:

```toml
[dependencies]
evento-sql-migrator = { version = "1.8", default-features = false, features = ["postgres"] }
```

## Features

- `sqlite` - SQLite database support
- `mysql` - MySQL database support
- `postgres` - PostgreSQL database support

## Usage

```rust
use sqlx_migrator::{Migrate, Plan};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create your database connection pool
    let pool = sqlx::SqlitePool::connect(":memory:").await?;

    // Acquire a connection
    let mut conn = pool.acquire().await?;

    // Create the migrator for your database type
    let migrator = evento_sql_migrator::new::<sqlx::Sqlite>()?;

    // Run all pending migrations
    migrator.run(&mut *conn, &Plan::apply_all()).await?;

    Ok(())
}
```

When using the main `evento` crate, the migrator is re-exported:

```rust
let migrator = evento::sql_migrator::new::<sqlx::Sqlite>()?;
migrator.run(&mut *conn, &Plan::apply_all()).await?;
```

## Migrations

The crate includes the following migrations:

| Migration | Description |
|-----------|-------------|
| `InitMigration` | Creates the initial schema (event, snapshot, subscriber tables) |
| `M0002` | Adds `timestamp_subsec` column for sub-second precision |
| `M0003` | Drops snapshot table, extends event name column to VARCHAR(50) |

## Database Schema

After running all migrations, the following tables are created:

### Event Table

Stores all domain events:

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(26) | Event ID (ULID format) |
| `name` | VARCHAR(50) | Event type name |
| `aggregator_type` | VARCHAR(50) | Aggregate root type |
| `aggregator_id` | VARCHAR(26) | Aggregate root instance ID |
| `version` | INTEGER | Event sequence number |
| `data` | BLOB | Serialized event data |
| `metadata` | BLOB | Serialized event metadata |
| `routing_key` | VARCHAR(50) | Optional routing key |
| `timestamp` | BIGINT | Event timestamp (seconds) |
| `timestamp_subsec` | BIGINT | Sub-second precision |

**Indexes:**
- `idx_event_type` - On `aggregator_type`
- `idx_event_type_id` - On `(aggregator_type, aggregator_id)`
- `idx_event_routing_key_type` - On `(routing_key, aggregator_type)`
- Unique constraint on `(aggregator_type, aggregator_id, version)`

### Subscriber Table

Tracks event subscription progress:

| Column | Type | Description |
|--------|------|-------------|
| `key` | VARCHAR(50) | Subscriber identifier (primary key) |
| `worker_id` | VARCHAR(26) | Associated worker ID |
| `cursor` | TEXT | Current event stream position |
| `lag` | INTEGER | Subscription lag counter |
| `enabled` | BOOLEAN | Whether subscription is active |
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

## License

See the [LICENSE](../LICENSE) file in the repository root.
