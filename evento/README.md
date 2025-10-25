# Evento

[![Crates.io](https://img.shields.io/crates/v/evento.svg)](https://crates.io/crates/evento)
[![Documentation](https://docs.rs/evento/badge.svg)](https://docs.rs/evento)
[![License](https://img.shields.io/crates/l/evento.svg)](https://github.com/timayz/evento/blob/main/LICENSE)

A collection of libraries and tools that help you build DDD, CQRS, and event sourcing applications in Rust.

More information about this crate can be found in the [crate documentation][docs].

## Features

- **Event Sourcing**: Store state changes as immutable events with complete audit trail
- **CQRS Pattern**: Separate read and write models for scalable architectures
- **SQL Database Support**: Built-in support for SQLite, PostgreSQL, and MySQL
- **Event Handlers**: Async event processing with automatic retries
- **Event Subscriptions**: Continuous event stream processing
- **Event Streaming**: Real-time event streams (with `stream` feature)
- **Snapshots**: Periodic state captures to optimize aggregate loading
- **Database Migrations**: Automated schema management
- **Macro Free API**: Clean, macro-free API (macros available optionally)
- **Type Safety**: Fully typed events and aggregates with compile-time guarantees

## Usage Example

```rust
use evento::{EventDetails, AggregatorName};
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};

// Define events
#[derive(AggregatorName, Encode, Decode)]
struct UserCreated {
    name: String,
    email: String,
}

#[derive(AggregatorName, Encode, Decode)]
struct UserEmailChanged {
    email: String,
}

// Define aggregate
#[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
struct User {
    name: String,
    email: String,
}

// Implement event handlers on the aggregate
#[evento::aggregator]
impl User {
    async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
        self.name = event.data.name;
        self.email = event.data.email;
        Ok(())
    }

    async fn user_email_changed(&mut self, event: EventDetails<UserEmailChanged>) -> anyhow::Result<()> {
        self.email = event.data.email;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup SQLite executor
    let pool = sqlx::SqlitePool::connect("sqlite:events.db").await?;
    let executor: evento::Sqlite = pool.into();

    // Create and save events
    let user_id = evento::create::<User>()
        .data(&UserCreated {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        })?
        .metadata(&true)?
        .commit(&executor)
        .await?;

    // Update user
    evento::save::<User>(&user_id)
        .data(&UserEmailChanged {
            email: "newemail@example.com".to_string(),
        })?
        .metadata(&true)?
        .commit(&executor)
        .await?;

    // Load aggregate from events
    let user = evento::load::<User, _>(&executor, &user_id).await?;
    println!("User: {:?}", user.item);

    Ok(())
}
```

## Event Handlers and Subscriptions

```rust
use evento::{Context, EventDetails, Executor};

// Define a handler function
#[evento::handler(User)]
async fn on_user_created<E: Executor>(
    context: &Context<'_, E>,
    event: EventDetails<UserCreated>,
) -> anyhow::Result<()> {
    println!("User created: {} ({})", event.data.name, event.data.email);
    // Trigger side effects, send emails, update read models, etc.
    Ok(())
}

// Subscribe to events
evento::subscribe("user-handlers")
    .handler(on_user_created())
    .run(&executor)
    .await?;
```

## Feature Flags

- **`macro`** _(enabled by default)_  Enable procedural macros for cleaner code
- **`handler`** _(enabled by default)_  Enable event handlers with retry support
- **`stream`**  Enable streaming support with `tokio-stream`
- **`group`**  Enable event grouping functionality
- **`sql`**  Enable all SQL database backends (SQLite, MySQL, PostgreSQL)
- **`sqlite`**  SQLite support via sqlx
- **`mysql`**  MySQL support via sqlx
- **`postgres`**  PostgreSQL support via sqlx

## Minimum Supported Rust Version

Evento's MSRV is 1.75.

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## Examples

The [examples] directory contains sample applications demonstrating various features:

- **todos** - A complete todo application with CQRS, demonstrating command/query separation and event handlers

## Getting Help

If you have questions or need help, please:

- Check the [documentation][docs]
- Look at the [examples]
- Open an issue on [GitHub](https://github.com/timayz/evento/issues)

## License

This project is licensed under the [Apache-2.0 license](LICENSE).

[docs]: https://docs.rs/evento
[examples]: https://github.com/timayz/evento/tree/main/examples
