# Evento

[![Crates.io](https://img.shields.io/crates/v/evento.svg)](https://crates.io/crates/evento)
[![Documentation](https://docs.rs/evento/badge.svg)](https://docs.rs/evento)
[![License](https://img.shields.io/crates/l/evento.svg)](https://github.com/timayz/evento/blob/main/LICENSE)

Event sourcing and CQRS toolkit with SQL persistence, projections, and subscriptions.

More information about this crate can be found in the [crate documentation][docs].

## Features

- **Event Sourcing**: Store state changes as immutable events with complete audit trail
- **CQRS Pattern**: Separate read and write models for scalable architectures
- **SQL Database Support**: Built-in support for SQLite, PostgreSQL, and MySQL
- **Embedded Storage**: Fjall key-value store for embedded applications
- **Projections**: Build read models by replaying events
- **Subscriptions**: Continuous event stream processing with cursor tracking
- **Database Migrations**: Automated schema management
- **Compact Serialization**: Fast binary serialization with bitcode
- **Type Safety**: Fully typed events and aggregates with compile-time guarantees

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
evento = { version = "2", features = ["sqlite"] }
bitcode = "0.6"
```

## Usage Example

```rust
use evento::{Executor, metadata::{Event, Metadata}, projection::Projection};

// Define events using an enum
#[evento::aggregator]
pub enum User {
    UserCreated {
        name: String,
        email: String,
    },
    UserEmailChanged {
        email: String,
    },
}

// Define a view/projection with cursor tracking
#[evento::projection]
#[derive(Debug)]
pub struct UserView {
    pub name: String,
    pub email: String,
}

// Define projection handlers - they update state from events
#[evento::handler]
async fn on_user_created(
    event: Event<UserCreated>,
    view: &mut UserView,
) -> anyhow::Result<()> {
    view.name = event.data.name.clone();
    view.email = event.data.email.clone();
    Ok(())
}

#[evento::handler]
async fn on_email_changed(
    event: Event<UserEmailChanged>,
    view: &mut UserView,
) -> anyhow::Result<()> {
    view.email = event.data.email.clone();
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup SQLite executor
    let pool = sqlx::SqlitePool::connect("sqlite:events.db").await?;
    let mut conn = pool.acquire().await?;

    // Run migrations
    evento::sql_migrator::new()?
        .run(&mut *conn, &evento::migrator::Plan::apply_all())
        .await?;

    let executor: evento::Sqlite = pool.into();

    // Create and save events
    let user_id = evento::create()
        .event(&UserCreated {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        })
        .metadata(&Metadata::default())
        .commit(&executor)
        .await?;

    // Update user
    evento::aggregator(&user_id)
        .original_version(1)
        .event(&UserEmailChanged {
            email: "newemail@example.com".to_string(),
        })
        .metadata(&Metadata::default())
        .commit(&executor)
        .await?;

    // Build projection and load state
    let result = Projection::<_, UserView>::new::<User>(&user_id)
        .handler(on_user_created())
        .handler(on_email_changed())
        .execute(&executor)
        .await?;

    if let Some(user) = result {
        println!("User: {} ({})", user.name, user.email);
    }

    Ok(())
}
```

## Continuous Subscriptions

Subscriptions process events in real-time with side effects:

```rust
use std::time::Duration;
use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};

// Subscription handlers receive context and can perform side effects
#[evento::sub_handler]
async fn notify_user_created<E: Executor>(
    context: &Context<'_, E>,
    event: Event<UserCreated>,
) -> anyhow::Result<()> {
    println!("New user: {}", event.data.name);
    // Send welcome email, update external systems, etc.
    Ok(())
}

// Start a subscription that continuously processes events
let subscription = SubscriptionBuilder::<evento::Sqlite>::new("user-notifier")
    .handler(notify_user_created())
    .routing_key("users")
    .chunk_size(100)
    .retry(5)
    .delay(Duration::from_secs(10))
    .start(&executor)
    .await?;

// On application shutdown
subscription.shutdown().await?;
```

## Handle All Events

Use `sub_all_handler` to process all events from an aggregate without deserializing:

```rust
use evento::{Executor, SkipEventData, subscription::Context};

#[evento::sub_all_handler]
async fn audit_user_events<E: Executor>(
    context: &Context<'_, E>,
    event: SkipEventData<User>,
) -> anyhow::Result<()> {
    println!("Event {} on user {}", event.name, event.aggregator_id);
    Ok(())
}
```

## Feature Flags

- **`macro`** _(enabled by default)_ - Procedural macros for cleaner code
- **`group`** - Multi-executor support for querying across databases
- **`rw`** - Read-write split executor for CQRS patterns
- **`sql`** - Enable all SQL database backends (SQLite, MySQL, PostgreSQL)
- **`sqlite`** - SQLite support via sqlx
- **`mysql`** - MySQL support via sqlx
- **`postgres`** - PostgreSQL support via sqlx
- **`fjall`** - Embedded key-value storage with Fjall

## Minimum Supported Rust Version

Evento's MSRV is 1.75.

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## Examples

The [examples] directory contains sample applications demonstrating various features:

- **bank** - Bank account domain model with commands, queries, and projections
- **bank-axum-sqlite** - Integration with Axum web framework and SQLite

## Getting Help

If you have questions or need help, please:

- Check the [documentation][docs]
- Look at the [examples]
- Open an issue on [GitHub](https://github.com/timayz/evento/issues)

## License

This project is licensed under the [Apache-2.0 license](LICENSE).

[docs]: https://docs.rs/evento
[examples]: https://github.com/timayz/evento/tree/main/examples
