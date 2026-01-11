# Evento

A collection of libraries and tools that help you build DDD, CQRS, and event sourcing applications in Rust.

## Features

- Event sourcing with SQL databases (SQLite, MySQL, PostgreSQL) and embedded storage (Fjall)
- CQRS pattern implementation
- Domain-driven design support
- Event handlers and subscriptions
- Built-in migrations
- Macro support for easy aggregator implementation
- Compact binary serialization with bitcode

## Quick Start

Add Evento to your `Cargo.toml`:

```toml
[dependencies]
evento = "2"
bitcode = "0.6"
```

For SQL database support, enable the appropriate features:

```toml
[dependencies]
evento = { version = "2", features = ["sqlite"] }
bitcode = "0.6"
```

## Basic Usage

### 1. Define Events with Aggregator Enum

```rust
use evento::aggregator;

// Define your events using an enum
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
```

This generates individual event structs (`UserCreated`, `UserEmailChanged`) with all required traits.

### 2. Create Events

```rust
use evento::metadata::Metadata;

async fn create_user(executor: &evento::Sqlite) -> anyhow::Result<String> {
    let user_id = evento::create()
        .event(&UserCreated {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    Ok(user_id)
}
```

### 3. Save Events to Existing Aggregates

```rust
use evento::metadata::Metadata;

async fn change_user_email(
    executor: &evento::Sqlite,
    user_id: &str,
    original_version: u16,
    new_email: &str
) -> anyhow::Result<()> {
    evento::aggregator(user_id)
        .original_version(original_version)
        .event(&UserEmailChanged {
            email: new_email.to_string(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    Ok(())
}
```

### 4. Build Projections to Load State

```rust
use evento::{metadata::Event, projection::Projection};

#[evento::projection]
pub struct UserView {
    pub name: String,
    pub email: String,
}

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

async fn get_user(executor: &evento::Sqlite, user_id: &str) -> anyhow::Result<Option<UserView>> {
    let result = Projection::<_, UserView>::new::<User>(user_id)
        .handler(on_user_created())
        .handler(on_email_changed())
        .execute(executor)
        .await?;

    Ok(result)
}
```

### 5. Subscribe to Events with Continuous Processing

```rust
use std::time::Duration;
use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};

#[evento::sub_handler]
async fn on_user_created_subscription<E: Executor>(
    _context: &Context<'_, E>,
    event: Event<UserCreated>,
) -> anyhow::Result<()> {
    println!("User created: {}", event.data.name);
    // Perform side effects: send emails, update read models, etc.
    Ok(())
}

async fn setup_subscriptions(executor: evento::Sqlite) -> anyhow::Result<()> {
    let subscription = SubscriptionBuilder::new("user-processor")
        .handler(on_user_created_subscription())
        .routing_key("users")
        .chunk_size(100)
        .retry(5)
        .delay(Duration::from_secs(10))
        .start(&executor)
        .await?;

    // On application shutdown
    subscription.shutdown().await?;

    Ok(())
}
```

### 6. Complete Example with SQLite

```rust
use evento::{metadata::{Event, Metadata}, projection::Projection};
use sqlx::SqlitePool;

#[evento::aggregator]
pub enum User {
    UserCreated { name: String, email: String },
    UserEmailChanged { email: String },
}

#[evento::projection]
pub struct UserView {
    pub name: String,
    pub email: String,
}

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
    // Setup database
    let pool = SqlitePool::connect("sqlite:events.db").await?;
    let mut conn = pool.acquire().await?;

    // Run migrations
    evento::sql_migrator::new()?
        .run(&mut *conn, &evento::migrator::Plan::apply_all())
        .await?;

    let executor: evento::Sqlite = pool.into();

    // Create a user
    let user_id = evento::create()
        .event(&UserCreated {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        })
        .metadata(&Metadata::default())
        .commit(&executor)
        .await?;

    // Load the user via projection
    let user = Projection::<_, UserView>::new::<User>(&user_id)
        .handler(on_user_created())
        .handler(on_email_changed())
        .execute(&executor)
        .await?;

    if let Some(user) = user {
        println!("Loaded user: {} ({})", user.name, user.email);
    }

    // Update the user
    evento::aggregator(&user_id)
        .original_version(1)
        .event(&UserEmailChanged {
            email: "alice.doe@example.com".to_string(),
        })
        .metadata(&Metadata::default())
        .commit(&executor)
        .await?;

    Ok(())
}
```

## Database Support

### SQLite
```toml
evento = { version = "2", features = ["sqlite"] }
```

### PostgreSQL
```toml
evento = { version = "2", features = ["postgres"] }
```

### MySQL
```toml
evento = { version = "2", features = ["mysql"] }
```

### Fjall (Embedded)
```toml
evento = { version = "2", features = ["fjall"] }
```

## Key Concepts

- **Events**: Immutable facts that represent something that happened
- **Aggregators**: Domain objects that group related events
- **Projections**: Read models built by replaying events
- **Handlers**: Functions that react to events and can trigger side effects
- **Subscriptions**: Continuous processing of events with cursor tracking
- **CQRS**: Command Query Responsibility Segregation pattern support

## Features

- `macro` - Enable procedural macros for aggregators and handlers (default)
- `sql` - Enable all SQL database backends
- `sqlite` - SQLite support with automatic migrations
- `postgres` - PostgreSQL support with automatic migrations
- `mysql` - MySQL support with automatic migrations
- `fjall` - Embedded key-value storage with Fjall
- `group` - Multi-executor support for querying across databases
- `rw` - Read-write split executor for CQRS patterns

## Examples

See the `examples/` directory for complete working examples:

- `examples/bank/` - Bank account domain model with commands, queries, and projections
- `examples/bank-axum-sqlite/` - Integration with Axum web framework and SQLite

## License

Licensed under the Apache License, Version 2.0.
