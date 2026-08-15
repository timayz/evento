# Evento

A collection of libraries and tools that help you build DDD, CQRS, and event sourcing applications in Rust.

## Features

- Event sourcing with SQL databases (SQLite, MySQL, PostgreSQL) and embedded storage (Fjall)
- CQRS pattern implementation
- Domain-driven design support
- Event handlers and subscriptions
- Built-in migrations
- Macros for easy aggregate and handler implementation
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

### 1. Define Events with an Aggregate Enum

```rust
// Define your events using an enum; each variant becomes an event struct.
#[evento::aggregate]
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

### 2. Create a New Aggregate

```rust
async fn create_user(executor: &evento::Sqlite) -> anyhow::Result<String> {
    // `create()` starts a brand-new aggregate and returns its generated id.
    let user_id = evento::create()
        .event(&UserCreated {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        })
        .commit(executor)
        .await?;

    Ok(user_id)
}
```

### 3. Append Events to an Existing Aggregate

```rust
async fn change_user_email(
    executor: &evento::Sqlite,
    user_id: &str,
    original_version: u16,
    new_email: &str,
) -> anyhow::Result<()> {
    // `append(id)` continues an existing aggregate. `original_version` enables
    // optimistic concurrency: the commit fails if another writer raced ahead.
    evento::append(user_id)
        .original_version(original_version)
        .event(&UserEmailChanged {
            email: new_email.to_string(),
        })
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
    // Register handlers, then `load(id)` the aggregate and `execute` to replay it.
    let result = Projection::<_, UserView>::new::<User>()
        .handler(on_user_created())
        .handler(on_email_changed())
        .load(user_id)
        .execute(executor)
        .await?;

    Ok(result)
}
```

### 5. Subscribe to Events with Continuous Processing

```rust
use std::time::Duration;
use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};

#[evento::subscription]
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

To drain currently-pending events once instead of running a background loop, use
`run_once(&executor)` (optionally after `no_retry()`) rather than `start`.

### 6. Complete Example with SQLite

```rust
use evento::{metadata::Event, projection::Projection};
use sqlx::SqlitePool;

#[evento::aggregate]
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

    // Run migrations (the database type is required)
    evento::sql_migrator::new::<sqlx::Sqlite>()?
        .run(&mut *conn, &evento::migrator::Plan::apply_all())
        .await?;
    drop(conn);

    let executor: evento::Sqlite = pool.into();

    // Create a user
    let user_id = evento::create()
        .event(&UserCreated {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        })
        .commit(&executor)
        .await?;

    // Load the user via projection
    let user = Projection::<_, UserView>::new::<User>()
        .handler(on_user_created())
        .handler(on_email_changed())
        .load(&user_id)
        .execute(&executor)
        .await?;

    if let Some(user) = user {
        println!("Loaded user: {} ({})", user.name, user.email);
    }

    // Update the user (version 1 -> 2)
    evento::append(&user_id)
        .original_version(1)
        .event(&UserEmailChanged {
            email: "alice.doe@example.com".to_string(),
        })
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

### Remote (Client/Server)
```toml
evento = { version = "2", features = ["remote"] }
```

Serve any executor over framed TCP and connect to it from another process; the
client implements `Executor`, so commands, projections, and subscriptions work
unchanged:

```rust,ignore
// Server process
let handle = evento::remote::serve(listener, executor);

// Client process
let client = evento::RemoteClient::connect(addr).await?;
```

## Key Concepts

- **Events**: Immutable facts that represent something that happened
- **Aggregates**: Domain entities whose state is the fold of their events
- **Projections**: Read models built by replaying events
- **Handlers**: Functions that react to events (pure for projections, side-effecting for subscriptions)
- **Subscriptions**: Continuous processing of events with cursor tracking
- **CQRS**: Command Query Responsibility Segregation pattern support

## Core API at a Glance

| Concern | Entry point |
|---------|-------------|
| Define events | `#[evento::aggregate] enum` |
| Start a new aggregate | `evento::create()` → `WriteBuilder` |
| Append to an aggregate | `evento::append(id)` → `WriteBuilder` |
| Load a read model | `Projection::new::<A>().handler(..).load(id).execute(exec)` |
| Filter events when reading | `EventFilter::by_type / by_id / by_event / exact` |
| Continuous processing | `SubscriptionBuilder::new(key)...start(exec)` |
| One-shot processing | `SubscriptionBuilder::new(key)...run_once(exec)` |
| Fail on unhandled events | `.strict()` |
| Keep going after a handler error | `.continue_on_error()` |

## Features

- `macro` - Enable procedural macros for aggregates and handlers (default)
- `sql` - Enable all SQL database backends
- `sqlite` - SQLite support with automatic migrations
- `postgres` - PostgreSQL support with automatic migrations
- `mysql` - MySQL support with automatic migrations
- `fjall` - Embedded key-value storage with Fjall
- `remote` - Client/server executor over framed TCP
- `group` - Multi-executor support for querying across databases
- `rw` - Read-write split executor for CQRS patterns

## Examples

See the `examples/` directory for complete working examples:

- `examples/bank/` - Bank account domain model with commands, queries, and projections
- `examples/bank-axum-sqlite/` - Integration with the Axum web framework and SQLite
- `examples/bank-axum-fjall/` - Integration with the Axum web framework and embedded Fjall storage
- `examples/bank-axum-remote/` - Axum web app(s) talking to a store served over `evento-remote` (client/server split)

## License

Licensed under the Apache License, Version 2.0.
