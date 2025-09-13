# Evento

A collection of libraries and tools that help you build DDD, CQRS, and event sourcing applications in Rust.

## Features

- Event sourcing with SQL databases (SQLite, MySQL, PostgreSQL)
- CQRS pattern implementation
- Domain-driven design support
- Event handlers and subscriptions
- Built-in migrations
- Macro support for easy aggregator implementation

## Quick Start

Add Evento to your `Cargo.toml`:

```toml
[dependencies]
evento = "1.0"
```

For SQL database support, enable the appropriate features:

```toml
[dependencies]
evento = { version = "1.0", features = ["sqlite", "sqlite-migrator"] }
```

## Basic Usage

### 1. Define Events and Aggregators

```rust
use evento::prelude::*;
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};

// Define your events
#[derive(AggregatorName, Encode, Decode)]
struct UserCreated {
    pub name: String,
    pub email: String,
}

#[derive(AggregatorName, Encode, Decode)]
struct UserEmailChanged {
    pub email: String,
}

// Define your aggregator
#[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
struct User {
    pub name: String,
    pub email: String,
}

// Implement the aggregator using the macro
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
```

### 2. Create Events

```rust
use evento::create;

async fn create_user(executor: &evento::Sqlite) -> anyhow::Result<String> {
    let user_id = create::<User>()
        .data(&UserCreated {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        })?
        .metadata(&true)?  // Optional metadata
        .commit(executor)
        .await?;
    
    Ok(user_id)
}
```

### 3. Save Events to Existing Aggregates

```rust
use evento::save;

async fn change_user_email(
    executor: &evento::Sqlite, 
    user_id: &str, 
    new_email: &str
) -> anyhow::Result<()> {
    save::<User>(user_id)
        .data(&UserEmailChanged {
            email: new_email.to_string(),
        })?
        .metadata(&false)?
        .commit(executor)
        .await?;
    
    Ok(())
}
```

### 4. Load Aggregates

```rust
use evento::load;

async fn get_user(executor: &evento::Sqlite, user_id: &str) -> anyhow::Result<User> {
    let result = load::<User, _>(executor, user_id).await?;
    Ok(result.item)
}
```

### 5. Subscribe to Events with Handlers

```rust
#[evento::handler(User)]
async fn on_user_created<E: evento::Executor>(
    context: &evento::Context<'_, E>,
    event: EventDetails<UserCreated>,
) -> anyhow::Result<()> {
    println!("User created: {} ({})", event.data.name, event.data.email);
    
    // You can emit new events here
    save::<User>(&event.aggregator_id)
        .data(&UserEmailChanged {
            email: format!("verified-{}", event.data.email),
        })?
        .metadata(&false)?
        .commit(context.executor)
        .await?;
    
    Ok(())
}

async fn setup_subscriptions(executor: evento::Sqlite) -> anyhow::Result<()> {
    evento::subscribe("user-handlers")
        .aggregator::<User>()
        .handler(on_user_created())
        .run(&executor)
        .await?;
    
    Ok(())
}
```

### 6. Complete Example with SQLite

```rust
use evento::prelude::*;
use sqlx::SqlitePool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup database
    let pool = SqlitePool::connect("sqlite:events.db").await?;
    let mut conn = pool.acquire().await?;
    
    // Run migrations
    evento::sql_migrator::new_migrator::<sqlx::Sqlite>()?
        .run(&mut *conn, &Plan::apply_all())
        .await?;
    
    let executor: evento::Sqlite = pool.into();
    
    // Setup event subscriptions
    evento::subscribe("user-service")
        .aggregator::<User>()
        .handler(on_user_created())
        .run(&executor)
        .await?;
    
    // Create a user
    let user_id = create::<User>()
        .data(&UserCreated {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        })?
        .metadata(&true)?
        .commit(&executor)
        .await?;
    
    // Load the user
    let user = load::<User, _>(&executor, &user_id).await?;
    println!("Loaded user: {:?}", user.item);
    
    // Update the user
    save::<User>(&user_id)
        .data(&UserEmailChanged {
            email: "alice.doe@example.com".to_string(),
        })?
        .metadata(&false)?
        .commit(&executor)
        .await?;
    
    Ok(())
}
```

## Database Support

### SQLite
```toml
evento = { version = "1.0.0-alpha.18", features = ["sqlite", "sqlite-migrator"] }
```

### PostgreSQL
```toml
evento = { version = "1.0.0-alpha.18", features = ["postgres", "postgres-migrator"] }
```

### MySQL
```toml
evento = { version = "1.0.0-alpha.18", features = ["mysql", "mysql-migrator"] }
```

## Key Concepts

- **Events**: Immutable facts that represent something that happened
- **Aggregators**: Domain objects that process events and maintain state
- **Handlers**: Functions that react to events and can trigger side effects
- **Subscriptions**: Continuous processing of events with specified handlers
- **CQRS**: Command Query Responsibility Segregation pattern support

## Features

- `macro` - Enable procedural macros for aggregators and handlers (default)
- `handler` - Enable event handler functionality (default)
- `stream` - Enable stream processing capabilities
- `sql` - Enable all SQL database backends
- `sqlite` - SQLite support
- `postgres` - PostgreSQL support
- `mysql` - MySQL support
- `sql-migrator` - Enable all SQL migrations
- `sqlite-migrator` - SQLite migrations
- `postgres-migrator` - PostgreSQL migrations
- `mysql-migrator` - MySQL migrations

## Examples

See the `examples/` directory for complete working examples:

- `examples/todos/` - A todo application demonstrating CQRS patterns

## License

Licensed under the Apache License, Version 2.0.
