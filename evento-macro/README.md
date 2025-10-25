# Evento Macros

[![Crates.io](https://img.shields.io/crates/v/evento-macro.svg)](https://crates.io/crates/evento-macro)
[![Documentation](https://docs.rs/evento-macro/badge.svg)](https://docs.rs/evento-macro)
[![License](https://img.shields.io/crates/l/evento-macro.svg)](https://github.com/timayz/evento/blob/main/LICENSE)

Procedural macros for the [Evento](https://crates.io/crates/evento) event sourcing framework.

This crate provides macros that simplify building event-sourced applications by automatically generating boilerplate code for aggregators and event handlers.

More information about this crate can be found in the [crate documentation][docs].

## Usage

This crate is typically used through the main `evento` crate with the `macro` feature enabled (which is on by default):

```toml
[dependencies]
evento = "1.5"
```

Or explicitly:

```toml
[dependencies]
evento = { version = "1.5", features = ["macro"] }
```

## Provided Macros

### `#[evento::aggregator]`

Automatically implements the `Aggregator` trait by generating event dispatching logic based on your handler methods.

```rust
use evento::{EventDetails, AggregatorName};
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};

#[derive(AggregatorName, Encode, Decode)]
struct UserCreated {
    name: String,
    email: String,
}

#[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
struct User {
    name: String,
    email: String,
}

#[evento::aggregator]
impl User {
    async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
        self.name = event.data.name;
        self.email = event.data.email;
        Ok(())
    }
}
```

The macro generates:
- Implementation of `evento::Aggregator::aggregate` with event dispatching
- Implementation of `evento::Aggregator::revision` based on a hash of handler methods
- Implementation of `evento::AggregatorName` using the package name and struct name

### `#[evento::handler(AggregateType)]`

Creates an event handler for use with event subscriptions.

```rust
use evento::{Context, EventDetails, Executor};

#[evento::handler(User)]
async fn on_user_created<E: Executor>(
    context: &Context<'_, E>,
    event: EventDetails<UserCreated>,
) -> anyhow::Result<()> {
    println!("User created: {}", event.data.name);
    // Trigger side effects, send notifications, update read models, etc.
    Ok(())
}

// Use with subscription
evento::subscribe("user-handlers")
    .handler(on_user_created())
    .run(&executor)
    .await?;
```

The macro generates:
- A struct implementing `evento::SubscribeHandler`
- A constructor function returning an instance of that struct
- Type-safe event filtering based on the event type

### `#[derive(AggregatorName)]`

Derives the `AggregatorName` trait which provides a name identifier for event types.

```rust
use evento::AggregatorName;
use bincode::{Encode, Decode};

#[derive(AggregatorName, Encode, Decode)]
struct UserCreated {
    name: String,
    email: String,
}

assert_eq!(UserCreated::name(), "UserCreated");
```

## Requirements

When using these macros, your types must implement the necessary traits:

- **Aggregates** must implement: `Default`, `Send`, `Sync`, `Clone`, `Debug`, `bincode::Encode`, `bincode::Decode`
- **Events** must implement: `bincode::Encode`, `bincode::Decode`
- **Handler functions** must be `async` and return `anyhow::Result<()>`

## Minimum Supported Rust Version

Evento-macro's MSRV is 1.75.

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## Getting Help

If you have questions or need help, please:

- Check the [documentation][docs]
- Look at the [examples] in the main evento repository
- Open an issue on [GitHub](https://github.com/timayz/evento/issues)

## License

This project is licensed under the [Apache-2.0 license](LICENSE).

[docs]: https://docs.rs/evento-macro
[examples]: https://github.com/timayz/evento/tree/main/examples
