# Evento

[![crates.io](https://img.shields.io/crates/v/evento.svg)](https://crates.io/crates/evento)
[![docs.rs](https://img.shields.io/docsrs/evento)](https://docs.rs/evento)
[![CI](https://github.com/timayz/evento/actions/workflows/ci.yml/badge.svg)](https://github.com/timayz/evento/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A collection of libraries and tools that help you build DDD, CQRS, and event sourcing
applications in Rust.

- **Event sourcing** — state changes stored as immutable events, with optimistic
  concurrency and a complete audit trail
- **CQRS** — commands, projections (read models), and continuous subscriptions
- **Macros** — `#[evento::aggregate]`, `#[evento::command]`, `#[evento::projection]`,
  `#[evento::snapshot]`, `#[evento::handler]`, `#[evento::subscription]`
- **Compact storage** — fast binary serialization with [bitcode](https://crates.io/crates/bitcode)

One `Executor` trait, many backends:

| Backend | Feature | Crate | Notes |
|---------|---------|-------|-------|
| SQLite / PostgreSQL / MySQL | `sqlite` / `postgres` / `mysql` | [evento-sql](evento-sql) | via sqlx, with [built-in migrations](evento-sql-migrator) |
| Fjall (embedded LSM-tree) | `fjall` | [evento-fjall](evento-fjall) | no external server |
| Remote (client/server TCP) | `remote` | [evento-remote](evento-remote) | serve any executor over framed TCP |
| Accord (consensus, alpha) | — | [evento-accord](evento-accord) | leaderless replicated store, strictly serializable ([design](evento-accord/DESIGN.md)) |

## Installation

Evento 2.x is currently in **alpha**; pin the exact pre-release version (a bare `"2"`
does not resolve pre-releases):

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["sqlite"] }
bitcode = "0.6"
anyhow = "1"
```

Swap `sqlite` for `postgres`, `mysql`, `fjall`, or `remote` as needed (see
[Feature flags](#feature-flags)).

## Quick start

### 1. Define events with an aggregate enum

Each variant becomes an event struct with all required traits (bitcode serialization,
`Aggregate`, `AggregateEvent`):

```rust
#[evento::aggregate]
pub enum BankAccount {
    AccountOpened { owner: String, initial_balance: i64 },
    MoneyDeposited { amount: i64 },
}
```

The aggregate type defaults to `"{package_name}/{enum_name}"`. Pin it (and event
names) so refactors never orphan stored events:

```rust
#[evento::aggregate(name = "bank/BankAccount")]
pub enum BankAccount {
    #[evento(name = "AccountOpened")]
    AccountOpened { owner: String },
}
```

### 2. Write events

`create()` starts a new aggregate; `append(id)` continues an existing one with
optimistic concurrency:

```rust,no_run
# #[evento::aggregate]
# pub enum BankAccount {
#     AccountOpened { owner: String, initial_balance: i64 },
#     MoneyDeposited { amount: i64 },
# }
# async fn run<E: evento::Executor>(executor: &E) -> anyhow::Result<()> {
let id = evento::create()
    .event(&AccountOpened { owner: "Alice".into(), initial_balance: 1000 })
    .routing_key("accounts")
    .commit(executor)
    .await?;

// Fails with WriteError::InvalidOriginalVersion if another writer raced ahead.
evento::append(&id)
    .original_version(1)
    .event(&MoneyDeposited { amount: 100 })
    .commit(executor)
    .await?;
# Ok(())
# }
```

### 3. Build read models with projections

Handlers are pure `(event, &mut view)` functions; `load(id)` replays the aggregate's
events through them:

```rust,no_run
use evento::{metadata::Event, projection::Projection};

# #[evento::aggregate]
# pub enum BankAccount {
#     AccountOpened { owner: String, initial_balance: i64 },
#     MoneyDeposited { amount: i64 },
# }
// bitcode derives make the view snapshottable through the executor.
#[evento::projection(bitcode::Encode, bitcode::Decode)]
pub struct AccountView {
    pub owner: String,
    pub balance: i64,
}

#[evento::handler]
async fn on_opened(event: Event<AccountOpened>, view: &mut AccountView) -> anyhow::Result<()> {
    view.owner = event.data.owner.clone();
    view.balance = event.data.initial_balance;
    Ok(())
}

#[evento::handler]
async fn on_deposited(event: Event<MoneyDeposited>, view: &mut AccountView) -> anyhow::Result<()> {
    view.balance += event.data.amount;
    Ok(())
}

# async fn run<E: evento::Executor>(executor: &E, id: &str) -> anyhow::Result<()> {
let view: Option<AccountView> = Projection::<_, AccountView>::new::<BankAccount>()
    .handler(on_opened())
    .handler(on_deposited())
    .load(id)
    .execute(executor)
    .await?;
# Ok(())
# }
```

### 4. Commands and the write gateway

The write side loads current state, guards invariants, then emits events through the
loaded projection's `write()` gateway — which continues the stream at the version the
load observed, so concurrent commands conflict instead of clobbering each other.
`#[evento::command]` generates routing-key variants from one method body:

```rust,no_run
use evento::{metadata::Event, Executor, Projection, ProjectionAggregate};

# #[evento::aggregate]
# pub enum BankAccount {
#     AccountOpened { initial_balance: i64 },
#     MoneyDeposited { amount: i64 },
# }
// The write model: `id = id` implements ProjectionAggregate (enables `write()`),
// `snapshot(memory)` keeps a process-local materialized row per aggregate.
#[evento::projection(id = id)]
#[evento::snapshot(memory)]
pub struct Account {
    pub id: String,
    pub balance: i64,
}

# #[evento::handler]
# async fn on_opened(event: Event<AccountOpened>, row: &mut Account) -> anyhow::Result<()> {
#     row.id = event.aggregate_id.to_owned();
#     row.balance = event.data.initial_balance;
#     Ok(())
# }
# #[evento::handler]
# async fn on_deposited(event: Event<MoneyDeposited>, row: &mut Account) -> anyhow::Result<()> {
#     row.balance += event.data.amount;
#     Ok(())
# }
fn account_projection<E: Executor>() -> Projection<E, Account> {
    Projection::new::<BankAccount>()
        .handler(on_opened())
        .handler(on_deposited())
        .strict() // fail on events nobody handles
}

pub struct Command<E: Executor>(pub E);

#[evento::command]
impl<E: Executor> Command<E> {
    /// Written once; the trailing `routing_key` parameter makes the macro
    /// generate `deposit_money(id, amount)`, `deposit_money_with_routing(id,
    /// amount, key)`, and `deposit_money_opt(id, amount, Option<String>)`.
    pub async fn deposit_money(
        &self,
        id: impl Into<String>,
        amount: i64,
        routing_key: Option<String>,
    ) -> anyhow::Result<()> {
        let Some(account) = account_projection().load(id).execute(&self.0).await? else {
            anyhow::bail!("account not found");
        };
        if amount <= 0 {
            anyhow::bail!("invalid amount");
        }

        account
            .write()?
            .routing_key_opt(routing_key)
            .event(&MoneyDeposited { amount })
            .commit(&self.0)
            .await?;
        Ok(())
    }
}

# async fn run<E: Executor>(cmd: Command<E>) -> anyhow::Result<()> {
cmd.deposit_money("account-1", 50).await?;
cmd.deposit_money_with_routing("account-1", 50, "eu-west").await?;
# Ok(())
# }
```

See [`examples/bank`](examples/bank) for the full pattern with domain errors and ten
commands.

### 5. Snapshots

Loading replays an aggregate's events; snapshots cut that short. Three modes:

- **Executor-backed** (default): derive `bitcode::Encode`/`bitcode::Decode` on the
  projection (`#[evento::projection(bitcode::Encode, bitcode::Decode)]`) and the
  snapshot is persisted in the event store.
- **`#[evento::snapshot(memory)]`**: a process-local table keyed by aggregate id, with
  a `snapshot_rows()` accessor for reading materialized rows.
- **`#[evento::snapshot(none)]`**: opt out — always replay from scratch.

```rust
#[evento::projection]
#[evento::snapshot(memory)]
pub struct MemView {
    pub id: String,
    pub balance: i64,
}

#[evento::projection]
#[evento::snapshot(none)]
pub struct StatusView {
    pub frozen: bool,
}

let rows = MemView::snapshot_rows().read().unwrap();
# drop(rows);
```

### 6. Subscriptions

Process events continuously (side effects allowed), with cursor tracking, retries, and
graceful shutdown:

```rust,no_run
use evento::{metadata::Event, subscription::{Context, SubscriptionBuilder}, Executor};

# #[evento::aggregate]
# pub enum BankAccount {
#     MoneyDeposited { amount: i64 },
# }
#[evento::subscription]
async fn notify<E: Executor>(
    _ctx: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    println!("deposited {}", event.data.amount);
    Ok(())
}

# async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
let subscription = SubscriptionBuilder::new("deposit-notifier")
    .handler(notify())
    .routing_key("accounts")
    .chunk_size(100)
    .retry(5)
    .start(executor)
    .await?;

// On application shutdown
subscription.shutdown().await?;
# Ok(())
# }
```

To drain currently-pending events once instead of running a background loop, use
`run_once(&executor)` (optionally after `no_retry()`). To keep a projection
auto-updated, use `projection.subscription("key").start(&executor)`. Handlers for all
events of an aggregate without deserializing go through `#[evento::subscription_all]`
with `RawEvent<A>`.

## Wiring a backend

### Fjall (embedded, zero setup)

```rust,no_run
# fn run() -> anyhow::Result<()> {
let executor = evento::Fjall::open("./data")?;
# let _ = executor;
# Ok(())
# }
```

### SQLite (or PostgreSQL/MySQL) with migrations

```rust,no_run
use evento::migrator::{Migrate, Plan};
use sqlx::sqlite::SqlitePoolOptions;

# async fn run() -> anyhow::Result<()> {
let pool = SqlitePoolOptions::new().connect("sqlite:events.db").await?;

// Run migrations (generic over the database type)
let mut conn = pool.acquire().await?;
evento::sql_migrator::new::<sqlx::Sqlite>()?
    .run(&mut *conn, &Plan::apply_all())
    .await?;
drop(conn);

let executor: evento::Sqlite = pool.into();
# let _ = executor;
# Ok(())
# }
```

### Remote (client/server split)

Serve any executor over framed TCP; the client implements `Executor`, so commands,
projections, and subscriptions work unchanged across the network:

```rust,no_run
# async fn run() -> anyhow::Result<()> {
// Server process
let executor = evento::Fjall::open("./data")?;
let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
let handle = evento::remote::serve(listener, executor);

// Client process
let client = evento::RemoteClient::connect("127.0.0.1:4321".parse()?).await?;
# let _ = (handle, client);
# Ok(())
# }
```

### Accord (replicated, alpha)

[evento-accord](evento-accord) replicates writes through the Accord consensus protocol
(Cassandra CEP-15): leaderless, strictly serializable, highly available, with any local
backend (Fjall/SQL) serving reads. See its [README](evento-accord/README.md),
[DESIGN.md](evento-accord/DESIGN.md), and [OPERATIONS.md](evento-accord/OPERATIONS.md),
plus the [`bank-axum-accord`](examples/bank-axum-accord) 3-node demo.

## Core API at a glance

| Concern | Entry point |
|---------|-------------|
| Define events | `#[evento::aggregate] enum` |
| Start a new aggregate | `evento::create()` → `WriteBuilder` |
| Append to an aggregate | `evento::append(id)` → `WriteBuilder` |
| Command with routing variants | `#[evento::command] impl Command<E>` |
| Read model | `#[evento::projection]` + `#[evento::handler]` fns |
| Emit events from loaded state | `#[evento::projection(id = ...)]` → `view.write()?` |
| Snapshot strategy | bitcode derives / `#[evento::snapshot(memory)]` / `#[evento::snapshot(none)]` |
| Load a read model | `Projection::new::<A>().handler(..).load(id).execute(exec)` |
| Co-keyed secondary aggregate | `.load(id).aggregate::<Other>(other_id)` |
| Filter events when reading | `EventFilter::by_type / by_id / by_event / exact` |
| Continuous processing | `SubscriptionBuilder::new(key)...start(exec)` |
| One-shot processing | `SubscriptionBuilder::new(key)...run_once(exec)` |
| Keep a projection updated | `projection.subscription(key).start(exec)` |
| Fail on unhandled events | `.strict()` |
| Keep going after a handler error | `.continue_on_error()` |

Full macro reference: [evento-macro/README.md](evento-macro/README.md).

## Feature flags

- `macro` *(default)* - Procedural macros for aggregates and handlers
- `sql` - Enable all SQL database backends
- `sqlite` / `postgres` / `mysql` - Individual SQL backends with migrations
- `fjall` - Embedded key-value storage with Fjall
- `remote` - Client/server executor over framed TCP
- `group` - Multi-executor support for querying across databases
- `rw` - Read-write split executor for CQRS patterns

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [evento](evento) | Facade: re-exports core + feature-gated backends |
| [evento-core](evento-core) | `Executor` trait, write path, projections, subscriptions |
| [evento-macro](evento-macro) | Procedural macros |
| [evento-sql](evento-sql) | SQLite/MySQL/PostgreSQL executor (sqlx) |
| [evento-sql-migrator](evento-sql-migrator) | Schema migrations for the SQL backend |
| [evento-fjall](evento-fjall) | Embedded LSM-tree executor |
| [evento-remote](evento-remote) | Client/server executor over framed TCP |
| [evento-accord](evento-accord) | Accord consensus replicated executor (alpha) |

## Examples

Complete working examples in [`examples/`](examples):

- [`quickstart`](examples/quickstart) - Smallest end-to-end run (Fjall): `cargo run -p quickstart`
- [`bank`](examples/bank) - Bank domain: aggregates, ten commands, projections, snapshots
- [`bank-axum-sqlite`](examples/bank-axum-sqlite) - Axum + SQLite + migrations: `cargo run -p bank-axum-sqlite`
- [`bank-axum-fjall`](examples/bank-axum-fjall) - Axum + embedded Fjall: `cargo run -p bank-axum-fjall`
- [`bank-axum-remote`](examples/bank-axum-remote) - Two-process client/server split: `cargo run -p bank-axum-remote -- store` then `cargo run -p bank-axum-remote`
- [`bank-axum-accord`](examples/bank-axum-accord) - 1- or 3-node Accord cluster: `make accord` or `make accord.cluster`

## License

Licensed under the Apache License, Version 2.0.
