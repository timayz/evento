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
| Fjall (embedded LSM-tree) | `fjall` | [evento-fjall](evento-fjall) | no external server; `Fjall::temporary()` for tests |
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
tracing-subscriber = "0.3"
```

Swap `sqlite` for `postgres`, `mysql`, `fjall`, or `remote` as needed (see
[Feature flags](#feature-flags)).

Evento reports itself through [`tracing`](https://crates.io/crates/tracing), which does
nothing until the application installs a subscriber — so install one in `main` before
anything else:

```rust,no_run
fn main() {
    // Without this, every evento log — including the error a failing
    // subscription emits on its way out — is silently discarded.
    // `RUST_LOG=evento_core=debug` turns up the volume.
    tracing_subscriber::fmt::init();
}
```

The default level is `info`. Add `features = ["env-filter"]` and
`.with_env_filter(..)` when a dependency is chatty at that level — the embedded
Fjall store is, so every app under [`examples/`](#examples) does exactly that.

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

Additional derives are passed the same way and combine with `name =` in any
order. They land on every generated event struct, so putting an event on the
wire as JSON needs no hand-written DTO:

```rust
#[evento::aggregate(name = "bank/BankAccount", serde::Serialize, serde::Deserialize)]
pub enum BankAccount {
    AccountOpened { owner: String },
}
```

Each variant is also collected into a sibling `{Enum}Event` enum, so a stored
event can be matched exhaustively on the way back out — to SSE, webhooks, an
outbox table or an audit log — instead of laddering over `event.name`:

```rust
# use evento::Aggregate;
# #[evento::aggregate(name = "bank/BankAccount")]
# pub enum BankAccount {
#     AccountOpened { owner: String, initial_balance: i64 },
#     MoneyDeposited { amount: i64 },
# }
# let event = evento::Event {
#     aggregate_type: BankAccount::aggregate_type().to_owned(),
#     name: "MoneyDeposited".to_owned(),
#     data: bitcode::encode(&MoneyDeposited { amount: 100 }),
#     ..Default::default()
# };
match BankAccountEvent::try_from(&event)? {
    BankAccountEvent::AccountOpened(AccountOpened { owner, .. }) => println!("opened by {owner}"),
    BankAccountEvent::MoneyDeposited(d) => println!("deposit {}", d.amount),
}
# Ok::<(), evento::FromEventError>(())
```

Adding a variant now breaks every match site at compile time. Events decode
verbatim: `upcast_to` is not applied here, so an old stored event decodes to its
own variant. Derives passed to the attribute land on this enum too, so
`serde::Serialize` is enough to forward a whole decoded event.

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
  snapshot is persisted in the event store, keyed by
  `(aggregate type, projection name, aggregate id)` — so an aggregate can have any
  number of snapshotted views. The projection name defaults to
  `"<module path>::<Struct>"`; pin it with `name = "..."` so that renaming or moving
  the struct does not orphan its snapshots.
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

// Executor-backed, with a name that survives refactors:
#[evento::projection(name = "myapp/BalanceView", bitcode::Encode, bitcode::Decode)]
pub struct BalanceView {
    pub balance: i64,
}
```

Changing the shape of an executor-backed projection needs a `.revision(n)` bump on its
`Projection`, so snapshots taken with the old shape are dropped instead of mis-decoded.
A stored snapshot that no longer decodes is treated as a miss and rebuilt from events.

### 6. Subscriptions

Process events continuously (side effects allowed), with cursor tracking, retries, and
graceful shutdown:

```rust,no_run
use evento::{metadata::Event, subscription::{Context, SubscriptionBuilder}, Executor};

# #[evento::aggregate]
# pub enum BankAccount {
#     MoneyDeposited { amount: i64 },
# }
#[derive(Clone)]
pub struct Smtp { /* … */ }

#[evento::subscription]
async fn notify<E: Executor>(
    ctx: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    // Shared data comes back under the type it was registered with
    let _smtp: Smtp = ctx.extract();
    println!("deposited {}", event.data.amount);
    Ok(())
}

# async fn run<E: Executor + Clone>(executor: &E, smtp: Smtp) -> anyhow::Result<()> {
let subscription = SubscriptionBuilder::new("deposit-notifier")
    .data(smtp)
    .handler(notify())
    .routing_key("accounts")
    .chunk_size(100)
    .retry(5)
    .start(executor)
    .await?;

// A subscription can also stop on its own — supervise it.
tokio::select! {
    _ = tokio::signal::ctrl_c() => {}
    reason = subscription.stopped() => {
        tracing::error!(%reason, "subscription stopped, no longer processing events");
    }
}

// On application shutdown
subscription.shutdown().await?;
# Ok(())
# }
```

By default a subscription **stops on the first handler error** —
`.continue_on_error()` is opt-in — and a stopped worker never processes another event.
The handle is how you find out: `stopped()` resolves with a
[`StopReason`](https://docs.rs/evento/latest/evento/subscription/enum.StopReason.html)
(`Failed`, `LostOwnership`, `Shutdown`, `StoppedByHandler`, `Panicked`), `stop_reason()`
is the non-blocking peek, and `stop()` signals a worker held behind an `Arc`. Combined with
the subscriber above, a broken handler is visible in minute one instead of hour two.

`.data(v)` stores `v` under its own type; a handler reads it back with
`ctx.extract::<T>()`, or `ctx.try_extract::<T>()?` to get an error instead of a panic
when it was never registered. Extraction clones, so the type should be `Clone` and cheap
to clone — wrap anything else in `evento::context::Data` and extract it as `Data<T>`.

#### Live bridges (SSE, WebSocket, fanout)

Forwarding events to a connection inverts every default above. History is noise to a client
that just connected, the cursor is not worth a database write, and the handler — not a
supervisor — is often the first thing to learn the consumer is gone. Three opt-ins cover it:

```rust,no_run
# use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};
# #[evento::aggregate]
# pub enum BankAccount { MoneyDeposited { amount: i64 } }
#[evento::subscription]
async fn fanout<E: Executor>(
    ctx: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    let tx: tokio::sync::broadcast::Sender<i64> = ctx.extract();
    // No receivers left: every client disconnected, so there is nothing to bridge to.
    if tx.send(event.data.amount).is_err() {
        ctx.stop();
    }
    Ok(())
}

# async fn run<E: Executor + Clone>(
#     executor: &E,
#     tx: tokio::sync::broadcast::Sender<i64>,
# ) -> anyhow::Result<()> {
let subscription = SubscriptionBuilder::new("sse")
    .handler(fanout())
    .data(tx)
    // `.ephemeral().start_from_latest().start(executor)` in one call
    .live(executor)
    .await?;
# subscription.shutdown().await?;
# Ok(())
# }
```

`.ephemeral()` keeps the cursor in memory, so the key stops being an identity: any number
of connections can share one, which is the whole point when there is a subscription per
connection. `.start_from_latest()` applies only when there is no cursor yet, so a *durable*
subscription still resumes on restart rather than jumping forward. `ctx.stop()` ends the
worker with `StopReason::StoppedByHandler` — a normal end, not a failure.

Reach past `.live()` for the combinations it does not cover: `.ephemeral()` on its own is a
throwaway in-memory index rebuilt from the whole stream on every boot, and
`.start_from_latest()` on its own is a durable subscription that skips history on its
*first* start and resumes normally after.

Each subscription is its own poller. One per connection earns its cost when each wants a
different slice (`.aggregate::<A>(id)`, `.routing_key(tenant)`); for an unfiltered global
feed run **one** of them fanning out over a broadcast channel, as above.
`examples/bank-axum-fjall` serves a per-account SSE feed this way.

To drain currently-pending events once instead of running a background loop, use
`run_once(&executor)` (optionally after `no_retry()`). To keep a projection
auto-updated, use `projection.subscription("key").start(&executor)`. Handlers for all
events of an aggregate without deserializing go through `#[evento::subscription_all]`
with `RawEvent<A>`, whose `.decode()` yields the same `{Enum}Event` when you do
want it typed.

### 7. Evolving events

A stored event never changes: bitcode is positional, so its layout is frozen once a
database holds one. When an event needs a new shape, add a new variant and point the
old one at it. Older stored events are then converted **before** handlers see them, so
only the newest handler has to exist:

```rust
use evento::{metadata::Event, projection::Projection, Executor};

#[evento::aggregate(name = "myapp/Payment")]
pub enum Payment {
    // Still in old streams, no longer written. Keep it: it is the decode schema.
    #[evento(upcast_to = PaymentRefundedV2)]
    PaymentRefunded { amount: i64 },
    PaymentRefundedV2 { amount: i64, reason: String },
}

impl From<PaymentRefunded> for PaymentRefundedV2 {
    fn from(old: PaymentRefunded) -> Self {
        Self { amount: old.amount, reason: "unknown".to_owned() }
    }
}

#[evento::projection]
#[evento::snapshot(none)]
pub struct RefundsView {
    pub total: i64,
}

#[evento::handler]
async fn on_refunded(event: Event<PaymentRefundedV2>, view: &mut RefundsView) -> anyhow::Result<()> {
    view.total += event.data.amount;
    Ok(())
}

fn refunds<E: Executor>() -> Projection<E, RefundsView> {
    // The only handler: it receives `PaymentRefunded` events too, upcast.
    Projection::new::<Payment>().handler(on_refunded()).strict()
}
```

- It is declared once, on the aggregate. Every `Projection` and `SubscriptionBuilder`
  that registers a handler — or a `.skip::<New>()` — for the newer event picks it up;
  `tombstone::<New>()` and `has_event::<New>()` follow the older names as well.
- Chains work (`V1 -> V2 -> V3`) and are folded into one decode and one encode. With
  handlers for both `V2` and `V3`, a `V1` event goes to the nearest one.
- A handler registered for the older event itself wins over the upcast, so consumers
  can migrate one at a time.
- The handler sees the newer event: `event.name` and `event.data` are the newer ones,
  everything else (id, version, timestamp, metadata) is the stored event's.
  `#[evento::subscription_all]` handlers keep receiving stored events as they are.
- Snapshots hold folded state, not events: if the conversion yields different state
  than the handler you removed did, bump the projection's `.revision(n)`.

#### Locking persisted shapes

Nothing in the compiler stops someone from editing a variant, or `Money`, or a
snapshotted view. [`evento-lock`](evento-lock) does: it scans the workspace with
`syn`, writes every persisted shape to `events.lock`, and fails a test when a line
that is already there changes.

```text
event bank/BankAccount::MoneyDeposited { amount: i64, transaction_id: String, description: String }
type bank::value_object::AccountType enum { Checking, Savings, Business }
view bank::query::account_balance::AccountBalanceView rev=0 { balance: i64, …, cursor: String, aggregate_version: u16 }
```

- `event` lines (keyed by the stored name) and `type` lines (every `Encode` type an
  event reaches, enums included: appending a variant changes the packed discriminant)
  are **frozen**. A new variant, companion event or upcast target is a new line.
- `view` lines (projections snapshotted through the executor) **may change** when their
  projection's `.revision(n)` grows in the same commit.
- Write-side `#[evento::snapshot(none | memory)]` state and SQL read models are not
  persisted as bitcode, so they are not in the lock: they change freely.

```rust,ignore
// tests/events_lock.rs, with evento-lock as a dev-dependency
#[test]
fn persisted_shapes_only_grow() {
    evento_lock::check(env!("CARGO_MANIFEST_DIR")).unwrap();
}
```

Run `EVENTO_LOCK=update cargo test` after adding events (it refuses breaking changes),
and `EVENTO_LOCK=force` only for shapes no deployed database has ever stored.

### 8. Reading events directly

Projections fold events into state. When you want the events themselves — to feed an
SSE stream, a webhook, an outbox row or an audit log — read the stream directly:

```rust,no_run
# #[evento::aggregate]
# pub enum Account {
#     AccountOpened { owner: String },
#     MoneyDeposited { amount: i64 },
# }
# async fn run<E: evento::Executor>(executor: &E, id: &str) -> anyhow::Result<()> {
// The whole stream, oldest first. Pages are drained internally.
let events: Vec<evento::Event> = evento::read::<Account>(id).execute(executor).await?;

// Typed and exhaustively matchable — a new variant becomes a compile error.
for event in evento::read::<Account>(id).decode(executor).await? {
    match event {
        AccountEvent::AccountOpened(opened) => println!("opened by {}", opened.owner),
        AccountEvent::MoneyDeposited(d) => println!("+{}", d.amount),
    }
}

// The last 10 events, still oldest-first, and just the deposits.
let recent = evento::read::<Account>(id).backward().limit(10).execute(executor).await?;
let deposits = evento::read::<Account>(id).event::<MoneyDeposited>().execute(executor).await?;
# let _ = (events, recent, deposits);
# Ok(())
# }
```

`limit(n)` caps the **total** number of events, not a page size, so a plain `execute()`
never silently truncates a stream. When you want to drive pagination yourself — a
GraphQL connection, an infinite scroll — `page()` returns one page plus the cursors to
continue from:

```rust,no_run
# #[evento::aggregate]
# pub enum Account {
#     AccountOpened { owner: String },
# }
# async fn run<E: evento::Executor>(executor: &E, id: &str) -> anyhow::Result<()> {
let page = evento::read::<Account>(id).limit(50).page(executor).await?;
if let Some(cursor) = page.page_info.end_cursor {
    let next = evento::read::<Account>(id).limit(50).after(cursor).page(executor).await?;
    let _ = next;
}
# Ok(())
# }
```

Unlike a subscription, a reader with no `routing_key(..)` reads events under **every**
routing key; `no_routing_key()` narrows it to events committed without one. Use
`read_raw(type, id)` when the aggregate type is only known as a string, and drop to
`executor.read(..)` with hand-built `EventFilter`s for queries spanning several
aggregates.

## Wiring a backend

### Fjall (embedded, zero setup)

```rust,no_run
# fn run() -> anyhow::Result<()> {
let executor = evento::Fjall::open("./data")?;

// Tests, examples, experiments: a temp directory the executor owns and removes
// when its last clone drops — no path to pick, nothing to clean up.
let ephemeral = evento::Fjall::temporary()?;
# let _ = (executor, ephemeral);
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
| Decode a stored event | `{Enum}Event::try_from(&event)?` |
| Start a new aggregate | `evento::create()` → `WriteBuilder` |
| Append to an aggregate | `evento::append(id)` → `WriteBuilder` |
| Command with routing variants | `#[evento::command] impl Command<E>` |
| Read model | `#[evento::projection]` + `#[evento::handler]` fns |
| Emit events from loaded state | `#[evento::projection(id = ...)]` → `view.write()?` |
| Snapshot strategy | bitcode derives / `#[evento::snapshot(memory)]` / `#[evento::snapshot(none)]` |
| Load a read model | `Projection::new::<A>().handler(..).load(id).execute(exec)` |
| Co-keyed secondary aggregate | `.load(id).aggregate::<Other>(other_id)` |
| Read an aggregate's events | `evento::read::<A>(id).execute(exec)` |
| Read them typed | `evento::read::<A>(id).decode(exec)` |
| Paginate a read | `.limit(n)` / `.after(cursor)` / `.page(exec)` |
| Filter events when reading | `EventFilter::by_type::<A>() / by_id::<A>(id) / by_event::<Ev>() / exact::<Ev>(id)` |
| Continuous processing | `SubscriptionBuilder::new(key)...start(exec)` |
| One-shot processing | `SubscriptionBuilder::new(key)...run_once(exec)` |
| Keep a projection updated | `projection.subscription(key).start(exec)` |
| Fail on unhandled events | `.strict()` |
| Keep going after a handler error | `.continue_on_error()` |
| Notice a stopped subscription | `subscription.stopped().await` → `StopReason` |
| Live bridge (SSE/WebSocket) | `.live(exec)` = `.ephemeral().start_from_latest().start(exec)` |
| Skip history on a new subscription | `.start_from_latest()` |
| Stop a subscription from inside a handler | `ctx.stop()` |

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
| [evento-lock](evento-lock) | `events.lock`: test that persisted shapes only grow |

## Examples

Complete working examples in [`examples/`](examples):

- [`quickstart`](examples/quickstart) - Smallest end-to-end run (Fjall): `cargo run -p quickstart`
- [`bank`](examples/bank) - Bank domain: aggregates, ten commands, projections, snapshots
- [`bank-axum-sqlite`](examples/bank-axum-sqlite) - Axum + SQLite + migrations: `cargo run -p bank-axum-sqlite`
- [`bank-axum-fjall`](examples/bank-axum-fjall) - Axum + embedded Fjall, plus a live SSE feed per account: `cargo run -p bank-axum-fjall`
- [`bank-axum-remote`](examples/bank-axum-remote) - Two-process client/server split: `cargo run -p bank-axum-remote -- store` then `cargo run -p bank-axum-remote`
- [`bank-axum-accord`](examples/bank-axum-accord) - 1- or 3-node Accord cluster: `make accord` or `make accord.cluster`

## License

Licensed under the Apache License, Version 2.0.
