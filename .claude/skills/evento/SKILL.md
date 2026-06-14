---
name: evento
description: >-
  Build event-sourcing / CQRS / DDD features in Rust with the `evento` crate.
  Use when defining aggregates and events, writing commands that commit events,
  building projections/read models, wiring subscriptions, or setting up the
  SQL/Fjall executor and migrations. Triggers on `#[evento::aggregate]`,
  `evento::create`/`evento::append`, `Projection`, `SubscriptionBuilder`,
  `Executor`, or a Cargo dependency on `evento`.
---

# Evento — event sourcing in Rust

Evento stores state changes as immutable events and folds them into read models.
Backends: SQLite / PostgreSQL / MySQL (via `sqlx`) and embedded Fjall. Events are
serialized with `bitcode`.

## Setup

```toml
[dependencies]
evento = { version = "2", features = ["sqlite"] }  # or "postgres", "mysql", "fjall"
bitcode = "0.6"
anyhow = "1"
tokio = { version = "1", features = ["full"] }
```

Feature flags: `sqlite` / `postgres` / `mysql` / `fjall`, `sql` (all SQL),
`macro` (default, the proc-macros), `group` (multi-executor), `rw` (read/write split).

### Executor + migrations (SQL)

```rust
let pool = sqlx::SqlitePool::connect("sqlite:events.db").await?;
let mut conn = pool.acquire().await?;
// The database type is REQUIRED on `new`.
evento::sql_migrator::new::<sqlx::Sqlite>()?
    .run(&mut *conn, &evento::migrator::Plan::apply_all())
    .await?;
drop(conn);
let executor: evento::Sqlite = pool.into();   // also: evento::Postgres / evento::MySql
```

### Executor (Fjall, embedded — no migrations)

```rust
let executor = evento::Fjall::open("./events.db")?;
```

## 1. Define aggregates & events

One enum per aggregate; each variant becomes a generated event struct with the
`Aggregate` + `AggregateEvent` traits and bitcode derives. The aggregate *type*
is `"{crate_name}/{EnumName}"`.

```rust
#[evento::aggregate]
pub enum Account {
    AccountOpened { owner: String, initial_balance: i64 },
    MoneyDeposited { amount: i64 },
    MoneyWithdrawn { amount: i64 },
}
// Generated: structs `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`, and a
// unit struct `Account`. Pass extra derives: `#[evento::aggregate(serde::Serialize)]`.
```

## 2. Write events

`create()` starts a new aggregate (auto-generated ULID id, returned by `commit`).
`append(id)` continues an existing one. Use `original_version` for optimistic
concurrency — `commit` returns `WriteError::InvalidOriginalVersion` on a race.

```rust
use evento::metadata::Metadata;

let id = evento::create()
    .event(&AccountOpened { owner: "Alice".into(), initial_balance: 1000 })
    .metadata(&Metadata::default())   // or .requested_by("user-1")
    .routing_key("accounts")          // optional partition key; .routing_key_opt(Option<String>)
    .commit(&executor)
    .await?;

evento::append(&id)
    .original_version(1)              // current version before this commit
    .event(&MoneyDeposited { amount: 100 })
    .commit(&executor)
    .await?;
```

`WriteBuilder` methods: `.event(&D)`, `.metadata(key, &val)`, `.metadata_from(m)`,
`.requested_by(s)`, `.requested_as(s)`, `.routing_key(s)`, `.routing_key_opt(opt)`,
`.original_version(v)`, `.commit(&executor)`. Multiple `.event(..)` calls commit a batch.

## 3. Projections (read models)

`#[evento::projection]` adds a `cursor` field and implements `ProjectionCursor`.
`#[evento::handler]` turns an async fn into a pure handler. Handler signature is
`(event: Event<SomeEvent>, view: &mut View)` — note the event comes first.

```rust
use evento::{metadata::Event, projection::Projection};

#[evento::projection]
pub struct AccountView { pub owner: String, pub balance: i64 }

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

let view: Option<AccountView> = Projection::<_, AccountView>::new::<Account>()
    .handler(on_opened())
    .handler(on_deposited())
    .strict()              // fail if an event has no handler (default: skip unhandled)
    .load(&id)             // or .load_ids(vec![a, b]) to fold several aggregates into one view
    .execute(&executor)
    .await?;
```

`Projection` builder: `.handler(h)`, `.skip::<Ev>()`, `.data(v)`, `.revision(n)`
(bumps snapshot version → invalidates old snapshots), `.strict()`,
`.tombstone::<Ev>()` (an event that deletes the projection), then a terminal
`.load(id)` / `.load_ids(ids)` → `LoadBuilder`, or `.subscription(key)` →
`ProjectionSubscription`. On `LoadBuilder`, register related aggregates with
`.aggregate::<Other>(other_id)` and read them in a handler via `context.aggregate::<Other>()`.

### Snapshots

Any projection that is `bitcode::Encode + DecodeOwned` gets snapshots for free
(stored via the executor). Override the `Snapshot` trait for a custom store.

### Emitting events from a projection (command pattern)

Implement `ProjectionAggregate` and call `.write()` (the write gateway) to get a
`WriteBuilder` pre-filled with the aggregate id + version:

```rust
use evento::projection::ProjectionAggregate;

impl ProjectionAggregate for AccountView {
    fn aggregate_id(&self) -> String { /* a field populated from the first event */ self.owner.clone() }
}

// In a command: load -> validate -> write
let Some(view) = projection.load(&id).execute(&executor).await? else {
    anyhow::bail!("not found");
};
view.write()?                              // pre-configured id + original_version
    .event(&MoneyWithdrawn { amount: 50 })
    .commit(&executor)
    .await?;
```

`aggregate_id()` is **required** (no default). `aggregate_version()` comes from the cursor.

## 4. Subscriptions (continuous side effects)

`#[evento::subscription]` handlers take the context first, then the event:
`(context: &Context<'_, E>, event: Event<SomeEvent>)`. They may do side effects
(read models, notifications) via `context.executor` and `context.extract::<Data<T>>()`.

```rust
use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};

#[evento::subscription]
async fn notify_deposit<E: Executor>(
    _ctx: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    println!("deposit: {}", event.data.amount);
    Ok(())
}

let sub = SubscriptionBuilder::<evento::Sqlite>::new("notifier") // unique key = cursor scope
    .handler(notify_deposit())
    .routing_key("accounts")   // or .all() for every routing key; default = only NULL routing key
    .chunk_size(100)
    .retry(5)                  // exponential backoff; .no_retry() to disable
    .start(&executor)          // background task; returns a handle
    .await?;
// ...later:
sub.shutdown().await?;
```

- `.start(exec)` runs a background loop; `.run_once(exec)` drains pending events once and returns.
- `.strict()` fails on an unhandled event; `.continue_on_error()` keeps going after a handler error.
- Process **all** raw events of an aggregate (no payload deserialization) with
  `#[evento::subscription_all]` + `event: evento::metadata::RawEvent<Account>`.
- Keep a projection auto-updated: `Projection::new::<A>().handler(..).subscription("key").start(&exec)`.

## 5. Reading events directly

```rust
use evento::{EventFilter, cursor::Args};

let page = executor.read(
    Some(vec![EventFilter::by_id("mycrate/Account", &id)]),  // by_type / by_event / exact
    None,                                                    // routing key filter
    Args::forward(50, None),                                 // cursor pagination
).await?;
```

`EventFilter`: `by_type(type)`, `by_id(type, id)`, `by_event(type, name)`,
`exact(type, id, name)`. The ext trait `AggregateExt` adds
`executor.has_event::<Ev>(id)` and `executor.original_version::<Ev>(id)`.

## Gotchas

- **Routing keys scope subscriptions.** A subscription with no `.routing_key()`/`.all()`
  only sees events whose routing key is NULL. `.all()` subscriptions are stored per
  executor-default-routing-key, so multi-tenant setups stay isolated.
- **The subscription key is the cursor identity.** Reusing a key across two different
  subscriptions makes them share (and corrupt) one cursor. Keep keys unique.
- **Optimistic concurrency:** always pass the correct `original_version` to `append`;
  handle `WriteError::InvalidOriginalVersion` (retry by reloading).
- **Handler order doesn't matter, coverage does.** Unhandled events are silently
  skipped unless you call `.strict()`.
- **Don't `.unwrap()` a load.** `projection.load(id).execute(exec).await` returns
  `anyhow::Result<Option<T>>`; propagate the error with `?` and treat `None` as not-found.

## Reference

Canonical, compiling usage lives in this repo:
- `examples/bank/` — aggregates, commands (write path), queries/projections, snapshots.
- `examples/bank-axum-sqlite/` and `examples/bank-axum-fjall/` — web wiring + migrations.
- `evento-test/src/lib.rs` — the behavioral contract suite (load, routing isolation,
  snapshots, optimistic locking, multi-aggregate, full command lifecycle).
