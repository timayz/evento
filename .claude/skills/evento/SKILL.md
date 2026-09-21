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
Backends: SQLite / PostgreSQL / MySQL (via `sqlx`), embedded Fjall, a remote
client/server executor (`remote`), and a consensus-replicated executor
(`evento-accord`, alpha). Events are serialized with `bitcode`.

## Setup

2.x is a pre-release — pin the exact version (a bare `"2"` won't resolve):

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["sqlite"] }  # or "postgres", "mysql", "fjall", "remote"
bitcode = "0.6"
anyhow = "1"
tokio = { version = "1", features = ["full"] }
```

Feature flags: `sqlite` / `postgres` / `mysql` / `fjall` / `remote`, `sql` (all SQL),
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

// Tests/examples: a temp store, removed when the last clone of the executor drops.
let executor = evento::Fjall::temporary()?;
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
// Generated: structs `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`, a
// unit struct `Account`, and an enum `AccountEvent` newtyping those structs.
// Pass extra derives: `#[evento::aggregate(serde::Serialize)]` (they also apply
// to `AccountEvent`).
```

**Going back out.** `AccountEvent::try_from(&event)?` turns a stored
`evento::Event` into an exhaustively matchable value — use it for SSE, webhooks,
outbox rows and audit logs instead of matching on `event.name`, so a new variant
is a compile error rather than a silent `_` fallthrough. `.event_name()` gives
the stored name back; errors are `evento::FromEventError`. Decoding is verbatim,
so an `upcast_to` predecessor decodes to its *own* variant.

```rust
match AccountEvent::try_from(&event)? {
    AccountEvent::AccountOpened(AccountOpened { owner, .. }) => sse.send(owner),
    AccountEvent::MoneyDeposited(d) => sse.send(d.amount),
    AccountEvent::MoneyWithdrawn(_) => {}
}
```

Pin on-disk identities so refactors never orphan stored events:
`#[evento::aggregate(name = "bank/BankAccount")]` on the enum,
`#[evento(name = "opened.v1")]` on a variant.

**Evolving an event.** A stored event's layout is frozen (bitcode is positional):
never edit a variant that a database may hold. Add a new variant and upcast the
old one to it — old stored events are converted before handlers see them, so only
the newest handler is needed:

```rust
#[evento::aggregate(name = "myapp/Payment")]
pub enum Payment {
    #[evento(upcast_to = PaymentRefundedV2)]   // keep: it is the decode schema
    PaymentRefunded { amount: i64 },
    PaymentRefundedV2 { amount: i64, reason: String },
}
impl From<PaymentRefunded> for PaymentRefundedV2 { /* fill the new fields */ }
```

Declared once on the aggregate; every `Projection`/`SubscriptionBuilder` that
registers `.handler(..)` or `.skip::<PaymentRefundedV2>()` also accepts
`PaymentRefunded` (`tombstone::<New>()` and `has_event::<New>()` too). Chains
(`V1 -> V2 -> V3`) work; a handler for the old event itself still wins, so
consumers migrate one at a time. `#[evento::subscription_all]` sees stored
events un-upcast.

**Locking shapes.** `evento-lock` (dev-dependency) records every event, every
`Encode` type an event reaches, and every executor-snapshotted view in
`events.lock`; a test (`evento_lock::check(env!("CARGO_MANIFEST_DIR")).unwrap()`)
fails when a frozen line changes. After adding an event run
`EVENTO_LOCK=update cargo test` and commit `events.lock`. A failing lock means:
restore the edit and add a variant, or bump the view's `.revision(n)`.

## 2. Write events

`create()` starts a new aggregate (auto-generated ULID id, returned by `commit`).
`append(id)` continues an existing one. Use `original_version` for optimistic
concurrency — `commit` returns `WriteError::InvalidOriginalVersion` on a race.

```rust
let id = evento::create()
    .event(&AccountOpened { owner: "Alice".into(), initial_balance: 1000 })
    .metadata("request_id", &req_id)  // key/value; or .requested_by("user-1") / .metadata_from(m)
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
Options: `cursor = <Type>` (custom cursor type, e.g. `evento::cursor::Value`);
`id = <field>` (also implements `ProjectionAggregate` → enables `view.write()`);
extra derive paths (e.g. `bitcode::Encode, bitcode::Decode` for executor-backed
snapshots). `#[evento::handler]` turns an async fn into a pure handler. Handler
signature is `(event: Event<SomeEvent>, view: &mut View)` — the event comes first.

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

`Projection` builder: `.handler(h)`, `.skip::<Ev>()`, `.data(v)` (reaches hand-written
`Snapshot` impls, not handlers — projection handlers take no context), `.revision(n)`
(bumps snapshot version → invalidates old snapshots), `.strict()`,
`.tombstone::<Ev>()` (an event that deletes the projection), then a terminal
`.load(id)` / `.load_ids(ids)` → `LoadBuilder`, or `.subscription(key)` →
`ProjectionSubscription`. On `LoadBuilder`, register related aggregates with
`.aggregate::<Other>(other_id)` and read them in a handler via `context.aggregate::<Other>()`.

### Snapshots — three modes

- **Executor-backed** (default): give the projection bitcode derives —
  `#[evento::projection(bitcode::Encode, bitcode::Decode)]` — and snapshots are
  persisted via the executor (blanket `Snapshot` impl), keyed by
  `(aggregate type, projection name, id)`: several snapshotted views of one
  aggregate are fine. The name defaults to `"<module path>::<Struct>"`; pin it
  with `#[evento::projection(name = "myapp/View", ...)]` so a rename/move does
  not orphan the stored snapshots. An undecodable snapshot is a cache miss.
- **`#[evento::snapshot(memory)]`**: process-local table keyed by aggregate id;
  read materialized rows with `View::snapshot_rows().read().unwrap()`.
- **`#[evento::snapshot(none)]`**: no snapshots, always replay.

Projections backed by a custom table (SQL, …) implement `Snapshot` by hand.

### Commands and the write gateway (`#[evento::command]`)

The write model is a projection with `id = <field>` (this implements
`ProjectionAggregate`, whose `write()` returns a `WriteBuilder` pre-filled with
the aggregate id + the version the load observed — optimistic concurrency for
free). `#[evento::command]` on an impl block turns each method with a trailing
`routing_key: Option<String>` parameter into three wrappers: `x(..)`,
`x_with_routing(.., key)`, `x_opt(.., Option<String>)`.

```rust
use evento::{Executor, Projection, ProjectionAggregate};

#[evento::projection(id = id)]
#[evento::snapshot(memory)]
pub struct BankAccount { pub id: String, pub balance: i64 /* … */ }

pub struct Command<E: Executor>(pub E);

#[evento::command]
impl<E: Executor> Command<E> {
    pub async fn withdraw_money(
        &self,
        id: impl Into<String>,
        amount: i64,
        routing_key: Option<String>,
    ) -> anyhow::Result<()> {
        // load -> guard -> write
        let Some(account) = account_projection().load(id).execute(&self.0).await? else {
            anyhow::bail!("not found");
        };
        if amount <= 0 { anyhow::bail!("invalid amount"); }
        account.write()?
            .routing_key_opt(routing_key)
            .event(&MoneyWithdrawn { amount })
            .commit(&self.0)
            .await?;
        Ok(())
    }
}
// Callers: cmd.withdraw_money(id, 50).await? / cmd.withdraw_money_with_routing(id, 50, "eu").await?
```

## 4. Subscriptions (continuous side effects)

`#[evento::subscription]` handlers take the context first, then the event:
`(context: &Context<'_, E>, event: Event<SomeEvent>)`. They may do side effects
(read models, notifications) via `context.executor` and `context.extract::<T>()` —
`.data(v)` stores `v` under its own type, and `extract` clones it, so `T` must be `Clone`
and cheap to clone; wrap anything else in `Data` and extract `Data<T>`.
`context.try_extract::<T>()?` returns a `MissingData` error instead of panicking.

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
    .routing_key("accounts")   // .any_routing_key() for every key; default = only NULL routing key
    .chunk_size(100)
    .retry(5)                  // exponential backoff; .no_retry() to disable
    .start(&executor)          // background task; returns a handle
    .await?;
// ...later:
sub.shutdown().await?;
```

- `.start(exec)` runs a background loop; `.run_once(exec)` drains pending events once and returns.
- `.strict()` fails on an unhandled event; `.continue_on_error()` keeps going after a handler error.
- **Live bridge (SSE, WebSocket, fanout):** `.live(exec)` is the whole shape in one call —
  `.ephemeral().start_from_latest().start(exec)`. `.ephemeral()` keeps the cursor in memory — no
  subscriber row, no fence, no acknowledge, reads only — and `.start_from_latest()` begins at
  the stream head instead of replaying history (it applies only when there is no cursor yet, so
  a durable subscription still resumes on restart); use them separately for the combinations
  `.live()` does not cover. A handler ends its own subscription with
  `ctx.stop()`, reporting `StopReason::StoppedByHandler` — a normal end, not a failure.
  Neither `.ephemeral()` nor `.start_from_latest()` is offered on `ProjectionSubscription`.
  One subscription per connection is worth it only when each wants a different slice
  (`.aggregate::<A>(id)`); otherwise run one fanning out over a `broadcast` channel.
- Process **all** raw events of an aggregate (no payload deserialization) with
  `#[evento::subscription_all]` + `event: evento::metadata::RawEvent<Account>`;
  call `event.decode()?` for the typed `AccountEvent` when you want it.
- Keep a projection auto-updated: `Projection::new::<A>().handler(..).subscription("key").start(&exec)`.

## 5. Reading events directly

`evento::read::<A>(id)` is the read-side counterpart of `Projection::load` — use it
when you want the events themselves (SSE, webhooks, outbox rows, audit logs).

```rust
// Whole stream, oldest first; pages are drained internally.
let events: Vec<evento::Event> = evento::read::<Account>(&id).execute(&executor).await?;

// Typed: exhaustive match, a new variant is a compile error.
for event in evento::read::<Account>(&id).decode(&executor).await? {
    match event {
        AccountEvent::MoneyDeposited(d) => sse.send(d.amount),
        _ => {}
    }
}
```

`ReadBuilder`: `.event::<Ev>()` (one event type), `.routing_key(k)` /
`.no_routing_key()`, `.limit(n)`, `.after(cursor)` / `.before(cursor)`,
`.backward()`, `.args(Args)`, then a terminal `.execute()` → `Vec<Event>`,
`.decode()` → `Vec<{Enum}Event>`, or `.page()` → `ReadResult<Event>` (one page +
`page_info` for cursor-driven callers). `evento::read_raw(type, id)` takes the
aggregate type as a string; it has no `.decode()`.

- **`.limit(n)` is a total, not a page size** — `.execute()` never silently truncates.
- **`.backward()` takes the tail** of the stream (GraphQL `last:`); events still come
  back oldest-first.
- Drop to `executor.read(filters, routing_key, args, to_micros)` only for queries
  spanning several aggregates. `EventFilter` constructors are typed —
  `by_type::<A>()`, `by_id::<A>(id)`, `by_event::<Ev>()`, `exact::<Ev>(id)` — each with
  a `*_raw` string form (`by_id_raw(type, id)`, …) for runtime types.
- The ext trait `AggregateExt` adds `executor.has_event::<Ev>(id)` and
  `executor.original_version::<Ev>(id)`.

## Gotchas

- **Readers and subscriptions default routing keys oppositely.** `evento::read(..)` with
  no `.routing_key()` reads **every** routing key; narrow it with `.no_routing_key()`.
  A subscription with no `.routing_key()`/`.any_routing_key()` only sees events whose
  routing key is NULL. `.any_routing_key()` subscriptions are stored per
  executor-default-routing-key, so multi-tenant setups stay isolated.
- **The subscription key is the cursor identity.** Reusing a key across two different
  subscriptions makes them share (and corrupt) one cursor. Keep keys unique — except under
  `.ephemeral()`, where nothing is stored under the key and any number of subscriptions may
  share one concurrently.
- **Optimistic concurrency:** always pass the correct `original_version` to `append`;
  handle `WriteError::InvalidOriginalVersion` (retry by reloading).
- **Handler order doesn't matter, coverage does.** Unhandled events are silently
  skipped unless you call `.strict()`. An older event declared with
  `#[evento(upcast_to = New)]` counts as handled when `New` is handled or skipped.
- **Upcasting changes what handlers fold, not what snapshots hold.** If the `From`
  conversion yields different state than the old handler you deleted, bump
  `.revision(n)` so stored snapshots are rebuilt.
- **Frozen means nested types too.** Adding a field to `Money`, or a variant to an
  enum used in an event (even at the end), breaks decoding as surely as editing the
  event. `events.lock` catches it; `EVENTO_LOCK=force` is only for never-deployed shapes.
- **Don't `.unwrap()` a load.** `projection.load(id).execute(exec).await` returns
  `anyhow::Result<Option<T>>`; propagate the error with `?` and treat `None` as not-found.

## Reference

Canonical, compiling usage lives in this repo:
- `examples/quickstart/` — smallest end-to-end run (aggregate → command → load → subscription).
- `examples/bank/` — aggregates, `#[evento::command]` commands, queries/projections
  (one per snapshot mode), co-keyed `Owner` aggregate.
- `examples/bank-axum-sqlite/` and `examples/bank-axum-fjall/` — web wiring,
  migrations, projection subscriptions; `bank-axum-remote/` (client/server split)
  and `bank-axum-accord/` (consensus cluster).
- `evento-macro/README.md` — full macro reference (all options).
- `evento-test/src/lib.rs` — the behavioral contract suite (load, routing isolation,
  snapshots, optimistic locking, multi-aggregate, full command lifecycle).
