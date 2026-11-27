---
name: evento
description: >-
  Build event-sourcing / CQRS / DDD features in Rust with the `evento` crate.
  Use when defining aggregates and events, writing commands that commit events,
  building read models (projections kept current by a projection subscription),
  wiring subscriptions for sagas and stats, or setting up the
  SQL/Fjall/Turso/remote executor and migrations. Triggers on `#[evento::aggregate]`,
  `evento::create`/`evento::append`, `Projection`, `Snapshot`, `SubscriptionBuilder`,
  `Executor`, or a Cargo dependency on `evento`.
---

# Evento — event sourcing in Rust

Evento stores state changes as immutable events and folds them into read models.
Backends: SQLite / PostgreSQL / MySQL (via `sqlx`), embedded Fjall, embedded Turso
(SQLite's schema in a pure-Rust async engine, `turso`), a remote client/server
executor (`remote`), and a consensus-replicated executor (`evento-cluster`, alpha). Events are serialized with `bitcode`.

## Setup

2.x is a pre-release — pin the exact version (a bare `"2"` won't resolve):

```toml
[dependencies]
evento = { version = "2.0.0-alpha.43", features = ["sqlite"] }  # or "postgres", "mysql", "fjall", "turso", "remote"
bitcode = "0.6"
anyhow = "1"
tokio = { version = "1", features = ["full"] }
```

Feature flags: `sqlite` / `postgres` / `mysql` / `fjall` / `turso` / `remote`, `sql` (all SQL),
`macro` (default, the proc-macros), `group` (multi-executor), `rw` (executor combinators:
`Rw` read/write split and `Sidecar`; implied by `fjall`).

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

### Executor (Turso, embedded SQL — migrations run on open)

```rust
let executor = evento::Turso::open("./events.db").await?;

// Tests/simulations: an in-memory database, gone when the last clone drops.
let executor = evento::Turso::in_memory().await?;

// Your own migrations share the database under your app name
// (evento's are the app "evento"):
const APP: evento::turso::migrate::Migrations = evento::turso::migrate::Migrations::new(
    "my-app", &[evento::turso::migrate::Migration::up("001_views", "CREATE TABLE ...")]);
let mut conn = executor.connect()?;
APP.to_latest(&mut conn).await.map_err(|(_, e)| e)?;
```

Several writers in one database (the store, its cluster journal, your read models) need
the store opened in concurrent mode — `evento::Turso::open_concurrent(path)` /
`in_memory_concurrent()` — which puts the file in Turso's MVCC mode (recorded in it; a
later plain `open` finds it, `store.write_mode()` tells). The store's and journal's writes
are then `BEGIN CONCURRENT` transactions retried on a write-write conflict, and your own
writes go through `store.transaction(&env, |tx, env| Box::pin(async move { … Ok(()) }))`:
the body is lent the transaction's connection and `env`, and must be repeatable (it is run
again whole on a conflict). A plain `open` keeps Turso's single-writer WAL mode, where a
second writer gets `Busy`. DDL needs a plain transaction even in MVCC mode: run migrations
before the writers start.

Read models on Turso mirror the SQL ones: `#[evento::projection(backend = turso,
evento::turso::FromRow, Cursor, Debug)]` derives the row decoding (`#[turso(default)]`
for a non-column field, `#[turso(rename = "col")]` for another column name; tuples such as
`(String, i64)` decode by position; `evento::turso::Text<T>` for an enum stored as text) and the
`#[cursor(by_name(Col::X, n))]` wrappers; `evento::turso::Reader::new(select).args(args)
.execute::<ViewByName>(&conn)` pages them; `evento::turso::{execute, fetch_all,
fetch_optional, fetch_one}` run sea-query statements on a `conn = executor.connect()?`.
Full-text search needs the `turso-fts` feature: it is an index, not an FTS5 table —
`CREATE INDEX t_fts ON t USING fts(a, b)`, `WHERE (a, b) MATCH ?`, and
`fts_score(a, b, ?1)` / `fts_match(a, b, ?1)` naming every index column, then the query.

### Remote (client/server split) and executor combinators

Serve any executor over framed TCP; `RemoteClient` implements `Executor`, so
commands, projections and subscriptions work unchanged across the network.

```rust
// Server process — `serve` is the open single-store case (tokens ignored).
let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
let handle = evento::remote::serve(listener, evento::Fjall::open("./data")?);
// Client process
let client = evento::RemoteClient::connect("127.0.0.1:4321").await?;
// handle.shutdown().await — dropping the handle does NOT stop the server.
```

- **Addresses:** `connect`/`builder` take `impl Into<RemoteAddress>`: a `SocketAddr`,
  `"127.0.0.1:4321"` or `"store.example.com:4321"`. A host name is looked up again
  on every (re)connect, so a client behind a load balancer or DNS failover follows
  it. The server clamps a bounded `read` to its own `stable_timestamp`, so a
  client that reconnects to a lagging node never skips events.
- **Store away at startup:** `connect()` fails when the server does not answer.
  `RemoteClient::builder(addr).connect_lazy()` gives the client at once and connects
  in the background: calls fail with `evento::remote::NotConnected` until the first
  `Hello` (which also brings the default routing key), a refused token is final
  (`ClientState::Refused`, calls fail with `Rejection::Unauthorized`), `client.state()`
  → `Connecting { error, since } | Connected | Refused`, `client.connected().await`
  waits. Start subscriptions on it `.supervised()`: an unsupervised `start` fails while
  the store is away, a supervised one starts and handles its first event when it comes.
- **Tenants / authentication:** implement `evento::remote::Resolver`.
  Every connection opens with a `Hello { token, peer }` (client side:
  `RemoteClient::builder(addr).token("…").connect().await?`); `resolve` answers
  `Session::new(executor)` — optionally `.keep(guard)` (dropped when the connection
  ends) and `.close_when(watch_rx)` (server-side kick on revocation) — or
  `Err(Rejection)`, which the client sees as its `connect` error.
  Pass it to `serve_with(listener, resolver)`. It runs again on every reconnect.
  To confine a client to its tenant over one shared store, serve
  `Session::new(store.clone()).tenant(tenant)?`: every request on the connection
  acts in that tenant whatever it names, the events it reads come back in the
  default tenant (the client sees a store of its own), and `replicate` and the
  subscriber calls are refused — a tenant's app keeps its subscriptions local
  (`Sidecar`).
- **TLS:** `serve_tls(listener, acceptor, exec)` / `serve_tls_with(listener, acceptor, resolver)`
  take a `tokio_rustls::TlsAcceptor`; the client side is
  `.builder(addr).tls(connector, server_name)`. Plain TCP is for trusted networks —
  a token travels in clear text there.
- **Limits (a listener anyone can dial):** `evento::remote::ServerBuilder::new(resolver)`
  (or `::open(exec)`) `.tls(acceptor).max_connections(n).max_pending(n).serve(listener)`.
  Before the resolver accepts a connection the server reads a `Hello` and nothing
  longer (16 KiB) and holds `max_pending` such connections (1024 by default);
  `max_connections` caps all of them (no cap by default). A dial past a cap is closed
  at once.
- **A client that runs its own subscriptions** (a replica of a web app following the
  shared store) must keep cursors and snapshots in a store it owns, or replicas fence
  each other out of one subscription key:

```rust
let client = evento::RemoteClient::connect("127.0.0.1:4321").await?;
let local = evento::Fjall::open("./replica-state")?;          // owned by THIS process only
let executor = evento::Sidecar::from((client, local));        // events: client; cursors/snapshots: local
```

- `Rw::from((read_exec, write_exec))` splits reads from writes (CQRS with two databases).
- `evento::Evento::new(exec)` type-erases any executor (`Arc<Box<dyn Executor>>`) —
  use it when a struct must hold "some executor" without a generic parameter.
- **Cluster (consensus-replicated, alpha):** the `cluster` feature (`evento::cluster`)
  replicates writes leaderlessly with any local backend serving reads, whose
  consensus journal it also enables (`evento::fjall::FjallJournal`, `evento::sql::SqlJournal`,
  `evento::turso::TursoJournal`; each backend then implements
  `evento::cluster::ClusterBackend`: `open_journal`, `empty_for_join`, `discard_journal`);
  one process runs one node of one cluster, opened from its settings:
  `evento::cluster::Cluster::builder(store).settings(Some(settings)).state_dir(dir).open().await?`
  → `(Arc<Cluster>, ClusterExecutor<_>)`; `Settings` deserializes a `[cluster]` section
  (`node_id`, `peers`, `listen?`, `absent_after_secs`, `write_backlog`, `linearizable_reads?`),
  `None` = a node alone; a peer is `ip:port` or `host:port` (a name is resolved at each
  dial; a node named by host must set `listen`); a `tls` section (`ca`, `cert`, `key` files)
  puts the nodes on mutual TLS, each let in as the node its certificate names (the host or
  address it has in `peers`), renewed by replacing the files, a refused link said in
  `health().peers[i].refused`; `cluster.health()`, `until_confirmed`, `live_nodes`, `shutdown`;
  with peers a read of one aggregate costs a consensus round (`linearizable_reads` on by
  default): `executor.local_reads()` is a cheap handle over the same node that skips it —
  build subscriptions over it and use it for reads that need no read-your-writes;
  `executor.linearizable_reads()` is the opposite;
  `Cluster::mark_for_rejoin(dir)` for a lost store
  (`examples/bank-axum-cluster/`, `evento-cluster/OPERATIONS.md` §2). A deployer
  touches only the crate root; the protocol's types (`TxnId`, `CommandState`,
  `Message`, the journal records) sit under `evento::cluster::protocol`.

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

**Locking shapes.** `evento-lock` (dev-dependency, or `evento` with the `lock` feature as `evento::lock`) records every event, every
`Encode` type an event reaches, and every executor-snapshotted view in
`events.lock`; a test (`evento_lock::check(env!("CARGO_MANIFEST_DIR")).unwrap()`, or `evento::lock::check(..)`)
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

The read model of an aggregate is a projection, not a subscription:

- **One aggregate by id** (a detail page, the state a command checks):
  `projection.load(id)`. It restores the snapshot and folds every event written after
  it, past the stability watermark too, so it shows a write that just happened.
- **Lists and search**: make the snapshot a row of your own table (*Snapshots*, below)
  and keep every row current with `projection.subscription(key).start(&exec)`; query the
  table. The rows trail the writes by the stability margin (see Gotchas).
- **A `SubscriptionBuilder` (§4)** is for sagas and stats, not for the row of one
  aggregate.

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

### Snapshots — four modes

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
- **A table of your own** — the read model: implement `Snapshot` by hand. The row *is*
  the snapshot: `restore` reads it, `take_snapshot` upserts it, `drop_snapshot` deletes it.

```rust
use evento::{Executor, projection::{Context, Projection, Snapshot}};
use sqlx::SqlitePool;

// No bitcode derives and no `#[evento::snapshot]`: the table is the snapshot.
#[evento::projection]
pub struct AccountView { pub owner: String, pub balance: i64 }

pub fn account_view<E: Executor>(pool: SqlitePool) -> Projection<E, AccountView> {
    Projection::new::<Account>()
        .handler(on_opened())
        .handler(on_deposited())
        .data(pool) // reaches the impl below through `context.extract()`
}

impl<E: Executor> Snapshot<E> for AccountView {
    async fn restore(context: &Context<'_, E>) -> anyhow::Result<Option<Self>> {
        let pool: SqlitePool = context.extract();
        let row: Option<(String, i64, String, i64)> = sqlx::query_as(
            "SELECT owner, balance, cursor, aggregate_version FROM accounts
             WHERE tenant = ?1 AND id = ?2",
        )
        .bind(&context.tenant).bind(&context.id)
        .fetch_optional(&pool).await?;
        Ok(row.map(|(owner, balance, cursor, version)| AccountView {
            owner, balance, cursor, aggregate_version: version as u16, ..Default::default()
        }))
    }

    async fn take_snapshot(&self, context: &Context<'_, E>) -> anyhow::Result<()> {
        let pool: SqlitePool = context.extract();
        sqlx::query(
            "INSERT INTO accounts (tenant, id, owner, balance, cursor, aggregate_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (tenant, id) DO UPDATE SET owner = excluded.owner,
                balance = excluded.balance, cursor = excluded.cursor,
                aggregate_version = excluded.aggregate_version",
        )
        .bind(&context.tenant).bind(&context.id).bind(&self.owner).bind(self.balance)
        .bind(&self.cursor).bind(self.aggregate_version as i64)
        .execute(&pool).await?;
        Ok(())
    }

    // Called by the projection subscription on the `.tombstone::<Ev>()` event.
    async fn drop_snapshot(context: &Context<'_, E>) -> anyhow::Result<()> {
        let pool: SqlitePool = context.extract();
        sqlx::query("DELETE FROM accounts WHERE tenant = ?1 AND id = ?2")
            .bind(&context.tenant).bind(&context.id)
            .execute(&pool).await?;
        Ok(())
    }
}

// Keep every row current; the key is the cursor identity, keep it stable.
let sub = account_view(pool.clone()).subscription("accounts").supervised().start(&executor).await?;
// One account, without lag: its row, then the events written after it.
let view = account_view(pool.clone()).load(&id).execute(&executor).await?;
// Lists and search: query `accounts`.
```

- **Keep `cursor` and `aggregate_version` with the data, in one statement.** `restore`
  gives both back: the cursor is where the load goes on from, the version is what
  `write()` commits at (left at 0, a commit fails with `InvalidOriginalVersion`). Key
  the row by `context.tenant` and `context.id`; the `tenant` field is set by the load.
- **No bitcode derives on it**, nor `#[evento::snapshot]`: they bring an impl of their
  own, which conflicts with yours.
- **A row holds the stable prefix only.** Any load calls `take_snapshot`, but only with
  the events below `stable_timestamp`; the newer ones are folded into what the load
  returns and written by a later pass. Right after a write, a load returns the new
  state while the row still shows the old one.
- **`.tombstone::<Ev>()`** makes the subscription call `drop_snapshot` and a load answer
  `None`, for one more read per load (it looks for the tombstone first).
- **`.revision(n)` does not reach your table**: it keys executor snapshots only. Change
  the table's shape with a migration and a new subscription key, so the projection
  replays into the new rows.
- **Over a `Sidecar`** the row is local and the events after it come from the events
  store: a load is one round trip there, warm row or not.
- **A projection subscription follows co-keyed aggregates only** (secondary aggregates
  under the same id). Data of an aggregate under another id comes from
  `LoadBuilder::aggregate` at load time, or from a `SubscriptionBuilder`.

### Commands and the write gateway (`#[evento::command]`)

The write model is a projection with `id = <field>` (this implements
`ProjectionAggregate`, whose `write()` returns a `WriteBuilder` pre-filled with
the aggregate id, the version the load observed and the tenant it was loaded in
— optimistic concurrency for free, in the right stream). `#[evento::command]` on an impl block turns each method with a trailing
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

## 4. Subscriptions (sagas and stats)

A `SubscriptionBuilder` is for what is not the state of one aggregate: sagas (an event
that leads to a command on another aggregate, a mail, a webhook) and stats (counters
and tables across many aggregates). The row of one aggregate is a projection kept by a
projection subscription (§3), which a page can also load by id without lag.

`#[evento::subscription]` handlers take the context first, then the event:
`(context: &Context<'_, E>, event: Event<SomeEvent>)`. They do their side effects
via `context.executor` and `context.extract::<T>()` —
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
    .tenant("")                // one tenant's events only; default = every tenant
    .chunk_size(100)
    .retry(5)                  // exponential backoff; .no_retry() to disable
    .shutdown_grace(Duration::from_secs(10)) // bound on waiting for the pass in flight (default 30s); .no_shutdown_grace() waits forever
    .start(&executor)          // background task; returns a handle
    .await?;
// ...later:
sub.shutdown().await?;
```

- `.start(exec)` runs a background loop; `.run_once(exec)` drains pending events once and returns.
- `sub.stopped().await` resolves to a `StopReason` once the worker ends: `Shutdown`,
  `StoppedByHandler`, `LostOwnership` (another worker claimed the key), `Failed(err)`
  (retries exhausted, no `.continue_on_error()`), or `Panicked`. Await it to notice a
  dead subscription instead of discovering a stale read model later.
- `.supervised()` replaces a hand-written supervisor: after a pass that failed past `.retry(n)`
  (or a handler panic) the worker does not stop with `Failed`; it waits (1 s doubling to 5 min,
  `.restart_backoff(min, max)`) and goes on from its cursor at the failing event. `sub.state()` →
  `State::Running | Restarting { error, since, attempts } | Stopped(reason)`; consecutive
  failures are one run (`since` kept, `attempts` counting) until the worker went on for `max`
  without failing. Shutdown, `ctx.stop()` and `LostOwnership` still stop it. Also on
  `ProjectionSubscription`. Pair with `.dead_letter()` when one event can be the trouble.
- `sub.lag().await?` → `Lag { behind, pending_since }`, asked of the store now with the
  subscription's own filters: `behind` = seconds from the cursor's event to the newest event it
  follows, `pending_since` = when the oldest unacknowledged one was written (`.waiting()` = its
  age, `.is_caught_up()`). An event it does not follow never counts, and a handler that never
  returns shows as `waiting()` growing. `builder.lag(exec)` answers for one not running here.
  Use it for health checks instead of comparing a cursor with the store's newest event.
- `.strict()` fails on an unhandled event; `.continue_on_error()` keeps going after a handler error.
- `.dead_letter()` (with `.retry(n)`) is the saga mode between the two: a handler error is retried
  on that event with back-off, then the event is recorded as a `DeadLetter` beside the cursor,
  acknowledged, and the pass goes on. `.fault(|err| Some(Fault::Store | Fault::Event))` classifies
  an error as the store's (fail the pass) or the event's own (dead-letter at once).
  `exec.dead_letters(key, limit)` / `dead_letter_count(key)` list them (key = storage key, i.e.
  `{routing_key}.{key}` under a default routing key); `builder.redeliver(exec, &letter)` re-runs one.
  Not on an ephemeral subscription, nor over a bare remote client (use a `Sidecar`).
- A payload that does not decode as the handled event is poison, not a handler error: logged,
  acknowledged and skipped by default; `.on_undecodable(Undecodable::Stop)` stops on it instead.
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
- Keep a projection's rows current: `Projection::new::<A>().handler(..).subscription("key").start(&exec)`
  (§3, *A table of your own*).

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
  `executor.original_version::<Ev>(id)`; both read the default tenant unless
  `.tenant(t)` is chained before the `.await`.

## Gotchas

- **Readers and subscriptions default routing keys oppositely.** `evento::read(..)` with
  no `.routing_key()` reads **every** routing key; narrow it with `.no_routing_key()`.
  A subscription with no `.routing_key()`/`.any_routing_key()` only sees events whose
  routing key is NULL. `.any_routing_key()` subscriptions are stored per
  executor-default-routing-key.
- **Tenants are part of stream identity** (`(tenant, type, id)`, default tenant `""`).
  Writes, loads and reads act in the default tenant unless the builder says
  `.tenant(t)` (writes, `Projection::load`, `evento::read`); `has_event` /
  `original_version` read the default tenant. Subscriptions have no tenant option:
  they read every tenant, handlers see `event.tenant` (write back with
  `.tenant(&event.tenant)`), and a projection subscription loads each event in its
  own tenant.
- **Subscriptions trail the writes by the stability margin.** They handle an event once
  it is below `stable_timestamp` (SQL: `stable_margin`, 1 s by default; a cluster: the
  node's watermark; Fjall and Turso: none), so a table they fill, projection subscription
  or not, shows a write about that long after it. `load(id)` reads past the watermark:
  use it where a page must show the write that just happened (on a cluster, not through
  `local_reads()`).
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
- **A `Sidecar` local store belongs to exactly one process.** Two processes sharing it
  share cursors again. Cursor keys are prefixed by the events store's routing key, so one
  local store may follow several event stores with distinct routing keys.
- **Don't `.unwrap()` a load.** `projection.load(id).execute(exec).await` returns
  `anyhow::Result<Option<T>>`; propagate the error with `?` and treat `None` as not-found.

## Reference

Canonical, compiling usage lives in this repo:
- `examples/quickstart/` — smallest end-to-end run (aggregate → command → load → subscription).
- `examples/bank/` — aggregates, `#[evento::command]` commands, queries/projections
  (one per snapshot mode), co-keyed `Owner` aggregate.
- `examples/bank-axum-sqlite/` and `examples/bank-axum-fjall/` — web wiring,
  migrations, projection subscriptions; `bank-axum-remote/` (client/server split)
  and `bank-axum-cluster/` (consensus cluster).
- `evento-remote/README.md` — resolver, `Hello` token, TLS; `evento-cluster/OPERATIONS.md`.
- `evento-macro/README.md` — full macro reference (all options).
- `evento-test/src/lib.rs` — the behavioral contract suite (load, routing isolation,
  snapshots, optimistic locking, multi-aggregate, full command lifecycle).
