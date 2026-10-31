# evento-turso

[Turso](https://github.com/tursodatabase/turso) storage backend for the
[evento](https://github.com/timayz/evento) event sourcing toolkit: SQLite's schema and
queries — the same tables as evento's SQLite backend — in a pure-Rust, in-process,
async engine. No `sqlx`, no C library, no external server.

Most applications depend on the [`evento`](https://crates.io/crates/evento) facade with
the `turso` feature:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.36", features = ["turso"] }
```

Opening a store runs the pending migrations:

```rust,no_run
# async fn run() -> anyhow::Result<()> {
let executor = evento_turso::Turso::open("./events.db").await?;
// via the facade: evento::Turso::open("./events.db").await?

// Tests, examples, simulations: an in-memory database, gone with its last clone.
let ephemeral = evento_turso::Turso::in_memory().await?;
# let _ = (executor, ephemeral);
# Ok(())
# }
```

Like the fjall backend, a `Turso` is the single writer of its database: writes are
serialized on one connection and stamped with a monotonic commit clock, so events
are totally ordered without a stability watermark. The clock is injectable
(`Turso::with_clock`) for deterministic simulations.

## Migrations

The schema lives in [`migrations/`](migrations). The `evento_turso::migrate` runner,
derived from [turso-migrate](https://github.com/maun/turso-migrate) (see
[`LICENSE-turso-migrate`](LICENSE-turso-migrate)), records each applied migration by
name under an **app** namespace in the table `_evento_turso_migrations`, so several
lists share one database: evento's schema is the app `evento`, the accord journal's
is `evento-accord`, and your application's own migrations run under a name of yours.
`Turso::open` applies evento's pending migrations; your list runs next to it:

```rust,no_run
use evento_turso::migrate::{Migration, Migrations};

const APP_MIGRATIONS: Migrations = Migrations::new(
    "my-app",
    &[Migration::up("001_friends", "CREATE TABLE friend(name TEXT NOT NULL);")],
);

# async fn run() -> anyhow::Result<()> {
let store = evento_turso::Turso::open("./events.db").await?;
let mut conn = store.database().connect()?;
APP_MIGRATIONS.to_latest(&mut conn).await.map_err(|(_, err)| err)?;
# Ok(())
# }
```

## Accord journal

With the `accord` feature the crate also provides `TursoJournal`, the
[evento-accord](../evento-accord) consensus journal on Turso — in the same database
as the events (`TursoJournal::new(store.database().clone()).await`) or in one of its
own (`TursoJournal::open(path)`); its constructors apply the journal's own
migrations (`ACCORD_MIGRATIONS`, app `evento-accord`).

## Learn more

- [API documentation](https://docs.rs/evento-turso)
- [Workspace README](https://github.com/timayz/evento#readme)
