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

The schema lives in [`migrations/`](migrations) and is tracked in the database's
`PRAGMA user_version` by the `evento_turso::migrate` module, a vendored copy of
[turso-migrate](https://github.com/maun/turso-migrate) (see
[`LICENSE-turso-migrate`](LICENSE-turso-migrate)). `Turso::open` applies the pending
migrations; `evento_turso::MIGRATIONS.to_latest(&mut conn)` applies them by hand.

## Accord journal

With the `accord` feature the crate also provides `TursoJournal`, the
[evento-accord](../evento-accord) consensus journal on Turso — in the same database
as the events (`TursoJournal::new(store.database().clone())`) or in one of its own
(`TursoJournal::open(path)`). The migrations then include the journal's tables.

## Learn more

- [API documentation](https://docs.rs/evento-turso)
- [Workspace README](https://github.com/timayz/evento#readme)
