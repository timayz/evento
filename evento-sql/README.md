# evento-sql

SQL storage backend for the [evento](https://github.com/timayz/evento) event sourcing
toolkit: `Sql<DB>` wraps a [sqlx](https://crates.io/crates/sqlx) pool and implements
the `Executor` trait for SQLite, MySQL, and PostgreSQL (feature flags `sqlite` /
`mysql` / `postgres`).

Most applications depend on the [`evento`](https://crates.io/crates/evento) facade,
which also re-exports the schema migrations:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["sqlite"] }
```

Wire a pool into an executor (run the
[evento-sql-migrator](https://crates.io/crates/evento-sql-migrator) migrations first):

```rust,no_run
# #[cfg(feature = "sqlite")]
# async fn run() -> anyhow::Result<()> {
let pool = sqlx::sqlite::SqlitePoolOptions::new()
    .connect("sqlite:events.db")
    .await?;
let executor: evento_sql::Sqlite = pool.into();
// via the facade: let executor: evento::Sqlite = pool.into();
# let _ = executor;
# Ok(())
# }
```

`write` stamps events with the database server clock and subscriptions gate on a
stability watermark, so multi-process writers stay safe; see the
[crate docs](https://docs.rs/evento-sql) for the ordering model.

## Learn more

- [API documentation](https://docs.rs/evento-sql)
- [Workspace README](https://github.com/timayz/evento#readme)
- Runnable example: [`bank-axum-sqlite`](https://github.com/timayz/evento/tree/main/examples/bank-axum-sqlite)
