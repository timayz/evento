# evento-sql-migrator

Database schema migrations for the [evento](https://github.com/timayz/evento) SQL
backend ([evento-sql](https://crates.io/crates/evento-sql)), built on
[sqlx_migrator](https://crates.io/crates/sqlx_migrator). Supports SQLite, MySQL, and
PostgreSQL through feature flags.

Most applications use it through the [`evento`](https://crates.io/crates/evento)
facade, where it is re-exported as `evento::sql_migrator`:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["sqlite"] }
```

Run all migrations before creating the executor (the snippet needs a database feature
enabled, e.g. `sqlite`):

```rust,no_run
use sqlx_migrator::{Migrate, Plan};

# #[cfg(feature = "sqlite")]
# async fn run(pool: sqlx::SqlitePool) -> anyhow::Result<()> {
let mut conn = pool.acquire().await?;
evento_sql_migrator::new::<sqlx::Sqlite>()?
    .run(&mut *conn, &Plan::apply_all())
    .await?;
# Ok(())
# }
```

The resulting schema (event, snapshot, and subscriber tables) is documented in the
[crate docs](https://docs.rs/evento-sql-migrator).

## Learn more

- [API documentation](https://docs.rs/evento-sql-migrator)
- [Workspace README](https://github.com/timayz/evento#readme)
- Runnable example: [`bank-axum-sqlite`](https://github.com/timayz/evento/tree/main/examples/bank-axum-sqlite)
