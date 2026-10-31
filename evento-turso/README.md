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

## Read models

Your own tables live in the same database. `Reader` pages any sea-query select by a
cursor, `#[evento::projection(backend = turso, FromRow, Cursor, ..)]` derives the row
decoding and the named cursors, and `execute` / `fetch_all` / `fetch_optional` run
statements — the Turso counterparts of `evento::sql::Reader` and sqlx's `FromRow`:

```rust,ignore
use evento::turso::{self, Reader, Text};
use evento::Cursor;

#[evento::projection(backend = turso, turso::FromRow, Cursor, Debug)]
pub struct AdminView {
    #[cursor(by_name, UserAdmin::Id, 1)]
    pub id: String,
    #[cursor(by_name, UserAdmin::Email, 2)]
    pub email: String,
    pub role: Text<Role>,          // an enum stored as text
    #[turso(default)]
    pub deleted: bool,             // not a column
}

let conn = store.connect()?;
let page = Reader::new(statement)
    .args(args)
    .execute::<AdminViewByName>(&conn)
    .await?;
let one: Option<AdminView> = turso::fetch_optional(&conn, &by_id).await?;
```

## Full-text search (feature `fts`)

Turso's full-text search is a [tantivy](https://crates.io/crates/tantivy)-backed index on
a regular table, not an FTS5 virtual table: `CREATE INDEX user_admin_fts ON user_admin
USING fts(email, full_name)`, then `WHERE (email, full_name) MATCH ?` to filter (as a
subquery inside a `Reader` select, for a searched page) and
`SELECT fts_score(email, full_name, ?1) AS score … WHERE fts_match(email, full_name, ?1)
ORDER BY score DESC` to rank. `fts_match` / `fts_score` name every column of the index,
then the query: a subset of the columns still matches but scores nothing. The `fts`
feature (`turso-fts` on the `evento` facade) compiles it in and switches on Turso's
index-method support for every database this crate opens; a database you build
yourself goes through `Turso::builder(path)` to get the same.

## Accord journal

With the `accord` feature the crate also provides `TursoJournal`, the
[evento-accord](../evento-accord) consensus journal on Turso — in the same database
as the events (`TursoJournal::new(store.database().clone()).await`) or in one of its
own (`TursoJournal::open(path)`); its constructors apply the journal's own
migrations (`ACCORD_MIGRATIONS`, app `evento-accord`).

## Learn more

- [API documentation](https://docs.rs/evento-turso)
- [Workspace README](https://github.com/timayz/evento#readme)
