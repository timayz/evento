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

Like the fjall backend, a `Turso` serializes its writes on one connection and stamps
them with a monotonic commit clock, so events are totally ordered without a stability
watermark. The clock is injectable (`Turso::with_clock`) for deterministic simulations.
Events are committed with `synchronous = FULL`; subscription cursors and snapshots go
through a second connection with `synchronous = NORMAL` (a power cut may lose the last
of them: a subscription then takes a few events again, a snapshot is rebuilt), so the
acknowledgement every subscription makes per event costs no fsync.
Other writers of the same database (your read models, the cluster journal) need the
store opened in [concurrent mode](#concurrent-writes-several-writers-one-database).

## Migrations

The schema lives in [`migrations/`](migrations). The `evento_turso::migrate` runner,
derived from [turso-migrate](https://github.com/maun/turso-migrate) (see
[`LICENSE-turso-migrate`](LICENSE-turso-migrate)), records each applied migration by
name under an **app** namespace in the table `_evento_turso_migrations`, so several
lists share one database: evento's schema is the app `evento`, the cluster journal's
is `evento-cluster`, and your application's own migrations run under a name of yours.
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

let conn = store.connect_read()?;
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

## Concurrent writes (several writers, one database)

A `Turso` opened with `open` is the one writer of its file: Turso's default journal
(WAL) gives a second connection that writes at the same time `Busy` at once. When the
application's own tables (read models written by subscriptions) and the cluster journal
live in the same database as the events, open the store with `open_concurrent` (or
`in_memory_concurrent` / `temporary_concurrent` in tests): the database is switched to
Turso's MVCC mode (`PRAGMA journal_mode = 'mvcc'`, recorded in the file, so a later
plain `open` finds it), and every write the store and the journal make is a
`BEGIN CONCURRENT` transaction that runs alongside the others and only collides with
one that changed the same rows — a conflict, reported on the statement or at the
commit, after which the transaction is run again (`transaction`, up to `MAX_ATTEMPTS`).

The application writes its tables the same way, through `Turso::transaction`: a body
over an environment it is lent (the rows to write), so it can be run again whole:

```rust,no_run
# async fn run(store: evento_turso::Turso, email: String) -> anyhow::Result<()> {
store
    .transaction(&email, |tx, email| {
        Box::pin(async move {
            tx.execute(
                "INSERT INTO user_admin (id, email) VALUES (?1, ?2)
                 ON CONFLICT (id) DO UPDATE SET email = excluded.email",
                ("u1", email.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
# Ok(())
# }
```

An MVCC checkpoint stops the world for a moment, and a transaction that starts
meanwhile — a read included — finds the database locked: the store runs a read or a
write that comes back `Busy` again itself, in short pauses. Its write connections
first let Turso retry for `BUSY_TIMEOUT` (50 ms); its read connections do not, because
Turso polls through that wait with a thread kept busy and a store under writes
checkpoints often. Take the connections you only read with from
`store.connect_read()`: `Reader` and the `fetch_*` helpers retry a `Busy` read the
same way.

How long that moment lasts is set by how much there is to checkpoint: about 170 ms
for each megabyte of Turso's logical log, some 20 ms more on a file, with every
transaction waiting meanwhile. A store in MVCC mode therefore keeps the log short:
it checkpoints every 256 KiB (`CHECKPOINT_BYTES`; Turso's own default is about 4 MB),
which under a burst of writes is a wait of 40 to 70 ms every 450 events or so where
Turso's default gives 0.7 s every 7,000. The share of the time the store is closed is
the same either way; so is the time the writes take.
`store.checkpoint_every(Some(bytes))` sets another threshold and
`store.checkpoint_every(None)` leaves the log to `store.checkpoint()`, for a store
that has a quiet hour to pay the wait in (a log left to grow keeps its row versions
in memory and is replayed at the next open, about 50 ms a megabyte). Neither is
recorded in the file: set it after each open.

Turso has a checkpoint that does not stop the world, behind an experimental flag. On
the measurement of issue #544 it removed the waits altogether (the longest read took
5 ms on a file where the default's took 750 ms), and this crate's tests pass with it;
it is still Turso's experiment, so the store does not switch it on. To try it, build
the database yourself:

```rust,no_run
# async fn run() -> anyhow::Result<()> {
use evento_turso::{enable_concurrent_writes, Turso, MIGRATIONS};

let db = Turso::builder("./events.db")
    .experimental_mvcc_passive_checkpoint(true)
    .build()
    .await?;
enable_concurrent_writes(&db).await?;
let mut conn = db.connect()?;
MIGRATIONS
    .to_latest(&mut conn)
    .await
    .map_err(|(applied, err)| anyhow::anyhow!("after {applied} migrations: {err}"))?;
let store = Turso::from_database(db).await?;
# let _ = store;
# Ok(())
# }
```

`cargo test -p evento-turso --test concurrent sizing_mvcc -- --ignored --nocapture`
prints the measurement for your disk. `store.write_mode()` tells
which mode the database is in (`WriteMode::Serial` or
`WriteMode::Concurrent`); `evento_turso::transaction(&mut conn, mode, &env, body)` is
the same loop on a connection of yours, and `is_retryable(&err)` the test it applies
(`Busy`, a stale snapshot, a write-write conflict). DDL (`CREATE TABLE`, `CREATE INDEX`)
still needs a plain transaction in MVCC mode: the migration runner uses one, which is
why migrations run before the writers start.

## Accord journal

With the `cluster` feature the crate also provides `TursoJournal`, the
[evento-cluster](../evento-cluster) consensus journal on Turso — in the same database
as the events (`TursoJournal::new(store.database().clone()).await`) or in one of its
own (`TursoJournal::open(path)`); its constructors apply the journal's own
migrations (`CLUSTER_MIGRATIONS`, app `evento-cluster`). The journal writes in the mode
its database is in: concurrent transactions next to a store opened with
`open_concurrent`, plain ones otherwise. `Turso` also implements
`evento_cluster::ClusterBackend` (`open_journal` over its own database,
`empty_for_join` dropping `event`/`snapshot`/`subscriber` and creating them again in
one transaction — a schema change per table rather than a delete per row —,
`discard_journal` deleting the `cluster_*` tables).

## Learn more

- [API documentation](https://docs.rs/evento-turso)
- [Workspace README](https://github.com/timayz/evento#readme)
