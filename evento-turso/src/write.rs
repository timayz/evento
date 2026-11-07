//! Writing with several writers in one database: Turso's MVCC mode,
//! `BEGIN CONCURRENT` transactions and the retry a conflict asks for.
//!
//! Turso's default journal (WAL) admits one write transaction at a time: a
//! second connection that writes meanwhile gets `Busy` at once. In MVCC mode
//! (`PRAGMA journal_mode = 'mvcc'`, [`enable_concurrent_writes`]) a
//! `BEGIN CONCURRENT` transaction runs alongside other writers and only
//! collides with one that changed a row it also changes — reported as a
//! conflict on the statement or at `COMMIT`, after which the transaction is
//! gone and has to be run again from the start. [`transaction`] does that:
//! one body, one transaction per attempt, a bounded retry on a conflict (or
//! on `Busy`, in either mode).
//!
//! A store ([`Turso`](crate::Turso)) and a journal
//! ([`TursoJournal`](crate::TursoJournal)) write through this module in the
//! mode their database is in, so an application whose own tables live next
//! to the events (read models written by subscriptions) writes them through
//! [`Turso::transaction`](crate::Turso::transaction) and never blocks the
//! store, nor is blocked by it.
//!
//! DDL (`CREATE TABLE`, `CREATE INDEX`) needs an exclusive transaction even
//! in MVCC mode; the migration runner uses a plain `BEGIN`, which is why
//! migrations run before the writers start.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use turso::transaction::{DropBehavior, TransactionBehavior};
use turso::Connection;

/// How a database takes its writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Turso's default (WAL): one write transaction at a time, a second
    /// writer gets `Busy`. For a store whose connection is the only writer
    /// of its database.
    #[default]
    Serial,
    /// Turso's MVCC: writers on separate connections run as
    /// `BEGIN CONCURRENT` transactions and only collide on the rows they
    /// both change, which [`transaction`] retries. For a store that shares
    /// its database with the application's own tables and the cluster
    /// journal.
    Concurrent,
}

impl WriteMode {
    /// The `BEGIN` a transaction in this mode opens with.
    pub fn behavior(self) -> TransactionBehavior {
        match self {
            WriteMode::Serial => TransactionBehavior::Deferred,
            WriteMode::Concurrent => TransactionBehavior::Concurrent,
        }
    }
}

/// Switches `db` to MVCC, so its writers can run concurrently. Recorded in
/// the file: a database once switched opens in MVCC mode from then on, and
/// switching it again is a no-op. Run it before anything writes; with
/// [`Turso::open_concurrent`](crate::Turso::open_concurrent) and its
/// siblings the store does it for you.
pub async fn enable_concurrent_writes(db: &turso::Database) -> turso::Result<()> {
    let conn = db.connect()?;
    conn.pragma_update("journal_mode", "'mvcc'").await?;
    Ok(())
}

/// The [`WriteMode`] of `conn`'s database, from its journal mode.
pub async fn write_mode_of(conn: &Connection) -> turso::Result<WriteMode> {
    let mut rows = conn.query("PRAGMA journal_mode", ()).await?;
    let mode = match rows.next().await? {
        Some(row) => match row.get_value(0)? {
            turso::Value::Text(mode) if mode.eq_ignore_ascii_case("mvcc") => WriteMode::Concurrent,
            _ => WriteMode::Serial,
        },
        None => WriteMode::Serial,
    };
    Ok(mode)
}

/// Whether `err` is what another writer did to this transaction — a
/// write-write conflict, a stale snapshot, a `Busy` lock — and running the
/// transaction again may succeed.
pub fn is_retryable(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<turso::Error>()
            .is_some_and(is_retryable_turso)
    })
}

fn is_retryable_turso(err: &turso::Error) -> bool {
    match err {
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_) => true,
        // Turso reports a write-write conflict as a plain error whose text
        // names it (`turso::Error` has no variant for it).
        turso::Error::Error(message) => message.contains("conflict"),
        _ => false,
    }
}

/// The body of a [`transaction`]: runs its statements on the transaction's
/// connection, which it is lent for the attempt, with what the caller passed
/// as `env` (the rows to write, the statements): lent too, so the body's
/// future borrows nothing of its own and can be made as many times as the
/// retry needs.
pub type Body<'c, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'c>>;

/// How many times [`transaction`] runs a body before giving its conflict up.
pub const MAX_ATTEMPTS: u32 = 64;

/// Runs `body` over `env` in one transaction on `conn` and commits it.
///
/// In [`WriteMode::Concurrent`] the transaction is `BEGIN CONCURRENT`; a
/// conflict with another writer (on a statement or at the commit) rolls it
/// back and runs `body` again, up to [`MAX_ATTEMPTS`] times with a short
/// pause in between. In [`WriteMode::Serial`] it is a plain `BEGIN` and a
/// `Busy` lock is retried the same way. Any other error returns at once,
/// rolled back. `body` therefore has to be repeatable: every effect it has
/// goes through the connection it is given.
///
/// ```rust,no_run
/// # async fn run(store: evento_turso::Turso) -> anyhow::Result<()> {
/// let mut conn = store.connect()?;
/// let counter = "hits".to_string();
/// let affected = evento_turso::transaction(&mut conn, store.write_mode(), &counter, |tx, counter| {
///     Box::pin(async move {
///         Ok(tx
///             .execute("UPDATE counters SET n = n + 1 WHERE id = ?", (counter.as_str(),))
///             .await?)
///     })
/// })
/// .await?;
/// # let _ = affected;
/// # Ok(())
/// # }
/// ```
pub async fn transaction<T, E, F>(
    conn: &mut Connection,
    mode: WriteMode,
    env: &E,
    mut body: F,
) -> anyhow::Result<T>
where
    E: ?Sized + Sync,
    F: for<'c> FnMut(&'c Connection, &'c E) -> Body<'c, T>,
{
    let behavior = mode.behavior();
    let mut attempt = 0;
    loop {
        attempt += 1;
        match attempt_once(conn, behavior, env, &mut body).await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < MAX_ATTEMPTS && is_retryable(&err) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(attempt, error = %err, "write transaction retried");
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(u64::from(attempt.min(20)))).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// One attempt: begin, run the body, commit; on any error leave the
/// connection out of any transaction.
async fn attempt_once<T, E, F>(
    conn: &mut Connection,
    behavior: TransactionBehavior,
    env: &E,
    body: &mut F,
) -> anyhow::Result<T>
where
    E: ?Sized + Sync,
    F: for<'c> FnMut(&'c Connection, &'c E) -> Body<'c, T>,
{
    let mut tx = conn.transaction_with_behavior(behavior).await?;
    // A conflict ends the transaction on the spot. The guard must not roll
    // back what is already gone when it drops (the next `BEGIN` on this
    // connection would fail on that late `ROLLBACK`): the rollback below is
    // explicit, and its failure on an ended transaction is harmless.
    tx.set_drop_behavior(DropBehavior::Ignore);
    let outcome = match body(&tx, env).await {
        Ok(value) => tx
            .commit()
            .await
            .map(|()| value)
            .map_err(anyhow::Error::from),
        Err(err) => {
            let _ = tx.rollback().await;
            Err(err)
        }
    };
    // Whatever happened, the connection must be free for the next attempt
    // or the next caller.
    if outcome.is_err() && !conn.is_autocommit()? {
        conn.execute("ROLLBACK", ()).await?;
    }
    outcome
}
