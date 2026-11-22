//! The connections a store reads with, kept from one read to the next with
//! the statements they prepared.
//!
//! Turso prepares a statement in a time that grows with its text: about a
//! millisecond for the read of a subscription that follows eight events, ten
//! times what running it takes over an index. A subscription with nothing to
//! handle asks the same few questions at every poll, so a store whose reads
//! each opened a connection and prepared their statement anew spent its time
//! there: some sixty idle subscriptions kept half a core busy (issue #610).
//! A connection taken here runs its statements through Turso's cache of
//! prepared statements, which a connection keeps for as long as it lives.

use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use turso::Connection;

/// How many connections are kept between two reads: what a burst of reads at
/// once leaves behind, past which they are closed.
const KEPT_CONNECTIONS: usize = 16;

/// How many statements a connection is kept with. Turso's cache has no
/// bound of its own, and a store asked statements without end (a list of
/// values of any length, say) would grow it without end: a connection that
/// prepared this many is closed instead of kept.
const KEPT_STATEMENTS: usize = 256;

/// What a rendered statement is run on: a connection, which keeps the
/// statements it prepares or not.
pub(crate) trait Reads {
    /// The connection.
    fn connection(&self) -> &Connection;

    /// Told as a statement begins; whether the connection keeps it prepared.
    fn begins(&self, _sql: &str) -> bool {
        false
    }

    /// Told once a statement ran to its end.
    fn ended(&self) {}
}

impl Reads for Connection {
    fn connection(&self) -> &Connection {
        self
    }
}

/// A connection out of use, and the statements it prepared, by their hash.
struct Kept {
    conn: Connection,
    statements: HashSet<u64>,
}

/// The read connections of one store, shared by its clones.
#[derive(Clone)]
pub(crate) struct Readers {
    db: turso::Database,
    kept: Arc<Mutex<Vec<Kept>>>,
    hasher: RandomState,
}

impl Readers {
    pub(crate) fn new(db: turso::Database) -> Self {
        Self {
            db,
            kept: Arc::default(),
            hasher: RandomState::new(),
        }
    }

    /// A connection for a read: the one used last, or a new one. It goes
    /// back when dropped.
    pub(crate) fn take(&self) -> anyhow::Result<ReadConnection> {
        let kept = self.kept.lock().expect("readers poisoned").pop();
        let Kept { conn, statements } = match kept {
            Some(kept) => kept,
            None => Kept {
                conn: crate::executor::connect_read(&self.db)?,
                statements: HashSet::new(),
            },
        };
        Ok(ReadConnection {
            conn,
            statements: Mutex::new(statements),
            running: AtomicBool::new(false),
            readers: self.clone(),
        })
    }
}

/// A read connection of a store ([`Readers::take`]), kept for the next read
/// once dropped, unless a statement was left midway on it.
pub(crate) struct ReadConnection {
    conn: Connection,
    statements: Mutex<HashSet<u64>>,
    /// Whether a statement began on the connection and did not end: a read
    /// that failed, or one given up while it ran.
    running: AtomicBool,
    readers: Readers,
}

impl Reads for ReadConnection {
    fn connection(&self) -> &Connection {
        &self.conn
    }

    fn begins(&self, sql: &str) -> bool {
        self.running.store(true, Ordering::Relaxed);
        self.statements
            .lock()
            .expect("readers poisoned")
            .insert(self.readers.hasher.hash_one(sql));
        true
    }

    fn ended(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for ReadConnection {
    fn drop(&mut self) {
        if self.running.load(Ordering::Relaxed) {
            return;
        }
        let statements = std::mem::take(&mut *self.statements.lock().expect("readers poisoned"));
        if statements.len() > KEPT_STATEMENTS {
            return;
        }
        let mut kept = self.readers.kept.lock().expect("readers poisoned");
        if kept.len() < KEPT_CONNECTIONS {
            kept.push(Kept {
                conn: self.conn.clone(),
                statements,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{self, EventTable};
    use sea_query::{Expr, ExprTrait, Query};

    impl Readers {
        fn kept(&self) -> usize {
            self.kept.lock().unwrap().len()
        }
    }

    /// A read of the events, as a statement of its own text for each `n`.
    fn named(n: usize) -> query::Prepared {
        query::prepare(
            &Query::select()
                .column(EventTable::Id)
                .from(EventTable::Table)
                .and_where(Expr::col(EventTable::Version).is_in(0..=n as i64))
                .to_owned(),
        )
    }

    /// A read leaves its connection for the next one, which finds the
    /// statement prepared: one connection serves the reads that follow one
    /// another.
    #[tokio::test]
    async fn a_connection_serves_the_reads_that_follow_one_another() -> anyhow::Result<()> {
        let store = crate::Turso::in_memory().await?;
        let readers = Readers::new(store.database().clone());
        for _ in 0..3 {
            let conn = readers.take()?;
            assert!(query::fetch_rows(&conn, named(0)).await?.1.is_empty());
            drop(conn);
            assert_eq!(readers.kept(), 1);
        }
        let conn = readers.take()?;
        assert_eq!(conn.statements.lock().unwrap().len(), 1);

        // Two reads at once are two connections, both kept.
        let other = readers.take()?;
        drop((conn, other));
        assert_eq!(readers.kept(), 2);
        Ok(())
    }

    /// A connection a statement was left midway on is closed: a read given
    /// up while it ran, or one that failed.
    #[tokio::test]
    async fn a_connection_left_midway_is_not_kept() -> anyhow::Result<()> {
        let store = crate::Turso::in_memory().await?;
        let readers = Readers::new(store.database().clone());
        let conn = readers.take()?;
        assert!(conn.begins("SELECT 1"));
        drop(conn);
        assert_eq!(readers.kept(), 0);

        let conn = readers.take()?;
        let unknown = query::Prepared::raw("SELECT * FROM no_such_table");
        assert!(query::fetch_rows(&conn, unknown).await.is_err());
        drop(conn);
        assert_eq!(readers.kept(), 0);
        Ok(())
    }

    /// Past [`KEPT_STATEMENTS`] statements a connection is closed, and
    /// past [`KEPT_CONNECTIONS`] connections the rest are.
    #[tokio::test]
    async fn what_is_kept_is_bounded() -> anyhow::Result<()> {
        let store = crate::Turso::in_memory().await?;
        let readers = Readers::new(store.database().clone());
        let conn = readers.take()?;
        for n in 0..KEPT_STATEMENTS {
            query::fetch_rows(&conn, named(n)).await?;
        }
        drop(conn);
        assert_eq!(readers.kept(), 1);
        let conn = readers.take()?;
        query::fetch_rows(&conn, named(KEPT_STATEMENTS)).await?;
        drop(conn);
        assert_eq!(readers.kept(), 0);

        let taken: Vec<_> = (0..KEPT_CONNECTIONS + 4)
            .map(|_| readers.take())
            .collect::<Result<_, _>>()?;
        drop(taken);
        assert_eq!(readers.kept(), KEPT_CONNECTIONS);
        Ok(())
    }
}
