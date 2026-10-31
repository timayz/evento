//! Schema migrations tracked in the database's `PRAGMA user_version`.
//!
//! A vendored copy of [turso-migrate](https://github.com/maun/turso-migrate)
//! 0.1.2 by Manuel Adameit (MIT OR Apache-2.0; see `LICENSE-turso-migrate`),
//! kept API-compatible so the crate can be swapped back in once it depends on
//! a current `turso`. Vendored because turso-migrate pins `turso = "0.4"`,
//! which Cargo cannot unify with the `turso` this crate uses.
//!
//! Migrations are applied in definition order, each in its own transaction,
//! and the count applied so far is the database's `user_version`: no
//! migrations table. Never modify a migration once it has been applied
//! somewhere — append a new one.
//!
//! ```rust
//! use evento_turso::migrate::{Migration, Migrations};
//!
//! const MIGRATIONS: Migrations = Migrations::new(&[
//!     Migration::up("001", "CREATE TABLE friend(name TEXT NOT NULL);"),
//!     evento_turso::up_file!("../migrations/0001_init.sql"),
//! ]);
//!
//! # async fn run() -> anyhow::Result<()> {
//! let db = turso::Builder::new_local(":memory:").build().await?;
//! let mut conn = db.connect()?;
//! let applied = MIGRATIONS.to_latest(&mut conn).await.map_err(|(_, err)| err)?;
//! # let _ = applied;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use turso::{Connection, Error};

/// The future a function migration returns.
pub type MigrationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

/// Why [`Migrations::to_latest`] stopped.
#[derive(Error, Debug)]
pub enum MigrationError {
    /// The database rejected a statement or a transaction.
    #[error("Turso error: {0}")]
    Turso(#[from] Error),
    /// The database's `user_version` is negative or above the number of
    /// migrations known here: it was migrated by a newer schema (or with a
    /// feature this build lacks, see [`MIGRATIONS`](crate::MIGRATIONS)).
    #[error("Database has user_version {0}, but it must be between 0 and {1}")]
    InvalidUserVersion(i32, usize),
}

/// One migration step: a SQL script or an async Rust function.
#[derive(Clone, Copy)]
pub enum Migration<'a> {
    /// A SQL script, run with `execute_batch`.
    Sql {
        /// A name for logs.
        name: &'a str,
        /// The script.
        sql: &'a str,
    },
    /// A Rust function run inside the migration's transaction.
    Fn {
        /// A name for logs.
        name: &'a str,
        /// The function.
        f: fn(&Connection) -> MigrationFuture<'_>,
    },
}

impl<'a> Migration<'a> {
    /// A migration from a SQL script.
    pub const fn up(name: &'a str, sql: &'a str) -> Self {
        Self::Sql { name, sql }
    }

    /// A migration from a Rust function taking the transaction's connection.
    /// [`up_fn!`](crate::up_fn) wraps an `async fn` for it.
    pub const fn up_fn(name: &'a str, f: fn(&Connection) -> MigrationFuture<'_>) -> Self {
        Self::Fn { name, f }
    }

    /// The migration's name.
    pub const fn name(&self) -> &'a str {
        match self {
            Migration::Sql { name, .. } | Migration::Fn { name, .. } => name,
        }
    }
}

/// A migration from a SQL file, embedded with `include_str!` (the path is
/// relative to the file invoking the macro, as for `include_str!`).
#[macro_export]
macro_rules! up_file {
    ($path:literal) => {
        $crate::migrate::Migration::up($path, include_str!($path))
    };
}

/// A migration from an `async fn(&turso::Connection) -> turso::Result<()>`.
#[macro_export]
macro_rules! up_fn {
    ($name:expr, $func:path) => {{
        fn wrapper(conn: &$crate::turso::Connection) -> $crate::migrate::MigrationFuture<'_> {
            Box::pin($func(conn))
        }
        $crate::migrate::Migration::up_fn($name, wrapper)
    }};
}

/// An ordered list of migrations.
pub struct Migrations<'a> {
    migrations: &'a [Migration<'a>],
}

impl<'a> Migrations<'a> {
    /// Over `migrations`, in the order they apply.
    pub const fn new(migrations: &'a [Migration<'a>]) -> Self {
        Self { migrations }
    }

    /// How many migrations the list holds: the `user_version` of a database
    /// at the latest schema.
    pub const fn len(&self) -> usize {
        self.migrations.len()
    }

    /// Whether the list is empty.
    pub const fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }

    /// Applies every migration the database has not seen, each in its own
    /// transaction that also advances `user_version`; returns how many were
    /// applied. On an error, the tuple carries how many succeeded before it.
    pub async fn to_latest(&self, conn: &mut Connection) -> Result<usize, (usize, MigrationError)> {
        let current_version = match get_user_version(conn).await {
            Ok(v) => v,
            Err(e) => return Err((0, e.into())),
        };
        let target_version = self.migrations.len() as i32;
        if current_version == target_version {
            return Ok(0);
        }
        if current_version < 0 || current_version > target_version {
            return Err((
                0,
                MigrationError::InvalidUserVersion(current_version, self.migrations.len()),
            ));
        }

        let mut applied_count = 0;
        for (i, migration) in self
            .migrations
            .iter()
            .enumerate()
            .skip(current_version as usize)
        {
            let version = (i + 1) as i32;
            #[cfg(feature = "tracing")]
            let name = migration.name();

            let tx = match conn.transaction().await {
                Ok(tx) => tx,
                Err(e) => return Err((applied_count, e.into())),
            };

            let result = match migration {
                Migration::Sql { sql, .. } => tx.execute_batch(sql).await,
                Migration::Fn { f, .. } => f(&tx).await,
            };
            if let Err(e) = result {
                #[cfg(feature = "tracing")]
                tracing::error!(error = ?e, migration = %name, "migration failed");
                return Err((applied_count, e.into()));
            }

            if let Err(e) = set_user_version(&tx, version).await {
                #[cfg(feature = "tracing")]
                tracing::error!(error = ?e, migration = %name, "failed to update user_version");
                return Err((applied_count, e.into()));
            }

            if let Err(e) = tx.commit().await {
                #[cfg(feature = "tracing")]
                tracing::error!(error = ?e, migration = %name, "failed to commit");
                return Err((applied_count, e.into()));
            }

            #[cfg(feature = "tracing")]
            tracing::debug!(migration = %name, "migration applied");
            applied_count += 1;
        }

        Ok(applied_count)
    }

    /// Applies the whole list to a fresh in-memory database and returns a
    /// connection to it: a check that the list applies cleanly, or a
    /// migrated test database.
    pub async fn run_all_in_memory(&self) -> Result<Connection, MigrationError> {
        let db = turso::Builder::new_local(":memory:").build().await?;
        let mut conn = db.connect()?;
        if let Err((_, e)) = self.to_latest(&mut conn).await {
            return Err(e);
        }
        Ok(conn)
    }
}

/// The database's `PRAGMA user_version`.
pub async fn get_user_version(conn: &Connection) -> Result<i32, Error> {
    let version = std::sync::Mutex::new(Ok(0));
    conn.pragma_query("user_version", |row| {
        *version.lock().expect("user_version poisoned") = row.get::<i32>(0);
        Ok(())
    })
    .await?;
    version.into_inner().expect("user_version poisoned")
}

async fn set_user_version(conn: &Connection, version: i32) -> Result<(), Error> {
    conn.pragma_update("user_version", version).await?;
    Ok(())
}
