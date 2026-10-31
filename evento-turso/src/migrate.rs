//! Schema migrations, recorded per **app** in the database.
//!
//! Derived from [turso-migrate](https://github.com/maun/turso-migrate) 0.1.2
//! by Manuel Adameit (MIT OR Apache-2.0; see `LICENSE-turso-migrate`), with
//! one change: where turso-migrate counts applied migrations in the
//! database's `PRAGMA user_version`, this runner records each applied
//! migration by name under an app namespace, in the table
//! `_evento_turso_migrations`. Several independent lists — evento's schema
//! under [`"evento"`](crate::MIGRATIONS), the accord journal's under
//! `"evento-accord"`, and an application's own — then share one database
//! without stepping on each other, as `sqlx_migrator`'s apps do.
//!
//! Migrations are applied in definition order, each in its own transaction
//! together with its record. Never modify or reorder a migration once it has
//! been applied somewhere — append a new one; a list whose names no longer
//! match what the database recorded is refused.
//!
//! ```rust
//! use evento_turso::migrate::{Migration, Migrations};
//!
//! // The application's own migrations, next to evento's in the same database.
//! const APP_MIGRATIONS: Migrations = Migrations::new(
//!     "my-app",
//!     &[
//!         Migration::up("001_friends", "CREATE TABLE friend(name TEXT NOT NULL);"),
//!         evento_turso::up_fn!("002_seed", seed),
//!     ],
//! );
//!
//! async fn seed(conn: &turso::Connection) -> turso::Result<()> {
//!     conn.execute("INSERT INTO friend (name) VALUES ('Alice')", ()).await?;
//!     Ok(())
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let store = evento_turso::Turso::in_memory().await?; // applied evento's list
//! let mut conn = store.database().connect()?;
//! let applied = APP_MIGRATIONS.to_latest(&mut conn).await.map_err(|(_, err)| err)?;
//! # let _ = applied;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use turso::{Connection, Error};

/// The table every app's applied migrations are recorded in.
pub const MIGRATIONS_TABLE: &str = "_evento_turso_migrations";

/// The future a function migration returns.
pub type MigrationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

/// Why [`Migrations::to_latest`] stopped.
#[derive(Error, Debug)]
pub enum MigrationError {
    /// The database rejected a statement or a transaction.
    #[error("Turso error: {0}")]
    Turso(#[from] Error),
    /// The database records more migrations for the app than the list
    /// knows: it was migrated by a newer schema.
    #[error("app {app}: database has {applied} migrations applied, but only {known} are known")]
    Unknown {
        /// The app namespace.
        app: String,
        /// How many migrations the database records for it.
        applied: usize,
        /// How many the list holds.
        known: usize,
    },
    /// The migration recorded at a position is not the one the list holds
    /// there: a migration was modified, renamed or reordered after it was
    /// applied.
    #[error("app {app}: migration {position} is recorded as {recorded:?}, but the list has {expected:?}")]
    Mismatch {
        /// The app namespace.
        app: String,
        /// The position in the list, from 1.
        position: usize,
        /// The name the database recorded.
        recorded: String,
        /// The name the list holds.
        expected: String,
    },
}

/// One migration step: a SQL script or an async Rust function.
#[derive(Clone, Copy)]
pub enum Migration<'a> {
    /// A SQL script, run with `execute_batch`.
    Sql {
        /// The name recorded once applied; unique within its app.
        name: &'a str,
        /// The script.
        sql: &'a str,
    },
    /// A Rust function run inside the migration's transaction.
    Fn {
        /// The name recorded once applied; unique within its app.
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
/// relative to the file invoking the macro, as for `include_str!`); its name
/// is the path.
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

/// An app's ordered list of migrations.
pub struct Migrations<'a> {
    app: &'a str,
    migrations: &'a [Migration<'a>],
}

impl<'a> Migrations<'a> {
    /// The migrations of `app`, in the order they apply. The app name is the
    /// namespace their records live under: pick one per list and keep it.
    pub const fn new(app: &'a str, migrations: &'a [Migration<'a>]) -> Self {
        Self { app, migrations }
    }

    /// The app namespace.
    pub const fn app(&self) -> &'a str {
        self.app
    }

    /// How many migrations the list holds.
    pub const fn len(&self) -> usize {
        self.migrations.len()
    }

    /// Whether the list is empty.
    pub const fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }

    /// The names the database records as applied for this app, in order.
    pub async fn applied(&self, conn: &Connection) -> Result<Vec<String>, Error> {
        ensure_table(conn).await?;
        let mut rows = conn
            .query(
                format!("SELECT name FROM {MIGRATIONS_TABLE} WHERE app = ? ORDER BY position ASC"),
                [self.app],
            )
            .await?;
        let mut names = Vec::new();
        while let Some(row) = rows.next().await? {
            names.push(row.get::<String>(0)?);
        }
        Ok(names)
    }

    /// Applies every migration the database has not recorded for this app,
    /// each in its own transaction together with its record; returns how
    /// many were applied. On an error, the tuple carries how many succeeded
    /// before it. Refuses a database that records migrations the list does
    /// not know, or under other names.
    pub async fn to_latest(&self, conn: &mut Connection) -> Result<usize, (usize, MigrationError)> {
        let applied = match self.applied(conn).await {
            Ok(applied) => applied,
            Err(e) => return Err((0, e.into())),
        };
        if applied.len() > self.migrations.len() {
            return Err((
                0,
                MigrationError::Unknown {
                    app: self.app.to_string(),
                    applied: applied.len(),
                    known: self.migrations.len(),
                },
            ));
        }
        for (i, (recorded, migration)) in applied.iter().zip(self.migrations).enumerate() {
            if recorded != migration.name() {
                return Err((
                    0,
                    MigrationError::Mismatch {
                        app: self.app.to_string(),
                        position: i + 1,
                        recorded: recorded.clone(),
                        expected: migration.name().to_string(),
                    },
                ));
            }
        }

        let mut applied_count = 0;
        for (i, migration) in self.migrations.iter().enumerate().skip(applied.len()) {
            let position = (i + 1) as i64;
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
                tracing::error!(error = ?e, app = %self.app, migration = %name, "migration failed");
                return Err((applied_count, e.into()));
            }

            if let Err(e) = record(&tx, self.app, position, name).await {
                #[cfg(feature = "tracing")]
                tracing::error!(error = ?e, app = %self.app, migration = %name, "failed to record the migration");
                return Err((applied_count, e.into()));
            }

            if let Err(e) = tx.commit().await {
                #[cfg(feature = "tracing")]
                tracing::error!(error = ?e, app = %self.app, migration = %name, "failed to commit");
                return Err((applied_count, e.into()));
            }

            #[cfg(feature = "tracing")]
            tracing::debug!(app = %self.app, migration = %name, "migration applied");
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

/// Creates the records table if absent.
async fn ensure_table(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            app TEXT NOT NULL,
            position INTEGER NOT NULL,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL,
            PRIMARY KEY (app, position)
        )"
    ))
    .await
}

/// Records `name` as `app`'s migration at `position`, on `conn` (the
/// migration's transaction).
async fn record(conn: &Connection, app: &str, position: i64, name: &str) -> Result<(), Error> {
    let applied_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    conn.execute(
        format!(
            "INSERT INTO {MIGRATIONS_TABLE} (app, position, name, applied_at) VALUES (?, ?, ?, ?)"
        ),
        (app, position, name, applied_at),
    )
    .await?;
    Ok(())
}
