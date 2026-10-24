//! Where each consensus group keeps its state on this host, and how a group is
//! opened over it.
//!
//! **Two files per group.** A group — the catalog (`system`) or a tenant
//! (`tenants/{slug}`) — keeps its events and projections in `{name}.db` and its
//! consensus journal in `{name}.journal.db`. Either can be lost without the
//! other, and each loss has its own way back:
//!
//! - **The event file** is rebuilt from a peer's snapshot over the kept
//!   journal (`GroupHost::open_joining`), as for a replica told to rejoin.
//! - **The journal file** held the replica's votes, promises and reports. A
//!   plain open refuses the events it left behind (`LostJournal`): the
//!   replica drops them and opens joining over an empty journal, under its
//!   own node id. The join waits for a peer that vouches past everything the
//!   replica may have forgotten (`OPERATIONS.md` §7).
//!
//! **The host ledger.** With both files gone, nothing on disk would say the
//! group was ever here: a plain open would start it afresh and vote at once,
//! every earlier vote forgotten. `host.db` lists every group this host has
//! opened, so that open rejoins instead. A group is listed once its files
//! exist and before its first open, while it has voted nothing yet. Listed any
//! earlier, a crash in between would leave a listed group without files, whose
//! next open waits to rejoin. If every host crashed there, none could vouch
//! for another, and the new group would never open. A rejoin is marked in the
//! ledger before it deletes anything and cleared once the join is done, so a
//! crash midway resumes it.
//!
//! **Single-file groups**, from before this layout, kept their journal in the
//! event file. The first open moves it to a file of its own.
//!
//! **A lone node** has no peer to join from, and no peer holds its votes, so
//! it always opens plainly.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, ExecutorDataStore, GroupHost, GroupId, Journal, LostJournal, Node, NodeConfig,
    NodeId, Topology,
};
use evento_sql::{Sql, SqlJournal};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection, Sqlite, SqlitePool};
use sqlx_migrator::{Info, Migrate, Migrator, Plan};

/// The journal's tables, as `AccordMigration` creates them.
const JOURNAL_TABLES: [&str; 4] = [
    "accord_commands",
    "accord_meta",
    "accord_metadata_log",
    "accord_acceptors",
];

/// A group's two files.
#[derive(Clone, Debug)]
pub struct GroupFiles {
    /// Events and projections.
    pub events: PathBuf,
    /// The consensus journal.
    pub journal: PathBuf,
}

impl GroupFiles {
    /// The files of the group named `name` (`system`, `tenants/{slug}`) under
    /// `data_dir`.
    pub fn new(data_dir: &Path, name: &str) -> Self {
        Self {
            events: data_dir.join(format!("{name}.db")),
            journal: data_dir.join(format!("{name}.journal.db")),
        }
    }
}

/// An open group's databases.
pub struct GroupDbs {
    /// One `Sql` for both the data-store bridge and the executor: two `.into()`s
    /// from the same pool would not share the write-wake channel subscriptions
    /// rely on.
    pub sql: Sql<Sqlite>,
    events: SqlitePool,
    journal: SqlitePool,
}

impl GroupDbs {
    /// Opens a group's two files, creating and migrating them.
    async fn open(files: &GroupFiles) -> anyhow::Result<Self> {
        let events = open_events(&files.events).await?;
        let journal = match open_journal(&files.journal).await {
            Ok(journal) => journal,
            Err(err) => {
                events.close().await;
                return Err(err);
            }
        };
        Ok(Self {
            sql: events.clone().into(),
            events,
            journal,
        })
    }

    fn store(&self) -> Arc<dyn DataStore> {
        Arc::new(ExecutorDataStore::new(self.sql.clone()))
    }

    fn journal(&self) -> Arc<dyn Journal> {
        Arc::new(SqlJournal::<Sqlite>::new(self.journal.clone()))
    }

    /// Closes both files. Call it once the group is closed on the host.
    pub async fn close(&self) {
        self.events.close().await;
        self.journal.close().await;
    }
}

/// A rejoin that did not complete. Nothing is left open, and the ledger still
/// routes the group's next open through a rejoin, so opening it again retries.
#[derive(Debug)]
pub struct RejoinFailed;

impl fmt::Display for RejoinFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the group could not rejoin yet")
    }
}

/// A group's entry in the host ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hosted {
    /// A rejoin was started and has not completed.
    pub rejoining: bool,
}

/// A host's groups on disk: their files, and the ledger of the groups it has
/// opened. See the module docs.
pub struct Storage {
    data_dir: PathBuf,
    ledger: SqlitePool,
}

impl Storage {
    /// Opens `host.db` under `data_dir`. A new ledger is seeded with `on_disk()`
    /// (the groups whose files are on disk, by id and name) in the transaction
    /// that creates it. That covers a data directory from before the ledger, or
    /// one whose ledger was lost.
    pub async fn open(
        data_dir: PathBuf,
        on_disk: impl FnOnce() -> anyhow::Result<Vec<(GroupId, String)>>,
    ) -> anyhow::Result<Self> {
        let ledger = connect(&data_dir.join("host.db"), 1, SqliteJournalMode::Wal).await?;
        let mut tx = ledger.begin().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hosted_groups')",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            sqlx::query(
                "CREATE TABLE hosted_groups (
                    group_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    rejoining INTEGER NOT NULL DEFAULT 0
                )",
            )
            .execute(&mut *tx)
            .await?;
            let found = on_disk()?;
            for (group, name) in &found {
                sqlx::query("INSERT INTO hosted_groups (group_id, name) VALUES (?, ?)")
                    .bind(group.0 as i64)
                    .bind(name)
                    .execute(&mut *tx)
                    .await?;
            }
            if !found.is_empty() {
                tracing::warn!(
                    groups = found.len(),
                    dir = %data_dir.display(),
                    "no host ledger: seeded it from the groups found on disk"
                );
            }
        }
        tx.commit().await?;
        Ok(Self { data_dir, ledger })
    }

    /// The files of the group named `name`.
    pub fn files(&self, name: &str) -> GroupFiles {
        GroupFiles::new(&self.data_dir, name)
    }

    /// `group`'s ledger entry, if this host has opened it.
    pub async fn hosted(&self, group: GroupId) -> anyhow::Result<Option<Hosted>> {
        let rejoining: Option<i64> =
            sqlx::query_scalar("SELECT rejoining FROM hosted_groups WHERE group_id = ?")
                .bind(group.0 as i64)
                .fetch_optional(&self.ledger)
                .await?;
        Ok(rejoining.map(|rejoining| Hosted {
            rejoining: rejoining != 0,
        }))
    }

    /// Records `group` in the ledger, with a rejoin pending or not.
    pub async fn set_hosted(
        &self,
        group: GroupId,
        name: &str,
        rejoining: bool,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO hosted_groups (group_id, name, rejoining) VALUES (?, ?, ?)
             ON CONFLICT (group_id) DO UPDATE SET rejoining = excluded.rejoining",
        )
        .bind(group.0 as i64)
        .bind(name)
        .bind(i64::from(rejoining))
        .execute(&self.ledger)
        .await?;
        Ok(())
    }

    /// Closes the ledger.
    pub async fn close(&self) {
        self.ledger.close().await;
    }

    /// Opens `group` (named `name`) on `host` over its files: plainly when
    /// they are whole, by rejoining when either was lost or a rejoin is
    /// pending. See the module docs. A [`RejoinFailed`] error can be retried.
    pub async fn open_group(
        &self,
        host: &GroupHost,
        group: GroupId,
        name: &str,
        topology: Arc<dyn Topology>,
        config: NodeConfig,
    ) -> anyhow::Result<(Node, GroupDbs)> {
        let files = self.files(name);
        if files.events.exists() {
            split_single_file(&files).await?;
        }
        let contacts: Vec<NodeId> = topology
            .nodes()
            .into_iter()
            .filter(|&node| node != host.id())
            .collect();
        let hosted = self.hosted(group).await?;
        let rejoin = !contacts.is_empty()
            && hosted.is_some_and(|hosted| {
                hosted.rejoining || !files.events.exists() || !files.journal.exists()
            });
        if !rejoin {
            let dbs = GroupDbs::open(&files).await?;
            if hosted.is_none() {
                // Its files exist and it has not voted yet: see the module docs.
                if let Err(err) = self.set_hosted(group, name, false).await {
                    dbs.close().await;
                    return Err(err);
                }
            }
            match host
                .open(group, topology.clone(), dbs.store(), dbs.journal(), config)
                .await
            {
                Ok(node) => return Ok((node, dbs)),
                Err(err) => {
                    dbs.close().await;
                    if contacts.is_empty() || err.downcast_ref::<LostJournal>().is_none() {
                        return Err(err);
                    }
                    tracing::warn!(
                        group = group.0,
                        name,
                        error = format!("{err:#}"),
                        "the group's journal was lost: rejoining under this node id"
                    );
                }
            }
        }
        self.rejoin(host, group, name, &files, topology, config, &contacts)
            .await
    }

    /// Discards `group`'s event file and opens it joining over its journal
    /// file, which is kept, or created empty if it was lost.
    #[allow(clippy::too_many_arguments)]
    async fn rejoin(
        &self,
        host: &GroupHost,
        group: GroupId,
        name: &str,
        files: &GroupFiles,
        topology: Arc<dyn Topology>,
        config: NodeConfig,
        contacts: &[NodeId],
    ) -> anyhow::Result<(Node, GroupDbs)> {
        // Before anything is deleted: a crash from here on resumes the rejoin.
        self.set_hosted(group, name, true).await?;
        remove_sqlite_files(&files.events)?;
        let dbs = GroupDbs::open(files).await?;
        tracing::info!(group = group.0, name, "rejoining the group from a peer");
        match host
            .open_joining(
                group,
                topology,
                dbs.store(),
                dbs.journal(),
                config,
                contacts,
            )
            .await
        {
            Ok(node) => {
                if let Err(err) = self.set_hosted(group, name, false).await {
                    // The next open rejoins again: slower, but safe.
                    tracing::warn!(group = group.0, name, error = %err, "rejoin not cleared");
                }
                tracing::info!(group = group.0, name, "rejoined the group");
                Ok((node, dbs))
            }
            Err(err) => {
                dbs.close().await;
                // Whatever part of a snapshot landed must go before the next
                // try. The journal stays.
                remove_sqlite_files(&files.events)?;
                Err(err.context(RejoinFailed))
            }
        }
    }
}

/// Opens (creating and migrating) a group's event file.
async fn open_events(path: &Path) -> anyhow::Result<SqlitePool> {
    let pool = connect(path, 3, SqliteJournalMode::Wal).await?;
    let mut conn = pool.acquire().await?;
    evento_sql_migrator::new::<Sqlite>()?
        .run(&mut *conn, &Plan::apply_all())
        .await?;
    Ok(pool)
}

/// Opens (creating and migrating) a group's journal file. It holds the
/// journal's tables only.
async fn open_journal(path: &Path) -> anyhow::Result<SqlitePool> {
    let pool = connect(path, 2, SqliteJournalMode::Wal).await?;
    migrate_journal(&pool).await?;
    Ok(pool)
}

async fn migrate_journal(pool: &SqlitePool) -> anyhow::Result<()> {
    let mut migrator = Migrator::<Sqlite>::default();
    migrator.add_migration(Box::new(evento_sql_migrator::AccordMigration))?;
    let mut conn = pool.acquire().await?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    Ok(())
}

/// A pool over one SQLite file. sqlx-sqlite runs one OS thread per open
/// connection, so pools stay small and let idle connections close. Both files
/// use `synchronous(Full)`: a vote in the journal must survive a power loss,
/// and so must an event the journal records as applied.
async fn connect(
    path: &Path,
    max_connections: u32,
    mode: SqliteJournalMode,
) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(mode)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    Ok(SqlitePoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .idle_timeout(Some(Duration::from_secs(30)))
        .connect_with(options)
        .await?)
}

/// Moves the journal of a group from before the split out of its event file.
/// The copy is written to `{journal}.tmp` and renamed into place: the rename
/// commits it. The event file's copy is cleared after that, also when a crash
/// came in between. A binary from before the split then finds no journal in
/// the event file and refuses it (`LostJournal`), rather than read a stale
/// one.
async fn split_single_file(files: &GroupFiles) -> anyhow::Result<()> {
    let events = connect(&files.events, 1, SqliteJournalMode::Wal).await?;
    let split = split_with(&events, files).await;
    events.close().await;
    split
}

async fn split_with(events: &SqlitePool, files: &GroupFiles) -> anyhow::Result<()> {
    let tmp = with_suffix(&files.journal, ".tmp");
    // A copy a crash interrupted.
    remove_sqlite_files(&tmp)?;
    if !holds_journal(events).await? {
        return Ok(());
    }
    if !files.journal.exists() {
        // Rollback-journal mode, so the copy is one self-contained file.
        let copy = connect(&tmp, 1, SqliteJournalMode::Delete).await?;
        let copied = copy_journal(&copy, &files.events).await;
        copy.close().await;
        copied?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &files.journal)?;
        sync_dir(&files.journal)?;
        tracing::info!(
            from = %files.events.display(),
            to = %files.journal.display(),
            "moved the journal into a file of its own"
        );
    }
    let mut tx = events.begin().await?;
    for statement in [
        "DELETE FROM accord_commands",
        "DELETE FROM accord_meta",
        "DELETE FROM accord_metadata_log",
        "DELETE FROM accord_acceptors",
    ] {
        sqlx::query(statement).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Whether the event file holds journal rows (only a file from before the
/// split does).
async fn holds_journal(events: &SqlitePool) -> anyhow::Result<bool> {
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
         AND name IN ('accord_commands', 'accord_meta', 'accord_metadata_log', 'accord_acceptors')",
    )
    .fetch_one(events)
    .await?;
    if tables < JOURNAL_TABLES.len() as i64 {
        return Ok(false);
    }
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM accord_commands) OR EXISTS (SELECT 1 FROM accord_meta)
         OR EXISTS (SELECT 1 FROM accord_metadata_log) OR EXISTS (SELECT 1 FROM accord_acceptors)",
    )
    .fetch_one(events)
    .await?)
}

/// Copies every journal row of the event file at `events` into `copy`, in
/// one transaction.
async fn copy_journal(copy: &SqlitePool, events: &Path) -> anyhow::Result<()> {
    migrate_journal(copy).await?;
    let mut conn = copy.acquire().await?;
    sqlx::query("ATTACH DATABASE ? AS single")
        .bind(events.to_string_lossy().into_owned())
        .execute(&mut *conn)
        .await?;
    let mut tx = conn.begin().await?;
    for statement in [
        "INSERT INTO main.accord_commands (txn, data) SELECT txn, data FROM single.accord_commands",
        "INSERT INTO main.accord_meta (k, v) SELECT k, v FROM single.accord_meta",
        "INSERT INTO main.accord_metadata_log (epoch, layout) SELECT epoch, layout FROM single.accord_metadata_log",
        "INSERT INTO main.accord_acceptors (epoch, state) SELECT epoch, state FROM single.accord_acceptors",
    ] {
        sqlx::query(statement).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    sqlx::query("DETACH DATABASE single")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Deletes a SQLite file and the files beside it (`-wal`, `-shm`,
/// `-journal`), as far as they exist.
pub fn remove_sqlite_files(path: &Path) -> std::io::Result<()> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        match std::fs::remove_file(with_suffix(path, suffix)) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err),
            _ => {}
        }
    }
    Ok(())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Makes a rename in the directory holding `path` durable.
fn sync_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
