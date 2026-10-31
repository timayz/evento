//! The [`Turso`] executor.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use sea_query::{Expr, ExprTrait, Func, OnConflict, Query};
use tokio::sync::{watch, Mutex};
use ulid::Ulid;

use evento_core::{
    cursor::{Args, ReadResult, Value},
    Event, EventFilter, Executor, RoutingKey, SubscriberStatus, TenantFilter, WriteError,
};

use crate::query::{
    self, apply_filters, EventRow, EventTable, MetaTable, Prepared, SnapshotTable, SubscriberTable,
    EVENT_COLUMNS,
};
use crate::write::{self, Body, WriteMode};
use crate::Reader;

/// The `meta` key of the monotonic commit clock.
const LAST_STAMP_KEY: &str = "last_stamp";

/// Rows per `INSERT` into the event table: eleven bound values each keeps a
/// statement well under SQLite's bound-value cap (32766).
const INSERT_CHUNK: usize = 1000;

/// How many connections [`Turso::transaction`] keeps for the next calls.
const APP_CONNECTIONS: usize = 16;

/// Which write connection a statement goes through.
#[derive(Clone, Copy)]
enum Durability {
    /// `synchronous = FULL`: the events.
    Durable,
    /// `synchronous = NORMAL`: the subscription cursors and the snapshots.
    Relaxed,
}

/// The clock a [`Turso`] stamps its writes with: unix time in milliseconds.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Unix time in milliseconds from the system clock.
fn system_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// An [`Executor`] over a [Turso](https://github.com/tursodatabase/turso)
/// database: SQLite's schema and queries (the same tables as `evento-sql`'s
/// SQLite backend), in-process, async, pure Rust.
///
/// # Ordering
///
/// Like the fjall backend, a `Turso` is the **single writer** of its database
/// (Turso does not share a file between processes): writes are serialized on
/// one write connection and stamped with a monotonic millisecond clock,
/// `max(now, last_stamp + 1)`, persisted with the batch, so cursor order is
/// commit order and never runs backwards across restarts. Nothing can commit
/// below an already-visible stamp, so [`stable_timestamp`](Executor::stable_timestamp)
/// stays `None` and subscriptions see a write as soon as it commits.
/// `replicate` stores the caller's stamps verbatim.
///
/// The clock defaults to the system clock; [`with_clock`](Self::with_clock)
/// injects another (a simulation's virtual time).
///
/// # Connections
///
/// Reads open a connection per call (cheap: connections share the database's
/// pager); every write goes through the one write connection, under a lock, so
/// two write statements never race on a connection (Turso returns `Busy` for
/// the second).
///
/// # Durability
///
/// Events are written with `synchronous = FULL`: an acknowledged write is on
/// disk. Subscription cursors and snapshots go through a second connection
/// with `synchronous = NORMAL`: a power cut may lose the last of them, never
/// corrupt the file, and a subscription then takes a few events again (it
/// delivers at least once) while a snapshot is rebuilt from the events. Every
/// subscription acknowledges each event it takes; an fsync apiece would make
/// the store's event writes queue behind them.
///
/// # Sharing the database
///
/// Opened with [`open`](Self::open), the store expects to be the only writer
/// of its database ([`WriteMode::Serial`]): another connection writing at
/// the same time — the application's own tables, a
/// [`TursoJournal`](crate::TursoJournal) in the same file — gets `Busy`.
/// Opened with [`open_concurrent`](Self::open_concurrent) the database is in
/// Turso's MVCC mode ([`WriteMode::Concurrent`]): the store's writes are
/// `BEGIN CONCURRENT` transactions that run alongside other writers, retried
/// on a conflict (see [`transaction`](crate::transaction)), and the
/// application writes its tables the same way through
/// [`transaction`](Self::transaction). The mode is recorded in the file;
/// [`from_database`](Self::from_database) reads it back.
///
/// # Example
///
/// ```rust,no_run
/// # async fn run() -> anyhow::Result<()> {
/// let executor = evento_turso::Turso::open("./events.db").await?; // runs the migrations
/// let in_memory = evento_turso::Turso::in_memory().await?;        // tests
/// # let _ = (executor, in_memory);
/// # Ok(())
/// # }
/// ```
pub struct Turso {
    db: turso::Database,
    /// How the database takes its writes; read from the file when opened.
    mode: WriteMode,
    /// The single writer of events; holds `BEGIN … COMMIT`.
    write: Arc<Mutex<turso::Connection>>,
    /// The writer of subscription cursors and snapshots, `synchronous =
    /// NORMAL` (see "Durability" above).
    relaxed: Arc<Mutex<turso::Connection>>,
    /// The monotonic commit clock, in milliseconds; only ever written under
    /// `write`, persisted under [`LAST_STAMP_KEY`] with each stamped batch.
    last_stamp: Arc<AtomicU64>,
    clock: Clock,
    /// Bumped after every in-process write (`write_watch`); clones share it.
    write_watch: watch::Sender<u64>,
    /// Connections [`transaction`](Self::transaction) ran on, kept for the
    /// next: a connection's first `BEGIN CONCURRENT` costs milliseconds
    /// (Turso sets its MVCC state up), the next ones microseconds.
    app: Arc<std::sync::Mutex<Vec<turso::Connection>>>,
    /// Keeps a [`temporary`](Self::temporary) database's directory alive.
    _temp: Option<Arc<tempfile::TempDir>>,
}

impl std::fmt::Debug for Turso {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Turso")
            .field("mode", &self.mode)
            .field("last_stamp", &self.last_stamp.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Clone for Turso {
    fn clone(&self) -> Self {
        // Clones share the writer, the clock and the wake channel, so every
        // clone notifies the same subscriptions and stamps the same sequence.
        Self {
            db: self.db.clone(),
            mode: self.mode,
            write: Arc::clone(&self.write),
            relaxed: Arc::clone(&self.relaxed),
            last_stamp: Arc::clone(&self.last_stamp),
            clock: Arc::clone(&self.clock),
            write_watch: self.write_watch.clone(),
            app: Arc::clone(&self.app),
            _temp: self._temp.clone(),
        }
    }
}

impl Turso {
    /// A Turso [`Builder`](turso::Builder) for the database at `path`
    /// (`":memory:"` for an in-memory one) with this crate's defaults: with
    /// the `fts` feature, Turso's index-method support (which its full-text
    /// indexes need) is switched on. [`open`](Self::open) and
    /// [`in_memory`](Self::in_memory) use it; call it yourself to set more
    /// Turso options before [`from_database`](Self::from_database).
    pub fn builder(path: &str) -> turso::Builder {
        let builder = turso::Builder::new_local(path);
        #[cfg(feature = "fts")]
        let builder = builder.experimental_index_method(true);
        builder
    }

    /// Opens (creating if absent) the database at `path` and applies the
    /// pending [`MIGRATIONS`](crate::MIGRATIONS). The database stays in the
    /// mode it is in: Turso's default for a new file ([`WriteMode::Serial`]),
    /// MVCC for one [`open_concurrent`](Self::open_concurrent) created.
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Self::builder(path_str(path.as_ref())?).build().await?;
        Self::migrated(db).await
    }

    /// Opens (creating if absent) the database at `path` for several writers
    /// ([`WriteMode::Concurrent`], see [`enable_concurrent_writes`](crate::enable_concurrent_writes))
    /// and applies the pending [`MIGRATIONS`](crate::MIGRATIONS).
    pub async fn open_concurrent(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Self::builder(path_str(path.as_ref())?).build().await?;
        write::enable_concurrent_writes(&db).await?;
        Self::migrated(db).await
    }

    /// A fresh in-memory database, migrated: for tests and simulations. Every
    /// clone (and every [`TursoJournal`](crate::TursoJournal) built over
    /// [`database`](Self::database)) sees the same data; the data goes when
    /// the last handle does.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let db = Self::builder(":memory:").build().await?;
        Self::migrated(db).await
    }

    /// [`in_memory`](Self::in_memory) for several writers
    /// ([`WriteMode::Concurrent`]).
    pub async fn in_memory_concurrent() -> anyhow::Result<Self> {
        let db = Self::builder(":memory:").build().await?;
        write::enable_concurrent_writes(&db).await?;
        Self::migrated(db).await
    }

    /// A migrated database in a temporary directory, deleted with the last
    /// clone: for tests that need a file (a close/reopen).
    pub async fn temporary() -> anyhow::Result<Self> {
        let dir = tempfile::Builder::new().prefix("evento-turso-").tempdir()?;
        let mut turso = Self::open(dir.path().join("events.db")).await?;
        turso._temp = Some(Arc::new(dir));
        Ok(turso)
    }

    /// [`temporary`](Self::temporary) for several writers
    /// ([`WriteMode::Concurrent`]).
    pub async fn temporary_concurrent() -> anyhow::Result<Self> {
        let dir = tempfile::Builder::new().prefix("evento-turso-").tempdir()?;
        let mut turso = Self::open_concurrent(dir.path().join("events.db")).await?;
        turso._temp = Some(Arc::new(dir));
        Ok(turso)
    }

    /// Applies the pending [`MIGRATIONS`](crate::MIGRATIONS) to `db`, then
    /// wraps it.
    async fn migrated(db: turso::Database) -> anyhow::Result<Self> {
        let mut conn = db.connect()?;
        crate::MIGRATIONS
            .to_latest(&mut conn)
            .await
            .map_err(|(applied, err)| {
                anyhow::anyhow!("migration failed after {applied} applied: {err}")
            })?;
        Self::from_database(db).await
    }

    /// Wraps a database whose schema the caller already migrated (see
    /// [`MIGRATIONS`](crate::MIGRATIONS)); loads the commit clock and reads
    /// the [`WriteMode`] the database is in.
    pub async fn from_database(db: turso::Database) -> anyhow::Result<Self> {
        let write = db.connect()?;
        let mode = write::write_mode_of(&write).await?;
        let relaxed = db.connect()?;
        relaxed.pragma_update("synchronous", "NORMAL").await?;
        let q = query::prepare(
            &Query::select()
                .column(MetaTable::V)
                .from(MetaTable::Table)
                .and_where(Expr::col(MetaTable::K).eq(LAST_STAMP_KEY))
                .to_owned(),
        );
        let last_stamp = match query::fetch_optional_prepared(&write, q).await? {
            Some(row) => {
                let bytes = query::blob(&row, 0)?;
                let bytes: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("corrupt last_stamp meta row"))?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        Ok(Self {
            db,
            mode,
            write: Arc::new(Mutex::new(write)),
            relaxed: Arc::new(Mutex::new(relaxed)),
            last_stamp: Arc::new(AtomicU64::new(last_stamp)),
            clock: Arc::new(system_millis),
            write_watch: watch::channel(0).0,
            app: Arc::new(std::sync::Mutex::new(Vec::new())),
            _temp: None,
        })
    }

    /// Stamps writes from `clock` (unix milliseconds) instead of the system
    /// clock. Shared by every clone made afterwards.
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    /// The underlying database, to share with a
    /// [`TursoJournal`](crate::TursoJournal) or for ad-hoc queries.
    pub fn database(&self) -> &turso::Database {
        &self.db
    }

    /// How the database takes its writes: what the store's writes run as,
    /// and what the application's should (see [`transaction`](Self::transaction)).
    pub fn write_mode(&self) -> WriteMode {
        self.mode
    }

    /// Runs `body` over `env` as one transaction of the application's own,
    /// committed, in the store's [`write_mode`](Self::write_mode):
    /// `BEGIN CONCURRENT` and a retry on conflict when the database is shared
    /// ([`WriteMode::Concurrent`]), a plain transaction otherwise. What a
    /// subscription handler writes its read-model rows through. See
    /// [`transaction`](crate::transaction) for the body's contract.
    ///
    /// Runs on a connection of the store's own, kept from one call to the
    /// next (a connection's first concurrent transaction is slow, the next
    /// ones are not); calls at the same time each get one.
    pub async fn transaction<T, E, F>(&self, env: &E, body: F) -> anyhow::Result<T>
    where
        E: ?Sized + Sync,
        F: for<'c> FnMut(&'c turso::Connection, &'c E) -> Body<'c, T>,
    {
        let pooled = self.app.lock().expect("connection pool poisoned").pop();
        let mut conn = match pooled {
            Some(conn) => conn,
            None => self.connect()?,
        };
        let outcome = write::transaction(&mut conn, self.mode, env, body).await;
        // Only a connection left outside any transaction goes back.
        if conn.is_autocommit().unwrap_or(false) {
            let mut pool = self.app.lock().expect("connection pool poisoned");
            if pool.len() < APP_CONNECTIONS {
                pool.push(conn);
            }
        }
        outcome
    }

    /// The commit clock as the store last stamped it, in milliseconds.
    pub fn last_stamp(&self) -> u64 {
        self.last_stamp.load(Ordering::Acquire)
    }

    /// A connection for reads of your own — a read model's queries through
    /// [`Reader`], [`fetch_all`](crate::fetch_all) and friends. Cheap:
    /// connections share the database's pager. Writes of your own may use it
    /// too, serialized by you; the store's writes go through its own
    /// connection.
    pub fn connect(&self) -> anyhow::Result<turso::Connection> {
        Ok(self.db.connect()?)
    }

    /// A connection for a read.
    fn reader(&self) -> anyhow::Result<turso::Connection> {
        self.connect()
    }

    /// The clock's time in whole seconds (for `created_at` / `updated_at`).
    fn now_secs(&self) -> i64 {
        i64::try_from((self.clock)() / 1000).unwrap_or(i64::MAX)
    }

    /// Runs one statement on a write connection — the durable one, or the
    /// relaxed one for cursors and snapshots: on its own in
    /// [`WriteMode::Serial`], as a concurrent transaction (retried on a
    /// conflict) in [`WriteMode::Concurrent`]. The rows affected.
    async fn write_one(&self, durability: Durability, q: Prepared) -> anyhow::Result<u64> {
        let mut write = match durability {
            Durability::Durable => self.write.lock().await,
            Durability::Relaxed => self.relaxed.lock().await,
        };
        match self.mode {
            WriteMode::Serial => Ok(query::execute_prepared(&write, &q).await?),
            WriteMode::Concurrent => {
                write::transaction(&mut write, self.mode, &q, |tx, q| {
                    Box::pin(async move { Ok(query::execute_prepared(tx, q).await?) })
                })
                .await
            }
        }
    }

    /// Inserts a batch of events in one transaction; with `restamp`,
    /// `timestamp`/`timestamp_subsec` are replaced by the commit clock so
    /// cursor order matches commit order regardless of writer clocks.
    async fn insert_events(&self, mut events: Vec<Event>, restamp: bool) -> Result<(), WriteError> {
        let mut write = self.write.lock().await;

        let stamp = restamp.then(|| {
            // Only ever written under the write lock, so load-then-store is
            // race-free; the atomic exists for lock-free readers.
            let now = (self.clock)();
            let stamp = Ord::max(
                now,
                self.last_stamp.load(Ordering::Acquire).saturating_add(1),
            );
            for event in &mut events {
                event.timestamp = stamp / 1000;
                event.timestamp_subsec = (stamp % 1000) as u32;
            }
            stamp
        });

        // One statement binds eleven values per row and SQLite caps the bound
        // values of a statement: a large batch — a joining replica's snapshot
        // — goes in chunks, inside one transaction so it stays all-or-nothing.
        let mut statements = Vec::new();
        let mut events = events.into_iter().peekable();
        while events.peek().is_some() {
            let mut statement = Query::insert()
                .into_table(EventTable::Table)
                .columns(EVENT_COLUMNS)
                .to_owned();
            for event in events.by_ref().take(INSERT_CHUNK) {
                let metadata = bitcode::encode(&event.metadata);
                statement.values_panic([
                    event.id.to_string().into(),
                    event.name.into(),
                    event.aggregate_type.into(),
                    event.aggregate_id.into(),
                    event.version.into(),
                    event.data.into(),
                    metadata.into(),
                    event.routing_key.into(),
                    event.tenant.into(),
                    event.timestamp.into(),
                    event.timestamp_subsec.into(),
                ]);
            }
            statements.push(query::prepare(&statement));
        }

        let stamp_statement = stamp.map(|stamp| {
            query::prepare(
                &Query::insert()
                    .into_table(MetaTable::Table)
                    .columns([MetaTable::K, MetaTable::V])
                    .values_panic([LAST_STAMP_KEY.into(), stamp.to_be_bytes().to_vec().into()])
                    .on_conflict(
                        OnConflict::column(MetaTable::K)
                            .update_column(MetaTable::V)
                            .to_owned(),
                    )
                    .to_owned(),
            )
        });

        // One transaction, run again as a whole if another writer's commit
        // conflicts with it (only possible in a shared database); the unique
        // index still reports a version conflict as a constraint error.
        let env = (statements, stamp_statement);
        write::transaction(&mut write, self.mode, &env, |tx, (statements, stamp)| {
            Box::pin(async move {
                for q in statements {
                    query::execute_prepared(tx, q).await?;
                }
                if let Some(q) = stamp {
                    query::execute_prepared(tx, q).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|err| match err.downcast::<turso::Error>() {
            Ok(err) => insert_error(err),
            Err(err) => WriteError::Unknown(err),
        })?;

        if let Some(stamp) = stamp {
            // After the commit: a failed batch leaves the clock where it was.
            self.last_stamp.store(stamp, Ordering::Release);
        }
        Ok(())
    }
}

/// Structured detection of a version conflict. The only unique constraints on
/// `event` are the primary key (random ULID, collision-free in practice) and
/// the `(type, id, tenant, version)` index, so a unique violation means a
/// version conflict. Turso reports it as `Constraint("UNIQUE constraint
/// failed: …")`; other constraint kinds (`NOT NULL`, `CHECK`) are not
/// conflicts.
fn insert_error(err: turso::Error) -> WriteError {
    match err {
        turso::Error::Constraint(ref msg) if msg.starts_with("UNIQUE constraint failed") => {
            WriteError::InvalidOriginalVersion
        }
        err => WriteError::Unknown(err.into()),
    }
}

#[async_trait::async_trait]
impl Executor for Turso {
    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        let reader = {
            let mut statement = Query::select()
                .columns(EVENT_COLUMNS)
                .from(EventTable::Table)
                .to_owned();
            apply_filters(
                &mut statement,
                aggregators.as_deref(),
                routing_key.as_ref(),
                &tenant,
            );
            if let Some(bound) = to_micros {
                // Exclusive bound on the event stamp (`ts` seconds + `subsec`
                // milliseconds): stamp < bound ⇔ ts < bound_secs, or ts =
                // bound_secs and subsec·1000 < bound_rem_micros — the latter as
                // an integer comparison (subsec < ceil(rem / 1000)). The leading
                // `timestamp <` term keeps the predicate sargable.
                let secs = (bound / 1_000_000) as i64;
                let subsec_bound = ((bound % 1_000_000).div_ceil(1_000)) as i64;
                statement.and_where(
                    Expr::col(EventTable::Timestamp)
                        .lt(secs)
                        .or(Expr::col(EventTable::Timestamp)
                            .eq(secs)
                            .and(Expr::col(EventTable::TimestampSubsec).lt(subsec_bound))),
                );
            }
            let mut reader = Reader::new(statement);
            reader.args(args);
            reader
        };

        Ok(reader
            .execute::<EventRow>(&self.reader()?)
            .await?
            .map(|row| row.0))
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        let q = {
            let mut statement = Query::select()
                .expr(Func::max(Expr::col(EventTable::Timestamp)))
                .from(EventTable::Table)
                .to_owned();
            apply_filters(
                &mut statement,
                aggregators.as_deref(),
                routing_key.as_ref(),
                &tenant,
            );
            query::prepare(&statement)
        };
        let row = query::fetch_optional_prepared(&self.reader()?, q).await?;
        let ts = match row {
            Some(row) => query::opt_int(&row, 0)?,
            None => None,
        };
        Ok(ts.map(|v| u64::try_from(v).unwrap_or(0)).unwrap_or(0))
    }

    async fn latest_version(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        let q = query::prepare(
            &Query::select()
                .expr(Func::max(Expr::col(EventTable::Version)))
                .from(EventTable::Table)
                .and_where(Expr::col(EventTable::AggregatorType).eq(aggregate_type))
                .and_where(Expr::col(EventTable::AggregatorId).eq(aggregate_id))
                .and_where(Expr::col(EventTable::Tenant).eq(tenant))
                .to_owned(),
        );
        let row = query::fetch_optional_prepared(&self.reader()?, q).await?;
        let last = match row {
            Some(row) => query::opt_int(&row, 0)?,
            None => None,
        };
        Ok(last
            .map(|v| u16::try_from(v).unwrap_or(u16::MAX))
            .unwrap_or(0))
    }

    async fn stream_routing_key(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        // Seek on the unique `(type, id, tenant, version)` index: version 1
        // is the stream's first event and carries the routing key the stream
        // was created with.
        let q = query::prepare(
            &Query::select()
                .column(EventTable::RoutingKey)
                .from(EventTable::Table)
                .and_where(Expr::col(EventTable::AggregatorType).eq(aggregate_type))
                .and_where(Expr::col(EventTable::AggregatorId).eq(aggregate_id))
                .and_where(Expr::col(EventTable::Tenant).eq(tenant))
                .and_where(Expr::col(EventTable::Version).eq(1))
                .limit(1)
                .to_owned(),
        );
        match query::fetch_optional_prepared(&self.reader()?, q).await? {
            Some(row) => Ok(Some(query::opt_text(&row, 0)?)),
            None => Ok(None),
        }
    }

    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }

        // Contiguity check, per event: the unique `(type, id, tenant, version)`
        // index only catches duplicate versions, not a gap (writing version 11
        // when the aggregate is at 5), and a batch may carry several events of
        // one stream or several streams at once. `seen` carries the version
        // assigned earlier in THIS batch, so each stream costs one
        // `latest_version` round trip. A concurrent writer that advances a
        // version after its check can only make the batch collide on the
        // unique index — never create a gap — so check + index together
        // enforce contiguity. A brand-new stream (version 1) needs no lookup.
        let mut seen: std::collections::HashMap<(&str, &str, &str), u16> =
            std::collections::HashMap::new();
        for event in &events {
            let stream = (
                event.tenant.as_str(),
                event.aggregate_type.as_str(),
                event.aggregate_id.as_str(),
            );
            let last = match seen.get(&stream) {
                Some(last) => *last,
                None if event.version == 1 => 0,
                None => self
                    .latest_version(
                        event.tenant.clone(),
                        event.aggregate_type.clone(),
                        event.aggregate_id.clone(),
                    )
                    .await
                    .map_err(WriteError::Unknown)?,
            };
            if u64::from(event.version) != u64::from(last) + 1 {
                return Err(WriteError::InvalidOriginalVersion);
            }
            seen.insert(stream, event.version);
        }

        self.insert_events(events, true).await?;

        // Wake any in-process subscriptions immediately instead of waiting
        // for their next poll tick.
        self.write_watch.send_modify(|v| *v += 1);
        Ok(())
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }
        // Replication layers own ordering and versioning: persist the
        // caller's timestamps verbatim and skip the contiguity pre-check.
        self.insert_events(events, false).await?;
        self.write_watch.send_modify(|v| *v += 1);
        Ok(())
    }

    fn write_watch(&self) -> Option<watch::Receiver<u64>> {
        Some(self.write_watch.subscribe())
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        let q = query::prepare(
            &Query::select()
                .column(SubscriberTable::Cursor)
                .from(SubscriberTable::Table)
                .and_where(Expr::col(SubscriberTable::Key).eq(key))
                .limit(1)
                .to_owned(),
        );
        match query::fetch_optional_prepared(&self.reader()?, q).await? {
            Some(row) => Ok(query::opt_text(&row, 0)?.map(query::cursor_value)),
            None => Ok(None),
        }
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        Ok(self.subscriber_status(key, worker_id).await?.running)
    }

    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        let q = query::prepare(
            &Query::select()
                .columns([
                    SubscriberTable::WorkerId,
                    SubscriberTable::Enabled,
                    SubscriberTable::Cursor,
                ])
                .from(SubscriberTable::Table)
                .and_where(Expr::col(SubscriberTable::Key).eq(key))
                .limit(1)
                .to_owned(),
        );
        // A missing row (an operator deleted the subscriber to stop it) means
        // "not running", not an error.
        let Some(row) = query::fetch_optional_prepared(&self.reader()?, q).await? else {
            return Ok(SubscriberStatus::default());
        };
        let id = query::text(&row, 0)?;
        let enabled = query::int(&row, 1)? != 0;
        let cursor = query::opt_text(&row, 2)?.map(query::cursor_value);
        Ok(SubscriberStatus {
            running: worker_id.to_string() == id && enabled,
            cursor,
        })
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        let now = self.now_secs();
        let q = query::prepare(
            &Query::insert()
                .into_table(SubscriberTable::Table)
                .columns([
                    SubscriberTable::Key,
                    SubscriberTable::WorkerId,
                    SubscriberTable::Lag,
                    SubscriberTable::CreatedAt,
                ])
                .values_panic([
                    key.into(),
                    worker_id.to_string().into(),
                    0.into(),
                    now.into(),
                ])
                // Cursor, lag and enabled survive a takeover.
                .on_conflict(
                    OnConflict::column(SubscriberTable::Key)
                        .update_columns([SubscriberTable::WorkerId])
                        .value(SubscriberTable::UpdatedAt, now)
                        .to_owned(),
                )
                .to_owned(),
        );
        self.write_one(Durability::Relaxed, q).await?;
        Ok(())
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        let q = query::prepare(
            &Query::update()
                .table(SubscriberTable::Table)
                .values([
                    (SubscriberTable::Cursor, query::cursor_text(&cursor).into()),
                    (SubscriberTable::Lag, lag.into()),
                    (SubscriberTable::UpdatedAt, self.now_secs().into()),
                ])
                .and_where(Expr::col(SubscriberTable::Key).eq(key))
                // Fenced on the worker id: a superseded worker's ack must not
                // rewind the cursor the new owner is advancing. The enabled
                // condition makes a disabled subscriber read as "lost" too,
                // matching `is_subscriber_running`.
                .and_where(Expr::col(SubscriberTable::WorkerId).eq(worker_id.to_string()))
                .and_where(Expr::col(SubscriberTable::Enabled).eq(1))
                .to_owned(),
        );
        // Zero matched rows means the fenced update was a no-op: the key is
        // owned by another worker, disabled, or gone.
        let affected = self.write_one(Durability::Relaxed, q).await?;
        Ok(affected > 0)
    }

    async fn get_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        let q = query::prepare(
            &Query::select()
                .columns([SnapshotTable::Data, SnapshotTable::Cursor])
                .from(SnapshotTable::Table)
                .and_where(Expr::col(SnapshotTable::Tenant).eq(tenant))
                .and_where(Expr::col(SnapshotTable::Type).eq(aggregate_type))
                .and_where(Expr::col(SnapshotTable::Projection).eq(projection))
                .and_where(Expr::col(SnapshotTable::Id).eq(id))
                // A revision mismatch is a miss.
                .and_where(Expr::col(SnapshotTable::Revision).eq(aggregate_revision))
                .limit(1)
                .to_owned(),
        );
        match query::fetch_optional_prepared(&self.reader()?, q).await? {
            Some(row) => Ok(Some((
                query::blob(&row, 0)?,
                query::cursor_value(query::text(&row, 1)?),
            ))),
            None => Ok(None),
        }
    }

    async fn save_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        let now = self.now_secs();
        let q = query::prepare(
            &Query::insert()
                .into_table(SnapshotTable::Table)
                .columns([
                    SnapshotTable::Tenant,
                    SnapshotTable::Type,
                    SnapshotTable::Projection,
                    SnapshotTable::Id,
                    SnapshotTable::Cursor,
                    SnapshotTable::Revision,
                    SnapshotTable::Data,
                    SnapshotTable::CreatedAt,
                ])
                .values_panic([
                    tenant.into(),
                    aggregate_type.into(),
                    projection.into(),
                    id.into(),
                    query::cursor_text(&cursor).into(),
                    aggregate_revision.into(),
                    data.into(),
                    now.into(),
                ])
                .on_conflict(
                    OnConflict::columns([
                        SnapshotTable::Tenant,
                        SnapshotTable::Type,
                        SnapshotTable::Projection,
                        SnapshotTable::Id,
                    ])
                    .update_columns([
                        SnapshotTable::Data,
                        SnapshotTable::Cursor,
                        SnapshotTable::Revision,
                    ])
                    .value(SnapshotTable::UpdatedAt, now)
                    .to_owned(),
                )
                .to_owned(),
        );
        self.write_one(Durability::Relaxed, q).await?;
        Ok(())
    }

    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()> {
        let q = query::prepare(
            &Query::delete()
                .from_table(SnapshotTable::Table)
                .and_where(Expr::col(SnapshotTable::Tenant).eq(tenant))
                .and_where(Expr::col(SnapshotTable::Type).eq(aggregate_type))
                .and_where(Expr::col(SnapshotTable::Projection).eq(projection))
                .and_where(Expr::col(SnapshotTable::Id).eq(id))
                .to_owned(),
        );
        self.write_one(Durability::Relaxed, q).await?;
        Ok(())
    }

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        let q = query::prepare(
            &Query::delete()
                .from_table(EventTable::Table)
                .and_where(Expr::col(EventTable::Tenant).eq(tenant))
                .and_where(Expr::col(EventTable::AggregatorType).eq(aggregate_type))
                .and_where(Expr::col(EventTable::AggregatorId).eq(aggregate_id))
                .to_owned(),
        );
        self.write_one(Durability::Durable, q).await?;
        Ok(())
    }
}

/// `path` as the string Turso's builder takes.
fn path_str(path: &Path) -> anyhow::Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow::anyhow!("database path is not UTF-8"))
}

impl From<Turso> for evento_core::Evento {
    fn from(value: Turso) -> Self {
        evento_core::Evento::new(value)
    }
}

impl From<&Turso> for evento_core::Evento {
    fn from(value: &Turso) -> Self {
        evento_core::Evento::new(value.clone())
    }
}
