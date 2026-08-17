//! Core SQL implementation for event sourcing.

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;
use sea_query::{
    Cond, Expr, ExprTrait, Func, Iden, IntoColumnRef, OnConflict, Query, SelectStatement,
};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Database, Pool};
use ulid::Ulid;

use evento_core::{
    cursor::{self, Args, Cursor, Edge, PageInfo, ReadResult, Value},
    EventFilter, Executor, SubscriberStatus, WriteError,
};

/// Dialect-agnostic access to a statement's affected-row count.
///
/// sqlx exposes `rows_affected` only on each driver's concrete `QueryResult`;
/// this bridges them so the fenced UPDATE in [`Executor::acknowledge`] can
/// report ownership without a follow-up SELECT. On MySQL sqlx connects with
/// `CLIENT_FOUND_ROWS`, so the count is rows *matched* (not changed) — a
/// re-ack of an identical cursor still reports the row as owned.
pub trait RowsAffected {
    /// Number of rows matched/affected by the statement.
    fn affected_rows(&self) -> u64;
}

#[cfg(feature = "sqlite")]
impl RowsAffected for sqlx::sqlite::SqliteQueryResult {
    fn affected_rows(&self) -> u64 {
        self.rows_affected()
    }
}

#[cfg(feature = "mysql")]
impl RowsAffected for sqlx::mysql::MySqlQueryResult {
    fn affected_rows(&self) -> u64 {
        self.rows_affected()
    }
}

#[cfg(feature = "postgres")]
impl RowsAffected for sqlx::postgres::PgQueryResult {
    fn affected_rows(&self) -> u64 {
        self.rows_affected()
    }
}

/// Column identifiers for the `event` table.
///
/// Used with sea-query for type-safe SQL query construction.
///
/// # Columns
///
/// - `Id` - Event identifier (ULID format, VARCHAR(26))
/// - `Name` - Event type name (VARCHAR(50))
/// - `AggregatorType` - Aggregate root type (VARCHAR(50))
/// - `AggregatorId` - Aggregate root instance ID (VARCHAR(64))
/// - `Version` - Event sequence number within the aggregate
/// - `Data` - Serialized event payload (BLOB, bitcode format)
/// - `Metadata` - Serialized event metadata (BLOB, bitcode format)
/// - `RoutingKey` - Optional routing key for partitioning (VARCHAR(50))
/// - `Timestamp` - Event timestamp in seconds (BIGINT)
/// - `TimestampSubsec` - Sub-second precision (BIGINT)
#[derive(Iden, Clone)]
pub enum Event {
    /// The table name: `event`
    Table,
    /// Event ID column (ULID)
    Id,
    /// Event type name
    Name,
    /// Aggregate root type
    AggregatorType,
    /// Aggregate root instance ID
    AggregatorId,
    /// Event version/sequence number
    Version,
    /// Serialized event data
    Data,
    /// Serialized event metadata
    Metadata,
    /// Optional routing key
    RoutingKey,
    /// Timestamp in seconds
    Timestamp,
    /// Sub-second precision
    TimestampSubsec,
}

/// Column identifiers for the `snapshot` table.
///
/// Used with sea-query for type-safe SQL query construction. The snapshot table
/// backs projection snapshots via [`Executor::get_snapshot`], [`Executor::save_snapshot`],
/// and [`Executor::delete_snapshot`].
#[derive(Iden)]
pub enum Snapshot {
    /// The table name: `snapshot`
    Table,
    /// Snapshot ID
    Id,
    /// Snapshot type
    Type,
    /// Event stream cursor position
    Cursor,
    /// Revision identifier
    Revision,
    /// Serialized snapshot data
    Data,
    /// Creation timestamp
    CreatedAt,
    /// Last update timestamp
    UpdatedAt,
}

/// Column identifiers for the `subscriber` table.
///
/// Used with sea-query for type-safe SQL query construction.
///
/// # Columns
///
/// - `Key` - Subscriber identifier (primary key)
/// - `WorkerId` - ULID of the current worker processing events
/// - `Cursor` - Current position in the event stream
/// - `Lag` - Seconds behind the newest matching event (not an event count)
/// - `Enabled` - Whether the subscription is active
/// - `CreatedAt` / `UpdatedAt` - Timestamps
#[derive(Iden)]
pub enum Subscriber {
    /// The table name: `subscriber`
    Table,
    /// Subscriber key (primary key)
    Key,
    /// Current worker ID (ULID)
    WorkerId,
    /// Current cursor position
    Cursor,
    /// Event lag counter
    Lag,
    /// Whether subscription is enabled
    Enabled,
    /// Creation timestamp
    CreatedAt,
    /// Last update timestamp
    UpdatedAt,
}

/// Type alias for MySQL executor.
///
/// Equivalent to `Sql<sqlx::MySql>`.
#[cfg(feature = "mysql")]
pub type MySql = Sql<sqlx::MySql>;

/// Read-write executor pair for MySQL.
///
/// Used in CQRS patterns where you may have separate read and write connections.
#[cfg(feature = "mysql")]
pub type RwMySql = evento_core::Rw<MySql, MySql>;

/// Type alias for PostgreSQL executor.
///
/// Equivalent to `Sql<sqlx::Postgres>`.
#[cfg(feature = "postgres")]
pub type Postgres = Sql<sqlx::Postgres>;

/// Read-write executor pair for PostgreSQL.
///
/// Used in CQRS patterns where you may have separate read and write connections.
#[cfg(feature = "postgres")]
pub type RwPostgres = evento_core::Rw<Postgres, Postgres>;

/// Type alias for SQLite executor.
///
/// Equivalent to `Sql<sqlx::Sqlite>`.
#[cfg(feature = "sqlite")]
pub type Sqlite = Sql<sqlx::Sqlite>;

/// Read-write executor pair for SQLite.
///
/// Used in CQRS patterns where you may have separate read and write connections.
#[cfg(feature = "sqlite")]
pub type RwSqlite = evento_core::Rw<Sqlite, Sqlite>;

/// SQL database executor for event sourcing operations.
///
/// A generic wrapper around a SQLx connection pool that implements the
/// [`Executor`](evento_core::Executor) trait for storing and querying events.
///
/// # Type Parameters
///
/// - `DB` - The SQLx database type (e.g., `sqlx::Sqlite`, `sqlx::MySql`, `sqlx::Postgres`)
///
/// # Example
///
/// ```rust,ignore
/// use evento_sql::Sql;
/// use sqlx::sqlite::SqlitePoolOptions;
///
/// // Create a connection pool
/// let pool = SqlitePoolOptions::new()
///     .connect(":memory:")
///     .await?;
///
/// // Convert to Sql executor
/// let executor: Sql<sqlx::Sqlite> = pool.into();
///
/// // Or use the type alias
/// let executor: evento_sql::Sqlite = pool.into();
/// ```
///
/// # Executor Implementation
///
/// The `Sql` type implements [`Executor`](evento_core::Executor) with the following operations:
///
/// - **`read`** - Query events with filtering and cursor-based pagination
/// - **`write`** - Persist events with optimistic concurrency control
/// - **`get_subscriber_cursor`** - Get the current cursor position for a subscriber
/// - **`is_subscriber_running`** - Check if a subscriber is active with a specific worker
/// - **`upsert_subscriber`** - Create or update a subscriber record
/// - **`acknowledge`** - Update subscriber cursor after processing events
///
/// # Ordering and the stability watermark
///
/// `write` stamps `timestamp`/`timestamp_subsec` with the **database server
/// clock** (statement time), so cursor order cannot be skewed by writer-host
/// clocks. Because several processes can still commit out of cursor order
/// within a small window, `stable_timestamp` returns DB-server time minus
/// [`stable_margin`](Self::stable_margin) (default 1s) and subscriptions only
/// process events below that watermark. The margin must exceed the worst-case
/// duration of a single event INSERT (plus replica lag when reading from a
/// replica via `Rw`); subscription end-to-end latency grows by roughly the
/// margin.
pub struct Sql<DB: Database> {
    pool: Pool<DB>,
    write_watch: tokio::sync::watch::Sender<u64>,
    stable_margin: Duration,
    /// Last DB-server clock sample: (when it was taken locally, DB `now()` in
    /// microseconds). Shared across clones so one re-sample per TTL serves
    /// every subscription on this executor.
    clock_sample: Arc<Mutex<Option<(Instant, u64)>>>,
}

/// Default stability margin for [`Sql::stable_margin`].
const DEFAULT_STABLE_MARGIN: Duration = Duration::from_secs(1);

/// How long a DB-clock sample may serve [`Executor::stable_timestamp`] before
/// being refreshed with a round trip. Must stay well under any configured
/// [`Sql::stable_margin`], which already absorbs far more skew than a
/// TTL-stale sample introduces.
const CLOCK_SAMPLE_TTL: Duration = Duration::from_secs(1);

impl<DB: Database> Sql<DB> {
    /// Sets the stability margin subtracted from DB-server time to form the
    /// subscription watermark (default 1s).
    ///
    /// Lower values reduce subscription latency but must stay above the
    /// worst-case commit duration of a single event INSERT — otherwise a slow
    /// commit can land below an already-acknowledged cursor and be skipped.
    pub fn stable_margin(mut self, v: Duration) -> Self {
        self.stable_margin = v;
        self
    }

    fn build_sqlx<S: SqlxBinder>(statement: S) -> (String, sea_query_sqlx::SqlxValues) {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }

    /// Per-dialect SQL expressions evaluating to the DB server's current time
    /// as `(whole seconds, milliseconds-within-second)`.
    ///
    /// All date/time functions are statement-stable on every supported
    /// backend (SQLite caches the instant per statement, MySQL's `NOW()` is
    /// statement-start, Postgres' `now()` is transaction-start), so the two
    /// columns — and every row of a multi-event batch — always agree.
    fn server_time_exprs() -> (Expr, Expr) {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => (
                Expr::cust("CAST(unixepoch('subsec') AS INTEGER)"),
                Expr::cust("CAST(unixepoch('subsec') * 1000 AS INTEGER) % 1000"),
            ),
            #[cfg(feature = "mysql")]
            "MySQL" => (
                Expr::cust("FLOOR(UNIX_TIMESTAMP(NOW(3)))"),
                Expr::cust("MOD(FLOOR(UNIX_TIMESTAMP(NOW(3)) * 1000), 1000)"),
            ),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => (
                Expr::cust("FLOOR(EXTRACT(EPOCH FROM now()))::BIGINT"),
                Expr::cust("MOD(FLOOR(EXTRACT(EPOCH FROM now()) * 1000)::BIGINT, 1000)"),
            ),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }

    /// SQL expression evaluating to the DB server's current time in microseconds.
    fn server_time_micros_expr() -> &'static str {
        match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => "CAST(unixepoch('subsec') * 1000000 AS INTEGER)",
            #[cfg(feature = "mysql")]
            "MySQL" => "CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS SIGNED)",
            #[cfg(feature = "postgres")]
            "PostgreSQL" => "(EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT",
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        }
    }
}

#[async_trait::async_trait]
impl<DB> Executor for Sql<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    bool: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    i64: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
    DB::QueryResult: RowsAffected,
{
    async fn read(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<evento_core::RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<evento_core::Event>> {
        let to_micros = args.to_micros;
        let statement = Query::select()
            .columns([
                Event::Id,
                Event::Name,
                Event::AggregatorType,
                Event::AggregatorId,
                Event::Version,
                Event::Data,
                Event::Metadata,
                Event::RoutingKey,
                Event::Timestamp,
                Event::TimestampSubsec,
            ])
            .from(Event::Table)
            .conditions(
                aggregators.is_some(),
                |q| {
                    let Some(aggregators) = aggregators else {
                        return;
                    };

                    let mut cond = Cond::any();

                    for aggregator in aggregators {
                        let mut aggregator_cond = Cond::all()
                            .add(Expr::col(Event::AggregatorType).eq(aggregator.aggregate_type));

                        if let Some(id) = aggregator.aggregate_id {
                            aggregator_cond =
                                aggregator_cond.add(Expr::col(Event::AggregatorId).eq(id));
                        }

                        if let Some(name) = aggregator.name {
                            aggregator_cond = aggregator_cond.add(Expr::col(Event::Name).eq(name));
                        }

                        cond = cond.add(aggregator_cond);
                    }

                    q.and_where(cond.into());
                },
                |_| {},
            )
            .conditions(
                matches!(routing_key, Some(evento_core::RoutingKey::Value(_))),
                |q| {
                    if let Some(evento_core::RoutingKey::Value(Some(ref routing_key))) = routing_key
                    {
                        q.and_where(Expr::col(Event::RoutingKey).eq(routing_key));
                    }

                    if let Some(evento_core::RoutingKey::Value(None)) = routing_key {
                        q.and_where(Expr::col(Event::RoutingKey).is_null());
                    }
                },
                |_q| {},
            )
            .conditions(
                to_micros.is_some(),
                |q| {
                    let Some(bound) = to_micros else {
                        return;
                    };

                    // Exclusive bound on the event stamp (`ts` seconds +
                    // `subsec` milliseconds): stamp < bound ⇔
                    // ts < bound_secs, or ts = bound_secs and
                    // subsec·1000 < bound_rem_micros — the latter rewritten as
                    // an integer comparison (subsec < ceil(rem / 1000)). The
                    // leading `timestamp <` term keeps the predicate sargable.
                    let secs = (bound / 1_000_000) as i64;
                    let subsec_bound = ((bound % 1_000_000).div_ceil(1_000)) as i64;
                    q.and_where(
                        Expr::col(Event::Timestamp)
                            .lt(secs)
                            .or(Expr::col(Event::Timestamp)
                                .eq(secs)
                                .and(Expr::col(Event::TimestampSubsec).lt(subsec_bound))),
                    );
                },
                |_q| {},
            )
            .to_owned();

        Ok(Reader::new(statement)
            .args(args)
            .execute::<_, SqlEvent, _>(&self.pool)
            .await?
            .map(|e| e.0))
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<evento_core::RoutingKey>,
    ) -> anyhow::Result<u64> {
        let statement = Query::select()
            .expr(Func::max(Expr::col(Event::Timestamp)))
            .from(Event::Table)
            .conditions(
                aggregators.is_some(),
                |q| {
                    let Some(aggregators) = aggregators else {
                        return;
                    };

                    let mut cond = Cond::any();

                    for aggregator in aggregators {
                        let mut aggregator_cond = Cond::all()
                            .add(Expr::col(Event::AggregatorType).eq(aggregator.aggregate_type));

                        if let Some(id) = aggregator.aggregate_id {
                            aggregator_cond =
                                aggregator_cond.add(Expr::col(Event::AggregatorId).eq(id));
                        }

                        if let Some(name) = aggregator.name {
                            aggregator_cond = aggregator_cond.add(Expr::col(Event::Name).eq(name));
                        }

                        cond = cond.add(aggregator_cond);
                    }

                    q.and_where(cond.into());
                },
                |_| {},
            )
            .conditions(
                matches!(routing_key, Some(evento_core::RoutingKey::Value(_))),
                |q| {
                    if let Some(evento_core::RoutingKey::Value(Some(ref routing_key))) = routing_key
                    {
                        q.and_where(Expr::col(Event::RoutingKey).eq(routing_key));
                    }

                    if let Some(evento_core::RoutingKey::Value(None)) = routing_key {
                        q.and_where(Expr::col(Event::RoutingKey).is_null());
                    }
                },
                |_q| {},
            )
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let (ts,): (Option<i64>,) =
            sqlx::query_as_with::<DB, (Option<i64>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_one(&self.pool)
                .await?;

        Ok(ts.map(|v| if v < 0 { 0 } else { v as u64 }).unwrap_or(0))
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        let statement = Query::select()
            .columns([Subscriber::Cursor])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let Some((cursor,)) = sqlx::query_as_with::<DB, (Option<String>,), _>(
            sqlx::AssertSqlSafe(sql.as_str()),
            values,
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        Ok(cursor.map(|c| c.into()))
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        let statement = Query::select()
            .columns([Subscriber::WorkerId, Subscriber::Enabled])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        // A missing row (e.g. an operator deleted the subscriber to stop it)
        // means "not running", not an error.
        let Some((id, enabled)) =
            sqlx::query_as_with::<DB, (String, bool), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_optional(&self.pool)
                .await?
        else {
            return Ok(false);
        };

        Ok(worker_id.to_string() == id && enabled)
    }

    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        let statement = Query::select()
            .columns([
                Subscriber::WorkerId,
                Subscriber::Enabled,
                Subscriber::Cursor,
            ])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        // A missing row (e.g. an operator deleted the subscriber to stop it)
        // means "not running", not an error.
        let Some((id, enabled, cursor)) = sqlx::query_as_with::<
            DB,
            (String, bool, Option<String>),
            _,
        >(sqlx::AssertSqlSafe(sql.as_str()), values)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(SubscriberStatus::default());
        };

        Ok(SubscriberStatus {
            running: worker_id.to_string() == id && enabled,
            cursor: cursor.map(Into::into),
        })
    }

    async fn latest_version(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        // CAST so the result decodes as i64 on every dialect (Postgres
        // returns MAX(INT4) as INT4; MySQL spells the target type SIGNED).
        let max_version_expr = match DB::NAME {
            #[cfg(feature = "mysql")]
            "MySQL" => "CAST(MAX(version) AS SIGNED)",
            _ => "CAST(MAX(version) AS BIGINT)",
        };
        let statement = Query::select()
            .expr(Expr::cust(max_version_expr))
            .from(Event::Table)
            .and_where(Expr::col(Event::AggregatorType).eq(aggregate_type))
            .and_where(Expr::col(Event::AggregatorId).eq(aggregate_id))
            .to_owned();
        let (sql, values) = Self::build_sqlx(statement);
        let (last,): (Option<i64>,) =
            sqlx::query_as_with::<DB, (Option<i64>,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_one(&self.pool)
                .await?;

        Ok(last
            .map(|v| u16::try_from(v).unwrap_or(u16::MAX))
            .unwrap_or(0))
    }

    async fn stream_routing_key(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        // Seek on the unique `(type, id, version)` index: version 1 is the
        // stream's first event and carries the routing key the stream was
        // created with — no full event row (data/metadata blobs) needed.
        let statement = Query::select()
            .columns([Event::RoutingKey])
            .from(Event::Table)
            .and_where(Expr::col(Event::AggregatorType).eq(aggregate_type))
            .and_where(Expr::col(Event::AggregatorId).eq(aggregate_id))
            .and_where(Expr::col(Event::Version).eq(1))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let row = sqlx::query_as_with::<DB, (Option<String>,), _>(
            sqlx::AssertSqlSafe(sql.as_str()),
            values,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(routing_key,)| routing_key))
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        let statement = Query::insert()
            .into_table(Subscriber::Table)
            .columns([Subscriber::Key, Subscriber::WorkerId, Subscriber::Lag])
            .values_panic([key.into(), worker_id.to_string().into(), 0.into()])
            .on_conflict(
                OnConflict::column(Subscriber::Key)
                    .update_columns([Subscriber::WorkerId])
                    .value(Subscriber::UpdatedAt, Expr::current_timestamp())
                    .to_owned(),
            )
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn write(&self, events: Vec<evento_core::Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }

        // Contiguity check: the unique `(type, id, version)` index only catches
        // duplicate versions, not a gap (e.g. writing version 11 when the
        // aggregate is at 5). A concurrent writer that advances the version
        // after this check can only cause the batch to collide on the unique
        // index — never to create a gap — so check + index together enforce
        // contiguity. A brand-new stream (version 1) needs no pre-check: no
        // gap is possible below version 1, and if the stream already exists
        // the unique index turns the insert into `InvalidOriginalVersion`.
        if let Some(first) = events.first() {
            if first.version != 1 {
                let last = self
                    .latest_version(first.aggregate_type.clone(), first.aggregate_id.clone())
                    .await
                    .map_err(WriteError::Unknown)?;

                if u64::from(first.version) != u64::from(last) + 1 {
                    return Err(WriteError::InvalidOriginalVersion);
                }
            }
        }

        self.insert_events(events, true).await?;

        // Wake any in-process subscriptions immediately instead of waiting for
        // their next poll tick.
        self.write_watch.send_modify(|v| *v += 1);

        Ok(())
    }

    async fn replicate(&self, events: Vec<evento_core::Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }

        // Replication layers own ordering and versioning: persist the caller's
        // timestamps verbatim and skip the contiguity pre-check.
        self.insert_events(events, false).await?;
        self.write_watch.send_modify(|v| *v += 1);

        Ok(())
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.write_watch.subscribe())
    }

    async fn stable_timestamp(&self) -> anyhow::Result<Option<u64>> {
        // Derive DB-server "now" from a cached sample plus locally elapsed
        // time instead of a round trip per call — the subscription loop calls
        // this once per pass. The sample is taken *after* the query returns,
        // so the derived clock lags the true DB clock slightly (never leads
        // it), which only makes the watermark more conservative.
        let derived = {
            let guard = self.clock_sample.lock().expect("clock sample poisoned");
            guard.and_then(|(at, db_micros)| {
                let elapsed = at.elapsed();
                (elapsed < CLOCK_SAMPLE_TTL)
                    .then(|| db_micros.saturating_add(elapsed.as_micros() as u64))
            })
        };

        let now_micros = match derived {
            Some(v) => v,
            None => {
                let statement = Query::select()
                    .expr(Expr::cust(Self::server_time_micros_expr()))
                    .to_owned();
                let (sql, values) = Self::build_sqlx(statement);
                let (now_micros,): (i64,) =
                    sqlx::query_as_with::<DB, (i64,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                        .fetch_one(&self.pool)
                        .await?;

                let now_micros = u64::try_from(now_micros).unwrap_or(0);
                *self.clock_sample.lock().expect("clock sample poisoned") =
                    Some((Instant::now(), now_micros));
                now_micros
            }
        };

        Ok(Some(now_micros.saturating_sub(
            self.stable_margin.as_micros().min(u128::from(u64::MAX)) as u64,
        )))
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        let statement = Query::update()
            .table(Subscriber::Table)
            .values([
                (Subscriber::Cursor, cursor.0.into()),
                (Subscriber::Lag, lag.into()),
                (Subscriber::UpdatedAt, Expr::current_timestamp()),
            ])
            .and_where(Expr::col(Subscriber::Key).eq(key.as_str()))
            // Fenced on the worker id: a superseded worker's ack must not
            // rewind the cursor the new owner is advancing. The enabled
            // condition makes a disabled subscriber read as "lost" too,
            // matching `is_subscriber_running`.
            .and_where(Expr::col(Subscriber::WorkerId).eq(worker_id.to_string()))
            .and_where(Expr::col(Subscriber::Enabled).eq(true))
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        // Zero matched rows means the fenced update was a no-op: the key is
        // owned by another worker, disabled, or gone. `affected_rows` counts
        // *matched* rows on every dialect (sqlx's MySQL connection sets
        // CLIENT_FOUND_ROWS), so re-acking an identical cursor still reports
        // ownership.
        let result = sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(result.affected_rows() > 0)
    }

    async fn get_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        let statement = Query::select()
            .columns([Snapshot::Data, Snapshot::Cursor])
            .from(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Type).eq(Expr::value(aggregate_type)))
            .and_where(Expr::col(Snapshot::Id).eq(Expr::value(id)))
            .and_where(Expr::col(Snapshot::Revision).eq(Expr::value(aggregate_revision)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        Ok(sqlx::query_as_with::<DB, (Vec<u8>, String), _>(
            sqlx::AssertSqlSafe(sql.as_str()),
            values,
        )
        .fetch_optional(&self.pool)
        .await
        .map(|res| res.map(|(data, cursor)| (data, cursor.into())))?)
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        let statement = Query::insert()
            .into_table(Snapshot::Table)
            .columns([
                Snapshot::Type,
                Snapshot::Id,
                Snapshot::Cursor,
                Snapshot::Revision,
                Snapshot::Data,
            ])
            .values_panic([
                aggregate_type.into(),
                id.to_string().into(),
                cursor.to_string().into(),
                aggregate_revision.into(),
                data.into(),
            ])
            .on_conflict(
                OnConflict::columns([Snapshot::Type, Snapshot::Id])
                    .update_columns([Snapshot::Data, Snapshot::Cursor, Snapshot::Revision])
                    .value(Snapshot::UpdatedAt, Expr::current_timestamp())
                    .to_owned(),
            )
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        let statement = Query::delete()
            .from_table(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Type).eq(Expr::value(aggregate_type)))
            .and_where(Expr::col(Snapshot::Id).eq(Expr::value(id)))
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

impl<DB> Sql<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
{
    /// Inserts a batch of events; with `restamp`, `timestamp`/
    /// `timestamp_subsec` are replaced by DB-server statement time so cursor
    /// order matches commit order regardless of writer-host clocks.
    async fn insert_events(
        &self,
        events: Vec<evento_core::Event>,
        restamp: bool,
    ) -> Result<(), WriteError> {
        let mut statement = Query::insert()
            .into_table(Event::Table)
            .columns([
                Event::Id,
                Event::Name,
                Event::Data,
                Event::Metadata,
                Event::AggregatorType,
                Event::AggregatorId,
                Event::Version,
                Event::RoutingKey,
                Event::Timestamp,
                Event::TimestampSubsec,
            ])
            .to_owned();

        for event in events {
            let metadata = bitcode::encode(&event.metadata);
            let (timestamp, timestamp_subsec) = if restamp {
                Self::server_time_exprs()
            } else {
                (event.timestamp.into(), event.timestamp_subsec.into())
            };
            statement.values_panic([
                event.id.to_string().into(),
                event.name.into(),
                event.data.into(),
                metadata.into(),
                event.aggregate_type.into(),
                event.aggregate_id.into(),
                event.version.into(),
                event.routing_key.into(),
                timestamp,
                timestamp_subsec,
            ]);
        }

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                // Structured detection: driver `Display` strings are localized
                // (Postgres `lc_messages`, MySQL locale) and matching them
                // silently breaks optimistic concurrency on non-English
                // servers. The only unique constraints on `event` are the
                // primary key (random ULID, collision-free in practice) and
                // the `(type, id, version)` index, so a unique violation means
                // a version conflict.
                let is_unique = err
                    .as_database_error()
                    .is_some_and(|db_err| db_err.is_unique_violation());
                if is_unique {
                    WriteError::InvalidOriginalVersion
                } else {
                    WriteError::Unknown(err.into())
                }
            })?;

        Ok(())
    }
}

impl<D: Database> Clone for Sql<D> {
    fn clone(&self) -> Self {
        // `watch::Sender` clones share the same channel, so all clones of this
        // executor notify the same set of subscription receivers on write.
        Self {
            pool: self.pool.clone(),
            write_watch: self.write_watch.clone(),
            stable_margin: self.stable_margin,
            clock_sample: self.clock_sample.clone(),
        }
    }
}

impl<D: Database> From<Pool<D>> for Sql<D> {
    /// Builds an executor with a fresh write-wake channel and the default
    /// [`stable_margin`](Sql::stable_margin).
    ///
    /// Note: two executors built from the same pool via separate `.into()`
    /// calls do **not** share the wake channel — a subscription started on one
    /// is not woken by writes through the other and falls back to its poll
    /// interval. Build once and [`Clone`] (clones share the channel) when
    /// low-latency wakeups matter.
    fn from(value: Pool<D>) -> Self {
        Self {
            pool: value,
            write_watch: tokio::sync::watch::channel(0).0,
            stable_margin: DEFAULT_STABLE_MARGIN,
            clock_sample: Arc::new(Mutex::new(None)),
        }
    }
}

/// Query builder for reading events with cursor-based pagination.
///
/// `Reader` wraps a sea-query [`SelectStatement`] and adds support for:
/// - Forward pagination (first N after cursor)
/// - Backward pagination (last N before cursor)
/// - Ascending/descending order
///
/// # Example
///
/// ```rust,ignore
/// use evento_sql::{Reader, Event};
/// use sea_query::Query;
///
/// let statement = Query::select()
///     .columns([Event::Id, Event::Name, Event::Data])
///     .from(Event::Table)
///     .to_owned();
///
/// let result = Reader::new(statement)
///     .forward(10, None)  // First 10 events
///     .execute::<_, MyEvent, _>(&pool)
///     .await?;
///
/// for edge in result.edges {
///     println!("Event: {:?}, Cursor: {:?}", edge.node, edge.cursor);
/// }
///
/// // Continue with next page
/// if result.page_info.has_next_page {
///     let next_result = Reader::new(statement)
///         .forward(10, result.page_info.end_cursor)
///         .execute::<_, MyEvent, _>(&pool)
///         .await?;
/// }
/// ```
///
/// # Deref
///
/// `Reader` implements `Deref` and `DerefMut` to the underlying `SelectStatement`,
/// allowing direct access to sea-query builder methods.
pub struct Reader {
    statement: SelectStatement,
    args: Args,
    order: cursor::Order,
}

impl Reader {
    /// Creates a new reader from a sea-query select statement.
    pub fn new(statement: SelectStatement) -> Self {
        Self {
            statement,
            args: Args::default(),
            order: cursor::Order::Asc,
        }
    }

    /// Sets the sort order for results.
    pub fn order(&mut self, order: cursor::Order) -> &mut Self {
        self.order = order;

        self
    }

    /// Sets descending sort order.
    pub fn desc(&mut self) -> &mut Self {
        self.order(cursor::Order::Desc)
    }

    /// Sets pagination arguments directly.
    pub fn args(&mut self, args: Args) -> &mut Self {
        self.args = args;

        self
    }

    /// Configures backward pagination (last N before cursor).
    ///
    /// # Arguments
    ///
    /// - `last` - Number of items to return
    /// - `before` - Optional cursor to paginate before
    pub fn backward(&mut self, last: u16, before: Option<Value>) -> &mut Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    /// Configures forward pagination (first N after cursor).
    ///
    /// # Arguments
    ///
    /// - `first` - Number of items to return
    /// - `after` - Optional cursor to paginate after
    pub fn forward(&mut self, first: u16, after: Option<Value>) -> &mut Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    /// Executes the query and returns paginated results.
    ///
    /// # Type Parameters
    ///
    /// - `DB` - The SQLx database type
    /// - `O` - The output row type (must implement `FromRow`, `Cursor`, and `Bind`)
    /// - `E` - The executor type
    ///
    /// # Returns
    ///
    /// A [`ReadResult`](evento_core::cursor::ReadResult) containing edges with nodes and cursors,
    /// plus pagination info.
    pub async fn execute<'e, 'c: 'e, DB, O, E>(
        &mut self,
        executor: E,
    ) -> anyhow::Result<ReadResult<O>>
    where
        DB: Database,
        E: 'e + sqlx::Executor<'c, Database = DB>,
        O: for<'r> sqlx::FromRow<'r, DB::Row>,
        O: Cursor,
        O: Send + Unpin,
        O: Bind<Cursor = O>,
        <<O as Bind>::I as IntoIterator>::IntoIter: DoubleEndedIterator,
        <<O as Bind>::V as IntoIterator>::IntoIter: DoubleEndedIterator,
        sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    {
        let limit = self.build_reader::<O, O>()?;

        let (sql, values) = match DB::NAME {
            #[cfg(feature = "sqlite")]
            "SQLite" => self.statement.build_sqlx(SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            "MySQL" => self.build_sqlx(MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            "PostgreSQL" => self.build_sqlx(PostgresQueryBuilder),
            name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
        };

        let mut rows = sqlx::query_as_with::<DB, O, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_all(executor)
            .await?;

        let has_more = rows.len() > limit as usize;
        if has_more {
            rows.pop();
        }

        let mut edges = vec![];
        for node in rows.into_iter() {
            edges.push(Edge {
                cursor: node.serialize_cursor()?,
                node,
            });
        }

        if self.args.is_backward() {
            edges = edges.into_iter().rev().collect();
        }

        // Both boundary cursors are always populated so a caller can reverse
        // direction from either end of a page. Only the paging direction's
        // "more" flag can be computed from the probe row; the opposite flag
        // stays `false` (unknown), per the GraphQL cursor-connection spec.
        let start_cursor = edges.first().map(|e| e.cursor.clone());
        let end_cursor = edges.last().map(|e| e.cursor.clone());
        let page_info = if self.args.is_backward() {
            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor,
            }
        } else {
            PageInfo {
                has_previous_page: false,
                has_next_page: has_more,
                start_cursor,
                end_cursor,
            }
        };

        Ok(ReadResult { edges, page_info })
    }

    fn build_reader<O: Cursor, B: Bind<Cursor = O>>(&mut self) -> Result<u16, cursor::CursorError>
    where
        B::T: Clone,
        <<B as Bind>::I as IntoIterator>::IntoIter: DoubleEndedIterator,
        <<B as Bind>::V as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        let (limit, cursor) = self.args.get_info();

        if let Some(cursor) = cursor.as_ref() {
            self.build_reader_where::<O, B>(cursor)?;
        }

        self.build_reader_order::<B>();
        self.limit(limit as u64 + 1);

        Ok(limit)
    }

    fn build_reader_where<O, B>(&mut self, cursor: &Value) -> Result<(), cursor::CursorError>
    where
        O: Cursor,
        B: Bind<Cursor = O>,
        B::T: Clone,
        <<B as Bind>::I as IntoIterator>::IntoIter: DoubleEndedIterator,
        <<B as Bind>::V as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        let is_order_desc = self.is_order_desc();
        let cursor = O::deserialize_cursor(cursor)?;
        let columns = B::columns().into_iter().rev();
        let values = B::values(cursor).into_iter().rev();

        let mut expr = None::<Expr>;
        for (col, value) in columns.zip(values) {
            let current_expr = if is_order_desc {
                Expr::col(col.clone()).lt(value.clone())
            } else {
                Expr::col(col.clone()).gt(value.clone())
            };

            let Some(ref prev_expr) = expr else {
                expr = Some(current_expr.clone());
                continue;
            };

            expr = Some(current_expr.or(Expr::col(col).eq(value).and(prev_expr.clone())));
        }

        // `expr` is only `None` when a `Bind` impl declares zero columns; such
        // a cursor cannot constrain anything, so leave the query unfiltered
        // rather than panicking on a public-trait misuse.
        if let Some(expr) = expr {
            self.and_where(expr);
        }

        Ok(())
    }

    fn build_reader_order<O: Bind>(&mut self) {
        let order = if self.is_order_desc() {
            sea_query::Order::Desc
        } else {
            sea_query::Order::Asc
        };

        let columns = O::columns();
        for col in columns {
            self.order_by(col, order.clone());
        }
    }

    fn is_order_desc(&self) -> bool {
        matches!(
            (&self.order, self.args.is_backward()),
            (cursor::Order::Asc, true) | (cursor::Order::Desc, false)
        )
    }
}

impl Deref for Reader {
    type Target = SelectStatement;

    fn deref(&self) -> &Self::Target {
        &self.statement
    }
}

impl DerefMut for Reader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.statement
    }
}

/// Trait for binding cursor values in paginated queries.
///
/// This trait defines how to serialize cursor data for keyset pagination.
/// It specifies which columns are used for ordering and how to extract
/// their values from a cursor.
///
/// # Implementation
///
/// The trait is implemented for [`evento_core::Event`] to enable pagination
/// over the event table using timestamp, version, and ID columns.
///
/// # Associated Types
///
/// - `T` - Column reference type
/// - `I` - Iterator over column references
/// - `V` - Iterator over value expressions
/// - `Cursor` - The cursor type that provides pagination data
pub trait Bind {
    /// Column reference type (e.g., `Event` enum variant).
    type T: IntoColumnRef + Clone;
    /// Iterator type for columns.
    type I: IntoIterator<Item = Self::T>;
    /// Iterator type for values.
    type V: IntoIterator<Item = Expr>;
    /// The cursor type used for pagination.
    type Cursor: Cursor;

    /// Returns the columns used for cursor-based ordering.
    fn columns() -> Self::I;
    /// Extracts values from a cursor for WHERE clause construction.
    fn values(cursor: <<Self as Bind>::Cursor as Cursor>::T) -> Self::V;
}

impl evento_core::cursor::Cursor for SqlEvent {
    type T = evento_core::EventCursor;

    fn serialize(&self) -> Self::T {
        evento_core::EventCursor {
            i: self.0.id.to_string(),
            v: self.0.version,
            t: self.0.timestamp,
            s: self.0.timestamp_subsec,
        }
    }
}

impl Bind for SqlEvent {
    type T = Event;
    type I = [Self::T; 4];
    type V = [Expr; 4];
    type Cursor = Self;

    fn columns() -> Self::I {
        [
            Event::Timestamp,
            Event::TimestampSubsec,
            Event::Version,
            Event::Id,
        ]
    }

    fn values(cursor: <<Self as Bind>::Cursor as Cursor>::T) -> Self::V {
        [
            cursor.t.into(),
            cursor.s.into(),
            cursor.v.into(),
            cursor.i.into(),
        ]
    }
}

#[cfg(feature = "sqlite")]
impl From<Sqlite> for evento_core::Evento {
    fn from(value: Sqlite) -> Self {
        evento_core::Evento::new(value)
    }
}

#[cfg(feature = "sqlite")]
impl From<&Sqlite> for evento_core::Evento {
    fn from(value: &Sqlite) -> Self {
        evento_core::Evento::new(value.clone())
    }
}

#[cfg(feature = "mysql")]
impl From<MySql> for evento_core::Evento {
    fn from(value: MySql) -> Self {
        evento_core::Evento::new(value)
    }
}

#[cfg(feature = "mysql")]
impl From<&MySql> for evento_core::Evento {
    fn from(value: &MySql) -> Self {
        evento_core::Evento::new(value.clone())
    }
}

#[cfg(feature = "postgres")]
impl From<Postgres> for evento_core::Evento {
    fn from(value: Postgres) -> Self {
        evento_core::Evento::new(value)
    }
}

#[cfg(feature = "postgres")]
impl From<&Postgres> for evento_core::Evento {
    fn from(value: &Postgres) -> Self {
        evento_core::Evento::new(value.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SqlEvent(pub evento_core::Event);

impl<R: sqlx::Row> sqlx::FromRow<'_, R> for SqlEvent
where
    i32: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    Vec<u8>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::Type<R::Database> + sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
{
    fn from_row(row: &R) -> Result<Self, sqlx::Error> {
        let timestamp: i64 = sqlx::Row::try_get(row, "timestamp")?;
        let timestamp_subsec: i64 = sqlx::Row::try_get(row, "timestamp_subsec")?;
        let version: i32 = sqlx::Row::try_get(row, "version")?;
        let metadata: Vec<u8> = sqlx::Row::try_get(row, "metadata")?;
        let metadata: evento_core::metadata::Metadata =
            bitcode::decode(&metadata).map_err(|e| sqlx::Error::Decode(e.into()))?;

        // Checked narrowing: an out-of-range column value (negative timestamp,
        // version above u16::MAX) is a decode error, not silent wraparound.
        let version = u16::try_from(version)
            .map_err(|_| sqlx::Error::Decode(format!("version {version} out of range").into()))?;
        let timestamp = u64::try_from(timestamp).map_err(|_| {
            sqlx::Error::Decode(format!("timestamp {timestamp} out of range").into())
        })?;
        let timestamp_subsec = u32::try_from(timestamp_subsec).map_err(|_| {
            sqlx::Error::Decode(format!("timestamp_subsec {timestamp_subsec} out of range").into())
        })?;

        Ok(SqlEvent(evento_core::Event {
            id: Ulid::from_string(sqlx::Row::try_get(row, "id")?)
                .map_err(|err| sqlx::Error::InvalidArgument(err.to_string()))?,
            aggregate_id: sqlx::Row::try_get(row, "aggregator_id")?,
            aggregate_type: sqlx::Row::try_get(row, "aggregator_type")?,
            version,
            name: sqlx::Row::try_get(row, "name")?,
            routing_key: sqlx::Row::try_get(row, "routing_key")?,
            data: sqlx::Row::try_get(row, "data")?,
            timestamp,
            timestamp_subsec,
            metadata,
        }))
    }
}
