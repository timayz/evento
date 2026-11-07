//! Core SQL implementation for event sourcing.

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sea_query::{
    Alias, Asterisk, Cond, Expr, ExprTrait, Func, Iden, IntoColumnRef, OnConflict, Query,
    SelectStatement, UnionType,
};
use sqlx::{Database, Pool};
use ulid::Ulid;

use evento_core::{
    cursor::{self, Args, Cursor, Edge, PageInfo, ReadResult, Value},
    EventFilter, Executor, FilterArm, RoutingKey, SubscriberStatus, TenantFilter, WriteError,
};

use crate::dialect::{build_sqlx, unsupported};

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
/// - `RoutingKey` - Optional routing key for partitioning (VARCHAR(255))
/// - `Tenant` - The tenant the aggregate belongs to; `''` is the default
///   tenant (VARCHAR(255), part of the stream's identity)
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
    /// The tenant the aggregate belongs to (`''` = default tenant)
    Tenant,
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
    /// The tenant the snapshotted aggregate belongs to
    Tenant,
    /// Snapshot ID
    Id,
    /// Snapshot type
    Type,
    /// Name of the projection the snapshot belongs to
    Projection,
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
/// ```rust,no_run
/// use evento_sql::Sql;
/// use sqlx::sqlite::SqlitePoolOptions;
///
/// # async fn run() -> anyhow::Result<()> {
/// // Create a connection pool
/// let pool = SqlitePoolOptions::new()
///     .connect(":memory:")
///     .await?;
///
/// // Convert to Sql executor
/// let executor: Sql<sqlx::Sqlite> = pool.clone().into();
///
/// // Or use the type alias
/// let executor: evento_sql::Sqlite = pool.into();
/// # let _ = executor;
/// # Ok(())
/// # }
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

/// Rows per `INSERT` into the event table: eleven bound values each keeps a
/// statement well under every backend's bound-value cap (SQLite: 32766).
const INSERT_CHUNK: usize = 1000;

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
            name => unsupported(name),
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
            name => unsupported(name),
        }
    }
}

/// The `WHERE` conditions of a filtered read, one per seek.
///
/// A single `WHERE (type = ? AND name = ?) OR (…)` is read by every planner
/// as one scan of what the arms share — the type at best, the whole table
/// when the types differ — so a subscription's pass cost time proportional to
/// the store, whatever its cursor (#478). So when the filters split into
/// pairwise disjoint arms ([`EventFilter::arms`]), each gets a condition of its
/// own — `type = ? [AND id = ?] [AND name = ? | name IN (…)]` — for the caller
/// to read as a `UNION ALL`, one index seek per arm: the `(type, id, tenant,
/// …)` index serves a stream, `idx_event_type_name_cursor` a `(type, name)`
/// arm and `idx_event_type_cursor` a `(type)` one. Filters that could match
/// the same event fall back to the single `OR` condition, so no event is
/// returned twice.
///
/// Shared by [`Executor::read`] and [`Executor::latest_timestamp`], so a
/// subscription's reads and its lag `MAX(timestamp)` always cover the same
/// rows. `aggregators` is `None` for every aggregate; an empty slice matches
/// nothing (sea-query renders an empty `Cond::any()` as `FALSE`). The routing
/// key and the tenant go on every condition.
fn filter_conditions(
    aggregators: Option<&[EventFilter]>,
    routing_key: Option<&RoutingKey>,
    tenant: &TenantFilter,
) -> Vec<Cond> {
    let scope = |mut cond: Cond| {
        match routing_key {
            Some(RoutingKey::Value(Some(key))) => {
                cond = cond.add(Expr::col(Event::RoutingKey).eq(key));
            }
            Some(RoutingKey::Value(None)) => {
                cond = cond.add(Expr::col(Event::RoutingKey).is_null());
            }
            Some(RoutingKey::All) | None => {}
        }
        if let TenantFilter::Only(tenant) = tenant {
            cond = cond.add(Expr::col(Event::Tenant).eq(tenant));
        }
        cond
    };

    let Some(aggregators) = aggregators else {
        return vec![scope(Cond::all())];
    };
    match EventFilter::arms(aggregators) {
        Some(arms) if !arms.is_empty() => arms
            .iter()
            .map(|arm| scope(Cond::all().add(arm_condition(arm))))
            .collect(),
        _ => {
            let mut any = Cond::any();
            for aggregator in aggregators {
                let mut cond = Cond::all()
                    .add(Expr::col(Event::AggregatorType).eq(aggregator.aggregate_type.clone()));
                if let Some(id) = &aggregator.aggregate_id {
                    cond = cond.add(Expr::col(Event::AggregatorId).eq(id.clone()));
                }
                if let Some(name) = &aggregator.name {
                    cond = cond.add(Expr::col(Event::Name).eq(name.clone()));
                }
                any = any.add(cond);
            }
            vec![scope(Cond::all().add(any))]
        }
    }
}

/// The condition of one arm: `type = ? [AND id = ?] [AND name = ? | name IN
/// (…)]`.
fn arm_condition(arm: &FilterArm<'_>) -> Cond {
    let mut cond = Cond::all().add(Expr::col(Event::AggregatorType).eq(arm.aggregate_type));
    if let Some(id) = arm.aggregate_id {
        cond = cond.add(Expr::col(Event::AggregatorId).eq(id));
    }
    match &arm.names {
        None => {}
        Some(names) if names.len() == 1 => {
            let name = *names.iter().next().expect("one name");
            cond = cond.add(Expr::col(Event::Name).eq(name));
        }
        Some(names) => {
            cond = cond.add(Expr::col(Event::Name).is_in(names.iter().copied()));
        }
    }
    cond
}

/// The reader of [`Executor::read`]: one `SELECT … FROM event WHERE cond` per
/// condition of [`filter_conditions`], the stamp bound `to_micros` on each.
fn read_reader(
    aggregators: Option<&[EventFilter]>,
    routing_key: Option<&RoutingKey>,
    tenant: &TenantFilter,
    to_micros: Option<u64>,
) -> Reader {
    let statements = filter_conditions(aggregators, routing_key, tenant)
        .into_iter()
        .map(|cond| {
            let mut statement = Query::select()
                .columns([
                    Event::Id,
                    Event::Name,
                    Event::AggregatorType,
                    Event::AggregatorId,
                    Event::Version,
                    Event::Data,
                    Event::Metadata,
                    Event::RoutingKey,
                    Event::Tenant,
                    Event::Timestamp,
                    Event::TimestampSubsec,
                ])
                .from(Event::Table)
                .cond_where(cond)
                .to_owned();

            if let Some(bound) = to_micros {
                // Exclusive bound on the event stamp (`ts` seconds +
                // `subsec` milliseconds): stamp < bound ⇔
                // ts < bound_secs, or ts = bound_secs and
                // subsec·1000 < bound_rem_micros — the latter rewritten as
                // an integer comparison (subsec < ceil(rem / 1000)). The
                // leading `timestamp <=` term is the half the planners take
                // as an index bound (PostgreSQL reads the `OR` as a filter).
                let secs = (bound / 1_000_000) as i64;
                let subsec_bound = ((bound % 1_000_000).div_ceil(1_000)) as i64;
                statement.and_where(
                    Expr::col(Event::Timestamp).lte(secs).and(
                        Expr::col(Event::Timestamp)
                            .lt(secs)
                            .or(Expr::col(Event::Timestamp)
                                .eq(secs)
                                .and(Expr::col(Event::TimestampSubsec).lt(subsec_bound))),
                    ),
                );
            }
            statement
        })
        .collect();
    Reader::arms(statements)
}

/// The statement of [`Executor::latest_timestamp`]: `MAX(timestamp)` under the
/// condition of [`filter_conditions`] — or, over several, the greatest of one
/// `MAX` per arm, each a seek to the arm's last index entry; the aggregate
/// `MAX` ignores the `NULL` of an arm without rows.
fn latest_timestamp_statement(
    aggregators: Option<&[EventFilter]>,
    routing_key: Option<&RoutingKey>,
    tenant: &TenantFilter,
) -> SelectStatement {
    let latest = Alias::new("latest");
    let mut arms = filter_conditions(aggregators, routing_key, tenant)
        .into_iter()
        .map(|cond| {
            Query::select()
                .expr_as(Func::max(Expr::col(Event::Timestamp)), latest.clone())
                .from(Event::Table)
                .cond_where(cond)
                .to_owned()
        });
    let first = arms.next().expect("a read has a condition");
    match arms.next() {
        None => first,
        Some(second) => {
            let mut union = first;
            union.union(UnionType::All, second);
            for arm in arms {
                union.union(UnionType::All, arm);
            }
            Query::select()
                .expr(Func::max(Expr::col(latest)))
                .from_subquery(union, Alias::new("arms"))
                .to_owned()
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
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<evento_core::RoutingKey>,
        tenant: TenantFilter,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<evento_core::Event>> {
        Ok(read_reader(
            aggregators.as_deref(),
            routing_key.as_ref(),
            &tenant,
            to_micros,
        )
        .args(args)
        .execute::<_, SqlEvent, _>(&self.pool)
        .await?
        .map(|e| e.0))
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<evento_core::RoutingKey>,
        tenant: TenantFilter,
    ) -> anyhow::Result<u64> {
        let statement =
            latest_timestamp_statement(aggregators.as_deref(), routing_key.as_ref(), &tenant);
        let (sql, values) = build_sqlx::<DB, _>(&statement);

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

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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
        tenant: String,
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
            .and_where(Expr::col(Event::Tenant).eq(tenant))
            .to_owned();
        let (sql, values) = build_sqlx::<DB, _>(&statement);
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
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        // Seek on the unique `(type, id, tenant, version)` index: version 1
        // is the stream's first event and carries the routing key the stream
        // was created with — no full event row (data/metadata blobs) needed.
        let statement = Query::select()
            .columns([Event::RoutingKey])
            .from(Event::Table)
            .and_where(Expr::col(Event::AggregatorType).eq(aggregate_type))
            .and_where(Expr::col(Event::AggregatorId).eq(aggregate_id))
            .and_where(Expr::col(Event::Tenant).eq(tenant))
            .and_where(Expr::col(Event::Version).eq(1))
            .limit(1)
            .to_owned();

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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

        let (sql, values) = build_sqlx::<DB, _>(&statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn write(&self, events: Vec<evento_core::Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }

        // Contiguity check, per event: the unique `(type, id, tenant, version)`
        // index only catches duplicate versions, not a gap (e.g. writing
        // version 11 when the aggregate is at 5), and a batch may carry
        // several events of one stream or several streams at once. `seen`
        // carries the version assigned earlier in THIS batch, so each stream
        // costs one `latest_version` round trip. A concurrent writer that
        // advances a version after its check can only make the batch collide
        // on the unique index — never create a gap — so check + index together
        // enforce contiguity. A brand-new stream (version 1) needs no lookup:
        // no gap is possible below version 1, and if the stream already
        // exists the unique index turns the insert into
        // `InvalidOriginalVersion`.
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
                let (sql, values) = build_sqlx::<DB, _>(&statement);
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

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        let statement = Query::select()
            .columns([Snapshot::Data, Snapshot::Cursor])
            .from(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Tenant).eq(Expr::value(tenant)))
            .and_where(Expr::col(Snapshot::Type).eq(Expr::value(aggregate_type)))
            .and_where(Expr::col(Snapshot::Projection).eq(Expr::value(projection)))
            .and_where(Expr::col(Snapshot::Id).eq(Expr::value(id)))
            .and_where(Expr::col(Snapshot::Revision).eq(Expr::value(aggregate_revision)))
            .limit(1)
            .to_owned();

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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
        tenant: String,
        aggregate_type: String,
        projection: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        let statement = Query::insert()
            .into_table(Snapshot::Table)
            .columns([
                Snapshot::Tenant,
                Snapshot::Type,
                Snapshot::Projection,
                Snapshot::Id,
                Snapshot::Cursor,
                Snapshot::Revision,
                Snapshot::Data,
            ])
            .values_panic([
                tenant.into(),
                aggregate_type.into(),
                projection.into(),
                id.to_string().into(),
                cursor.to_string().into(),
                aggregate_revision.into(),
                data.into(),
            ])
            .on_conflict(
                OnConflict::columns([
                    Snapshot::Tenant,
                    Snapshot::Type,
                    Snapshot::Projection,
                    Snapshot::Id,
                ])
                .update_columns([Snapshot::Data, Snapshot::Cursor, Snapshot::Revision])
                .value(Snapshot::UpdatedAt, Expr::current_timestamp())
                .to_owned(),
            )
            .to_owned();

        let (sql, values) = build_sqlx::<DB, _>(&statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn delete_snapshot(
        &self,
        tenant: String,
        aggregate_type: String,
        projection: String,
        id: String,
    ) -> anyhow::Result<()> {
        let statement = Query::delete()
            .from_table(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Tenant).eq(Expr::value(tenant)))
            .and_where(Expr::col(Snapshot::Type).eq(Expr::value(aggregate_type)))
            .and_where(Expr::col(Snapshot::Projection).eq(Expr::value(projection)))
            .and_where(Expr::col(Snapshot::Id).eq(Expr::value(id)))
            .to_owned();

        let (sql, values) = build_sqlx::<DB, _>(&statement);

        sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn delete_stream(
        &self,
        tenant: String,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<()> {
        let statement = Query::delete()
            .from_table(Event::Table)
            .and_where(Expr::col(Event::Tenant).eq(Expr::value(tenant)))
            .and_where(Expr::col(Event::AggregatorType).eq(Expr::value(aggregate_type)))
            .and_where(Expr::col(Event::AggregatorId).eq(Expr::value(aggregate_id)))
            .to_owned();

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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
        // One statement binds eleven values per row, and every backend caps the
        // bound values of a statement (SQLite at 32766): a large batch — a
        // joining replica's snapshot — goes in chunks, inside one transaction
        // so the batch stays all-or-nothing.
        let mut statements = Vec::new();
        let mut events = events.into_iter().peekable();
        while events.peek().is_some() {
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
                    Event::Tenant,
                    Event::Timestamp,
                    Event::TimestampSubsec,
                ])
                .to_owned();

            for event in events.by_ref().take(INSERT_CHUNK) {
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
                    event.tenant.into(),
                    timestamp,
                    timestamp_subsec,
                ]);
            }
            statements.push(build_sqlx::<DB, _>(&statement));
        }

        // A single statement needs no transaction. Taken by value: the bound
        // values carry every row's data and metadata blobs, and a copy of
        // them on the hot write path is pure waste.
        if statements.len() == 1 {
            let (sql, values) = statements.pop().expect("one statement");
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&self.pool)
                .await
                .map_err(Self::insert_error)?;
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| WriteError::Unknown(err.into()))?;
        for (sql, values) in statements {
            sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
                .execute(&mut *tx)
                .await
                .map_err(Self::insert_error)?;
        }
        tx.commit()
            .await
            .map_err(|err| WriteError::Unknown(err.into()))?;

        Ok(())
    }

    fn insert_error(err: sqlx::Error) -> WriteError {
        // Structured detection: driver `Display` strings are localized
        // (Postgres `lc_messages`, MySQL locale) and matching them silently
        // breaks optimistic concurrency on non-English servers. The only
        // unique constraints on `event` are the primary key (random ULID,
        // collision-free in practice) and the `(type, id, tenant, version)` index,
        // so a unique violation means a version conflict.
        let is_unique = err
            .as_database_error()
            .is_some_and(|db_err| db_err.is_unique_violation());
        if is_unique {
            WriteError::InvalidOriginalVersion
        } else {
            WriteError::Unknown(err.into())
        }
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
/// ```rust,no_run
/// use evento_sql::{Event, Reader, SqlEvent};
/// use sea_query::Query;
///
/// # async fn run(pool: sqlx::SqlitePool) -> anyhow::Result<()> {
/// let statement = Query::select()
///     .columns([Event::Id, Event::Name, Event::Data])
///     .from(Event::Table)
///     .to_owned();
///
/// let mut reader = Reader::new(statement);
/// let result = reader
///     .forward(10, None)  // First 10 events
///     .execute::<_, SqlEvent, _>(&pool)
///     .await?;
///
/// for edge in &result.edges {
///     println!("Event: {:?}, Cursor: {:?}", edge.node, edge.cursor);
/// }
///
/// // Continue with the next page on the same reader: each `execute` applies
/// // the cursor, order and limit to a copy of the statement.
/// if result.page_info.has_next_page {
///     let next_result = reader
///         .forward(10, result.page_info.end_cursor)
///         .execute::<_, SqlEvent, _>(&pool)
///         .await?;
/// #   let _ = next_result;
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Deref
///
/// `Reader` implements `Deref` and `DerefMut` to the underlying `SelectStatement`,
/// allowing direct access to sea-query builder methods.
///
/// # The keyset predicate
///
/// A page after a cursor is bounded by `a >= ? AND (a > ? OR (a = ? AND …))`:
/// the nested `OR` is the exact bound, and the redundant leading `a >= ?` on
/// the first sort column is the half a planner takes as an index range —
/// PostgreSQL reads the `OR` alone as a filter over every row of the
/// equality prefix.
pub struct Reader {
    /// The base statements: one for a plain read, several for
    /// [`arms`](Self::arms).
    statements: Vec<SelectStatement>,
    args: Args,
    order: cursor::Order,
}

impl Reader {
    /// Creates a new reader from a sea-query select statement.
    pub fn new(statement: SelectStatement) -> Self {
        Self::arms(vec![statement])
    }

    /// A reader over several base statements, paged as one: each arm is
    /// paged on its own (the keyset predicate, the order and the limit), and
    /// the `UNION ALL` of the arms is paged again. The top rows of the union
    /// of each arm's top rows are the top rows of the whole, so the page is
    /// the one a single `OR` of the arms would give — but each arm is an index
    /// seek where the planners scan the `OR` (see `filter_conditions`). The
    /// arms must be disjoint, or a row is returned twice. [`Deref`] gives the
    /// first arm.
    pub(crate) fn arms(statements: Vec<SelectStatement>) -> Self {
        debug_assert!(!statements.is_empty(), "a reader needs a statement");
        Self {
            statements,
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
        sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    {
        // The keyset predicate, the ORDER BY and the LIMIT go on a copy: the
        // stored statement stays the caller's base query, so the same reader
        // can be executed again (a retry, the next page, the other direction)
        // without stacking a second cursor bound on the first.
        let (statement, limit) = self.prepared::<O, O>()?;

        let (sql, values) = build_sqlx::<DB, _>(&statement);

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

    /// The statement a read runs — the keyset predicate, the ORDER BY and the
    /// LIMIT (+1 probe row) on every arm, the arms united and paged again —
    /// with the page size.
    pub(crate) fn prepared<O: Cursor, B: Bind<Cursor = O>>(
        &self,
    ) -> Result<(SelectStatement, u16), cursor::CursorError>
    where
        B::T: Clone,
    {
        let (limit, cursor) = self.args.get_info();
        let keyset = match cursor.as_ref() {
            Some(cursor) => self.keyset::<O, B>(cursor)?,
            None => None,
        };

        let mut arms = self.statements.iter().cloned().map(|mut arm| {
            if let Some(keyset) = &keyset {
                arm.and_where(keyset.clone());
            }
            self.page::<B>(&mut arm, limit);
            arm
        });
        let first = arms.next().expect("a reader has a statement");
        let statement = match arms.next() {
            None => first,
            Some(second) => {
                // No dialect takes an ORDER BY on a bare compound member, so
                // each arm is wrapped: `SELECT * FROM (arm) UNION ALL SELECT *
                // FROM (arm) …`, then the union is paged from a subquery.
                let wrap = |arm: SelectStatement| {
                    Query::select()
                        .column(Asterisk)
                        .from_subquery(arm, Alias::new("arm"))
                        .to_owned()
                };
                let mut union = wrap(first);
                union.union(UnionType::All, wrap(second));
                for arm in arms {
                    union.union(UnionType::All, wrap(arm));
                }
                let mut statement = Query::select()
                    .column(Asterisk)
                    .from_subquery(union, Alias::new("arms"))
                    .to_owned();
                self.page::<B>(&mut statement, limit);
                statement
            }
        };
        Ok((statement, limit))
    }

    /// The keyset predicate for `cursor`: nested from the last column out,
    /// `a > ? OR (a = ? AND (b > ? OR (b = ? AND c > ?)))` (`<` when
    /// descending), led by `a >= ?` (`<=`) when there is more than one
    /// column — the sargable half the planners seek on. `None` when the
    /// `Bind` declares no columns: such a cursor constrains nothing.
    fn keyset<O, B>(&self, cursor: &Value) -> Result<Option<Expr>, cursor::CursorError>
    where
        O: Cursor,
        B: Bind<Cursor = O>,
        B::T: Clone,
    {
        let is_order_desc = self.is_order_desc();
        let cursor = O::deserialize_cursor(cursor)?;
        let columns: Vec<B::T> = B::columns().into_iter().collect();
        let values: Vec<Expr> = B::values(cursor).into_iter().collect();

        let mut expr = None::<Expr>;
        for (col, value) in columns.iter().cloned().zip(values.iter().cloned()).rev() {
            let current = if is_order_desc {
                Expr::col(col.clone()).lt(value.clone())
            } else {
                Expr::col(col.clone()).gt(value.clone())
            };
            expr = Some(match expr {
                None => current,
                Some(prev) => current.or(Expr::col(col).eq(value).and(prev)),
            });
        }
        let Some(expr) = expr else {
            return Ok(None);
        };
        if columns.len() < 2 {
            return Ok(Some(expr));
        }
        let (col, value) = (columns[0].clone(), values[0].clone());
        let bound = if is_order_desc {
            Expr::col(col).lte(value)
        } else {
            Expr::col(col).gte(value)
        };
        Ok(Some(bound.and(expr)))
    }

    /// Adds the ORDER BY and the LIMIT (+1 probe row) to `statement`.
    fn page<B: Bind>(&self, statement: &mut SelectStatement, limit: u16) {
        let order = if self.is_order_desc() {
            sea_query::Order::Desc
        } else {
            sea_query::Order::Asc
        };
        for col in B::columns() {
            statement.order_by(col, order.clone());
        }
        statement.limit(u64::from(limit) + 1);
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
        &self.statements[0]
    }
}

impl DerefMut for Reader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.statements[0]
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

/// Newtype over [`evento_core::Event`] carrying the `sqlx::FromRow` mapping
/// from the events table.
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
            // A hand-built select without the column reads as the default
            // tenant, as every row stored before the column existed.
            tenant: match sqlx::Row::try_get(row, "tenant") {
                Ok(tenant) => tenant,
                Err(sqlx::Error::ColumnNotFound(_)) => String::new(),
                Err(err) => return Err(err),
            },
            data: sqlx::Row::try_get(row, "data")?,
            timestamp,
            timestamp_subsec,
            metadata,
        }))
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    //! The plans of the read shapes, on SQLite: every one is an index seek,
    //! with no scan of the type and no `MULTI-INDEX OR` (#478).

    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx_migrator::{Migrate, Plan};

    use super::*;

    fn event(
        aggregate_type: &str,
        aggregate_id: &str,
        name: &str,
        version: u16,
    ) -> evento_core::Event {
        evento_core::Event {
            id: Ulid::generate(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            name: name.into(),
            version,
            ..Default::default()
        }
    }

    /// An in-memory store at the crate schema, holding three events of two
    /// types.
    async fn store() -> anyhow::Result<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let mut conn = pool.acquire().await?;
        evento::sql_migrator::new::<sqlx::Sqlite>()?
            .run(&mut *conn, &Plan::apply_all())
            .await?;
        drop(conn);
        let store = Sqlite::from(pool);
        store
            .write(vec![
                event("demo/Page", "p1", "Created", 1),
                event("demo/Page", "p1", "Edited", 2),
                event("demo/Comment", "c1", "Posted", 1),
            ])
            .await?;
        Ok(store)
    }

    /// The cursor of the first event.
    async fn head(store: &Sqlite) -> anyhow::Result<Value> {
        let page = store
            .read(
                None,
                Some(RoutingKey::All),
                TenantFilter::All,
                Args::forward(1, None),
                None,
            )
            .await?;
        Ok(page.page_info.end_cursor.expect("an event"))
    }

    /// The `EXPLAIN QUERY PLAN` lines of `statement`.
    async fn plan(store: &Sqlite, statement: &SelectStatement) -> anyhow::Result<Vec<String>> {
        let (sql, values) = build_sqlx::<sqlx::Sqlite, _>(statement);
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let rows: Vec<(i64, i64, i64, String)> =
            sqlx::query_as_with(sqlx::AssertSqlSafe(explain.as_str()), values)
                .fetch_all(&store.pool)
                .await?;
        Ok(rows.into_iter().map(|row| row.3).collect())
    }

    /// The statement and plan of a read of `filters` after `cursor`.
    async fn read_plan(
        store: &Sqlite,
        filters: &[EventFilter],
        tenant: &TenantFilter,
        cursor: Option<Value>,
    ) -> anyhow::Result<(String, Vec<String>)> {
        let mut reader = read_reader(Some(filters), Some(&RoutingKey::All), tenant, None);
        reader.forward(100, cursor);
        let (statement, _) = reader.prepared::<SqlEvent, SqlEvent>()?;
        let (sql, _) = build_sqlx::<sqlx::Sqlite, _>(&statement);
        Ok((sql, plan(store, &statement).await?))
    }

    fn count(plan: &[String], needle: &str) -> usize {
        plan.iter().filter(|line| line.contains(needle)).count()
    }

    fn assert_no_scan(plan: &[String]) {
        assert_eq!(count(plan, "SCAN event"), 0, "{plan:#?}");
        assert_eq!(count(plan, "MULTI-INDEX OR"), 0, "{plan:#?}");
    }

    #[tokio::test]
    async fn a_read_by_event_seeks_the_name_then_the_cursor() -> anyhow::Result<()> {
        let store = store().await?;
        let cursor = head(&store).await?;
        let (sql, plan) = read_plan(
            &store,
            &[EventFilter::by_event_raw("demo/Page", "Edited")],
            &TenantFilter::All,
            Some(cursor),
        )
        .await?;
        assert!(!sql.contains("UNION"), "{sql}");
        assert_eq!(
            plan,
            vec![
                "SEARCH event USING INDEX idx_event_type_name_cursor (aggregator_type=? AND name=? AND timestamp>?)"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_read_by_type_seeks_the_cursor() -> anyhow::Result<()> {
        let store = store().await?;
        let cursor = head(&store).await?;
        let (_, plan) = read_plan(
            &store,
            &[EventFilter::by_type_raw("demo/Page")],
            &TenantFilter::All,
            Some(cursor),
        )
        .await?;
        assert_eq!(
            plan,
            vec!["SEARCH event USING INDEX idx_event_type_cursor (aggregator_type=? AND timestamp>?)"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_read_over_several_arms_seeks_each() -> anyhow::Result<()> {
        let store = store().await?;
        let cursor = head(&store).await?;
        let filters = [
            EventFilter::by_event_raw("demo/Page", "Edited"),
            EventFilter::by_event_raw("demo/Comment", "Posted"),
        ];
        let (sql, plan) = read_plan(&store, &filters, &TenantFilter::All, Some(cursor)).await?;
        assert_eq!(sql.matches("UNION ALL").count(), 1, "{sql}");
        assert_no_scan(&plan);
        assert_eq!(
            count(
                &plan,
                "SEARCH event USING INDEX idx_event_type_name_cursor (aggregator_type=? AND name=? AND timestamp>?)"
            ),
            2,
            "{plan:#?}"
        );
        // The arms are merged in order; each arm's sorter is over its own
        // page (at most `limit + 1` rows), not the arm.
        assert_eq!(count(&plan, "MERGE (UNION ALL)"), 1, "{plan:#?}");

        // Without a cursor, the seek is on the name alone.
        let (_, plan) = read_plan(&store, &filters, &TenantFilter::All, None).await?;
        assert_no_scan(&plan);
        assert_eq!(
            count(
                &plan,
                "SEARCH event USING INDEX idx_event_type_name_cursor (aggregator_type=? AND name=?)"
            ),
            2,
            "{plan:#?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_projection_load_seeks_its_stream_once() -> anyhow::Result<()> {
        let store = store().await?;
        let filters = [
            EventFilter::exact_raw("demo/Page", "p1", "Created"),
            EventFilter::exact_raw("demo/Page", "p1", "Edited"),
        ];
        // A load names its tenant.
        let tenant = TenantFilter::Only("".into());
        let (sql, plan) = read_plan(&store, &filters, &tenant, None).await?;
        assert!(!sql.contains("UNION"), "{sql}");
        assert!(sql.contains(" IN ("), "{sql}");
        assert_eq!(
            plan,
            vec![
                "SEARCH event USING INDEX idx_event_type_id_tenant_cursor (aggregator_type=? AND aggregator_id=? AND tenant=?)"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn overlapping_filters_read_as_one_or() -> anyhow::Result<()> {
        let store = store().await?;
        let filters: Arc<[EventFilter]> = vec![
            EventFilter::by_type_raw("demo/Page"),
            EventFilter::by_event_raw("demo/Page", "Edited"),
        ]
        .into();
        let (sql, _) = read_plan(&store, &filters, &TenantFilter::All, None).await?;
        assert!(!sql.contains("UNION"), "{sql}");
        assert!(sql.contains(" OR "), "{sql}");
        // Each event once.
        let page = store
            .read(
                Some(filters),
                Some(RoutingKey::All),
                TenantFilter::All,
                Args::forward(10, None),
                None,
            )
            .await?;
        let names: Vec<&str> = page.edges.iter().map(|e| e.node.name.as_str()).collect();
        assert_eq!(names, ["Created", "Edited"]);
        Ok(())
    }

    #[tokio::test]
    async fn latest_timestamp_seeks_each_arm() -> anyhow::Result<()> {
        let store = store().await?;
        let filters: Arc<[EventFilter]> = vec![
            EventFilter::by_event_raw("demo/Page", "Edited"),
            EventFilter::by_event_raw("demo/Comment", "Posted"),
            // Never written: an arm without rows, whose `MAX` is `NULL`.
            EventFilter::by_event_raw("demo/Page", "Deleted"),
        ]
        .into();
        let statement =
            latest_timestamp_statement(Some(&filters), Some(&RoutingKey::All), &TenantFilter::All);
        let (sql, _) = build_sqlx::<sqlx::Sqlite, _>(&statement);
        assert_eq!(sql.matches("UNION ALL").count(), 2, "{sql}");
        let plan = plan(&store, &statement).await?;
        assert_no_scan(&plan);
        assert_eq!(
            count(
                &plan,
                "SEARCH event USING COVERING INDEX idx_event_type_name_cursor (aggregator_type=? AND name=?)"
            ),
            3,
            "{plan:#?}"
        );

        let latest = store
            .latest_timestamp(Some(filters), Some(RoutingKey::All), TenantFilter::All)
            .await?;
        let every = store
            .latest_timestamp(None, Some(RoutingKey::All), TenantFilter::All)
            .await?;
        assert!(latest > 0);
        assert_eq!(latest, every);
        Ok(())
    }
}
