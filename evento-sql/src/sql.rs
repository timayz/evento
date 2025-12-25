//! Core SQL implementation for event sourcing.

use std::ops::{Deref, DerefMut};

#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;
use sea_query::{Cond, Expr, ExprTrait, Iden, IntoColumnRef, OnConflict, Query, SelectStatement};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Database, Pool};
use ulid::Ulid;

use evento_core::{
    cursor::{self, Args, Cursor, Edge, PageInfo, ReadResult, Value},
    Executor, ReadAggregator, WriteError,
};

/// Column identifiers for the `event` table.
///
/// Used with sea-query for type-safe SQL query construction.
///
/// # Columns
///
/// - `Id` - Event identifier (ULID format, VARCHAR(26))
/// - `Name` - Event type name (VARCHAR(50))
/// - `AggregatorType` - Aggregate root type (VARCHAR(50))
/// - `AggregatorId` - Aggregate root instance ID (VARCHAR(26))
/// - `Version` - Event sequence number within the aggregate
/// - `Data` - Serialized event payload (BLOB, rkyv format)
/// - `Metadata` - Serialized event metadata (BLOB, rkyv format)
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
/// Used with sea-query for type-safe SQL query construction.
///
/// **Note:** The snapshot table is dropped in migration M0003 and is no longer used.
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
/// - `Lag` - Number of events behind the latest
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
pub struct Sql<DB: Database>(Pool<DB>);

impl<DB: Database> Sql<DB> {
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
}

#[async_trait::async_trait]
impl<DB> Executor for Sql<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    bool: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    Vec<u8>: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_core::Event: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<evento_core::RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<evento_core::Event>> {
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
                            .add(Expr::col(Event::AggregatorType).eq(aggregator.aggregator_type));

                        if let Some(id) = aggregator.aggregator_id {
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

        Ok(Reader::new(statement)
            .args(args)
            .execute::<_, evento_core::Event, _>(&self.0)
            .await?)
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        let statement = Query::select()
            .columns([Subscriber::Cursor])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let Some((cursor,)) = sqlx::query_as_with::<DB, (Option<String>,), _>(&sql, values)
            .fetch_optional(&self.0)
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

        let (id, enabled) = sqlx::query_as_with::<DB, (String, bool), _>(&sql, values)
            .fetch_one(&self.0)
            .await?;

        Ok(worker_id.to_string() == id && enabled)
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

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.0)
            .await?;

        Ok(())
    }

    async fn write(&self, events: Vec<evento_core::Event>) -> Result<(), WriteError> {
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
            statement.values_panic([
                event.id.to_string().into(),
                event.name.into(),
                event.data.into(),
                event.metadata.into(),
                event.aggregator_type.into(),
                event.aggregator_id.into(),
                event.version.into(),
                event.routing_key.into(),
                event.timestamp.into(),
                event.timestamp_subsec.into(),
            ]);
        }

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.0)
            .await
            .map_err(|err| {
                let err_str = err.to_string();
                if err_str.contains("(code: 2067)") {
                    return WriteError::InvalidOriginalVersion;
                }
                if err_str.contains("1062 (23000): Duplicate entry") {
                    return WriteError::InvalidOriginalVersion;
                }
                if err_str.contains("duplicate key value violates unique constraint") {
                    return WriteError::InvalidOriginalVersion;
                }
                WriteError::Unknown(err.into())
            })?;

        Ok(())
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        let statement = Query::update()
            .table(Subscriber::Table)
            .values([
                (Subscriber::Cursor, cursor.0.into()),
                (Subscriber::Lag, lag.into()),
                (Subscriber::UpdatedAt, Expr::current_timestamp()),
            ])
            .and_where(Expr::col(Subscriber::Key).eq(key))
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.0)
            .await?;

        Ok(())
    }
}

impl<D: Database> Clone for Sql<D> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<D: Database> From<Pool<D>> for Sql<D> {
    fn from(value: Pool<D>) -> Self {
        Self(value)
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
        sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
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

        let mut rows = sqlx::query_as_with::<DB, O, _>(&sql, values)
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

        let page_info = if self.args.is_backward() {
            let start_cursor = edges.first().map(|e| e.cursor.clone());

            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor: None,
            }
        } else {
            let end_cursor = edges.last().map(|e| e.cursor.clone());
            PageInfo {
                has_previous_page: false,
                has_next_page: has_more,
                start_cursor: None,
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
        self.limit((limit + 1).into());

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
        let colums = B::columns().into_iter().rev();
        let values = B::values(cursor).into_iter().rev();

        let mut expr = None::<Expr>;
        for (col, value) in colums.zip(values) {
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

        self.and_where(expr.unwrap());

        Ok(())
    }

    fn build_reader_order<O: Bind>(&mut self) {
        let order = if self.is_order_desc() {
            sea_query::Order::Desc
        } else {
            sea_query::Order::Asc
        };

        let colums = O::columns();
        for col in colums {
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

impl Bind for evento_core::Event {
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
