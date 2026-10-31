//! sea-query → Turso glue: statements are built with sea-query's SQLite
//! dialect, their bound values converted to [`turso::Value`]s, and rows read
//! back by column position.

use sea_query::{Cond, Expr, ExprTrait, Iden, SelectStatement, SqliteQueryBuilder, Values};
use turso::{Connection, Row};
use ulid::Ulid;

use evento_core::{
    cursor::{Args, Cursor, Edge, PageInfo, ReadResult, Value},
    Event, EventFilter, RoutingKey, TenantFilter,
};

/// Column identifiers for the `event` table.
#[derive(Iden, Clone, Copy)]
pub enum EventTable {
    /// The table name: `event`
    #[iden = "event"]
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
    /// Sub-second precision (milliseconds)
    TimestampSubsec,
}

/// Column identifiers for the `snapshot` table.
#[derive(Iden, Clone, Copy)]
pub enum SnapshotTable {
    /// The table name: `snapshot`
    #[iden = "snapshot"]
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
    /// Creation time (unix seconds)
    CreatedAt,
    /// Last update time (unix seconds)
    UpdatedAt,
}

/// Column identifiers for the `subscriber` table.
#[derive(Iden, Clone, Copy)]
pub enum SubscriberTable {
    /// The table name: `subscriber`
    #[iden = "subscriber"]
    Table,
    /// Subscriber key (primary key)
    Key,
    /// Current worker ID (ULID)
    WorkerId,
    /// Current cursor position
    Cursor,
    /// Seconds behind the newest matching event
    Lag,
    /// Whether the subscription is enabled (`1`/`0`)
    Enabled,
    /// Creation time (unix seconds)
    CreatedAt,
    /// Last update time (unix seconds)
    UpdatedAt,
}

/// Column identifiers for the `meta` table.
#[derive(Iden, Clone, Copy)]
pub enum MetaTable {
    /// The table name: `meta`
    #[iden = "meta"]
    Table,
    /// The key
    K,
    /// The value
    V,
}

/// A rendered statement: SQL text and its parameters. Built synchronously so
/// no sea-query value (not `Send`) is held across an `await`.
pub(crate) struct Prepared {
    sql: String,
    params: Vec<turso::Value>,
}

/// Renders `statement` for SQLite.
pub(crate) fn prepare<S: SqlStatement>(statement: &S) -> Prepared {
    let (sql, values) = statement.build_sqlite();
    Prepared {
        sql,
        params: params(values),
    }
}

/// The sea-query statement kinds this crate renders.
pub(crate) trait SqlStatement {
    fn build_sqlite(&self) -> (String, Values);
}

impl SqlStatement for sea_query::SelectStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl SqlStatement for sea_query::InsertStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl SqlStatement for sea_query::UpdateStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl SqlStatement for sea_query::DeleteStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

/// sea-query bound values as Turso parameters.
fn params(values: Values) -> Vec<turso::Value> {
    values.0.into_iter().map(param).collect()
}

fn param(value: sea_query::Value) -> turso::Value {
    use sea_query::Value as V;
    use turso::Value as T;
    fn int<I: Into<i64>>(v: Option<I>) -> T {
        v.map(|v| T::Integer(v.into())).unwrap_or(T::Null)
    }
    match value {
        V::Bool(v) => int(v.map(i64::from)),
        V::TinyInt(v) => int(v),
        V::SmallInt(v) => int(v),
        V::Int(v) => int(v),
        V::BigInt(v) => int(v),
        V::TinyUnsigned(v) => int(v),
        V::SmallUnsigned(v) => int(v),
        V::Unsigned(v) => int(v),
        // Clamped: a u64 above i64::MAX has no SQLite representation, and the
        // only u64s bound here are timestamps and lags.
        V::BigUnsigned(v) => int(v.map(|v| i64::try_from(v).unwrap_or(i64::MAX))),
        V::Float(v) => v.map(|v| T::Real(f64::from(v))).unwrap_or(T::Null),
        V::Double(v) => v.map(T::Real).unwrap_or(T::Null),
        V::String(v) => v.map(T::Text).unwrap_or(T::Null),
        V::Char(v) => v.map(|c| T::Text(c.to_string())).unwrap_or(T::Null),
        V::Bytes(v) => v.map(T::Blob).unwrap_or(T::Null),
        // sea-query's other variants are behind features this crate does not
        // enable; nothing here constructs them.
        other => unreachable!("unsupported bound value {other:?}"),
    }
}

/// Runs a statement that returns no rows; the number of rows affected.
pub(crate) async fn execute(conn: &Connection, query: Prepared) -> turso::Result<u64> {
    conn.execute(query.sql, turso::params_from_iter(query.params))
        .await
}

/// Every row a statement returns.
pub(crate) async fn fetch_all(conn: &Connection, query: Prepared) -> turso::Result<Vec<Row>> {
    let mut rows = conn
        .query(query.sql, turso::params_from_iter(query.params))
        .await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row);
    }
    Ok(out)
}

/// The first row a statement returns, if any (the rest are drained so the
/// statement runs to completion).
pub(crate) async fn fetch_optional(
    conn: &Connection,
    query: Prepared,
) -> turso::Result<Option<Row>> {
    Ok(fetch_all(conn, query).await?.into_iter().next())
}

/// Column `i` of `row` as text.
pub(crate) fn text(row: &Row, i: usize) -> anyhow::Result<String> {
    opt_text(row, i)?.ok_or_else(|| anyhow::anyhow!("column {i}: unexpected NULL"))
}

/// Column `i` of `row` as optional text.
pub(crate) fn opt_text(row: &Row, i: usize) -> anyhow::Result<Option<String>> {
    match row.get_value(i)? {
        turso::Value::Null => Ok(None),
        turso::Value::Text(s) => Ok(Some(s)),
        other => anyhow::bail!("column {i}: expected text, got {other:?}"),
    }
}

/// Column `i` of `row` as an integer.
pub(crate) fn int(row: &Row, i: usize) -> anyhow::Result<i64> {
    opt_int(row, i)?.ok_or_else(|| anyhow::anyhow!("column {i}: unexpected NULL"))
}

/// Column `i` of `row` as an optional integer.
pub(crate) fn opt_int(row: &Row, i: usize) -> anyhow::Result<Option<i64>> {
    match row.get_value(i)? {
        turso::Value::Null => Ok(None),
        turso::Value::Integer(v) => Ok(Some(v)),
        other => anyhow::bail!("column {i}: expected integer, got {other:?}"),
    }
}

/// Column `i` of `row` as a blob.
pub(crate) fn blob(row: &Row, i: usize) -> anyhow::Result<Vec<u8>> {
    match row.get_value(i)? {
        turso::Value::Blob(v) => Ok(v),
        other => anyhow::bail!("column {i}: expected blob, got {other:?}"),
    }
}

/// The event columns [`decode_event`] expects, in order.
pub(crate) const EVENT_COLUMNS: [EventTable; 11] = [
    EventTable::Id,
    EventTable::Name,
    EventTable::AggregatorType,
    EventTable::AggregatorId,
    EventTable::Version,
    EventTable::Data,
    EventTable::Metadata,
    EventTable::RoutingKey,
    EventTable::Tenant,
    EventTable::Timestamp,
    EventTable::TimestampSubsec,
];

/// An event row selected as [`EVENT_COLUMNS`]. Checked narrowing: an
/// out-of-range column (negative timestamp, version above `u16::MAX`) is a
/// decode error, not silent wraparound.
pub(crate) fn decode_event(row: &Row) -> anyhow::Result<Event> {
    let version = int(row, 4)?;
    let timestamp = int(row, 9)?;
    let timestamp_subsec = int(row, 10)?;
    let metadata = blob(row, 6)?;
    Ok(Event {
        id: Ulid::from_string(&text(row, 0)?)?,
        name: text(row, 1)?,
        aggregate_type: text(row, 2)?,
        aggregate_id: text(row, 3)?,
        version: u16::try_from(version)
            .map_err(|_| anyhow::anyhow!("version {version} out of range"))?,
        data: blob(row, 5)?,
        metadata: bitcode::decode(&metadata)?,
        routing_key: opt_text(row, 7)?,
        tenant: text(row, 8)?,
        timestamp: u64::try_from(timestamp)
            .map_err(|_| anyhow::anyhow!("timestamp {timestamp} out of range"))?,
        timestamp_subsec: u32::try_from(timestamp_subsec)
            .map_err(|_| anyhow::anyhow!("timestamp_subsec {timestamp_subsec} out of range"))?,
    })
}

/// Adds the aggregator, routing-key and tenant filters to `statement`.
///
/// Shared by `read` and `latest_timestamp`, so a subscription's reads and its
/// lag `MAX(timestamp)` always cover the same rows. `aggregators` is `None`
/// for every aggregate; an empty slice matches nothing (sea-query renders an
/// empty `Cond::any()` as `FALSE`).
pub(crate) fn apply_filters(
    statement: &mut SelectStatement,
    aggregators: Option<&[EventFilter]>,
    routing_key: Option<&RoutingKey>,
    tenant: &TenantFilter,
) {
    if let Some(aggregators) = aggregators {
        let mut cond = Cond::any();
        for aggregator in aggregators {
            let mut aggregator_cond = Cond::all()
                .add(Expr::col(EventTable::AggregatorType).eq(aggregator.aggregate_type.clone()));
            if let Some(id) = &aggregator.aggregate_id {
                aggregator_cond =
                    aggregator_cond.add(Expr::col(EventTable::AggregatorId).eq(id.clone()));
            }
            if let Some(name) = &aggregator.name {
                aggregator_cond = aggregator_cond.add(Expr::col(EventTable::Name).eq(name.clone()));
            }
            cond = cond.add(aggregator_cond);
        }
        statement.and_where(cond.into());
    }

    match routing_key {
        Some(RoutingKey::Value(Some(key))) => {
            statement.and_where(Expr::col(EventTable::RoutingKey).eq(key));
        }
        Some(RoutingKey::Value(None)) => {
            statement.and_where(Expr::col(EventTable::RoutingKey).is_null());
        }
        Some(RoutingKey::All) | None => {}
    }

    if let TenantFilter::Only(tenant) = tenant {
        statement.and_where(Expr::col(EventTable::Tenant).eq(tenant));
    }
}

/// The cursor columns, in sort order: `timestamp, timestamp_subsec, version, id`.
const CURSOR_COLUMNS: [EventTable; 4] = [
    EventTable::Timestamp,
    EventTable::TimestampSubsec,
    EventTable::Version,
    EventTable::Id,
];

/// One keyset page of events, ready to run (see [`page`]).
pub(crate) struct Page {
    query: Prepared,
    limit: u16,
    backward: bool,
}

/// Turns `statement` (a select of [`EVENT_COLUMNS`] with its filters) into one
/// keyset page: the cursor predicate, `ORDER BY` the cursor columns, and
/// `LIMIT page + 1` to probe for more.
pub(crate) fn page(mut statement: SelectStatement, args: &Args) -> anyhow::Result<Page> {
    let (limit, cursor) = args.get_info();
    let backward = args.is_backward();

    if let Some(cursor) = cursor.as_ref() {
        let cursor = Event::deserialize_cursor(cursor)?;
        let values: [Expr; 4] = [
            cursor.t.into(),
            cursor.s.into(),
            cursor.v.into(),
            cursor.i.into(),
        ];
        // Nested from the last column out: `t > ? OR (t = ? AND (s > ? OR
        // (s = ? AND (v > ? OR (v = ? AND id > ?)))))`, `<` when descending.
        let mut expr = None::<Expr>;
        for (col, value) in CURSOR_COLUMNS.iter().rev().zip(values.into_iter().rev()) {
            let current = if backward {
                Expr::col(*col).lt(value.clone())
            } else {
                Expr::col(*col).gt(value.clone())
            };
            expr = Some(match expr {
                None => current,
                Some(prev) => current.or(Expr::col(*col).eq(value).and(prev)),
            });
        }
        if let Some(expr) = expr {
            statement.and_where(expr);
        }
    }

    let order = if backward {
        sea_query::Order::Desc
    } else {
        sea_query::Order::Asc
    };
    for col in CURSOR_COLUMNS {
        statement.order_by(col, order.clone());
    }
    statement.limit(u64::from(limit) + 1);

    Ok(Page {
        query: prepare(&statement),
        limit,
        backward,
    })
}

/// Runs a [`page`]. Matches `evento_core::cursor::Reader` byte for byte: the
/// edges of a backward page are reversed into ascending order, both boundary
/// cursors are set, and only the paging direction's "more" flag is computed
/// (the other stays `false`).
pub(crate) async fn read_page(conn: &Connection, page: Page) -> anyhow::Result<ReadResult<Event>> {
    let Page {
        query,
        limit,
        backward,
    } = page;
    let mut rows = fetch_all(conn, query).await?;
    let has_more = rows.len() > usize::from(limit);
    if has_more {
        rows.pop();
    }

    let mut edges = Vec::with_capacity(rows.len());
    for row in &rows {
        let node = decode_event(row)?;
        edges.push(Edge {
            cursor: node.serialize_cursor()?,
            node,
        });
    }
    if backward {
        edges.reverse();
    }

    let start_cursor = edges.first().map(|e| e.cursor.clone());
    let end_cursor = edges.last().map(|e| e.cursor.clone());
    let page_info = if backward {
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

/// A cursor's opaque string, as stored in `TEXT` columns.
pub(crate) fn cursor_text(value: &Value) -> String {
    value.0.clone()
}

/// A stored cursor string as a cursor value.
pub(crate) fn cursor_value(text: String) -> Value {
    Value(text)
}
