//! Running sea-query statements on Turso: statements are built with
//! sea-query's SQLite dialect, their bound values converted to
//! [`turso::Value`]s, and rows read back through [`FromRow`].

use std::collections::{BTreeMap, BTreeSet};

use sea_query::{Cond, Expr, ExprTrait, Iden, SqliteQueryBuilder, Values};
use turso::Connection;

use evento_core::{
    cursor::{Cursor, Value},
    Event, EventCursor, EventFilter, RoutingKey, TenantFilter,
};

use crate::{Bind, FromRow, Row};

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
#[derive(Clone)]
pub(crate) struct Prepared {
    sql: String,
    params: Vec<turso::Value>,
}

#[cfg(test)]
impl Prepared {
    /// The SQL text.
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    /// Turso's query plan for the statement, one line per step.
    pub(crate) async fn explain(&self, conn: &Connection) -> anyhow::Result<Vec<String>> {
        let mut rows = conn
            .query(
                &format!("EXPLAIN QUERY PLAN {}", self.sql),
                turso::params_from_iter(self.params.iter().cloned()),
            )
            .await?;
        let mut plan = Vec::new();
        while let Some(row) = rows.next().await? {
            plan.push(text(&row, 3)?);
        }
        Ok(plan)
    }
}

/// Renders `statement` for SQLite.
pub(crate) fn prepare<S: Statement + ?Sized>(statement: &S) -> Prepared {
    let (sql, values) = statement.build_sqlite();
    Prepared {
        sql,
        params: params(values),
    }
}

/// A sea-query statement [`execute`], [`fetch_all`] and the other helpers run:
/// select, insert, update or delete.
pub trait Statement {
    /// Renders the statement in SQLite's dialect, with its bound values.
    fn build_sqlite(&self) -> (String, Values);
}

impl Statement for sea_query::SelectStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl Statement for sea_query::InsertStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl Statement for sea_query::UpdateStatement {
    fn build_sqlite(&self) -> (String, Values) {
        self.build(SqliteQueryBuilder)
    }
}

impl Statement for sea_query::DeleteStatement {
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

/// Runs a rendered statement that returns no rows; the rows affected.
pub(crate) async fn execute_prepared(conn: &Connection, query: &Prepared) -> turso::Result<u64> {
    conn.execute(
        &query.sql,
        turso::params_from_iter(query.params.iter().cloned()),
    )
    .await
}

/// How many times a read finding the database locked is run again.
const READ_ATTEMPTS: u32 = 200;

/// Every row a rendered statement returns, with the column names. A read
/// that finds the database locked — an MVCC checkpoint stopping the world
/// for a moment — is run again after a short pause, up to [`READ_ATTEMPTS`].
pub(crate) async fn fetch_rows(
    conn: &Connection,
    query: Prepared,
) -> turso::Result<(Vec<String>, Vec<turso::Row>)> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match fetch_rows_once(conn, &query).await {
            Err(turso::Error::Busy(_)) if attempt < READ_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(u64::from(Ord::min(
                    attempt, 20,
                ))))
                .await;
            }
            outcome => return outcome,
        }
    }
}

async fn fetch_rows_once(
    conn: &Connection,
    query: &Prepared,
) -> turso::Result<(Vec<String>, Vec<turso::Row>)> {
    let mut rows = conn
        .query(
            &query.sql,
            turso::params_from_iter(query.params.iter().cloned()),
        )
        .await?;
    let columns = rows.column_names();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row);
    }
    Ok((columns, out))
}

/// Every row a rendered statement returns, decoded.
pub(crate) async fn fetch_prepared<O: FromRow>(
    conn: &Connection,
    query: Prepared,
) -> anyhow::Result<Vec<O>> {
    let (columns, rows) = fetch_rows(conn, query).await?;
    rows.iter()
        .map(|row| O::from_row(&Row::new(&columns, row)))
        .collect()
}

/// Runs a statement that returns no rows — an insert, update or delete —
/// on `conn`; the number of rows affected.
///
/// ```rust,no_run
/// # use sea_query::{Expr, ExprTrait, Query};
/// # use evento_turso::{Turso, SubscriberTable};
/// # async fn run(store: Turso) -> anyhow::Result<()> {
/// let conn = store.connect()?;
/// let deleted = evento_turso::execute(
///     &conn,
///     &Query::delete()
///         .from_table(SubscriberTable::Table)
///         .and_where(Expr::col(SubscriberTable::Key).eq("orders.report"))
///         .to_owned(),
/// )
/// .await?;
/// # let _ = deleted;
/// # Ok(())
/// # }
/// ```
pub async fn execute<S: Statement + ?Sized>(
    conn: &Connection,
    statement: &S,
) -> anyhow::Result<u64> {
    let prepared = prepare(statement);
    Ok(execute_prepared(conn, &prepared).await?)
}

/// Every row `statement` returns on `conn`, decoded as `O`.
pub async fn fetch_all<O: FromRow, S: Statement + ?Sized>(
    conn: &Connection,
    statement: &S,
) -> anyhow::Result<Vec<O>> {
    let prepared = prepare(statement);
    fetch_prepared(conn, prepared).await
}

/// The first row `statement` returns on `conn`, decoded as `O`, if any (the
/// rest are drained so the statement runs to completion).
pub async fn fetch_optional<O: FromRow, S: Statement + ?Sized>(
    conn: &Connection,
    statement: &S,
) -> anyhow::Result<Option<O>> {
    Ok(fetch_all(conn, statement).await?.into_iter().next())
}

/// The one row `statement` returns on `conn`, decoded as `O`; an error when
/// there is none.
pub async fn fetch_one<O: FromRow, S: Statement + ?Sized>(
    conn: &Connection,
    statement: &S,
) -> anyhow::Result<O> {
    fetch_optional(conn, statement)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the query returned no row"))
}

/// Every row a rendered statement returns, without the column names.
pub(crate) async fn fetch_rows_only(
    conn: &Connection,
    query: Prepared,
) -> turso::Result<Vec<turso::Row>> {
    Ok(fetch_rows(conn, query).await?.1)
}

/// The first row a rendered statement returns, if any.
pub(crate) async fn fetch_optional_prepared(
    conn: &Connection,
    query: Prepared,
) -> turso::Result<Option<turso::Row>> {
    Ok(fetch_rows(conn, query).await?.1.into_iter().next())
}

/// Column `i` of `row` as text.
pub(crate) fn text(row: &turso::Row, i: usize) -> anyhow::Result<String> {
    opt_text(row, i)?.ok_or_else(|| anyhow::anyhow!("column {i}: unexpected NULL"))
}

/// Column `i` of `row` as optional text.
pub(crate) fn opt_text(row: &turso::Row, i: usize) -> anyhow::Result<Option<String>> {
    match row.get_value(i)? {
        turso::Value::Null => Ok(None),
        turso::Value::Text(s) => Ok(Some(s)),
        other => anyhow::bail!("column {i}: expected text, got {other:?}"),
    }
}

/// Column `i` of `row` as an integer.
pub(crate) fn int(row: &turso::Row, i: usize) -> anyhow::Result<i64> {
    opt_int(row, i)?.ok_or_else(|| anyhow::anyhow!("column {i}: unexpected NULL"))
}

/// Column `i` of `row` as an optional integer.
pub(crate) fn opt_int(row: &turso::Row, i: usize) -> anyhow::Result<Option<i64>> {
    match row.get_value(i)? {
        turso::Value::Null => Ok(None),
        turso::Value::Integer(v) => Ok(Some(v)),
        other => anyhow::bail!("column {i}: expected integer, got {other:?}"),
    }
}

/// Column `i` of `row` as a blob.
pub(crate) fn blob(row: &turso::Row, i: usize) -> anyhow::Result<Vec<u8>> {
    match row.get_value(i)? {
        turso::Value::Blob(v) => Ok(v),
        other => anyhow::bail!("column {i}: expected blob, got {other:?}"),
    }
}

/// The event columns an [`EventRow`] reads, in order.
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

/// An event row of the `event` table: [`FromRow`] over [`EVENT_COLUMNS`],
/// paged by `timestamp, timestamp_subsec, version, id` — what the executor's
/// `read` runs through the [`Reader`](crate::Reader).
pub(crate) struct EventRow(pub(crate) Event);

impl FromRow for EventRow {
    fn from_row(row: &Row<'_>) -> anyhow::Result<Self> {
        let metadata: Vec<u8> = row.get("metadata")?;
        Ok(EventRow(Event {
            id: row.get("id")?,
            name: row.get("name")?,
            aggregate_type: row.get("aggregator_type")?,
            aggregate_id: row.get("aggregator_id")?,
            // Checked narrowing: a version above `u16::MAX` or a negative
            // timestamp is a decode error, not silent wraparound.
            version: row.get("version")?,
            data: row.get("data")?,
            metadata: bitcode::decode(&metadata)?,
            routing_key: row.get("routing_key")?,
            // A select without the column reads as the default tenant.
            tenant: row.try_get("tenant")?.unwrap_or_default(),
            timestamp: row.get("timestamp")?,
            timestamp_subsec: row.get("timestamp_subsec")?,
        }))
    }
}

impl Cursor for EventRow {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        self.0.serialize()
    }
}

impl Bind for EventRow {
    type T = EventTable;
    type I = [EventTable; 4];
    type V = [Expr; 4];
    type Cursor = Self;

    fn columns() -> Self::I {
        [
            EventTable::Timestamp,
            EventTable::TimestampSubsec,
            EventTable::Version,
            EventTable::Id,
        ]
    }

    fn values(cursor: EventCursor) -> Self::V {
        [
            cursor.t.into(),
            cursor.s.into(),
            cursor.v.into(),
            cursor.i.into(),
        ]
    }
}

/// The `WHERE` conditions of a filtered read, one per seek.
///
/// Turso has no OR optimization worth the name: a single `WHERE (type = ?
/// AND name = ?) OR (…)` only seeks on what every arm shares (the type, at
/// best), then reads every row of it — a subscription's pass costs time
/// proportional to the store, whatever its cursor (#478). So when the filters
/// split into pairwise disjoint arms, each gets a condition of its own —
/// `type = ? [AND id = ?] [AND name = ? | name IN (…)]` — for the caller to
/// read as a `UNION ALL`, one index seek per arm. The arms: one per `(type,
/// id)` for the filters naming an aggregate (its stream is short and the
/// `(type, id, tenant, …)` index serves it whole), one per `(type, name)` or
/// `(type)` for the others (served by `idx_event_type_name_cursor` and
/// `idx_event_type_cursor`). Filters that could match the same event — a type
/// and one of its names, an aggregate and its type — fall back to the single
/// `OR` condition, so no event is returned twice.
///
/// `aggregators` is `None` for every aggregate; an empty slice matches nothing
/// (sea-query renders an empty `Cond::any()` as `FALSE`). The routing key and
/// the tenant go on every condition, so a subscription's reads and its lag
/// `MAX(timestamp)` always cover the same rows.
pub(crate) fn filter_conditions(
    aggregators: Option<&[EventFilter]>,
    routing_key: Option<&RoutingKey>,
    tenant: &TenantFilter,
) -> Vec<Cond> {
    let scope = |mut cond: Cond| {
        match routing_key {
            Some(RoutingKey::Value(Some(key))) => {
                cond = cond.add(Expr::col(EventTable::RoutingKey).eq(key));
            }
            Some(RoutingKey::Value(None)) => {
                cond = cond.add(Expr::col(EventTable::RoutingKey).is_null());
            }
            Some(RoutingKey::All) | None => {}
        }
        if let TenantFilter::Only(tenant) = tenant {
            cond = cond.add(Expr::col(EventTable::Tenant).eq(tenant));
        }
        cond
    };

    let Some(aggregators) = aggregators else {
        return vec![scope(Cond::all())];
    };
    match arms(aggregators) {
        Some(arms) if !arms.is_empty() => arms
            .into_iter()
            .map(|arm| scope(Cond::all().add(arm.condition())))
            .collect(),
        _ => {
            let mut any = Cond::any();
            for aggregator in aggregators {
                let mut cond = Cond::all().add(
                    Expr::col(EventTable::AggregatorType).eq(aggregator.aggregate_type.clone()),
                );
                if let Some(id) = &aggregator.aggregate_id {
                    cond = cond.add(Expr::col(EventTable::AggregatorId).eq(id.clone()));
                }
                if let Some(name) = &aggregator.name {
                    cond = cond.add(Expr::col(EventTable::Name).eq(name.clone()));
                }
                any = any.add(cond);
            }
            vec![scope(Cond::all().add(any))]
        }
    }
}

/// One seek of a filtered read: the events of `aggregate_type`, of the
/// aggregate `aggregate_id` when set, named one of `names` — or any name when
/// `None`.
#[derive(Debug, PartialEq, Eq)]
struct Arm<'a> {
    aggregate_type: &'a str,
    aggregate_id: Option<&'a str>,
    names: Option<BTreeSet<&'a str>>,
}

impl Arm<'_> {
    fn condition(&self) -> Cond {
        let mut cond =
            Cond::all().add(Expr::col(EventTable::AggregatorType).eq(self.aggregate_type));
        if let Some(id) = self.aggregate_id {
            cond = cond.add(Expr::col(EventTable::AggregatorId).eq(id));
        }
        match &self.names {
            None => {}
            Some(names) if names.len() == 1 => {
                let name = *names.iter().next().expect("one name");
                cond = cond.add(Expr::col(EventTable::Name).eq(name));
            }
            Some(names) => {
                cond = cond.add(Expr::col(EventTable::Name).is_in(names.iter().copied()));
            }
        }
        cond
    }

    /// Whether an event could match both arms.
    fn overlaps(&self, other: &Self) -> bool {
        let ids = match (self.aggregate_id, other.aggregate_id) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        };
        let names = match (&self.names, &other.names) {
            (Some(a), Some(b)) => !a.is_disjoint(b),
            _ => true,
        };
        self.aggregate_type == other.aggregate_type && ids && names
    }
}

/// Groups `filters` into the arms of [`filter_conditions`], in a fixed order
/// so the same filters render the same SQL; `None` when two arms overlap.
fn arms(filters: &[EventFilter]) -> Option<Vec<Arm<'_>>> {
    // Keyed by `(type, id, name)`, the name only for the filters without an
    // aggregate id: the ones naming an aggregate merge into one arm per
    // stream, with `name IN (…)`.
    let mut grouped: BTreeMap<(&str, Option<&str>, Option<&str>), Arm<'_>> = BTreeMap::new();
    for filter in filters {
        let aggregate_id = filter.aggregate_id.as_deref();
        let name = filter.name.as_deref();
        let key = (
            filter.aggregate_type.as_str(),
            aggregate_id,
            aggregate_id.is_none().then_some(name).flatten(),
        );
        let arm = grouped.entry(key).or_insert_with(|| Arm {
            aggregate_type: &filter.aggregate_type,
            aggregate_id,
            names: Some(BTreeSet::new()),
        });
        match (name, &mut arm.names) {
            // A filter without a name takes every name of the stream.
            (None, names) => *names = None,
            (Some(name), Some(names)) => {
                names.insert(name);
            }
            (Some(_), None) => {}
        }
    }
    let arms: Vec<Arm<'_>> = grouped.into_values().collect();
    let disjoint = arms
        .iter()
        .enumerate()
        .all(|(i, arm)| arms[i + 1..].iter().all(|other| !arm.overlaps(other)));
    disjoint.then_some(arms)
}

/// A cursor's opaque string, as stored in `TEXT` columns.
pub(crate) fn cursor_text(value: &Value) -> String {
    value.0.clone()
}

/// A stored cursor string as a cursor value.
pub(crate) fn cursor_value(text: String) -> Value {
    Value(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arm<'a>(
        aggregate_type: &'a str,
        aggregate_id: Option<&'a str>,
        names: Option<&[&'a str]>,
    ) -> Arm<'a> {
        Arm {
            aggregate_type,
            aggregate_id,
            names: names.map(|names| names.iter().copied().collect()),
        }
    }

    #[test]
    fn a_subscription_gets_one_arm_per_handled_event() {
        let filters = [
            EventFilter::by_event_raw("Page", "Edited"),
            EventFilter::by_event_raw("Page", "Created"),
            EventFilter::by_event_raw("Comment", "Posted"),
            // Handlers often collapse to the same filter.
            EventFilter::by_event_raw("Page", "Edited"),
        ];
        assert_eq!(
            arms(&filters),
            Some(vec![
                arm("Comment", None, Some(&["Posted"])),
                arm("Page", None, Some(&["Created"])),
                arm("Page", None, Some(&["Edited"])),
            ])
        );
    }

    #[test]
    fn a_projection_load_gets_one_arm_per_stream() {
        let filters = [
            EventFilter::exact_raw("Page", "p1", "Edited"),
            EventFilter::exact_raw("Page", "p1", "Created"),
            EventFilter::exact_raw("Comment", "c1", "Posted"),
        ];
        assert_eq!(
            arms(&filters),
            Some(vec![
                arm("Comment", Some("c1"), Some(&["Posted"])),
                arm("Page", Some("p1"), Some(&["Created", "Edited"])),
            ])
        );
        // A filter without a name takes the whole stream.
        let filters = [
            EventFilter::exact_raw("Page", "p1", "Edited"),
            EventFilter::by_id_raw("Page", "p1"),
        ];
        assert_eq!(arms(&filters), Some(vec![arm("Page", Some("p1"), None)]));
    }

    #[test]
    fn overlapping_filters_have_no_arms() {
        // A type and one of its names.
        assert_eq!(
            arms(&[
                EventFilter::by_type_raw("Page"),
                EventFilter::by_event_raw("Page", "Edited"),
            ]),
            None
        );
        // An aggregate and its type's name.
        assert_eq!(
            arms(&[
                EventFilter::exact_raw("Page", "p1", "Edited"),
                EventFilter::by_event_raw("Page", "Edited"),
            ]),
            None
        );
        // An aggregate and its type.
        assert_eq!(
            arms(&[
                EventFilter::by_id_raw("Page", "p1"),
                EventFilter::by_type_raw("Page"),
            ]),
            None
        );
        // Two aggregates of a type, two names of a type: disjoint.
        assert!(arms(&[
            EventFilter::by_id_raw("Page", "p1"),
            EventFilter::by_id_raw("Page", "p2"),
            EventFilter::by_event_raw("Comment", "Posted"),
            EventFilter::by_event_raw("Comment", "Deleted"),
        ])
        .is_some());
    }

    #[test]
    fn no_filter_is_one_condition_and_no_filters_match_nothing() {
        assert_eq!(filter_conditions(None, None, &TenantFilter::All).len(), 1);
        let none = filter_conditions(Some(&[]), None, &TenantFilter::All);
        assert_eq!(none.len(), 1);
        let (sql, _) = sea_query::Query::select()
            .column(EventTable::Id)
            .from(EventTable::Table)
            .cond_where(none.into_iter().next().unwrap())
            .build_sqlite();
        assert!(sql.contains("FALSE"), "{sql}");
    }
}
