use std::sync::Arc;

use evento::{
    cursor::{Args, Order, ReadResult, Value},
    sql::{Reader, Sql},
    sql_migrator::{
        InitMigration, M0002, M0003, M0004, M0005, M0006, M0007, M0008, M0009, M0010, M0011,
    },
    Event, EventFilter, Executor, RoutingKey, TenantFilter,
};
use evento_test::assert_read_result;
use sea_query::{MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::{any::install_default_drivers, migrate::MigrateDatabase, Any, Database, Pool};
use sqlx_migrator::Plan;

pub async fn forward_asc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    let data = get_data(&pool).await?;
    let order = Order::Asc;

    let args = Args::forward((data.len() + 1) as u16, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::forward(4, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;
    let end_cursor = result.page_info.end_cursor.clone();

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::forward(4, end_cursor);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order, data, result)?;

    Ok(())
}

pub async fn forward_desc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    let data = get_data(&pool).await?;
    let order = Order::Desc;

    let args = Args::forward((data.len() + 1) as u16, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::forward(4, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;
    let end_cursor = result.page_info.end_cursor.clone();

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::forward(4, end_cursor);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order, data, result)?;

    Ok(())
}

pub async fn backward_asc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    let data = get_data(&pool).await?;
    let order = Order::Asc;

    let args = Args::backward((data.len() + 1) as u16, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::backward(4, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;
    let start_cursor = result.page_info.start_cursor.clone();

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::backward(4, start_cursor);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order, data, result)?;

    Ok(())
}

pub async fn backward_desc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    let data = get_data(&pool).await?;
    let order = Order::Desc;

    let args = Args::backward((data.len() + 1) as u16, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::backward(4, None);
    let result = read(&pool, args.clone(), order.to_owned()).await?;
    let start_cursor = result.page_info.start_cursor.clone();

    assert_read_result(args, order.to_owned(), data.clone(), result)?;

    let args = Args::backward(4, start_cursor);
    let result = read(&pool, args.clone(), order.to_owned()).await?;

    assert_read_result(args, order, data, result)?;

    Ok(())
}

/// One `Reader` serves a retry, the next page and the opposite direction:
/// `execute` must not leave its cursor predicate, ORDER BY or LIMIT on the
/// stored statement.
pub async fn reader_reuse<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    let data = get_data(&pool).await?;
    let order = Order::Asc;
    let mut reader = reader(order.to_owned());

    // A retry: the same page twice.
    let args = Args::forward(4, None);
    let first = reader
        .args(args.clone())
        .execute::<_, evento_sql::SqlEvent, _>(&pool)
        .await?
        .map(|e| e.0);
    let again = reader
        .args(args.clone())
        .execute::<_, evento_sql::SqlEvent, _>(&pool)
        .await?
        .map(|e| e.0);
    assert_eq!(first.edges.len(), again.edges.len());
    assert_eq!(first.page_info, again.page_info);
    assert_read_result(args, order.to_owned(), data.clone(), again)?;

    // The next page, on the same reader.
    let args = Args::forward(4, first.page_info.end_cursor.clone());
    let second = reader
        .args(args.clone())
        .execute::<_, evento_sql::SqlEvent, _>(&pool)
        .await?
        .map(|e| e.0);
    let second_start = second.page_info.start_cursor.clone();
    assert_read_result(args, order.to_owned(), data.clone(), second)?;

    // Back to the first page from the second one, still on the same reader.
    let args = Args::backward(4, second_start);
    let back = reader
        .args(args.clone())
        .execute::<_, evento_sql::SqlEvent, _>(&pool)
        .await?
        .map(|e| e.0);
    assert_read_result(args, order, data, back)?;

    Ok(())
}

pub async fn read<DB>(
    pool: &Pool<DB>,
    args: Args,
    order: Order,
) -> anyhow::Result<ReadResult<Event>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    evento_sql::SqlEvent: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    Ok(reader(order)
        .args(args)
        .execute::<_, evento_sql::SqlEvent, _>(pool)
        .await?
        .map(|e| e.0))
}

/// A reader over every event column, in `order`, with no args yet.
fn reader(order: Order) -> Reader {
    let statement = Query::select()
        .columns([
            evento::sql::Event::Id,
            evento::sql::Event::Name,
            evento::sql::Event::AggregatorType,
            evento::sql::Event::AggregatorId,
            evento::sql::Event::Version,
            evento::sql::Event::Data,
            evento::sql::Event::Metadata,
            evento::sql::Event::RoutingKey,
            evento::sql::Event::Timestamp,
            evento::sql::Event::TimestampSubsec,
        ])
        .from(evento::sql::Event::Table)
        .to_owned();
    let mut reader = Reader::new(statement);
    reader.order(order);
    reader
}

pub async fn get_data<DB>(pool: &Pool<DB>) -> anyhow::Result<Vec<Event>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
{
    let data = evento_test::get_data();
    let mut statement = Query::insert()
        .into_table(evento::sql::Event::Table)
        .columns([
            evento::sql::Event::Id,
            evento::sql::Event::Name,
            evento::sql::Event::Data,
            evento::sql::Event::Metadata,
            evento::sql::Event::AggregatorType,
            evento::sql::Event::AggregatorId,
            evento::sql::Event::Version,
            evento::sql::Event::RoutingKey,
            evento::sql::Event::Timestamp,
            evento::sql::Event::TimestampSubsec,
        ])
        .to_owned();

    for event in data.clone() {
        let metadata = bitcode::encode(&event.metadata);
        statement.values_panic([
            event.id.to_string().into(),
            event.name.into(),
            event.data.into(),
            metadata.into(),
            event.aggregate_type.into(),
            event.aggregate_id.to_string().into(),
            event.version.into(),
            event.routing_key.into(),
            event.timestamp.into(),
            event.timestamp_subsec.into(),
        ]);
    }

    let (sql, values) = match DB::NAME {
        "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
        "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
        "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
        name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
    };

    sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
        .execute(pool)
        .await?;

    Ok(data)
}

pub async fn create_pool<DB: Database>(url: impl Into<String>) -> anyhow::Result<Pool<DB>>
where
    DB::Arguments: sqlx::IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    M0005: sqlx_migrator::Migration<DB>,
    M0006: sqlx_migrator::Migration<DB>,
    M0007: sqlx_migrator::Migration<DB>,
    M0008: sqlx_migrator::Migration<DB>,
    M0009: sqlx_migrator::Migration<DB>,
    M0010: sqlx_migrator::Migration<DB>,
    M0011: sqlx_migrator::Migration<DB>,
    DB: sqlx_migrator::migrator::SupportedDatabase,
    sqlx_migrator::Migrator<DB>: Default,
{
    install_default_drivers();

    let url = url.into();

    let _ = Any::drop_database(&url).await;
    Any::create_database(&url).await?;

    let pool = Pool::<DB>::connect(&url).await?;
    let mut conn = pool.acquire().await?;
    let migrator = evento::sql_migrator::new::<DB>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;

    Ok(pool)
}

/// Runs `statement` on `pool`, rendered for its backend.
async fn execute<DB>(pool: &Pool<DB>, statement: impl SqlxBinder) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
{
    let (sql, values) = match DB::NAME {
        "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
        "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
        "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
        name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
    };
    sqlx::query_with::<DB, _>(sqlx::AssertSqlSafe(sql.as_str()), values)
        .execute(pool)
        .await?;
    Ok(())
}

/// The tenants of the events of aggregate `aggregate_id`.
async fn event_tenants<DB>(pool: &Pool<DB>, aggregate_id: &str) -> anyhow::Result<Vec<String>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    use evento::sql::Event as EventTable;
    use sea_query::{Expr, ExprTrait};

    let statement = Query::select()
        .column(EventTable::Tenant)
        .from(EventTable::Table)
        .and_where(Expr::col(EventTable::AggregatorId).eq(aggregate_id))
        .to_owned();
    let (sql, values) = match DB::NAME {
        "SQLite" => statement.build_sqlx(SqliteQueryBuilder),
        "MySQL" => statement.build_sqlx(MysqlQueryBuilder),
        "PostgreSQL" => statement.build_sqlx(PostgresQueryBuilder),
        name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
    };
    let rows: Vec<(String,)> =
        sqlx::query_as_with::<DB, (String,), _>(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(tenant,)| tenant).collect())
}

/// An `event` row, in `tenant` when given (else the column's default).
fn event_row(aggregate_id: &str, tenant: Option<&str>) -> sea_query::InsertStatement {
    use evento::sql::Event as EventTable;

    let mut columns = vec![
        EventTable::Id,
        EventTable::Name,
        EventTable::Data,
        EventTable::Metadata,
        EventTable::AggregatorType,
        EventTable::AggregatorId,
        EventTable::Version,
        EventTable::Timestamp,
        EventTable::TimestampSubsec,
    ];
    let mut values: Vec<sea_query::Expr> = vec![
        ulid::Ulid::generate().to_string().into(),
        "Opened".into(),
        Vec::<u8>::new().into(),
        Vec::<u8>::new().into(),
        "test/Account".into(),
        aggregate_id.into(),
        1.into(),
        0.into(),
        0.into(),
    ];
    if let Some(tenant) = tenant {
        columns.push(EventTable::Tenant);
        values.push(tenant.into());
    }
    Query::insert()
        .into_table(EventTable::Table)
        .columns(columns)
        .values_panic(values)
        .to_owned()
}

/// `m0008` reverts and re-applies around an event of the default tenant,
/// which survives both; once a tenant other than the default one holds an
/// event, the revert is refused and nothing is reverted.
pub async fn m0008_round_trip<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    M0005: sqlx_migrator::Migration<DB>,
    M0006: sqlx_migrator::Migration<DB>,
    M0007: sqlx_migrator::Migration<DB>,
    M0008: sqlx_migrator::Migration<DB>,
    M0009: sqlx_migrator::Migration<DB>,
    M0010: sqlx_migrator::Migration<DB>,
    M0011: sqlx_migrator::Migration<DB>,
    DB: sqlx_migrator::migrator::SupportedDatabase,
    sqlx_migrator::Migrator<DB>: Default,
{
    execute(&pool, event_row("kept", None)).await?;

    let migrator = evento::sql_migrator::new::<DB>()?;
    let mut conn = pool.acquire().await?;
    migrator
        .run(&mut *conn, &Plan::revert_name("main", Some("m0008")))
        .await?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    drop(conn);
    assert_eq!(
        event_tenants(&pool, "kept").await?,
        vec![String::new()],
        "the event survives the revert and the re-apply, in the default tenant"
    );

    execute(&pool, event_row("a-1", Some("acme"))).await?;

    let mut conn = pool.acquire().await?;
    let err = migrator
        .run(&mut *conn, &Plan::revert_name("main", Some("m0008")))
        .await
        .expect_err("a tenant's event blocks the revert");
    drop(conn);
    assert!(
        err.to_string().contains("cannot be reverted"),
        "unexpected error: {err}"
    );
    assert_eq!(
        event_tenants(&pool, "a-1").await?,
        vec!["acme".to_owned()],
        "the refused revert changed nothing"
    );

    Ok(())
}

/// The `idx_*` indexes of the `event` table, by name.
async fn event_indexes<DB>(pool: &Pool<DB>) -> anyhow::Result<Vec<String>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let sql = match DB::NAME {
        "SQLite" => {
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'event' \
             AND name LIKE 'idx_%' ORDER BY name"
        }
        "MySQL" => {
            "SELECT DISTINCT index_name FROM information_schema.statistics \
             WHERE table_schema = DATABASE() AND table_name = 'event' \
             AND index_name LIKE 'idx_%' ORDER BY index_name"
        }
        "PostgreSQL" => {
            "SELECT indexname FROM pg_indexes WHERE tablename = 'event' \
             AND indexname LIKE 'idx_%' ORDER BY indexname"
        }
        name => panic!("'{name}' not supported, consider using SQLite, PostgreSQL or MySQL"),
    };
    let values = sea_query_sqlx::SqlxValues(sea_query::Values(vec![]));
    let rows: Vec<(String,)> = sqlx::query_as_with::<DB, (String,), _>(sql, values)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// The fresh schema has the eight `event` indexes; reverting `m0010` then
/// `m0009` takes one away each, and re-applying restores both.
pub async fn m0009_round_trip<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    String: for<'r> sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
    M0003: sqlx_migrator::Migration<DB>,
    M0004: sqlx_migrator::Migration<DB>,
    M0005: sqlx_migrator::Migration<DB>,
    M0006: sqlx_migrator::Migration<DB>,
    M0007: sqlx_migrator::Migration<DB>,
    M0008: sqlx_migrator::Migration<DB>,
    M0009: sqlx_migrator::Migration<DB>,
    M0010: sqlx_migrator::Migration<DB>,
    M0011: sqlx_migrator::Migration<DB>,
    DB: sqlx_migrator::migrator::SupportedDatabase,
    sqlx_migrator::Migrator<DB>: Default,
{
    let all = [
        "idx_event_cursor",
        "idx_event_routing_key_type",
        "idx_event_type_cursor",
        "idx_event_type_id_tenant_cursor",
        "idx_event_type_id_tenant_version",
        "idx_event_type_name_cursor",
        "idx_event_type_routing_cursor",
        "idx_event_type_tenant_cursor",
    ];
    assert_eq!(event_indexes(&pool).await?, all);

    let migrator = evento::sql_migrator::new::<DB>()?;
    let mut conn = pool.acquire().await?;
    migrator
        .run(&mut *conn, &Plan::revert_name("main", Some("m0010")))
        .await?;
    let without_cursor: Vec<&str> = all
        .iter()
        .copied()
        .filter(|name| *name != "idx_event_cursor")
        .collect();
    assert_eq!(event_indexes(&pool).await?, without_cursor);
    migrator
        .run(&mut *conn, &Plan::revert_name("main", Some("m0009")))
        .await?;
    let without_both: Vec<&str> = without_cursor
        .iter()
        .copied()
        .filter(|name| *name != "idx_event_type_name_cursor")
        .collect();
    assert_eq!(event_indexes(&pool).await?, without_both);

    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    assert_eq!(event_indexes(&pool).await?, all);
    Ok(())
}

/// Writes the events of three types, interleaved: `Page` (`Created`/`Edited`),
/// `Comment` (`Posted`), `Tag` (`Added`) — 160 events — then stamps every
/// `Created`/`Posted` at `T` and every `Edited`/`Added` at `T + 1`, so the
/// arms have distinct latest seconds whatever the commit clock did.
async fn seed_arms<DB>(pool: &Pool<DB>) -> anyhow::Result<Sql<DB>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Sql<DB>: Executor,
{
    use evento::sql::Event as EventTable;
    use sea_query::{Expr, ExprTrait};

    let store = Sql::<DB>::from(pool.clone());
    let event = |aggregate_type: &str, aggregate_id: String, name: &str, version: u16| Event {
        id: ulid::Ulid::generate(),
        aggregate_type: aggregate_type.into(),
        aggregate_id,
        name: name.into(),
        version,
        ..Default::default()
    };
    for i in 0..40u16 {
        store
            .write(vec![
                event("demo/Page", format!("p{i}"), "Created", 1),
                event("demo/Comment", format!("c{i}"), "Posted", 1),
            ])
            .await?;
        store
            .write(vec![
                event("demo/Page", format!("p{i}"), "Edited", 2),
                event("demo/Tag", format!("t{i}"), "Added", 1),
            ])
            .await?;
    }
    for (names, at) in [
        (["Created", "Posted"], SEED_AT),
        (["Edited", "Added"], SEED_AT + 1),
    ] {
        execute(
            pool,
            Query::update()
                .table(EventTable::Table)
                .value(EventTable::Timestamp, at)
                .and_where(Expr::col(EventTable::Name).is_in(names))
                .to_owned(),
        )
        .await?;
    }
    Ok(store)
}

/// The second the first events of [`seed_arms`] are stamped at.
const SEED_AT: i64 = 1_700_000_000;

/// Every page of `filters` in `direction`, as event ids in page order.
async fn pages<E: Executor>(
    store: &E,
    filters: Option<Arc<[EventFilter]>>,
    size: u16,
    backward: bool,
) -> anyhow::Result<Vec<Vec<String>>> {
    let mut pages = Vec::new();
    let mut cursor: Option<Value> = None;
    loop {
        let args = if backward {
            Args::backward(size, cursor.clone())
        } else {
            Args::forward(size, cursor.clone())
        };
        let page = store
            .read(
                filters.clone(),
                Some(RoutingKey::All),
                TenantFilter::All,
                args,
                None,
            )
            .await?;
        if page.edges.is_empty() {
            break;
        }
        pages.push(page.edges.iter().map(|e| e.node.id.to_string()).collect());
        let (more, next) = if backward {
            (
                page.page_info.has_previous_page,
                page.page_info.start_cursor,
            )
        } else {
            (page.page_info.has_next_page, page.page_info.end_cursor)
        };
        if !more {
            break;
        }
        cursor = next;
        assert!(pages.len() < 1000, "runaway paging");
    }
    Ok(pages)
}

/// A read over several filters is paged as the `UNION ALL` of one seek per
/// filter (#478): the pages are the ones a single `OR` of the filters gives,
/// forward and backward.
pub async fn several_filters_page_like_every_event<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Sql<DB>: Executor,
{
    let store = seed_arms(&pool).await?;
    // Three arms of three types: together they are every event.
    let arms: Arc<[EventFilter]> = vec![
        EventFilter::by_event_raw("demo/Page", "Created"),
        EventFilter::by_event_raw("demo/Page", "Edited"),
        EventFilter::by_event_raw("demo/Comment", "Posted"),
        EventFilter::by_type_raw("demo/Tag"),
    ]
    .into();
    for size in [1u16, 3, 7, 160, 500] {
        for backward in [false, true] {
            let expected = pages(&store, None, size, backward).await?;
            let actual = pages(&store, Some(arms.clone()), size, backward).await?;
            assert_eq!(actual, expected, "size {size} backward {backward}");
            assert_eq!(actual.concat().len(), 160);
        }
    }
    Ok(())
}

/// A subset of arms, one of them empty, pages in store order without
/// duplicates, both ways.
pub async fn a_subset_of_arms_pages_in_order_without_duplicates<DB>(
    pool: Pool<DB>,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Sql<DB>: Executor,
{
    let store = seed_arms(&pool).await?;
    let arms: Arc<[EventFilter]> = vec![
        EventFilter::by_event_raw("demo/Page", "Edited"),
        EventFilter::by_event_raw("demo/Comment", "Posted"),
        // Never written: an arm without rows.
        EventFilter::by_event_raw("demo/Page", "Deleted"),
    ]
    .into();
    let all: Vec<String> = pages(&store, None, 500, false).await?.concat();
    let ids: Vec<String> = pages(&store, Some(arms.clone()), 5, false).await?.concat();
    assert_eq!(ids.len(), 80);
    // In store order, each once.
    let expected: Vec<String> = all.iter().filter(|id| ids.contains(id)).cloned().collect();
    assert_eq!(ids, expected);
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 80);

    // Backward pages — last page first, each in store order — give the same
    // events.
    let mut back = pages(&store, Some(arms), 5, true).await?;
    back.reverse();
    assert_eq!(back.concat(), ids);
    Ok(())
}

/// `latest_timestamp` over several filters is the greatest arm, and 0 when
/// no arm has events.
pub async fn latest_timestamp_is_the_greatest_arm<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: sqlx::IntoArguments<DB>,
    Sql<DB>: Executor,
{
    let store = seed_arms(&pool).await?;
    let latest = |filters: Vec<EventFilter>| {
        let store = &store;
        async move {
            store
                .latest_timestamp(
                    Some(filters.into()),
                    Some(RoutingKey::All),
                    TenantFilter::All,
                )
                .await
        }
    };
    let created = latest(vec![EventFilter::by_event_raw("demo/Page", "Created")]).await?;
    let tag = latest(vec![EventFilter::by_type_raw("demo/Tag")]).await?;
    assert_eq!(created, SEED_AT as u64);
    assert_eq!(tag, created + 1);
    assert_eq!(
        latest(vec![
            EventFilter::by_event_raw("demo/Page", "Created"),
            EventFilter::by_type_raw("demo/Tag"),
            EventFilter::by_event_raw("demo/Page", "Deleted"),
        ])
        .await?,
        tag
    );
    assert_eq!(
        latest(vec![
            EventFilter::by_event_raw("demo/Page", "Deleted"),
            EventFilter::by_event_raw("demo/Comment", "Removed"),
        ])
        .await?,
        0
    );
    Ok(())
}
