use evento::{
    cursor::{Args, Order, ReadResult},
    sql::Reader,
    sql_migrator::{InitMigration, M0002, M0003, M0004, M0005, M0006, M0007, M0008},
    Event,
};
use evento_test::assert_read_result;
use sea_query::{MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::{any::install_default_drivers, migrate::MigrateDatabase, Any, Database, Pool};
use sqlx_migrator::{Migrate, Plan};

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

    Ok(Reader::new(statement)
        .args(args)
        .order(order)
        .execute::<_, evento_sql::SqlEvent, _>(pool)
        .await?
        .map(|e| e.0))
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
    sqlx_migrator::Migrator<DB>: sqlx_migrator::migrator::DatabaseOperation<DB>,
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
    sqlx_migrator::Migrator<DB>: sqlx_migrator::migrator::DatabaseOperation<DB>,
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
