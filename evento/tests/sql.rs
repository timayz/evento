#[path = "evento.rs"]
mod evento_test;

#[path = "cursor.rs"]
mod cursor_test;

use std::str::FromStr;

use evento::{
    cursor::{Args, Order, ReadResult},
    sql::{Reader, RwSqlite, Sql},
    sql_migrator::{InitMigration, M0002},
    Event,
};
use sea_query::{MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::{
    any::install_default_drivers,
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Any, Database, MySqlPool, PgPool, Pool, SqlitePool,
};
use sqlx_migrator::{Migrate, Plan};

use crate::cursor_test::assert_read_result;

#[tokio::test]
async fn sqlite_version() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("version").await?;

    evento_test::version(&executor).await
}

#[tokio::test]
async fn sqlite_routing_key() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn sqlite_load() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn sqlite_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn sqlite_subscriber_running() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::Sqlite>>(&pool.into()).await
}

#[tokio::test]
async fn sqlite_subscribe() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe::<Sql<sqlx::Sqlite>>(&pool.into(), data).await
}

#[tokio::test]
async fn sqlite_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_routing_key").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::Sqlite>>(&pool.into(), data).await
}

#[tokio::test]
async fn sqlite_subscribe_default() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_default").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default::<Sql<sqlx::Sqlite>>(&pool.into(), data).await
}

#[tokio::test]
async fn sqlite_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into(), data).await
}

#[tokio::test]
async fn sqlite_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_routing_key_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into(), data)
        .await
}

#[tokio::test]
async fn sqlite_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("subscribe_default_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::Sqlite>>(&pool.into(), data)
        .await
}

#[tokio::test]
async fn rw_sqlite_version() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("version").await?;

    evento_test::version(&executor).await
}

#[tokio::test]
async fn rw_sqlite_routing_key() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn rw_sqlite_load() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn rw_sqlite_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_rw_sqlite_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn rw_sqlite_subscriber_running() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscriber_running").await?;

    evento_test::subscriber_running::<RwSqlite>(&pool.into()).await
}

#[tokio::test]
async fn rw_sqlite_subscribe() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_routing_key").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe_routing_key::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_default() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_default").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe_default::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_multiple_aggregator").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe_multiple_aggregator::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_routing_key_multiple_aggregator").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn rw_sqlite_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_rw_sqlite_pool("subscribe_default_multiple_aggregator").await?;
    let data = get_data(&pool.1).await?;

    evento_test::subscribe_default_multiple_aggregator::<RwSqlite>(&pool.into(), data).await
}

#[tokio::test]
async fn mysql_version() -> anyhow::Result<()> {
    let executor = create_sqlite_executor("version").await?;

    evento_test::version(&executor).await
}

#[tokio::test]
async fn mysql_routing_key() -> anyhow::Result<()> {
    let executor = create_mysql_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn mysql_load() -> anyhow::Result<()> {
    let executor = create_mysql_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn mysql_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_mysql_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn mysql_subscriber_running() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::MySql>>(&pool.into()).await
}

#[tokio::test]
async fn mysql_subscribe() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe::<Sql<sqlx::MySql>>(&pool.into(), data).await
}

#[tokio::test]
async fn mysql_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_routing_key").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::MySql>>(&pool.into(), data).await
}

#[tokio::test]
async fn mysql_subscribe_default() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_default").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default::<Sql<sqlx::MySql>>(&pool.into(), data).await
}

#[tokio::test]
async fn mysql_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into(), data).await
}

#[tokio::test]
async fn mysql_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_routing_key_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into(), data)
        .await
}

#[tokio::test]
async fn mysql_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_mysql_pool("subscribe_default_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::MySql>>(&pool.into(), data).await
}

#[tokio::test]
async fn postgres_version() -> anyhow::Result<()> {
    let executor = create_postgres_executor("version").await?;

    evento_test::version(&executor).await
}

#[tokio::test]
async fn postgres_routing_key() -> anyhow::Result<()> {
    let executor = create_postgres_executor("routing_key").await?;

    evento_test::routing_key(&executor).await
}

#[tokio::test]
async fn postgres_load() -> anyhow::Result<()> {
    let executor = create_postgres_executor("load").await?;

    evento_test::load(&executor).await
}

#[tokio::test]
async fn postgres_invalid_original_version() -> anyhow::Result<()> {
    let executor = create_postgres_executor("invalid_original_version").await?;

    evento_test::invalid_original_version(&executor).await
}

#[tokio::test]
async fn postgres_subscriber_running() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscriber_running").await?;

    evento_test::subscriber_running::<Sql<sqlx::Postgres>>(&pool.into()).await
}

#[tokio::test]
async fn postgres_subscribe() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe::<Sql<sqlx::Postgres>>(&pool.into(), data).await
}

#[tokio::test]
async fn postgres_subscribe_routing_key() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_routing_key").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key::<Sql<sqlx::Postgres>>(&pool.into(), data).await
}

#[tokio::test]
async fn postgres_subscribe_default() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_default").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default::<Sql<sqlx::Postgres>>(&pool.into(), data).await
}

#[tokio::test]
async fn postgres_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_multiple_aggregator::<Sql<sqlx::Postgres>>(&pool.into(), data).await
}

#[tokio::test]
async fn postgres_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_routing_key_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_routing_key_multiple_aggregator::<Sql<sqlx::Postgres>>(
        &pool.into(),
        data,
    )
    .await
}

#[tokio::test]
async fn postgres_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    let pool = create_postgres_pool("subscribe_default_multiple_aggregator").await?;
    let data = get_data(&pool).await?;

    evento_test::subscribe_default_multiple_aggregator::<Sql<sqlx::Postgres>>(&pool.into(), data)
        .await
}

#[tokio::test]
async fn sqlite_forward_asc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("forward_asc").await?;

    forward_asc(pool).await
}

#[tokio::test]
async fn sqlite_forward_desc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("forward_desc").await?;

    forward_desc(pool).await
}

#[tokio::test]
async fn sqlite_backward_asc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("backward_asc").await?;

    backward_asc(pool).await
}

#[tokio::test]
async fn sqlite_backward_desc() -> anyhow::Result<()> {
    let pool = create_sqlite_pool("backward_desc").await?;

    backward_desc(pool).await
}

#[tokio::test]
async fn mysql_forward_asc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("forward_asc").await?;

    forward_asc(pool).await
}

#[tokio::test]
async fn mysql_forward_desc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("forward_desc").await?;

    forward_desc(pool).await
}

#[tokio::test]
async fn mysql_backward_asc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("backward_asc").await?;

    backward_asc(pool).await
}

#[tokio::test]
async fn mysql_backward_desc() -> anyhow::Result<()> {
    let pool = create_mysql_pool("backward_desc").await?;

    backward_desc(pool).await
}

#[tokio::test]
async fn postgres_forward_asc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("forward_asc").await?;

    forward_asc(pool).await
}

#[tokio::test]
async fn postgres_forward_desc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("forward_desc").await?;

    forward_desc(pool).await
}

#[tokio::test]
async fn postgres_backward_asc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("backward_asc").await?;

    backward_asc(pool).await
}

#[tokio::test]
async fn postgres_backward_desc() -> anyhow::Result<()> {
    let pool = create_postgres_pool("backward_desc").await?;

    backward_desc(pool).await
}

async fn forward_asc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Event: for<'r> sqlx::FromRow<'r, DB::Row>,
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

async fn forward_desc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Event: for<'r> sqlx::FromRow<'r, DB::Row>,
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

async fn backward_asc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Event: for<'r> sqlx::FromRow<'r, DB::Row>,
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

async fn backward_desc<DB>(pool: Pool<DB>) -> anyhow::Result<()>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Event: for<'r> sqlx::FromRow<'r, DB::Row>,
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

async fn read<DB>(pool: &Pool<DB>, args: Args, order: Order) -> anyhow::Result<ReadResult<Event>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Event: for<'r> sqlx::FromRow<'r, DB::Row>,
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
        .execute::<_, crate::Event, _>(pool)
        .await?)
}

async fn get_data<DB>(pool: &Pool<DB>) -> anyhow::Result<Vec<Event>>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    sea_query_sqlx::SqlxValues: for<'q> sqlx::IntoArguments<'q, DB>,
{
    let data = cursor_test::get_data();
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
        statement.values_panic([
            event.id.to_string().into(),
            event.name.into(),
            event.data.into(),
            event.metadata.into(),
            event.aggregator_type.into(),
            event.aggregator_id.to_string().into(),
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

    sqlx::query_with::<DB, _>(&sql, values)
        .execute(pool)
        .await?;

    Ok(data)
}

async fn create_postgres_executor(key: impl Into<String>) -> anyhow::Result<Sql<sqlx::Postgres>> {
    Ok(create_postgres_pool(key).await?.into())
}

async fn create_mysql_executor(key: impl Into<String>) -> anyhow::Result<Sql<sqlx::MySql>> {
    Ok(create_mysql_pool(key).await?.into())
}

async fn create_rw_sqlite_executor(
    key: impl Into<String>,
) -> anyhow::Result<evento::Rw<evento::Sqlite, evento::Sqlite>> {
    let executor: evento::Rw<evento::Sqlite, evento::Sqlite> =
        create_rw_sqlite_pool(key).await?.into();

    Ok(executor.into())
}

async fn create_sqlite_executor(key: impl Into<String>) -> anyhow::Result<evento::Evento> {
    let executor: evento::Sqlite = create_sqlite_pool(key).await?.into();

    Ok(executor.into())
}

async fn create_postgres_pool(key: impl Into<String>) -> anyhow::Result<PgPool> {
    let key = key.into();
    let url = format!("postgres://postgres:postgres@localhost:5432/{key}");

    create_pool(url).await
}

async fn create_mysql_pool(key: impl Into<String>) -> anyhow::Result<MySqlPool> {
    let key = key.into();
    let url = format!("mysql://root:root@localhost:3306/{key}");

    create_pool(url).await
}

async fn create_rw_sqlite_pool(key: impl Into<String>) -> anyhow::Result<(SqlitePool, SqlitePool)> {
    let key = key.into();
    let key = format!("rw_{key}");
    let url = format!("sqlite:../target/tmp/test_sql_{key}.db");

    let w = create_sqlite_pool(key).await?;
    sqlx::query("PRAGMA journal_mode = WAL").execute(&w).await?;
    sqlx::query("PRAGMA busy_timeout = 5000")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA cache_size = -20000")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA foreign_keys = true")
        .execute(&w)
        .await?;
    sqlx::query("PRAGMA temp_store = memory")
        .execute(&w)
        .await?;

    let options = SqliteConnectOptions::from_str(&url)?.read_only(true);

    let r = SqlitePoolOptions::new().connect_with(options).await?;
    sqlx::query("PRAGMA journal_mode = WAL").execute(&r).await?;
    sqlx::query("PRAGMA busy_timeout = 5000")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA cache_size = -20000")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA foreign_keys = true")
        .execute(&r)
        .await?;
    sqlx::query("PRAGMA temp_store = memory")
        .execute(&r)
        .await?;

    Ok((r, w))
}

async fn create_sqlite_pool(key: impl Into<String>) -> anyhow::Result<SqlitePool> {
    let key = key.into();
    let url = format!("sqlite:../target/tmp/test_sql_{key}.db");

    create_pool(url).await
}

async fn create_pool<DB: Database>(url: impl Into<String>) -> anyhow::Result<Pool<DB>>
where
    for<'q> DB::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
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
