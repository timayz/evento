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

#[derive(Iden, Clone)]
pub enum Event {
    Table,
    Id,
    Name,
    AggregatorType,
    AggregatorId,
    Version,
    Data,
    Metadata,
    RoutingKey,
    Timestamp,
    TimestampSubsec,
}

#[derive(Iden)]
pub enum Snapshot {
    Table,
    Id,
    Type,
    Cursor,
    Revision,
    Data,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
pub enum Subscriber {
    Table,
    Key,
    WorkerId,
    Cursor,
    Lag,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

#[cfg(feature = "mysql")]
pub type MySql = Sql<sqlx::MySql>;

#[cfg(feature = "mysql")]
pub type RwMySql = evento_core::Rw<MySql, MySql>;

#[cfg(feature = "postgres")]
pub type Postgres = Sql<sqlx::Postgres>;

#[cfg(feature = "postgres")]
pub type RwPostgres = evento_core::Rw<Postgres, Postgres>;

#[cfg(feature = "sqlite")]
pub type Sqlite = Sql<sqlx::Sqlite>;

#[cfg(feature = "sqlite")]
pub type RwSqlite = evento_core::Rw<Sqlite, Sqlite>;

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

pub struct Reader {
    statement: SelectStatement,
    args: Args,
    order: cursor::Order,
}

impl Reader {
    pub fn new(statement: SelectStatement) -> Self {
        Self {
            statement,
            args: Args::default(),
            order: cursor::Order::Asc,
        }
    }

    pub fn order(&mut self, order: cursor::Order) -> &mut Self {
        self.order = order;

        self
    }

    pub fn desc(&mut self) -> &mut Self {
        self.order(cursor::Order::Desc)
    }

    pub fn args(&mut self, args: Args) -> &mut Self {
        self.args = args;

        self
    }

    pub fn backward(&mut self, last: u16, before: Option<Value>) -> &mut Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    pub fn forward(&mut self, first: u16, after: Option<Value>) -> &mut Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

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

    fn build_reader<O: Cursor, B: Bind<Cursor = O>>(
        &mut self,
    ) -> Result<u16, bincode::error::DecodeError>
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

    fn build_reader_where<O, B>(
        &mut self,
        cursor: &Value,
    ) -> Result<(), bincode::error::DecodeError>
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

pub trait Bind {
    type T: IntoColumnRef + Clone;
    type I: IntoIterator<Item = Self::T>;
    type V: IntoIterator<Item = Expr>;
    type Cursor: Cursor;

    fn columns() -> Self::I;
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
