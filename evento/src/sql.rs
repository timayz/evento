use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
};

#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "postgres")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;
use sea_query::{Expr, ExprTrait, Iden, IntoColumnRef, OnConflict, Query, SelectStatement};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Database, Pool};
use ulid::Ulid;

use crate::{
    cursor::{self, Args, Cursor, Edge, PageInfo, ReadResult, Value},
    AcknowledgeError, Executor, ReadError, SubscribeError, WriteError,
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

#[cfg(feature = "postgres")]
pub type Postgres = Sql<sqlx::Postgres>;

#[cfg(feature = "sqlite")]
pub type Sqlite = Sql<sqlx::Sqlite>;

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
    crate::Event: for<'r> sqlx::FromRow<'r, DB::Row>,
{
    async fn get_event(&self, cursor: Value) -> Result<crate::Event, ReadError> {
        let cursor = crate::Event::deserialize_cursor(&cursor)?;
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
            .and_where(Expr::col(Event::Id).eq(Expr::value(cursor.i.to_string())))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_as_with::<DB, crate::Event, _>(&sql, values)
            .fetch_one(&self.0)
            .await
            .map_err(|err| ReadError::Unknown(err.into()))
    }

    async fn read_by_aggregator(
        &self,
        aggregator_type: String,
        id: String,
        args: Args,
    ) -> Result<ReadResult<crate::Event>, ReadError> {
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
            .and_where(Expr::col(Event::AggregatorType).eq(Expr::value(aggregator_type)))
            .and_where(Expr::col(Event::AggregatorId).eq(Expr::value(id)))
            .to_owned();

        Reader::new(statement)
            .args(args)
            .execute::<_, crate::Event, _>(&self.0)
            .await
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: crate::RoutingKey,
        args: Args,
    ) -> Result<ReadResult<crate::Event>, ReadError> {
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
            .and_where(Expr::col(Event::AggregatorType).in_tuples(aggregator_types))
            .conditions(
                matches!(routing_key, crate::RoutingKey::Value(_)),
                |q| {
                    if let crate::RoutingKey::Value(Some(ref routing_key)) = routing_key {
                        q.and_where(Expr::col(Event::RoutingKey).eq(routing_key));
                    }

                    if let crate::RoutingKey::Value(None) = routing_key {
                        q.and_where(Expr::col(Event::RoutingKey).is_null());
                    }
                },
                |_q| {},
            )
            .to_owned();

        Reader::new(statement)
            .args(args)
            .execute::<_, crate::Event, _>(&self.0)
            .await
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        let statement = Query::select()
            .columns([Subscriber::Cursor])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let Some((cursor,)) = sqlx::query_as_with::<DB, (Option<String>,), _>(&sql, values)
            .fetch_optional(&self.0)
            .await
            .map_err(|err| SubscribeError::Unknown(err.into()))?
        else {
            return Ok(None);
        };

        Ok(cursor.map(|c| c.into()))
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        let statement = Query::select()
            .columns([Subscriber::WorkerId, Subscriber::Enabled])
            .from(Subscriber::Table)
            .and_where(Expr::col(Subscriber::Key).eq(Expr::value(key)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        let (id, enabled) = sqlx::query_as_with::<DB, (String, bool), _>(&sql, values)
            .fetch_one(&self.0)
            .await
            .map_err(|err| SubscribeError::Unknown(err.into()))?;

        Ok(worker_id.to_string() == id && enabled)
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
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
            .await
            .map_err(|err| SubscribeError::Unknown(err.into()))?;

        Ok(())
    }

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        let statement = Query::select()
            .columns([Snapshot::Data, Snapshot::Cursor])
            .from(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Type).eq(Expr::value(aggregator_type)))
            .and_where(Expr::col(Snapshot::Id).eq(Expr::value(id)))
            .and_where(Expr::col(Snapshot::Revision).eq(Expr::value(aggregator_revision)))
            .limit(1)
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_as_with::<DB, (Vec<u8>, String), _>(&sql, values)
            .fetch_optional(&self.0)
            .await
            .map(|res| res.map(|(data, cursor)| (data, cursor.into())))
            .map_err(|err| ReadError::Unknown(err.into()))
    }

    async fn write(&self, events: Vec<crate::Event>) -> Result<(), WriteError> {
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

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError> {
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
                aggregator_type.into(),
                id.to_string().into(),
                cursor.to_string().into(),
                aggregator_revision.into(),
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

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.0)
            .await
            .map_err(|err| WriteError::Unknown(err.into()))?;

        Ok(())
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: u64,
    ) -> Result<(), AcknowledgeError> {
        let statement = Query::update()
            .table(Subscriber::Table)
            .values([
                (Subscriber::Cursor, cursor.to_string().into()),
                (Subscriber::Lag, lag.into()),
                (Subscriber::UpdatedAt, Expr::current_timestamp()),
            ])
            .and_where(Expr::col(Subscriber::Key).eq(key))
            .to_owned();

        let (sql, values) = Self::build_sqlx(statement);

        sqlx::query_with::<DB, _>(&sql, values)
            .execute(&self.0)
            .await
            .map_err(|err| AcknowledgeError::Unknown(err.into()))?;

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
    ) -> Result<ReadResult<O>, ReadError>
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
            .await
            .map_err(|err| ReadError::Unknown(err.into()))?;

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

    fn build_reader<O: Cursor, B: Bind<Cursor = O>>(&mut self) -> Result<u16, ReadError>
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

    fn build_reader_where<O, B>(&mut self, cursor: &Value) -> Result<(), ReadError>
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

impl Bind for crate::Event {
    type T = Event;
    type I = [Self::T; 4];
    type V = [Expr; 4];
    type Cursor = Self;

    fn columns() -> Self::I {
        [
            Event::TimestampSubsec,
            Event::Timestamp,
            Event::Version,
            Event::Id,
        ]
    }

    fn values(cursor: <<Self as Bind>::Cursor as Cursor>::T) -> Self::V {
        [
            cursor.s.into(),
            cursor.t.into(),
            cursor.v.into(),
            cursor.i.to_string().into(),
        ]
    }
}

impl<R: sqlx::Row> sqlx::FromRow<'_, R> for crate::Event
where
    i32: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    Vec<u8>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::Type<R::Database> + sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
{
    fn from_row(row: &R) -> Result<Self, sqlx::Error> {
        let timestamp: i64 = row.try_get("timestamp")?;
        let timestamp_subsec: i64 = row.try_get("timestamp_subsec")?;

        Ok(crate::Event {
            id: Ulid::from_string(row.try_get("id")?)
                .map_err(|err| sqlx::Error::InvalidArgument(err.to_string()))?,
            aggregator_id: row.try_get("aggregator_id")?,
            aggregator_type: row.try_get("aggregator_type")?,
            version: row.try_get("version")?,
            name: row.try_get("name")?,
            routing_key: row.try_get("routing_key")?,
            data: row.try_get("data")?,
            metadata: row.try_get("metadata")?,
            timestamp: timestamp as u64,
            timestamp_subsec: timestamp_subsec as u32,
        })
    }
}
