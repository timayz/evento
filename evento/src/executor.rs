use std::{hash::Hash, sync::Arc};
use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    Event, RoutingKey, WriteError,
};

#[derive(Clone, PartialEq, Eq)]
pub struct ReadAggregator {
    pub aggregator_type: String,
    pub aggregator_id: Option<String>,
    pub name: Option<String>,
}

impl ReadAggregator {
    pub fn new(
        aggregator_type: impl Into<String>,
        id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: Some(id.into()),
            name: Some(name.into()),
        }
    }

    pub fn aggregator(value: impl Into<String>) -> Self {
        Self {
            aggregator_type: value.into(),
            aggregator_id: None,
            name: None,
        }
    }

    pub fn id(aggregator_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: Some(id.into()),
            name: None,
        }
    }

    pub fn event(aggregator_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            aggregator_type: aggregator_type.into(),
            aggregator_id: None,
            name: Some(name.into()),
        }
    }
}

impl Hash for ReadAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.aggregator_type.hash(state);
        self.aggregator_id.hash(state);
        self.name.hash(state);
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;
    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>>;
    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool>;
    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()>;
    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()>;

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>>;
}

pub struct Evento(Arc<Box<dyn Executor>>);

impl Clone for Evento {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait::async_trait]
impl Executor for Evento {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.0.write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.0.read(aggregators, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.0.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.0.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.0.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.0.acknowledge(key, cursor, lag).await
    }
}

#[cfg(feature = "sqlite")]
impl From<crate::Sqlite> for Evento {
    fn from(value: crate::Sqlite) -> Self {
        Self(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "sqlite")]
impl From<&crate::Sqlite> for Evento {
    fn from(value: &crate::Sqlite) -> Self {
        Self(Arc::new(Box::new(value.clone())))
    }
}

#[cfg(feature = "mysql")]
impl From<crate::MySql> for Evento {
    fn from(value: crate::MySql) -> Self {
        Self(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "mysql")]
impl From<&crate::MySql> for Evento {
    fn from(value: &crate::MySql) -> Self {
        Self(Arc::new(Box::new(value.clone())))
    }
}

#[cfg(feature = "postgres")]
impl From<crate::Postgres> for Evento {
    fn from(value: crate::Postgres) -> Self {
        Self(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "postgres")]
impl From<&crate::Postgres> for Evento {
    fn from(value: &crate::Postgres) -> Self {
        Self(Arc::new(Box::new(value.clone())))
    }
}

#[cfg(feature = "rw")]
impl<R: Executor + Clone, W: Executor + Clone> From<crate::Rw<R, W>> for Evento {
    fn from(value: crate::Rw<R, W>) -> Self {
        Self(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "rw")]
impl<R: Executor + Clone, W: Executor + Clone> From<&crate::Rw<R, W>> for Evento {
    fn from(value: &crate::Rw<R, W>) -> Self {
        Self(Arc::new(Box::new(value.clone())))
    }
}

#[cfg(feature = "group")]
#[derive(Clone, Default)]
pub struct EventoGroup {
    executors: Vec<Evento>,
}

#[cfg(feature = "group")]
impl EventoGroup {
    pub fn executor(mut self, executor: impl Into<Evento>) -> Self {
        self.executors.push(executor.into());

        self
    }

    pub fn first(&self) -> &Evento {
        self.executors
            .first()
            .expect("EventoGroup must have at least one executor")
    }
}

#[cfg(feature = "group")]
#[async_trait::async_trait]
impl Executor for EventoGroup {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.first().write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        use crate::cursor;
        let futures = self
            .executors
            .iter()
            .map(|e| e.read(aggregators.to_owned(), routing_key.to_owned(), args.clone()));

        let results = futures_util::future::join_all(futures).await;
        let mut events = vec![];
        for res in results {
            for edge in res?.edges {
                events.push(edge.node);
            }
        }

        Ok(cursor::Reader::new(events).args(args).execute()?)
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.first().get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.first().is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.first().upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.first().acknowledge(key, cursor, lag).await
    }
}

#[cfg(feature = "rw")]
pub struct Rw<R: Executor, W: Executor> {
    r: R,
    w: W,
}

#[cfg(feature = "rw")]
impl<R: Executor + Clone, W: Executor + Clone> Clone for Rw<R, W> {
    fn clone(&self) -> Self {
        Self {
            r: self.r.clone(),
            w: self.w.clone(),
        }
    }
}

#[cfg(feature = "rw")]
#[async_trait::async_trait]
impl<R: Executor, W: Executor> Executor for Rw<R, W> {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        self.w.write(events).await
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        self.r.read(aggregators, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        self.r.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        self.r.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.w.upsert_subscriber(key, worker_id).await
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        self.w.acknowledge(key, cursor, lag).await
    }
}

#[cfg(feature = "rw")]
impl<R: Executor, W: Executor> From<(R, W)> for Rw<R, W> {
    fn from((r, w): (R, W)) -> Self {
        Self { r, w }
    }
}

#[cfg(all(
    feature = "rw",
    any(feature = "sqlite", feature = "postgres", feature = "mysql")
))]
impl<R: sqlx::Database, W: sqlx::Database> From<(sqlx::Pool<R>, sqlx::Pool<W>)>
    for Rw<crate::sql::Sql<R>, crate::sql::Sql<W>>
where
    str: sqlx::Type<W>,
    str: sqlx::Type<R>,
    for<'r> String: sqlx::Decode<'r, W> + sqlx::Type<W>,
    for<'r> String: sqlx::Decode<'r, R> + sqlx::Type<R>,
    for<'r> bool: sqlx::Decode<'r, W> + sqlx::Type<W>,
    for<'r> bool: sqlx::Decode<'r, R> + sqlx::Type<R>,
    for<'r> Vec<u8>: sqlx::Decode<'r, W> + sqlx::Type<W>,
    for<'r> Vec<u8>: sqlx::Decode<'r, R> + sqlx::Type<R>,
    crate::Event: for<'r> sqlx::FromRow<'r, <W as sqlx::Database>::Row>,
    crate::Event: for<'r> sqlx::FromRow<'r, <R as sqlx::Database>::Row>,
    usize: sqlx::ColumnIndex<<W as sqlx::Database>::Row>,
    usize: sqlx::ColumnIndex<<R as sqlx::Database>::Row>,
    for<'q> sea_query_sqlx::SqlxValues: sqlx::IntoArguments<'q, W>,
    for<'q> sea_query_sqlx::SqlxValues: sqlx::IntoArguments<'q, R>,
    for<'c> &'c mut <W as sqlx::Database>::Connection: sqlx::Executor<'c, Database = W>,
    for<'c> &'c mut <R as sqlx::Database>::Connection: sqlx::Executor<'c, Database = R>,
{
    fn from((r, w): (sqlx::Pool<R>, sqlx::Pool<W>)) -> Self {
        Self {
            r: r.into(),
            w: w.into(),
        }
    }
}
