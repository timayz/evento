pub mod context;
pub mod cursor;
mod load;
mod save;
mod subscribe;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql;
#[cfg(any(
    feature = "sqlite-migrator",
    feature = "mysql-migrator",
    feature = "postgres-migrator"
))]
pub mod sql_migrator;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use load::*;
pub use save::*;
pub use subscribe::*;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashSet, fmt::Debug};
use ulid::Ulid;

use crate::cursor::{Args, Cursor, ReadResult, Value};

pub mod prelude {
    #[cfg(feature = "stream")]
    pub use tokio_stream::StreamExt;
}

pub struct EventData<D, M> {
    pub details: Event,
    pub data: D,
    pub metadata: M,
}

#[cfg(feature = "mysql")]
pub use sql::MySql;

#[cfg(feature = "postgres")]
pub use sql::Postgres;

#[cfg(feature = "sqlite")]
pub use sql::Sqlite;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventCursor {
    pub i: Ulid,
    pub v: i32,
    pub t: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub id: Ulid,
    pub aggregator_id: String,
    pub aggregator_type: String,
    pub version: i32,
    pub name: String,
    pub routing_key: Option<String>,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub timestamp: i64,
}

impl Event {
    pub fn to_data<D: AggregatorName + DeserializeOwned, M: DeserializeOwned>(
        &self,
    ) -> Result<Option<EventData<D, M>>, ciborium::de::Error<std::io::Error>> {
        if D::name() != self.name {
            return Ok(None);
        }

        let data = ciborium::from_reader(&self.data[..])?;
        let metadata = ciborium::from_reader(&self.metadata[..])?;

        Ok(Some(EventData {
            data,
            metadata,
            details: self.clone(),
        }))
    }
}

impl Cursor for Event {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        EventCursor {
            i: self.id,
            v: self.version,
            t: self.timestamp,
        }
    }
}

pub trait Aggregator:
    Default + Send + Sync + Serialize + DeserializeOwned + Clone + AggregatorName + Debug
{
    fn aggregate<'async_trait>(
        &'async_trait mut self,
        event: &'async_trait Event,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait;
    fn revision() -> &'static str;
}

pub trait AggregatorName {
    fn name() -> &'static str;
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;

    async fn get_event<A: Aggregator>(&self, cursor: Value) -> Result<Event, ReadError>;

    async fn read_by_aggregator<A: Aggregator>(
        &self,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError>;

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError>;

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError>;

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError>;

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError>;

    async fn get_snapshot<A: Aggregator>(
        &self,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError>;

    async fn save_snapshot<A: Aggregator>(
        &self,
        id: String,
        data: Vec<u8>,
        cursor: cursor::Value,
    ) -> Result<(), WriteError>;

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: i64,
    ) -> Result<(), AcknowledgeError>;
}

#[derive(Clone)]
pub enum Evento {
    #[cfg(feature = "sqlite")]
    Sqlite(crate::Sqlite),
    #[cfg(feature = "mysql")]
    MySql(crate::MySql),
    #[cfg(feature = "postgres")]
    Postgres(crate::Postgres),
}

#[async_trait::async_trait]
impl Executor for Evento {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.write(events).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.write(events).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.write(events).await,
        }
    }

    async fn get_event<A: Aggregator>(&self, cursor: Value) -> Result<Event, ReadError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.get_event::<A>(cursor).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.get_event::<A>(cursor).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.get_event::<A>(cursor).await,
        }
    }

    async fn read_by_aggregator<A: Aggregator>(
        &self,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.read_by_aggregator::<A>(id, args).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.read_by_aggregator::<A>(id, args).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.read_by_aggregator::<A>(id, args).await,
        }
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.read(aggregator_types, routing_key, args).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.read(aggregator_types, routing_key, args).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.read(aggregator_types, routing_key, args).await,
        }
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.get_subscriber_cursor(key).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.get_subscriber_cursor(key).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.get_subscriber_cursor(key).await,
        }
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.is_subscriber_running(key, worker_id).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.is_subscriber_running(key, worker_id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.is_subscriber_running(key, worker_id).await,
        }
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.upsert_subscriber(key, worker_id).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.upsert_subscriber(key, worker_id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.upsert_subscriber(key, worker_id).await,
        }
    }

    async fn get_snapshot<A: Aggregator>(
        &self,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.get_snapshot::<A>(id).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.get_snapshot::<A>(id).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.get_snapshot::<A>(id).await,
        }
    }

    async fn save_snapshot<A: Aggregator>(
        &self,
        id: String,
        data: Vec<u8>,
        cursor: cursor::Value,
    ) -> Result<(), WriteError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.save_snapshot::<A>(id, data, cursor).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.save_snapshot::<A>(id, data, cursor).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.save_snapshot::<A>(id, data, cursor).await,
        }
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: i64,
    ) -> Result<(), AcknowledgeError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(executor) => executor.acknowledge(key, cursor, lag).await,
            #[cfg(feature = "mysql")]
            Self::MySql(executor) => executor.acknowledge(key, cursor, lag).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.acknowledge(key, cursor, lag).await,
        }
    }
}

#[cfg(feature = "sqlite")]
impl From<Sqlite> for Evento {
    fn from(value: Sqlite) -> Self {
        Self::Sqlite(value)
    }
}

#[cfg(feature = "mysql")]
impl From<MySql> for Evento {
    fn from(value: MySql) -> Self {
        Self::MySql(value)
    }
}

#[cfg(feature = "postgres")]
impl From<Postgres> for Evento {
    fn from(value: Postgres) -> Self {
        Self::Postgres(value)
    }
}
