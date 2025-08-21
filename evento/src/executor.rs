use std::collections::HashSet;

use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    AcknowledgeError, Aggregator, Event, ReadError, RoutingKey, SubscribeError, WriteError,
};

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
        cursor: Value,
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
        cursor: Value,
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
impl From<crate::Sqlite> for Evento {
    fn from(value: crate::Sqlite) -> Self {
        Self::Sqlite(value)
    }
}

#[cfg(feature = "mysql")]
impl From<crate::MySql> for Evento {
    fn from(value: crate::MySql) -> Self {
        Self::MySql(value)
    }
}

#[cfg(feature = "postgres")]
impl From<crate::Postgres> for Evento {
    fn from(value: crate::Postgres) -> Self {
        Self::Postgres(value)
    }
}

#[derive(Clone)]
pub struct External<S: Executor, I: Executor> {
    source: S,
    inner: I,
}

impl<S: Executor, I: Executor> External<S, I> {
    pub fn new(source: S, inner: I) -> Self {
        Self { source, inner }
    }
}

#[async_trait::async_trait]
impl<S: Executor, I: Executor> Executor for External<S, I> {
    async fn write(&self, _events: Vec<Event>) -> Result<(), WriteError> {
        todo!()
    }

    async fn get_event<A: Aggregator>(&self, cursor: Value) -> Result<Event, ReadError> {
        self.source.get_event::<A>(cursor).await
    }

    async fn read_by_aggregator<A: Aggregator>(
        &self,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.source.read_by_aggregator::<A>(id, args).await
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.source.read(aggregator_types, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        self.inner.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        self.inner.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
        self.inner.upsert_subscriber(key, worker_id).await
    }

    async fn get_snapshot<A: Aggregator>(
        &self,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        self.source.get_snapshot::<A>(id).await
    }

    async fn save_snapshot<A: Aggregator>(
        &self,
        _id: String,
        _data: Vec<u8>,
        _cursor: Value,
    ) -> Result<(), WriteError> {
        todo!()
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: i64,
    ) -> Result<(), AcknowledgeError> {
        self.inner.acknowledge(key, cursor, lag).await
    }
}
