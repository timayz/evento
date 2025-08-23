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

#[cfg(feature = "group")]
#[derive(Clone, Default)]
pub struct EventoGroup {
    executors: Vec<Evento>,
}

#[cfg(feature = "group")]
impl EventoGroup {
    pub fn executor(mut self, executor: &Evento) -> Self {
        self.executors.push(executor.clone());

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

    async fn get_event<A: Aggregator>(&self, cursor: Value) -> Result<Event, ReadError> {
        let futures = self
            .executors
            .iter()
            .map(|e| e.get_event::<A>(cursor.to_owned()));

        let results = futures_util::future::join_all(futures).await;
        for res in results.iter() {
            if let Ok(result) = res {
                return Ok(result.clone());
            }
        }

        if let Err(err) = results
            .first()
            .expect("EventoGroup must have at least one executor")
        {
            return Err(ReadError::Unknown(anyhow::anyhow!("{err:?}")));
        }

        unreachable!()
    }

    async fn read_by_aggregator<A: Aggregator>(
        &self,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        use crate::cursor;
        let futures = self
            .executors
            .iter()
            .map(|e| e.read_by_aggregator::<A>(id.to_owned(), args.clone()));

        let results = futures_util::future::join_all(futures).await;
        let mut events = vec![];
        for res in results {
            for edge in res?.edges {
                events.push(edge.node);
            }
        }

        Ok(cursor::Reader::new(events)
            .args(args)
            .execute()
            .map_err(|err| ReadError::Unknown(err.into()))?)
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        use crate::cursor;
        let futures = self.executors.iter().map(|e| {
            e.read(
                aggregator_types.to_owned(),
                routing_key.to_owned(),
                args.clone(),
            )
        });

        let results = futures_util::future::join_all(futures).await;
        let mut events = vec![];
        for res in results {
            for edge in res?.edges {
                events.push(edge.node);
            }
        }

        Ok(cursor::Reader::new(events)
            .args(args)
            .execute()
            .map_err(|err| ReadError::Unknown(err.into()))?)
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        self.first().get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        self.first().is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
        self.first().upsert_subscriber(key, worker_id).await
    }

    async fn get_snapshot<A: Aggregator>(
        &self,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        self.first().get_snapshot::<A>(id).await
    }

    async fn save_snapshot<A: Aggregator>(
        &self,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError> {
        self.first().save_snapshot::<A>(id, data, cursor).await
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: i64,
    ) -> Result<(), AcknowledgeError> {
        self.first().acknowledge(key, cursor, lag).await
    }
}
