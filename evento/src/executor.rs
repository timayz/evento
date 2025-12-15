use std::{collections::HashSet, sync::Arc};

use ulid::Ulid;

use crate::{
    cursor::{Args, ReadResult, Value},
    AcknowledgeError, Event, ReadError, RoutingKey, SubscribeError, WriteError,
};

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError>;

    async fn get_event(&self, cursor: Value) -> Result<Event, ReadError>;

    async fn read_by_aggregator(
        &self,
        aggregator_type: String,
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

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError>;

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError>;

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: u64,
    ) -> Result<(), AcknowledgeError>;
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

    async fn get_event(&self, cursor: Value) -> Result<Event, ReadError> {
        self.0.get_event(cursor).await
    }

    async fn read_by_aggregator(
        &self,
        aggregator_type: String,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.0.read_by_aggregator(aggregator_type, id, args).await
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.0.read(aggregator_types, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        self.0.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        self.0.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
        self.0.upsert_subscriber(key, worker_id).await
    }

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        self.0
            .get_snapshot(aggregator_type, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError> {
        self.0
            .save_snapshot(aggregator_type, aggregator_revision, id, data, cursor)
            .await
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: u64,
    ) -> Result<(), AcknowledgeError> {
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

    async fn get_event(&self, cursor: Value) -> Result<Event, ReadError> {
        let futures = self
            .executors
            .iter()
            .map(|e| e.get_event(cursor.to_owned()));

        let results = futures_util::future::join_all(futures).await;

        if let Some(Ok(result)) = results.iter().find(|res| res.is_ok()) {
            return Ok(result.clone());
        }

        if let Err(err) = results
            .first()
            .expect("EventoGroup must have at least one executor")
        {
            return Err(ReadError::Unknown(anyhow::anyhow!("{err:?}")));
        }

        unreachable!()
    }

    async fn read_by_aggregator(
        &self,
        aggregator_type: String,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        use crate::cursor;
        let futures = self
            .executors
            .iter()
            .map(|e| e.read_by_aggregator(aggregator_type.to_owned(), id.to_owned(), args.clone()));

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

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        self.first()
            .get_snapshot(aggregator_type, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError> {
        self.first()
            .save_snapshot(aggregator_type, aggregator_revision, id, data, cursor)
            .await
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: u64,
    ) -> Result<(), AcknowledgeError> {
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

    async fn get_event(&self, cursor: Value) -> Result<Event, ReadError> {
        self.r.get_event(cursor).await
    }

    async fn read_by_aggregator(
        &self,
        aggregator_type: String,
        id: String,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.r.read_by_aggregator(aggregator_type, id, args).await
    }

    async fn read(
        &self,
        aggregator_types: HashSet<String>,
        routing_key: RoutingKey,
        args: Args,
    ) -> Result<ReadResult<Event>, ReadError> {
        self.r.read(aggregator_types, routing_key, args).await
    }

    async fn get_subscriber_cursor(&self, key: String) -> Result<Option<Value>, SubscribeError> {
        self.r.get_subscriber_cursor(key).await
    }

    async fn is_subscriber_running(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> Result<bool, SubscribeError> {
        self.r.is_subscriber_running(key, worker_id).await
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> Result<(), SubscribeError> {
        self.w.upsert_subscriber(key, worker_id).await
    }

    async fn get_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
    ) -> Result<Option<(Vec<u8>, Value)>, ReadError> {
        self.r
            .get_snapshot(aggregator_type, aggregator_revision, id)
            .await
    }

    async fn save_snapshot(
        &self,
        aggregator_type: String,
        aggregator_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> Result<(), WriteError> {
        self.w
            .save_snapshot(aggregator_type, aggregator_revision, id, data, cursor)
            .await
    }

    async fn acknowledge(
        &self,
        key: String,
        cursor: Value,
        lag: u64,
    ) -> Result<(), AcknowledgeError> {
        self.w.acknowledge(key, cursor, lag).await
    }
}
