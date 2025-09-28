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

// Note: Due to generic methods in the Executor trait, we cannot create
// trait objects (DynExecutor). For polymorphism over different database 
// backends, consider using an enum or generic parameters instead.


// ExecutorGroup removed due to trait object limitations with generic methods
// Consider using a generic approach or concrete enum types instead

