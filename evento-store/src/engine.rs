use async_trait::async_trait;
use dyn_clone::DynClone;
use evento_query::{CursorType, QueryResult};
use serde_json::Value;

use crate::{
    error::Result,
    store::{Event, WriteEvent},
};

#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::*;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

#[cfg(feature = "pg")]
mod pg;
#[cfg(feature = "pg")]
pub use pg::*;

#[async_trait]
pub trait Engine: DynClone + Send + Sync {
    async fn write(
        &self,
        aggregate_id: &'_ str,
        events: Vec<WriteEvent>,
        original_version: u16,
    ) -> Result<Vec<Event>>;

    async fn insert(&self, events: Vec<Event>) -> Result<()>;

    async fn upsert(&self, event: Event) -> Result<()>;

    async fn read(
        &self,
        first: u16,
        after: Option<CursorType>,
        filters: Option<Vec<Value>>,
        aggregate_id: Option<&'_ str>,
    ) -> Result<QueryResult<Event>>;

    async fn last(&self) -> Result<Option<Event>>;
}

dyn_clone::clone_trait_object!(Engine);
