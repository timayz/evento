use async_trait::async_trait;
use dyn_clone::DynClone;
use evento_query::{QueryResult, CursorType};
use serde_json::Value;

use crate::{error::Result, event::Event, WriteEvent};

#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "pg")]
mod pg;

#[cfg(feature = "memory")]
pub use memory::*;
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
