#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::*;

use dyn_clone::DynClone;
use evento_store::CursorType;
use uuid::Uuid;

use crate::{consumer::Queue, error::Result};

#[cfg(feature = "pg")]
mod pg;
#[cfg(feature = "pg")]
pub use pg::*;

use async_trait::async_trait;

#[async_trait]
pub trait Engine: DynClone + Send + Sync {
    async fn upsert(&self, key: String, consumer_id: Uuid) -> Result<()>;
    async fn get(&self, key: String) -> Result<Queue>;
    async fn set_cursor(&self, key: String, cursor: CursorType) -> Result<()>;
}

dyn_clone::clone_trait_object!(Engine);
