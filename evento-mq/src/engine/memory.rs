use async_trait::async_trait;
use evento_store::{CursorType, MemoryStore};
use uuid::Uuid;

use crate::{
    consumer::{Consumer, Queue},
    engine::Engine,
    error::Result,
};

pub type MemoryConsumer = Consumer<Memory, evento_store::Memory>;

impl MemoryConsumer {
    pub fn new() -> Self {
        Self::create(Memory, MemoryStore::new(), MemoryStore::new())
    }
}

#[derive(Debug, Clone)]
pub struct Memory;

#[async_trait]
impl Engine for Memory {
    async fn upsert(&self, key: String, consumer: Uuid) -> Result<()> {
        todo!()
    }
    async fn get(&self, key: String) -> Result<Queue> {
        todo!()
    }
    async fn set_cursor(&self, key: String, cursor: CursorType) -> Result<Queue> {
        todo!()
    }
}
