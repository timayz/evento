use async_trait::async_trait;
use evento_store::MemoryStore;

use crate::{engine::Engine, Consumer};

pub type MemoryConsumer = Consumer<Memory, evento_store::Memory>;

impl MemoryConsumer {
    pub fn new() -> Self {
        Self::create(Memory, MemoryStore::new())
    }
}

#[derive(Debug, Clone)]
pub struct Memory;

#[async_trait]
impl Engine for Memory {}
