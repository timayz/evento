use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use evento_store::{CursorType, MemoryStore};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    consumer::{Consumer, Queue},
    engine::Engine,
    error::Result,
};

pub type MemoryConsumer = Consumer<Memory>;

impl MemoryConsumer {
    pub fn new() -> Self {
        Self::create(
            Memory::default(),
            MemoryStore::create(),
            MemoryStore::create(),
        )
    }
}

impl Default for MemoryConsumer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Memory(Arc<RwLock<HashMap<String, Queue>>>);

#[async_trait]
impl Engine for Memory {
    async fn upsert(&self, key: String, consumer_id: Uuid) -> Result<()> {
        let mut queues = self.0.write().await;

        let queue = queues.entry(key.to_owned()).or_insert(Queue {
            id: Uuid::new_v4(),
            consumer_id,
            rule: key,
            enabled: true,
            cursor: None,
            updated_at: None,
            created_at: Utc::now(),
        });

        queue.consumer_id = consumer_id;

        Ok(())
    }

    async fn get(&self, key: String) -> Result<Queue> {
        match self.0.read().await.get(key.as_str()) {
            Some(queue) => Ok(queue.clone()),
            _ => Err(anyhow!("queue {key} not found").into()),
        }
    }

    async fn set_cursor(&self, key: String, cursor: CursorType) -> Result<()> {
        let mut users = self.0.write().await;
        let Some(queue) = users.get_mut(key.as_str()) else {
            return Err(anyhow!("queue {key} not found").into());
        };

        queue.cursor = Some(cursor.0.to_owned());

        Ok(())
    }
}
