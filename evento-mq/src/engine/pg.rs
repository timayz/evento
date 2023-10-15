use async_trait::async_trait;
use evento_store::{CursorType, PgStore};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    consumer::{Consumer, Queue},
    engine::Engine,
    error::Result,
};

pub type PgConsumer = Consumer<Pg, evento_store::Pg>;

impl PgConsumer {
    pub fn new(pool: &PgPool) -> Self {
        Self::create(
            Pg::new(pool),
            PgStore::new(pool),
            PgStore::new(pool).prefix("deadletter"),
        )
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();

        self.store = self.store.prefix(&prefix);
        self.deadletter_store = self.deadletter_store.extend_prefix(&prefix);
        self.engine.prefix = Some(prefix);

        self
    }
}

#[derive(Debug, Clone)]
pub struct Pg {
    pool: PgPool,
    prefix: Option<String>,
}

impl Pg {
    pub fn new(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            prefix: None,
        }
    }

    pub fn table(&self, name: impl Into<String>) -> String {
        format!(
            "{}_{}",
            self.prefix.as_ref().unwrap_or(&"ev".to_owned()),
            name.into()
        )
    }

    pub fn table_subscriptions(&self) -> String {
        self.table("subscriptions")
    }

    pub fn table_deadletters(&self) -> String {
        self.table("deadletters")
    }
}

#[async_trait]
impl Engine for Pg {
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
