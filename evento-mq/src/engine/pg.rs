use async_trait::async_trait;
use evento_store::PgStore;
use sqlx::PgPool;

use crate::{engine::Engine, Consumer};

pub type PgConsumer = Consumer<Pg, evento_store::Pg>;

impl PgConsumer {
    pub fn new(pool: &PgPool) -> Self {
        Self::create(
            Pg {
                pool: pool.clone(),
                prefix: None,
            },
            PgStore::new(pool),
        )
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();

        self.store = self.store.prefix(&prefix);
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
impl Engine for Pg {}
