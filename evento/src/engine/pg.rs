use async_trait::async_trait;
use chrono::Utc;
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
        self.deadletter = self.deadletter.extend_prefix(&prefix);
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

    pub fn table_queue(&self) -> String {
        self.table("queue")
    }
}

#[async_trait]
impl Engine for Pg {
    async fn upsert(&self, key: String, consumer_id: Uuid) -> Result<()> {
        let table_queue = self.table_queue();

        sqlx::query_as::<_, (Uuid,)>(
            format!(
                r#"
            INSERT INTO {table_queue} (id, consumer_id, rule, enabled, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (rule)
            DO
                UPDATE SET consumer_id = $2
            RETURNING id
            "#
            )
            .as_str(),
        )
        .bind(Uuid::new_v4())
        .bind(consumer_id)
        .bind(&key)
        .bind(true)
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;

        Ok(())
    }
    async fn get(&self, key: String) -> Result<Queue> {
        let table_queue = self.table_queue();

        let queue = sqlx::query_as::<_, Queue>(
            format!(
                r#"
            SELECT * from {table_queue} WHERE rule = $1
            "#
            )
            .as_str(),
        )
        .bind(&key)
        .fetch_one(&self.pool)
        .await?;

        Ok(queue)
    }
    async fn set_cursor(&self, key: String, cursor: CursorType) -> Result<()> {
        let table_queue = self.table_queue();
        sqlx::query_as::<_, (Uuid,)>(
            format!(
                r#"
            UPDATE {table_queue}
            SET cursor = $2, updated_at = $3
            WHERE rule = $1
            RETURNING id
            "#
            )
            .as_str(),
        )
        .bind(&key)
        .bind(cursor.0.to_owned())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;

        Ok(())
    }
}
