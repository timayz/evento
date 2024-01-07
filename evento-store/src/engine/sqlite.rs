use std::collections::HashSet;

use async_trait::async_trait;
use evento_query::{Cursor, CursorType, PgQuery, QueryResult};
use serde_json::Value;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::{
    engine::Engine,
    error::{Result, StoreError},
    store::{Event, Store, WriteEvent},
};

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    prefix: Option<String>,
}

impl SqliteStore {
    pub fn create(pool: &SqlitePool) -> Store {
        Store::new(Self {
            pool: pool.clone(),
            prefix: None,
        })
    }

    pub fn with_prefix(pool: &SqlitePool, prefix: impl Into<String>) -> Store {
        Store::new(Self {
            pool: pool.clone(),
            prefix: Some(prefix.into()),
        })
    }

    pub fn table(&self, name: impl Into<String>) -> String {
        format!(
            "{}_{}",
            self.prefix.as_ref().unwrap_or(&"ev".to_owned()),
            name.into()
        )
    }

    pub fn table_events(&self) -> String {
        self.table("event")
    }
}

#[async_trait]
impl Engine for SqliteStore {
    async fn write(
        &self,
        aggregate_id: &'_ str,
        write_events: Vec<WriteEvent>,
        original_version: u16,
    ) -> Result<Vec<Event>> {
        let table_events = self.table_events();
        let mut tx = self.pool.begin().await?;

        let mut version = original_version;
        let mut events = Vec::new();

        for write_events in write_events.chunks(100).collect::<Vec<&[WriteEvent]>>() {
            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                    format!("INSERT INTO {table_events} (id, name, aggregate_id, version, data, metadata, created_at) ")
                );

            query_builder.push_values(write_events, |mut b, event| {
                version += 1;

                let event = event.to_event(aggregate_id, version);

                b.push_bind(event.id.to_owned())
                    .push_bind(event.name.to_owned())
                    .push_bind(event.aggregate_id.to_owned())
                    .push_bind(event.version)
                    .push_bind(event.data.clone())
                    .push_bind(event.metadata.clone())
                    .push_bind(event.created_at);

                events.push(event);
            });

            query_builder
                .push("ON CONFLICT (aggregate_id, version) DO NOTHING")
                .build()
                .execute(&mut *tx)
                .await?;
        }

        let next_event_id = sqlx::query_as::<_, Event>(
            format!(
                r#"
                SELECT * FROM {table_events}
                WHERE aggregate_id = $1 AND version = $2
                ORDER BY created_at ASC
                LIMIT 1
                "#
            )
            .as_str(),
        )
        .bind(aggregate_id)
        .bind(i32::from(original_version + 1))
        .fetch_optional(&mut *tx)
        .await?;

        let wrong_version = match (next_event_id, events.first()) {
            (Some(next), Some(current)) => next.id != current.id,
            _ => false,
        };

        if wrong_version {
            tx.rollback().await?;

            return Err(StoreError::UnexpectedOriginalVersion);
        }

        tx.commit().await?;

        Ok(events)
    }

    async fn insert(&self, events: Vec<Event>) -> Result<()> {
        let table_events = self.table_events();

        for events in events.chunks(100).collect::<Vec<&[Event]>>() {
            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                    format!("INSERT INTO {table_events} (id, name, aggregate_id, version, data, metadata, created_at) ")
                );

            query_builder.push_values(events, |mut b, event| {
                b.push_bind(event.id.to_owned())
                    .push_bind(event.name.to_owned())
                    .push_bind(event.aggregate_id.to_owned())
                    .push_bind(event.version)
                    .push_bind(event.data.clone())
                    .push_bind(event.metadata.clone())
                    .push_bind(event.created_at);
            });

            query_builder.build().execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn upsert(&self, event: Event) -> Result<()> {
        let table_events = self.table_events();

        sqlx::query_as::<_, (Uuid,)>(
            format!(
                r#"
            INSERT INTO {table_events} (id, name, aggregate_id, version, data, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (aggregate_id, version)
            DO
                UPDATE SET data = $5, metadata = $6
            RETURNING id
            "#
            )
            .as_str(),
        )
        .bind(event.id)
        .bind(event.name)
        .bind(&event.aggregate_id)
        .bind(event.version)
        .bind(event.data)
        .bind(event.metadata)
        .bind(event.created_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(())
    }

    async fn read(
        &self,
        first: u16,
        after: Option<CursorType>,
        filters: Option<Vec<Value>>,
        aggregate_id: Option<&'_ str>,
    ) -> Result<QueryResult<Event>> {
        let mut json_filters = HashSet::new();
        let table_events = self.table_events();

        if let Some(filters) = filters {
            for filter in filters {
                let json_filter = serde_json::to_string(&filter)?;
                println!("{json_filter}");
                json_filters.insert(format!("metadata @> '{json_filter}'::jsonb"));
            }
        }

        let filters = if json_filters.is_empty() {
            None
        } else {
            Some(
                json_filters
                    .into_iter()
                    .collect::<Vec<String>>()
                    .join(" OR "),
            )
        };

        // let query = match (aggregate_id, filters) {
        //     (Some(aggregate_id), Some(filters)) => PgQuery::<Event>::new(format!(
        //         "SELECT * FROM {table_events} WHERE aggregate_id = $1 AND ({filters})"
        //     ))
        //     .bind(aggregate_id),
        //     (None, Some(filters)) => {
        //         PgQuery::<Event>::new(format!("SELECT * FROM {table_events} WHERE ({filters})"))
        //     }
        //     (Some(aggregate_id), None) => PgQuery::<Event>::new(format!(
        //         "SELECT * FROM {table_events} WHERE aggregate_id = $1"
        //     ))
        //     .bind(aggregate_id),
        //     (None, None) => PgQuery::<Event>::new(format!("SELECT * FROM {table_events}")),
        // };

        // let events = query.forward(first, after).fetch_all(&self.pool).await?;

        // Ok(events)

        todo!()
    }

    async fn last(&self) -> Result<Option<Event>> {
        let table_events = self.table_events();
        let event = sqlx::query_as::<_, Event>(
            format!(
                r#"
                SELECT * from {table_events}
                ORDER BY created_at DESC
                LIMIT 1
            "#
            )
            .as_str(),
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(event)
    }
}

// impl Cursor for Event {
//     fn keys() -> Vec<&'static str> {
//         vec!["created_at", "version", "id"]
//     }

//     fn bind<'q, O>(
//         self,
//         query: sqlx::query::QueryAs<Postgres, O, sqlx::postgres::PgArguments>,
//     ) -> sqlx::query::QueryAs<Postgres, O, sqlx::postgres::PgArguments>
//     where
//         O: for<'r> sqlx::FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
//         O: 'q + std::marker::Send,
//         O: 'q + Unpin,
//         O: 'q + Cursor,
//     {
//         query.bind(self.created_at).bind(self.version).bind(self.id)
//     }

//     fn serialize(&self) -> Vec<String> {
//         vec![
//             Self::serialize_utc(self.created_at),
//             self.version.to_string(),
//             self.id.to_string(),
//         ]
//     }

//     fn deserialize(values: Vec<&str>) -> std::result::Result<Self, evento_query::QueryError> {
//         let mut values = values.iter();
//         let created_at = Self::deserialize_as_utc("created_at", values.next())?;
//         let version = Self::deserialize_as("version", values.next())?;
//         let id = Self::deserialize_as("id", values.next())?;

//         Ok(Event {
//             id,
//             version,
//             created_at,
//             ..Default::default()
//         })
//     }
// }
