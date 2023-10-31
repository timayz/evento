use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use convert_case::{Case, Casing};
use evento::{
    store::{Aggregate, Event},
    ConsumerContext, Query, QueryError, QueryHandler, QueryOutput, Rule, RuleHandler,
};
use evento_query::{Cursor, CursorType, Edge, PgQuery, QueryArgs, QueryResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::{Created, Edited, Product, ProductEvent, ThumbnailChanged, VisibilityChanged};

#[derive(Debug, Default, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ProductDetails {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub description: Option<String>,
    pub category: Option<String>,
    pub thumbnail: Option<String>,
    pub stock: i32,
    pub price: Option<f32>,
    pub visible: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct ProductDetailsHandler;

#[async_trait]
impl RuleHandler for ProductDetailsHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let db = ctx.extract::<PgPool>();
        let event_name: ProductEvent = event.name.parse()?;

        match event_name {
            ProductEvent::Created => {
                let data: Created = event.to_data().unwrap();
                let id = Product::to_id(event.aggregate_id);

                sqlx::query_as::<_, (String,)>(
                    "INSERT INTO iv_product (id, slug, name, created_at, visible) VALUES ($1, $2, $3, $4, $5) RETURNING id",
                )
                .bind(&id)
                .bind(data.name.to_case(Case::Kebab))
                .bind(data.name)
                .bind(Utc::now())
                .bind(false)
                .fetch_one(&db)
                .await?;
            }
            ProductEvent::Edited => {
                let data: Edited = event.to_data().unwrap();
                let id = Product::to_id(event.aggregate_id);
                sqlx::query_as::<_, (String,)>(
                    r#"
                    UPDATE iv_product SET name = $1, description = $2, category = $3, stock = $4, price = $5, visible = $6, updated_at = $7
                    WHERE id = $8
                    RETURNING id
                    "#,
                )
                .bind(data.name)
                .bind(data.description)
                .bind(data.category)
                .bind(data.stock)
                .bind(data.price)
                .bind(data.visible)
                .bind(Utc::now())
                .bind(&id)
                .fetch_one(&db)
                .await?;
            }
            ProductEvent::VisibilityChanged => {
                let data: VisibilityChanged = event.to_data().unwrap();
                let id = Product::to_id(event.aggregate_id);
                sqlx::query_as::<_, (String,)>(
                    r#"
                    UPDATE iv_product SET visible = $1, updated_at = $2
                    WHERE id = $3
                    RETURNING id
                    "#,
                )
                .bind(data.visible)
                .bind(Utc::now())
                .bind(&id)
                .fetch_one(&db)
                .await?;
            }
            ProductEvent::ThumbnailChanged => {
                let data: ThumbnailChanged = event.to_data().unwrap();
                let id = Product::to_id(event.aggregate_id);
                sqlx::query_as::<_, (String,)>(
                    r#"
                    UPDATE iv_product SET thumbnail = $1, updated_at = $2
                    WHERE id = $3
                    RETURNING id
                    "#,
                )
                .bind(data.thumbnail)
                .bind(Utc::now())
                .bind(&id)
                .fetch_one(&db)
                .await?;
            }
            ProductEvent::Deleted => {
                let id = Product::to_id(event.aggregate_id);
                sqlx::query::<_>("DELETE FROM iv_product WHERE id = $1")
                    .bind(&id)
                    .execute(&db)
                    .await?;
            }
        };

        Ok(None)
    }
}

pub fn product_details() -> Rule {
    Rule::new("product-details").handler("product/**", ProductDetailsHandler)
}

#[derive(Deserialize)]
pub struct GetProductDetails {
    pub id: String,
}

#[async_trait]
impl QueryHandler for GetProductDetails {
    type Output = Edge<ProductDetails>;
    async fn handle(&self, query: &Query) -> QueryOutput<Self::Output> {
        let db: sqlx::Pool<sqlx::Postgres> = query.extract::<PgPool>();
        let result = PgQuery::<ProductDetails>::new("SELECT * FROM iv_product WHERE id = $1")
            .bind(&self.id)
            .forward(1, None)
            .fetch_all(&db)
            .await?;

        match result.edges.first().cloned() {
            Some(edge) => Ok(edge),
            _ => Err(QueryError::NotFound("".to_owned())),
        }
    }
}

#[derive(Deserialize)]
pub struct ListProductDetails {
    pub first: Option<u16>,
    pub after: Option<CursorType>,
    pub last: Option<u16>,
    pub before: Option<CursorType>,
}

#[async_trait]
impl QueryHandler for ListProductDetails {
    type Output = QueryResult<ProductDetails>;
    async fn handle(&self, query: &Query) -> QueryOutput<Self::Output> {
        let db: sqlx::Pool<sqlx::Postgres> = query.extract::<PgPool>();
        let result = PgQuery::<ProductDetails>::new("SELECT * FROM iv_product")
            .build(QueryArgs {
                first: self.first.to_owned(),
                after: self.after.to_owned(),
                last: self.last.to_owned(),
                before: self.before.to_owned(),
            })
            .fetch_all(&db)
            .await?;

        Ok(result)
    }
}

impl Cursor for ProductDetails {
    fn keys() -> Vec<&'static str> {
        vec!["created_at", "id"]
    }

    fn bind<'q, O>(
        self,
        query: sqlx::query::QueryAs<sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<sqlx::Postgres, O, sqlx::postgres::PgArguments>
    where
        O: for<'r> sqlx::FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
        O: 'q + std::marker::Send,
        O: 'q + Unpin,
        O: 'q + Cursor,
    {
        query.bind(self.created_at).bind(self.id)
    }

    fn serialize(&self) -> Vec<String> {
        vec![Self::serialize_utc(self.created_at), self.id.to_string()]
    }

    fn deserialize(values: Vec<&str>) -> Result<Self, evento_query::QueryError> {
        let mut values = values.iter();
        let created_at = Self::deserialize_as_utc("created_at", values.next())?;
        let id = Self::deserialize_as("id", values.next())?;

        Ok(ProductDetails {
            id,
            created_at,
            ..Default::default()
        })
    }
}
