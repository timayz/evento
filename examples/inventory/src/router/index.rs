use askama::Template;
use async_trait::async_trait;
use axum::{extract::Query, Extension};
use evento::{
    store::{Aggregate, Event},
    ConsumerContext, RuleHandler,
};
use evento_query::{Edge, QueryResult};

use crate::{
    product::{GetProductDetails, ListProductDetails, Product, ProductDetails, ProductEvent},
    Publisher,
};

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub data: QueryResult<ProductDetails>,
    pub cursor: Option<String>,
}

pub async fn index(
    Query(input): Query<ListProductDetails>,
    Extension(query): Extension<evento::Query>,
) -> Result<IndexTemplate, crate::extract::Error> {
    let data = query.execute(&input).await?;

    let cursor = match (
        data.page_info.end_cursor.to_owned(),
        data.page_info.has_next_page,
    ) {
        (Some(cursor), true) => Some(cursor.0),
        _ => None,
    };

    Ok(IndexTemplate { data, cursor })
}

#[derive(Template)]
#[template(path = "_product.html")]
pub struct ProductTemplate {
    pub edge: Edge<ProductDetails>,
}

pub async fn product(
    Query(input): Query<GetProductDetails>,
    Extension(query): Extension<evento::Query>,
) -> Result<ProductTemplate, crate::extract::Error> {
    let edge = query.execute(&input).await?;

    Ok(ProductTemplate { edge })
}

#[derive(Clone)]
pub struct IndexProductHandler;

#[async_trait]
impl RuleHandler for IndexProductHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> anyhow::Result<()> {
        let id = Product::from_aggregate_id(&event.aggregate_id);
        let event_name = match event.name.parse::<ProductEvent>()? {
            ProductEvent::Created => "created",
            ProductEvent::Deleted => "deleted",
            ProductEvent::Edited
            | ProductEvent::VisibilityChanged
            | ProductEvent::ThumbnailChanged => "updated",
        };

        let publisher = ctx.extract::<Publisher>();
        publisher.send("index", event_name, &id).await;

        Ok(())
    }
}
