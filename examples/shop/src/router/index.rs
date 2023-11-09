use askama::Template;
use async_trait::async_trait;
use evento::{
    store::{Aggregate, Event},
    ConsumerContext, RuleHandler,
};
use evento_query::{QueryResult, Edge};
use inventory::product::{Product, ProductEvent};

use crate::{
    product::{ListProductDetails, ProductDetails, GetProductDetails},
    Publisher,
};

use super::Query;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub data: QueryResult<ProductDetails>,
    pub cursor: Option<String>,
}

pub async fn index(data: Query<ListProductDetails>) -> IndexTemplate {
    let cursor = match (
        data.output.page_info.end_cursor.to_owned(),
        data.output.page_info.has_next_page,
    ) {
        (Some(cursor), true) => Some(cursor.0),
        _ => None,
    };

    IndexTemplate {
        data: data.output,
        cursor,
    }
}

#[derive(Template)]
#[template(path = "_product.html")]
pub struct ProductTemplate {
    pub edge: Edge<ProductDetails>,
}

pub async fn product(data: Query<GetProductDetails>) -> ProductTemplate {
    ProductTemplate { edge: data.output }
}

#[derive(Clone)]
pub struct IndexProductHandler;

#[async_trait]
impl RuleHandler for IndexProductHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> anyhow::Result<()> {
        let id = Product::to_id(&event.aggregate_id);
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
