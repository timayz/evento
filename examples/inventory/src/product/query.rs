use anyhow::Result;
use async_trait::async_trait;
use evento::{store::Event, ConsumerContext, Rule, RuleHandler};
use sqlx::PgPool;

use super::ProductEvent;

pub struct ProductDetails {}

#[derive(Clone)]
pub struct ProductDetailsHandler;

#[async_trait]
impl RuleHandler for ProductDetailsHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let db = ctx.extract::<PgPool>();
        let event_name: ProductEvent = event.name.parse()?;

        Ok(None)
    }
}

pub fn product_details() -> Rule {
    Rule::new("product-details").handler("product/**", ProductDetailsHandler)
}
