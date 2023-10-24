use async_trait::async_trait;
use evento::{
    store::{Aggregate, Event, WriteEvent},
    CommandContext, CommandHandler,
};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use validator::Validate;

use super::{
    Created, Deleted, DescriptionUpdated, ProductEvent, QuantityUpdated, ReviewAdded,
    VisibilityUpdated,
};

#[derive(Debug, Deserialize, Validate)]
pub struct CreateProduct {
    #[validate(length(min = 3, max = 25))]
    pub name: String,
}

#[async_trait]
impl CommandHandler for CreateProduct {
    async fn handle(&self, ctx: &CommandContext) -> anyhow::Result<Vec<Event>> {
        let id = nanoid!(10);
        let events = ctx
            .publish::<Product, _>(
                &id,
                ProductEvent::Created.data(Created {
                    name: self.name.to_owned(),
                })?,
                0,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Product {
    pub name: String,
    pub description: String,
    pub visible: bool,
    pub quantity: u16,
    pub deleted: bool,
    pub reviews: Vec<(u8, String)>,
}

impl Aggregate for Product {
    fn apply(&mut self, event: &Event) {
        let product_event: ProductEvent = event.name.parse().unwrap();

        match product_event {
            ProductEvent::Created => {
                let data: Created = event.to_data().unwrap();
                self.name = data.name;
            }
            ProductEvent::QuantityUpdated => {
                let data: QuantityUpdated = event.to_data().unwrap();
                self.quantity = data.quantity;
            }
            ProductEvent::VisibilityUpdated => {
                let data: VisibilityUpdated = event.to_data().unwrap();
                self.visible = data.visible;
            }
            ProductEvent::DescriptionUpdated => {
                let data: DescriptionUpdated = event.to_data().unwrap();
                self.description = data.description;
            }
            ProductEvent::ReviewAdded => {
                let data: ReviewAdded = event.to_data().unwrap();
                self.reviews.push((data.note, data.message));
            }
            ProductEvent::Deleted => {
                let data: Deleted = event.to_data().unwrap();
                self.deleted = data.deleted;
            }
        }
    }

    fn aggregate_type<'a>() -> &'a str {
        "product"
    }
}

impl ProductEvent {
    pub fn data<D: Serialize>(&self, value: D) -> evento::store::Result<WriteEvent> {
        WriteEvent::new(self.to_string()).data(value)
    }
}
