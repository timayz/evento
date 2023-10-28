use async_trait::async_trait;
use evento::{
    store::{Aggregate, Event, WriteEvent},
    Command, CommandError, CommandHandler, CommandOutput,
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
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        // Err(CommandError::NotFound("()".to_owned()))
        let id = nanoid!(10);

        let events = cmd
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

#[derive(Deserialize, Validate)]
pub struct DeleteProduct {
    #[validate(length(equal = 10))]
    pub id: String,
}

#[async_trait]
impl CommandHandler for DeleteProduct {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct UpdateProductQuantity {
    #[validate(length(equal = 10))]
    pub id: String,
    #[validate(range(min = 0))]
    pub quantity: u16,
}

#[async_trait]
impl CommandHandler for UpdateProductQuantity {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (product, original_version) = load_product(cmd, &self.id).await?;

        if product.quantity == self.quantity {
            return Err(CommandError::Validation(
                [(
                    "quantity".to_owned(),
                    vec!["Please enter a value other than the current one".to_owned()],
                )]
                .into_iter()
                .collect(),
            ));
        }

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct UpdateProductVisibility {
    #[validate(length(equal = 10))]
    pub id: String,
    pub visible: bool,
}

#[async_trait]
impl CommandHandler for UpdateProductVisibility {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct UpdateProductDescription {
    #[validate(length(equal = 10))]
    pub id: String,
    #[validate(length(min = 3, max = 255))]
    pub description: String,
}

#[async_trait]
impl CommandHandler for UpdateProductDescription {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct AddReviewToProduct {
    #[validate(length(equal = 10))]
    pub id: String,
    #[validate(range(min = 0, max = 10))]
    pub note: u8,
    #[validate(length(min = 3, max = 255))]
    pub message: String,
}

#[async_trait]
impl CommandHandler for AddReviewToProduct {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

async fn load_product(ctx: &Command, id: &str) -> Result<(Product, u16), CommandError> {
    let Some((product, e)) = ctx.load::<Product, _>(id).await? else {
        return Err(CommandError::NotFound(format!("product {id} not found")));
    };

    if product.deleted {
        return Err(CommandError::NotFound(format!("product {id} not found")));
    }

    Ok((product, e))
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
