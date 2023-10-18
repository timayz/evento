use anyhow::{anyhow, Result};
use evento::{store::WriteEvent, Producer};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::Command;

use super::{
    aggregate::Product,
    event::{
        Created, Deleted, DescriptionUpdated, ProductEvent, QuantityUpdated, ReviewAdded,
        VisibilityUpdated,
    },
};

pub async fn load_product(producer: &Producer, id: &str) -> Result<(Product, u16)> {
    let (product, e) = match producer.load::<Product, _>(id).await? {
        Some(product) => product,
        _ => return Err(anyhow!(format!("product {}", id.to_owned()))),
    };

    if product.deleted {
        // not found
        return Err(anyhow!(format!("product {} not found", id.to_owned())));
    }

    Ok((product, e))
}

#[derive(Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct CreateCommand {
    #[validate(length(min = 3, max = 25))]
    pub name: String,
}

#[derive(Deserialize)]
pub struct DeleteCommand {
    pub id: String,
}

#[derive(Deserialize, Validate)]
pub struct UpdateQuantityCommand {
    pub id: String,
    #[validate(range(min = 0))]
    pub quantity: u16,
}

#[derive(Deserialize)]
pub struct UpdateVisibilityCommand {
    pub id: String,
    pub visible: bool,
}

#[derive(Deserialize, Validate)]
pub struct UpdateDescriptionCommand {
    pub id: String,
    #[validate(length(min = 3, max = 255))]
    pub description: String,
}

#[derive(Deserialize, Validate)]
pub struct AddReviewCommand {
    pub id: String,
    #[validate(range(min = 0, max = 10))]
    pub note: u8,
    #[validate(length(min = 3, max = 255))]
    pub message: String,
}

impl Command {
    pub async fn add_review_to_product(&self, input: AddReviewCommand) -> Result<String> {
        input.validate()?;

        let (_, original_version) = load_product(&self.producer, &input.id).await?;

        self.producer
            .publish::<Product, _>(
                &input.id,
                WriteEvent::new(ProductEvent::ReviewAdded).data(ReviewAdded {
                    note: input.note,
                    message: input.message,
                })?,
                original_version,
            )
            .await?;
        Ok(input.id)
    }

    pub async fn update_description_of_product(
        &self,
        input: UpdateDescriptionCommand,
    ) -> Result<String> {
        input.validate()?;

        let (product, original_version) = load_product(&self.producer, &input.id).await?;

        if product.description == input.description {
            return Err(anyhow!(format!(
                "product.description already `{}`",
                input.description
            )));
        }

        self.producer
            .publish::<Product, _>(
                &input.id,
                WriteEvent::new(ProductEvent::DescriptionUpdated).data(DescriptionUpdated {
                    description: input.description,
                })?,
                original_version,
            )
            .await?;
        Ok(input.id)
    }

    pub async fn update_visivility_of_product(
        &self,
        input: UpdateVisibilityCommand,
    ) -> Result<String> {
        let (product, original_version) = load_product(&self.producer, &input.id).await?;

        if product.visible == input.visible {
            return Err(anyhow!(format!(
                "product.visible already `{}`",
                input.visible
            )));
        }

        self.producer
            .publish::<Product, _>(
                &input.id,
                WriteEvent::new(ProductEvent::VisibilityUpdated).data(VisibilityUpdated {
                    visible: input.visible,
                })?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn update_quantity_of_product(&self, input: UpdateQuantityCommand) -> Result<String> {
        input.validate()?;

        let (product, original_version) = load_product(&self.producer, &input.id).await?;

        if product.quantity == input.quantity {
            return Err(anyhow!(format!(
                "product.quantity already `{}`",
                input.quantity
            )));
        }

        self.producer
            .publish::<Product, _>(
                &input.id,
                WriteEvent::new(ProductEvent::QuantityUpdated).data(QuantityUpdated {
                    quantity: input.quantity,
                })?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn delete_product(&self, input: DeleteCommand) -> Result<String> {
        let (_, original_version) = load_product(&self.producer, &input.id).await?;

        self.producer
            .publish::<Product, _>(
                &input.id,
                WriteEvent::new(ProductEvent::Deleted).data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn create_product(&self, input: CreateCommand) -> Result<String> {
        input.validate()?;

        let id = nanoid!();

        self.producer
            .publish::<Product, _>(
                &id,
                WriteEvent::new(ProductEvent::Created).data(Created { name: input.name })?,
                0,
            )
            .await?;
        Ok(id)
    }
}
