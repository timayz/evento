use anyhow::{anyhow, Result};
use evento::{store::WriteEvent, PgProducer};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::Command;

use super::{
    aggregate::{Order, Product, Status},
    event::{
        Canceled, Deleted, OrderEvent, Paid, Placed, ProductAdded, ProductQuantityUpdated,
        ProductRemoved, ShippingInfoUpdated,
    },
};

pub async fn load_order(producer: &PgProducer, id: &str) -> Result<(Order, u16)> {
    let (order, e) = match producer.load::<Order, _>(id).await? {
        Some(order) => order,
        _ => return Err(anyhow!(format!("order {}", id.to_owned()))),
    };

    if order.status == Status::Canceled || order.status == Status::Deleted {
        // Should be not found
        return Err(anyhow!(format!("order {} not found", id.to_owned())));
    }

    Ok((order, e))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceCommand {
    pub products: Vec<Product>,
}

#[derive(Deserialize)]
pub struct AddProductCommand {
    pub id: String,
    pub product: Product,
}

#[derive(Deserialize)]
pub struct RemoveProductCommand {
    pub id: String,
    pub product_id: String,
}

#[derive(Deserialize)]
pub struct UpdateProductQuantityCommand {
    pub id: String,
    pub product: Product,
}
#[derive(Deserialize)]
pub struct UpdateShippingInfoCommand {
    pub id: String,
    pub address: String,
}

#[derive(Deserialize)]
pub struct PayCommand {
    pub id: String,
}

#[derive(Deserialize)]
pub struct DeleteCommand {
    pub id: String,
}

#[derive(Deserialize)]
pub struct CancelCommand {
    pub id: String,
}

impl Command {
    pub async fn place_order(&self, input: PlaceCommand) -> Result<String> {
        let mut events = vec![WriteEvent::new(OrderEvent::Placed).data(Placed::default())?];

        for product in input.products {
            product.validate()?;
            events.push(
                WriteEvent::new(OrderEvent::ProductAdded).data::<ProductAdded>(product.into())?,
            )
        }

        let id = nanoid!();

        self.producer
            .publish_all::<Order, _>(&id, events, 0)
            .await?;

        Ok(id)
    }

    pub async fn add_product_to_order(&self, input: AddProductCommand) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            // Should be bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        input.product.validate()?;

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::ProductAdded)
                    .data::<ProductAdded>(input.product.into())?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn remove_product_from_order(&self, input: RemoveProductCommand) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            // bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        if !order.products.contains_key(&input.product_id) {
            // not found
            return Err(anyhow!(format!(
                "product {} not found",
                input.product_id.to_owned()
            )));
        }

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::ProductRemoved).data(ProductRemoved {
                    id: input.product_id,
                })?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn update_product_quantity_of_order(
        &self,
        input: UpdateProductQuantityCommand,
    ) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            // bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        if !order.products.contains_key(&input.product.id) {
            // not found
            return Err(anyhow!(format!(
                "product {} not found",
                input.product.id.to_owned()
            )));
        }

        input.product.validate()?;

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::ProductQuantityUpdated)
                    .data::<ProductQuantityUpdated>(input.product.into())?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn update_shipping_info_of_order(
        &self,
        input: UpdateShippingInfoCommand,
    ) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            //bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        if order.shipping_address == input.address {
            // bad request
            return Err(anyhow!(format!("address `{}` not changed", input.address)));
        }

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::ShippingInfoUpdated).data(ShippingInfoUpdated {
                    shipping_address: input.address,
                })?,
                original_version,
            )
            .await?;
        Ok(input.id)
    }

    pub async fn pay_order(&self, input: PayCommand) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            //bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        if order.shipping_address.is_empty() {
            //bad request
            return Err(anyhow!("order shipping address must not be empty"));
        }

        if order.products.is_empty() {
            //bad request
            return Err(anyhow!("order products must not be empty"));
        }

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::Paid).data(Paid::default())?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }

    pub async fn delete_order(&self, input: DeleteCommand) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Draft {
            // bad request
            return Err(anyhow!("order status must be `draft`"));
        }

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::Deleted).data(Deleted::default())?,
                original_version,
            )
            .await?;
        Ok(input.id)
    }

    pub async fn cancel_order(&self, input: CancelCommand) -> Result<String> {
        let (order, original_version) = load_order(&self.producer, &input.id).await?;

        if order.status != Status::Pending {
            // bad request
            return Err(anyhow!("order status must be `pending`"));
        }

        self.producer
            .publish::<Order, _>(
                &input.id,
                WriteEvent::new(OrderEvent::Canceled).data(Canceled::default())?,
                original_version,
            )
            .await?;

        Ok(input.id)
    }
}
