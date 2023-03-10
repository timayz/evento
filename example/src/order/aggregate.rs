use std::{collections::HashMap, hash::Hash};

use super::event::{
    Canceled, Deleted, OrderEvent, Paid, Placed, ProductAdded, ProductRemoved, ShippingInfoUpdated,
};
use evento::Aggregate;
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Draft,
    Pending,
    Shipping,
    Delivered,
    Canceled,
    Deleted,
}

#[derive(Default, Serialize, Deserialize, Eq, Validate)]
pub struct Product {
    pub id: String,
    #[validate(range(min = 1))]
    pub quantity: u16,
}

impl std::cmp::PartialEq for Product {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for Product {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Order {
    pub shipping_address: String,
    pub products: HashMap<String, Product>,
    pub status: Status,
}

impl Aggregate for Order {
    fn apply(&mut self, event: &evento::Event) {
        let order_event: OrderEvent = event.name.parse().unwrap();

        match order_event {
            OrderEvent::Placed => {
                let data: Placed = event.to_data().unwrap();
                self.status = data.status;
            }
            OrderEvent::ProductAdded => {
                let data: ProductAdded = event.to_data().unwrap();
                let product = self.products.entry(data.id).or_default();
                product.quantity += data.quantity;
            }
            OrderEvent::ProductRemoved => {
                let data: ProductRemoved = event.to_data().unwrap();
                self.products.remove(&data.id);
            }
            OrderEvent::ProductQuantityUpdated => {
                let data: ProductAdded = event.to_data().unwrap();
                let product = self.products.entry(data.id).or_default();
                product.quantity = data.quantity;
            }
            OrderEvent::ShippingInfoUpdated => {
                let data: ShippingInfoUpdated = event.to_data().unwrap();
                self.shipping_address = data.shipping_address;
            }
            OrderEvent::Paid => {
                let data: Paid = event.to_data().unwrap();
                self.status = data.status;
            }
            OrderEvent::Deleted => {
                let data: Deleted = event.to_data().unwrap();
                self.status = data.status;
            }
            OrderEvent::Canceled => {
                let data: Canceled = event.to_data().unwrap();
                self.status = data.status;
            }
        }
    }

    fn aggregate_type<'a>() -> &'a str {
        "order"
    }
}
