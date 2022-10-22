use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

use super::aggregate::{Product, Status};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum OrderEvent {
    Placed,
    ProductAdded,
    ProductRemoved,
    ProductQuantityUpdated,
    ShippingInfoUpdated,
    Paid,
    Deleted,
    Canceled,
}

impl From<OrderEvent> for String {
    fn from(o: OrderEvent) -> Self {
        o.to_string()
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Placed {
    pub status: Status,
}

#[derive(Serialize, Deserialize)]
pub struct ProductAdded {
    pub id: String,
    pub quantity: u16,
}

impl From<Product> for ProductAdded {
    fn from(product: Product) -> Self {
        Self {
            id: product.id,
            quantity: product.quantity,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ProductRemoved {
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ProductQuantityUpdated {
    pub id: String,
    pub quantity: u16,
}

impl From<Product> for ProductQuantityUpdated {
    fn from(product: Product) -> Self {
        Self {
            id: product.id,
            quantity: product.quantity,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ShippingInfoUpdated {
    pub shipping_address: String,
}

#[derive(Serialize, Deserialize)]
pub struct Paid {
    pub status: Status,
}

impl Default for Paid {
    fn default() -> Self {
        Self {
            status: Status::Pending,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Deleted {
    pub status: Status,
}

impl Default for Deleted {
    fn default() -> Self {
        Self {
            status: Status::Deleted,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Canceled {
    pub status: Status,
}

impl Default for Canceled {
    fn default() -> Self {
        Self {
            status: Status::Canceled,
        }
    }
}
