use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum OrderEvent {
    Placed,
    ProductAdded,
    ProductRemoved,
    ProductQuantityUpdated,
    ShippingInfoUpdated,
    Paid,
    Canceled
}

impl Into<String> for OrderEvent {
    fn into(self) -> String {
        self.to_string()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Placed {}

#[derive(Serialize, Deserialize)]
pub struct ProductAdded {}

#[derive(Serialize, Deserialize)]
pub struct ProductQuantityUpdated {}

#[derive(Serialize, Deserialize)]
pub struct ShippingInfoUpdated {}

#[derive(Serialize, Deserialize)]
pub struct Paid {}
