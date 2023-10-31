use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ProductEvent {
    Created,
    Deleted,
    Edited,
    VisibilityChanged,
}

impl From<ProductEvent> for String {
    fn from(p: ProductEvent) -> Self {
        p.to_string()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Created {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct Deleted {
    pub deleted: bool,
}

#[derive(Serialize, Deserialize)]
pub struct Edited {
    pub name: String,
    pub description: String,
    pub visible: bool,
    pub stock: i32,
    pub price: f32,
}

#[derive(Serialize, Deserialize)]
pub struct VisibilityChanged {
    pub visible: bool,
}
