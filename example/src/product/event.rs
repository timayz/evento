use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ProductEvent {
    Created,
    Deleted,
    QuantityUpdated,
    VisibilityUpdated,
    DescriptionUpdated,
    ReviewAdded,
}

impl Into<String> for ProductEvent {
    fn into(self) -> String {
        self.to_string()
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
pub struct QuantityUpdated {
    pub quantity: u16,
}

#[derive(Serialize, Deserialize)]
pub struct VisibilityUpdated {
    pub visible: bool,
}

#[derive(Serialize, Deserialize)]
pub struct DescriptionUpdated {
    pub description: String,
}

#[derive(Serialize, Deserialize)]
pub struct ReviewAdded {
    pub note: u8,
    pub message: String,
}
