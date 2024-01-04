use evento::PublisherEvent;
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

#[derive(Display, FromStr, PublisherEvent)]
#[display(style = "kebab-case")]
pub enum ProductEvent {
    Created,
    Deleted,
    Edited,
    VisibilityChanged,
    ThumbnailChanged,
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
    pub category: String,
    pub visible: bool,
    pub stock: i32,
    pub price: f32,
}

#[derive(Serialize, Deserialize)]
pub struct VisibilityChanged {
    pub visible: bool,
}

#[derive(Serialize, Deserialize)]
pub struct ThumbnailChanged {
    pub thumbnail: String,
}

#[derive(Display, FromStr, PublisherEvent)]
#[display(style = "kebab-case")]
pub enum ProductTaskEvent {
    GenerateProductsRequested,
}

#[derive(Serialize, Deserialize)]
pub struct GenerateProductsRequested {
    pub skip: u16,
}
