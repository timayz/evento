use super::event::{
    Created, Deleted, DescriptionUpdated, ProductEvent, QuantityUpdated, ReviewAdded,
    VisibilityUpdated,
};
use evento::Aggregate;
use serde::{Deserialize, Serialize};

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
    fn apply(&mut self, event: &evento::Event) {
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

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("product_{}", id.into())
    }
}
