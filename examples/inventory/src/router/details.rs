use askama::Template;

use crate::product::GetProductDetails;

use super::Query;

#[derive(Template)]
#[template(path = "details.html")]
pub struct DetailsTemplate {
    // id: String,
    name: String,
    description: String,
    // price: f32,
    // stock: i32,
    // visible: bool,
}

pub async fn get(query: Query<GetProductDetails>) -> DetailsTemplate {
    DetailsTemplate {
        // id: query.output.node.id,
        name: query.output.node.name,
        description: query.output.node.description.unwrap_or_default(),
        // price: query.output.node.price.unwrap_or_default(),
        // stock: query.output.node.stock,
        // visible: query.output.node.visible,
    }
}
