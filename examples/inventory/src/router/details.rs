use askama::Template;

use crate::product::GetProductDetails;

use super::Query;

#[derive(Template)]
#[template(path = "details.html")]
pub struct DetailsTemplate {
    name: String,
    description: String,
    thumbnail: Option<String>,
}

pub async fn get(query: Query<GetProductDetails>) -> DetailsTemplate {
    DetailsTemplate {
        name: query.output.node.name,
        description: query.output.node.description.unwrap_or_default(),
        thumbnail: query.output.node.thumbnail,
    }
}
