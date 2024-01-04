use askama::Template;
use axum::{extract::Query, Extension};

use crate::{extract, product::GetProductDetails};

#[derive(Template)]
#[template(path = "details.html")]
pub struct DetailsTemplate {
    name: String,
    description: String,
    thumbnail: Option<String>,
}

pub async fn get(
    Query(input): Query<GetProductDetails>,
    Extension(query): Extension<evento::Query>,
) -> Result<DetailsTemplate, extract::Error> {
    let edge = query.execute(&input).await?;

    Ok(DetailsTemplate {
        name: edge.node.name,
        description: edge.node.description.unwrap_or_default(),
        thumbnail: edge.node.thumbnail,
    })
}
