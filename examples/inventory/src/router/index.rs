use askama::Template;
use evento_query::{Edge, QueryResult};

use crate::product::{GetProductDetails, ListProductDetails, ProductDetails};

use super::Query;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub data: QueryResult<ProductDetails>,
    pub cursor: Option<String>,
}

pub async fn index(data: Query<ListProductDetails>) -> IndexTemplate {
    let cursor = match (
        data.output.page_info.end_cursor.to_owned(),
        data.output.page_info.has_next_page,
    ) {
        (Some(cursor), true) => Some(cursor.0),
        _ => None,
    };

    IndexTemplate {
        data: data.output,
        cursor,
    }
}

#[derive(Template)]
#[template(path = "_product.html")]
pub struct ProductTemplate {
    pub edge: Edge<ProductDetails>,
}

pub async fn product(data: Query<GetProductDetails>) -> ProductTemplate {
    ProductTemplate { edge: data.output }
}
