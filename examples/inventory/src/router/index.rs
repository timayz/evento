use askama::Template;
use evento_query::{Edge, QueryResult};

use crate::product::{GetProductDetails, ListProductDetails, ProductDetails};

use super::Query;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub data: QueryResult<ProductDetails>,
}

pub async fn index(data: Query<ListProductDetails>) -> IndexTemplate {
    IndexTemplate { data: data.output }
}

#[derive(Template)]
#[template(path = "_product.html")]
pub struct ProductTemplate {
    pub edge: Edge<ProductDetails>,
}

pub async fn product(data: Query<GetProductDetails>) -> ProductTemplate {
    ProductTemplate { edge: data.output }
}
