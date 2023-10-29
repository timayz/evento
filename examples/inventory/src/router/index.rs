use askama::Template;
use evento_query::QueryResult;

use crate::product::{ListProductDetails, ProductDetails};

use super::Query;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub data: QueryResult<ProductDetails>,
}

pub async fn index(data: Query<ListProductDetails>) -> IndexTemplate {
    IndexTemplate { data: data.output }
}
