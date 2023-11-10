use askama::Template;
use askama_axum::IntoResponse;
use http::StatusCode;

use crate::product::{DeleteProductInput, GetProductDetails};

use super::{Command, Query};

#[derive(Template)]
#[template(path = "delete.html")]
pub struct DeleteTemplate {
    id: String,
    name: Option<String>,
}

pub async fn get(query: Query<GetProductDetails>) -> DeleteTemplate {
    DeleteTemplate {
        id: query.output.node.id,
        name: Some(query.output.node.name),
    }
}

pub async fn post(_cmd: Command<DeleteProductInput>) -> impl IntoResponse {
    ([("Location", "/")], StatusCode::FOUND).into_response()
}
