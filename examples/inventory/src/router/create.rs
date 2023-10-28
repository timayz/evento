use std::collections::HashMap;

use askama::Template;
use askama_axum::IntoResponse;
use evento::CommandError;
use evento_axum::Command;
use http::StatusCode;

use crate::product::CreateProduct;

#[derive(Template)]
#[template(path = "create.html")]
pub struct CreateTemplate {
    errors: HashMap<String, Vec<String>>,
}

pub async fn get() -> CreateTemplate {
    CreateTemplate {
        errors: HashMap::default(),
    }
}

pub async fn post(cmd: Command<CreateProduct>) -> impl IntoResponse {
    if let Err(CommandError::Validation(errors)) = cmd.output {
        return (StatusCode::UNPROCESSABLE_ENTITY, CreateTemplate { errors }).into_response();
    }

    ([("Location", "/")], StatusCode::FOUND).into_response()
}
