use askama::Template;
use askama_axum::IntoResponse;
use axum::http::StatusCode;
use std::collections::HashMap;

use crate::product::GenerateProductsInput;

use super::Command;

#[derive(Template)]
#[template(path = "generate.html")]
pub struct GenerateTemplate {
    errors: HashMap<String, Vec<String>>,
    skip: u16,
}

pub async fn get() -> GenerateTemplate {
    GenerateTemplate {
        errors: HashMap::default(),
        skip: 0,
    }
}

pub async fn post(cmd: Command<GenerateProductsInput>) -> impl IntoResponse {
    if let Err(errors) = cmd.output {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            GenerateTemplate {
                errors,
                skip: cmd.input.skip,
            },
        )
            .into_response();
    }

    ([("Location", "/")], StatusCode::FOUND).into_response()
}
