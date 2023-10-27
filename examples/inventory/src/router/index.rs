use std::collections::HashMap;

use askama::Template;
use askama_axum::IntoResponse;
use evento::CommandError;
use evento_axum::Command;
use http::StatusCode;

use crate::product::CreateProduct;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    errors: HashMap<String, Vec<String>>,
    create_modal: bool,
}

pub async fn index() -> IndexTemplate {
    IndexTemplate {
        errors: HashMap::default(),
        create_modal: false,
    }
}

pub async fn create() -> IndexTemplate {
    IndexTemplate {
        errors: HashMap::default(),
        create_modal: true,
    }
}

pub async fn create_action(cmd: Command<CreateProduct>) -> impl IntoResponse {
    if let Err(CommandError::Validation(errors)) = cmd.output {
        return IndexTemplate {
            errors,
            create_modal: true,
        }
        .into_response();
    }

    ([("hx-location", "/")], StatusCode::NO_CONTENT).into_response()
}
