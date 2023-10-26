use std::collections::HashMap;

use askama::Template;
use evento::CommandError;
use evento_axum::Command;

use crate::product::CreateProduct;

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    errors: HashMap<String, Vec<String>>,
}

pub async fn index() -> IndexTemplate {
    IndexTemplate {
        errors: HashMap::default(),
    }
}

pub async fn create(cmd: Command<CreateProduct>) -> IndexTemplate {
    let errors = if let Err(CommandError::Validation(errors)) = cmd.output {
        errors
    } else {
        HashMap::default()
    };

    IndexTemplate { errors }
}
