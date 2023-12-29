use askama::Template;
use askama_axum::IntoResponse;
use axum::http::StatusCode;
use std::collections::HashMap;

use crate::product::{EditProductInput, GetProductDetails};

use super::{Command, Query};

#[derive(Template)]
#[template(path = "edit.html")]
pub struct EditTemplate {
    errors: HashMap<String, Vec<String>>,
    id: String,
    name: String,
    description: String,
    category: String,
    price: f32,
    stock: i32,
    visible: bool,
}

pub async fn get(query: Query<GetProductDetails>) -> EditTemplate {
    EditTemplate {
        errors: HashMap::default(),
        id: query.output.node.id,
        name: query.output.node.name,
        description: query.output.node.description.unwrap_or_default(),
        category: query.output.node.category.unwrap_or_default(),
        price: query.output.node.price.unwrap_or_default(),
        stock: query.output.node.stock,
        visible: query.output.node.visible,
    }
}

pub async fn post(cmd: Command<EditProductInput>) -> impl IntoResponse {
    if let Err(errors) = cmd.output {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            EditTemplate {
                errors,
                id: cmd.input.id,
                name: cmd.input.name,
                description: cmd.input.description,
                category: cmd.input.category,
                price: cmd.input.price,
                stock: cmd.input.stock,
                visible: cmd.input.visible.is_some(),
            },
        )
            .into_response();
    }

    ([("Location", "/")], StatusCode::FOUND).into_response()
}
