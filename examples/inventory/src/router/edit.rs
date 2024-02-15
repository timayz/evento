use askama::Template;
use askama_axum::IntoResponse;
use axum::{extract::Query, http::StatusCode, Extension, Form};
use evento::{Command, CommandError};
use std::collections::HashMap;

use crate::{
    lang::UserLanguage,
    product::{EditProductInput, GetProductDetails},
};

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

pub async fn get(
    Query(input): Query<GetProductDetails>,
    Extension(query): Extension<evento::Query>,
) -> Result<EditTemplate, crate::extract::Error> {
    let edge = query.execute(&input).await?;

    Ok(EditTemplate {
        errors: HashMap::default(),
        id: edge.node.id,
        name: edge.node.name,
        description: edge.node.description.unwrap_or_default(),
        category: edge.node.category.unwrap_or_default(),
        price: edge.node.price.unwrap_or_default(),
        stock: edge.node.stock,
        visible: edge.node.visible,
    })
}

pub async fn post(
    Extension(cmd): Extension<Command>,
    UserLanguage(lang): UserLanguage,
    Form(input): Form<EditProductInput>,
) -> impl IntoResponse {
    let Err(err) = cmd.execute(lang, &input).await else {
        return ([("Location", "/")], StatusCode::FOUND).into_response();
    };

    if let CommandError::Validation(errors) = err {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            EditTemplate {
                errors,
                id: input.id,
                name: input.name,
                description: input.description,
                category: input.category,
                price: input.price,
                stock: input.stock,
                visible: input.visible.is_some(),
            },
        )
            .into_response();
    }

    crate::extract::Error::Command(err).into_response()
}
