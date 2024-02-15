use askama::Template;
use askama_axum::IntoResponse;
use axum::{extract::Query, http::StatusCode, Extension, Form};
use evento::Command;

use crate::{
    extract,
    lang::UserLanguage,
    product::{DeleteProductInput, GetProductDetails},
};

#[derive(Template)]
#[template(path = "delete.html")]
pub struct DeleteTemplate {
    id: String,
    name: Option<String>,
}

pub async fn get(
    Query(input): Query<GetProductDetails>,
    Extension(query): Extension<evento::Query>,
) -> Result<DeleteTemplate, extract::Error> {
    let edge = query.execute(&input).await?;

    Ok(DeleteTemplate {
        id: edge.node.id,
        name: Some(edge.node.name),
    })
}

pub async fn post(
    Extension(cmd): Extension<Command>,
    UserLanguage(lang): UserLanguage,
    Form(input): Form<DeleteProductInput>,
) -> impl IntoResponse {
    let Err(err) = cmd.execute(lang, &input).await else {
        return ([("Location", "/")], StatusCode::FOUND).into_response();
    };

    extract::Error::Command(err).into_response()
}
