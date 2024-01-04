use askama::Template;
use askama_axum::IntoResponse;
use axum::{http::StatusCode, Extension, Form};
use evento::{Command, CommandError};
use std::collections::HashMap;

use crate::{
    extract::{self},
    lang::UserLanguage,
    product::CreateProductInput,
};

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

pub async fn post(
    Extension(cmd): Extension<Command>,
    UserLanguage(lang): UserLanguage,
    Form(input): Form<CreateProductInput>,
) -> impl IntoResponse {
    let Err(err) = cmd.execute(lang, &input).await else {
        return ([("Location", "/")], StatusCode::FOUND).into_response();
    };

    if let CommandError::Validation(errors) = err {
        return (StatusCode::UNPROCESSABLE_ENTITY, CreateTemplate { errors }).into_response();
    }

    extract::Error::Command(err).into_response()
}
