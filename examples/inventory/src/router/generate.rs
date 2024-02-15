use askama::Template;
use askama_axum::IntoResponse;
use axum::{http::StatusCode, Extension, Form};
use evento::{Command, CommandError};
use std::collections::HashMap;

use crate::{lang::UserLanguage, product::GenerateProductsInput};

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

pub async fn post(
    Extension(cmd): Extension<Command>,
    UserLanguage(lang): UserLanguage,
    Form(input): Form<GenerateProductsInput>,
) -> impl IntoResponse {
    let Err(err) = cmd.execute(lang, &input).await else {
        return ([("Location", "/")], StatusCode::FOUND).into_response();
    };

    if let CommandError::Validation(errors) = err {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            GenerateTemplate {
                errors,
                skip: input.skip,
            },
        )
            .into_response();
    }

    crate::extract::Error::Command(err).into_response()
}
