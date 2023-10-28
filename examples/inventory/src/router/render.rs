use askama::Template;
use askama_axum::{IntoResponse, Response};
use http::StatusCode;

#[derive(Template)]
#[template(path = "_404.html")]
pub struct NotFoundTemplate;

#[derive(Template)]
#[template(path = "_500.html")]
pub struct ServerErrorTemplate;

pub type Command<T> = evento_axum::Command<T, Render>;

pub struct Render;

impl evento_axum::Render for Render {
    fn not_found(_cmd: &evento::Command, _msg: String) -> Response {
        (
            StatusCode::NOT_FOUND,
            [("X-Up-Fail-Target", ".errors")],
            NotFoundTemplate,
        )
            .into_response()
    }

    fn server(_cmd: &evento::Command, _msg: String) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("X-Up-Fail-Target", ".errors")],
            ServerErrorTemplate,
        )
            .into_response()
    }
}
