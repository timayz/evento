mod create;
mod delete;
mod details;
mod edit;
mod index;
mod render;

use axum::{body::Body, routing::get, Router};

pub use render::*;

use crate::AppState;

pub fn create() -> Router<AppState, Body> {
    Router::new()
        .route("/", get(index::index))
        .route("/create", get(create::get).post(create::post))
        .route("/details", get(details::get))
        .route("/edit", get(edit::get).post(edit::post))
        .route("/delete", get(delete::get).post(delete::post))
}
