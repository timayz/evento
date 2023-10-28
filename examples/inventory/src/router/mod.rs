mod create;
mod index;
mod render;

use axum::{body::Body, routing::get, Router};

pub use render::*;

use crate::AppState;

pub fn create() -> Router<AppState, Body> {
    Router::new()
        .route("/", get(index::index))
        .route("/create", get(create::get).post(create::post))
}
