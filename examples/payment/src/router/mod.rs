mod index;

use axum::{body::Body, routing::get, Router};

use crate::AppState;

pub fn create() -> Router<AppState, Body> {
    Router::new().route("/", get(index::index))
}
