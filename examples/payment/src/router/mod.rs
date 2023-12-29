mod index;
// mod render;

// pub use render::*;

use axum::{routing::get, Router};

use crate::AppState;

pub fn create() -> Router<AppState> {
    Router::new().route("/", get(index::index))
}
