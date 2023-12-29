mod index;
mod render;

use evento::Rule;
pub use render::*;

use axum::{routing::get, Router};

use crate::{product::ProductRule, AppState};

pub fn create() -> Router<AppState> {
    Router::new()
        .route("/", get(index::index))
        .route("/_product", get(index::product))
}

pub fn rules() -> Vec<Rule> {
    vec![Rule::new(ProductRule::ProductDetails).handler("product/**", index::IndexProductHandler)]
}
