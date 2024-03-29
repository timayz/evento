mod create;
mod delete;
mod details;
mod edit;
mod generate;
mod index;

use axum::{routing::get, Router};
use evento::Rule;

use crate::{product::ProductRule, AppState};

pub fn create() -> Router<AppState> {
    Router::new()
        .route("/", get(index::index))
        .route("/_product", get(index::product))
        .route("/create", get(create::get).post(create::post))
        .route("/details", get(details::get))
        .route("/edit", get(edit::get).post(edit::post))
        .route("/generate", get(generate::get).post(generate::post))
        .route("/delete", get(delete::get).post(delete::post))
}

pub fn rules() -> Vec<Rule> {
    vec![Rule::new(ProductRule::ProductDetails).handler("product/**", index::IndexProductHandler)]
}
