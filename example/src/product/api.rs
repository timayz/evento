use axum::{body::Body, extract::State, routing, Json, Router};
use evento_axum::{CommandResponse, CommandResult};
use serde_json::Value;

use crate::AppState;

use super::command::{
    AddReviewCommand, CreateCommand, DeleteCommand, UpdateDescriptionCommand,
    UpdateQuantityCommand, UpdateVisibilityCommand,
};

async fn create(
    State(state): State<AppState>,
    Json(input): Json<CreateCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.create_product(input).await).into()
}

async fn delete(
    State(state): State<AppState>,
    Json(input): Json<DeleteCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.delete_product(input).await).into()
}

async fn update_quantity(
    State(state): State<AppState>,
    Json(input): Json<UpdateQuantityCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.update_quantity_of_product(input).await).into()
}

async fn update_visibility(
    State(state): State<AppState>,
    Json(input): Json<UpdateVisibilityCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.update_visivility_of_product(input).await).into()
}

async fn update_description(
    State(state): State<AppState>,
    Json(input): Json<UpdateDescriptionCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.update_description_of_product(input).await).into()
}

async fn add_review(
    State(state): State<AppState>,
    Json(input): Json<AddReviewCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.add_review_to_product(input).await).into()
}

pub fn router() -> Router<AppState, Body> {
    Router::new()
        .route("/create", routing::post(create))
        .route("/delete", routing::delete(delete))
        .route("/update-quantity", routing::put(update_quantity))
        .route("/update-visibility", routing::put(update_visibility))
        .route("/update-description", routing::put(update_description))
        .route("/add-review", routing::post(add_review))
}
