use axum::{body::Body, extract::State, routing, Json, Router};
use serde_json::Value;

use crate::{
    command::{CommandResponse, CommandResult},
    AppState,
};

use super::command::{
    AddProductCommand, CancelCommand, DeleteCommand, PayCommand, PlaceCommand,
    RemoveProductCommand, UpdateProductQuantityCommand, UpdateShippingInfoCommand,
};

async fn place(
    State(state): State<AppState>,
    Json(input): Json<PlaceCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.place_order(input).await).into()
}

async fn add_product(
    State(state): State<AppState>,
    Json(input): Json<AddProductCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.add_product_to_order(input).await).into()
}

async fn remove_product(
    State(state): State<AppState>,
    Json(input): Json<RemoveProductCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.remove_product_from_order(input).await).into()
}

async fn update_product_quantity(
    State(state): State<AppState>,
    Json(input): Json<UpdateProductQuantityCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.update_product_quantity_of_order(input).await).into()
}

async fn update_shipping_info(
    State(state): State<AppState>,
    Json(input): Json<UpdateShippingInfoCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.update_shipping_info_of_order(input).await).into()
}

async fn pay(
    State(state): State<AppState>,
    Json(input): Json<PayCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.pay_order(input).await).into()
}

async fn delete(
    State(state): State<AppState>,
    Json(input): Json<DeleteCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.delete_order(input).await).into()
}

async fn cancel(
    State(state): State<AppState>,
    Json(input): Json<CancelCommand>,
) -> CommandResult<Json<Value>> {
    CommandResponse(state.cmd.cancel_order(input).await).into()
}

pub fn router() -> Router<AppState, Body> {
    Router::new()
        .route("/place", routing::post(place))
        .route("/add-product", routing::post(add_product))
        .route("/remove-product", routing::delete(remove_product))
        .route(
            "/update-product-quantity",
            routing::put(update_product_quantity),
        )
        .route("/update-shipping-info", routing::put(update_shipping_info))
        .route("/pay", routing::post(pay))
        .route("/delete", routing::delete(delete))
        .route("/cancel", routing::delete(cancel))
}
