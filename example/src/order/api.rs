use actix_web::{delete, post, put, web, HttpResponse, Scope};

use crate::command::CommandResponse;
use crate::AppState;

use super::aggregate::Order;
use super::command::{
    AddProductCommand, CancelCommand, DeleteCommand, PayCommand, PlaceCommand,
    RemoveProductCommand, UpdateProductQuantityCommand, UpdateShippingInfoCommand,
};

#[post("/place")]
async fn place(data: web::Data<AppState>, input: web::Json<PlaceCommand>) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[post("/add-product")]
async fn add_product(
    data: web::Data<AppState>,
    input: web::Json<AddProductCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[delete("/remove-product")]
async fn remove_product(
    data: web::Data<AppState>,
    input: web::Json<RemoveProductCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[put("/update-product-quantity")]
async fn update_product_quantity(
    data: web::Data<AppState>,
    input: web::Json<UpdateProductQuantityCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[put("/update-shipping-info")]
async fn update_shipping_info(
    data: web::Data<AppState>,
    input: web::Json<UpdateShippingInfoCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[post("/pay")]
async fn pay(data: web::Data<AppState>, input: web::Json<PayCommand>) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[delete("/delete")]
async fn delete(data: web::Data<AppState>, input: web::Json<DeleteCommand>) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

#[delete("/cancel")]
async fn cancel(data: web::Data<AppState>, input: web::Json<CancelCommand>) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Order>(&data.store)
        .await
}

pub fn scope() -> Scope {
    web::scope("/orders")
        .service(place)
        .service(add_product)
        .service(remove_product)
        .service(update_product_quantity)
        .service(update_shipping_info)
        .service(pay)
        .service(delete)
        .service(cancel)
}
