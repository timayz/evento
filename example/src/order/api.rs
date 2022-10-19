use actix_web::{delete, post, put, web, HttpResponse, Scope};

use crate::command::CommandResponse;
use crate::order::command::{
    AddProductCommand, CancelCommand, PayCommand, PlaceCommand, RemoveProductCommand,
    UpdateProductQuantityCommand, UpdateShippingInfoCommand,
};
use crate::AppState;

#[post("/place")]
async fn place(data: web::Data<AppState>, input: web::Json<PlaceCommand>) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .into_http_response()
        .await
}

#[post("/add-product")]
async fn add_product(
    data: web::Data<AppState>,
    input: web::Json<AddProductCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .into_http_response()
        .await
}

#[delete("/remove-product")]
async fn remove_product(
    data: web::Data<AppState>,
    input: web::Json<RemoveProductCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .into_http_response()
        .await
}

#[put("/update-product-quantity")]
async fn update_product_quantity(
    data: web::Data<AppState>,
    input: web::Json<UpdateProductQuantityCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .into_http_response()
        .await
}

#[put("/update-shipping-info")]
async fn update_shipping_info(
    data: web::Data<AppState>,
    input: web::Json<UpdateShippingInfoCommand>,
) -> HttpResponse {
    CommandResponse(data.cmd.send(input.0).await)
        .into_http_response()
        .await
}

#[post("/pay")]
async fn pay(data: web::Data<AppState>) -> HttpResponse {
    CommandResponse(data.cmd.send(PayCommand).await)
        .into_http_response()
        .await
}

#[delete("/cancel")]
async fn cancel(data: web::Data<AppState>) -> HttpResponse {
    CommandResponse(data.cmd.send(CancelCommand).await)
        .into_http_response()
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
        .service(cancel)
}
