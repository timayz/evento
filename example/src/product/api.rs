use std::ops::DerefMut;

use actix_web::{delete, post, put, web, HttpResponse, Scope};

use crate::command::CommandResponse;
use crate::AppState;

use super::aggregate::Product;
use super::command::{
    AddReviewCommand, CreateCommand, DeleteCommand, UpdateDescriptionCommand,
    UpdateQuantityCommand, UpdateVisibilityCommand,
};

#[post("/create")]
async fn create(data: web::Data<AppState>, input: web::Json<CreateCommand>) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

#[delete("/delete")]
async fn delete(data: web::Data<AppState>, input: web::Json<DeleteCommand>) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

#[put("/update-quantity")]
async fn update_quantity(
    data: web::Data<AppState>,
    input: web::Json<UpdateQuantityCommand>,
) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

#[put("/update-visibility")]
async fn update_visibility(
    data: web::Data<AppState>,
    input: web::Json<UpdateVisibilityCommand>,
) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

#[put("/update-description")]
async fn update_description(
    data: web::Data<AppState>,
    input: web::Json<UpdateDescriptionCommand>,
) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

#[post("/add-review")]
async fn add_review(data: web::Data<AppState>, input: web::Json<AddReviewCommand>) -> HttpResponse {
    let mut producer = data.product_producer.lock().await;

    CommandResponse(data.cmd.send(input.0).await)
        .to_response::<Product>(&data.store, producer.deref_mut())
        .await
}

pub fn scope() -> Scope {
    web::scope("/products")
        .service(create)
        .service(delete)
        .service(update_quantity)
        .service(update_visibility)
        .service(update_description)
        .service(add_review)
}
