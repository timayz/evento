use actix_web::{delete, post, put, web, Responder, Result, Scope};

#[post("/place")]
async fn place() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[post("/add-product")]
async fn add_product() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[delete("/remove-product")]
async fn remove_product() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[put("/update-product-quantity")]
async fn update_product_quantity() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[put("/update-shipping-info")]
async fn update_shipping_info() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[post("/pay")]
async fn pay() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[delete("/cancel")]
async fn cancel() -> Result<impl Responder> {
    Ok(web::Json(true))
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
