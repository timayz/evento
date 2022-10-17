use actix_web::{delete, post, put, web, Responder, Result, Scope};

#[post("/create")]
async fn create() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[delete("/delete")]
async fn delete() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[put("/update-quantity")]
async fn update_quantity() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[put("/update-visibility")]
async fn update_visibility() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[put("/update-description")]
async fn update_description() -> Result<impl Responder> {
    Ok(web::Json(true))
}

#[post("/add-review")]
async fn add_review() -> Result<impl Responder> {
    Ok(web::Json(true))
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
