use actix_web::{post, web, Responder, Result, Scope};

#[post("")]
async fn create() -> Result<impl Responder> {
    Ok(web::Json(true))
}

pub fn scope() -> Scope {
    web::scope("/products").service(create)
}
