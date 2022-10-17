use actix_web::{post, web, Responder, Result, Scope};

#[post("place")]
async fn place() -> Result<impl Responder> {
    Ok(web::Json(true))
}

pub fn scope() -> Scope {
    web::scope("/orders").service(place)
}
