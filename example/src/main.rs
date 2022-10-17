mod order;
mod product;
mod shipping;

use actix_web::{App, HttpServer};

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(order::scope()).service(product::scope()))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
