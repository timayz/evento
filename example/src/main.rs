mod command;
mod order;
mod product;
mod shipping;

use actix::{Actor, Addr};
use actix_web::{web, App, HttpServer};
use command::Command;
use rbatis::Rbatis;

pub struct AppState {
    pub cmd: Addr<Command>,
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let rb = init_db().await;
    let cmd = Command::new(rb).start();

    HttpServer::new(move || {
        let cmd = cmd.clone();

        App::new()
            .app_data(web::Data::<AppState>::new(AppState { cmd }))
            .service(order::scope())
            .service(product::scope())
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

async fn init_db() -> Rbatis {
    let rb = Rbatis::new();
    rb.init(
        rbdc_pg::driver::PgDriver {},
        "postgres://postgres:postgres@localhost:5432/postgres",
    )
    .unwrap();

    let _ = rb.exec(" CREATE DATABASE evento_example;", vec![]).await;

    drop(rb);

    let rb = Rbatis::new();
    rb.init(
        rbdc_pg::driver::PgDriver {},
        "postgres://postgres:postgres@localhost:5432/evento_example",
    )
    .unwrap();

    let sql = std::fs::read_to_string("./example/db.sql").unwrap();
    let _ = rb.exec(&sql, vec![]).await;

    rb
}
