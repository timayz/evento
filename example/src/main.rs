pub(crate) mod command;
mod order;
mod product;

use actix::{Actor, Addr};
use actix_web::{web, App, HttpServer};
use command::Command;
use evento::{PgEngine, Publisher};
use mongodb::{options::ClientOptions, Client};
use sqlx::{Executor, PgPool};

pub struct AppState {
    pub cmd: Addr<Command>,
    pub publisher: Publisher<evento::store::PgEngine>,
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let pool = init_db().await;

    let client_options = ClientOptions::parse("mongodb://mongo:mongo@127.0.0.1:27017")
        .await
        .unwrap();

    let read_db = Client::with_options(client_options)
        .map(|client| client.database("evento_example"))
        .unwrap();

    let store = PgEngine::new(pool);
    let cmd = Command::new(store.clone()).start();
    let publisher = store
        .name("example")
        .data(read_db)
        .subscribe(order::subscribe())
        .subscribe(product::subscribe())
        .run()
        .await
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::<AppState>::new(AppState {
                cmd: cmd.clone(),
                publisher: publisher.clone(),
            }))
            .service(order::scope())
            .service(product::scope())
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

async fn init_db() -> PgPool {
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/postgres")
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let _ = conn.execute("create database evento_example;").await;

    drop(pool);

    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/evento_example")
        .await
        .unwrap();

    sqlx::migrate!("../migrations").run(&pool).await.unwrap();

    pool
}
