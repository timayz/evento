mod command;
mod order;
mod product;

use actix::{Actor, Addr};
use actix_web::{web, App, HttpServer};
use command::Command;
use evento::{EventStore, PgEngine};
use sqlx::{Executor, PgPool};

pub struct AppState {
    pub cmd: Addr<Command>,
    pub store: EventStore<PgEngine>,
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let pool = init_db().await;
    let cmd = Command::new(pool.clone()).start();

    HttpServer::new(move || {
        let cmd = cmd.clone();
        let store = PgEngine::new(pool.clone());

        App::new()
            .app_data(web::Data::<AppState>::new(AppState { cmd, store }))
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

    sqlx::migrate!().run(&pool).await.unwrap();

    pool
}
