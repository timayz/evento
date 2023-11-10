pub(crate) mod command;
mod order;
mod product;

use axum::Router;
use command::Command;
use evento::PgConsumer;
use mongodb::{options::ClientOptions, Client};
use sqlx::{Executor, PgPool};
use std::net::SocketAddr;

#[derive(Clone)]
pub struct AppState {
    pub cmd: Command,
}

#[tokio::main]
async fn main() {
    let pool = init_db().await;

    let client_options = ClientOptions::parse("mongodb://mongo:mongo@127.0.0.1:27017")
        .await
        .unwrap();

    let read_db = Client::with_options(client_options)
        .map(|client| client.database("evento_example"))
        .unwrap();

    let evento = PgConsumer::new(&pool)
        .name("example")
        .data(read_db)
        .rule(order::rule())
        .rule(product::rule());

    let producer = evento.start(0).await.unwrap();
    let cmd = Command::new(producer);

    let app = Router::new()
        .nest("/orders", order::router())
        .nest("/products", product::router())
        .with_state(AppState { cmd });

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
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
