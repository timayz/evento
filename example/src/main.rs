pub(crate) mod command;
mod order;
mod product;

use std::sync::Arc;

use actix::{Actor, Addr};
use actix_web::{web, App, HttpServer};
use command::Command;
use evento::{EventStore};
use mongodb::{options::ClientOptions, Client};
use pulsar::{Producer, Pulsar, TokioExecutor};
use sqlx::{Executor, PgPool};
use tokio::sync::Mutex;

pub struct AppState {
    pub cmd: Addr<Command>,
    pub store: EventStore<evento::store::PgEngine>,
    pub order_producer: Arc<Mutex<Producer<TokioExecutor>>>,
    pub product_producer: Arc<Mutex<Producer<TokioExecutor>>>,
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let pool = init_db().await;
    let cmd = Command::new(pool.clone()).start();
    let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
        .build()
        .await
        .unwrap();

    let client_options = ClientOptions::parse("mongodb://mongo:mongo@127.0.0.1:27017")
        .await
        .unwrap();

    let read_db = Client::with_options(client_options)
        .map(|client| client.database("evento_example"))
        .unwrap();

    order::start(&pulsar, &read_db).await;
    product::start(&pulsar, &read_db).await;

    let order_producer = Arc::new(Mutex::new(
        pulsar
            .producer()
            .with_topic("non-persistent://public/default/order")
            .with_name("exemple")
            .build()
            .await
            .expect("create order product failed"),
    ));

    let product_producer = Arc::new(Mutex::new(
        pulsar
            .producer()
            .with_topic("non-persistent://public/default/product")
            .with_name("exemple")
            .build()
            .await
            .expect("create order product failed"),
    ));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::<AppState>::new(AppState {
                cmd: cmd.clone(),
                store: evento::store::PgEngine::new(pool.clone()),
                order_producer: order_producer.clone(),
                product_producer: product_producer.clone(),
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
