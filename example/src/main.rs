pub(crate) mod command;
mod order;
mod product;

use axum::Router;
use command::Command;
use evento::PgEngine;
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

    let evento = PgEngine::new(pool)
        .name("example")
        .data(read_db)
        .subscribe(order::subscribe())
        .subscribe(product::subscribe());

    let producer = evento.run(0).await.unwrap();
    let cmd = Command::new(evento, producer);

    let app = Router::new()
        .nest("/orders", order::router())
        .nest("/products", product::router())
        .with_state(AppState { cmd });
    // `GET /` goes to `root`
    // .route("/", get(root))
    // // `POST /users` goes to `create_user`
    // .route("/users", post(create_user));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    // HttpServer::new(move || {
    //     App::new()
    //         .app_data(web::Data::<AppState>::new(AppState { cmd: cmd.clone() }))
    //         .service(order::scope())
    //         .service(product::scope())
    // })
    // .bind(("127.0.0.1", 8080))?
    // .run()
    // .await
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
