use actix_web::{delete, get, post, put, web, App, HttpServer, Responder, Result};
use serde::Serialize;

#[derive(Serialize)]
struct Todo {
    id: u64,
    text: String,
    done: bool,
}

#[get("")]
async fn list() -> Result<impl Responder> {
    let todos: Vec<Todo> = Vec::new();
    Ok(web::Json(todos))
}

#[post("")]
async fn create() -> Result<impl Responder> {
    let todo = Todo {
        id: 1,
        text: "Hello world".to_owned(),
        done: false,
    };

    Ok(web::Json(todo))
}

#[put("/{id}")]
async fn update(id: web::Path<u64>) -> Result<impl Responder> {
    let todo = Todo {
        id: id.to_owned(),
        text: "Hello world".to_owned(),
        done: false,
    };

    Ok(web::Json(todo))
}

#[delete("/{id}")]
async fn del(id: web::Path<u64>) -> Result<impl Responder> {
    let todo = Todo {
        id: id.to_owned(),
        text: "Hello world".to_owned(),
        done: false,
    };

    Ok(web::Json(todo))
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new().service(
            web::scope("/todos")
                .service(list)
                .service(create)
                .service(update)
                .service(del),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
