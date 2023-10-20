mod router;

use anyhow::Result;
use axum::{response::IntoResponse, routing::get};
use evento::{PgConsumer, Producer};
use http::{header, StatusCode, Uri};
use rust_embed::RustEmbed;
use sqlx::{migrate::MigrateDatabase, Any, PgPool};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub producer: Producer,
}

#[tokio::main]
async fn main() -> Result<()> {
    sqlx::any::install_default_drivers();

    let dsn = "postgres://postgres:postgres@localhost:5432/inventory";
    let _ = Any::create_database(dsn).await;
    let db = PgPool::connect(dsn).await?;

    sqlx::migrate!().run(&db).await?;

    let producer = PgConsumer::new(&db).start(0).await?;

    let app = router::create()
        .fallback(get(static_handler))
        .with_state(AppState { db, producer });

    let addr = "0.0.0.0:3002".parse()?;

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[derive(RustEmbed)]
#[folder = "public/"]
#[prefix = "/static/"]
struct Assets;

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let uri = uri.to_string();

    if !uri.starts_with("/static/") {
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/html")],
            "",
        )
            .into_response();
    }

    match Assets::get(uri.as_str()) {
        Some(content) => {
            let mime = mime_guess::from_path(uri).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}
