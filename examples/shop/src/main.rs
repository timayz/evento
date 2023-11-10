mod product;
mod router;

use std::{collections::HashMap, convert::Infallible, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::{sse::Event, IntoResponse, Sse},
    routing::get,
    Extension,
};
use evento::{Command, PgConsumer, Producer, Query};
use http::{header, StatusCode, Uri};
use rust_embed::RustEmbed;
use sqlx::{migrate::MigrateDatabase, Any, PgPool};
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        RwLock,
    },
    time::{interval_at, Instant},
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub producer: Producer,
    pub publisher: Publisher,
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::from_str("error,evento=debug")?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .init();

    sqlx::any::install_default_drivers();

    let dsn = "postgres://postgres:postgres@localhost:5432/shop";
    let _ = Any::create_database(dsn).await;
    let db = PgPool::connect(dsn).await?;

    sqlx::migrate!().run(&db).await?;

    let publisher = Publisher::default();

    let producer = PgConsumer::new(&db)
        .data(publisher.clone())
        .rules(product::rules().await?)
        .rules(router::rules())
        .start(0)
        .await?;

    let command = Command::new(&producer);
    let query = Query::new().data(db.clone());

    let app = router::create()
        .route("/events/:name", get(sse_handler))
        .layer(Extension(command))
        .layer(Extension(query))
        .fallback(get(static_handler))
        .with_state(AppState {
            db,
            producer,
            publisher,
        });

    let addr = "0.0.0.0:3000".parse()?;

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

async fn sse_handler(
    State(state): State<AppState>,
    Path((name,)): Path<(String,)>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = state.publisher.client(name).await;

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("ping"),
    )
}

pub type PublisherSender = Sender<Result<Event, Infallible>>;

#[derive(Debug, Clone)]
pub struct Publisher(Arc<RwLock<HashMap<String, Vec<PublisherSender>>>>);

impl Publisher {
    pub async fn send(
        &self,
        stream: impl AsRef<str>,
        event: impl AsRef<str>,
        data: impl AsRef<str>,
    ) {
        let event = Event::default().event(event).data(data);

        let streams = self.0.read().await;

        let Some(clients) = streams.get(stream.as_ref()) else {
            return;
        };

        let fut = clients
            .iter()
            .map(|client| client.send(Ok(event.clone())))
            .collect::<Vec<_>>();

        futures::future::join_all(fut).await;
    }

    pub async fn send_all(
        &self,
        streams: Vec<impl AsRef<str>>,
        event: impl AsRef<str>,
        data: impl AsRef<str>,
    ) {
        let fut = streams
            .iter()
            .map(|stream| self.send(stream, event.as_ref(), data.as_ref()))
            .collect::<Vec<_>>();

        futures::future::join_all(fut).await;
    }

    pub async fn client(&self, stream: String) -> ReceiverStream<Result<Event, Infallible>> {
        let (tx, rx) = channel::<Result<Event, Infallible>>(100);

        let mut streams = self.0.write().await;
        let clients = streams.entry(stream).or_default();
        clients.push(tx);

        rx.into()
    }
}

impl Default for Publisher {
    fn default() -> Self {
        let publisher = Self(Default::default());

        tokio::spawn({
            let publisher = publisher.clone();

            async move {
                let mut interval = interval_at(Instant::now(), Duration::from_secs(10));

                loop {
                    interval.tick().await;

                    let mut streams = publisher.0.write().await;

                    for (_, clients) in streams.iter_mut() {
                        clients.retain(|client| !client.is_closed());
                    }

                    drop(streams);
                }
            }
        });

        publisher
    }
}
