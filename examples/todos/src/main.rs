use std::fmt;

use askama::Template;
use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse},
    routing::{get, post},
    Form, Router,
};
use evento::{
    prelude::{Migrate, Plan},
    AggregatorName, EventDetails,
};
use heed::{
    types::{SerdeBincode, Str},
    Database, Env, EnvOpenOptions,
};
use serde::{Deserialize, Serialize};
use sqlx::{migrate::MigrateDatabase, SqlitePool};
use tempdir::TempDir;

#[derive(Debug)]
enum LogMsg {
    Checking(String),
    Failed(String),
    Ready,
}

#[derive(askama::Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    pub log_msg: Option<LogMsg>,
    pub todos: Vec<Todo>,
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    let rtxn = state.heed.read_txn().unwrap();
    let Some(db): Option<TodoDB> = state.heed.open_database(&rtxn, Some("query-todo")).unwrap()
    else {
        let template = IndexTemplate {
            log_msg: None,
            todos: vec![],
        };

        return Html(template.render().unwrap());
    };

    let todos: heed::Result<Vec<(&str, Todo)>> = db.iter(&rtxn).unwrap().collect();
    let todos = todos.unwrap().iter().map(|(_, t)| t.clone()).collect();
    rtxn.commit().unwrap();

    let template = IndexTemplate {
        log_msg: None,
        todos,
    };

    Html(template.render().unwrap())
}

#[derive(Deserialize)]
struct CreateInput {
    pub content: String,
}

async fn create(
    State(state): State<AppState>,
    Form(input): Form<CreateInput>,
) -> impl IntoResponse {
    let id = evento::create::<Todo>()
        .data(&CreationRequested {
            content: input.content,
            state: TodoState::Checking,
        })
        .unwrap()
        .metadata(&false)
        .unwrap()
        .commit(&state.evento)
        .await
        .unwrap();

    let template = IndexTemplate {
        log_msg: Some(LogMsg::Checking(id)),
        todos: vec![],
    };

    Html(template.render().unwrap())
}
async fn status(Path((id,)): Path<(String,)>, State(state): State<AppState>) -> impl IntoResponse {
    let todo = evento::load::<Todo, _>(&state.evento, &id).await.unwrap();
    let log_msg = match todo.item.state {
        TodoState::Checking => LogMsg::Checking(id),
        TodoState::Ready => LogMsg::Ready,
        TodoState::Failed(msg) => LogMsg::Failed(msg),
    };
    let template = IndexTemplate {
        log_msg: Some(log_msg),
        todos: vec![],
    };

    Html(template.render().unwrap())
}

#[derive(Clone)]
struct AppState {
    evento: evento::Sqlite,
    heed: Env,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = TempDir::new("todos")?;

    let env = unsafe { EnvOpenOptions::new().max_dbs(10).open(&dir)? };

    let filepath = dir.path().join("todos.sqlite3");
    let dsn = format!("sqlite://{}", filepath.display());

    let _ = sqlx::Sqlite::create_database(&dsn).await;

    let pool = SqlitePool::connect(&dsn).await?;
    let mut conn = pool.acquire().await.unwrap();
    evento::sql_migrator::new_migrator::<sqlx::Sqlite>()
        .unwrap()
        .run(&mut *conn, &Plan::apply_all())
        .await
        .unwrap();

    let executor: evento::Sqlite = pool.into();

    evento::subscribe("todo-command")
        .aggregator::<Todo>()
        .skip::<Todo, CreationSucceeded>()
        .skip::<Todo, CreationFailed>()
        .handler(command_creation_requested())
        .run(&executor)
        .await?;

    evento::subscribe("todo-query")
        .data(env.clone())
        .aggregator::<Todo>()
        .handler(query_creation_failed())
        .handler(query_creation_requested())
        .handler(query_creation_succeeded())
        .run(&executor)
        .await?;

    let app = Router::new()
        .route("/", get(index))
        .route("/create", post(create))
        .route("/status/{id}", get(status))
        .with_state(AppState {
            evento: executor,
            heed: env,
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
enum TodoState {
    #[default]
    Ready,
    Failed(String),
    Checking,
}

impl fmt::Display for TodoState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(AggregatorName, Serialize, Deserialize)]
struct CreationRequested {
    pub content: String,
    pub state: TodoState,
}

#[derive(AggregatorName, Serialize, Deserialize)]
struct CreationFailed {
    pub state: TodoState,
}

#[derive(AggregatorName, Serialize, Deserialize)]
struct CreationSucceeded {
    pub state: TodoState,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
struct Todo {
    pub content: String,
    pub state: TodoState,
}

#[evento::aggregator]
impl Todo {
    async fn creation_requested(
        &mut self,
        event: EventDetails<CreationRequested>,
    ) -> anyhow::Result<()> {
        self.content = event.data.content;
        self.state = event.data.state;

        Ok(())
    }

    async fn creation_failed(&mut self, event: EventDetails<CreationFailed>) -> anyhow::Result<()> {
        self.state = event.data.state;

        Ok(())
    }

    async fn creation_succeeded(
        &mut self,
        event: EventDetails<CreationSucceeded>,
    ) -> anyhow::Result<()> {
        self.state = event.data.state;

        Ok(())
    }
}

#[evento::handler(Todo)]
async fn command_creation_requested<E: evento::Executor>(
    context: &evento::Context<'_, E>,
    event: EventDetails<CreationRequested>,
) -> anyhow::Result<()> {
    if event.data.content.contains("fuck") {
        evento::save::<Todo>(&event.aggregator_id)
            .data(&CreationFailed {
                state: TodoState::Failed("forbidden content".to_owned()),
            })?
            .metadata(&false)?
            .commit(context.executor)
            .await?;
    } else {
        evento::save::<Todo>(&event.aggregator_id)
            .data(&CreationSucceeded {
                state: TodoState::Ready,
            })?
            .metadata(&false)?
            .commit(context.executor)
            .await?;
    }
    Ok(())
}

type TodoDB = Database<Str, SerdeBincode<Todo>>;

#[evento::handler(Todo)]
async fn query_creation_requested<E: evento::Executor>(
    context: &evento::Context<'_, E>,
    event: EventDetails<CreationRequested>,
) -> anyhow::Result<()> {
    let env: Env = context.extract();
    let mut wtxn = env.write_txn()?;
    let db: TodoDB = env.create_database(&mut wtxn, Some("query-todo"))?;
    db.put(
        &mut wtxn,
        &event.aggregator_id,
        &Todo {
            content: event.data.content.to_owned(),
            state: event.data.state.to_owned(),
        },
    )?;

    wtxn.commit()?;

    Ok(())
}

#[evento::handler(Todo)]
async fn query_creation_succeeded<E: evento::Executor>(
    context: &evento::Context<'_, E>,
    event: EventDetails<CreationSucceeded>,
) -> anyhow::Result<()> {
    let env: Env = context.extract();
    let mut wtxn = env.write_txn()?;
    let db: TodoDB = env.create_database(&mut wtxn, Some("query-todo"))?;
    let Some(mut todo) = db.get_or_put(
        &mut wtxn,
        &event.aggregator_id,
        &Todo {
            content: "".to_owned(),
            state: event.data.state.to_owned(),
        },
    )?
    else {
        wtxn.commit()?;
        return Ok(());
    };

    todo.state = event.data.state.to_owned();
    db.put(&mut wtxn, &event.aggregator_id, &todo)?;
    wtxn.commit()?;

    Ok(())
}

#[evento::handler(Todo)]
async fn query_creation_failed<E: evento::Executor>(
    context: &evento::Context<'_, E>,
    event: EventDetails<CreationFailed>,
) -> anyhow::Result<()> {
    let env: Env = context.extract();
    let mut wtxn = env.write_txn()?;
    let db: TodoDB = env.create_database(&mut wtxn, Some("query-todo"))?;
    let Some(mut todo) = db.get_or_put(
        &mut wtxn,
        &event.aggregator_id,
        &Todo {
            content: "".to_owned(),
            state: event.data.state.to_owned(),
        },
    )?
    else {
        wtxn.commit()?;
        return Ok(());
    };

    todo.state = event.data.state.to_owned();
    db.put(&mut wtxn, &event.aggregator_id, &todo)?;
    wtxn.commit()?;

    Ok(())
}
