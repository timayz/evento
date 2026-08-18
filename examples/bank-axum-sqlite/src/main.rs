//! Bank web app on **SQLite** (`evento-sql`) with schema migrations.
//!
//! An in-memory SQLite database is created and migrated on boot, commands are
//! executed through the shared `bank` domain crate, and a projection
//! subscription keeps the in-memory read model
//! (`AccountDetailsView::snapshot_rows()`) current — so `/accounts` lists every
//! account, not just the ones already visited.
//!
//! ```text
//! cargo run -p bank-axum-sqlite
//! # then open http://127.0.0.1:3000
//! ```
//!
//! Domain errors (insufficient funds, frozen account, …) surface as
//! `422 Unprocessable Entity` instead of being silently swallowed.

use std::sync::Arc;

use askama::Template;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Form, Router,
};
use bank::{
    account_details, AccountDetailsView, AccountType, BankAccountError, Command, DepositMoney,
    OpenAccount, TransferMoney, WithdrawMoney,
};
use evento::sql::Sql;
use serde::Deserialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx_migrator::{Migrate, Plan};
use ulid::Ulid;

type Executor = Sql<sqlx::Sqlite>;

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create in-memory SQLite database
    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    // Run migrations
    let mut conn = pool.acquire().await?;
    let migrator = evento::sql_migrator::new::<sqlx::Sqlite>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    drop(conn);

    let executor: Executor = pool.into();

    // Keep the in-memory read model (`AccountDetailsView::snapshot_rows()`)
    // current. The cache is per-process, so a fresh per-boot subscription key
    // replays all events and then streams new ones live.
    let subscription = account_details::create_projection()
        .subscription(format!("account-details-{}", Ulid::generate()))
        .all()
        .start(&executor)
        .await?;

    let state = AppState {
        executor: Arc::new(executor),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/accounts", get(list_accounts))
        .route("/accounts/new", get(new_account_form).post(create_account))
        .route("/accounts/{id}", get(view_account))
        .route("/accounts/{id}/deposit", post(deposit))
        .route("/accounts/{id}/withdraw", post(withdraw))
        .route("/accounts/{id}/transfer", post(transfer))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("Listening on http://127.0.0.1:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    subscription.shutdown().await?;
    Ok(())
}

/// Maps a command's outcome to an HTTP response: domain rejections become 422,
/// infrastructure failures 500, success redirects to the account page.
fn command_response(id: &str, result: Result<(), BankAccountError>) -> Response {
    match result {
        Ok(()) => Redirect::to(&format!("/accounts/{id}")).into_response(),
        Err(BankAccountError::Server(err)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err).into_response()
        }
        Err(err) => (StatusCode::UNPROCESSABLE_ENTITY, err.to_string()).into_response(),
    }
}

// Templates

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate;

#[derive(Template)]
#[template(path = "accounts/list.html")]
struct AccountsListTemplate {
    accounts: Vec<AccountView>,
}

#[derive(Template)]
#[template(path = "accounts/new.html")]
struct NewAccountTemplate;

#[derive(Template)]
#[template(path = "accounts/view.html")]
struct ViewAccountTemplate {
    account: AccountView,
    accounts: Vec<AccountView>,
}

struct AccountView {
    id: String,
    balance: i64,
    currency: String,
    status: String,
}

fn render<T: Template>(template: T) -> Response {
    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

// Handlers

async fn index() -> Response {
    render(IndexTemplate)
}

async fn list_accounts() -> Response {
    let accounts = get_all_accounts();
    render(AccountsListTemplate { accounts })
}

async fn new_account_form() -> Response {
    render(NewAccountTemplate)
}

#[derive(Deserialize)]
struct CreateAccountForm {
    owner_name: String,
    initial_balance: i64,
    currency: String,
}

async fn create_account(
    State(state): State<AppState>,
    Form(form): Form<CreateAccountForm>,
) -> Response {
    let cmd = Command(state.executor.as_ref().clone());
    let owner_id = Ulid::generate().to_string();

    match cmd
        .open_account(OpenAccount {
            owner_id,
            owner_name: form.owner_name,
            account_type: AccountType::Checking,
            currency: form.currency,
            initial_balance: form.initial_balance,
        })
        .await
    {
        Ok(id) => Redirect::to(&format!("/accounts/{id}")).into_response(),
        Err(BankAccountError::Server(err)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err).into_response()
        }
        Err(err) => (StatusCode::UNPROCESSABLE_ENTITY, err.to_string()).into_response(),
    }
}

async fn view_account(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    // The owner id lives on the view itself (set by AccountOpened): load the
    // account's own events first, then re-load co-keyed with the owner so the
    // owner's events (e.g. a name change) are folded in as well.
    let row = match account_details::create_projection()
        .load(&id)
        .execute(state.executor.as_ref())
        .await
    {
        Ok(Some(first)) => {
            match account_details::load(state.executor.as_ref(), &id, first.owner_id).await {
                Ok(row) => row,
                Err(err) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                }
            }
        }
        Ok(None) => None,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let accounts = get_all_accounts();

    match row {
        Some(view) => {
            let account = AccountView {
                id: id.to_owned(),
                balance: view.balance,
                currency: view.currency.to_owned(),
                status: format!("{:?}", view.status),
            };
            render(ViewAccountTemplate { account, accounts })
        }
        None => Html("<h1>Account not found</h1>".to_owned()).into_response(),
    }
}

#[derive(Deserialize)]
struct DepositForm {
    amount: i64,
}

async fn deposit(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Form(form): Form<DepositForm>,
) -> Response {
    let cmd = Command(state.executor.as_ref().clone());
    let result = cmd
        .deposit_money(
            &id,
            DepositMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web deposit".to_string(),
            },
        )
        .await;

    command_response(&id, result)
}

#[derive(Deserialize)]
struct WithdrawForm {
    amount: i64,
}

async fn withdraw(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Form(form): Form<WithdrawForm>,
) -> Response {
    let cmd = Command(state.executor.as_ref().clone());
    let result = cmd
        .withdraw_money(
            &id,
            WithdrawMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web withdrawal".to_string(),
            },
        )
        .await;

    command_response(&id, result)
}

#[derive(Deserialize)]
struct TransferForm {
    to_account_id: String,
    amount: i64,
}

async fn transfer(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Form(form): Form<TransferForm>,
) -> Response {
    let cmd = Command(state.executor.as_ref().clone());
    let result = cmd
        .transfer_money(
            &id,
            TransferMoney {
                amount: form.amount,
                to_account_id: form.to_account_id,
                transaction_id: Ulid::generate().to_string(),
                description: "Web transfer".to_string(),
            },
        )
        .await;

    command_response(&id, result)
}

// Helper functions

fn get_all_accounts() -> Vec<AccountView> {
    let rows = AccountDetailsView::snapshot_rows().read().unwrap();
    let mut accounts: Vec<AccountView> = rows
        .iter()
        .map(|(id, view)| AccountView {
            id: id.to_owned(),
            balance: view.balance,
            currency: view.currency.to_owned(),
            status: format!("{:?}", view.status),
        })
        .collect();
    // The read model is an unordered `HashMap`, whose iteration order is random
    // per process. Account ids are ULIDs, so sorting by id gives a stable
    // creation-time order that is identical across runs and nodes.
    accounts.sort_by(|a, b| a.id.cmp(&b.id));
    accounts
}
