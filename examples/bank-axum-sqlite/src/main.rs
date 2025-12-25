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
    account_details_subscription, AccountType, Command, DepositMoney, OpenAccount, TransferMoney,
    WithdrawMoney, ACCOUNT_DETAILS_ROWS,
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

    // Start subscription in background to populate ACCOUNT_DETAILS_ROWS
    account_details_subscription().start(&executor).await?;

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
    axum::serve(listener, app).await?;

    Ok(())
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
) -> impl IntoResponse {
    let owner_id = Ulid::new().to_string();

    let _ = Command::open_account(
        OpenAccount {
            owner_id,
            owner_name: form.owner_name,
            account_type: AccountType::Checking,
            currency: form.currency,
            initial_balance: form.initial_balance,
        },
        state.executor.as_ref(),
    )
    .await;

    Redirect::to("/accounts")
}

async fn view_account(axum::extract::Path(id): axum::extract::Path<String>) -> Response {
    let accounts = get_all_accounts();
    let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

    match rows.get(&id) {
        Some((view, _, _)) => {
            let account = AccountView {
                id: id.to_owned(),
                balance: view.balance,
                currency: view.currency.to_owned(),
                status: format!("{:?}", view.status),
            };
            drop(rows);
            render(ViewAccountTemplate { account, accounts })
        }
        None => {
            drop(rows);
            Html("<h1>Account not found</h1>".to_owned()).into_response()
        }
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
) -> impl IntoResponse {
    if let Some(account) = bank::load(state.executor.as_ref(), &id)
        .await
        .ok()
        .flatten()
    {
        let _ = account
            .deposit_money(
                DepositMoney {
                    amount: form.amount,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web deposit".to_string(),
                },
                state.executor.as_ref(),
            )
            .await;
    }

    Redirect::to(&format!("/accounts/{}", id))
}

#[derive(Deserialize)]
struct WithdrawForm {
    amount: i64,
}

async fn withdraw(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Form(form): Form<WithdrawForm>,
) -> impl IntoResponse {
    if let Some(account) = bank::load(state.executor.as_ref(), &id)
        .await
        .ok()
        .flatten()
    {
        let _ = account
            .withdraw_money(
                WithdrawMoney {
                    amount: form.amount,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web withdrawal".to_string(),
                },
                state.executor.as_ref(),
            )
            .await;
    }

    Redirect::to(&format!("/accounts/{}", id))
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
) -> impl IntoResponse {
    if let Some(account) = bank::load(state.executor.as_ref(), &id)
        .await
        .ok()
        .flatten()
    {
        let _ = account
            .transfer_money(
                TransferMoney {
                    amount: form.amount,
                    to_account_id: form.to_account_id,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web transfer".to_string(),
                },
                state.executor.as_ref(),
            )
            .await;
    }

    Redirect::to(&format!("/accounts/{}", id))
}

// Helper functions

fn get_all_accounts() -> Vec<AccountView> {
    let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();
    rows.iter()
        .map(|(id, (view, _version, _routing_key))| AccountView {
            id: id.to_owned(),
            balance: view.balance,
            currency: view.currency.to_owned(),
            status: format!("{:?}", view.status),
        })
        .collect()
}
