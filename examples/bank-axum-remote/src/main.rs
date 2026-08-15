//! Bank web app over a **remote** event store (`evento-remote`).
//!
//! Two processes instead of one: a store server owning the Fjall-backed event
//! log, and a web app that talks to it through [`evento::RemoteClient`] — which
//! implements `Executor`, so the commands, projections, and subscription below
//! are identical to the embedded `bank-axum-fjall` example.
//!
//! ```text
//! # Terminal 1 — serve the event store on 127.0.0.1:4321
//! cargo run -p bank-axum-remote -- store
//!
//! # Terminal 2 (3, 4, …) — web app(s) connected to it
//! cargo run -p bank-axum-remote
//! PORT=3001 cargo run -p bank-axum-remote
//! ```
//!
//! Start several web apps: each keeps its own in-memory read model current via
//! a subscription over the remote client — a deposit made through one app shows
//! up in the others without polling, because the store server pushes write
//! notifications to every connected client.

use std::sync::Arc;

use anyhow::Context as _;
use askama::Template;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Form, Router,
};
use bank::{
    account_details, AccountType, Command, DepositMoney, OpenAccount, ReceiveMoney, TransferMoney,
    WithdrawMoney, ACCOUNT_DETAILS_ROWS,
};
use serde::Deserialize;
use ulid::Ulid;

type Executor = evento::RemoteClient;

const STORE_ADDR: &str = "127.0.0.1:4321";

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("store") => run_store().await,
        None => run_web().await,
        Some(other) => anyhow::bail!("unknown mode {other:?}; run with no argument or `store`"),
    }
}

/// The store process: any executor, served over TCP with `evento::remote`.
async fn run_store() -> anyhow::Result<()> {
    // A *persistent* store path (not a temp dir), so events — and therefore
    // accounts — survive a restart. Point this at real durable storage in
    // production; here it's a stable directory under the system temp dir.
    let store_path = std::env::temp_dir().join("bank-axum-remote-store");
    std::fs::create_dir_all(&store_path)?;
    println!("Fjall store: {}", store_path.display());
    let executor = evento::Fjall::open(&store_path)?;

    let listener = tokio::net::TcpListener::bind(STORE_ADDR).await?;
    println!("Serving event store on {STORE_ADDR} (ctrl-c to stop)");
    let handle = evento::remote::serve(listener, executor);

    tokio::signal::ctrl_c().await?;
    handle.shutdown().await;
    Ok(())
}

/// The web process: identical to `bank-axum-fjall`, except the executor is a
/// `RemoteClient` connected to the store server instead of an embedded Fjall.
async fn run_web() -> anyhow::Result<()> {
    let executor: Executor = evento::RemoteClient::connect(STORE_ADDR.parse()?)
        .await
        .with_context(|| {
            format!(
                "connecting to the store server on {STORE_ADDR} — \
                 start it first with `cargo run -p bank-axum-remote -- store`"
            )
        })?;

    // Rebuild this app's read model from the event log and then keep it current.
    // The cache (`ACCOUNT_DETAILS_ROWS`) is in-memory, so it must be re-projected
    // from the start on every boot: a fresh per-boot subscription key has no
    // saved cursor, so the subscription replays all events and then streams new
    // ones live. Writes made through *other* web apps arrive without polling —
    // the store server pushes a notification on every write.
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

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_owned());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    println!("Listening on http://127.0.0.1:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Stop the projection subscription cleanly; dropping the last RemoteClient
    // clone on return then stops the connection actor — nothing else to close.
    subscription.shutdown().await?;
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
    let cmd = Command(state.executor.as_ref().clone());
    let owner_id = Ulid::generate().to_string();

    let id = cmd
        .open_account(OpenAccount {
            owner_id,
            owner_name: form.owner_name,
            account_type: AccountType::Checking,
            currency: form.currency,
            initial_balance: form.initial_balance,
        })
        .await
        .unwrap();

    Redirect::to(&format!("/accounts/{id}"))
}

async fn view_account(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let row = account_details::load(state.executor.as_ref(), &id, "")
        .await
        .unwrap();
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
) -> impl IntoResponse {
    let cmd = Command(state.executor.as_ref().clone());
    let _ = cmd
        .deposit_money(
            &id,
            DepositMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web deposit".to_string(),
            },
        )
        .await;

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
    let cmd = Command(state.executor.as_ref().clone());
    let _ = cmd
        .withdraw_money(
            &id,
            WithdrawMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web withdrawal".to_string(),
            },
        )
        .await;

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
    let cmd = Command(state.executor.as_ref().clone());
    let transfer_tx = Ulid::generate().to_string();

    let _ = cmd
        .transfer_money(
            &id,
            TransferMoney {
                amount: form.amount,
                to_account_id: form.to_account_id.clone(),
                transaction_id: transfer_tx.clone(),
                description: "Web transfer".to_string(),
            },
        )
        .await;

    // Mirror the sender's transfer with a matching receive on the destination,
    // so the recipient's balance projection updates immediately.
    let _ = cmd
        .receive_money(
            &form.to_account_id,
            ReceiveMoney {
                amount: form.amount,
                from_account_id: id.clone(),
                transaction_id: transfer_tx,
                description: "Web transfer".to_string(),
            },
        )
        .await;

    Redirect::to(&format!("/accounts/{}", id))
}

// Helper functions

fn get_all_accounts() -> Vec<AccountView> {
    let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();
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
