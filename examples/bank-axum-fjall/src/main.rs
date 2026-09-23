//! Bank web app on the **embedded Fjall** store (`evento-fjall`) — no external
//! database, no migrations.
//!
//! Events live in a temp directory (fresh per run); commands are executed
//! through the shared `bank` domain crate, and a projection subscription keeps
//! the in-memory read model (`AccountDetailsView::snapshot_rows()`) current so
//! `/accounts` lists every account.
//!
//! `GET /accounts/{id}/events` is the other half: a **live bridge**. It serves
//! server-sent events from an `.ephemeral().start_from_latest()` subscription
//! started per connection and scoped to that one account, ended by
//! `context.stop()` when the browser disconnects. See `account_events` at the
//! bottom of this file.
//!
//! ```text
//! cargo run -p bank-axum-fjall
//! # then open http://127.0.0.1:3000
//! ```

use std::convert::Infallible;
use std::future::IntoFuture;
use std::sync::Arc;

use askama::Template;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event as SseEvent, KeepAlive, Sse},
        Html, IntoResponse, Redirect, Response,
    },
    routing::{get, post},
    Form, Router,
};
use bank::aggregator::BankAccount;
use bank::{
    account_details, AccountDetailsView, AccountType, Command, DepositMoney, OpenAccount,
    ReceiveMoney, TransferMoney, WithdrawMoney,
};
use evento::subscription::{Context, SubscriptionBuilder};
use evento::Fjall;
use serde::Deserialize;
use tokio::sync::mpsc::error::TrySendError;
use ulid::Ulid;

type Executor = Fjall;

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Without a subscriber, every `tracing` event evento emits — including the
    // error a failing subscription logs on its way out — is a no-op. The
    // default filter keeps evento audible while muting the embedded storage
    // engine; `RUST_LOG=evento_core=debug` overrides it.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fjall=warn,lsm_tree=warn".into()),
        )
        .init();

    // An ephemeral Fjall store: a temp directory owned by the executor and
    // removed once the last clone of it drops, so each run starts clean. Swap
    // for `Fjall::open("./data")` to keep data across runs.
    let executor: Executor = Fjall::temporary()?;
    println!("Fjall store: temporary (removed on exit)");

    // Keep the in-memory read model (`AccountDetailsView::snapshot_rows()`)
    // current, so `/accounts` lists every account. Fjall's in-process write
    // signal wakes the subscription the instant a command commits.
    let subscription = account_details::create_projection()
        .subscription(format!("account-details-{}", Ulid::generate()))
        .any_routing_key()
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
        .route("/accounts/{id}/events", get(account_events))
        .route("/accounts/{id}/deposit", post(deposit))
        .route("/accounts/{id}/withdraw", post(withdraw))
        .route("/accounts/{id}/transfer", post(transfer))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("Listening on http://127.0.0.1:3000");
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .into_future();
    tokio::pin!(server);

    // The projection worker can stop on its own — a handler error (the default
    // is to stop on the first one), or another process taking over its key. If
    // it does, the read model silently freezes, so stop serving stale data.
    tokio::select! {
        res = &mut server => res?,
        reason = subscription.stopped() => {
            tracing::error!(%reason, "projection subscription stopped, shutting down");
        }
    }

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

// Live account feed (SSE)
//
// One ephemeral subscription per open connection. Three opt-ins make that
// affordable, and each is load-bearing:
//
// - `.ephemeral()` keeps the cursor in memory. No subscriber row, no ownership
//   fence, no acknowledge — the store sees only reads. Without it every browser
//   tab would leave a row behind and, because the key is normally a cursor
//   identity, a second tab would fence the first one out.
// - `.start_from_latest()` begins at the head. Without it a connection opened
//   on an account with a long history would have that history replayed into it
//   before it saw anything live.
// - `.aggregate::<BankAccount>(&id)` pushes the filter into the store's read,
//   so this connection never even loads another account's events. That server-
//   side slice is what makes a subscription *per connection* worth its cost; an
//   unfiltered global feed should instead run one subscription fanning out into
//   a `tokio::sync::broadcast` channel.

/// One line of the live feed, as the browser receives it.
#[derive(serde::Serialize)]
struct AccountUpdate {
    kind: &'static str,
    detail: String,
    balance_change: i64,
}

async fn account_events(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    // Bounded, so one stalled client cannot make the worker buffer without
    // limit. The handler below uses `try_send` for the same reason.
    let (tx, rx) = tokio::sync::mpsc::channel::<AccountUpdate>(32);

    let subscription = match SubscriptionBuilder::new("sse-account-feed")
        .handler(forward_to_sse())
        .data(tx)
        .aggregate::<BankAccount>(&id)
        .any_routing_key()
        .ephemeral()
        .start_from_latest()
        .start(state.executor.as_ref())
        .await
    {
        Ok(subscription) => subscription,
        Err(err) => {
            tracing::error!(%err, "could not start the live feed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "live feed unavailable").into_response();
        }
    };

    // `unfold` owns `(rx, subscription)` for the life of the response body, so
    // when the client disconnects axum drops the body, which drops the handle,
    // which stops the worker. For a channel-backed bridge that is the prompt
    // signal and it is all you need: the receiver dies the instant the
    // connection does, whereas a handler can only notice on the *next* event.
    //
    // `context.stop()` in the handler below is still worth having, and is the
    // primary mechanism in the shape this example does not use: one shared
    // subscription fanning out to every client over a `broadcast` channel,
    // where "no receivers left" is a fact only the sending handler can observe.
    let stream = futures_util::stream::unfold((rx, subscription), |(mut rx, sub)| async move {
        let update = rx.recv().await?;
        let event = SseEvent::default()
            .event("account")
            .json_data(update)
            .expect("AccountUpdate serializes");
        Some((Ok::<_, Infallible>(event), (rx, sub)))
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Forwards every event of the watched account to its SSE connection.
///
/// `#[evento::subscription_all]` plus `decode()` gives an exhaustive match, so
/// adding a `BankAccount` variant is a compile error here rather than an update
/// that silently never reaches the browser.
#[evento::subscription_all]
async fn forward_to_sse<E: evento::Executor>(
    context: &Context<'_, E>,
    event: evento::metadata::RawEvent<BankAccount>,
) -> anyhow::Result<()> {
    use bank::aggregator::BankAccountEvent as Ev;

    let update = match event.decode()? {
        Ev::AccountOpened(e) => AccountUpdate {
            kind: "opened",
            detail: format!("{} opened a {:?} account", e.owner_name, e.account_type),
            balance_change: e.initial_balance,
        },
        Ev::MoneyDeposited(e) => AccountUpdate {
            kind: "deposit",
            detail: e.description,
            balance_change: e.amount,
        },
        Ev::MoneyWithdrawn(e) => AccountUpdate {
            kind: "withdrawal",
            detail: e.description,
            balance_change: -e.amount,
        },
        Ev::MoneyTransferred(e) => AccountUpdate {
            kind: "transfer-out",
            detail: format!("to {}", e.to_account_id),
            balance_change: -e.amount,
        },
        Ev::MoneyReceived(e) => AccountUpdate {
            kind: "transfer-in",
            detail: format!("from {}", e.from_account_id),
            balance_change: e.amount,
        },
        Ev::AccountFrozen(e) => AccountUpdate {
            kind: "frozen",
            detail: e.reason,
            balance_change: 0,
        },
        Ev::AccountUnfrozen(e) => AccountUpdate {
            kind: "unfrozen",
            detail: e.reason,
            balance_change: 0,
        },
        Ev::DailyWithdrawalLimitChanged(e) => AccountUpdate {
            kind: "limit",
            detail: format!("daily withdrawal limit is now {}", e.new_limit),
            balance_change: 0,
        },
        Ev::OverdraftLimitChanged(e) => AccountUpdate {
            kind: "limit",
            detail: format!("overdraft limit is now {}", e.new_limit),
            balance_change: 0,
        },
        Ev::AccountClosed(e) => AccountUpdate {
            kind: "closed",
            detail: e.reason,
            balance_change: 0,
        },
    };

    // `try_send`, never `send().await`: awaiting a full channel would block the
    // subscription worker mid-chunk on one slow browser.
    match context
        .try_extract::<tokio::sync::mpsc::Sender<AccountUpdate>>()?
        .try_send(update)
    {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            tracing::warn!("SSE client is behind, dropping an update");
        }
        // The receiver is gone: the response body was dropped, so this client
        // disconnected. Stop now rather than keep reading the store — and keep
        // reporting it as a *normal* end (`StopReason::StoppedByHandler`),
        // which returning an error here would not.
        Err(TrySendError::Closed(_)) => context.stop(),
    }

    Ok(())
}
