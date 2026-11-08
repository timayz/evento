//! Bank web app on the **Accord consensus** executor (`evento-cluster`): writes
//! are replicated through the cluster (strictly serializable, leaderless);
//! reads and subscriptions are served from each node's local Fjall store. The
//! node is opened from its settings alone (`Cluster::builder`): the same code
//! runs a node alone and a node of a cluster, and a node that falls behind or
//! loses its store copies it back from a peer at its next start (see
//! `OPERATIONS.md` §2 and §7).
//!
//! Run standalone (single node, in-memory transport, no ports):
//!
//! ```text
//! cargo run -p bank-axum-cluster            # or: make cluster
//! # then open http://127.0.0.1:3000
//! ```
//!
//! Or as a 3-node localhost TCP cluster (`NODE_ID=0..2` → Accord port
//! `7000+id`, web port `3000+id`); writes need a 2-of-3 quorum, so it
//! tolerates one node down:
//!
//! ```text
//! # each in its own terminal…
//! NODE_ID=0 cargo run -p bank-axum-cluster  # or: make cluster.node0
//! NODE_ID=1 cargo run -p bank-axum-cluster  # or: make cluster.node1
//! NODE_ID=2 cargo run -p bank-axum-cluster  # or: make cluster.node2
//! # …or the whole cluster at once:
//! make cluster.all
//! ```
//!
//! A subscription stops on the first handler error unless
//! `.continue_on_error()` is set, and a stopped projection worker means this
//! node keeps answering reads from a read model that no longer updates. `/health`
//! reports that as 503 so an orchestrator depools this replica while the rest of
//! the cluster keeps serving — the other bank examples, which are single
//! processes with nothing to fail over to, exit instead.

use std::net::SocketAddr;
use std::sync::Arc;

use askama::Template;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Form, Router,
};
use bank::{
    account_details, AccountDetailsView, AccountType, Command, DepositMoney, OpenAccount,
    ReceiveMoney, TransferMoney, WithdrawMoney,
};
use evento::cluster::{Cluster, ClusterExecutor, Settings};
use evento::Fjall;
use serde::Deserialize;
use ulid::Ulid;

/// Accord membership for the demo cluster: 3 nodes on localhost by default
/// (override with `CLUSTER_SIZE`). With `N = 2f+1` this tolerates
/// `(N-1)/2` nodes down (writes need a majority quorum).
const DEFAULT_CLUSTER_SIZE: u64 = 3;
const CLUSTER_BASE_PORT: u16 = 7000;
const WEB_BASE_PORT: u16 = 3000;

/// The executor is the Accord consensus engine over a Fjall-backed event store:
/// writes are coordinated through the (here single-node) cluster, reads/snapshots
/// are served from the local backend. The web handlers are identical to the plain
/// Fjall example — `ClusterExecutor` is a drop-in `evento::Executor`.
type Executor = ClusterExecutor<Fjall>;

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
    /// This node, as the host runs it: its health for `/health`.
    cluster: Arc<Cluster>,
    /// The projection worker behind `/health`. Held as an `Arc` because the
    /// handler only observes it — `Subscription::stop_reason` takes `&self`.
    subscription: Arc<evento::subscription::Subscription>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Without a subscriber, every `tracing` event evento emits — including the
    // error a failing subscription logs on its way out — is a no-op. Accord is
    // the workspace's busiest emitter, so this is where it pays most. The
    // default filter mutes the embedded storage engine;
    // `RUST_LOG=evento_cluster=debug` overrides it.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fjall=warn,lsm_tree=warn".into()),
        )
        .init();

    // A Fjall-backed event store. A temp directory keeps each run self-contained;
    // swap for a persistent path to keep data across runs.
    // Single-node by default; set NODE_ID=0..N to join the localhost TCP cluster.
    let node_id = std::env::var("NODE_ID").ok().map(|v| {
        v.parse::<u64>()
            .expect("NODE_ID must be an integer 0..CLUSTER_SIZE")
    });
    let cluster_size = std::env::var("CLUSTER_SIZE")
        .ok()
        .map(|v| {
            v.parse::<u64>()
                .expect("CLUSTER_SIZE must be a positive integer")
        })
        .unwrap_or(DEFAULT_CLUSTER_SIZE);

    let label = node_id
        .map(|n| n.to_string())
        .unwrap_or_else(|| "single".into());
    // A *persistent* data directory (not a temp dir), so events — and
    // therefore accounts — survive a restart. Point this at real durable
    // storage in production; here it's a stable directory under the system
    // temp dir. The store (with its consensus journal inside) is one
    // sub-directory; the host keeps its rejoin mark next to it.
    let data_dir = std::env::temp_dir().join(format!("bank-axum-cluster-{label}"));
    std::fs::create_dir_all(&data_dir)?;
    println!("Data directory: {}", data_dir.display());
    let local = Fjall::open(data_dir.join("store"))?;

    // The `[cluster]` section a deployment would read from its configuration
    // file: here from the environment. `None` runs a node alone.
    let settings = match node_id {
        Some(id) => {
            assert!(id < cluster_size, "NODE_ID must be 0..{cluster_size}");
            let peers: Vec<SocketAddr> = (0..cluster_size)
                .map(|n| {
                    format!("127.0.0.1:{}", CLUSTER_BASE_PORT + n as u16)
                        .parse()
                        .unwrap()
                })
                .collect();
            println!(
                "Cluster node {id} listening on {} ({cluster_size}-node cluster)",
                peers[id as usize]
            );
            Some(Settings::new(id, peers))
        }
        None => {
            println!(
                "single-node mode — set NODE_ID=0..{} for a TCP cluster",
                cluster_size - 1
            );
            None
        }
    };
    let web_port = WEB_BASE_PORT + node_id.unwrap_or(0) as u16;
    let (cluster, executor) = Cluster::builder(local)
        .settings(settings)
        .state_dir(&data_dir)
        .open()
        .await?;

    // Rebuild this node's read model from the event log and then keep it current.
    // The cache (`AccountDetailsView::snapshot_rows()`) is in-memory, so it must be re-projected
    // from the start on every boot. A fresh per-boot subscription key has no saved
    // cursor, so the subscription replays all events from 0 (restoring every
    // account into the cache) and then streams new/replicated events live. (A
    // stable key would instead resume from the last cursor — wrong for a RAM cache
    // that doesn't survive restarts.)
    let subscription = Arc::new(
        account_details::create_projection()
            .subscription(format!("account-details-{}", Ulid::generate()))
            .any_routing_key()
            .start(&executor)
            .await?,
    );

    let state = AppState {
        executor: Arc::new(executor),
        cluster: cluster.clone(),
        subscription: subscription.clone(),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/accounts", get(list_accounts))
        .route("/accounts/new", get(new_account_form).post(create_account))
        .route("/accounts/{id}", get(view_account))
        .route("/accounts/{id}/deposit", post(deposit))
        .route("/accounts/{id}/withdraw", post(withdraw))
        .route("/accounts/{id}/transfer", post(transfer))
        // Ops endpoints (see OPERATIONS.md): Prometheus scrape target + liveness.
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(state);

    let web_addr = format!("127.0.0.1:{web_port}");
    let listener = tokio::net::TcpListener::bind(&web_addr).await?;
    println!("Listening on http://{web_addr}");
    // Unlike the other bank examples, this node does not exit when its
    // projection worker dies — it keeps serving and reports the failure through
    // `/health`, so an orchestrator depools this replica while the rest of the
    // cluster stays up. Log it too, since a probe nobody reads is no better
    // than silence.
    let watched = subscription.clone();
    tokio::spawn(async move {
        let reason = watched.stopped().await;
        if reason.is_failure() {
            tracing::error!(%reason, "projection subscription stopped, /health now fails");
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // `stop` is the `&self` half of `shutdown`, for a handle shared behind an
    // `Arc` like this one.
    subscription.stop();
    cluster.shutdown().await;

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

/// Prometheus scrape target: the node's consensus counters
/// (`cluster_*_total` — writes, fast/slow path, recoveries, compactions, journal
/// flushes, messages, shed) in text exposition format. See OPERATIONS.md.
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.executor.node().metrics().to_prometheus();
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

/// Liveness probe: 200 while the process is up, the node takes writes *and*
/// its projection worker is still running. A node that fell behind or whose
/// journal rolled back refuses writes until it is restarted (the host marked
/// its store for a copy, see OPERATIONS.md §7); a dead worker means this node
/// keeps answering reads from a read model that has stopped updating — up,
/// but not healthy. Readiness (is this node caught up?) is derived from
/// `/metrics` — see OPERATIONS.md.
async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let node = state.cluster.health();
    if !node.serving() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "node not serving (confirmed: {}, rejoin required: {}, rolled back: {}, copy pending: {}): restart it",
                node.confirmed, node.rejoin_required, node.rolled_back, node.copy_pending
            ),
        );
    }
    match state.subscription.stop_reason() {
        None => (StatusCode::OK, "ok".to_owned()),
        Some(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("projection subscription stopped: {reason}"),
        ),
    }
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
        .map(|((_tenant, id), view)| AccountView {
            id: id.to_owned(),
            balance: view.balance,
            currency: view.currency.to_owned(),
            status: format!("{:?}", view.status),
        })
        .collect();
    // The read model is an unordered `HashMap`, whose iteration order is random
    // per process — so without this every node (and every request) would list
    // accounts in a different order. Account ids are ULIDs, so sorting by id is
    // a stable, creation-time order that is identical on every node.
    accounts.sort_by(|a, b| a.id.cmp(&b.id));
    accounts
}
