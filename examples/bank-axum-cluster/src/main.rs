//! Bank application running on a 3-node ACCORD cluster.
//!
//! Run each node in a separate terminal:
//! ```
//! cargo run -p bank-axum-cluster -- --node 0
//! cargo run -p bank-axum-cluster -- --node 1
//! cargo run -p bank-axum-cluster -- --node 2
//! ```
//!
//! Then access any node's web interface:
//! - Node 0: http://127.0.0.1:3000
//! - Node 1: http://127.0.0.1:3001
//! - Node 2: http://127.0.0.1:3002
//!
//! All writes go through ACCORD consensus, so you can write to any node
//! and read from any node (with read-your-writes consistency on the writing node).

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
use clap::Parser;
use evento::sql::Sql;
use evento_accord::{
    load_executed_txn_ids, AccordConfig, AccordExecutor, CancellationToken, NodeAddr,
    ShutdownHandle, SqlDurableStore,
};
use serde::Deserialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx_migrator::{Migrate, Plan};
use ulid::Ulid;

type Executor = AccordExecutor<Sql<sqlx::Sqlite>>;

#[derive(Parser)]
#[command(name = "bank-axum-cluster")]
#[command(about = "Bank application running on ACCORD cluster")]
struct Args {
    /// Node ID (0, 1, or 2)
    #[arg(short, long)]
    node: u16,

    /// Clear database on startup (use when restarting nodes)
    #[arg(long)]
    fresh: bool,
}

/// Cluster configuration
const CLUSTER_NODES: &[(&str, u16, u16)] = &[
    // (host, accord_port, http_port)
    ("127.0.0.1", 9000, 3000),
    ("127.0.0.1", 9001, 3001),
    ("127.0.0.1", 9002, 3002),
];

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
    node_id: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("evento_accord=info".parse()?)
                .add_directive("bank_axum_cluster=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let node_id = args.node;

    if node_id as usize >= CLUSTER_NODES.len() {
        anyhow::bail!("Invalid node ID. Must be 0, 1, or 2");
    }

    let (host, accord_port, http_port) = CLUSTER_NODES[node_id as usize];

    tracing::info!("Starting node {} on {}:{}", node_id, host, http_port);

    // Create SQLite database for this node
    let db_path = format!("target/tmp/bank_cluster_node{}.db", node_id);
    std::fs::create_dir_all("target/tmp")?;

    // Clear database if --fresh flag is set
    if args.fresh {
        if std::path::Path::new(&db_path).exists() {
            std::fs::remove_file(&db_path)?;
            tracing::info!("Cleared existing database: {}", db_path);
        }
    }

    let options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    // Run migrations
    let mut conn = pool.acquire().await?;
    let migrator = evento::sql_migrator::new::<sqlx::Sqlite>()?;
    migrator.run(&mut *conn, &Plan::apply_all()).await?;
    drop(conn);

    let inner: Sql<sqlx::Sqlite> = pool.clone().into();

    // Create durable store for crash recovery
    let durable_store = SqlDurableStore::new(pool.clone());

    // Load previously executed transaction IDs
    let executed_ids = load_executed_txn_ids(&pool).await?;
    if !executed_ids.is_empty() {
        tracing::info!(
            "Found {} previously executed transactions",
            executed_ids.len()
        );
    }

    // Build cluster configuration
    let local = NodeAddr {
        id: node_id,
        host: host.to_string(),
        port: accord_port,
    };

    let nodes: Vec<NodeAddr> = CLUSTER_NODES
        .iter()
        .enumerate()
        .map(|(i, (h, p, _))| NodeAddr {
            id: i as u16,
            host: h.to_string(),
            port: *p,
        })
        .collect();

    let config = AccordConfig::cluster(local, nodes);

    // Create AccordExecutor in cluster mode with durable storage
    let (executor, shutdown_handle): (Executor, ShutdownHandle) =
        AccordExecutor::cluster_with_durable(inner, config, Some(durable_store)).await?;

    // Restore executed transactions to skip re-execution
    if !executed_ids.is_empty() {
        let restored = executor.restore_executed(executed_ids.clone()).await;
        tracing::info!("Restored {} executed transactions", restored);
    }

    // Sync with peers to catch up on any transactions missed while down
    // Wait a moment for other nodes to be ready
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    match executor.sync_with_peers(executed_ids).await {
        Ok(synced) if synced > 0 => {
            tracing::info!("Synced {} transactions from peers", synced);
            // Wait for execution workers to process the synced transactions
            // This ensures events are written to DB before subscription starts
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(_) => {
            tracing::debug!("No new transactions to sync from peers");
        }
        Err(e) => {
            tracing::warn!("Failed to sync with peers: {}", e);
        }
    }

    // Recover any missing dependencies (transactions that were committed but have
    // dependencies that don't exist locally)
    match executor.recover_missing_dependencies().await {
        Ok(recovered) if recovered > 0 => {
            tracing::info!("Recovered {} missing dependencies", recovered);
            // Wait for execution
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to recover missing dependencies: {}", e);
        }
    }

    let executor = Arc::new(executor);

    // Start background recovery task to handle any future stuck transactions
    let recovery_cancel = CancellationToken::new();
    let recovery_handle = executor.start_recovery_task(
        std::time::Duration::from_secs(5),
        recovery_cancel.clone(),
    );

    // Start subscription in background to populate ACCOUNT_DETAILS_ROWS
    account_details_subscription()
        .start(executor.as_ref())
        .await?;
    bank::subscription().start(executor.as_ref()).await?;

    let state = AppState { executor, node_id };

    let app = Router::new()
        .route("/", get(index))
        .route("/accounts", get(list_accounts))
        .route("/accounts/new", get(new_account_form).post(create_account))
        .route("/accounts/{id}", get(view_account))
        .route("/accounts/{id}/deposit", post(deposit))
        .route("/accounts/{id}/withdraw", post(withdraw))
        .route("/accounts/{id}/transfer", post(transfer))
        .with_state(state);

    let addr = format!("{}:{}", host, http_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Node {} listening on http://{}", node_id, addr);
    tracing::info!("ACCORD cluster port: {}", accord_port);

    // Handle graceful shutdown
    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
    }

    // Cancel recovery task and wait for it to finish
    recovery_cancel.cancel();
    let _ = recovery_handle.await;

    shutdown_handle.shutdown().await;

    Ok(())
}

// Templates

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    node_id: u16,
}

#[derive(Template)]
#[template(path = "accounts/list.html")]
struct AccountsListTemplate {
    node_id: u16,
    accounts: Vec<AccountView>,
}

#[derive(Template)]
#[template(path = "accounts/new.html")]
struct NewAccountTemplate {
    node_id: u16,
}

#[derive(Template)]
#[template(path = "accounts/view.html")]
struct ViewAccountTemplate {
    node_id: u16,
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

async fn index(State(state): State<AppState>) -> Response {
    render(IndexTemplate {
        node_id: state.node_id,
    })
}

async fn list_accounts(State(state): State<AppState>) -> Response {
    let accounts = get_all_accounts();
    render(AccountsListTemplate {
        node_id: state.node_id,
        accounts,
    })
}

async fn new_account_form(State(state): State<AppState>) -> Response {
    render(NewAccountTemplate {
        node_id: state.node_id,
    })
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

    match Command::open_account(
        OpenAccount {
            owner_id,
            owner_name: form.owner_name,
            account_type: AccountType::Checking,
            currency: form.currency,
            initial_balance: form.initial_balance,
        },
        state.executor.as_ref(),
    )
    .await
    {
        Ok(_) => tracing::info!("Account created via ACCORD consensus"),
        Err(e) => tracing::error!("Failed to create account: {}", e),
    }

    Redirect::to("/accounts")
}

async fn view_account(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
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
            render(ViewAccountTemplate {
                node_id: state.node_id,
                account,
                accounts,
            })
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
        match account
            .deposit_money(
                DepositMoney {
                    amount: form.amount,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web deposit".to_string(),
                },
                state.executor.as_ref(),
            )
            .await
        {
            Ok(_) => tracing::info!("Deposit completed via ACCORD consensus"),
            Err(e) => tracing::error!("Failed to deposit: {}", e),
        }
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
        match account
            .withdraw_money(
                WithdrawMoney {
                    amount: form.amount,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web withdrawal".to_string(),
                },
                state.executor.as_ref(),
            )
            .await
        {
            Ok(_) => tracing::info!("Withdrawal completed via ACCORD consensus"),
            Err(e) => tracing::error!("Failed to withdraw: {}", e),
        }
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
        match account
            .transfer_money(
                TransferMoney {
                    amount: form.amount,
                    to_account_id: form.to_account_id,
                    transaction_id: Ulid::new().to_string(),
                    description: "Web transfer".to_string(),
                },
                state.executor.as_ref(),
            )
            .await
        {
            Ok(_) => tracing::info!("Transfer completed via ACCORD consensus"),
            Err(e) => tracing::error!("Failed to transfer: {}", e),
        }
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
