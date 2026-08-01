use std::collections::HashMap;
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
    account_details, AccountType, Command, DepositMoney, OpenAccount, ReceiveMoney, TransferMoney,
    WithdrawMoney, ACCOUNT_DETAILS_ROWS,
};
use evento::Fjall;
use evento_accord::{
    serve, AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
    InMemoryNetwork, Journal, MessageSink, Node, NodeId, StaticTopology, TcpTransport, Topology,
};
use serde::Deserialize;
use tokio::sync::mpsc;
use ulid::Ulid;

/// Accord membership for the demo cluster: 3 nodes on localhost. With `N = 2f+1`
/// this tolerates one node down (writes need a 2-of-3 quorum).
const CLUSTER_SIZE: u64 = 3;
const ACCORD_BASE_PORT: u16 = 7000;
const WEB_BASE_PORT: u16 = 3000;

/// The executor is the Accord consensus engine over a Fjall-backed event store:
/// writes are coordinated through the (here single-node) cluster, reads/snapshots
/// are served from the local backend. The web handlers are identical to the plain
/// Fjall example — `AccordExecutor` is a drop-in `evento::Executor`.
type Executor = AccordExecutor<Fjall>;

#[derive(Clone)]
struct AppState {
    executor: Arc<Executor>,
}

/// Builds a **single-node** Accord cluster over `local` (a Fjall event store),
/// talking to itself over the in-memory transport. Used when no `NODE_ID` is set
/// — runs standalone with no ports to coordinate.
fn build_single_node(local: Fjall) -> Executor {
    let id = NodeId(0);
    let net = InMemoryNetwork::new();
    let inbox = net.register(id);

    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, vec![id])) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
        Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    node.start(inbox);
    node.start_recovery();

    AccordExecutor::new(node, local)
}

/// Builds one node of a **multi-node** Accord cluster over real TCP. Each node
/// runs in its own process with its own `local` store; they reach each other via
/// `peers` (which includes this node). Writes are coordinated across the cluster
/// and committed once a quorum agrees; reads are served from this node's store.
async fn build_cluster_node(
    id: NodeId,
    listen: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
    local: Fjall,
) -> anyhow::Result<Executor> {
    let ids: Vec<NodeId> = {
        let mut ids: Vec<NodeId> = peers.keys().copied().collect();
        ids.sort();
        ids
    };

    // Inbound: accept peer connections, decode frames, feed this node's inbox.
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    serve(listener, inbox_tx);

    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids)) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        // Outbound: lazily connects to each peer over TCP (swap for `with_tls`
        // + `serve_tls` to encrypt and mutually authenticate inter-node traffic).
        Arc::new(TcpTransport::new(id, peers)) as Arc<dyn MessageSink>,
        Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    node.start(inbox_rx);
    node.start_recovery();

    Ok(AccordExecutor::new(node, local))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A Fjall-backed event store. A temp directory keeps each run self-contained;
    // swap for a persistent path to keep data across runs.
    // Single-node by default; set NODE_ID=0..N to join the localhost TCP cluster.
    let node_id = std::env::var("NODE_ID").ok().map(|v| {
        v.parse::<u64>()
            .expect("NODE_ID must be an integer 0..CLUSTER_SIZE")
    });

    let label = node_id
        .map(|n| n.to_string())
        .unwrap_or_else(|| "single".into());
    // A *persistent* store path (not a temp dir), so events — and therefore
    // accounts — survive a restart. Point this at real durable storage in
    // production; here it's a stable directory under the system temp dir.
    let store_path = std::env::temp_dir().join(format!("bank-axum-accord-{label}"));
    std::fs::create_dir_all(&store_path)?;
    println!("Fjall store: {}", store_path.display());
    let local = Fjall::open(&store_path)?;

    let (executor, web_port) = match node_id {
        Some(id) => {
            assert!(id < CLUSTER_SIZE, "NODE_ID must be 0..{CLUSTER_SIZE}");
            let peers: HashMap<NodeId, SocketAddr> = (0..CLUSTER_SIZE)
                .map(|n| {
                    let addr = format!("127.0.0.1:{}", ACCORD_BASE_PORT + n as u16)
                        .parse()
                        .unwrap();
                    (NodeId(n), addr)
                })
                .collect();
            let listen = peers[&NodeId(id)];
            println!("Accord node {id} listening on {listen} ({CLUSTER_SIZE}-node cluster)");
            let executor = build_cluster_node(NodeId(id), listen, peers, local).await?;
            (executor, WEB_BASE_PORT + id as u16)
        }
        None => {
            println!(
                "single-node mode — set NODE_ID=0..{} for a TCP cluster",
                CLUSTER_SIZE - 1
            );
            (build_single_node(local), WEB_BASE_PORT)
        }
    };

    // Rebuild this node's read model from the event log and then keep it current.
    // The cache (`ACCOUNT_DETAILS_ROWS`) is in-memory, so it must be re-projected
    // from the start on every boot. A fresh per-boot subscription key has no saved
    // cursor, so the subscription replays all events from 0 (restoring every
    // account into the cache) and then streams new/replicated events live. (A
    // stable key would instead resume from the last cursor — wrong for a RAM cache
    // that doesn't survive restarts.)
    let _subscription = account_details::create_projection()
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
        // Ops endpoints (see OPERATIONS.md): Prometheus scrape target + liveness.
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(state);

    let web_addr = format!("127.0.0.1:{web_port}");
    let listener = tokio::net::TcpListener::bind(&web_addr).await?;
    println!("Listening on http://{web_addr}");
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

/// Prometheus scrape target: the node's consensus counters
/// (`accord_*_total` — writes, fast/slow path, recoveries, compactions, journal
/// flushes, messages, shed) in text exposition format. See OPERATIONS.md.
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.executor.node().metrics().to_prometheus();
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

/// Liveness probe: 200 while the process is up. Readiness (is this node caught up
/// and serving fresh reads?) is derived from `/metrics` — see OPERATIONS.md.
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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
    // per process — so without this every node (and every request) would list
    // accounts in a different order. Account ids are ULIDs, so sorting by id is
    // a stable, creation-time order that is identical on every node.
    accounts.sort_by(|a, b| a.id.cmp(&b.id));
    accounts
}
