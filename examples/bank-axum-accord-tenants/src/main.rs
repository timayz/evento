//! Multi-tenant bank on the **Accord consensus** executor with **one SQLite database
//! per tenant** — and one Accord consensus group per tenant, all hosted in this
//! process over a single multiplexed transport.
//!
//! Tenants are created at runtime by users of the application (`POST /tenants`), not
//! by configuration: the catalog is itself an Accord group (`system.db`), so every
//! host learns of a new tenant through replication and opens its database. Writes
//! for `/t/{slug}/…` are coordinated in that tenant's group and land only in
//! `tenants/{slug}.db`; cold tenants are evicted (LRU) and re-opened on demand.
//!
//! Single node (no peers to coordinate):
//!
//! ```text
//! cargo run -p bank-axum-accord-tenants          # or: make tenants
//! # then open http://127.0.0.1:3100
//! ```
//!
//! Or a 3-node localhost TCP cluster (`NODE_ID=0..2` → Accord port `7100+id`, web
//! port `3100+id`); create a tenant on one node, use it on another:
//!
//! ```text
//! make tenants.cluster       # = CLUSTER_SIZE=3 NODE_ID={0,1,2} cargo run -p bank-axum-accord-tenants
//! ```
//!
//! `DATA_DIR` (default: a per-node directory under the system temp dir) holds
//! `system.db` and `tenants/*.db`.

mod catalog;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use askama::Template;
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Form, Router,
};
use bank::{
    account_details, AccountType, Command, DepositMoney, OpenAccount, ReceiveMoney, TransferMoney,
    WithdrawMoney,
};
use evento::cursor::Args;
use evento::{EventFilter, Executor};
use evento_accord::{GroupHost, MuxTransport, NodeId, SweepConfig, SweepScheduler};
use serde::Deserialize;
use ulid::Ulid;

use catalog::{CreateError, TenantExecutor, TenantInfo, TenantRegistry};

const DEFAULT_CLUSTER_SIZE: u64 = 1;
const ACCORD_BASE_PORT: u16 = 7100;
const WEB_BASE_PORT: u16 = 3100;
/// Most tenants open (SQLite file + consensus group) on this host at once.
const MAX_OPEN_TENANTS: usize = 256;

#[derive(Clone)]
struct AppState {
    registry: Arc<TenantRegistry>,
    node_id: NodeId,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    let node_id: u64 = std::env::var("NODE_ID")
        .ok()
        .map(|v| v.parse().expect("NODE_ID must be an integer"))
        .unwrap_or(0);
    let cluster_size: u64 = std::env::var("CLUSTER_SIZE")
        .ok()
        .map(|v| v.parse().expect("CLUSTER_SIZE must be a positive integer"))
        .unwrap_or(DEFAULT_CLUSTER_SIZE);
    assert!(node_id < cluster_size, "NODE_ID must be 0..{cluster_size}");
    let id = NodeId(node_id);
    let ids: Vec<NodeId> = (0..cluster_size).map(NodeId).collect();
    let peers: HashMap<NodeId, SocketAddr> = ids
        .iter()
        .map(|n| {
            let addr = format!("127.0.0.1:{}", ACCORD_BASE_PORT + n.0 as u16)
                .parse()
                .unwrap();
            (*n, addr)
        })
        .collect();
    let data_dir = std::env::var("DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("bank-axum-accord-tenants-{node_id}"))
        });
    std::fs::create_dir_all(&data_dir)?;
    println!("Data dir: {}", data_dir.display());

    // --- Once per process: one connection set, one sweep task, one host. -------
    // The unrouted hook needs the registry, which needs the host, which needs the
    // transport — so the hook looks the registry up once it exists.
    let registry_slot: Arc<OnceLock<Arc<TenantRegistry>>> = Arc::new(OnceLock::new());
    let hook = Arc::clone(&registry_slot);
    let mux = Arc::new(
        MuxTransport::new(id, peers.clone())
            // Frames for a tenant still being opened here wait this long for it.
            .with_pending_buffer(64, 1024, Duration::from_secs(10))
            .with_unrouted_handler(move |group| {
                if let Some(registry) = hook.get() {
                    registry.reopen(group);
                }
            }),
    );
    let listener = tokio::net::TcpListener::bind(peers[&id]).await?;
    mux.serve(listener);
    println!(
        "Accord node {node_id} listening on {} ({cluster_size}-node cluster)",
        peers[&id]
    );
    let scheduler = SweepScheduler::new(SweepConfig::default());
    scheduler.start();
    let host = Arc::new(GroupHost::new(mux, scheduler));

    // --- The catalog (system group) and the tenant registry. -------------------
    let registry = TenantRegistry::new(host, ids, data_dir, MAX_OPEN_TENANTS).await?;
    let _ = registry_slot.set(Arc::clone(&registry));
    let catalog = registry.start_catalog_subscription().await?;

    let state = AppState {
        registry,
        node_id: id,
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/tenants", post(create_tenant))
        .route("/t/{slug}", get(list_accounts))
        .route("/t/{slug}/suspend", post(suspend_tenant))
        .route(
            "/t/{slug}/accounts/new",
            get(new_account_form).post(create_account),
        )
        .route("/t/{slug}/accounts/{id}", get(view_account))
        .route("/t/{slug}/accounts/{id}/deposit", post(deposit))
        .route("/t/{slug}/accounts/{id}/withdraw", post(withdraw))
        .route("/t/{slug}/accounts/{id}/transfer", post(transfer))
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(state);

    let web_addr = format!("127.0.0.1:{}", WEB_BASE_PORT + node_id as u16);
    let listener = tokio::net::TcpListener::bind(&web_addr).await?;
    println!("Listening on http://{web_addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    catalog.stop();
    Ok(())
}

// Templates

struct TenantRow {
    slug: String,
    name: String,
    status: &'static str,
}

impl From<TenantInfo> for TenantRow {
    fn from(t: TenantInfo) -> Self {
        TenantRow {
            slug: t.slug,
            name: t.name,
            status: if t.suspended { "suspended" } else { "active" },
        }
    }
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    tenants: Vec<TenantRow>,
    node: u64,
}

#[derive(Template)]
#[template(path = "tenant/accounts.html")]
struct AccountsTemplate {
    slug: String,
    name: String,
    accounts: Vec<AccountView>,
}

#[derive(Template)]
#[template(path = "tenant/new.html")]
struct NewAccountTemplate {
    slug: String,
}

#[derive(Template)]
#[template(path = "tenant/view.html")]
struct ViewAccountTemplate {
    slug: String,
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

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, msg.into()).into_response()
}

/// Resolves a tenant's executor, or the response to send instead (404 unknown, 423
/// suspended, 500 on an open failure).
async fn tenant(state: &AppState, slug: &str) -> Result<TenantExecutor, (StatusCode, String)> {
    match state.registry.executor(slug).await {
        Ok(Some(executor)) => Ok(executor),
        Ok(None) => Err((StatusCode::NOT_FOUND, format!("no tenant {slug}"))),
        Err(err) if err.to_string().contains("suspended") => {
            Err((StatusCode::LOCKED, err.to_string()))
        }
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

// Tenant handlers

async fn index(State(state): State<AppState>) -> Response {
    render(IndexTemplate {
        tenants: state
            .registry
            .tenants()
            .into_iter()
            .map(Into::into)
            .collect(),
        node: state.node_id.0,
    })
}

#[derive(Deserialize)]
struct CreateTenantForm {
    slug: String,
    name: String,
}

async fn create_tenant(
    State(state): State<AppState>,
    Form(form): Form<CreateTenantForm>,
) -> Response {
    let slug = form.slug.trim().to_lowercase();
    match state.registry.create(&slug, form.name.trim()).await {
        Ok(_) => Redirect::to(&format!("/t/{slug}")).into_response(),
        Err(CreateError::InvalidSlug) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "slug must be 1-32 chars of a-z, 0-9, '-' and start with a letter or digit",
        ),
        Err(CreateError::Taken) => error(
            StatusCode::CONFLICT,
            format!("tenant {slug} already exists"),
        ),
        Err(CreateError::GroupCollision) => error(
            StatusCode::CONFLICT,
            format!("slug {slug} collides with another tenant's group id; pick another"),
        ),
        Err(CreateError::Other(err)) => error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn suspend_tenant(State(state): State<AppState>, Path(slug): Path<String>) -> Response {
    match state
        .registry
        .suspend(&slug, "suspended from the web UI")
        .await
    {
        Ok(()) => Redirect::to("/").into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

// Bank handlers, scoped to a tenant

/// Every account in a tenant's store: the ids of its `AccountOpened` events, each
/// folded through the `account_details` projection.
async fn load_accounts(executor: &TenantExecutor) -> anyhow::Result<Vec<AccountView>> {
    let filters: Arc<[EventFilter]> = Arc::from(vec![EventFilter::by_event::<
        bank::aggregator::AccountOpened,
    >()]);
    let page = executor
        .read(Some(filters), None, Args::forward(500, None), None)
        .await?;
    let mut accounts = Vec::new();
    for edge in page.edges {
        let id = edge.node.aggregate_id;
        if let Some(view) = account_details::load(executor, &id, "").await? {
            accounts.push(AccountView {
                id,
                balance: view.balance,
                currency: view.currency,
                status: format!("{:?}", view.status),
            });
        }
    }
    accounts.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(accounts)
}

async fn list_accounts(State(state): State<AppState>, Path(slug): Path<String>) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let name = state
        .registry
        .info(&slug)
        .map(|t| t.name)
        .unwrap_or_default();
    match load_accounts(&executor).await {
        Ok(accounts) => render(AccountsTemplate {
            slug,
            name,
            accounts,
        }),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn new_account_form(State(state): State<AppState>, Path(slug): Path<String>) -> Response {
    if let Err(resp) = tenant(&state, &slug).await {
        return resp.into_response();
    }
    render(NewAccountTemplate { slug })
}

#[derive(Deserialize)]
struct CreateAccountForm {
    owner_name: String,
    initial_balance: i64,
    currency: String,
}

async fn create_account(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    Form(form): Form<CreateAccountForm>,
) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let cmd = Command(executor);
    match cmd
        .open_account(OpenAccount {
            owner_id: Ulid::generate().to_string(),
            owner_name: form.owner_name,
            account_type: AccountType::Checking,
            currency: form.currency,
            initial_balance: form.initial_balance,
        })
        .await
    {
        Ok(id) => Redirect::to(&format!("/t/{slug}/accounts/{id}")).into_response(),
        Err(err) => error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string()),
    }
}

async fn view_account(
    State(state): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let row = match account_details::load(&executor, &id, "").await {
        Ok(row) => row,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let accounts = match load_accounts(&executor).await {
        Ok(accounts) => accounts,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    match row {
        Some(view) => render(ViewAccountTemplate {
            slug,
            account: AccountView {
                id,
                balance: view.balance,
                currency: view.currency,
                status: format!("{:?}", view.status),
            },
            accounts,
        }),
        None => error(StatusCode::NOT_FOUND, "account not found"),
    }
}

#[derive(Deserialize)]
struct AmountForm {
    amount: i64,
}

async fn deposit(
    State(state): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
    Form(form): Form<AmountForm>,
) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let result = Command(executor)
        .deposit_money(
            &id,
            DepositMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web deposit".to_string(),
            },
        )
        .await;
    match result {
        Ok(_) => Redirect::to(&format!("/t/{slug}/accounts/{id}")).into_response(),
        Err(err) => error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string()),
    }
}

async fn withdraw(
    State(state): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
    Form(form): Form<AmountForm>,
) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let result = Command(executor)
        .withdraw_money(
            &id,
            WithdrawMoney {
                amount: form.amount,
                transaction_id: Ulid::generate().to_string(),
                description: "Web withdrawal".to_string(),
            },
        )
        .await;
    match result {
        Ok(_) => Redirect::to(&format!("/t/{slug}/accounts/{id}")).into_response(),
        Err(err) => error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string()),
    }
}

#[derive(Deserialize)]
struct TransferForm {
    to_account_id: String,
    amount: i64,
}

async fn transfer(
    State(state): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
    Form(form): Form<TransferForm>,
) -> Response {
    let executor = match tenant(&state, &slug).await {
        Ok(e) => e,
        Err(resp) => return resp.into_response(),
    };
    let cmd = Command(executor);
    let transaction_id = Ulid::generate().to_string();
    if let Err(err) = cmd
        .transfer_money(
            &id,
            TransferMoney {
                amount: form.amount,
                to_account_id: form.to_account_id.clone(),
                transaction_id: transaction_id.clone(),
                description: "Web transfer".to_string(),
            },
        )
        .await
    {
        return error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string());
    }
    // Mirror the transfer with a matching receive on the destination (same tenant —
    // accounts never span tenants, since each tenant is its own store).
    if let Err(err) = cmd
        .receive_money(
            &form.to_account_id,
            ReceiveMoney {
                amount: form.amount,
                from_account_id: id.clone(),
                transaction_id,
                description: "Web transfer".to_string(),
            },
        )
        .await
    {
        return error(StatusCode::UNPROCESSABLE_ENTITY, err.to_string());
    }
    Redirect::to(&format!("/t/{slug}/accounts/{id}")).into_response()
}

// Ops

/// Prometheus scrape target: the catalog node's consensus counters, plus process-level
/// gauges for the multi-tenant machinery (tenants known/open, sweep scheduler,
/// transport shed/unrouted frames).
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let node = state.node_id.0.to_string();
    let registry = &state.registry;
    let mut body = registry
        .system()
        .node()
        .metrics()
        .to_prometheus_labeled(&[("node", &node), ("group", "system")]);
    let transport = registry.host().transport().metrics();
    let sweeps = registry.host().scheduler().stats();
    for (name, help, value) in [
        (
            "tenants_known",
            "Tenants in the catalog.",
            registry.tenants().len() as u64,
        ),
        (
            "tenants_open",
            "Tenants open (SQLite file + consensus group) on this host.",
            registry.open_count() as u64,
        ),
        (
            "sweeps_run_total",
            "Node sweeps the shared scheduler started.",
            sweeps.sweeps_run,
        ),
        (
            "sweeps_deferred_total",
            "Due sweeps deferred for want of a slot.",
            sweeps.sweeps_deferred,
        ),
        (
            "transport_messages_shed_total",
            "Outbound frames shed for a full peer queue.",
            transport.messages_shed,
        ),
        (
            "transport_messages_unrouted_total",
            "Inbound frames for an unregistered group that were dropped.",
            transport.messages_unrouted,
        ),
    ] {
        body.push_str(&format!(
            "# HELP tenants_{name} {help}\n# TYPE tenants_{name} {}\ntenants_{name}{{node=\"{node}\"}} {value}\n",
            if name.ends_with("_total") { "counter" } else { "gauge" }
        ));
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
