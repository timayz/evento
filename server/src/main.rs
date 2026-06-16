//! evento gRPC server daemon.
//!
//! Serves any evento backend over the [`EventStore`] gRPC service. The backend
//! is chosen at startup from environment variables; the gRPC service layer is
//! identical regardless of backend or deployment mode.
//!
//! ## Configuration (environment variables)
//!
//! | Var | Default | Meaning |
//! |-----|---------|---------|
//! | `EVENTO_GRPC_ADDR` | `127.0.0.1:50051` | client-facing gRPC listen address |
//! | `EVENTO_MODE` | `single` | `single` \| `accord-single` \| `accord-cluster` |
//! | `EVENTO_BACKEND` | `fjall` | `fjall` \| `sqlite` \| `postgres` \| `mysql` |
//! | `EVENTO_STORE_PATH` | `./evento-data/store` | fjall event-store path |
//! | `EVENTO_DATABASE_URL` | — | connection URL for SQL backends |
//! | `EVENTO_DEFAULT_ROUTING_KEY` | — | global default routing key (applied to writes that omit one) |
//!
//! Accord cluster mode (`EVENTO_MODE=accord-cluster`, requires the `accord`
//! feature and the `fjall` backend) additionally reads:
//!
//! | Var | Default | Meaning |
//! |-----|---------|---------|
//! | `EVENTO_NODE_ID` | — | this node's id (`u64`) |
//! | `EVENTO_PEERS` | — | cluster membership: `0=host:port,1=host:port,...` (must include this node) |
//! | `EVENTO_JOURNAL_PATH` | `./evento-data/journal` | durable consensus journal path |

use std::net::SocketAddr;

use evento_core::Evento;
use evento_server::{EventStoreServer, EventStoreService};
use tonic::transport::Server;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Single,
    AccordSingle,
    AccordCluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Fjall,
    Sqlite,
    Postgres,
    Mysql,
}

struct Config {
    grpc_addr: SocketAddr,
    mode: Mode,
    backend: Backend,
    store_path: String,
    database_url: Option<String>,
    default_routing_key: Option<String>,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let grpc_addr = env_or("EVENTO_GRPC_ADDR", "127.0.0.1:50051").parse()?;
        let mode = match env_or("EVENTO_MODE", "single").as_str() {
            "single" => Mode::Single,
            "accord-single" => Mode::AccordSingle,
            "accord-cluster" => Mode::AccordCluster,
            other => anyhow::bail!("invalid EVENTO_MODE {other:?}"),
        };
        let backend = match env_or("EVENTO_BACKEND", "fjall").as_str() {
            "fjall" => Backend::Fjall,
            "sqlite" => Backend::Sqlite,
            "postgres" => Backend::Postgres,
            "mysql" => Backend::Mysql,
            other => anyhow::bail!("invalid EVENTO_BACKEND {other:?}"),
        };
        Ok(Self {
            grpc_addr,
            mode,
            backend,
            store_path: env_or("EVENTO_STORE_PATH", "./evento-data/store"),
            database_url: std::env::var("EVENTO_DATABASE_URL").ok(),
            default_routing_key: std::env::var("EVENTO_DEFAULT_ROUTING_KEY").ok(),
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = Config::from_env()?;

    let executor = build_executor(&cfg).await?;
    let executor = match &cfg.default_routing_key {
        Some(key) => executor.default_routing_key(key.clone()),
        None => executor,
    };

    let svc = EventStoreService::new(executor);
    tracing::info!(
        addr = %cfg.grpc_addr,
        mode = ?cfg.mode,
        backend = ?cfg.backend,
        "evento-server listening"
    );

    Server::builder()
        .add_service(EventStoreServer::new(svc))
        .serve(cfg.grpc_addr)
        .await?;
    Ok(())
}

/// Builds the configured backend, type-erased into [`Evento`] so the serve path
/// is uniform across modes.
async fn build_executor(cfg: &Config) -> anyhow::Result<Evento> {
    match cfg.mode {
        Mode::Single => build_single(cfg).await,
        Mode::AccordSingle | Mode::AccordCluster => build_accord(cfg).await,
    }
}

async fn build_single(cfg: &Config) -> anyhow::Result<Evento> {
    match cfg.backend {
        Backend::Fjall => {
            let fjall = open_fjall(&cfg.store_path)?;
            Ok(Evento::new(fjall))
        }
        Backend::Sqlite | Backend::Postgres | Backend::Mysql => build_sql(cfg).await,
    }
}

fn open_fjall(path: &str) -> anyhow::Result<evento_fjall::Fjall> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    evento_fjall::Fjall::open(path)
}

async fn build_sql(cfg: &Config) -> anyhow::Result<Evento> {
    use sqlx_migrator::{Migrate, Plan};

    let url = cfg
        .database_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EVENTO_DATABASE_URL is required for SQL backends"))?;

    match cfg.backend {
        Backend::Sqlite => {
            let pool = sqlx::SqlitePool::connect(url).await?;
            let mut conn = pool.acquire().await?;
            evento_sql_migrator::new::<sqlx::Sqlite>()?
                .run(&mut *conn, &Plan::apply_all())
                .await?;
            drop(conn);
            let executor: evento_sql::Sqlite = pool.into();
            Ok(Evento::new(executor))
        }
        Backend::Postgres => {
            let pool = sqlx::PgPool::connect(url).await?;
            let mut conn = pool.acquire().await?;
            evento_sql_migrator::new::<sqlx::Postgres>()?
                .run(&mut *conn, &Plan::apply_all())
                .await?;
            drop(conn);
            let executor: evento_sql::Postgres = pool.into();
            Ok(Evento::new(executor))
        }
        Backend::Mysql => {
            let pool = sqlx::MySqlPool::connect(url).await?;
            let mut conn = pool.acquire().await?;
            evento_sql_migrator::new::<sqlx::MySql>()?
                .run(&mut *conn, &Plan::apply_all())
                .await?;
            drop(conn);
            let executor: evento_sql::MySql = pool.into();
            Ok(Evento::new(executor))
        }
        Backend::Fjall => unreachable!("fjall handled in build_single"),
    }
}

#[cfg(not(feature = "accord"))]
async fn build_accord(_cfg: &Config) -> anyhow::Result<Evento> {
    anyhow::bail!("Accord modes require building evento-server with the `accord` feature")
}

#[cfg(feature = "accord")]
async fn build_accord(cfg: &Config) -> anyhow::Result<Evento> {
    use std::sync::Arc;

    if cfg.backend != Backend::Fjall {
        anyhow::bail!("Accord modes currently support only the `fjall` backend");
    }
    let local = open_fjall(&cfg.store_path)?;

    use evento_accord::{
        AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, Journal, MessageSink,
        Node, NodeId, StaticTopology, Topology,
    };

    if cfg.mode == Mode::AccordSingle {
        use evento_accord::{InMemoryJournal, InMemoryNetwork};

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
        return Ok(Evento::new(AccordExecutor::new(node, local)));
    }

    // accord-cluster: real TCP transport + durable journal.
    use evento_accord::{serve, TcpTransport};
    use evento_fjall::FjallJournal;

    let id = NodeId(
        std::env::var("EVENTO_NODE_ID")
            .map_err(|_| anyhow::anyhow!("EVENTO_NODE_ID is required for accord-cluster"))?
            .parse::<u64>()?,
    );
    let peers = parse_peers(
        &std::env::var("EVENTO_PEERS")
            .map_err(|_| anyhow::anyhow!("EVENTO_PEERS is required for accord-cluster"))?,
    )
    .await?;
    anyhow::ensure!(
        peers.contains_key(&id),
        "EVENTO_PEERS must contain EVENTO_NODE_ID {id:?}"
    );
    let listen = peers[&id];
    let ids: Vec<NodeId> = {
        let mut ids: Vec<NodeId> = peers.keys().copied().collect();
        ids.sort();
        ids
    };

    let journal_path = env_or("EVENTO_JOURNAL_PATH", "./evento-data/journal");
    if let Some(parent) = std::path::Path::new(&journal_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let journal: Arc<dyn Journal> = Arc::new(FjallJournal::open(&journal_path)?);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    let (inbox_tx, inbox_rx) = tokio::sync::mpsc::channel(1024);
    serve(listener, inbox_tx);

    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, ids)) as Arc<dyn Topology>,
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(TcpTransport::new(id, peers)) as Arc<dyn MessageSink>,
        Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
        journal,
    );
    // Resume consensus + applied state from the durable journal before serving.
    node.recover_state().await?;
    node.start(inbox_rx);
    node.start_recovery();

    tracing::info!(?id, %listen, "accord cluster node up");
    Ok(Evento::new(AccordExecutor::new(node, local)))
}

/// Parses `EVENTO_PEERS` (`id=host:port,…`), resolving each host to a `SocketAddr`.
#[cfg(feature = "accord")]
async fn parse_peers(
    raw: &str,
) -> anyhow::Result<std::collections::HashMap<evento_accord::NodeId, SocketAddr>> {
    use evento_accord::NodeId;

    let mut peers = std::collections::HashMap::new();
    for pair in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = pair
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("bad EVENTO_PEERS entry {pair:?}, want id=host:port"))?;
        let id = NodeId(id.trim().parse::<u64>()?);
        let addr = tokio::net::lookup_host(addr.trim())
            .await?
            .next()
            .ok_or_else(|| anyhow::anyhow!("could not resolve peer address {addr:?}"))?;
        peers.insert(id, addr);
    }
    Ok(peers)
}
