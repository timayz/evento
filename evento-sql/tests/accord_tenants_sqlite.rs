//! One SQLite file per tenant, each its own Accord consensus group, all hosted in one
//! process over one `MuxTransport`: a tenant's events and consensus journal land only
//! in that tenant's file, and a read through one tenant's executor never sees the
//! other's events. The shape a multi-tenant deployment uses (see the
//! `bank-axum-accord-tenants` example); here a single-node "cluster" so the test
//! needs no peers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, GroupHost, GroupId, Journal, MuxTransport,
    NodeConfig, NodeId, StaticTopology, SweepConfig, SweepScheduler, Topology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor};
use evento_sql::{Sql, SqlJournal};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool};
use sqlx_migrator::{Migrate, Plan};
use tokio::net::TcpListener;

/// Opens (creating + migrating) one tenant's SQLite file: events, projections,
/// and the consensus journal all live in it.
async fn open_tenant_db(path: &std::path::Path) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .connect_with(opts)
        .await?;
    // The event schema through the canonical migrator…
    let mut conn = pool.acquire().await?;
    evento::sql_migrator::new::<Sqlite>()?
        .run(&mut *conn, &Plan::apply_all())
        .await?;
    drop(conn);
    // …and the journal tables (identical to `AccordMigration`'s, which a deployment
    // runs via `evento-sql-migrator`'s `accord` feature instead).
    SqlJournal::<Sqlite>::new(pool.clone()).migrate().await?;
    Ok(pool)
}

fn event(aggregate_id: &str, version: u16, name: &str) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "tenant/Thing".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: name.into(),
        ..Default::default()
    }
}

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    let sql = match table {
        "event" => "SELECT COUNT(*) FROM event",
        "accord_commands" => "SELECT COUNT(*) FROM accord_commands",
        other => panic!("unexpected table {other}"),
    };
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_tenant_has_its_own_sqlite_file_and_consensus_group() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let id = NodeId(0);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let peers: HashMap<NodeId, std::net::SocketAddr> =
        HashMap::from([(id, listener.local_addr()?)]);

    // One transport, one scheduler, one host per process — shared by every tenant.
    let mux = Arc::new(MuxTransport::new(id, peers));
    let _serve = mux.serve(listener);
    let scheduler = SweepScheduler::new(SweepConfig::default());
    let _sweeps = scheduler.start();
    let host = GroupHost::new(mux, scheduler);

    // Open two tenants: file + pool + Sql + SqlJournal + Node each.
    let mut tenants = Vec::new();
    for (slug, group) in [("acme", GroupId(1)), ("globex", GroupId(2))] {
        let pool = open_tenant_db(&dir.path().join(format!("{slug}.db"))).await?;
        // One `Sql` shared by the data-store bridge and the executor (two `.into()`s
        // from the same pool would not share the write-wake channel).
        let sql: Sql<Sqlite> = pool.clone().into();
        let topology: Arc<dyn Topology> = Arc::new(StaticTopology::new(id, vec![id]));
        let node = host
            .open(
                group,
                topology,
                Arc::new(ExecutorDataStore::new(sql.clone())) as Arc<dyn DataStore>,
                Arc::new(SqlJournal::<Sqlite>::new(pool.clone())) as Arc<dyn Journal>,
                NodeConfig::default(),
            )
            .await?;
        tenants.push((slug, pool, AccordExecutor::new(node, sql)));
    }
    let (_, acme_pool, acme) = &tenants[0];
    let (_, globex_pool, globex) = &tenants[1];

    // Writes go through consensus into each tenant's own file.
    acme.write(vec![event("thing-1", 1, "Created")]).await?;
    acme.write(vec![event("thing-1", 2, "Renamed")]).await?;
    acme.write(vec![event("thing-2", 1, "Created")]).await?;
    globex.write(vec![event("thing-1", 1, "Created")]).await?; // same id, other tenant

    assert_eq!(count(acme_pool, "event").await, 3);
    assert_eq!(count(globex_pool, "event").await, 1);
    // The consensus journal lives in the same per-tenant file.
    assert!(count(acme_pool, "accord_commands").await >= 1);
    assert!(count(globex_pool, "accord_commands").await >= 1);

    // Optimistic concurrency is per tenant: globex's `thing-1` is at version 1, so
    // version 2 is free there even though acme's `thing-1` already has it.
    globex.write(vec![event("thing-1", 2, "Renamed")]).await?;
    assert_eq!(count(globex_pool, "event").await, 2);
    // …and a stale version is refused by that tenant's group.
    let stale = acme.write(vec![event("thing-1", 2, "Dup")]).await;
    assert!(matches!(
        stale,
        Err(evento_core::WriteError::InvalidOriginalVersion)
    ));

    // Reading through a tenant's executor sees only that tenant's events.
    let filters: Arc<[EventFilter]> = Arc::from(vec![EventFilter::by_type_raw("tenant/Thing")]);
    let acme_events = acme
        .read(Some(filters.clone()), None, Args::forward(100, None), None)
        .await?;
    let globex_events = globex
        .read(Some(filters), None, Args::forward(100, None), None)
        .await?;
    assert_eq!(acme_events.edges.len(), 3);
    assert_eq!(globex_events.edges.len(), 2);
    let acme_ids: Vec<_> = acme_events.edges.iter().map(|e| e.node.id).collect();
    assert!(globex_events
        .edges
        .iter()
        .all(|e| !acme_ids.contains(&e.node.id)));

    // Two files on disk, nothing shared.
    assert!(dir.path().join("acme.db").exists());
    assert!(dir.path().join("globex.db").exists());
    assert_eq!(host.len(), 2);
    host.close(GroupId(1));
    host.close(GroupId(2));
    Ok(())
}
