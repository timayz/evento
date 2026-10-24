//! Two SQLite files per tenant, its events and its consensus journal, each tenant its
//! own Accord consensus group, all hosted in one process over one `MuxTransport`: a
//! tenant's events and journal land only in that tenant's files, and a read through
//! one tenant's executor never sees the other's events. The shape a multi-tenant
//! deployment uses (see the `bank-axum-accord-tenants` example); here a single-node
//! "cluster" so the test needs no peers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, GroupHost, GroupId, Journal, LostJournal,
    MuxTransport, NodeConfig, NodeId, StaticTopology, SweepConfig, SweepScheduler, Topology,
};
use evento_core::{cursor::Args, Event, EventFilter, Executor};
use evento_sql::{Sql, SqlJournal};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool};
use sqlx_migrator::{Migrate, Plan};
use tokio::net::TcpListener;

/// A pool over one SQLite file, created if missing. `synchronous(Full)` on both of
/// a tenant's files: a vote in the journal must survive a power loss, and so must
/// an event the journal records as applied.
async fn connect(path: &std::path::Path, max_connections: u32) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    Ok(SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await?)
}

/// Opens (creating + migrating) one tenant's two SQLite files: `{slug}.db` holds its
/// events and projections, `{slug}.journal.db` its consensus journal. Either can then
/// be lost without the other (see `OPERATIONS.md` §10).
async fn open_tenant_dbs(
    dir: &std::path::Path,
    slug: &str,
) -> anyhow::Result<(SqlitePool, SqlitePool)> {
    // The event schema through the canonical migrator…
    let events = connect(&dir.join(format!("{slug}.db")), 3).await?;
    let mut conn = events.acquire().await?;
    evento::sql_migrator::new::<Sqlite>()?
        .run(&mut *conn, &Plan::apply_all())
        .await?;
    drop(conn);
    // …and the journal tables in a file of their own (identical to
    // `AccordMigration`'s, which a deployment runs alone on that file instead).
    let journal = connect(&dir.join(format!("{slug}.journal.db")), 2).await?;
    SqlJournal::<Sqlite>::new(journal.clone()).migrate().await?;
    Ok((events, journal))
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
async fn each_tenant_has_its_own_sqlite_files_and_consensus_group() -> anyhow::Result<()> {
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

    // Open two tenants: two files + pools + Sql + SqlJournal + Node each.
    let mut tenants = Vec::new();
    for (slug, group) in [("acme", GroupId(1)), ("globex", GroupId(2))] {
        let (events, journal) = open_tenant_dbs(dir.path(), slug).await?;
        // One `Sql` shared by the data-store bridge and the executor (two `.into()`s
        // from the same pool would not share the write-wake channel).
        let sql: Sql<Sqlite> = events.clone().into();
        let topology: Arc<dyn Topology> = Arc::new(StaticTopology::new(id, vec![id]));
        let node = host
            .open(
                group,
                topology,
                Arc::new(ExecutorDataStore::new(sql.clone())) as Arc<dyn DataStore>,
                Arc::new(SqlJournal::<Sqlite>::new(journal.clone())) as Arc<dyn Journal>,
                NodeConfig::default(),
            )
            .await?;
        tenants.push((slug, events, journal, AccordExecutor::new(node, sql)));
    }
    let (_, acme_pool, acme_journal, acme) = &tenants[0];
    let (_, globex_pool, globex_journal, globex) = &tenants[1];

    // Writes go through consensus into each tenant's own file.
    acme.write(vec![event("thing-1", 1, "Created")]).await?;
    acme.write(vec![event("thing-1", 2, "Renamed")]).await?;
    acme.write(vec![event("thing-2", 1, "Created")]).await?;
    globex.write(vec![event("thing-1", 1, "Created")]).await?; // same id, other tenant

    assert_eq!(count(acme_pool, "event").await, 3);
    assert_eq!(count(globex_pool, "event").await, 1);
    // Each tenant's consensus journal lives in its journal file.
    assert!(count(acme_journal, "accord_commands").await >= 1);
    assert!(count(globex_journal, "accord_commands").await >= 1);

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

    // Two files per tenant on disk, nothing shared.
    for slug in ["acme", "globex"] {
        assert!(dir.path().join(format!("{slug}.db")).exists());
        assert!(dir.path().join(format!("{slug}.journal.db")).exists());
    }
    assert_eq!(host.len(), 2);
    host.close(GroupId(1)).await;
    host.close(GroupId(2)).await;
    Ok(())
}

/// A tenant whose journal file was lost while its event file was kept: a plain
/// open refuses the events, since the votes that went with them are gone
/// (`LostJournal`). The tenant must rejoin from a peer instead
/// (`GroupHost::open_joining`). A cluster of this one node is exempt: no other
/// replica holds its votes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_open_refuses_events_whose_journal_file_was_lost() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let id = NodeId(0);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let peers: HashMap<NodeId, std::net::SocketAddr> =
        HashMap::from([(id, listener.local_addr()?)]);
    let mux = Arc::new(MuxTransport::new(id, peers));
    let _serve = mux.serve(listener);
    let scheduler = SweepScheduler::new(SweepConfig::default());
    let _sweeps = scheduler.start();
    let host = GroupHost::new(mux, scheduler);

    // An event file holding an event, and a new journal file: the journal that
    // went with the event was lost.
    let (events, journal) = open_tenant_dbs(dir.path(), "acme").await?;
    let sql: Sql<Sqlite> = events.clone().into();
    sql.write(vec![event("thing-1", 1, "Created")]).await?;
    let open = |topology: Vec<NodeId>| {
        host.open(
            GroupId(1),
            Arc::new(StaticTopology::new(id, topology)) as Arc<dyn Topology>,
            Arc::new(ExecutorDataStore::new(sql.clone())) as Arc<dyn DataStore>,
            Arc::new(SqlJournal::<Sqlite>::new(journal.clone())) as Arc<dyn Journal>,
            NodeConfig::default(),
        )
    };

    let Err(refused) = open(vec![id, NodeId(1), NodeId(2)]).await else {
        panic!("a plain open over a lost journal succeeded");
    };
    assert_eq!(
        refused.downcast_ref::<LostJournal>(),
        Some(&LostJournal {
            join_pending: false
        })
    );
    assert!(!host.is_open(GroupId(1)));

    // Alone in its cluster, the node keeps its store.
    open(vec![id]).await?;
    assert!(host.is_open(GroupId(1)));
    host.close(GroupId(1)).await;
    Ok(())
}
