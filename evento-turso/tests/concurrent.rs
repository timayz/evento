//! The store in [`WriteMode::Concurrent`]: the shared conformance suite over
//! an in-memory database in Turso's MVCC mode (every store write a
//! `BEGIN CONCURRENT` transaction), then what the mode is for — the store,
//! its journal and an application's own tables written at the same time, in
//! the same database, without `Busy`, and a conflict between two of the
//! application's transactions retried until both are in.

use std::sync::Arc;

use evento_core::{Event, Executor as _};
use evento_turso::{Turso, WriteMode};

/// The `version`th event of the stream `acc`.
fn bump(version: u16) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: "test/Account".into(),
        aggregate_id: "acc".into(),
        version,
        name: "Bumped".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn concurrent_routing_key() -> anyhow::Result<()> {
    evento_test::routing_key(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_load() -> anyhow::Result<()> {
    evento_test::load(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_load_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::load_multiple_aggregator(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_load_with_snapshot() -> anyhow::Result<()> {
    evento_test::load_with_snapshot(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_invalid_original_version() -> anyhow::Result<()> {
    evento_test::invalid_original_version(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscriber_running() -> anyhow::Result<()> {
    evento_test::subscriber_running(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe() -> anyhow::Result<()> {
    evento_test::subscribe(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_low_latency() -> anyhow::Result<()> {
    evento_test::subscribe_low_latency(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_routing_key() -> anyhow::Result<()> {
    evento_test::subscribe_routing_key(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_default() -> anyhow::Result<()> {
    evento_test::subscribe_default(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_multiple_aggregator(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_co_keyed_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_co_keyed_aggregator(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_load_co_keyed_aggregator() -> anyhow::Result<()> {
    evento_test::load_co_keyed_aggregator(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_routing_key_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_routing_key_multiple_aggregator(&Turso::in_memory_concurrent().await?)
        .await
}

#[tokio::test]
async fn concurrent_subscribe_default_multiple_aggregator() -> anyhow::Result<()> {
    evento_test::subscribe_default_multiple_aggregator(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_default_routing_key() -> anyhow::Result<()> {
    evento_test::subscribe_default_routing_key(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscribe_default_routing_key_all_isolation() -> anyhow::Result<()> {
    evento_test::subscribe_default_routing_key_all_isolation(&Turso::in_memory_concurrent().await?)
        .await
}

#[tokio::test]
async fn concurrent_all_commands() -> anyhow::Result<()> {
    evento_test::all_commands(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_order_timestamp() -> anyhow::Result<()> {
    evento_test::read_order_timestamp(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_write_restamps_client_clock() -> anyhow::Result<()> {
    evento_test::write_restamps_client_clock(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_exact_filter() -> anyhow::Result<()> {
    evento_test::exact_filter(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_concurrent_append() -> anyhow::Result<()> {
    evento_test::concurrent_append(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_strict_unhandled() -> anyhow::Result<()> {
    evento_test::strict_unhandled(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tombstone() -> anyhow::Result<()> {
    evento_test::tombstone(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_data() -> anyhow::Result<()> {
    evento_test::subscription_data(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_stop_reason() -> anyhow::Result<()> {
    evento_test::subscription_stop_reason(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_shutdown_acknowledges_in_flight() -> anyhow::Result<()> {
    evento_test::subscription_shutdown_acknowledges_in_flight(&Turso::in_memory_concurrent().await?)
        .await
}

#[tokio::test]
async fn concurrent_subscription_all_counts() -> anyhow::Result<()> {
    evento_test::subscription_all_counts(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_snapshot_revision_scope() -> anyhow::Result<()> {
    evento_test::snapshot_revision_scope(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_snapshot_projection_scope() -> anyhow::Result<()> {
    evento_test::snapshot_projection_scope(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_load() -> anyhow::Result<()> {
    evento_test::upcast_load(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_nearest_target() -> anyhow::Result<()> {
    evento_test::upcast_nearest_target(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_explicit_wins() -> anyhow::Result<()> {
    evento_test::upcast_explicit_wins(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_skip() -> anyhow::Result<()> {
    evento_test::upcast_skip(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_strict_unhandled_target() -> anyhow::Result<()> {
    evento_test::upcast_strict_unhandled_target(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_subscription() -> anyhow::Result<()> {
    evento_test::upcast_subscription(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_projection_subscription() -> anyhow::Result<()> {
    evento_test::upcast_projection_subscription(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_tombstone() -> anyhow::Result<()> {
    evento_test::upcast_tombstone(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_upcast_has_event() -> anyhow::Result<()> {
    evento_test::upcast_has_event(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_stream() -> anyhow::Result<()> {
    evento_test::read_stream(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_drains_pages() -> anyhow::Result<()> {
    evento_test::read_drains_pages(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_write_validates_every_event() -> anyhow::Result<()> {
    evento_test::write_validates_every_event(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_limit() -> anyhow::Result<()> {
    evento_test::read_limit(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_page_cursor() -> anyhow::Result<()> {
    evento_test::read_page_cursor(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_decode() -> anyhow::Result<()> {
    evento_test::read_decode(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_read_routing_key() -> anyhow::Result<()> {
    evento_test::read_routing_key(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_start_from_latest() -> anyhow::Result<()> {
    evento_test::subscription_start_from_latest(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_ephemeral() -> anyhow::Result<()> {
    evento_test::subscription_ephemeral(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_ephemeral_start_from_latest() -> anyhow::Result<()> {
    evento_test::subscription_ephemeral_start_from_latest(&Turso::in_memory_concurrent().await?)
        .await
}

#[tokio::test]
async fn concurrent_subscription_ephemeral_concurrent_same_key() -> anyhow::Result<()> {
    evento_test::subscription_ephemeral_concurrent_same_key(&Turso::in_memory_concurrent().await?)
        .await
}

#[tokio::test]
async fn concurrent_subscription_context_stop() -> anyhow::Result<()> {
    evento_test::subscription_context_stop(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_live() -> anyhow::Result<()> {
    evento_test::subscription_live(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_subscription_ephemeral_run_once() -> anyhow::Result<()> {
    evento_test::subscription_ephemeral_run_once(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_replicate_large_batch() -> anyhow::Result<()> {
    evento_test::replicate_large_batch(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tenant_streams_are_isolated() -> anyhow::Result<()> {
    evento_test::tenant_streams_are_isolated(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tenant_subscriptions() -> anyhow::Result<()> {
    evento_test::tenant_subscriptions(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tenant_projection_subscription() -> anyhow::Result<()> {
    evento_test::tenant_projection_subscription(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tenant_projection_write_gateway() -> anyhow::Result<()> {
    evento_test::tenant_projection_write_gateway(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_tenant_snapshots_are_isolated() -> anyhow::Result<()> {
    evento_test::tenant_snapshots_are_isolated(&Turso::in_memory_concurrent().await?).await
}

#[tokio::test]
async fn concurrent_delete_stream() -> anyhow::Result<()> {
    evento_test::delete_stream(&Turso::in_memory_concurrent().await?).await
}

/// `in_memory` and `open` leave the database in Turso's default mode;
/// the concurrent constructors record MVCC in the file, where a later plain
/// `open` reads it back.
#[tokio::test]
async fn the_mode_is_read_from_the_database() -> anyhow::Result<()> {
    assert_eq!(Turso::in_memory().await?.write_mode(), WriteMode::Serial);
    assert_eq!(
        Turso::in_memory_concurrent().await?.write_mode(),
        WriteMode::Concurrent
    );

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.db");
    let store = Turso::open_concurrent(&path).await?;
    assert_eq!(store.write_mode(), WriteMode::Concurrent);
    drop(store);
    let reopened = Turso::open(&path).await?;
    assert_eq!(reopened.write_mode(), WriteMode::Concurrent);
    // A clone and `from_database` see the same mode.
    assert_eq!(reopened.clone().write_mode(), WriteMode::Concurrent);
    let wrapped = Turso::from_database(reopened.database().clone()).await?;
    assert_eq!(wrapped.write_mode(), WriteMode::Concurrent);
    Ok(())
}

const APP_SCHEMA: &str = "CREATE TABLE counter (id TEXT NOT NULL PRIMARY KEY, n INTEGER NOT NULL);
INSERT INTO counter (id, n) VALUES ('hits', 0);
CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL);";

async fn counter(store: &Turso) -> anyhow::Result<i64> {
    let conn = store.connect()?;
    let mut rows = conn
        .query("SELECT n FROM counter WHERE id = 'hits'", ())
        .await?;
    let row = rows.next().await?.expect("the counter row");
    Ok(row.get::<i64>(0)?)
}

/// The application's tables are written through `Turso::transaction` while
/// the store writes events on its own connection: in MVCC mode neither gets
/// `Busy`, and everything written is there.
#[tokio::test]
async fn application_writes_run_alongside_the_store() -> anyhow::Result<()> {
    let store = Turso::in_memory_concurrent().await?;
    store.connect()?.execute_batch(APP_SCHEMA).await?;

    let writer = {
        let store = store.clone();
        tokio::spawn(async move {
            for version in 1..=200u16 {
                store.write(vec![bump(version)]).await?;
            }
            anyhow::Ok(())
        })
    };
    let app = {
        let store = store.clone();
        tokio::spawn(async move {
            for i in 0..200i64 {
                store
                    .transaction(&i, |tx, i| {
                        Box::pin(async move {
                            tx.execute("UPDATE counter SET n = n + 1 WHERE id = 'hits'", ())
                                .await?;
                            tx.execute(
                                "INSERT INTO audit (note) VALUES (?1)",
                                (format!("write {i}"),),
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await?;
            }
            anyhow::Ok(())
        })
    };
    writer.await??;
    app.await??;

    assert_eq!(counter(&store).await?, 200);
    assert_eq!(
        store
            .latest_version(String::new(), "test/Account".into(), "acc".into())
            .await?,
        200
    );
    Ok(())
}

/// Several of the application's transactions change the same row at once:
/// the losers of each write-write conflict run again, and every increment
/// lands.
#[tokio::test]
async fn conflicting_transactions_are_retried() -> anyhow::Result<()> {
    let store = Turso::in_memory_concurrent().await?;
    store.connect()?.execute_batch(APP_SCHEMA).await?;

    let store = Arc::new(store);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            for _ in 0..25 {
                store
                    .transaction(&(), |tx, ()| {
                        Box::pin(async move {
                            let mut rows = tx
                                .query("SELECT n FROM counter WHERE id = 'hits'", ())
                                .await?;
                            let n: i64 = rows.next().await?.expect("row").get(0)?;
                            drop(rows);
                            tx.execute("UPDATE counter SET n = ?1 WHERE id = 'hits'", (n + 1,))
                                .await?;
                            Ok(())
                        })
                    })
                    .await?;
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(counter(&store).await?, 200);
    Ok(())
}

/// A body that fails for a reason of its own is not retried, is rolled
/// back, and leaves the connection usable.
#[tokio::test]
async fn a_failing_body_rolls_back_once() -> anyhow::Result<()> {
    let store = Turso::in_memory_concurrent().await?;
    store.connect()?.execute_batch(APP_SCHEMA).await?;

    let attempts = std::sync::atomic::AtomicU32::new(0);
    let err = store
        .transaction::<(), _, _>(&attempts, |tx, attempts| {
            Box::pin(async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tx.execute("UPDATE counter SET n = 99 WHERE id = 'hits'", ())
                    .await?;
                anyhow::bail!("the body gave up")
            })
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("gave up"), "{err}");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(counter(&store).await?, 0, "rolled back");

    // The same connection goes on to the next transaction.
    let mut conn = store.connect()?;
    let first =
        evento_turso::transaction::<(), _, _>(&mut conn, store.write_mode(), &(), |tx, ()| {
            Box::pin(async move {
                tx.execute("UPDATE counter SET n = 1 WHERE id = 'hits'", ())
                    .await?;
                anyhow::bail!("again")
            })
        })
        .await;
    assert!(first.is_err());
    evento_turso::transaction(&mut conn, store.write_mode(), &(), |tx, ()| {
        Box::pin(async move {
            tx.execute("UPDATE counter SET n = 2 WHERE id = 'hits'", ())
                .await?;
            Ok(())
        })
    })
    .await?;
    assert_eq!(counter(&store).await?, 2);
    Ok(())
}
