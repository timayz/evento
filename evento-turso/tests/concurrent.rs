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
async fn concurrent_dead_letters() -> anyhow::Result<()> {
    evento_test::dead_letters(&Turso::in_memory_concurrent().await?).await
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

/// Not a test: what idle subscriptions cost a store in MVCC mode under
/// writes (#536). `cargo test -p evento-turso --test concurrent sizing -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a test"]
async fn sizing_idle_subscriptions() -> anyhow::Result<()> {
    evento_test::sizing_idle_subscriptions(&Turso::in_memory_concurrent().await?).await
}

/// One store of the [`sizing_mvcc_checkpoints`] measurement.
struct Sized {
    name: String,
    /// `PRAGMA mvcc_checkpoint_threshold`, when not the store's default
    /// (`CHECKPOINT_BYTES`).
    threshold: Option<i64>,
    /// Turso's experimental passive checkpoint.
    passive: bool,
}

impl Sized {
    /// `default`, a threshold in bytes (`-1`: no automatic checkpoint), or
    /// either after `passive:`.
    fn parse(spec: &str) -> Self {
        let (passive, threshold) = match spec.strip_prefix("passive") {
            Some(rest) => (true, rest.trim_start_matches(':')),
            None => (false, spec),
        };
        Self {
            name: spec.to_owned(),
            threshold: threshold.parse().ok(),
            passive,
        }
    }
}

/// A migrated MVCC store at `path` as `sized` describes it.
async fn sized_store(path: &str, sized: &Sized) -> anyhow::Result<Turso> {
    let db = Turso::builder(path)
        .experimental_mvcc_passive_checkpoint(sized.passive)
        .build()
        .await?;
    evento_turso::enable_concurrent_writes(&db).await?;
    let mut conn = db.connect()?;
    evento_turso::MIGRATIONS
        .to_latest(&mut conn)
        .await
        .map_err(|(applied, err)| anyhow::anyhow!("migration failed after {applied}: {err}"))?;
    let store = Turso::from_database(db).await?;
    if let Some(threshold) = sized.threshold {
        store
            .checkpoint_every(u64::try_from(threshold).ok())
            .await?;
    }
    Ok(store)
}

/// Not a test: what Turso's MVCC checkpoints cost a store under writes
/// (#544). `SIZING_WRITERS` writers (default 20) write `SIZING_EVENTS`
/// single-event streams (default 20,000) while `SIZING_READERS` readers
/// (default 8) each read one stream every 20 ms and a probe asks, every
/// half millisecond, whether a transaction can begin. Printed for each
/// store: the time the writes took, what a read waited, and the spans the
/// probe found the store closed for — the checkpoints, counted and timed —
/// on an in-memory store and on a file. `SIZING_STORES` lists the stores
/// (`default` for the store's own threshold, a checkpoint threshold in
/// bytes — 4120000 is Turso's —, `-1` for no automatic checkpoint, and
/// `passive`, alone or before `:` and a threshold, for Turso's experimental
/// passive checkpoint) and `SIZING_ON` where (`memory`, `file`).
/// `cargo test -p evento-turso --test concurrent sizing_mvcc -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a test"]
async fn sizing_mvcc_checkpoints() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use evento_core::cursor::Args;
    use evento_core::{EventFilter, TenantFilter};

    fn setting(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
    fn millis(d: Duration) -> f64 {
        d.as_secs_f64() * 1000.0
    }
    let (writers, events, readers) = (
        setting("SIZING_WRITERS", 20),
        setting("SIZING_EVENTS", 20_000),
        setting("SIZING_READERS", 8),
    );
    let stores: Vec<Sized> = std::env::var("SIZING_STORES")
        .unwrap_or_else(|_| "default,4120000,1048576,-1,passive".to_owned())
        .split(',')
        .map(Sized::parse)
        .collect();
    let on = std::env::var("SIZING_ON").unwrap_or_else(|_| "memory,file".to_owned());
    for on_file in [false, true] {
        if !on.contains(if on_file { "file" } else { "memory" }) {
            continue;
        }
        for sized in &stores {
            let dir = tempfile::Builder::new()
                .prefix("evento-sizing-")
                .tempdir()?;
            let path = if on_file {
                dir.path().join("events.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let store = sized_store(&path, sized).await?;
            store.write(vec![bump(1)]).await?;

            let done = Arc::new(AtomicBool::new(false));
            let mut reading = Vec::new();
            for _ in 0..readers {
                let (store, done) = (store.clone(), done.clone());
                reading.push(tokio::spawn(async move {
                    let mut waits = Vec::new();
                    while !done.load(Ordering::Acquire) {
                        let started = Instant::now();
                        store
                            .read(
                                Some([EventFilter::by_id_raw("test/Account", "acc")].into()),
                                None,
                                TenantFilter::All,
                                Args::forward(10, None),
                                None,
                            )
                            .await
                            .expect("read");
                        waits.push(started.elapsed());
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    waits
                }));
            }
            // The spans in which no transaction can begin.
            let probing = {
                let (conn, done) = (store.connect_read()?, done.clone());
                tokio::spawn(async move {
                    let mut closed: Vec<Duration> = Vec::new();
                    let mut since: Option<Instant> = None;
                    while !done.load(Ordering::Acquire) {
                        let busy = match conn.query("SELECT 1 FROM event LIMIT 1", ()).await {
                            Ok(mut rows) => matches!(rows.next().await, Err(turso::Error::Busy(_))),
                            Err(turso::Error::Busy(_)) => true,
                            Err(err) => panic!("probe: {err}"),
                        };
                        match (busy, since) {
                            (true, None) => since = Some(Instant::now()),
                            (false, Some(at)) => {
                                closed.push(at.elapsed());
                                since = None;
                            }
                            _ => {}
                        }
                        tokio::time::sleep(Duration::from_micros(500)).await;
                    }
                    closed
                })
            };

            let started = Instant::now();
            let mut writing = Vec::new();
            for writer in 0..writers {
                let store = store.clone();
                writing.push(tokio::spawn(async move {
                    for n in 0..events / writers {
                        let event = Event {
                            id: ulid::Ulid::generate(),
                            aggregate_type: "sizing/Order".into(),
                            aggregate_id: format!("{writer}-{n}"),
                            version: 1,
                            name: "Placed".into(),
                            ..Default::default()
                        };
                        store.write(vec![event]).await.expect("write");
                    }
                }));
            }
            for task in writing {
                task.await?;
            }
            let wrote = started.elapsed();
            done.store(true, Ordering::Release);
            let mut waits = Vec::new();
            for task in reading {
                waits.extend(task.await?);
            }
            waits.sort();
            let closed = probing.await?;

            let mean = waits.iter().sum::<Duration>() / waits.len().max(1) as u32;
            let p99 = waits[(waits.len() * 99 / 100).min(waits.len() - 1)];
            let slow = waits
                .iter()
                .filter(|w| **w > Duration::from_millis(5))
                .count();
            let shut: Duration = closed.iter().sum();
            let mut line = format!(
                "{:<9} {:<26} writes {:>6.2?} | read mean {:>7.2} ms p99 {:>7.2} max {:>7.2}, {:>3}% over 5 ms | closed {:>3} times, {:>5.0} ms in all ({:>2.0}% of the run), longest {:>6.1} ms",
                if on_file { "file" } else { "in-memory" },
                sized.name,
                wrote,
                millis(mean),
                millis(p99),
                millis(*waits.last().unwrap()),
                slow * 100 / waits.len(),
                closed.len(),
                millis(shut),
                shut.as_secs_f64() * 100.0 / wrote.as_secs_f64(),
                millis(closed.iter().max().copied().unwrap_or_default()),
            );
            if on_file {
                // What the store left on disk, and what opening it again costs.
                drop(store);
                let mut sizes = Vec::new();
                for entry in std::fs::read_dir(dir.path())? {
                    let entry = entry?;
                    sizes.push(format!(
                        "{} {:.1} MB",
                        entry.file_name().to_string_lossy(),
                        entry.metadata()?.len() as f64 / 1e6
                    ));
                }
                sizes.sort();
                let started = Instant::now();
                let reopened = sized_store(&path, sized).await?;
                let opened = started.elapsed();
                drop(reopened);
                line.push_str(&format!(" | reopen {opened:.2?} | {}", sizes.join(", ")));
            }
            println!("{line}");
        }
    }
    Ok(())
}
