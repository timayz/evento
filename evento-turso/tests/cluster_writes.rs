//! A node alone over a file store: what its writes cost in syncs (#530).
//! Gated by the `cluster` feature.

#![cfg(feature = "cluster")]

use std::sync::Arc;
use std::time::Instant;

use evento_cluster::{Cluster, ClusterExecutor};
use evento_core::cursor::Args;
use evento_core::{Event, EventFilter, Executor, TenantFilter};
use evento_turso::Turso;

fn event(id: String) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_id: id,
        aggregate_type: "sizing/Order".to_owned(),
        version: 1,
        name: "Placed".to_owned(),
        ..Default::default()
    }
}

/// `writers` tasks write `events` single-event streams between them.
async fn write(executor: &ClusterExecutor<Turso>, writers: usize, events: usize) {
    let mut tasks = Vec::new();
    for writer in 0..writers {
        let executor = executor.clone();
        tasks.push(tokio::spawn(async move {
            for n in 0..events / writers {
                executor
                    .write(vec![event(format!("order-{writer}-{n}"))])
                    .await
                    .expect("write");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer panicked");
    }
}

/// The writes decided together on a node alone are applied in one
/// transaction of the store (#530), and every one of them is stored. (What a
/// write costs the journal is timed by the lazy flusher and the sweep, so it
/// is counted in `evento-cluster`'s own tests, on a journal that counts.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_alone_applies_concurrent_writes_together() -> anyhow::Result<()> {
    const EVENTS: usize = 200;
    let store = Turso::temporary_concurrent().await?;
    let (cluster, executor) = Cluster::builder(store.clone()).open().await?;
    executor.write(vec![event("warm".to_owned())]).await?;

    let before = cluster.node().metrics();
    write(&executor, 20, EVENTS).await;
    let after = cluster.node().metrics();
    assert!(
        after.applies_batched - before.applies_batched > 0,
        "no apply shared a write of the store"
    );

    let filter: Arc<[EventFilter]> = Arc::from([EventFilter::by_type_raw("sizing/Order")]);
    let page = store
        .read(
            Some(filter),
            None,
            TenantFilter::All,
            Args::forward(1000, None),
            None,
        )
        .await?;
    assert_eq!(page.edges.len(), 1 + EVENTS, "every write is stored");
    cluster.shutdown().await;
    Ok(())
}

/// Not a test: prints what `SIZING_WRITERS` writers (default 1) of
/// `SIZING_EVENTS` single-event writes in all (default 1,000) cost. Run it
/// under `strace -f -c -e trace=fsync,fdatasync` to count the syncs:
///
/// ```text
/// cargo test --all-features -p evento-turso --test cluster_writes -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement, not a test"]
async fn sizing() -> anyhow::Result<()> {
    let var = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let writers = var("SIZING_WRITERS", 1);
    let events = var("SIZING_EVENTS", 1_000);
    let dir = match std::env::var("SIZING_DIR") {
        Ok(dir) => tempfile::Builder::new().prefix("sizing-").tempdir_in(dir)?,
        Err(_) => tempfile::Builder::new().prefix("sizing-").tempdir()?,
    };
    let store = Turso::open_concurrent(dir.path().join("events.db")).await?;
    let (cluster, executor) = Cluster::builder(store).open().await?;

    // The first write pays the clock bound and the caches.
    executor.write(vec![event("warm".to_owned())]).await?;
    let before = cluster.node().metrics();
    let started = Instant::now();
    write(&executor, writers, events).await;
    let elapsed = started.elapsed();
    let after = cluster.node().metrics();
    let written = (events / writers * writers) as f64;
    println!(
        "{writers} writers, {written} events in {elapsed:.2?}: {:.0} events/s, {:.2} journal flushes per event",
        written / elapsed.as_secs_f64(),
        (after.journal_flushes - before.journal_flushes) as f64 / written,
    );
    cluster.shutdown().await;
    Ok(())
}
