//! A node alone over a file store: what its writes cost in syncs (#530).
//! Gated by the `cluster` feature.

#![cfg(feature = "cluster")]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use evento_cluster::host::Settings;
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
/// `SIZING_EVENTS` single-event writes in all (default 1,000) cost
/// `SIZING_NODES` nodes (default 1; three talk over TCP on localhost, each
/// over a store of its own under `SIZING_DIR`, the writers spread over
/// them). With `SIZING_RATE`, each writer writes that many events a second
/// at most. Run it under `strace` to count the syncs of each node's log
/// (#530, #561):
///
/// ```text
/// cargo test --features cluster -p evento-turso --test cluster_writes --no-run
/// strace -f -y -e trace=fsync,fdatasync -o syncs.txt <the test binary> --ignored --nocapture
/// grep -c 'node0/events.db-log' syncs.txt
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
    let nodes = var("SIZING_NODES", 1);
    let rate = var("SIZING_RATE", 0);
    let dir = match std::env::var("SIZING_DIR") {
        Ok(dir) => tempfile::Builder::new().prefix("sizing-").tempdir_in(dir)?,
        Err(_) => tempfile::Builder::new().prefix("sizing-").tempdir()?,
    };
    let mut listeners = Vec::new();
    for _ in 0..nodes {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let addrs: Vec<SocketAddr> = listeners
        .iter()
        .map(|listener| listener.local_addr())
        .collect::<Result<_, _>>()?;
    drop(listeners);
    let mut opens = Vec::new();
    for id in 0..nodes {
        let node_dir = dir.path().join(format!("node{id}"));
        std::fs::create_dir_all(&node_dir)?;
        let addrs = addrs.clone();
        opens.push(async move {
            let store = Turso::open_concurrent(node_dir.join("events.db")).await?;
            let settings = (addrs.len() > 1).then(|| Settings::new(id as u64, addrs));
            Cluster::builder(store)
                .settings(settings)
                .state_dir(&node_dir)
                .open()
                .await
        });
    }
    let mut members = Vec::new();
    for opened in futures_all(opens).await {
        members.push(opened?);
    }
    for (cluster, _) in &members {
        cluster.until_confirmed(Duration::from_secs(30)).await?;
    }

    // The first write pays the clock bound and the caches.
    for (n, (_, executor)) in members.iter().enumerate() {
        executor.write(vec![event(format!("warm-{n}"))]).await?;
    }
    let before: Vec<_> = members.iter().map(|(c, _)| c.node().metrics()).collect();
    let started = Instant::now();
    let mut tasks = Vec::new();
    for writer in 0..writers {
        let executor = members[writer % nodes].1.clone();
        tasks.push(tokio::spawn(async move {
            let mut took = Vec::new();
            for n in 0..events / writers {
                let began = Instant::now();
                executor
                    .write(vec![event(format!("order-{writer}-{n}"))])
                    .await
                    .expect("write");
                took.push(began.elapsed());
                if rate > 0 {
                    tokio::time::sleep_until((began + Duration::from_secs(1) / rate as u32).into())
                        .await;
                }
            }
            took
        }));
    }
    let mut took = Vec::new();
    for task in tasks {
        took.extend(task.await.expect("writer panicked"));
    }
    let elapsed = started.elapsed();
    took.sort();
    let written = took.len() as f64;
    let at = |p: usize| took[(took.len() * p / 100).min(took.len() - 1)];
    println!(
        "{nodes} nodes, {writers} writers, {written} events in {elapsed:.2?}: {:.0} events/s, \
         p50 {:.1?}, p99 {:.1?}, longest {:.1?}, {} over 1 s",
        written / elapsed.as_secs_f64(),
        at(50),
        at(99),
        at(100),
        took.iter().filter(|t| **t > Duration::from_secs(1)).count(),
    );
    for (n, (cluster, _)) in members.iter().enumerate() {
        let after = cluster.node().metrics();
        println!(
            "node {n}: {:.2} journal flushes per event, {} recoveries, {} held",
            (after.journal_flushes - before[n].journal_flushes) as f64 / written,
            after.recoveries - before[n].recoveries,
            after.recoveries_held - before[n].recoveries_held,
        );
        // The disk's part (#559), the warm-up included.
        for (what, times) in [
            ("journal flushes", after.journal_flush_seconds),
            ("store writes", after.store_apply_seconds),
        ] {
            println!(
                "  {} {what}: mean {:.2?}, {} over 10 ms, {} over 100 ms, {} over 1 s",
                times.count,
                times
                    .sum()
                    .checked_div(times.count as u32)
                    .unwrap_or_default(),
                times.over(Duration::from_millis(10)),
                times.over(Duration::from_millis(100)),
                times.over(Duration::from_secs(1)),
            );
        }
    }
    for (cluster, _) in &members {
        cluster.shutdown().await;
    }
    Ok(())
}

/// Every future's output, the futures run together.
async fn futures_all<T>(futures: Vec<impl Future<Output = T> + Send + 'static>) -> Vec<T>
where
    T: Send + 'static,
{
    let tasks: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut out = Vec::new();
    for task in tasks {
        out.push(task.await.expect("an open panicked"));
    }
    out
}
