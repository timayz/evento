//! A write made through one node wakes the subscriptions of the others
//! (#610): an event reaches a node's store through its applier, which tells
//! the store's write signal, so a subscription there handles it without
//! waiting for a poll. That is what lets a subscription over a cluster node
//! poll every few seconds by default
//! ([`Executor::hears_every_write`]) instead of four times a second.

mod common;

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use evento_cluster::host::Settings;
use evento_cluster::{Cluster, ClusterBackend, ClusterExecutor};
use evento_core::subscription::{Context, Handler, SubscriptionBuilder};
use evento_core::{Event, Executor};
use evento_fjall::Fjall;
use evento_turso::Turso;

use common::addrs::free_addr;

const AGG: &str = "test/Order";

/// A poll that never comes within a test: what is handled was woken for.
const NEVER: Duration = Duration::from_secs(3600);

fn event(id: &str) -> Event {
    Event {
        id: ulid::Ulid::generate(),
        aggregate_type: AGG.into(),
        aggregate_id: id.into(),
        version: 1,
        name: "Placed".into(),
        ..Default::default()
    }
}

/// The streams a node's subscription handled.
type Handled = Arc<Mutex<Vec<String>>>;

struct Recorder(Handled);

impl<E: Executor> Handler<E> for Recorder {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event.aggregate_id.clone());
            Ok(())
        })
    }

    fn aggregate_type(&self) -> &'static str {
        AGG
    }

    fn event_name(&self) -> &'static str {
        "Placed"
    }
}

async fn free_addrs() -> Vec<SocketAddr> {
    (0..3).map(|_| free_addr("127.0.0.1")).collect()
}

async fn open<E: ClusterBackend>(
    store: E,
    dir: &Path,
    id: u64,
    peers: &[SocketAddr],
) -> (Arc<Cluster>, ClusterExecutor<E>) {
    Cluster::builder(store)
        .settings(Some(Settings::new(id, peers.to_vec())))
        .state_dir(dir)
        .open()
        .await
        .unwrap()
}

/// Three nodes over the stores `make` opens; a subscription on each of the
/// two that do not take the write, which never polls.
async fn a_write_wakes_the_other_nodes<E, Fut>(test: &str, make: impl Fn(std::path::PathBuf) -> Fut)
where
    E: ClusterBackend + Executor + Clone,
    Fut: Future<Output = E>,
{
    let root = std::env::temp_dir().join(format!(
        "evento-subscription-wake-{}-{test}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let addrs = free_addrs().await;
    let mut nodes = Vec::new();
    for id in 0..3u64 {
        let dir = root.join(format!("node{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        nodes.push((make(dir.clone()).await, dir, id));
    }
    let (a, b, c) = tokio::join!(
        open(nodes[0].0.clone(), &nodes[0].1, 0, &addrs),
        open(nodes[1].0.clone(), &nodes[1].1, 1, &addrs),
        open(nodes[2].0.clone(), &nodes[2].1, 2, &addrs),
    );
    let nodes = [a, b, c];
    assert!(
        nodes[1].1.local_reads().hears_every_write(),
        "the node's executor says its signal tells of every write"
    );

    let mut subscriptions = Vec::new();
    let mut handled = Vec::new();
    for (_, executor) in &nodes[1..] {
        let seen = Handled::default();
        subscriptions.push(
            SubscriptionBuilder::new("orders")
                .handler(Recorder(seen.clone()))
                .poll_interval(NEVER)
                .start(&executor.local_reads())
                .await
                .unwrap(),
        );
        handled.push(seen);
    }
    // Past the pass each makes as it starts.
    tokio::time::sleep(Duration::from_millis(300)).await;

    for n in 0..3 {
        let id = format!("order-{n}");
        // A write right after the nodes opened can fail once: made again.
        let deadline = Instant::now() + Duration::from_secs(30);
        while let Err(err) = nodes[0].1.write(vec![event(&id)]).await {
            assert!(Instant::now() < deadline, "the write of {id}: {err:?}");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        for (node, seen) in handled.iter().enumerate() {
            let deadline = Instant::now() + Duration::from_secs(15);
            while !seen.lock().unwrap().contains(&id) {
                assert!(
                    Instant::now() < deadline,
                    "node {} was not woken for {id}",
                    node + 1
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    for subscription in subscriptions {
        subscription.shutdown().await.unwrap();
    }
    for (cluster, _) in &nodes {
        cluster.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_wakes_the_other_nodes_subscriptions_over_fjall() {
    a_write_wakes_the_other_nodes("fjall", |dir| async move {
        Fjall::open(dir.join("store")).unwrap()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_wakes_the_other_nodes_subscriptions_over_turso() {
    a_write_wakes_the_other_nodes("turso", |dir| async move {
        Turso::open_concurrent(dir.join("store.db")).await.unwrap()
    })
    .await;
}
