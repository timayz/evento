//! Three nodes that start together on a machine short of CPU (#610): how
//! long they take to commit a first write, and what they cost while nothing
//! is asked of them.
//!
//! A sizing harness, not a check: each node runs on a runtime of its own, as
//! a process would, over mutual TLS, and says its routing key through the
//! cover as a deployment does at its start. Run it alone, then short of CPU:
//!
//! ```text
//! cargo test --release -p evento-cluster --test starved_host -- --ignored --nocapture
//! # one core for the three nodes, shared with eight busy loops
//! for i in $(seq 8); do taskset -c 0 sh -c 'while :; do :; done' & done
//! taskset -c 0 cargo test --release -p evento-cluster --test starved_host -- --ignored --nocapture
//! ```
//!
//! `SIZING_SUBSCRIPTIONS` is how many subscriptions each node runs over its
//! own copy, with nothing to handle (0), each following `SIZING_HANDLERS`
//! events (8): what an application's workers cost a node at rest.
//! `SIZING_NODES` starts that many of the three (3): what a node costs while
//! it reaches no majority. `SIZING_THREADS` is each node's worker threads
//! (2), `SIZING_IDLE_SECS` how long the nodes are left alone once their keys
//! are said (20), `SIZING_WITHIN_SECS` how long they are given to say them
//! (300), `SIZING_STORE` the store (`fjall`, or `turso`), `SIZING_ROOT` where
//! the stores go (the temporary directory).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use evento_cluster::host::Settings;
use evento_cluster::{keys, Cluster, ClusterBackend, TlsSettings};
use evento_core::subscription::{Context, Handler, SubscriptionBuilder};
use evento_core::{Event, Executor};
use evento_fjall::Fjall;
use evento_turso::Turso;

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// The loopback address node `id` is dialled at, and named by in its
/// certificate.
fn ip(id: u64) -> String {
    format!("127.0.0.{}", id + 1)
}

fn free_addrs(count: u64) -> Vec<SocketAddr> {
    let listeners: Vec<_> = (0..count)
        .map(|id| std::net::TcpListener::bind(format!("{}:0", ip(id))).unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// A certificate for each node, issued by one authority, under `root`.
fn certificates(root: &Path, count: u64) -> Vec<TlsSettings> {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_pem = ca.self_signed(&ca_key).unwrap().pem();
    (0..count)
        .map(|id| {
            let dir = root.join(format!("node{id}")).join("tls");
            std::fs::create_dir_all(&dir).unwrap();
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec![ip(id)]).unwrap();
            let issuer = rcgen::Issuer::from_params(&ca, &ca_key);
            let cert = params.signed_by(&key, &issuer).unwrap();
            let files = TlsSettings {
                ca: dir.join("ca.crt"),
                cert: dir.join("tls.crt"),
                key: dir.join("tls.key"),
            };
            std::fs::write(&files.ca, &ca_pem).unwrap();
            std::fs::write(&files.cert, cert.pem()).unwrap();
            std::fs::write(&files.key, key.serialize_pem()).unwrap();
            files
        })
        .collect()
}

/// The processor time this process used so far, user and system.
fn cpu_used() -> Duration {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    // Past the command, which may hold spaces: state is the first field.
    let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    // USER_HZ is 100 on every Linux this runs on.
    Duration::from_millis(ticks * 10)
}

/// Handles nothing: no event of its name is ever written.
struct Idle(&'static str);

impl<E: Executor> Handler<E> for Idle {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        _event: &'a Event,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn aggregate_type(&self) -> &'static str {
        "sizing/Order"
    }

    fn event_name(&self) -> &'static str {
        self.0
    }
}

/// One node: opens, says its key through the cover, tells when the key is on
/// its own copy of the list, then runs until `stop` closes.
async fn node<E: ClusterBackend + Executor + Clone>(
    store: E,
    dir: PathBuf,
    settings: Settings,
    said: mpsc::Sender<(u64, Duration)>,
    stop: tokio::sync::oneshot::Receiver<()>,
) {
    let started = Instant::now();
    let id = settings.node_id;
    let key = format!("key-{id}");
    let (cluster, executor) = Cluster::builder(store)
        .settings(Some(settings))
        .state_dir(&dir)
        .serves([key.clone()])
        .open()
        .await
        .unwrap();
    let local = executor.local_reads();
    let covering = cluster.cover(local.clone()).start();
    let mut subscriptions = Vec::new();
    for n in 0..env("SIZING_SUBSCRIPTIONS", 0usize) {
        let mut builder = SubscriptionBuilder::new(format!("sizing-idle-{n}"));
        for handler in 0..env("SIZING_HANDLERS", 8usize) {
            builder = builder.handler(Idle(format!("Idle{handler}").leak()));
        }
        subscriptions.push(builder.start(&local).await.unwrap());
    }
    let mut stop = std::pin::pin!(stop);
    let on_the_list = async {
        while !keys::known(&local).await.unwrap().contains(&key) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::select! {
        _ = on_the_list => {
            let _ = said.send((id, started.elapsed()));
            let _ = stop.await;
        }
        // Left without a majority (`SIZING_NODES`): the key is never said.
        _ = &mut stop => {}
    }
    for subscription in subscriptions {
        let _ = subscription.shutdown().await;
    }
    covering.shutdown().await;
    cluster.shutdown().await;
}

#[test]
#[ignore = "sizing harness: run it alone, then on a machine short of CPU"]
fn sizing_three_nodes_start_together() {
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();
    }
    let threads: usize = env("SIZING_THREADS", 2);
    let idle = Duration::from_secs(env("SIZING_IDLE_SECS", 20));
    let within = Duration::from_secs(env("SIZING_WITHIN_SECS", 300));
    let store: String = env("SIZING_STORE", "fjall".to_owned());
    let root = std::env::var_os("SIZING_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("evento-starved-host-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    let addrs = free_addrs(3);
    let tls = certificates(&root, 3);
    let (said_tx, said_rx) = mpsc::channel();
    let mut stops = Vec::new();
    let mut handles = Vec::new();
    let began = Instant::now();
    let cpu_began = cpu_used();
    let up: u64 = env("SIZING_NODES", 3);
    for id in 0..up {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        stops.push(stop_tx);
        let dir = root.join(format!("node{id}"));
        let settings = Settings {
            tls: Some(tls[id as usize].clone()),
            ..Settings::new(id, addrs.clone())
        };
        let (said, store) = (said_tx.clone(), store.clone());
        handles.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                match store.as_str() {
                    "turso" => {
                        let store = Turso::open_concurrent(dir.join("store.db")).await.unwrap();
                        node(store, dir, settings, said, stop_rx).await
                    }
                    _ => {
                        let store = Fjall::open(dir.join("store")).unwrap();
                        node(store, dir, settings, said, stop_rx).await
                    }
                }
            });
        }));
    }

    let mut left = up;
    while left > 0 {
        let wait = within.saturating_sub(began.elapsed());
        match said_rx.recv_timeout(wait) {
            Ok((id, took)) => {
                left -= 1;
                println!("node {id}: its key is on the list after {took:.1?}");
            }
            Err(_) => {
                println!("{left} nodes did not say their key within {within:?}");
                break;
            }
        }
    }
    let start_wall = began.elapsed();
    let start_cpu = cpu_used() - cpu_began;
    println!(
        "start: {start_wall:.1?}, {start_cpu:.1?} of processor ({:.0}% of a core between them)",
        100.0 * start_cpu.as_secs_f64() / start_wall.as_secs_f64()
    );

    let (idle_began, cpu_began) = (Instant::now(), cpu_used());
    std::thread::sleep(idle);
    let (idle_wall, idle_cpu) = (idle_began.elapsed(), cpu_used() - cpu_began);
    println!(
        "idle: {idle_wall:.1?}, {idle_cpu:.1?} of processor ({:.1}% of a core between them)",
        100.0 * idle_cpu.as_secs_f64() / idle_wall.as_secs_f64()
    );

    drop(stops);
    for handle in handles {
        handle.join().unwrap();
    }
    let _ = std::fs::remove_dir_all(&root);
}
