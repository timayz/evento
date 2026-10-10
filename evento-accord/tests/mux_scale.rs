//! Scale smoke test for hosting many consensus groups per process: three hosts ×
//! 200 groups (600 nodes) over one `MuxTransport` per host, swept by one
//! `SweepScheduler` per host. Proves (a) every group commits and converges, (b) once
//! idle, sweep traffic collapses far below the per-node full cadence a lone
//! `start_recovery` task would produce, and (c) a cold group still commits a new
//! write — backing off never costs liveness.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{
    DataStore, GroupHost, GroupId, InMemoryDataStore, InMemoryJournal, Journal, MuxTransport, Node,
    NodeConfig, NodeId, StaticTopology, SweepConfig, SweepScheduler, Topology,
};
use evento_core::Event;
use tokio::net::TcpListener;

const HOSTS: u64 = 3;
const GROUPS: u64 = 200;

fn event(aggregate_id: &str, version: u16) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Opened".into(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hundreds_of_groups_per_process_commit_then_go_quiet() {
    let ids: Vec<NodeId> = (0..HOSTS).map(NodeId).collect();
    let mut listeners = Vec::new();
    let mut peers: HashMap<NodeId, SocketAddr> = HashMap::new();
    for &id in &ids {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(id, listener.local_addr().unwrap());
        listeners.push((id, listener));
    }

    let sweep = SweepConfig {
        tick: Duration::from_millis(100),
        active_hold: Duration::from_secs(1),
        idle_interval: Duration::from_secs(2),
        max_concurrent: 256,
    };
    let mut hosts: Vec<Arc<GroupHost>> = Vec::new();
    let mut tasks = Vec::new();
    for (id, listener) in listeners {
        // Many groups share each peer queue; give it room for the burst.
        let mux = Arc::new(MuxTransport::new(id, peers.clone()).with_queue_capacity(16_384));
        tasks.push(mux.serve(listener));
        let scheduler = SweepScheduler::new(sweep);
        tasks.push(scheduler.start());
        hosts.push(Arc::new(GroupHost::new(mux, scheduler)));
    }

    // Open every group on every host.
    let mut nodes: Vec<Vec<Node>> = Vec::new(); // [group][host]
    let mut stores: Vec<Vec<Arc<InMemoryDataStore>>> = Vec::new();
    for g in 0..GROUPS {
        let group = GroupId(1000 + g);
        let mut group_nodes = Vec::new();
        let mut group_stores = Vec::new();
        for (h, host) in hosts.iter().enumerate() {
            let store = Arc::new(InMemoryDataStore::new());
            let topology: Arc<dyn Topology> = Arc::new(StaticTopology::new(ids[h], ids.clone()));
            let node = host
                .open(
                    group,
                    topology,
                    Arc::clone(&store) as Arc<dyn DataStore>,
                    Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
                    NodeConfig::default(),
                )
                .await
                .unwrap();
            group_nodes.push(node);
            group_stores.push(store);
        }
        nodes.push(group_nodes);
        stores.push(group_stores);
    }
    assert_eq!(hosts[0].len(), GROUPS as usize);

    // One write per group, round-robin coordinators, all concurrently.
    let mut writes = Vec::new();
    for (g, group_nodes) in nodes.iter().enumerate() {
        let node = group_nodes[g % HOSTS as usize].clone();
        writes.push(tokio::spawn(async move {
            node.write(vec![event(&format!("acc-{g}"), 1)]).await
        }));
    }
    for (g, w) in writes.into_iter().enumerate() {
        let outcome = tokio::time::timeout(Duration::from_secs(30), w)
            .await
            .unwrap_or_else(|_| panic!("group {g} write timed out"))
            .unwrap()
            .unwrap_or_else(|e| panic!("group {g} write failed: {e}"));
        assert!(!outcome.conflict);
    }
    // Every replica of every group converges on its one write.
    for _ in 0..1000 {
        if stores
            .iter()
            .all(|gs| gs.iter().all(|s| s.applied_log().len() == 1))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        stores
            .iter()
            .all(|gs| gs.iter().all(|s| s.applied_log().len() == 1)),
        "every group converges"
    );

    // Let the activity hold (1 s) lapse, then measure an idle window.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before: Vec<_> = hosts.iter().map(|h| h.scheduler().stats()).collect();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after: Vec<_> = hosts.iter().map(|h| h.scheduler().stats()).collect();
    for (h, (b, a)) in before.iter().zip(&after).enumerate() {
        let swept = a.sweeps_run - b.sweeps_run;
        // Full cadence would be 200 groups × 20 ticks = 4000 sweeps in 2 s; idle
        // cadence is one sweep per group per 2 s ≈ 200 (allow jitter).
        assert!(
            swept <= 2 * GROUPS,
            "host {h}: {swept} sweeps in a 2 s idle window — idle backoff not in effect"
        );
        assert!(swept > 0, "host {h}: idle groups must still be swept");
        assert_eq!(
            a.sweeps_deferred, b.sweeps_deferred,
            "host {h}: no deferrals while idle"
        );
    }

    // A cold group still commits a fresh write (and the fast path is intact).
    let cold = &nodes[GROUPS as usize / 2][1];
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        cold.write(vec![event("acc-cold", 1)]),
    )
    .await
    .expect("cold write timed out")
    .expect("cold write failed");
    assert!(!outcome.conflict);
    for _ in 0..500 {
        if stores[GROUPS as usize / 2]
            .iter()
            .all(|s| s.applied_log().len() == 2)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(stores[GROUPS as usize / 2]
        .iter()
        .all(|s| s.applied_log().len() == 2));
    drop(tasks);
}
