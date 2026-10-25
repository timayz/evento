//! Anti-entropy replies are bounded. A peer missing more than one page's
//! worth of commands gets an execution-order prefix per round, and the
//! coverage the reply proves stays below every command it left out — it is
//! what the requester's watermark report and subscriptions trust, so it must
//! never vouch for a transaction still on its way. A write too large for any
//! page to carry is refused up front.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::message::SyncKnown;
use evento_accord::{
    snapshot_event_size, CommandState, CommandTooLarge, DataStore, HybridLogicalClock,
    InMemoryDataStore, InMemoryJournal, InMemoryNetwork, Journal, Message, MessageSink, Node,
    NodeConfig, NodeId, StaticTopology, Timestamp, TxnId, MAX_COMMAND_BYTES,
};
use evento_core::Event;
use tokio::sync::mpsc;

const PAGE_BYTES: usize = 8 * 1024;

fn event(aggregate_id: &str, version: u16, data: usize) -> Event {
    Event {
        aggregate_type: "test/Account".into(),
        aggregate_id: aggregate_id.into(),
        version,
        name: "Bumped".into(),
        data: vec![7; data],
        ..Default::default()
    }
}

fn wire_size(cmd: &CommandState) -> usize {
    cmd.events.iter().map(snapshot_event_size).sum()
}

/// Nodes 0 and 1 of a three-node group, real; node 2 is the test, answering
/// nothing (so the others commit on the slow path and never compact).
fn two_of_three(
    config: NodeConfig,
) -> (
    Arc<InMemoryNetwork>,
    Vec<Node>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let net = InMemoryNetwork::new();
    let ids = vec![NodeId(0), NodeId(1), NodeId(2)];
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for &id in &ids[..2] {
        let inbox = net.register(id);
        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids.clone())),
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
            Arc::new(InMemoryDataStore::new()) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        )
        .with_config(config);
        tasks.push(node.start(inbox));
        nodes.push(node);
    }
    (net, nodes, tasks)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anti_entropy_ships_a_budgeted_prefix_and_never_covers_what_it_left_out() {
    let (net, nodes, _tasks) = two_of_three(NodeConfig {
        snapshot_page_bytes: PAGE_BYTES,
        ..NodeConfig::default()
    });
    // Forty 1 KiB writes, over two aggregates: five pages' worth.
    for seq in 0..40u16 {
        let agg = if seq % 2 == 0 { "a" } else { "b" };
        tokio::time::timeout(
            Duration::from_secs(10),
            nodes[seq as usize % 2].write(vec![event(agg, seq / 2 + 1, 1024)]),
        )
        .await
        .expect("write timed out")
        .expect("write");
    }
    // Past the compaction margin, node 0's coverage would span every write.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Node 2 runs anti-entropy against node 0 by hand, holding what it got.
    let mut inbox = net.register(NodeId(2));
    let (tx, mut replies) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(env) = inbox.recv().await {
            if let Message::SyncData {
                id,
                coverage,
                commands,
                ..
            } = env.message
            {
                let _ = tx.send((id, coverage, commands));
            }
        }
    });
    let sink = net.sink(NodeId(2));
    let mut held: Vec<TxnId> = Vec::new();
    let mut rounds: Vec<(Timestamp, Vec<CommandState>)> = Vec::new();
    for id in 1.. {
        sink.send(
            NodeId(0),
            Message::SyncRequest {
                id,
                known: SyncKnown {
                    since: Timestamp::MIN,
                    txns: held.clone(),
                },
            },
        )
        .await
        .unwrap();
        let (got, coverage, commands) =
            tokio::time::timeout(Duration::from_secs(5), replies.recv())
                .await
                .expect("node 0 answers anti-entropy")
                .unwrap();
        assert_eq!(got, id);
        if commands.is_empty() {
            break;
        }
        held.extend(commands.iter().map(|cmd| cmd.txn));
        rounds.push((coverage, commands));
        assert!(rounds.len() <= 40, "every round ships something new");
    }

    let shipped: HashMap<TxnId, usize> = rounds
        .iter()
        .enumerate()
        .flat_map(|(round, (_, commands))| commands.iter().map(move |cmd| (cmd.txn, round)))
        .collect();
    assert_eq!(shipped.len(), 40, "every write was repaired once");
    assert!(
        rounds.len() >= 4,
        "the backlog crossed in budgeted rounds, not one reply: {} round(s)",
        rounds.len()
    );
    for (round, (coverage, commands)) in rounds.iter().enumerate() {
        let bytes: usize = commands.iter().map(wire_size).sum();
        assert!(
            commands.len() == 1 || bytes <= PAGE_BYTES,
            "round {round} ships {bytes} bytes over a {PAGE_BYTES} budget"
        );
        for (txn, later) in &shipped {
            if *later > round {
                assert!(
                    *coverage <= txn.0,
                    "round {round} proves coverage {coverage:?} past {txn:?}, which only round {later} shipped"
                );
            }
        }
    }
}

/// A transaction too large for any frame is refused before it enters
/// consensus: no page can split a command, so a replica that missed it
/// could never be repaired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_over_the_command_cap_is_refused() {
    let net = InMemoryNetwork::new();
    let id = NodeId(0);
    let inbox = net.register(id);
    let store = Arc::new(InMemoryDataStore::new());
    let node = Node::new(
        id,
        Arc::new(StaticTopology::new(id, vec![id])),
        Arc::new(HybridLogicalClock::new(id)),
        Arc::new(net.sink(id)) as Arc<dyn MessageSink>,
        Arc::clone(&store) as Arc<dyn DataStore>,
        Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
    );
    let _loop = node.start(inbox);

    let err = node
        .write(vec![event("big", 1, MAX_COMMAND_BYTES)])
        .await
        .expect_err("a write over the cap is refused");
    let refused = err
        .downcast_ref::<CommandTooLarge>()
        .unwrap_or_else(|| panic!("unexpected error: {err:#}"));
    assert!(refused.bytes > MAX_COMMAND_BYTES);
    assert_eq!(refused.cap, MAX_COMMAND_BYTES);
    assert!(store.committed_events().is_empty(), "nothing was written");
    assert_eq!(node.command_count(), 0);

    node.write(vec![event("big", 1, 1024)])
        .await
        .expect("a write under the cap commits");
}
