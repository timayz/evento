//! End-to-end tests for the gRPC event-store service.
//!
//! A tonic server is bound on an ephemeral loopback port and driven through the
//! generated client, exercising the real HTTP/2 + protobuf wire path. The core
//! suite runs against both the Fjall and in-memory SQLite backends; a separate
//! test wires a 3-node Accord cluster and proves replication is transparent
//! through the gRPC layer.

use std::collections::HashMap;
use std::time::Duration;

use evento_core::Executor;
use evento_server::proto::event_store_client::EventStoreClient;
use evento_server::proto::{self};
use evento_server::{EventStoreServer, EventStoreService};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Code;

const AGG: &str = "test/Account";

// ---- harness ----

/// Serves `executor` over a fresh loopback port and returns a lazily-connected client.
async fn spawn<E: Executor>(executor: E) -> EventStoreClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let svc = EventStoreService::new(executor);
    tokio::spawn(async move {
        Server::builder()
            .add_service(EventStoreServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    EventStoreClient::new(channel)
}

fn new_event(name: &str, data: &[u8]) -> proto::NewEvent {
    proto::NewEvent {
        name: name.to_string(),
        data: data.to_vec(),
    }
}

fn fjall() -> (evento_fjall::Fjall, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let f = evento_fjall::Fjall::open(dir.path()).unwrap();
    (f, dir)
}

async fn sqlite() -> evento_sql::Sqlite {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx_migrator::{Migrate, Plan};

    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let migrator = evento_sql_migrator::new::<sqlx::Sqlite>().unwrap();
    migrator.run(&mut *conn, &Plan::apply_all()).await.unwrap();
    drop(conn);

    pool.into()
}

// ---- shared assertions ----

async fn run_suite(mut client: EventStoreClient<Channel>) {
    // (g) LatestTimestamp is 0 on a fresh, empty store.
    let ts = client
        .latest_timestamp(proto::LatestTimestampRequest {
            aggregators: vec![],
            routing_key: None,
        })
        .await
        .unwrap()
        .into_inner()
        .timestamp;
    assert_eq!(ts, 0, "empty store should report timestamp 0");

    // (a) Create -> server-generated id, version 1.
    let mut meta = HashMap::new();
    meta.insert("k".to_string(), vec![1u8, 2, 3]);
    let created = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: None,
            metadata: Some(proto::Metadata {
                id: "meta-1".to_string(),
                meta,
            }),
            events: vec![new_event("Created", b"hello")],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!created.aggregate_id.is_empty());
    assert_eq!(created.last_version, 1);
    let id = created.aggregate_id;

    // (b) Read by id round-trips opaque data + metadata byte-for-byte.
    let read = client
        .read(proto::ReadRequest {
            aggregators: vec![proto::EventFilter {
                aggregate_type: AGG.to_string(),
                aggregate_id: Some(id.clone()),
                name: None,
            }],
            routing_key: None,
            args: Some(proto::Args {
                first: Some(10),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.edges.len(), 1);
    let node = read.edges[0].node.as_ref().unwrap();
    assert_eq!(node.name, "Created");
    assert_eq!(node.version, 1);
    assert_eq!(node.data, b"hello");
    let md = node.metadata.as_ref().unwrap();
    assert_eq!(md.id, "meta-1");
    assert_eq!(md.meta.get("k").unwrap(), &vec![1u8, 2, 3]);

    // (c) Append with the correct original_version succeeds.
    let appended = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some(id.clone()),
            original_version: 1,
            routing_key: None,
            metadata: None,
            events: vec![new_event("Updated", b"world")],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(appended.last_version, 2);

    // (c) Append with a stale original_version -> FAILED_PRECONDITION.
    let stale = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some(id.clone()),
            original_version: 1,
            routing_key: None,
            metadata: None,
            events: vec![new_event("Updated", b"again")],
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);

    // (d) Empty event list -> INVALID_ARGUMENT.
    let empty = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: None,
            metadata: None,
            events: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(empty.code(), Code::InvalidArgument);

    // (e) original_version beyond u16::MAX -> INVALID_ARGUMENT.
    let overflow = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some("x".to_string()),
            original_version: 70_000,
            routing_key: None,
            metadata: None,
            events: vec![new_event("Created", b"")],
        })
        .await
        .unwrap_err();
    assert_eq!(overflow.code(), Code::InvalidArgument);

    // (f) Forward pagination chains end_cursor.
    let paged = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: None,
            metadata: None,
            events: vec![
                new_event("E1", b"1"),
                new_event("E2", b"2"),
                new_event("E3", b"3"),
            ],
        })
        .await
        .unwrap()
        .into_inner();

    let filter = proto::EventFilter {
        aggregate_type: AGG.to_string(),
        aggregate_id: Some(paged.aggregate_id.clone()),
        name: None,
    };
    let page1 = client
        .read(proto::ReadRequest {
            aggregators: vec![filter.clone()],
            routing_key: None,
            args: Some(proto::Args {
                first: Some(2),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page1.edges.len(), 2);
    let pi1 = page1.page_info.as_ref().unwrap();
    assert!(pi1.has_next_page);
    let after = pi1.end_cursor.clone();
    assert!(after.is_some());

    let page2 = client
        .read(proto::ReadRequest {
            aggregators: vec![filter],
            routing_key: None,
            args: Some(proto::Args {
                first: Some(2),
                after,
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page2.edges.len(), 1);
    assert!(!page2.page_info.as_ref().unwrap().has_next_page);

    // (g) LatestTimestamp is non-zero after writes.
    let ts = client
        .latest_timestamp(proto::LatestTimestampRequest {
            aggregators: vec![],
            routing_key: None,
        })
        .await
        .unwrap()
        .into_inner()
        .timestamp;
    assert!(ts > 0);
}

// ---- backend parametrizations ----

#[tokio::test]
async fn roundtrip_fjall() {
    let (executor, _dir) = fjall();
    let client = spawn(executor).await;
    run_suite(client).await;
}

#[tokio::test]
async fn roundtrip_sqlite() {
    let executor = sqlite().await;
    let client = spawn(executor).await;
    run_suite(client).await;
}

// ---- global default routing key ----

#[tokio::test]
async fn default_routing_key_applied() {
    let (fjall, _dir) = fjall();
    let executor = evento_core::Evento::new(fjall).default_routing_key("tenant-a");
    let mut client = spawn(executor).await;

    // A write that omits routing_key inherits the server default.
    let a = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: None,
            metadata: None,
            events: vec![new_event("Created", b"a")],
        })
        .await
        .unwrap()
        .into_inner();

    let read_a = client
        .read(proto::ReadRequest {
            aggregators: vec![proto::EventFilter {
                aggregate_type: AGG.to_string(),
                aggregate_id: Some(a.aggregate_id),
                name: None,
            }],
            routing_key: None,
            args: Some(proto::Args {
                first: Some(1),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        read_a.edges[0]
            .node
            .as_ref()
            .unwrap()
            .routing_key
            .as_deref(),
        Some("tenant-a"),
    );

    // A write that supplies its own routing_key keeps it.
    let b = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: Some("tenant-b".to_string()),
            metadata: None,
            events: vec![new_event("Created", b"b")],
        })
        .await
        .unwrap()
        .into_inner();

    let read_b = client
        .read(proto::ReadRequest {
            aggregators: vec![proto::EventFilter {
                aggregate_type: AGG.to_string(),
                aggregate_id: Some(b.aggregate_id),
                name: None,
            }],
            routing_key: None,
            args: Some(proto::Args {
                first: Some(1),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        read_b.edges[0]
            .node
            .as_ref()
            .unwrap()
            .routing_key
            .as_deref(),
        Some("tenant-b"),
    );
}

// ---- subscriptions ----

use evento_server::proto::subscribe_request::Message as ReqMsg;
use tokio_stream::wrappers::ReceiverStream;

fn start_msg(key: &str, id: &str) -> proto::SubscribeRequest {
    proto::SubscribeRequest {
        message: Some(ReqMsg::Start(proto::SubscribeStart {
            key: key.to_string(),
            aggregators: vec![proto::EventFilter {
                aggregate_type: AGG.to_string(),
                aggregate_id: Some(id.to_string()),
                name: None,
            }],
            routing_key: None,
            chunk_size: 10,
        })),
    }
}

fn ack_msg(cursor: String) -> proto::SubscribeRequest {
    proto::SubscribeRequest {
        message: Some(ReqMsg::Ack(proto::SubscribeAck { cursor })),
    }
}

#[tokio::test]
async fn subscribe_streams_and_resumes() {
    let (executor, _dir) = fjall();
    let mut client = spawn(executor).await;

    // Seed two events.
    let created = client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: None,
            original_version: 0,
            routing_key: None,
            metadata: None,
            events: vec![new_event("E1", b"1"), new_event("E2", b"2")],
        })
        .await
        .unwrap()
        .into_inner();
    let id = created.aggregate_id;
    let key = format!("sub-{id}");

    // Open the subscription; the first batch replays the two existing events.
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::SubscribeRequest>(8);
    tx.send(start_msg(&key, &id)).await.unwrap();
    let mut stream = client
        .subscribe(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let batch = stream.message().await.unwrap().unwrap();
    assert_eq!(batch.edges.len(), 2);
    assert_eq!(batch.edges[0].node.as_ref().unwrap().name, "E1");
    assert_eq!(batch.edges[1].node.as_ref().unwrap().name, "E2");
    tx.send(ack_msg(batch.edges.last().unwrap().cursor.clone()))
        .await
        .unwrap();

    // A live append arrives on the open stream. Receiving E3 proves the E1/E2
    // ack was persisted (the server resumed its read past E2). Deliberately do
    // NOT ack E3.
    client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some(id.clone()),
            original_version: 2,
            routing_key: None,
            metadata: None,
            events: vec![new_event("E3", b"3")],
        })
        .await
        .unwrap();

    let batch = stream.message().await.unwrap().unwrap();
    assert_eq!(batch.edges.len(), 1);
    assert_eq!(batch.edges[0].node.as_ref().unwrap().name, "E3");

    // Drop the stream (E3 left unacked), append E4, then resume with the same
    // key. The saved cursor is at E2, so E1/E2 are NOT redelivered, but the
    // unacked E3 IS (at-least-once), alongside the new E4.
    drop(tx);
    drop(stream);
    client
        .write(proto::WriteRequest {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some(id.clone()),
            original_version: 3,
            routing_key: None,
            metadata: None,
            events: vec![new_event("E4", b"4")],
        })
        .await
        .unwrap();

    let (tx2, rx2) = tokio::sync::mpsc::channel::<proto::SubscribeRequest>(8);
    tx2.send(start_msg(&key, &id)).await.unwrap();
    let mut stream2 = client
        .subscribe(ReceiverStream::new(rx2))
        .await
        .unwrap()
        .into_inner();

    let batch = stream2.message().await.unwrap().unwrap();
    let names: Vec<&str> = batch
        .edges
        .iter()
        .map(|e| e.node.as_ref().unwrap().name.as_str())
        .collect();
    assert_eq!(
        names,
        ["E3", "E4"],
        "resume redelivers the unacked E3 + new E4, never the acked E1/E2"
    );
}

// ---- snapshots ----

#[tokio::test]
async fn snapshot_roundtrip_and_revision_gating() {
    let (executor, _dir) = fjall();
    let mut client = spawn(executor).await;

    let get = |client: &mut EventStoreClient<Channel>, rev: &str| {
        let mut client = client.clone();
        let rev = rev.to_string();
        async move {
            client
                .get_snapshot(proto::GetSnapshotRequest {
                    aggregate_type: AGG.to_string(),
                    aggregate_revision: rev,
                    id: "acc-1".to_string(),
                })
                .await
                .unwrap()
                .into_inner()
                .snapshot
        }
    };

    // Absent initially.
    assert!(get(&mut client, "v1").await.is_none());

    // Save, then read back the opaque data + cursor verbatim.
    client
        .save_snapshot(proto::SaveSnapshotRequest {
            aggregate_type: AGG.to_string(),
            aggregate_revision: "v1".to_string(),
            id: "acc-1".to_string(),
            data: b"state-v1".to_vec(),
            cursor: "cursor-1".to_string(),
        })
        .await
        .unwrap();

    let snap = get(&mut client, "v1").await.expect("snapshot present");
    assert_eq!(snap.data, b"state-v1");
    assert_eq!(snap.cursor, "cursor-1");

    // A different revision must not match (forces a rebuild).
    assert!(get(&mut client, "v2").await.is_none());

    // Saving v2 upserts the single (type, id) row: v2 now matches, v1 no longer.
    client
        .save_snapshot(proto::SaveSnapshotRequest {
            aggregate_type: AGG.to_string(),
            aggregate_revision: "v2".to_string(),
            id: "acc-1".to_string(),
            data: b"state-v2".to_vec(),
            cursor: "cursor-2".to_string(),
        })
        .await
        .unwrap();
    assert!(get(&mut client, "v1").await.is_none());
    assert_eq!(get(&mut client, "v2").await.unwrap().data, b"state-v2");

    // Delete is revision-independent and idempotent.
    client
        .delete_snapshot(proto::DeleteSnapshotRequest {
            aggregate_type: AGG.to_string(),
            id: "acc-1".to_string(),
        })
        .await
        .unwrap();
    assert!(get(&mut client, "v2").await.is_none());
    client
        .delete_snapshot(proto::DeleteSnapshotRequest {
            aggregate_type: AGG.to_string(),
            id: "acc-1".to_string(),
        })
        .await
        .unwrap(); // idempotent
}

// ---- Accord cluster smoke test ----

#[cfg(feature = "accord")]
mod cluster {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use evento_accord::{
        serve, AccordExecutor, DataStore, ExecutorDataStore, HybridLogicalClock, InMemoryJournal,
        Journal, MessageSink, Node, NodeId, StaticTopology, TcpTransport, Topology,
    };
    use tokio::sync::mpsc;

    fn build_node(
        id: NodeId,
        ids: Vec<NodeId>,
        peers: HashMap<NodeId, SocketAddr>,
        listener: tokio::net::TcpListener,
        local: evento_fjall::Fjall,
    ) -> AccordExecutor<evento_fjall::Fjall> {
        let (inbox_tx, inbox_rx) = mpsc::channel(1024);
        serve(listener, inbox_tx);

        let node = Node::new(
            id,
            Arc::new(StaticTopology::new(id, ids)) as Arc<dyn Topology>,
            Arc::new(HybridLogicalClock::new(id)),
            Arc::new(TcpTransport::new(id, peers)) as Arc<dyn MessageSink>,
            Arc::new(ExecutorDataStore::new(local.clone())) as Arc<dyn DataStore>,
            Arc::new(InMemoryJournal::new()) as Arc<dyn Journal>,
        );
        node.start(inbox_rx);
        node.start_recovery();
        AccordExecutor::new(node, local)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replicates_through_grpc() {
        let n = 3u64;
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();

        // Bind every Accord listener first so the peer map is complete.
        let mut listeners = Vec::new();
        let mut peers: HashMap<NodeId, SocketAddr> = HashMap::new();
        for &id in &ids {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, l.local_addr().unwrap());
            listeners.push((id, l));
        }

        let mut clients = Vec::new();
        let mut _dirs = Vec::new();
        for (id, listener) in listeners {
            let (local, dir) = fjall();
            _dirs.push(dir);
            let executor = build_node(id, ids.clone(), peers.clone(), listener, local);
            clients.push(spawn(executor).await);
        }

        // Write through node 0's gRPC endpoint.
        let written = clients[0]
            .write(proto::WriteRequest {
                aggregate_type: AGG.to_string(),
                aggregate_id: None,
                original_version: 0,
                routing_key: None,
                metadata: None,
                events: vec![new_event("Opened", b"payload")],
            })
            .await
            .unwrap()
            .into_inner();
        let id = written.aggregate_id;

        // Read it back through node 1's gRPC endpoint, polling for replication.
        let filter = proto::EventFilter {
            aggregate_type: AGG.to_string(),
            aggregate_id: Some(id.clone()),
            name: None,
        };
        let mut found = None;
        for _ in 0..400 {
            let result = clients[1]
                .read(proto::ReadRequest {
                    aggregators: vec![filter.clone()],
                    routing_key: None,
                    args: Some(proto::Args {
                        first: Some(1),
                        ..Default::default()
                    }),
                })
                .await
                .unwrap()
                .into_inner();
            if let Some(edge) = result.edges.into_iter().next() {
                found = edge.node;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let node = found.expect("event did not replicate to node 1 via gRPC");
        assert_eq!(node.name, "Opened");
        assert_eq!(node.data, b"payload");
    }
}
