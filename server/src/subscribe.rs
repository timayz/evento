//! Streaming subscriptions.
//!
//! A durable, resumable, at-least-once event subscription built on the
//! [`Executor`] subscriber primitives (`upsert_subscriber` /
//! `get_subscriber_cursor` / `acknowledge` / `is_subscriber_running`) plus a
//! tailing `read` loop — the same machinery the in-process
//! `evento_core::SubscriptionBuilder` uses, exposed over a bidirectional gRPC
//! stream.
//!
//! Protocol: the client sends a [`SubscribeStart`], the server pushes
//! [`EventBatch`] messages from the key's saved cursor (replay then live tail),
//! and the client returns a [`SubscribeAck`] with the last cursor it processed.
//! The server only advances the persisted cursor on ack, so a dropped stream
//! resumes rather than redelivering everything.
//!
//! [`Executor`]: evento_core::Executor
//! [`SubscribeStart`]: crate::proto::SubscribeStart
//! [`EventBatch`]: crate::proto::EventBatch
//! [`SubscribeAck`]: crate::proto::SubscribeAck

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use evento_core::{
    cursor::{Args, Value},
    Executor,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Status, Streaming};
use ulid::Ulid;

use crate::proto::subscribe_request::Message as ReqMessage;
use crate::{convert, error, proto};

const DEFAULT_CHUNK_SIZE: u16 = 100;
/// How long to wait before re-polling once caught up (live tail).
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The server-streaming type returned by `EventStore::subscribe`.
pub type SubscribeStream = Pin<Box<dyn Stream<Item = Result<proto::EventBatch, Status>> + Send>>;

/// Spawns the subscription loop and returns the outbound event stream.
pub(crate) fn subscribe<E: Executor>(
    executor: Arc<E>,
    inbound: Streaming<proto::SubscribeRequest>,
) -> SubscribeStream {
    let (tx, rx) = mpsc::channel::<Result<proto::EventBatch, Status>>(4);
    tokio::spawn(async move {
        let mut inbound = inbound;
        if let Err(status) = run(executor, &mut inbound, &tx).await {
            // Best-effort: surface the terminal error to the client.
            let _ = tx.send(Err(status)).await;
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

async fn run<E: Executor>(
    executor: Arc<E>,
    inbound: &mut Streaming<proto::SubscribeRequest>,
    tx: &mpsc::Sender<Result<proto::EventBatch, Status>>,
) -> Result<(), Status> {
    // The first message must open the subscription.
    let start = match inbound.message().await? {
        Some(proto::SubscribeRequest {
            message: Some(ReqMessage::Start(start)),
        }) => start,
        Some(_) => return Err(Status::invalid_argument("first message must be a Start")),
        None => return Ok(()), // client closed before starting
    };

    if start.key.is_empty() {
        return Err(Status::invalid_argument("subscription key is required"));
    }
    let chunk_size = if start.chunk_size == 0 {
        DEFAULT_CHUNK_SIZE
    } else {
        convert::u16_field(start.chunk_size, "chunk_size")?
    };
    let aggregators = convert::event_filters(start.aggregators);
    let routing_key = convert::routing_key(start.routing_key);
    let key = start.key;

    // Claim the subscription (last writer wins) and resume from its cursor.
    let worker_id = Ulid::new();
    executor
        .upsert_subscriber(key.clone(), worker_id)
        .await
        .map_err(error::internal)?;
    let mut after = executor
        .get_subscriber_cursor(key.clone())
        .await
        .map_err(error::internal)?;

    loop {
        // Yield if another stream has taken over this key.
        if !executor
            .is_subscriber_running(key.clone(), worker_id)
            .await
            .map_err(error::internal)?
        {
            return Ok(());
        }

        let result = executor
            .read(
                aggregators.clone(),
                routing_key.clone(),
                Args::forward(chunk_size, after.clone()),
            )
            .await
            .map_err(error::internal)?;

        if result.edges.is_empty() {
            // Caught up — wait, then poll again for new events.
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }

        let edges: Vec<proto::Edge> = result
            .edges
            .into_iter()
            .map(|edge| proto::Edge {
                cursor: edge.cursor.0,
                node: Some(convert::event_to_proto(edge.node)),
            })
            .collect();

        if tx.send(Ok(proto::EventBatch { edges })).await.is_err() {
            return Ok(()); // client dropped the stream
        }

        // Wait for the client to acknowledge before advancing the durable cursor.
        let cursor = match inbound.message().await? {
            Some(proto::SubscribeRequest {
                message: Some(ReqMessage::Ack(ack)),
            }) => Value(ack.cursor),
            Some(_) => return Err(Status::invalid_argument("expected an Ack")),
            None => return Ok(()), // client closed
        };
        executor
            .acknowledge(key.clone(), cursor.clone(), 0)
            .await
            .map_err(error::internal)?;
        after = Some(cursor);
    }
}
