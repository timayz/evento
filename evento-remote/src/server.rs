//! Server side: expose any [`Executor`] over framed TCP.
//!
//! [`serve`] accepts connections and answers [`Request`]s by delegating to the
//! wrapped executor. Requests on one connection are handled **concurrently**
//! (spawn per request) — multiple application tasks share one client
//! connection, and a slow `write` (e.g. a consensus round on an Accord-backed
//! server) must not head-of-line-block unrelated reads; correlation ids make
//! out-of-order replies safe.
//!
//! Writes are fanned out to every connection as [`ServerFrame::Notify`]: from
//! the executor's own `write_watch` when it has one, otherwise bumped after
//! each successful `write` handled here (still low-latency for writes through
//! this server; writers bypassing it are covered by the client's poll-interval
//! fallback, the same caveat every `write_watch` backend documents).

use bytes::Bytes;
use evento_core::Executor;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::wire::{
    encode_tagged, ClientFrame, RecordKind, Request, Response, ServerFrame, WireReadResult,
    WireWriteError, MAX_FRAME_LENGTH,
};

/// Capacity of a connection's outbound queue. Sends into it are awaited, so a
/// slow client backpressures its own request handlers instead of shedding
/// replies.
const OUT_CAPACITY: usize = 1024;

/// Maximum concurrently-executing request handlers per connection. Without a
/// cap a client pipelining arbitrarily many large frames could spawn unbounded
/// tasks; at the cap the read loop stops pulling frames (TCP backpressure).
const MAX_INFLIGHT_PER_CONNECTION: usize = 256;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}

/// Handle to a running server. Dropping it does **not** stop the server; call
/// [`shutdown`](ServerHandle::shutdown).
pub struct ServerHandle {
    accept_task: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl ServerHandle {
    /// Stops accepting connections and closes every established one. In-flight
    /// request handlers may still complete against the executor, but their
    /// replies are no longer delivered.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
    }
}

/// Serves `executor` on `listener` until [`ServerHandle::shutdown`].
pub fn serve<E: Executor + Clone>(listener: TcpListener, executor: E) -> ServerHandle {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (notify_tx, _) = watch::channel(0u64);

    // A backend with its own write_watch covers writes from every process
    // sharing the store; forward it. Otherwise the Write handler bumps the
    // channel itself (see `handle`).
    let executor_has_watch = executor.write_watch().is_some();
    if let Some(mut write_rx) = executor.write_watch() {
        let notify_tx = notify_tx.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = write_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        notify_tx.send_modify(|g| *g += 1);
                    }
                    _ = shutdown.changed() => break,
                }
            }
        });
    }

    let accept_task = {
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let _ = stream.set_nodelay(true);
                        tokio::spawn(connection(
                            stream,
                            executor.clone(),
                            notify_tx.subscribe(),
                            shutdown.clone(),
                            !executor_has_watch,
                            notify_tx.clone(),
                        ));
                    }
                    _ = shutdown.changed() => break,
                }
            }
        })
    };

    ServerHandle {
        accept_task,
        shutdown_tx,
    }
}

/// One established connection: a writer task draining the outbound queue, a
/// notify task forwarding write notifications, and the inbound read loop
/// spawning a handler per request.
async fn connection<E: Executor + Clone>(
    stream: TcpStream,
    executor: E,
    mut notify_rx: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    bump_on_write: bool,
    notify_tx: watch::Sender<u64>,
) {
    let framed = Framed::new(stream, codec());
    let (mut sink, mut inbound) = framed.split();
    let (out_tx, mut out_rx) = mpsc::channel::<ServerFrame>(OUT_CAPACITY);
    let inflight = std::sync::Arc::new(tokio::sync::Semaphore::new(
        MAX_INFLIGHT_PER_CONNECTION,
    ));

    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            let Ok(bytes) = encode_tagged(RecordKind::ServerFrame, &frame) else {
                continue;
            };
            if sink.send(Bytes::from(bytes)).await.is_err() {
                break;
            }
        }
    });

    let notifier = {
        let out_tx = out_tx.clone();
        let executor = executor.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = notify_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let generation = *notify_rx.borrow_and_update();
                        let stable_timestamp =
                            executor.stable_timestamp().await.ok().flatten();
                        let frame = ServerFrame::Notify {
                            generation,
                            stable_timestamp,
                        };
                        if out_tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        })
    };

    loop {
        let item = tokio::select! {
            item = inbound.next() => item,
            _ = shutdown.changed() => break,
        };
        let Some(Ok(bytes)) = item else {
            break; // closed, or a transport error
        };
        let frame: ClientFrame = match crate::wire::decode_tagged(RecordKind::ClientFrame, &bytes) {
            Ok(frame) => frame,
            Err(err) => {
                // An RPC peer speaking garbage is unrecoverable; drop the link.
                tracing::debug!(?err, "undecodable frame, closing connection");
                break;
            }
        };
        let ClientFrame::Request { id, request } = frame;

        if let Request::Hello = request {
            let response = Response::Hello {
                default_routing_key: executor.default_routing_key().map(str::to_owned),
            };
            let frame = ServerFrame::Response {
                id,
                response,
                stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
            };
            if out_tx.send(frame).await.is_err() {
                break;
            }
            continue;
        }

        // Bound concurrent handlers: at the cap, stop pulling frames until one
        // finishes (backpressure), instead of spawning without limit.
        let Ok(permit) = std::sync::Arc::clone(&inflight).acquire_owned().await else {
            break;
        };
        let executor = executor.clone();
        let out_tx = out_tx.clone();
        let notify_tx = notify_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let is_write = matches!(request, Request::Write { .. } | Request::Replicate { .. });
            let response = handle(&executor, request).await;
            if bump_on_write && is_write && matches!(response, Response::Write(Ok(()))) {
                notify_tx.send_modify(|g| *g += 1);
            }
            let frame = ServerFrame::Response {
                id,
                response,
                stable_timestamp: executor.stable_timestamp().await.ok().flatten(),
            };
            let _ = out_tx.send(frame).await;
        });
    }

    notifier.abort();
    drop(out_tx);
    let _ = writer.await;
}

fn err_string<T>(result: anyhow::Result<T>) -> Result<T, String> {
    result.map_err(|e| format!("{e:#}"))
}

async fn handle<E: Executor>(executor: &E, request: Request) -> Response {
    match request {
        Request::Hello => unreachable!("Hello is answered inline by the read loop"),
        Request::Write { events } => Response::Write(
            executor
                .write(events)
                .await
                .map_err(|e| WireWriteError::from(&e)),
        ),
        Request::Replicate { events } => Response::Write(
            executor
                .replicate(events)
                .await
                .map_err(|e| WireWriteError::from(&e)),
        ),
        Request::Read {
            aggregators,
            routing_key,
            args,
        } => Response::Read(err_string(
            executor
                .read(aggregators, routing_key, args)
                .await
                .map(WireReadResult::from),
        )),
        Request::LatestTimestamp {
            aggregators,
            routing_key,
        } => Response::LatestTimestamp(err_string(
            executor.latest_timestamp(aggregators, routing_key).await,
        )),
        Request::GetSubscriberCursor { key } => {
            Response::SubscriberCursor(err_string(executor.get_subscriber_cursor(key).await))
        }
        Request::IsSubscriberRunning { key, worker_id } => Response::SubscriberRunning(err_string(
            executor.is_subscriber_running(key, worker_id).await,
        )),
        Request::UpsertSubscriber { key, worker_id } => {
            Response::Unit(err_string(executor.upsert_subscriber(key, worker_id).await))
        }
        Request::Acknowledge {
            key,
            worker_id,
            cursor,
            lag,
        } => Response::Acknowledge(err_string(
            executor.acknowledge(key, worker_id, cursor, lag).await,
        )),
        Request::GetSnapshot {
            aggregate_type,
            aggregate_revision,
            id,
        } => Response::Snapshot(err_string(
            executor
                .get_snapshot(aggregate_type, aggregate_revision, id)
                .await,
        )),
        Request::SaveSnapshot {
            aggregate_type,
            aggregate_revision,
            id,
            data,
            cursor,
        } => Response::Unit(err_string(
            executor
                .save_snapshot(aggregate_type, aggregate_revision, id, data, cursor)
                .await,
        )),
        Request::DeleteSnapshot { aggregate_type, id } => Response::Unit(err_string(
            executor.delete_snapshot(aggregate_type, id).await,
        )),
    }
}
