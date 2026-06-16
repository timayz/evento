//! The write path: turn a `WriteRequest` into committed events.
//!
//! This mirrors `evento_core::WriteBuilder::commit`: the server is authoritative
//! for event ids (ULID), versions (`original_version + 1..`), and timestamps,
//! and inherits an existing aggregate's routing key from its first event. It
//! cannot reuse `WriteBuilder` because that requires a typed, bitcode-encodable
//! event, whereas here the payload is opaque bytes.

use std::time::{SystemTime, UNIX_EPOCH};

use evento_core::{cursor::Args, Event, EventFilter, Executor};
use tonic::Status;
use ulid::Ulid;

use crate::{convert, error, proto};

pub(crate) async fn write<E: Executor>(
    executor: &E,
    req: proto::WriteRequest,
) -> Result<proto::WriteResponse, Status> {
    if req.events.is_empty() {
        return Err(Status::invalid_argument("no events to write"));
    }

    let original_version = convert::u16_field(req.original_version, "original_version")?;

    // The last assigned version must still fit in a u16.
    let count = req.events.len() as u64;
    let last_version = (original_version as u64) + count;
    if last_version > u16::MAX as u64 {
        return Err(Status::invalid_argument(
            "resulting version sequence exceeds the maximum of 65535",
        ));
    }
    let last_version = last_version as u16;

    let aggregate_type = req.aggregate_type;
    let aggregate_id = match req.aggregate_id {
        Some(id) if !id.is_empty() => id,
        _ => Ulid::new().to_string(),
    };

    // Inherit the aggregate's existing routing key (from its first event) so an
    // append cannot change it; fall back to the request's routing key.
    let first = executor
        .read(
            Some(vec![EventFilter::by_id(&aggregate_type, &aggregate_id)]),
            None,
            Args::forward(1, None),
        )
        .await
        .map_err(error::internal)?;
    let routing_key = match first.edges.first() {
        Some(edge) => edge.node.routing_key.clone(),
        None => req.routing_key,
    };

    let metadata = convert::metadata(req.metadata);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            tracing::error!(error = %err, "system time before unix epoch");
            Status::internal("internal error")
        })?;

    let events: Vec<Event> = (original_version + 1..)
        .zip(req.events)
        .map(|(version, new)| Event {
            id: Ulid::new(),
            aggregate_id: aggregate_id.clone(),
            aggregate_type: aggregate_type.clone(),
            version,
            name: new.name,
            routing_key: routing_key.clone(),
            data: new.data,
            metadata: metadata.clone(),
            timestamp: now.as_secs(),
            timestamp_subsec: now.subsec_millis(),
        })
        .collect();

    executor
        .write(events)
        .await
        .map_err(error::write_error_to_status)?;

    Ok(proto::WriteResponse {
        aggregate_id,
        last_version: last_version as u32,
    })
}
