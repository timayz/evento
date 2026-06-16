//! Conversions between the generated protobuf types and `evento_core` types.
//!
//! Event `data` and metadata values are opaque bytes — they pass through
//! untouched. Pagination cursors are opaque strings. `u16` core fields arrive
//! as protobuf `uint32` and are range-validated here.

use std::collections::HashMap;

use evento_core::{
    cursor::{self, Args, Edge, ReadResult, Value},
    metadata::Metadata,
    Event, EventFilter, RoutingKey,
};
use tonic::Status;

use crate::proto;

/// Validates a protobuf `uint32` that maps to a core `u16`.
pub(crate) fn u16_field(value: u32, field: &str) -> Result<u16, Status> {
    u16::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("`{field}` exceeds the maximum of 65535")))
}

/// Empty list -> `None` (read everything); otherwise the mapped filters.
pub(crate) fn event_filters(filters: Vec<proto::EventFilter>) -> Option<Vec<EventFilter>> {
    if filters.is_empty() {
        return None;
    }
    Some(
        filters
            .into_iter()
            .map(|f| EventFilter {
                aggregate_type: f.aggregate_type,
                aggregate_id: f.aggregate_id,
                name: f.name,
            })
            .collect(),
    )
}

/// Absent message -> `None`; otherwise maps onto evento's `RoutingKey` enum.
pub(crate) fn routing_key(rk: Option<proto::RoutingKey>) -> Option<RoutingKey> {
    rk.map(|rk| {
        if rk.all {
            RoutingKey::All
        } else {
            RoutingKey::Value(rk.value)
        }
    })
}

/// Maps pagination arguments, validating the `u16` limits.
pub(crate) fn args(args: Option<proto::Args>) -> Result<Args, Status> {
    let Some(args) = args else {
        return Ok(Args::default());
    };
    Ok(Args {
        first: args.first.map(|v| u16_field(v, "args.first")).transpose()?,
        after: args.after.map(Value),
        last: args.last.map(|v| u16_field(v, "args.last")).transpose()?,
        before: args.before.map(Value),
    })
}

/// Reconstructs core [`Metadata`] from wire data, preserving arbitrary keys.
pub(crate) fn metadata(meta: Option<proto::Metadata>) -> Metadata {
    let Some(meta) = meta else {
        return Metadata::default();
    };
    let id = if meta.id.is_empty() {
        ulid::Ulid::new().to_string()
    } else {
        meta.id
    };
    Metadata::from_parts(id, meta.meta)
}

/// Serializes a core [`Event`] to its protobuf form (opaque payloads verbatim).
pub(crate) fn event_to_proto(event: Event) -> proto::Event {
    let meta: HashMap<String, Vec<u8>> = event
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    proto::Event {
        id: event.id.to_string(),
        aggregate_id: event.aggregate_id,
        aggregate_type: event.aggregate_type,
        version: event.version as u32,
        name: event.name,
        routing_key: event.routing_key,
        data: event.data,
        metadata: Some(proto::Metadata {
            id: event.metadata.id,
            meta,
        }),
        timestamp: event.timestamp,
        timestamp_subsec: event.timestamp_subsec,
    }
}

fn value_to_string(v: Value) -> String {
    v.0
}

/// Maps a paginated read result into the gRPC response.
pub(crate) fn read_result_to_proto(result: ReadResult<Event>) -> proto::ReadResponse {
    let edges = result
        .edges
        .into_iter()
        .map(|Edge { cursor, node }| proto::Edge {
            cursor: value_to_string(cursor),
            node: Some(event_to_proto(node)),
        })
        .collect();

    let cursor::PageInfo {
        has_previous_page,
        has_next_page,
        start_cursor,
        end_cursor,
    } = result.page_info;

    proto::ReadResponse {
        edges,
        page_info: Some(proto::PageInfo {
            has_previous_page,
            has_next_page,
            start_cursor: start_cursor.map(value_to_string),
            end_cursor: end_cursor.map(value_to_string),
        }),
    }
}
