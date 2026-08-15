//! Wire protocol: tagged bitcode frames over a length-delimited TCP stream.
//!
//! Every frame is a 4-byte header (magic + format version + direction) followed
//! by the bitcode encoding of a [`ClientFrame`] or [`ServerFrame`]. bitcode is
//! positional and not self-describing, so the header is what turns a schema or
//! dialect mismatch into a loud error instead of garbage. The magic (`Er`) is
//! distinct from evento-accord's (`Ac`), so pointing a remote client at an
//! accord port fails immediately.

use evento_core::cursor::{Args, PageInfo, Value};
use evento_core::{Event, EventFilter, RoutingKey, WriteError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use ulid::Ulid;

/// Identifies an evento-remote frame. Guards against decoding an unrelated
/// byte stream (a misconfigured port, a different protocol) as a valid frame.
pub const MAGIC: [u8; 2] = *b"Er";

/// Current wire format version. Bump on any breaking change to the frame
/// enums' layout; add the matching decode branch then.
pub const FORMAT_VERSION: u8 = 1;

/// Length of the fixed header: `MAGIC (2) + version (1) + kind (1)`.
const HEADER_LEN: usize = 4;

/// Which direction a frame travels. A misconfigured peer that parses its own
/// dialect (client reading client frames) errors instead of mis-decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// A [`ClientFrame`] (client → server).
    ClientFrame = 1,
    /// A [`ServerFrame`] (server → client).
    ServerFrame = 2,
}

impl RecordKind {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ClientFrame),
            2 => Some(Self::ServerFrame),
            _ => None,
        }
    }
}

/// Frames above the codec's 8 MB default exist in practice (large read chunks,
/// snapshot blobs), so both sides raise the cap.
pub const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

/// Serializes `value` as a tagged frame: the 4-byte header followed by its
/// bitcode encoding.
pub fn encode_tagged<T: Serialize>(kind: RecordKind, value: &T) -> anyhow::Result<Vec<u8>> {
    let payload = bitcode::serialize(value)?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.push(kind as u8);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Deserializes a tagged frame, validating the header first. Fails loudly —
/// bad magic, an unknown format version, or a mismatched kind all error rather
/// than risk decoding the payload under the wrong schema.
pub fn decode_tagged<T: DeserializeOwned>(expected: RecordKind, bytes: &[u8]) -> anyhow::Result<T> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame too short: {} bytes", bytes.len());
    }
    if bytes[0..2] != MAGIC {
        anyhow::bail!("bad frame magic");
    }
    let version = bytes[2];
    if version != FORMAT_VERSION {
        anyhow::bail!(
            "unsupported wire format version {version} (this build speaks {FORMAT_VERSION})"
        );
    }
    let kind = RecordKind::from_byte(bytes[3])
        .ok_or_else(|| anyhow::anyhow!("unknown frame kind {}", bytes[3]))?;
    if kind != expected {
        anyhow::bail!("frame kind mismatch: expected {expected:?}, found {kind:?}");
    }
    Ok(bitcode::deserialize(&bytes[HEADER_LEN..])?)
}

/// A client → server frame: a request with a per-connection correlation id.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientFrame {
    Request { id: u64, request: Request },
}

/// A server → client frame: either a correlated reply or an unsolicited write
/// notification. Every frame piggybacks the server executor's
/// `stable_timestamp` so the client's cached watermark is at least as fresh as
/// the data it accompanies (the watermark only advances, so a stale-low cache
/// is merely conservative, never unsafe).
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerFrame {
    Response {
        id: u64,
        response: Response,
        stable_timestamp: Option<u64>,
    },
    /// Pushed after each server-side write; wakes client-side subscriptions
    /// without waiting for their poll interval.
    Notify {
        generation: u64,
        stable_timestamp: Option<u64>,
    },
}

/// One variant per [`Executor`](evento_core::Executor) method that crosses the
/// wire, plus the connect-time `Hello` exchange.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Sent once per connection; the reply carries connection-scoped facts the
    /// sync `Executor` methods need cached client-side.
    Hello,
    Write {
        #[serde(with = "wire_events")]
        events: Vec<Event>,
    },
    Read {
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    },
    LatestTimestamp {
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
    },
    GetSubscriberCursor {
        key: String,
    },
    IsSubscriberRunning {
        key: String,
        worker_id: Ulid,
    },
    UpsertSubscriber {
        key: String,
        worker_id: Ulid,
    },
    Acknowledge {
        key: String,
        cursor: Value,
        lag: u64,
    },
    GetSnapshot {
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    },
    SaveSnapshot {
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    },
    DeleteSnapshot {
        aggregate_type: String,
        id: String,
    },
}

/// Reply payloads. `anyhow` errors travel as their `Display` string and are
/// rewrapped client-side; `write` keeps variant fidelity via
/// [`WireWriteError`] because callers match on it (retry-on-conflict).
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Hello {
        default_routing_key: Option<String>,
    },
    Write(Result<(), WireWriteError>),
    Read(Result<WireReadResult, String>),
    LatestTimestamp(Result<u64, String>),
    SubscriberCursor(Result<Option<Value>, String>),
    SubscriberRunning(Result<bool, String>),
    /// upsert_subscriber, acknowledge, save_snapshot, delete_snapshot.
    Unit(Result<(), String>),
    Snapshot(Result<Option<(Vec<u8>, Value)>, String>),
}

/// `ReadResult<Event>` cannot derive serde (`Event` is not serde), so edges
/// travel as parallel `cursors`/`events` vectors and are re-zipped client-side.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireReadResult {
    pub cursors: Vec<Value>,
    #[serde(with = "wire_events")]
    pub events: Vec<Event>,
    pub page_info: PageInfo,
}

impl From<evento_core::cursor::ReadResult<Event>> for WireReadResult {
    fn from(result: evento_core::cursor::ReadResult<Event>) -> Self {
        let (cursors, events) = result.edges.into_iter().map(|e| (e.cursor, e.node)).unzip();
        WireReadResult {
            cursors,
            events,
            page_info: result.page_info,
        }
    }
}

impl From<WireReadResult> for evento_core::cursor::ReadResult<Event> {
    fn from(wire: WireReadResult) -> Self {
        evento_core::cursor::ReadResult {
            edges: wire
                .cursors
                .into_iter()
                .zip(wire.events)
                .map(|(cursor, node)| evento_core::cursor::Edge { cursor, node })
                .collect(),
            page_info: wire.page_info,
        }
    }
}

/// Serde mirror of [`WriteError`], which is not itself serializable.
/// `InvalidOriginalVersion` and `MissingData` map 1:1 — `WriteBuilder`'s
/// retry-on-conflict matches on the variant, so fidelity matters;
/// `Unknown`/`SystemTime` collapse to their `Display` strings.
#[derive(Debug, Serialize, Deserialize)]
pub enum WireWriteError {
    InvalidOriginalVersion,
    MissingData,
    Other(String),
}

impl From<&WriteError> for WireWriteError {
    fn from(e: &WriteError) -> Self {
        match e {
            WriteError::InvalidOriginalVersion => WireWriteError::InvalidOriginalVersion,
            WriteError::MissingData => WireWriteError::MissingData,
            other => WireWriteError::Other(other.to_string()),
        }
    }
}

impl From<WireWriteError> for WriteError {
    fn from(e: WireWriteError) -> Self {
        match e {
            WireWriteError::InvalidOriginalVersion => WriteError::InvalidOriginalVersion,
            WireWriteError::MissingData => WriteError::MissingData,
            WireWriteError::Other(msg) => WriteError::Unknown(anyhow::anyhow!(msg)),
        }
    }
}

// NOTE: keep in sync with evento-accord/src/message.rs `mod wire_events`.
/// Serde bridge for `Vec<Event>`. evento's [`Event`] is not itself
/// (de)serializable, so it is mirrored field-for-field by [`WireEvent`], whose
/// only non-trivial field is the bitcode-encoded
/// [`Metadata`](evento_core::metadata::Metadata). This lets the frame enums
/// derive serde without modifying evento-core.
mod wire_events {
    use evento_core::{metadata::Metadata, Event};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use ulid::Ulid;

    #[derive(Serialize, Deserialize)]
    struct WireEvent {
        id: Ulid,
        aggregate_id: String,
        aggregate_type: String,
        version: u16,
        name: String,
        routing_key: Option<String>,
        data: Vec<u8>,
        /// bitcode-encoded [`Metadata`].
        metadata: Vec<u8>,
        timestamp: u64,
        timestamp_subsec: u32,
    }

    impl From<&Event> for WireEvent {
        fn from(e: &Event) -> Self {
            WireEvent {
                id: e.id,
                aggregate_id: e.aggregate_id.clone(),
                aggregate_type: e.aggregate_type.clone(),
                version: e.version,
                name: e.name.clone(),
                routing_key: e.routing_key.clone(),
                data: e.data.clone(),
                metadata: bitcode::encode(&e.metadata),
                timestamp: e.timestamp,
                timestamp_subsec: e.timestamp_subsec,
            }
        }
    }

    impl TryFrom<WireEvent> for Event {
        type Error = bitcode::Error;

        fn try_from(w: WireEvent) -> Result<Self, Self::Error> {
            Ok(Event {
                id: w.id,
                aggregate_id: w.aggregate_id,
                aggregate_type: w.aggregate_type,
                version: w.version,
                name: w.name,
                routing_key: w.routing_key,
                data: w.data,
                metadata: bitcode::decode::<Metadata>(&w.metadata)?,
                timestamp: w.timestamp,
                timestamp_subsec: w.timestamp_subsec,
            })
        }
    }

    pub fn serialize<S: Serializer>(events: &[Event], s: S) -> Result<S::Ok, S::Error> {
        let wire: Vec<WireEvent> = events.iter().map(WireEvent::from).collect();
        wire.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Event>, D::Error> {
        Vec::<WireEvent>::deserialize(d)?
            .into_iter()
            .map(|w| Event::try_from(w).map_err(serde::de::Error::custom))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evento_core::metadata::Metadata;

    fn sample_event() -> Event {
        let mut metadata = Metadata::default();
        metadata.set_requested_by("tester");
        Event {
            id: Ulid::generate(),
            aggregate_id: "agg-1".to_owned(),
            aggregate_type: "account".to_owned(),
            version: 7,
            name: "opened".to_owned(),
            routing_key: Some("eu".to_owned()),
            data: vec![1, 2, 3],
            metadata,
            timestamp: 1_700_000_000,
            timestamp_subsec: 42,
        }
    }

    #[test]
    fn round_trips_each_kind() {
        for kind in [RecordKind::ClientFrame, RecordKind::ServerFrame] {
            let value = (42u64, "hello".to_string());
            let bytes = encode_tagged(kind, &value).unwrap();
            let back: (u64, String) = decode_tagged(kind, &bytes).unwrap();
            assert_eq!(back, value);
        }
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode_tagged(RecordKind::ClientFrame, &7u64).unwrap();
        bytes[0] = b'X';
        let err = decode_tagged::<u64>(RecordKind::ClientFrame, &bytes).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = encode_tagged(RecordKind::ClientFrame, &7u64).unwrap();
        bytes[2] = FORMAT_VERSION + 1;
        let err = decode_tagged::<u64>(RecordKind::ClientFrame, &bytes).unwrap_err();
        assert!(err.to_string().contains("format version"));
    }

    #[test]
    fn rejects_kind_mismatch() {
        let bytes = encode_tagged(RecordKind::ClientFrame, &7u64).unwrap();
        let err = decode_tagged::<u64>(RecordKind::ServerFrame, &bytes).unwrap_err();
        assert!(err.to_string().contains("kind mismatch"));
    }

    #[test]
    fn rejects_truncated_input() {
        let bytes = [b'E', b'r', FORMAT_VERSION]; // one byte short of the header
        let err = decode_tagged::<u64>(RecordKind::ClientFrame, &bytes).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn event_round_trip() {
        let event = sample_event();
        let frame = ClientFrame::Request {
            id: 9,
            request: Request::Write {
                events: vec![event.clone()],
            },
        };
        let bytes = encode_tagged(RecordKind::ClientFrame, &frame).unwrap();
        let back: ClientFrame = decode_tagged(RecordKind::ClientFrame, &bytes).unwrap();
        let ClientFrame::Request {
            id,
            request: Request::Write { events },
        } = back
        else {
            panic!("wrong frame");
        };
        assert_eq!(id, 9);
        assert_eq!(events, vec![event]);
    }

    #[test]
    fn read_result_round_trip() {
        let event = sample_event();
        let read = evento_core::cursor::ReadResult {
            edges: vec![evento_core::cursor::Edge {
                cursor: Value("cur".to_owned()),
                node: event,
            }],
            page_info: PageInfo {
                has_previous_page: true,
                has_next_page: false,
                start_cursor: Some(Value("cur".to_owned())),
                end_cursor: Some(Value("cur".to_owned())),
            },
        };
        let wire = WireReadResult::from(read.clone());
        let bytes = bitcode::serialize(&wire).unwrap();
        let back: WireReadResult = bitcode::deserialize(&bytes).unwrap();
        assert_eq!(evento_core::cursor::ReadResult::<Event>::from(back), read);
    }

    #[test]
    fn write_error_round_trip() {
        let cases = [
            WriteError::InvalidOriginalVersion,
            WriteError::MissingData,
            WriteError::Unknown(anyhow::anyhow!("boom")),
        ];
        for original in cases {
            let wire = WireWriteError::from(&original);
            let bytes = bitcode::serialize(&wire).unwrap();
            let back: WireWriteError = bitcode::deserialize(&bytes).unwrap();
            assert_eq!(
                WriteError::from(back).to_string(),
                original.to_string(),
                "message fidelity"
            );
        }
    }
}
