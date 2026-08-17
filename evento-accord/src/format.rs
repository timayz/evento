//! On-the-wire and on-disk record framing: a small, explicit version tag in front
//! of every bitcode payload, so a format change is detectable instead of silently
//! mis-parsed.
//!
//! bitcode is **positional and not self-describing**: a field added to or reordered
//! in [`CommandState`](crate::message::CommandState) or [`Message`](crate::message::Message)
//! would deserialize old bytes into garbage rather than fail. The 4-byte header here
//! guards that boundary — magic + format version + a record-kind discriminant — so a
//! reader rejects anything it does not understand.
//!
//! ```text
//! ┌────────┬────────┬──────────────┬──────────────────────────┐
//! │ MAGIC  │ format │  RecordKind  │   bitcode payload …       │
//! │ 2 byte │ 1 byte │   1 byte     │                           │
//! └────────┴────────┴──────────────┴──────────────────────────┘
//! ```
//!
//! ## Scope
//!
//! Only the **bitcode boundary** is tagged: the framed-TCP wire ([`crate::tcp`]) and
//! the disk journal *values* ([``evento_fjall::FjallJournal``]). The in-memory transport and
//! journal pass typed structs by value and never serialize, so they are untouched.
//! Journal **keys** are likewise left bare (a `TxnId` whose ordering the journal's
//! truncation scan relies on); the value carries the schema that evolves.
//!
//! ## Rolling-upgrade path
//!
//! The tag is what makes a future format change *safe*, not a compatibility layer for
//! the past — this is a pre-1.0 alpha, so today's tag is mandatory and there is no
//! reader for untagged records (an operator upgrading from an untagged build wipes the
//! journal and re-bootstraps via `join`, which transfers a fresh snapshot).
//!
//! - **Wire:** a node on a newer [`FORMAT_VERSION`] and a peer on an older one mutually
//!   *shed* each other's frames (decode returns `Err`, and the inbound loop already
//!   drops un-decodable frames — loss the consensus layer tolerates). So a cluster
//!   rolls **drain-and-replace**: keep a quorum on one version at a time; never restart
//!   every node onto a breaking format at once.
//! - **Journal:** to add a field, bump [`FORMAT_VERSION`] and extend [`decode_tagged`]
//!   with a per-version branch (e.g. deserialize the old layout, then fill new fields
//!   with defaults). The version byte is what selects that branch; without it the old
//!   bytes would parse as the new layout and corrupt state.

use serde::{de::DeserializeOwned, Serialize};

/// Identifies an evento-accord record. Guards against decoding an unrelated byte
/// stream (a misconfigured port, a different protocol) as a valid frame.
pub const MAGIC: [u8; 2] = *b"Ac";

/// Current record format version. Bump on any breaking change to a tagged payload's
/// layout (a new/reordered [`CommandState`](crate::message::CommandState) or
/// [`Message`](crate::message::Message) field); add the matching decode branch then.
pub const FORMAT_VERSION: u8 = 1;

/// Length of the fixed header: `MAGIC (2) + version (1) + kind (1)`.
const HEADER_LEN: usize = 4;

/// Which payload schema a tagged record carries. Lets the wire frame and the two
/// journal value-types share one codec while staying independently identifiable, so a
/// journal value can never be mistaken for a wire frame or vice versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// A [`crate::tcp`] wire frame (sender id + [`Message`](crate::message::Message)).
    WireFrame = 1,
    /// A journal [`CommandState`](crate::message::CommandState).
    Command = 2,
    /// The journal truncation watermark.
    Watermark = 3,
    /// A decided metadata-log entry — one epoch's committed topology layout.
    MetadataEntry = 4,
    /// A config-Paxos acceptor's durable state for one epoch.
    AcceptorState = 5,
}

impl RecordKind {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::WireFrame),
            2 => Some(Self::Command),
            3 => Some(Self::Watermark),
            4 => Some(Self::MetadataEntry),
            5 => Some(Self::AcceptorState),
            _ => None,
        }
    }
}

/// Serializes `value` as a tagged record: the 4-byte header followed by its bitcode
/// encoding.
pub fn encode_tagged<T: Serialize>(kind: RecordKind, value: &T) -> anyhow::Result<Vec<u8>> {
    let payload = bitcode::serialize(value)?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.push(kind as u8);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Deserializes a tagged record, validating the header first. Fails loudly — bad magic,
/// an unknown format version, or a mismatched kind all error rather than risk decoding
/// the payload under the wrong schema.
pub fn decode_tagged<T: DeserializeOwned>(expected: RecordKind, bytes: &[u8]) -> anyhow::Result<T> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("record too short: {} bytes", bytes.len());
    }
    if bytes[0..2] != MAGIC {
        anyhow::bail!("bad record magic");
    }
    let version = bytes[2];
    if version != FORMAT_VERSION {
        anyhow::bail!(
            "unsupported record format version {version} (this build speaks {FORMAT_VERSION})"
        );
    }
    let kind = RecordKind::from_byte(bytes[3])
        .ok_or_else(|| anyhow::anyhow!("unknown record kind {}", bytes[3]))?;
    if kind != expected {
        anyhow::bail!("record kind mismatch: expected {expected:?}, found {kind:?}");
    }
    Ok(bitcode::deserialize(&bytes[HEADER_LEN..])?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_each_kind() {
        for kind in [
            RecordKind::WireFrame,
            RecordKind::Command,
            RecordKind::Watermark,
            RecordKind::MetadataEntry,
            RecordKind::AcceptorState,
        ] {
            let value = (42u64, "hello".to_string());
            let bytes = encode_tagged(kind, &value).unwrap();
            let back: (u64, String) = decode_tagged(kind, &bytes).unwrap();
            assert_eq!(back, value);
        }
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode_tagged(RecordKind::Command, &7u64).unwrap();
        bytes[0] = b'X';
        let err = decode_tagged::<u64>(RecordKind::Command, &bytes).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = encode_tagged(RecordKind::Command, &7u64).unwrap();
        bytes[2] = FORMAT_VERSION + 1;
        let err = decode_tagged::<u64>(RecordKind::Command, &bytes).unwrap_err();
        assert!(err.to_string().contains("format version"));
    }

    #[test]
    fn rejects_kind_mismatch() {
        let bytes = encode_tagged(RecordKind::Command, &7u64).unwrap();
        let err = decode_tagged::<u64>(RecordKind::Watermark, &bytes).unwrap_err();
        assert!(err.to_string().contains("kind mismatch"));
    }

    #[test]
    fn rejects_truncated_input() {
        let bytes = [b'A', b'c', FORMAT_VERSION]; // one byte short of the header
        let err = decode_tagged::<u64>(RecordKind::Command, &bytes).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }
}
