//! Mapping evento errors onto gRPC [`Status`] codes.

use evento_core::WriteError;
use tonic::{metadata::MetadataValue, Code, Status};

/// Trailing-metadata key carrying a stable, machine-readable error reason.
pub const REASON_KEY: &str = "evento-reason";

/// Reason value sent when a write hits an optimistic-concurrency conflict.
pub const REASON_INVALID_ORIGINAL_VERSION: &str = "INVALID_ORIGINAL_VERSION";

/// Maps a [`WriteError`] to a gRPC [`Status`].
///
/// `InvalidOriginalVersion` becomes `FAILED_PRECONDITION` (the client must
/// re-read the aggregate's current version before retrying) and carries the
/// `evento-reason = INVALID_ORIGINAL_VERSION` trailer so clients can detect it
/// without string-matching. Since `Write` only ever returns
/// `FAILED_PRECONDITION` for this case, the code alone is also a reliable
/// signal.
pub(crate) fn write_error_to_status(err: WriteError) -> Status {
    match err {
        WriteError::InvalidOriginalVersion => {
            let mut status = Status::new(Code::FailedPrecondition, "invalid original version");
            status.metadata_mut().insert(
                REASON_KEY,
                MetadataValue::from_static(REASON_INVALID_ORIGINAL_VERSION),
            );
            status
        }
        WriteError::MissingData => Status::invalid_argument("no events to write"),
        WriteError::SystemTime(err) => {
            tracing::error!(error = %err, "write failed: system time error");
            Status::internal("internal error")
        }
        WriteError::Unknown(err) => {
            tracing::error!(error = ?err, "write failed");
            Status::internal("internal error")
        }
    }
}

/// Maps an opaque executor error (from `read` / `latest_timestamp`) to
/// `INTERNAL`, logging the full chain server-side.
pub(crate) fn internal(err: anyhow::Error) -> Status {
    tracing::error!(error = ?err, "executor error");
    Status::internal("internal error")
}
