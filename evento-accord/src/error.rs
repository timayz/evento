//! Error types for ACCORD consensus.

use crate::txn::{Ballot, TxnId, TxnStatus};
use thiserror::Error;

/// Errors that can occur during ACCORD consensus.
#[derive(Error, Debug)]
pub enum AccordError {
    /// Transaction not found in the store
    #[error("transaction {0} not found")]
    TxnNotFound(TxnId),

    /// Transaction already exists
    #[error("transaction {0} already exists")]
    TxnExists(TxnId),

    /// Invalid transaction status transition
    #[error("invalid transaction status: expected {expected}, got {actual}")]
    InvalidStatus {
        expected: TxnStatus,
        actual: TxnStatus,
    },

    /// Ballot is too low (another coordinator has a higher ballot)
    #[error("ballot {attempted} is lower than existing ballot {existing}")]
    BallotTooLow { attempted: Ballot, existing: Ballot },

    /// Quorum not reached for consensus
    #[error("quorum not reached: got {got}, need {need}")]
    QuorumNotReached { got: usize, need: usize },

    /// Node is not reachable
    #[error("node {0} not reachable")]
    NodeUnreachable(u16),

    /// Operation timed out
    #[error("timeout waiting for {0}")]
    Timeout(String),

    /// Dependency cycle detected
    #[error("dependency cycle detected involving transaction {0}")]
    DependencyCycle(TxnId),

    /// Storage error from underlying executor
    #[error("storage error: {0}")]
    Storage(#[from] evento_core::WriteError),

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Network/transport error
    #[error("transport error: {0}")]
    Transport(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Execution failed (e.g., version conflict, constraint violation)
    #[error("execution failed for {txn_id}: {error}")]
    ExecutionFailed { txn_id: TxnId, error: String },
}

impl AccordError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NodeUnreachable(_)
                | Self::Timeout(_)
                | Self::QuorumNotReached { .. }
                | Self::Transport(_)
        )
    }

    /// Check if this error indicates a conflict that requires coordination
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::BallotTooLow { .. })
    }
}

/// Result type for ACCORD operations
pub type Result<T> = std::result::Result<T, AccordError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timestamp::Timestamp;

    #[test]
    fn test_error_display() {
        let txn_id = TxnId::new(Timestamp::new(100, 0, 1));
        let err = AccordError::TxnNotFound(txn_id);
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_error_retryable() {
        assert!(AccordError::NodeUnreachable(1).is_retryable());
        assert!(AccordError::Timeout("test".to_string()).is_retryable());
        assert!(AccordError::QuorumNotReached { got: 1, need: 2 }.is_retryable());

        let txn_id = TxnId::new(Timestamp::new(100, 0, 1));
        assert!(!AccordError::TxnNotFound(txn_id).is_retryable());
    }

    #[test]
    fn test_error_conflict() {
        let ballot1 = Ballot::new(1, 1);
        let ballot2 = Ballot::new(2, 1);

        let err = AccordError::BallotTooLow {
            attempted: ballot1,
            existing: ballot2,
        };

        assert!(err.is_conflict());
        assert!(!AccordError::NodeUnreachable(1).is_conflict());
    }
}
