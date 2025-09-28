//! Unified error types for the evento library
//!
//! This module consolidates all error types used throughout the evento library
//! to provide a consistent error handling experience and reduce code duplication.

use thiserror::Error;

/// Top-level error type for all evento operations
#[derive(Debug, Error)]
pub enum EventoError {
    /// Read-related errors (loading aggregates, reading events)
    #[error("read error: {0}")]
    Read(#[from] ReadError),
    
    /// Write-related errors (saving events, snapshots)
    #[error("write error: {0}")]
    Write(#[from] WriteError),
    
    /// Subscription-related errors
    #[error("subscription error: {0}")]
    Subscribe(#[from] SubscribeError),
    
    /// Acknowledgment errors
    #[error("acknowledgment error: {0}")]
    Acknowledge(#[from] AcknowledgeError),
}

/// Errors that can occur during read operations
#[derive(Debug, Error)]
pub enum ReadError {
    /// Requested aggregate or event not found
    #[error("not found")]
    NotFound,

    /// Too many events to process (pagination limit exceeded)
    #[error("too many events to aggregate")]
    TooManyEvents,

    /// Serialization/encoding errors
    #[error("encoding error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Deserialization/decoding errors  
    #[error("decoding error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// Base64 decoding errors
    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    /// Write operation failed during read (e.g., snapshot creation)
    #[error("write operation during read failed: {0}")]
    Write(#[from] WriteError),

    /// Unknown/unexpected errors
    #[error("unknown error: {0}")]
    Unknown(#[from] anyhow::Error),
}

/// Errors that can occur during write operations
#[derive(Debug, Error)]
pub enum WriteError {
    /// Version conflict during optimistic concurrency control
    #[error("invalid original version - possible concurrent modification")]
    InvalidOriginalVersion,

    /// No event data provided
    #[error("missing event data")]
    MissingData,

    /// No metadata provided
    #[error("missing metadata")]
    MissingMetadata,

    /// Serialization/encoding errors
    #[error("encoding error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// System time errors
    #[error("system time error: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    /// Unknown/unexpected errors
    #[error("unknown error: {0}")]
    Unknown(#[from] anyhow::Error),
}

/// Errors that can occur during subscription operations
#[derive(Debug, Error)]
pub enum SubscribeError {
    /// Read operation failed during subscription
    #[error("read error during subscription: {0}")]
    Read(#[from] ReadError),

    /// ULID decoding errors
    #[error("ULID decode error: {0}")]
    UlidDecode(#[from] ulid::DecodeError),

    /// Unknown/unexpected errors
    #[error("unknown error: {0}")]
    Unknown(#[from] anyhow::Error),
}

/// Errors that can occur during acknowledgment operations
#[derive(Debug, Error)]
pub enum AcknowledgeError {
    /// Unknown/unexpected errors
    #[error("unknown error: {0}")]
    Unknown(#[from] anyhow::Error),
}

// Note: Individual error types are directly available in this module
// The main EventoError provides a unified interface