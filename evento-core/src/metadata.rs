//! Standard event metadata types.
//!
//! This module provides standard metadata types for events, including
//! user identification and unique metadata IDs.
//!
//! # Types
//!
//! - [`Metadata`] - Standard metadata with ID and user info
//! - [`MetadataUser`] - User identification (anonymous, user, or root)
//! - [`Event`] - Type alias for `EventData<D, Metadata>`
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::metadata::Metadata;
//!
//! // Create metadata for a user action
//! let metadata = Metadata::new("user-123");
//!
//! // Create metadata for a root/admin action
//! let metadata = Metadata::root("admin-456", "impersonated-user-789");
//!
//! // Use with event creation
//! create()
//!     .event(&my_event)
//!     .metadata(&metadata)
//!     .commit(&executor)
//!     .await?;
//! ```

use std::{collections::HashMap, ops::Deref};
use thiserror::Error;
use ulid::Ulid;

const REQUESTED_BY: &str = "REQUESTED_BY";
const REQUESTED_AS: &str = "REQUESTED_AS";

/// Errors when accessing metadata fields.
#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("not found")]
    NotFound,

    #[error("decode: {0}")]
    Decode(#[from] bitcode::Error),
}

/// Standard event metadata.
///
/// Contains a unique ID and user identification. Default creates
/// anonymous metadata with an auto-generated ULID.
#[derive(Clone, bitcode::Encode, bitcode::Decode)]
pub struct Metadata {
    /// Unique metadata ID (ULID)
    pub id: String,
    meta: HashMap<String, Vec<u8>>,
}

impl Metadata {
    pub(crate) fn insert_enc<V: bitcode::Encode>(
        &mut self,
        key: impl Into<String>,
        value: &V,
    ) -> &mut Self {
        self.meta.insert(key.into(), bitcode::encode(value));

        self
    }

    pub fn try_get<D: bitcode::DecodeOwned>(&self, key: &str) -> Result<D, MetadataError> {
        let Some(value) = self.meta.get(key) else {
            return Err(MetadataError::NotFound);
        };

        Ok(bitcode::decode(value)?)
    }

    pub fn set_requested_as(&mut self, value: impl Into<String>) -> &mut Self {
        let value = value.into();
        self.insert_enc(REQUESTED_AS, &value);

        self
    }

    pub fn requested_as(&self) -> Result<String, MetadataError> {
        self.try_get(REQUESTED_AS)
    }

    pub fn set_requested_by(&mut self, value: impl Into<String>) -> &mut Self {
        let value = value.into();
        self.insert_enc(REQUESTED_BY, &value);

        self
    }

    pub fn requested_by(&self) -> Result<String, MetadataError> {
        self.try_get(REQUESTED_BY)
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            id: Ulid::new().to_string(),
            meta: Default::default(),
        }
    }
}

impl Deref for Metadata {
    type Target = HashMap<String, Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl From<&Metadata> for Metadata {
    fn from(value: &Metadata) -> Self {
        value.clone()
    }
}
