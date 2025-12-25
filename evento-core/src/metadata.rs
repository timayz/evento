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

use crate::projection::EventData;

use thiserror::Error;
use ulid::Ulid;

/// Errors when accessing metadata fields.
#[derive(Debug, Error)]
pub enum MetadataError {
    /// User information not available (anonymous metadata)
    #[error("User not found")]
    NotFound,
}

/// User identification for event metadata.
#[derive(Clone, bitcode::Encode, bitcode::Decode)]
pub enum MetadataUser {
    /// Anonymous/system action
    Anonyme,
    /// Action by a specific user
    User(String),
    /// Action by a root user on behalf of another (root_id, user_id)
    Root(String, String),
}

/// Standard event metadata.
///
/// Contains a unique ID and user identification. Default creates
/// anonymous metadata with an auto-generated ULID.
#[derive(Clone, bitcode::Encode, bitcode::Decode)]
pub struct Metadata {
    /// Unique metadata ID (ULID)
    pub id: String,
    /// User who triggered the event
    pub user: MetadataUser,
}

impl Metadata {
    /// Creates metadata for a user action.
    pub fn new(user_id: impl Into<String>) -> Self {
        let user_id = user_id.into();

        Self {
            user: MetadataUser::User(user_id),
            ..Default::default()
        }
    }

    /// Creates metadata for a root/admin action on behalf of a user.
    pub fn root(user_id: impl Into<String>, root_id: impl Into<String>) -> Self {
        let user_id = user_id.into();
        let root_id = root_id.into();

        Self {
            user: MetadataUser::Root(user_id, root_id),
            ..Default::default()
        }
    }

    /// Gets the user ID from the metadata.
    ///
    /// Returns `MetadataError::NotFound` for anonymous metadata.
    pub fn user(&self) -> Result<String, MetadataError> {
        match self.user.to_owned() {
            MetadataUser::User(id) | MetadataUser::Root(_, id) => Ok(id),
            _ => Err(MetadataError::NotFound),
        }
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            id: Ulid::new().to_string(),
            user: MetadataUser::Anonyme,
        }
    }
}

/// Type alias for events with standard [`Metadata`].
///
/// This is the recommended event type for most applications.
///
/// # Example
///
/// ```rust,ignore
/// use evento::metadata::Event;
///
/// #[evento::handler]
/// async fn handle_event<E: Executor>(
///     event: Event<MyEventData>,
///     action: Action<'_, MyView, E>,
/// ) -> anyhow::Result<()> {
///     // Access event data
///     println!("Amount: {}", event.data.amount);
///
///     // Access metadata
///     if let Ok(user_id) = event.metadata.user() {
///         println!("User: {}", user_id);
///     }
///
///     Ok(())
/// }
/// ```
pub type Event<D> = EventData<D, Metadata>;
