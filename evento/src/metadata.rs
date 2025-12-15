use super::EventDetails;
use bincode::{Decode, Encode};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("User not found")]
    NotFound,
}

#[derive(Encode, Decode, Clone)]
pub enum MetadataUser {
    Anonyme,
    User(String),
    Root(String, String),
}

#[derive(Encode, Decode, Clone)]
pub struct Metadata {
    pub id: String,
    pub user: MetadataUser,
}

impl Metadata {
    pub fn new(user_id: impl Into<String>) -> Self {
        let user_id = user_id.into();

        Self {
            user: MetadataUser::User(user_id),
            ..Default::default()
        }
    }

    pub fn root(user_id: impl Into<String>, root_id: impl Into<String>) -> Self {
        let user_id = user_id.into();
        let root_id = root_id.into();

        Self {
            user: MetadataUser::Root(user_id, root_id),
            ..Default::default()
        }
    }

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

pub type Event<D> = EventDetails<D, Metadata>;
