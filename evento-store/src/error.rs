#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("unexpected original version while saving event")]
    UnexpectedOriginalVersion,

    #[error("metadata must be an object")]
    MetadataInvalidObjectType,

    #[error("unexpected empty events when trying to write single event")]
    EmptyWriteEvent,

    #[cfg(feature = "pg")]
    #[error("sqlx `{0}`")]
    Sqlx(#[from] sqlx::Error),

    #[error("serde_json `{0}`")]
    SerdeJson(#[from] serde_json::Error),

    #[error("sdt::num `{0}`")]
    TryFromInt(#[from] std::num::TryFromIntError),

    #[error("evento_query`{0}`")]
    Query(#[from] evento_query::QueryError),

    #[error("{0}`")]
    Any(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;
