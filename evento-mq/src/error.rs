#[derive(Debug, thiserror::Error)]
pub enum MqError {
    #[cfg(feature = "pg")]
    #[error("sqlx `{0}`")]
    Sqlx(#[from] sqlx::Error),

    #[error("serde_json `{0}`")]
    SerdeJson(#[from] serde_json::Error),

    #[error("{0}`")]
    Any(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, MqError>;
