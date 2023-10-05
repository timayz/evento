#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    MissingField(String),
    #[error("chrono: {0}")]
    ChronoParseError(#[from] chrono::ParseError),
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("str utf8: {0}")]
    StrUtf8(#[from] std::str::Utf8Error),
    #[error("harsh: {0}")]
    Harsh(#[from] harsh::Error),
    #[error("{0}")]
    Unknown(String, String, String),
}
