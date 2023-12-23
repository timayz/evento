/// Error type representing possible errors that may occur during database queries.
///
/// The `QueryError` enum encompasses various error variants related to missing fields,
/// parsing errors, SQLx errors, base64 decoding errors, UTF-8 string decoding errors,
/// Harsh encoding/decoding errors, and unknown errors.
///
/// # Example
///
/// ```rust
/// use evento_query::QueryError;
/// use sqlx::error::Error as SqlxError;
/// use base64::DecodeError as Base64DecodeError;
/// use std::str::Utf8Error;
/// use harsh::Error as HarshError;
/// use chrono::ParseError;
///
/// fn handle_query_error(error: QueryError) {
///     match error {
///         QueryError::MissingField(field) => {
///             // Handle MissingField error
///         }
///         QueryError::ChronoParseError(parse_error) => {
///             // Handle ChronoParseError
///         }
///         QueryError::Sqlx(sqlx_error) => {
///             // Handle SQLx error
///             if let SqlxError::Database(db_error) = sqlx_error {
///                 // Handle specific database error
///             }
///         }
///         QueryError::Base64(base64_error) => {
///             // Handle Base64 decoding error
///         }
///         QueryError::StrUtf8(utf8_error) => {
///             // Handle UTF-8 decoding error
///         }
///         QueryError::Harsh(harsh_error) => {
///             // Handle Harsh encoding/decoding error
///         }
///         QueryError::Unknown(field, value, description) => {
///             // Handle unknown error with additional details
///         }
///     }
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// Error variant indicating a missing field.
    #[error("{0}")]
    MissingField(String),

    /// Error variant indicating a chrono parsing error.
    #[error("chrono: {0}")]
    ChronoParseError(#[from] chrono::ParseError),

    /// Error variant indicating an SQLx error.
    #[cfg(feature = "pg")]
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// Error variant indicating a base64 decoding error.
    #[error("base64: {0}")]
    Base64(#[from] base64::DecodeError),

    /// Error variant indicating a UTF-8 decoding error.
    #[error("str utf8: {0}")]
    StrUtf8(#[from] std::str::Utf8Error),

    /// Error variant indicating a Harsh encoding/decoding error.
    #[error("harsh: {0}")]
    Harsh(#[from] harsh::Error),

    /// Error variant indicating an unknown error with additional details.
    #[error("{0}")]
    Unknown(String, String, String),
}
