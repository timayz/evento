use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

pub struct CommandResponse(pub Result<String, anyhow::Error>);

impl From<CommandResponse> for Result<Json<Value>, CommandError> {
    fn from(value: CommandResponse) -> Self {
        let id = value.0?;

        Ok(Json(json!({"id": id})))
    }
}

pub struct CommandError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for CommandError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for CommandError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

pub type CommandResult<T> = Result<T, CommandError>;
