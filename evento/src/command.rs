use actix::prelude::*;
use actix_web::{http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError};
use serde_json::json;
use validator::ValidationErrors;

use crate::StoreError;

#[derive(thiserror::Error, Debug, Clone)]
pub enum CommandError {
    #[error("{0}")]
    ValidationErrors(ValidationErrors),

    #[error("{0} `{1}` does not exist")]
    NotFound(String, String),

    #[error("{0}")]
    BadRequest(String),

    #[error("internal server error")]
    InternalServerErr(String),
}

impl ResponseError for CommandError {
    fn status_code(&self) -> StatusCode {
        match *self {
            CommandError::InternalServerErr(_) => StatusCode::INTERNAL_SERVER_ERROR,
            CommandError::BadRequest(_) => StatusCode::BAD_REQUEST,
            CommandError::NotFound(_, _) => StatusCode::NOT_FOUND,
            CommandError::ValidationErrors(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let mut res = HttpResponseBuilder::new(self.status_code());

        if let CommandError::InternalServerErr(e) = self {
            tracing::error!("{}", e);
        }

        res.json(
            serde_json::json!({"code": self.status_code().as_u16(), "message": self.to_string()}),
        )
    }
}

impl From<serde_json::Error> for CommandError {
    fn from(e: serde_json::Error) -> Self {
        CommandError::InternalServerErr(e.to_string())
    }
}

impl From<ValidationErrors> for CommandError {
    fn from(e: ValidationErrors) -> Self {
        CommandError::ValidationErrors(e)
    }
}

impl From<StoreError> for CommandError {
    fn from(e: StoreError) -> Self {
        CommandError::InternalServerErr(e.to_string())
    }
}

impl From<sqlx::Error> for CommandError {
    fn from(e: sqlx::Error) -> Self {
        CommandError::InternalServerErr(e.to_string())
    }
}

impl From<uuid::Error> for CommandError {
    fn from(e: uuid::Error) -> Self {
        CommandError::InternalServerErr(e.to_string())
    }
}

impl From<actix::MailboxError> for CommandError {
    fn from(e: actix::MailboxError) -> Self {
        CommandError::InternalServerErr(e.to_string())
    }
}

pub type CommandResult = Result<String, CommandError>;

pub struct CommandResponse(pub Result<CommandResult, MailboxError>);

impl Into<HttpResponse> for CommandResponse {
    fn into(self) -> HttpResponse {
        match &self.0 {
            Ok(res) => match res {
                Ok(aggregate_id) => HttpResponse::Ok().json(json!({ "id": aggregate_id })),
                Err(e) => return HttpResponse::from_error(e.clone()),
            },
            Err(e) => {
                return HttpResponse::from_error(CommandError::InternalServerErr(e.to_string()))
            }
        }
    }
}
