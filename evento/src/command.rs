use actix::prelude::*;
use actix_web::{http::StatusCode, HttpResponse, HttpResponseBuilder, ResponseError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use validator::ValidationErrors;

use crate::{Aggregate, Event, Publisher, StoreEngine, StoreError};

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

impl CommandError {
    pub fn into_response(self) -> Result<HttpResponse, Self> {
        Err(self)
    }
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CommandInfo {
    pub aggregate_id: String,
    pub original_version: i32,
    pub events: Vec<Event>,
}

impl From<Event> for CommandInfo {
    fn from(e: Event) -> Self {
        Self {
            aggregate_id: e.aggregate_id.to_owned(),
            original_version: e.version,
            events: vec![e],
        }
    }
}

pub type CommandResult = Result<CommandInfo, CommandError>;

pub struct CommandResponse(pub Result<CommandResult, MailboxError>);

impl CommandResponse {
    pub async fn to_response<A: Aggregate, S: StoreEngine + Send + Sync + 'static>(
        &self,
        publisher: &Publisher<S>,
    ) -> HttpResponse {
        let info = match &self.0 {
            Ok(res) => match res {
                Ok(event) => event,
                Err(e) => return HttpResponse::from_error(e.clone()),
            },
            Err(e) => {
                return HttpResponse::from_error(CommandError::InternalServerErr(e.to_string()))
            }
        };

        let res = publisher
            .publish::<A, _>(
                &info.aggregate_id,
                info.events.clone(),
                info.original_version,
            )
            .await;

        match res {
            Ok(_) => HttpResponse::Ok().json(json!({
                "id": info.aggregate_id
            })),
            Err(e) => HttpResponse::from_error(Into::<CommandError>::into(e)),
        }
    }
}
