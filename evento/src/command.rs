use actix::prelude::*;
use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};
use serde_json::json;
use validator::ValidationErrors;

use crate::{Aggregate, Event, Publisher, StoreEngine, StoreError};

#[derive(thiserror::Error, Debug)]
pub enum CommandError {
    #[error("internal server error")]
    SerdeJson(serde_json::Error),

    #[error("internal server error")]
    Evento(StoreError),

    #[error("{0}")]
    ValidationErrors(ValidationErrors),

    #[error("{0} `{1}` does not exist")]
    NotFound(String, String),

    #[error("{0}")]
    BadRequest(String),

    #[error("internal server error")]
    InternalServerErr(String),
}

impl From<serde_json::Error> for CommandError {
    fn from(e: serde_json::Error) -> Self {
        CommandError::SerdeJson(e)
    }
}

impl From<ValidationErrors> for CommandError {
    fn from(e: ValidationErrors) -> Self {
        CommandError::ValidationErrors(e)
    }
}

impl From<StoreError> for CommandError {
    fn from(e: StoreError) -> Self {
        CommandError::Evento(e)
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
                Err(e) => {
                    return match e {
                        CommandError::NotFound(_, _) => HttpResponse::NotFound().json(json!({
                            "code": "not_found",
                            "reason": e.to_string()
                        })),
                        CommandError::ValidationErrors(errors) => {
                            HttpResponse::BadRequest().json(json!({
                                "code": "validation_errors",
                                "errors": errors
                            }))
                        }
                        CommandError::BadRequest(_) => HttpResponse::BadRequest().json(json!({
                            "code": "validation_errors",
                            "reason": e.to_string()
                        })),
                        _ => {
                            tracing::error!("{}", e);

                            HttpResponse::InternalServerError().json(json!({
                                "code": "internal_server_error",
                                "reason": e.to_string()
                            }))
                        }
                    }
                }
            },
            Err(e) => {
                return HttpResponse::InternalServerError().json(json!({
                    "code": "internal_server_error",
                    "reason": e.to_string()
                }))
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
            Err(e) => HttpResponse::InternalServerError().json(json!({
                "code": "internal_server_error",
                "reason": e.to_string()
            })),
        }
    }
}
