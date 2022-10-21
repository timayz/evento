use actix::prelude::*;
use actix_web::HttpResponse;
use evento::{Aggregate, Engine, Event, EventStore, RbatisEngine};
use rbatis::Rbatis;
use serde_json::json;
use validator::ValidationErrors;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("internal server error")]
    SerdeJson(serde_json::Error),

    #[error("internal server error")]
    Evento(evento::Error),

    #[error("{0}")]
    ValidationErrors(ValidationErrors),

    #[error("{0} `{1}` does not exist")]
    NotFound(String, String),

    #[error("{0}")]
    BadRequest(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerdeJson(e)
    }
}

impl From<ValidationErrors> for Error {
    fn from(e: ValidationErrors) -> Self {
        Error::ValidationErrors(e)
    }
}

impl From<evento::Error> for Error {
    fn from(e: evento::Error) -> Self {
        Error::Evento(e)
    }
}

pub type CommandResult = Result<Event, Error>;

pub struct Command {
    pub store: EventStore<RbatisEngine>,
}

impl Command {
    pub fn new(rb: Rbatis) -> Self {
        Self {
            store: RbatisEngine::new(rb),
        }
    }
}

impl Actor for Command {
    type Context = Context<Self>;
}

pub struct CommandResponse(pub Result<CommandResult, MailboxError>);

impl CommandResponse {
    pub async fn into_http_response<A: Aggregate>(
        &self,
        store: &EventStore<RbatisEngine>,
    ) -> HttpResponse {
        let event = match &self.0 {
            Ok(res) => match res {
                Ok(event) => event,
                Err(e) => {
                    return match e {
                        Error::NotFound(_, _) => HttpResponse::NotFound().json(json!({
                            "code": "not_found",
                            "reason": e.to_string()
                        })),
                        Error::ValidationErrors(errors) => HttpResponse::BadRequest().json(json!({
                            "code": "validation_errors",
                            "errors": errors
                        })),
                        Error::BadRequest(_) => HttpResponse::BadRequest().json(json!({
                            "code": "validation_errors",
                            "reason": e.to_string()
                        })),
                        _ => HttpResponse::InternalServerError().json(json!({
                            "code": "internal_server_error",
                            "reason": e.to_string()
                        })),
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

        let res = store
            .save::<A, _>(&event.aggregate_id, vec![event.clone()], event.version)
            .await;

        match res {
            Ok(_) => HttpResponse::Ok().json(json!({
                "id": event.aggregate_id
            })),
            Err(e) => HttpResponse::InternalServerError().json(json!({
                "code": "internal_server_error",
                "reason": e.to_string()
            })),
        }
    }
}
