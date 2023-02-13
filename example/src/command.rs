use actix::prelude::*;
use actix_web::HttpResponse;
use evento::store::{Engine, PgEngine};
use evento::{Aggregate, Event, EventStore};
use pulsar::{producer, DeserializeMessage, Payload, Producer, SerializeMessage, TokioExecutor};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use validator::ValidationErrors;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("internal server error")]
    SerdeJson(serde_json::Error),

    #[error("internal server error")]
    Pulsar(pulsar::Error),

    #[error("internal server error")]
    Evento(evento::StoreError),

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

impl From<pulsar::Error> for Error {
    fn from(e: pulsar::Error) -> Self {
        Error::Pulsar(e)
    }
}

impl From<ValidationErrors> for Error {
    fn from(e: ValidationErrors) -> Self {
        Error::ValidationErrors(e)
    }
}

impl From<evento::StoreError> for Error {
    fn from(e: evento::StoreError) -> Self {
        Error::Evento(e)
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CommandMessage {
    pub producer: String,
    pub event: Event,
}

impl SerializeMessage for CommandMessage {
    fn serialize_message(input: Self) -> Result<producer::Message, pulsar::Error> {
        let payload =
            serde_json::to_vec(&input).map_err(|e| pulsar::Error::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for CommandMessage {
    type Output = Result<CommandMessage, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

pub type CommandResult = Result<CommandInfo, Error>;

pub struct Command {
    pub store: EventStore<PgEngine>,
}

impl Command {
    pub fn new(pool: PgPool) -> Self {
        Self {
            store: PgEngine::new(pool),
        }
    }
}

impl Actor for Command {
    type Context = Context<Self>;
}

pub struct CommandResponse(pub Result<CommandResult, MailboxError>);

impl CommandResponse {
    pub async fn to_response<A: Aggregate>(
        &self,
        store: &EventStore<PgEngine>,
        producer: &mut Producer<TokioExecutor>,
    ) -> HttpResponse {
        let info = match &self.0 {
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
            .save::<A, _>(
                &info.aggregate_id,
                info.events.clone(),
                info.original_version,
            )
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|e| CommandMessage {
                        event: e,
                        producer: "example.eu-west-3".to_owned(),
                    })
                    .collect::<Vec<CommandMessage>>()
            });

        match res {
            Ok(events) => {
                if let Err(e) = producer.send_all(events).await {
                    return HttpResponse::InternalServerError().json(json!({
                        "code": "internal_server_error",
                        "reason": e.to_string()
                    }));
                }

                HttpResponse::Ok().json(json!({
                    "id": info.aggregate_id
                }))
            }
            Err(e) => HttpResponse::InternalServerError().json(json!({
                "code": "internal_server_error",
                "reason": e.to_string()
            })),
        }
    }
}
