use actix::prelude::*;
use actix_web::{HttpResponse};
use evento::{Event, EventStore, RbatisEngine};
use rbatis::Rbatis;

pub enum Error {
    V,
}

pub type CommandResult = Result<Vec<Event>, Error>;

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
    pub async fn into_http_response(&self) -> HttpResponse {
        HttpResponse::Ok().json("")
    }
}
