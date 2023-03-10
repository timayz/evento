use actix::{Actor, Context};
use evento::{PgEvento, PgProducer};

pub struct Command {
    pub evento: PgEvento,
    pub producer: PgProducer,
}

impl Command {
    pub fn new(evento: PgEvento, producer: PgProducer) -> Self {
        Self { evento, producer }
    }
}

impl Actor for Command {
    type Context = Context<Self>;
}
