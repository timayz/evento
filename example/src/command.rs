use actix::{Actor, Context};
use evento::{Evento, PgEngine};

pub struct Command {
    pub store: Evento<PgEngine, evento::store::PgEngine>,
}

impl Command {
    pub fn new(store: Evento<PgEngine, evento::store::PgEngine>) -> Self {
        Self { store }
    }
}

impl Actor for Command {
    type Context = Context<Self>;
}
