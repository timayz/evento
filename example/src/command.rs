use evento::{PgEvento, PgProducer};

#[derive(Clone)]
pub struct Command {
    pub evento: PgEvento,
    pub producer: PgProducer,
}

impl Command {
    pub fn new(evento: PgEvento, producer: PgProducer) -> Self {
        Self { evento, producer }
    }
}
