use evento::PgProducer;

#[derive(Clone)]
pub struct Command {
    pub producer: PgProducer,
}

impl Command {
    pub fn new(producer: PgProducer) -> Self {
        Self { producer }
    }
}
