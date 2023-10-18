use evento::Producer;

#[derive(Clone)]
pub struct Command {
    pub producer: Producer,
}

impl Command {
    pub fn new(producer: Producer) -> Self {
        Self { producer }
    }
}
