#[evento::aggregate]
pub enum Account {
    Opened { pub(crate) id: String },
}

fn main() {}
