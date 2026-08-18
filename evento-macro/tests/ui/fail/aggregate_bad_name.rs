#[evento::aggregate(name = 42)]
pub enum Account {
    Opened { id: String },
}

fn main() {}
