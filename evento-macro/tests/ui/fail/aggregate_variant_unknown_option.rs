#[evento::aggregate]
pub enum Account {
    #[evento(rename = "opened.v1")]
    Opened { id: String },
}

fn main() {}
