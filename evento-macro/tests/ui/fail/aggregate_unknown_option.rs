#[evento::aggregate(rename = "custom/Account")]
pub enum Account {
    Opened { id: String },
}

fn main() {}
