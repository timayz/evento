#[evento::projection(id = account_id)]
pub struct View {
    pub id: String,
    pub balance: i64,
}

fn main() {}
