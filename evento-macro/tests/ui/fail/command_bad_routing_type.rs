pub struct Command;

#[evento::command]
impl Command {
    pub async fn act(&self, routing_key: Option<i64>) -> i64 {
        routing_key.unwrap_or_default()
    }
}

fn main() {}
