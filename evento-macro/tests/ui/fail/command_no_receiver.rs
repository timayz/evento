pub struct Command;

#[evento::command]
impl Command {
    pub async fn act(routing_key: Option<String>) -> Option<String> {
        routing_key
    }
}

fn main() {}
