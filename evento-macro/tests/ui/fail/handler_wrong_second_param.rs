use evento::metadata::Event;

#[evento::aggregate]
pub enum Acc {
    Opened { name: String },
}

#[evento::projection]
pub struct Row {
    pub name: String,
}

#[evento::handler]
async fn handle_opened(event: Event<Opened>, row: Row) -> anyhow::Result<()> {
    let _ = (event, row);
    Ok(())
}

fn main() {}
