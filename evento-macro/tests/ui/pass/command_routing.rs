use evento::Executor;

#[evento::aggregate]
pub enum Reg {
    Appended { value: i64 },
}

pub struct Command<E: Executor>(pub E);

#[evento::command]
impl<E: Executor> Command<E> {
    /// Written once; wrappers `append` and `append_with_routing` are generated.
    pub async fn append(
        &self,
        id: impl Into<String>,
        value: i64,
        routing_key: Option<String>,
    ) -> anyhow::Result<String> {
        Ok(evento::append(id)
            .routing_key_opt(routing_key)
            .event(&Appended { value })
            .commit(&self.0)
            .await?)
    }

    /// No trailing `routing_key` parameter: passes through untouched.
    pub fn plain(&self) -> i64 {
        7
    }
}

async fn _use<E: Executor>(c: Command<E>) -> anyhow::Result<()> {
    let _ = c.append("id", 1).await?;
    let _ = c.append_with_routing("id", 1, "eu-west").await?;
    let _ = c.append_opt("id", 1, None).await?;
    let _ = c.plain();
    Ok(())
}

fn main() {}
