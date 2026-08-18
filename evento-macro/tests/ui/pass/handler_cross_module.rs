//! A `pub` handler fn yields a `pub` constructor, so handlers can live in a
//! different module than the projection wiring.

mod handlers {
    use evento::metadata::Event;

    #[evento::aggregate]
    pub enum Acc {
        Opened { name: String },
    }

    #[evento::projection(bitcode::Encode, bitcode::Decode)]
    pub struct Row {
        pub name: String,
    }

    #[evento::handler]
    pub async fn handle_opened(event: Event<Opened>, row: &mut Row) -> anyhow::Result<()> {
        row.name = event.data.name.clone();
        Ok(())
    }
}

fn _wiring<E: evento::Executor>() -> evento::Projection<E, handlers::Row> {
    evento::Projection::new::<handlers::Acc>().handler(handlers::handle_opened())
}

fn main() {}
