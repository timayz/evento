use evento::Aggregate;
use serde::{Deserialize, Serialize};
use super::event::OrderEvent;

#[derive(Default, Serialize, Deserialize)]
struct Order {
    
}

impl Aggregate for Order {
    fn apply(&mut self, event: &evento::Event) {
        let user_event: OrderEvent = event.name.parse().unwrap();

        match (user_event, &event.data.is_some()) {
            // (OrderEvent::Created, true) => {
            //     let data: Created = event.to_data().unwrap().unwrap();
            //     self.username = data.username;
            //     self.password = data.password;
            // }
            (_, _) => todo!(),
        }
    }

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("order_{}", id.into())
    }
}
