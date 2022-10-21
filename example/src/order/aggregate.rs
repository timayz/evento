use evento::Aggregate;
use serde::{Deserialize, Serialize};
use super::event::OrderEvent;

#[derive(Default, Serialize, Deserialize)]
pub struct Order {
    
}

impl Aggregate for Order {
    fn apply(&mut self, event: &evento::Event) {
        let order_event: OrderEvent = event.name.parse().unwrap();

        match order_event {
            // OrderEvent::Created, => {
            //     let data: Created = event.to_data().unwrap();
            //     self.username = data.username;
            //     self.password = data.password;
            // }
            _ => todo!(),
        }
    }

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("order_{}", id.into())
    }
}
