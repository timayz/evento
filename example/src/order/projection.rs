use futures::TryStreamExt;
use mongodb::Database;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use serde::{Deserialize, Serialize};

use crate::{command::CommandInfo, order::event::OrderEvent};

use super::{
    aggregate::Status,
    event::{Canceled, Deleted, Paid, Placed, ProductAdded, ProductRemoved, ShippingInfoUpdated},
};

#[derive(Default, Serialize, Deserialize)]
pub struct OrderProduct {
    pub id: String,
    pub name: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub shipping_address: String,
    pub products: Vec<OrderProduct>,
    pub status: Status,
}

pub async fn start(pulsar: &Pulsar<TokioExecutor>, read_db: &Database) {
    let mut consumer: Consumer<CommandInfo, _> = pulsar
        .consumer()
        .with_topic("non-persistent://public/default/order")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await
        .unwrap();

    let db = read_db.clone();

    tokio::spawn(async move {
        while let Some(msg) = consumer.try_next().await.unwrap() {
            consumer.ack(&msg).await.unwrap();
            let info = match msg.deserialize() {
                Ok(info) => info,
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            };

            for event in info.events {
                let order_event: OrderEvent = event.name.parse().unwrap();

                match order_event {
                    OrderEvent::Placed => {
                        let data: Placed = event.to_data().unwrap();

                        let collection = db.collection::<Order>("orders");
                        collection
                            .insert_one(
                                Order {
                                    id: info.aggregate_id.to_owned(),
                                    status: data.status,
                                    ..Order::default()
                                },
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    OrderEvent::ProductAdded => {
                        let data: ProductAdded = event.to_data().unwrap();
                        // let product = self.products.entry(data.id).or_default();
                        // product.quantity += data.quantity;
                    }
                    OrderEvent::ProductRemoved => {
                        let data: ProductRemoved = event.to_data().unwrap();
                        // self.products.remove(&data.id);
                    }
                    OrderEvent::ProductQuantityUpdated => {
                        let data: ProductAdded = event.to_data().unwrap();
                        // let product = self.products.entry(data.id).or_default();
                        // product.quantity = data.quantity;
                    }
                    OrderEvent::ShippingInfoUpdated => {
                        let data: ShippingInfoUpdated = event.to_data().unwrap();
                        // self.shipping_address = data.shipping_address;
                    }
                    OrderEvent::Paid => {
                        let data: Paid = event.to_data().unwrap();
                        // self.status = data.status;
                    }
                    OrderEvent::Deleted => {
                        let data: Deleted = event.to_data().unwrap();
                        // self.status = data.status;
                    }
                    OrderEvent::Canceled => {
                        let data: Canceled = event.to_data().unwrap();
                        // self.status = data.status;
                    }
                }
            }
        }
    });
}
