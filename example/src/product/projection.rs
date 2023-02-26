use evento::Aggregate;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, to_bson},
    Database,
};
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use serde::{Deserialize, Serialize};

use crate::{command::CommandMessage, product::event::ProductEvent};

use super::{
    aggregate,
    event::{Created, DescriptionUpdated, QuantityUpdated, ReviewAdded, VisibilityUpdated},
};

#[derive(Default, Serialize, Deserialize)]
pub struct Product {
    pub id: String,
    pub name: String,
    pub description: String,
    pub visible: bool,
    pub quantity: u16,
    pub deleted: bool,
    pub reviews: Vec<(u8, String)>,
}

pub async fn start(pulsar: &Pulsar<TokioExecutor>, read_db: &Database) {
    let mut consumer: Consumer<CommandMessage, _> = pulsar
        .consumer()
        .with_topic("non-persistent://public/default/product")
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
            let event = match msg.deserialize() {
                Ok(msg) => msg.event,
                Err(e) => {
                    println!("{e:?}");
                    break;
                }
            };

            let product_event: ProductEvent = event.name.parse().unwrap();

            match product_event {
                ProductEvent::Created => {
                    let data: Created = event.to_data().unwrap();

                    let collection = db.collection::<Product>("products");
                    collection
                        .insert_one(
                            Product {
                                id: aggregate::Product::to_id(event.aggregate_id),
                                name: data.name,
                                ..Product::default()
                            },
                            None,
                        )
                        .await
                        .unwrap();
                }
                ProductEvent::QuantityUpdated => {
                    let data: QuantityUpdated = event.to_data().unwrap();
                    let collection = db.collection::<Product>("products");
                    let filter = doc! {"id":aggregate::Product::to_id(event.aggregate_id) };

                    collection
                        .update_one(
                            filter,
                            doc! {"$set": {"quantity": to_bson(&data.quantity).unwrap()}},
                            None,
                        )
                        .await
                        .unwrap();
                }
                ProductEvent::VisibilityUpdated => {
                    let data: VisibilityUpdated = event.to_data().unwrap();
                    let collection = db.collection::<Product>("products");
                    let filter = doc! {"id":aggregate::Product::to_id(event.aggregate_id) };

                    collection
                        .update_one(filter, doc! {"$set": {"visible": data.visible}}, None)
                        .await
                        .unwrap();
                }
                ProductEvent::DescriptionUpdated => {
                    let data: DescriptionUpdated = event.to_data().unwrap();
                    let collection = db.collection::<Product>("products");
                    let filter = doc! {"id":aggregate::Product::to_id(event.aggregate_id) };

                    collection
                        .update_one(
                            filter,
                            doc! {"$set": {"description": data.description}},
                            None,
                        )
                        .await
                        .unwrap();
                }
                ProductEvent::ReviewAdded => {
                    let data: ReviewAdded = event.to_data().unwrap();
                    let collection = db.collection::<Product>("products");
                    let filter = doc! {"id":aggregate::Product::to_id(event.aggregate_id) };

                    collection
                            .update_one(
                                filter,
                                doc! {"$push": {"reviews": to_bson(&(data.note, data.message)).unwrap()}},
                                None,
                            )
                            .await
                            .unwrap();
                }
                ProductEvent::Deleted => {
                    let filter = doc! {"id":aggregate::Product::to_id(event.aggregate_id) };

                    let collection = db.collection::<Product>("products");
                    collection.delete_one(filter, None).await.unwrap();
                }
            }
        }
    });
}
