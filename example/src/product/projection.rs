use evento::{Aggregate, PgSubscriber, Subscriber};
use futures::FutureExt;
use mongodb::{
    bson::{doc, to_bson},
    Database,
};
use serde::{Deserialize, Serialize};

use crate::product::event::ProductEvent;

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

pub fn subscribe() -> PgSubscriber {
    Subscriber::new("products")
        .handler("product/**", |event, ctx| {
            let db = ctx.0.read().extract::<Database>().clone();

            async move {
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
                };

                Ok(None)
            }
            .boxed()
        })
}
