use anyhow::Result;
use async_trait::async_trait;
use evento::{
    store::{Aggregate, Event},
    ConsumerContext, Rule, RuleHandler,
};
use mongodb::{
    bson::{doc, to_bson},
    Database,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::order::event::OrderEvent;

use super::{
    aggregate::{self, Status},
    event::{Canceled, Paid, Placed, ProductAdded, ProductRemoved, ShippingInfoUpdated},
};

#[derive(Default, Serialize, Deserialize)]
pub struct OrderProduct {
    pub id: String,
    pub name: String,
    pub quantity: u16,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub shipping_address: String,
    pub products: HashMap<String, OrderProduct>,
    pub status: Status,
}

#[derive(Clone)]
struct OrdersHandler;

#[async_trait]
impl RuleHandler for OrdersHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let db = ctx.extract::<Database>();

        let order_event: OrderEvent = event.name.parse().unwrap();

        match order_event {
            OrderEvent::Placed => {
                let data: Placed = event.to_data().unwrap();

                let collection = db.collection::<Order>("orders");
                collection
                    .insert_one(
                        Order {
                            id: aggregate::Order::to_id(event.aggregate_id),
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

                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };
                let mut order = match collection.find_one(filter.clone(), None).await.unwrap() {
                    Some(order) => order,
                    _ => return Ok(None),
                };

                let product = order.products.entry(data.id).or_default();
                product.quantity += data.quantity;

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"products": to_bson(&order.products).unwrap()}},
                        None,
                    )
                    .await
                    .unwrap();
            }
            OrderEvent::ProductRemoved => {
                let data: ProductRemoved = event.to_data().unwrap();
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };
                let mut order = match collection.find_one(filter.clone(), None).await.unwrap() {
                    Some(order) => order,
                    _ => return Ok(None),
                };

                order.products.remove(&data.id);

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"products": to_bson(&order.products).unwrap()}},
                        None,
                    )
                    .await
                    .unwrap();
            }
            OrderEvent::ProductQuantityUpdated => {
                let data: ProductAdded = event.to_data().unwrap();
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };
                let mut order = match collection.find_one(filter.clone(), None).await.unwrap() {
                    Some(order) => order,
                    _ => return Ok(None),
                };

                let product = order.products.entry(data.id).or_default();
                product.quantity = data.quantity;

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"products": to_bson(&order.products).unwrap()}},
                        None,
                    )
                    .await
                    .unwrap();
            }
            OrderEvent::ShippingInfoUpdated => {
                let data: ShippingInfoUpdated = event.to_data().unwrap();
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"shipping_address": data.shipping_address}},
                        None,
                    )
                    .await
                    .unwrap();
            }
            OrderEvent::Paid => {
                let data: Paid = event.to_data().unwrap();
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"status": to_bson(&data.status).unwrap()}},
                        None,
                    )
                    .await
                    .unwrap();
            }
            OrderEvent::Deleted => {
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };

                collection.delete_one(filter, None).await.unwrap();
            }
            OrderEvent::Canceled => {
                let data: Canceled = event.to_data().unwrap();
                let collection = db.collection::<Order>("orders");
                let filter = doc! {"id":aggregate::Order::to_id(event.aggregate_id) };

                collection
                    .update_one(
                        filter,
                        doc! {"$set": {"status": to_bson(&data.status).unwrap()}},
                        None,
                    )
                    .await
                    .unwrap();
            }
        };

        Ok(None)
    }
}

pub fn rule() -> Rule {
    Rule::new("orders").handler("order/**", OrdersHandler)
}
