use actix::{ActorFutureExt, Context, Handler, Message, ResponseActFuture, WrapFuture};
use evento::{CommandError, CommandResult, Event, PgEvento};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::Command;

use super::{
    aggregate::{Order, Product, Status},
    event::{
        Canceled, Deleted, OrderEvent, Paid, Placed, ProductAdded, ProductQuantityUpdated,
        ProductRemoved, ShippingInfoUpdated,
    },
};

pub async fn load_order(evento: &PgEvento, id: &str) -> Result<(Order, Event), CommandError> {
    let (order, e) = match evento.load::<Order, _>(id).await? {
        Some(order) => order,
        _ => return Err(CommandError::NotFound("order".to_owned(), id.to_owned())),
    };

    if order.status == Status::Canceled || order.status == Status::Deleted {
        return Err(CommandError::NotFound("order".to_owned(), id.to_owned()));
    }

    Ok((order, e))
}

#[derive(Message, Deserialize)]
#[serde(rename_all = "camelCase")]
#[rtype(result = "CommandResult")]
pub struct PlaceCommand {
    pub products: Vec<Product>,
}

impl Handler<PlaceCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: PlaceCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let producer = self.producer.clone();

        async move {
            let mut events = vec![Event::new(OrderEvent::Placed).data(Placed::default())?];

            for product in msg.products {
                product.validate()?;
                events.push(
                    Event::new(OrderEvent::ProductAdded).data::<ProductAdded>(product.into())?,
                )
            }

            let id = nanoid!();

            producer.publish::<Order, _>(&id, events, 0).await?;

            Ok(id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct AddProductCommand {
    pub id: String,
    pub product: Product,
}

impl Handler<AddProductCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: AddProductCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            msg.product.validate()?;

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::ProductAdded)
                        .data::<ProductAdded>(msg.product.into())?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct RemoveProductCommand {
    pub id: String,
    pub product_id: String,
}

impl Handler<RemoveProductCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: RemoveProductCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            if !order.products.contains_key(&msg.product_id) {
                return Err(CommandError::NotFound(
                    "product".to_owned(),
                    msg.product_id.to_owned(),
                ));
            }

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::ProductRemoved)
                        .data(ProductRemoved { id: msg.product_id })?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct UpdateProductQuantityCommand {
    pub id: String,
    pub product: Product,
}

impl Handler<UpdateProductQuantityCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(
        &mut self,
        msg: UpdateProductQuantityCommand,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            if !order.products.contains_key(&msg.product.id) {
                return Err(CommandError::NotFound(
                    "product".to_owned(),
                    msg.product.id.to_owned(),
                ));
            }

            msg.product.validate()?;

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::ProductQuantityUpdated)
                        .data::<ProductQuantityUpdated>(msg.product.into())?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct UpdateShippingInfoCommand {
    pub id: String,
    pub address: String,
}

impl Handler<UpdateShippingInfoCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateShippingInfoCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            if order.shipping_address == msg.address {
                return Err(CommandError::BadRequest(format!(
                    "address `{}` not changed",
                    msg.address
                )));
            }

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::ShippingInfoUpdated).data(
                        ShippingInfoUpdated {
                            shipping_address: msg.address,
                        },
                    )?],
                    e.version,
                )
                .await?;
            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct PayCommand {
    pub id: String,
}

impl Handler<PayCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: PayCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            if order.shipping_address.is_empty() {
                return Err(CommandError::BadRequest(
                    "order shipping address must not be empty".to_owned(),
                ));
            }

            if order.products.is_empty() {
                return Err(CommandError::BadRequest(
                    "order products must not be empty".to_owned(),
                ));
            }

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::Paid).data(Paid::default())?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct DeleteCommand {
    pub id: String,
}

impl Handler<DeleteCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: DeleteCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(CommandError::BadRequest(
                    "order status must be `draft`".to_owned(),
                ));
            }

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::Deleted).data(Deleted::default())?],
                    e.version,
                )
                .await?;
            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct CancelCommand {
    pub id: String,
}

impl Handler<CancelCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: CancelCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (order, e) = load_order(&evento, &msg.id).await?;

            if order.status != Status::Pending {
                return Err(CommandError::BadRequest(
                    "order status must be `pending`".to_owned(),
                ));
            }

            producer
                .publish::<Order, _>(
                    &msg.id,
                    vec![Event::new(OrderEvent::Canceled).data(Canceled::default())?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}
