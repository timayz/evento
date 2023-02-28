use actix::{ActorFutureExt, Context, Handler, Message, ResponseActFuture, WrapFuture};
use evento::{Event, Evento};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::{Command, CommandInfo, CommandResult, Error};

use super::{
    aggregate::{Order, Product, Status},
    event::{
        Canceled, Deleted, OrderEvent, Paid, Placed, ProductAdded, ProductQuantityUpdated,
        ProductRemoved, ShippingInfoUpdated,
    },
};

pub async fn load_order(store: &Evento<evento::PgEngine, evento::store::PgEngine>, id: &str) -> Result<(Order, Event), Error> {
    let (order, e) = match store.load::<Order, _>(id).await? {
        Some(order) => order,
        _ => return Err(Error::NotFound("order".to_owned(), id.to_owned())),
    };

    if order.status == Status::Canceled || order.status == Status::Deleted {
        return Err(Error::NotFound("order".to_owned(), id.to_owned()));
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
        async move {
            let mut events = vec![Event::new(OrderEvent::Placed).data(Placed::default())?];

            for product in msg.products {
                product.validate()?;
                events.push(
                    Event::new(OrderEvent::ProductAdded).data::<ProductAdded>(product.into())?,
                )
            }

            let aggregate_id = nanoid!();

            Ok(CommandInfo {
                aggregate_id,
                original_version: 0,
                events,
            })
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            msg.product.validate()?;

            Ok(Event::new(OrderEvent::ProductAdded)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data::<ProductAdded>(msg.product.into())?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            if !order.products.contains_key(&msg.product_id) {
                return Err(Error::NotFound(
                    "product".to_owned(),
                    msg.product_id.to_owned(),
                ));
            }

            Ok(Event::new(OrderEvent::ProductRemoved)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(ProductRemoved { id: msg.product_id })?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            if !order.products.contains_key(&msg.product.id) {
                return Err(Error::NotFound(
                    "product".to_owned(),
                    msg.product.id.to_owned(),
                ));
            }

            msg.product.validate()?;

            Ok(Event::new(OrderEvent::ProductQuantityUpdated)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data::<ProductQuantityUpdated>(msg.product.into())?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            if order.shipping_address == msg.address {
                return Err(Error::BadRequest(format!(
                    "address `{}` not changed",
                    msg.address
                )));
            }

            Ok(Event::new(OrderEvent::ShippingInfoUpdated)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(ShippingInfoUpdated {
                    shipping_address: msg.address,
                })?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            if order.shipping_address.is_empty() {
                return Err(Error::BadRequest(
                    "order shipping address must not be empty".to_owned(),
                ));
            }

            if order.products.is_empty() {
                return Err(Error::BadRequest(
                    "order products must not be empty".to_owned(),
                ));
            }

            Ok(Event::new(OrderEvent::Paid)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(Paid::default())?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Draft {
                return Err(Error::BadRequest("order status must be `draft`".to_owned()));
            }

            Ok(Event::new(OrderEvent::Deleted)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(Deleted::default())?
                .into())
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
        let store = self.store.clone();

        async move {
            let (order, e) = load_order(&store, &msg.id).await?;

            if order.status != Status::Pending {
                return Err(Error::BadRequest(
                    "order status must be `pending`".to_owned(),
                ));
            }

            Ok(Event::new(OrderEvent::Canceled)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(Canceled::default())?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}
