use actix::{ActorFutureExt, Context, Handler, Message, ResponseActFuture, WrapFuture};
use evento::store::{Engine, PgEngine};
use evento::{Event, EventStore};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::{Command, CommandResult, Error};

use super::{
    aggregate::Product,
    event::{
        Created, Deleted, DescriptionUpdated, ProductEvent, QuantityUpdated, ReviewAdded,
        VisibilityUpdated,
    },
};

pub async fn load_product(
    store: &EventStore<PgEngine>,
    id: &str,
) -> Result<(Product, Event), Error> {
    let (product, e) = match store.load::<Product, _>(id).await? {
        Some(product) => product,
        _ => return Err(Error::NotFound("product".to_owned(), id.to_owned())),
    };

    if product.deleted {
        return Err(Error::NotFound("product".to_owned(), id.to_owned()));
    }

    Ok((product, e))
}

#[derive(Message, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
#[rtype(result = "CommandResult")]
pub struct CreateCommand {
    #[validate(length(min = 3, max = 25))]
    pub name: String,
}

impl Handler<CreateCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: CreateCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async move {
            msg.validate()?;

            let id = nanoid!();
            Ok(Event::new(ProductEvent::Created)
                .aggregate_id(id)
                .version(0)
                .data(Created { name: msg.name })?
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
            let (_, e) = load_product(&store, &msg.id).await?;

            Ok(Event::new(ProductEvent::Deleted)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(Deleted { deleted: true })?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize, Validate)]
#[rtype(result = "CommandResult")]
pub struct UpdateQuantityCommand {
    pub id: String,
    #[validate(range(min = 0))]
    pub quantity: u16,
}

impl Handler<UpdateQuantityCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateQuantityCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let store = self.store.clone();

        async move {
            msg.validate()?;

            let (product, e) = load_product(&store, &msg.id).await?;

            if product.quantity == msg.quantity {
                return Err(Error::BadRequest(format!(
                    "product.quantity already `{}`",
                    msg.quantity
                )));
            }

            Ok(Event::new(ProductEvent::QuantityUpdated)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(QuantityUpdated {
                    quantity: msg.quantity,
                })?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct UpdateVisibilityCommand {
    pub id: String,
    pub visible: bool,
}

impl Handler<UpdateVisibilityCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateVisibilityCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let store = self.store.clone();

        async move {
            let (product, e) = load_product(&store, &msg.id).await?;

            if product.visible == msg.visible {
                return Err(Error::BadRequest(format!(
                    "product.visible already `{}`",
                    msg.visible
                )));
            }

            Ok(Event::new(ProductEvent::VisibilityUpdated)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(VisibilityUpdated {
                    visible: msg.visible,
                })?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize, Validate)]
#[rtype(result = "CommandResult")]
pub struct UpdateDescriptionCommand {
    pub id: String,
    #[validate(length(min = 3, max = 255))]
    pub description: String,
}

impl Handler<UpdateDescriptionCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateDescriptionCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let store = self.store.clone();

        async move {
            msg.validate()?;

            let (product, e) = load_product(&store, &msg.id).await?;

            if product.description == msg.description {
                return Err(Error::BadRequest(format!(
                    "product.description already `{}`",
                    msg.description
                )));
            }

            Ok(Event::new(ProductEvent::DescriptionUpdated)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(DescriptionUpdated {
                    description: msg.description,
                })?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}

#[derive(Message, Deserialize, Validate)]
#[rtype(result = "CommandResult")]
pub struct AddReviewCommand {
    pub id: String,
    #[validate(range(min = 0, max = 10))]
    pub note: u8,
    #[validate(length(min = 3, max = 255))]
    pub message: String,
}

impl Handler<AddReviewCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: AddReviewCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let store = self.store.clone();

        async move {
            msg.validate()?;

            let (_, e) = load_product(&store, &msg.id).await?;

            Ok(Event::new(ProductEvent::ReviewAdded)
                .aggregate_id(&msg.id)
                .version(e.version)
                .data(ReviewAdded {
                    note: msg.note,
                    message: msg.message,
                })?
                .into())
        }
        .into_actor(self)
        .boxed_local()
    }
}
