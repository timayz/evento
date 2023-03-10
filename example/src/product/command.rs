use actix::{ActorFutureExt, Context, Handler, Message, ResponseActFuture, WrapFuture};
use evento::{CommandError, CommandResult, Event, Evento};
use nanoid::nanoid;
use serde::Deserialize;
use validator::Validate;

use crate::command::Command;

use super::{
    aggregate::Product,
    event::{
        Created, Deleted, DescriptionUpdated, ProductEvent, QuantityUpdated, ReviewAdded,
        VisibilityUpdated,
    },
};

pub async fn load_product(
    store: &Evento<evento::PgEngine, evento::store::PgEngine>,
    id: &str,
) -> Result<(Product, Event), CommandError> {
    let (product, e) = match store.load::<Product, _>(id).await? {
        Some(product) => product,
        _ => return Err(CommandError::NotFound("product".to_owned(), id.to_owned())),
    };

    if product.deleted {
        return Err(CommandError::NotFound("product".to_owned(), id.to_owned()));
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
        let producer = self.producer.clone();

        async move {
            msg.validate()?;

            let id = nanoid!();

            producer
                .publish::<Product, _>(
                    &id,
                    vec![Event::new(ProductEvent::Created).data(Created { name: msg.name })?],
                    0,
                )
                .await?;
            Ok(id)
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
            let (_, e) = load_product(&evento, &msg.id).await?;

            producer
                .publish::<Product, _>(
                    &msg.id,
                    vec![Event::new(ProductEvent::Deleted).data(Deleted { deleted: true })?],
                    e.version,
                )
                .await?;

            Ok(msg.id)
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
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            msg.validate()?;

            let (product, e) = load_product(&evento, &msg.id).await?;

            if product.quantity == msg.quantity {
                return Err(CommandError::BadRequest(format!(
                    "product.quantity already `{}`",
                    msg.quantity
                )));
            }

            producer
                .publish::<Product, _>(
                    &msg.id,
                    vec![
                        Event::new(ProductEvent::QuantityUpdated).data(QuantityUpdated {
                            quantity: msg.quantity,
                        })?,
                    ],
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
pub struct UpdateVisibilityCommand {
    pub id: String,
    pub visible: bool,
}

impl Handler<UpdateVisibilityCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateVisibilityCommand, _ctx: &mut Context<Self>) -> Self::Result {
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            let (product, e) = load_product(&evento, &msg.id).await?;

            if product.visible == msg.visible {
                return Err(CommandError::BadRequest(format!(
                    "product.visible already `{}`",
                    msg.visible
                )));
            }

            producer
                .publish::<Product, _>(
                    &msg.id,
                    vec![
                        Event::new(ProductEvent::VisibilityUpdated).data(VisibilityUpdated {
                            visible: msg.visible,
                        })?,
                    ],
                    e.version,
                )
                .await?;

            Ok(msg.id)
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
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            msg.validate()?;

            let (product, e) = load_product(&evento, &msg.id).await?;

            if product.description == msg.description {
                return Err(CommandError::BadRequest(format!(
                    "product.description already `{}`",
                    msg.description
                )));
            }

            producer
                .publish::<Product, _>(
                    &msg.id,
                    vec![Event::new(ProductEvent::DescriptionUpdated).data(
                        DescriptionUpdated {
                            description: msg.description,
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
        let evento = self.evento.clone();
        let producer = self.producer.clone();

        async move {
            msg.validate()?;

            let (_, e) = load_product(&evento, &msg.id).await?;

            producer
                .publish::<Product, _>(
                    &msg.id,
                    vec![Event::new(ProductEvent::ReviewAdded).data(ReviewAdded {
                        note: msg.note,
                        message: msg.message,
                    })?],
                    e.version,
                )
                .await?;
            Ok(msg.id)
        }
        .into_actor(self)
        .boxed_local()
    }
}
