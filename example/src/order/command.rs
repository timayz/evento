use actix::{ActorFutureExt, Context, Handler, Message, ResponseActFuture, WrapFuture};
use serde::Deserialize;

use crate::command::{Command, CommandResult};

#[derive(Deserialize)]
pub struct Product {}

#[derive(Message, Deserialize)]
#[serde(rename_all = "camelCase")]
#[rtype(result = "CommandResult")]
pub struct PlaceCommand {
    product_ids: Vec<String>,
}

impl Handler<PlaceCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: PlaceCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct AddProductCommand {
    id: String,
}

impl Handler<AddProductCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: AddProductCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct RemoveProductCommand {
    id: String,
}

impl Handler<RemoveProductCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: RemoveProductCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct UpdateProductQuantityCommand {
    id: String,
    quantity: u16,
}

impl Handler<UpdateProductQuantityCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(
        &mut self,
        msg: UpdateProductQuantityCommand,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct UpdateShippingInfoCommand {
    address: String,
}

impl Handler<UpdateShippingInfoCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: UpdateShippingInfoCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct PayCommand;

impl Handler<PayCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: PayCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}

#[derive(Message, Deserialize)]
#[rtype(result = "CommandResult")]
pub struct CancelCommand;

impl Handler<CancelCommand> for Command {
    type Result = ResponseActFuture<Self, CommandResult>;

    fn handle(&mut self, msg: CancelCommand, _ctx: &mut Context<Self>) -> Self::Result {
        async { Ok(todo!()) }.into_actor(self).boxed_local()
    }
}
