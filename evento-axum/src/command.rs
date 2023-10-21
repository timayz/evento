use async_trait::async_trait;
use axum::{
    extract::{rejection::QueryRejection, FromRequestParts, Query},
    http::request::Parts,
    response::IntoResponse,
    response::Response,
};
use evento_store::Event;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct CommandOutput<T> {
    pub events: Vec<Event>,
    phantom_data: PhantomData<T>,
}

#[derive(Clone)]
pub struct Command<T>(CommandOutput<T>);

#[async_trait]
impl<T, S> FromRequestParts<S> for Command<T>
where
    T: DeserializeOwned,
    T: CommandHandler<S>,
    T: Send + Sync,
    S: Send + Sync,
{
    type Rejection = CommandRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(input) = Query::<T>::from_request_parts(parts, state).await?;
        let events = input.handle(state).await?;

        Ok(Self(CommandOutput {
            events,
            phantom_data: PhantomData,
        }))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CommandRejection {
    #[error("{0}")]
    Query(#[from] QueryRejection),

    #[error("{0}")]
    Any(#[from] anyhow::Error),
}

impl IntoResponse for CommandRejection {
    fn into_response(self) -> Response {
        todo!()
    }
}

#[async_trait]
pub trait CommandHandler<S: Send + Sync> {
    async fn handle(&self, state: &S) -> anyhow::Result<Vec<Event>>;
}
