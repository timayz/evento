use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{
        rejection::{ExtensionRejection, FormRejection},
        Form, FromRequest, FromRequestParts,
    },
    http::Request,
    response::IntoResponse,
    response::Response,
    BoxError, Extension, RequestPartsExt,
};
use evento::{CommandError, CommandHandler};
use evento_store::Event;
use serde::de::DeserializeOwned;
use std::{collections::HashMap, marker::PhantomData};
use tracing::error;
use validator::Validate;

use crate::UserLanguage;

#[derive(Clone, Debug)]
pub struct Command<T, R> {
    pub output: Result<Vec<Event>, HashMap<String, Vec<String>>>,
    input: PhantomData<T>,
    render: PhantomData<R>,
}

#[async_trait]
impl<S, B, T, R> FromRequest<S, B> for Command<T, R>
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
    T: Send + Sync,
    T: DeserializeOwned,
    T: Validate,
    T: CommandHandler,
    R: CommandRender,
{
    type Rejection = CommandRejection;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let cmd = parts.extract::<Extension<evento::Command>>().await?;
        let lang = UserLanguage::from_request_parts(&mut parts, state)
            .await
            .map_err(|_| CommandRejection::Infallible)?;

        let req = Request::from_parts(parts, body);
        let Form(input) = Form::<T>::from_request(req, state).await?;
        let output = cmd
            .0
            .execute::<T, _>(
                lang.preferred_languages()
                    .first()
                    .cloned()
                    .unwrap_or("en".to_owned()),
                &input,
            )
            .await;

        match output {
            Ok(events) => Ok(Self {
                output: Ok(events),
                input: PhantomData,
                render: PhantomData,
            }),
            Err(err) => match err {
                CommandError::Server(msg) => Err(CommandRejection::Command(R::server(&cmd.0, msg))),
                CommandError::Validation(errors) => Ok(Self {
                    output: Err(errors),
                    input: PhantomData,
                    render: PhantomData,
                }),
                CommandError::NotFound(msg) => {
                    Err(CommandRejection::Command(R::not_found(&cmd.0, msg)))
                }
            },
        }
    }
}

pub trait CommandRender {
    fn not_found(cmd: &evento::Command, msg: String) -> Response;
    fn server(cmd: &evento::Command, msg: String) -> Response;
}

#[derive(thiserror::Error, Debug)]
pub enum CommandRejection {
    #[error("{0}")]
    Form(#[from] FormRejection),

    #[error("{0}")]
    Extension(#[from] ExtensionRejection),

    #[error("cmmand")]
    Command(Response),

    #[error("infallible")]
    Infallible,
}

impl IntoResponse for CommandRejection {
    fn into_response(self) -> Response {
        match self {
            CommandRejection::Form(rejection) => rejection.into_response(),
            CommandRejection::Extension(rejection) => rejection.into_response(),
            CommandRejection::Command(res) => res,
            _ => unreachable!(),
        }
    }
}
